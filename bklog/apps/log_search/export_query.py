"""UnifyQuery statistics over frozen routes and an authorized bound transport."""

from contextlib import AbstractContextManager
from copy import deepcopy
from typing import Protocol

import ujson
from django.conf import settings

from apps.log_search.export_contracts import PlanningError, nonnegative_integer


class Statistics(Protocol):
    def count(self, start: int, end: int, *, timeout: float) -> int: ...
    def histogram(self, start: int, end: int, interval: int, *, timeout: float) -> dict[int, int]: ...
    def sample(self, start: int, end: int, limit: int, *, timeout: float) -> list[bytes]: ...


class StatisticsFactory(Protocol):
    """Scope authorization/resources to the entire planning attempt."""

    def __call__(self, job) -> AbstractContextManager[Statistics]: ...


def encode_export_row(row):
    """Shared JSONL encoding for projected/desensitized sample and worker rows."""
    return (ujson.dumps(row, ensure_ascii=False) + "\n").encode("utf-8")


class UnifyQueryStatistics:
    """Use frozen query routes and bound, authorized UnifyQuery primitives.

    raw/reference must accept (params, timeout=...) and preserve tenant/user
    context and scene permission checks. project is the same batch conversion
    used by the Part reader. The factory owns restoring/clearing that context.
    No network capability is assumed: each mode requires deployment validation.
    """

    def __init__(self, job, *, raw, reference, project):
        if job.query_kind not in settings.ASYNC_EXPORT_VERIFIED_QUERY_KINDS:
            raise PlanningError("QUERY_PROTOCOL_NOT_VERIFIED")
        self.base = deepcopy(job.query_snapshot["unify_query"])
        if not self.base.get("query_list") or not job.resolved_resource_ids:
            raise PlanningError("INCOMPLETE_ROUTING_SNAPSHOT")
        if self.base["query_list"] != job.routing_snapshot.get("query_list"):
            raise PlanningError("ROUTING_SNAPSHOT_MISMATCH")
        self.tick = job.time_tick
        self.units = job.query_snapshot["time_units_per_second"]
        self.end_mode = job.policy_snapshot.get("query_end_mode")
        if self.end_mode not in {"exclusive", "inclusive"}:
            raise PlanningError("QUERY_BOUNDARY_NOT_VERIFIED")
        self.raw, self.reference, self.project = raw, reference, project

    def request(self, start, end):
        params = deepcopy(self.base)
        params.update(start_time=str(start), end_time=str(end if self.end_mode == "exclusive" else end - self.tick))
        params.update(slice_max=0, highlight={"enable": False})
        return params

    @staticmethod
    def checked(result):
        if (
            not isinstance(result, dict)
            or result.get("result") is False
            or result.get("partial")
            or result.get("errors")
            or result.get("timed_out")
        ):
            raise PlanningError("INCOMPLETE_STATISTICS", retryable=True)
        return result

    def count(self, start, end, *, timeout):
        params = self.request(start, end)
        params.update(limit=1, **{"from": 0})
        return nonnegative_integer(self.checked(self.raw(params, timeout=timeout)).get("total"))

    def histogram(self, start, end, interval, *, timeout):
        params = self.request(start, end)
        # Preserve every reference and the union merge expression.
        milliseconds = interval * 1000 // self.units
        if milliseconds * self.units != interval * 1000 or milliseconds < 1:
            raise PlanningError("UNSUPPORTED_HISTOGRAM_PRECISION")
        window = f"{milliseconds}ms"
        for query in params["query_list"]:
            query["function"] = [{"method": "count"}, {"method": "date_histogram", "window": window}]
            query["time_aggregation"] = {}
        params.update(step=window, order_by=[])
        result = self.checked(self.reference(params, timeout=timeout))
        series = result.get("series")
        if not isinstance(series, list) or len(series) > 1:
            raise PlanningError("INVALID_STATISTICS")
        buckets = {}
        for timestamp, count in series[0]["values"] if series else []:
            # Existing ts/reference responses expose epoch milliseconds.
            if not isinstance(timestamp, int) or timestamp * self.units % 1000:
                raise PlanningError("INVALID_STATISTICS")
            key = timestamp * self.units // 1000
            if key in buckets:
                raise PlanningError("INVALID_STATISTICS")
            buckets[key] = nonnegative_integer(count)
        return buckets

    def sample(self, start, end, limit, *, timeout):
        params = self.request(start, end)
        params.update(limit=limit, **{"from": 0})
        response = self.checked(self.raw(params, timeout=timeout))
        if not isinstance(response.get("list"), list) or len(response["list"]) > limit:
            raise PlanningError("INVALID_STATISTICS")
        return [encode_export_row(row) for row in self.project(response)]
