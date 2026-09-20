"""基于冻结路由与已授权绑定通道的 UnifyQuery 统计能力。"""

from contextlib import AbstractContextManager
from copy import deepcopy
from typing import Protocol

import ujson
from django.conf import settings

from apps.log_search.export.contracts import PlanningError, nonnegative_integer


class Statistics(Protocol):
    def count(self, start: int, end: int, *, timeout: float) -> int: ...
    def histogram(self, start: int, end: int, interval: int, *, timeout: float) -> dict[int, int]: ...
    def sample(self, start: int, end: int, limit: int, *, timeout: float) -> list[bytes]: ...


class StatisticsFactory(Protocol):
    """把授权与资源绑定到整次规划尝试的生命周期。"""

    def __call__(self, job) -> AbstractContextManager[Statistics]: ...


def encode_export_row(row):
    """采样与 Worker 共用的 JSONL 编码（已投影/脱敏）。"""
    return (ujson.dumps(row, ensure_ascii=False) + "\n").encode("utf-8")


class UnifyQueryStatistics:
    """使用冻结的查询路由与已授权的 UnifyQuery 原语。

    raw/reference 必须接受 (params, timeout=...) 并保持租户/用户上下文与场景
    权限校验；project 与 Part Reader 使用同一套批次转换逻辑。上下文的恢复与
    清理由工厂负责。这里不假设任何网络能力，每种模式都要经过部署验证。
    """

    def __init__(self, job, *, raw, reference, project):
        if job.query_kind not in settings.ASYNC_EXPORT_VERIFIED_QUERY_KINDS:
            raise PlanningError("QUERY_PROTOCOL_NOT_VERIFIED")
        self.base = deepcopy(job.query_snapshot["unify_query"])
        if not self.base.get("query_list"):
            raise PlanningError("INCOMPLETE_ROUTING_SNAPSHOT")
        self.tick = job.time_tick
        self.units = job.query_snapshot["time_units_per_second"]
        self.end_mode = settings.ASYNC_EXPORT_QUERY_END_MODES.get(job.query_kind)
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
        # 保留每个 reference 以及联合查询的 merge 表达式。
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
            # 现有 ts/reference 响应给出的时间戳是 epoch 毫秒。
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
