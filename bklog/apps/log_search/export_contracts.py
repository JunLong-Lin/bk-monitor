"""Value objects and errors shared by export planning, state and query I/O."""

from dataclasses import dataclass, replace

from django.conf import settings


SPLITTABLE_PART_ERROR_CODES = frozenset({"OVERSIZED"})


class ExportStateError(Exception):
    """A rejected export operation."""


class InvalidTransitionError(ExportStateError):
    pass


class StaleExportUpdateError(ExportStateError):
    pass


class PlanValidationError(ExportStateError):
    pass


class PartLimitExceededError(PlanValidationError):
    pass


class RetryLimitExceededError(ExportStateError):
    pass


class PlanningError(ExportStateError):
    def __init__(self, code, *, retryable=False):
        super().__init__(code)
        self.code = code
        self.retryable = retryable


@dataclass(frozen=True)
class PartSpec:
    part_no: int | None
    start_time: int
    end_time: int
    estimated_rows: int | None = None
    estimated_bytes: int | None = None
    oversized: bool = False


@dataclass(frozen=True)
class PlannerPolicy:
    target_rows: int = 30_000
    target_bytes: int = 64 * 1024 * 1024
    max_rows: int = 10_000_000
    max_parts: int = 500
    sample_rows: int = 100
    sample_bytes: int = 1024 * 1024
    fallback_row_bytes: int = 1024
    max_calls: int = 2048
    max_buckets: int = 1000
    deadline_seconds: int = 120
    request_timeout: int = 15

    def __post_init__(self):
        if any(type(value) is not int or value < 1 for value in vars(self).values()):
            raise PlanningError("INVALID_PLANNER_POLICY")

    @classmethod
    def for_job(cls, job):
        try:
            configured = cls(**settings.ASYNC_EXPORT_PLANNER_POLICY)
            policy = cls(**{**vars(configured), **job.policy_snapshot.get("planner", {})})
        except (TypeError, ValueError) as exc:
            raise PlanningError("INVALID_PLANNER_POLICY") from exc
        return replace(
            policy,
            max_rows=min(policy.max_rows, configured.max_rows),
            max_parts=min(policy.max_parts, configured.max_parts, settings.ASYNC_EXPORT_MAX_LEAF_PARTS),
        )


def nonnegative_integer(value):
    if type(value) is not int or value < 0:
        raise PlanningError("INVALID_STATISTICS")
    return value
