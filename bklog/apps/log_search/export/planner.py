"""有界自适应分片；状态写入与查询 I/O 都放在边界上。"""

import time
from dataclasses import replace

from django.utils import timezone

from apps.log_search.export import state
from apps.log_search.export.contracts import (
    InvalidTransitionError,
    PlannerPolicy,
    PlanningError,
    PartSpec,
    PartLimitExceededError,
    PlanValidationError,
    StaleExportUpdateError,
    nonnegative_integer,
)
from apps.log_search.export.models import ExportPart
from apps.log_search.export.query import Statistics, StatisticsFactory
from apps.utils.log import logger


class AdaptivePlanner:
    def __init__(
        self, job, statistics: Statistics, policy: PlannerPolicy, *, heartbeat=lambda: None, clock=None, started_at=None
    ):
        self.job, self.statistics, self.policy = job, statistics, policy
        self.heartbeat, self.clock = heartbeat, clock or time.monotonic
        self.deadline = (self.clock() if started_at is None else started_at) + policy.deadline_seconds
        self.calls = 0
        self.tick = job.time_tick
        units = job.query_snapshot["time_units_per_second"]
        if type(self.tick) is not int or self.tick < 1 or type(units) is not int or units < 1:
            raise PlanningError("INVALID_TIME_PRECISION")
        self.interval = ((30 * units + self.tick - 1) // self.tick) * self.tick
        self.average_bytes = policy.fallback_row_bytes

    def call(self, method, *args):
        ownership_remaining = self.heartbeat()
        remaining = self.deadline - self.clock()
        if ownership_remaining is not None:
            remaining = min(remaining, ownership_remaining)
        if remaining <= 0 or self.calls >= self.policy.max_calls:
            raise PlanningError("PLANNING_BUDGET_EXCEEDED")
        self.calls += 1
        result = method(*args, timeout=min(remaining, self.policy.request_timeout))
        if self.clock() >= self.deadline:
            raise PlanningError("PLANNING_BUDGET_EXCEEDED")
        self.heartbeat()
        return result

    def should_split(self, rows):
        """达到递归拆分触发值时细分时间范围。"""
        return rows >= self.policy.split_rows or rows * self.average_bytes >= self.policy.split_bytes

    def can_merge(self, rows):
        """相邻小范围合计不超过合并上限时合并，减少过小 Part。"""
        return rows <= self.policy.merge_rows and rows * self.average_bytes <= self.policy.merge_bytes

    def refine(self, start, end, rows):
        pending = [(start, end, rows)]
        while pending:
            start, end, rows = pending.pop()
            oversized = self.should_split(rows)
            if not oversized or end - start == self.tick:
                yield PartSpec(None, start, end, rows, rows * self.average_bytes, oversized)
                continue
            midpoint = start + ((end - start) // self.tick // 2) * self.tick
            left = nonnegative_integer(self.call(self.statistics.count, start, midpoint))
            right = nonnegative_integer(self.call(self.statistics.count, midpoint, end))
            pending.extend([(midpoint, end, right), (start, midpoint, left)])

    def range_parts(self, start, end):
        cursor = start
        while cursor < end:
            first_bucket = cursor // self.interval * self.interval
            stop = min(end, first_bucket + self.interval * self.policy.max_buckets)
            buckets = self.call(self.statistics.histogram, cursor, stop, self.interval)
            if not isinstance(buckets, dict) or len(buckets) > self.policy.max_buckets:
                raise PlanningError("INVALID_STATISTICS")
            for key, count in buckets.items():
                if type(key) is not int or key % self.interval or not first_bucket <= key < stop:
                    raise PlanningError("INVALID_STATISTICS")
                nonnegative_integer(count)
            for bucket in range(first_bucket, stop, self.interval):
                yield from self.refine(max(cursor, bucket), min(stop, bucket + self.interval), buckets.get(bucket, 0))
            cursor = stop

    def build(self, *, start=None, end=None, force_split=False):
        start = self.job.start_time if start is None else start
        end = self.job.end_time if end is None else end
        if (
            any(type(value) is not int or value % self.tick for value in (start, end))
            or not self.job.start_time <= start < end <= self.job.end_time
        ):
            raise PlanningError("INVALID_TIME_PRECISION")
        ranges = [(start, end)]
        if force_split:
            if end - start <= self.tick:
                raise PlanningError("OVERSIZED_UNSPLITTABLE")
            middle = start + ((end - start) // self.tick // 2) * self.tick
            ranges = [(start, middle), (middle, end)]
        total = nonnegative_integer(self.call(self.statistics.count, start, end))
        if not force_split and total > self.policy.max_rows:
            raise PlanningError("QUOTA_EXCEEDED")
        sample = self.call(self.statistics.sample, start, end, self.policy.sample_rows) if total else []
        if not isinstance(sample, list) or any(not isinstance(row, bytes) for row in sample):
            raise PlanningError("INVALID_STATISTICS")
        size = sum(map(len, sample))
        if len(sample) > self.policy.sample_rows or size > self.policy.sample_bytes:
            raise PlanningError("SAMPLE_BUDGET_EXCEEDED")
        if sample:
            self.average_bytes = max(1, (size + len(sample) - 1) // len(sample))

        parts = []
        for lower, upper in ranges:
            # 局部分裂只保留一个必需的中点边界，两侧仍然走普通的
            # 自适应细分与合并算法。
            pieces = self.range_parts(lower, upper) if total else [PartSpec(None, lower, upper, 0, 0)]
            for part in pieces:
                previous = parts[-1] if parts and parts[-1].end_time > lower else None
                if (
                    previous
                    and not previous.oversized
                    and not part.oversized
                    and self.can_merge(previous.estimated_rows + part.estimated_rows)
                ):
                    parts[-1] = replace(
                        previous,
                        end_time=part.end_time,
                        estimated_rows=previous.estimated_rows + part.estimated_rows,
                        estimated_bytes=previous.estimated_bytes + part.estimated_bytes,
                    )
                else:
                    parts.append(part)
                    if len(parts) > self.policy.max_parts:
                        raise PlanningError("PART_LIMIT_EXCEEDED")
        estimate = max(total, sum(part.estimated_rows for part in parts))
        if not force_split and estimate > self.policy.max_rows:
            raise PlanningError("QUOTA_EXCEEDED")
        if self.clock() >= self.deadline:
            raise PlanningError("PLANNING_BUDGET_EXCEEDED")
        self.heartbeat()
        return [replace(part, part_no=i + 1) for i, part in enumerate(parts)], estimate


def _classify_planning_error(job, exc):
    if isinstance(exc, PlanningError):
        return exc
    if isinstance(exc, PartLimitExceededError):
        return PlanningError("PART_LIMIT_EXCEEDED")
    if isinstance(exc, PlanValidationError):
        return PlanningError("INVALID_PLAN")
    # 不落库、不打印异常里的查询内容或凭据。
    logger.warning("export planning job=%s exception_type=%s", job.pk, type(exc).__name__)
    return PlanningError("STATISTICS_FAILED", retryable=True)


def _run_planning(attempt: state.PlanningAttempt | None, statistics_factory: StatisticsFactory):
    if attempt is None:
        return None
    try:
        policy = PlannerPolicy.configured()
        # 工厂负责租户/用户上下文，成功、失败、取消以及规划回调已过期时
        # 都必须恢复现场。
        attempt.heartbeat()
        started_at = time.monotonic()
        with statistics_factory(attempt.job) as statistics:
            planner = AdaptivePlanner(
                attempt.job, statistics, policy, heartbeat=attempt.heartbeat, started_at=started_at
            )
            parts, estimate = planner.build()
        return state.persist_plan(
            attempt.job.pk,
            query_hash=attempt.job.query_hash,
            target_rows=policy.target_rows,
            target_bytes=policy.target_bytes,
            histogram_interval=planner.interval,
            parts=parts,
            statistics_at=timezone.now(),
            planning_input={"policy": vars(policy), "calls": planner.calls},
            max_leaf_parts=policy.max_parts,
            estimated_total=estimate,
            attempt=attempt,
        )
    except StaleExportUpdateError:
        return None
    except Exception as exc:
        attempt.fail(_classify_planning_error(attempt.job, exc))
        return None


def _run_split(part: ExportPart | None, statistics_factory: StatisticsFactory):
    if part is None:
        return None
    try:
        policy = PlannerPolicy.configured()
        started_at = time.monotonic()
        with statistics_factory(part.plan.job) as statistics:
            planner = AdaptivePlanner(part.plan.job, statistics, policy, started_at=started_at)
            parts, _ = planner.build(start=part.start_time, end=part.end_time, force_split=True)
        return state.split_part(
            part.pk, children=[replace(p, part_no=None) for p in parts], max_leaf_parts=policy.max_parts
        )
    except (StaleExportUpdateError, InvalidTransitionError):
        # 另一个拆分已提交或任务已不再活跃，幂等忽略。
        return None
    except Exception as exc:
        error = _classify_planning_error(part.plan.job, exc)
        state.fail_split(part.pk, error_code=error.code, error_detail=type(exc).__name__)
        return None


def plan_job(job_id: int, statistics_factory: StatisticsFactory):
    return _run_planning(state.claim_planning(job_id=job_id), statistics_factory)


def replan_failed_part(part_id: int, statistics_factory: StatisticsFactory):
    return _run_split(state.begin_split(part_id), statistics_factory)
