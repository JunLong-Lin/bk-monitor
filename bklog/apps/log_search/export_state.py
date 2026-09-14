"""Export lifecycle writes, serialized in Job -> Part order.

Planners, coordinators and workers use these operations rather than updating
lifecycle columns themselves. Planning ownership and worker ownership are
separate: planning retries never consume a Part's execution attempts.
"""

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta

from django.conf import settings
from django.db import transaction
from django.db.models import Sum
from django.utils import timezone

from apps.log_search.export_contracts import (
    ExportStateError,
    InvalidTransitionError,
    PartSpec,
    PlanValidationError,
    PlanningError,
    PartLimitExceededError,
    RetryLimitExceededError,
    SPLITTABLE_PART_ERROR_CODES,
    StaleExportUpdateError,
)
from apps.log_search.export_models import ExportJob, ExportPart, ExportPlan


JOB_TRANSITIONS = {
    ExportJob.Status.PENDING: {ExportJob.Status.PLANNING, ExportJob.Status.CANCELED},
    ExportJob.Status.PLANNING: {ExportJob.Status.READY, ExportJob.Status.FAILED, ExportJob.Status.CANCELED},
    ExportJob.Status.READY: {ExportJob.Status.RUNNING, ExportJob.Status.FAILED, ExportJob.Status.CANCELED},
    ExportJob.Status.RUNNING: {ExportJob.Status.SUCCESS, ExportJob.Status.FAILED, ExportJob.Status.CANCELED},
}


def _now():
    return timezone.now()


def _save(record, **changes):
    for field, value in changes.items():
        setattr(record, field, value)
    record.save(update_fields=[*changes, "updated_at"])
    return record


def _ensure_job_transition(job, target):
    if target not in JOB_TRANSITIONS.get(job.status, set()):
        raise InvalidTransitionError(f"Job {job.pk}: {job.status} -> {target} is not allowed")


def _get_locked_part(part_id):
    job_id = ExportPart.objects.values_list("plan__job_id", flat=True).get(pk=part_id)
    job = ExportJob.objects.select_for_update().get(pk=job_id)
    part = ExportPart.objects.select_for_update().select_related("plan").get(pk=part_id)
    return part, part.plan, job


def _ensure_current_part(part, plan, job):
    if not part.is_leaf or plan.status != ExportPlan.Status.READY or job.current_plan_version != plan.plan_version:
        raise StaleExportUpdateError(f"Part {part.pk} is not a current active leaf")


def _ensure_worker(part, generation, lease_id, statuses, *, live=True):
    if (
        part.status not in statuses
        or part.dispatch_generation != generation
        or not lease_id
        or part.lease_id != lease_id
    ):
        raise StaleExportUpdateError(f"Part {part.pk}: stale worker credential")
    if live and (part.lease_until is None or part.lease_until <= _now()):
        raise StaleExportUpdateError(f"Part {part.pk}: lease expired")


def _ensure_running_job(job):
    if job.status != ExportJob.Status.RUNNING:
        raise StaleExportUpdateError(f"Job {job.pk}: no longer running")


def _partition(parts, *, start, end, tick):
    """Validate the same range/precision contract for initial and child leaves."""
    specs = sorted((p if isinstance(p, PartSpec) else PartSpec(**p) for p in parts), key=lambda p: p.start_time)
    if not specs or type(tick) is not int or tick < 1:
        raise PlanValidationError("a partition requires leaves and a positive time tick")
    cursor = start
    for part in specs:
        if any(type(t) is not int or t % tick for t in (part.start_time, part.end_time)):
            raise PlanValidationError("Part boundaries must align with the Job time tick")
        if part.start_time != cursor or not part.start_time < part.end_time <= end:
            raise PlanValidationError("Part ranges must cover the parent exactly without gaps or overlap")
        if any(
            value is not None and (type(value) is not int or value < 0)
            for value in (part.estimated_rows, part.estimated_bytes)
        ):
            raise PlanValidationError("Part estimates must be nonnegative integers")
        if part.oversized and part.end_time - part.start_time != tick:
            raise PlanValidationError("an oversized leaf must have the minimum time width")
        cursor = part.end_time
    if cursor != end:
        raise PlanValidationError("Part ranges must cover the parent exactly")
    return specs


def _leaf_limit(requested=None):
    limit = (
        settings.ASYNC_EXPORT_MAX_LEAF_PARTS
        if requested is None
        else min(requested, settings.ASYNC_EXPORT_MAX_LEAF_PARTS)
    )
    if type(limit) is not int or limit < 1:
        raise PlanValidationError("leaf limit must be positive")
    return limit


def _part_rows(plan, specs, parent=None):
    return [ExportPart(plan=plan, parent=parent, **vars(spec)) for spec in specs]


def begin_planning(job_id):
    with transaction.atomic():
        job = ExportJob.objects.select_for_update().get(pk=job_id)
        _ensure_job_transition(job, ExportJob.Status.PLANNING)
        return _save(job, status=ExportJob.Status.PLANNING, state_version=job.state_version + 1)


@dataclass(frozen=True)
class PlanningAttempt:
    """An immutable credential shared by initial planning and local splitting."""

    job: ExportJob
    record_id: int
    generation: int
    started_at: datetime
    deadline: datetime
    part: ExportPart | None = None

    def _locked_record(self):
        # Called only by state operations within transaction.atomic().
        if self.part is not None:
            record, plan, job = _get_locked_part(self.record_id)
            _ensure_current_part(record, plan, job)
            _ensure_running_job(job)
            expected = ExportPart.Status.FAILED
        else:
            record = job = ExportJob.objects.select_for_update().get(pk=self.record_id)
            expected = ExportJob.Status.PLANNING
        if record.status != expected or record.planning_generation != self.generation:
            raise StaleExportUpdateError("planning ownership changed")
        if record.planning_lease_until is None or record.planning_lease_until <= _now():
            raise StaleExportUpdateError("planning lease expired")
        return record, job

    def heartbeat(self) -> float:
        with transaction.atomic():
            record, _ = self._locked_record()
            now = _now()
            if now >= self.deadline:
                raise PlanningError("PLANNING_TIMEOUT")
            _save(
                record,
                planning_lease_until=min(
                    self.deadline, now + timedelta(seconds=settings.ASYNC_EXPORT_PLANNING_LEASE_SECONDS)
                ),
            )
            return (self.deadline - now).total_seconds()

    def fail(self, error: PlanningError) -> None:
        with transaction.atomic():
            try:
                record, job = self._locked_record()
            except StaleExportUpdateError:
                return
            if not error.retryable or record.planning_attempts >= settings.ASYNC_EXPORT_PLANNING_ATTEMPTS:
                _finish_job(job, ExportJob.Status.FAILED, error_code=error.code)
            else:
                changes = dict(
                    planning_lease_until=None,
                    next_planning_at=_now() + timedelta(seconds=settings.ASYNC_EXPORT_PLANNING_RETRY_SECONDS),
                )
                # Preserve a failed Part's execution classification for the scanner.
                if self.part is None:
                    changes["error_code"] = error.code
                _save(record, **changes)


def claim_planning(*, job_id: int | None = None, part_id: int | None = None) -> PlanningAttempt | None:
    if (job_id is None) == (part_id is None):
        raise ExportStateError("specify exactly one planning target")
    with transaction.atomic():
        if part_id is not None:
            record, plan, job = _get_locked_part(part_id)
            if record.status != ExportPart.Status.FAILED or job.status != ExportJob.Status.RUNNING:
                return None
            _ensure_current_part(record, plan, job)
        else:
            record = job = ExportJob.objects.select_for_update().get(pk=job_id)
            if job.status not in {ExportJob.Status.PENDING, ExportJob.Status.PLANNING}:
                return None
        now = _now()  # Take time after waiting for the lock, not before.
        if record.planning_lease_until and record.planning_lease_until > now:
            return None
        if record.next_planning_at and record.next_planning_at > now:
            return None
        if job.status == ExportJob.Status.PENDING:
            _save(job, status=ExportJob.Status.PLANNING, state_version=job.state_version + 1)
        started = record.planning_started_at or now
        deadline = started + timedelta(seconds=settings.ASYNC_EXPORT_PLANNING_DEADLINE)
        if now >= deadline or record.planning_attempts >= settings.ASYNC_EXPORT_PLANNING_ATTEMPTS:
            code = (
                "PLANNING_TIMEOUT"
                if now >= deadline
                else ("SPLIT_RETRIES_EXHAUSTED" if part_id else "PLANNING_RETRIES_EXHAUSTED")
            )
            _finish_job(job, ExportJob.Status.FAILED, error_code=code)
            return None
        _save(
            record,
            planning_started_at=started,
            planning_attempts=record.planning_attempts + 1,
            planning_generation=record.planning_generation + 1,
            next_planning_at=None,
            planning_lease_until=min(deadline, now + timedelta(seconds=settings.ASYNC_EXPORT_PLANNING_LEASE_SECONDS)),
        )
        return PlanningAttempt(
            job, record.pk, record.planning_generation, started, deadline, record if part_id else None
        )


def persist_plan(
    job_id,
    *,
    attempt: PlanningAttempt,
    query_hash: str,
    target_rows,
    target_bytes,
    histogram_interval,
    parts: Iterable[PartSpec | Mapping],
    planning_input=None,
    statistics_at=None,
    max_leaf_parts=None,
    estimated_total=None,
):
    plan_version = attempt.generation
    if any(
        type(value) is not int or value < 1 for value in (plan_version, target_rows, target_bytes, histogram_interval)
    ):
        raise PlanValidationError("plan version, targets and interval must be positive integers")
    if estimated_total is not None and (type(estimated_total) is not int or estimated_total < 0):
        raise PlanValidationError("estimated_total must be a nonnegative integer")
    with transaction.atomic():
        job = ExportJob.objects.select_for_update().get(pk=job_id)
        if attempt.part is not None or attempt.job.pk != job_id:
            raise StaleExportUpdateError("wrong planning target")
        attempt._locked_record()
        _ensure_job_transition(job, ExportJob.Status.READY)
        if job.current_plan_version is not None and plan_version <= job.current_plan_version:
            raise PlanValidationError("plan versions must increase")
        if not query_hash or query_hash != job.query_hash:
            raise PlanValidationError("plan query_hash must match the Job snapshot")
        specs = _partition(parts, start=job.start_time, end=job.end_time, tick=job.time_tick)
        if len(specs) > _leaf_limit(max_leaf_parts):
            raise PartLimitExceededError("plan exceeds the leaf limit")
        if any(type(p.part_no) is not int for p in specs) or {p.part_no for p in specs} != set(
            range(1, len(specs) + 1)
        ):
            raise PlanValidationError("initial Part numbers must be consecutive positive integers")
        plan = ExportPlan.objects.create(
            job=job,
            plan_version=plan_version,
            status=ExportPlan.Status.READY,
            planning_input=dict(planning_input or {}),
            query_hash=query_hash,
            statistics_at=statistics_at,
            target_rows=target_rows,
            target_bytes=target_bytes,
            histogram_interval=histogram_interval,
            part_count=len(specs),
        )
        ExportPart.objects.bulk_create(_part_rows(plan, specs))
        ExportPlan.objects.filter(job=job, status=ExportPlan.Status.READY).exclude(pk=plan.pk).update(
            status=ExportPlan.Status.SUPERSEDED, updated_at=_now()
        )
        _save(
            job,
            current_plan_version=plan_version,
            status=ExportJob.Status.READY,
            stage="",
            estimated_total=estimated_total,
            planning_lease_until=None,
            next_planning_at=None,
            error_code="",
            error_detail="",
            state_version=job.state_version + 1,
        )
        return plan


def dispatch_part(part_id, *, lease_id, task_id, lease_until):
    if not lease_id or not task_id:
        raise ExportStateError("lease_id and task_id are required before dispatch")
    with transaction.atomic():
        part, plan, job = _get_locked_part(part_id)
        _ensure_current_part(part, plan, job)
        now = _now()
        if part.status != ExportPart.Status.WAITING or job.status not in {
            ExportJob.Status.READY,
            ExportJob.Status.RUNNING,
        }:
            raise InvalidTransitionError("Part is not dispatchable")
        if part.next_retry_at and part.next_retry_at > now:
            raise InvalidTransitionError("retry delay has not elapsed")
        if lease_until <= now:
            raise ExportStateError("dispatch lease must expire in the future")
        if part.attempts >= settings.ASYNC_EXPORT_MAX_ATTEMPTS:
            raise RetryLimitExceededError("Part execution-attempt budget exhausted")
        _save(
            part,
            status=ExportPart.Status.DISPATCHED,
            dispatch_generation=part.dispatch_generation + 1,
            task_id=task_id,
            lease_id=lease_id,
            lease_until=lease_until,
            published_at=None,
            started_at=None,
            finished_at=None,
            heartbeat_at=None,
            stage="",
            error_code="",
            error_detail="",
        )
        if job.status == ExportJob.Status.READY:
            _save(job, status=ExportJob.Status.RUNNING, started_at=now, state_version=job.state_version + 1)
        return part


def mark_part_published(part_id, *, generation, published_at=None):
    with transaction.atomic():
        part, _, _ = _get_locked_part(part_id)
        if (
            part.status
            not in {
                ExportPart.Status.DISPATCHED,
                ExportPart.Status.RUNNING,
                ExportPart.Status.UPLOADING,
                ExportPart.Status.SUCCESS,
            }
            or part.dispatch_generation != generation
        ):
            raise StaleExportUpdateError("stale publish acknowledgement")
        return part if part.published_at else _save(part, published_at=published_at or _now())


def release_dispatch(part_id, *, generation, lease_id, error_code="DISPATCH_FAILED", error_detail=""):
    """Only use when publication certainly failed or an unclaimed lease expired."""
    with transaction.atomic():
        part, _, _ = _get_locked_part(part_id)
        _ensure_worker(part, generation, lease_id, {ExportPart.Status.DISPATCHED}, live=False)
        return _save(
            part,
            status=ExportPart.Status.WAITING,
            stage="",
            lease_id="",
            lease_until=None,
            published_at=None,
            error_code=error_code,
            error_detail=error_detail,
            next_retry_at=None,
        )


def replay_dispatched_part(part_id, *, generation, lease_id):
    with transaction.atomic():
        part, plan, job = _get_locked_part(part_id)
        _ensure_current_part(part, plan, job)
        _ensure_running_job(job)
        _ensure_worker(part, generation, lease_id, {ExportPart.Status.DISPATCHED})
        return part


def claim_part(part_id, *, generation, lease_id, worker_id):
    if not worker_id:
        raise ExportStateError("worker_id is required")
    with transaction.atomic():
        part, plan, job = _get_locked_part(part_id)
        _ensure_current_part(part, plan, job)
        _ensure_running_job(job)
        _ensure_worker(part, generation, lease_id, {ExportPart.Status.DISPATCHED})
        if part.attempts >= settings.ASYNC_EXPORT_MAX_ATTEMPTS:
            raise RetryLimitExceededError("Part execution-attempt budget exhausted")
        now = _now()
        return _save(
            part,
            status=ExportPart.Status.RUNNING,
            stage=ExportJob.Stage.DOWNLOAD_LOG,
            worker_id=worker_id,
            attempts=part.attempts + 1,
            started_at=now,
            heartbeat_at=now,
            processed_rows=0,
        )


def heartbeat_part(part_id, *, generation, lease_id, lease_until, processed_rows=None):
    """Terminal Jobs keep ownership until their already-started I/O exits."""
    if processed_rows is not None and (type(processed_rows) is not int or processed_rows < 0):
        raise ExportStateError("processed_rows must be a nonnegative integer")
    with transaction.atomic():
        part, plan, job = _get_locked_part(part_id)
        _ensure_current_part(part, plan, job)
        _ensure_worker(part, generation, lease_id, {ExportPart.Status.RUNNING, ExportPart.Status.UPLOADING})
        if lease_until <= _now() or lease_until < part.lease_until:
            raise ExportStateError("heartbeat must not shorten or expire the lease")
        changes = dict(lease_until=lease_until, heartbeat_at=_now())
        if processed_rows is not None:
            changes["processed_rows"] = processed_rows
        return _save(part, **changes)


def begin_part_package(part_id, *, generation, lease_id):
    with transaction.atomic():
        part, plan, job = _get_locked_part(part_id)
        _ensure_current_part(part, plan, job)
        _ensure_running_job(job)
        _ensure_worker(part, generation, lease_id, {ExportPart.Status.RUNNING})
        return _save(part, stage=ExportJob.Stage.PACKAGE, heartbeat_at=_now())


def begin_part_upload(part_id, *, generation, lease_id):
    with transaction.atomic():
        part, plan, job = _get_locked_part(part_id)
        _ensure_current_part(part, plan, job)
        _ensure_running_job(job)
        _ensure_worker(part, generation, lease_id, {ExportPart.Status.RUNNING})
        return _save(part, status=ExportPart.Status.UPLOADING, stage=ExportJob.Stage.UPLOAD, heartbeat_at=_now())


def note_unconfirmed_part(part_id, *, generation, lease_id, error_code):
    """Record uncertain remote I/O without releasing ownership or capacity."""
    with transaction.atomic():
        part, plan, job = _get_locked_part(part_id)
        _ensure_current_part(part, plan, job)
        _ensure_worker(part, generation, lease_id, {ExportPart.Status.RUNNING, ExportPart.Status.UPLOADING}, live=False)
        return _save(part, error_code=error_code, error_detail="Remote I/O exit requires verification")


def retry_part(part_id, *, generation, lease_id, error_code, error_detail, next_retry_at=None):
    """Record safely ended I/O. TTL expiration alone is not evidence of exit."""
    with transaction.atomic():
        part, plan, job = _get_locked_part(part_id)
        _ensure_current_part(part, plan, job)
        _ensure_worker(part, generation, lease_id, {ExportPart.Status.RUNNING, ExportPart.Status.UPLOADING}, live=False)
        if job.status in {ExportJob.Status.CANCELED, ExportJob.Status.FAILED}:
            target = ExportPart.Status.CANCELED
        else:
            _ensure_running_job(job)
            # Crossing the hard byte budget is deterministic for this fixed
            # range. Re-running the same export cannot make it safer; hand it
            # to the local split planner immediately.
            target = (
                ExportPart.Status.FAILED
                if error_code in SPLITTABLE_PART_ERROR_CODES or part.attempts >= settings.ASYNC_EXPORT_MAX_ATTEMPTS
                else ExportPart.Status.WAITING
            )
        return _save(
            part,
            status=target,
            stage="",
            lease_id="",
            lease_until=None,
            heartbeat_at=None,
            next_retry_at=next_retry_at if target == ExportPart.Status.WAITING else None,
            error_code=error_code,
            error_detail=error_detail,
            finished_at=_now(),
        )


def complete_part(part_id, *, generation, lease_id, actual_rows, actual_bytes, compressed_bytes, object_key, checksum):
    if (
        any(type(value) is not int or value < 0 for value in (actual_rows, actual_bytes, compressed_bytes))
        or not object_key
        or not checksum
    ):
        raise ExportStateError("successful artifacts require nonnegative sizes, an object key and checksum")
    with transaction.atomic():
        part, plan, job = _get_locked_part(part_id)
        _ensure_current_part(part, plan, job)
        _ensure_running_job(job)
        _ensure_worker(part, generation, lease_id, {ExportPart.Status.UPLOADING})
        _save(
            part,
            status=ExportPart.Status.SUCCESS,
            stage="",
            actual_rows=actual_rows,
            actual_bytes=actual_bytes,
            compressed_bytes=compressed_bytes,
            processed_rows=actual_rows,
            object_key=object_key,
            checksum=checksum,
            lease_id="",
            lease_until=None,
            heartbeat_at=None,
            finished_at=_now(),
            error_code="",
            error_detail="",
        )
        total = (
            plan.parts.filter(is_leaf=True, status=ExportPart.Status.SUCCESS).aggregate(value=Sum("actual_rows"))[
                "value"
            ]
            or 0
        )
        _save(job, actual_total=total)
        return part


def split_part(
    part_id: int, *, attempt: PlanningAttempt, children: Iterable[PartSpec | Mapping], max_leaf_parts=None
) -> list[ExportPart]:
    with transaction.atomic():
        part, plan, job = _get_locked_part(part_id)
        _ensure_current_part(part, plan, job)
        _ensure_running_job(job)
        if part.status != ExportPart.Status.FAILED:
            raise InvalidTransitionError("only a failed leaf may be split")
        if attempt.part is None or attempt.record_id != part_id:
            raise StaleExportUpdateError("wrong planning target")
        attempt._locked_record()
        specs = _partition(children, start=part.start_time, end=part.end_time, tick=job.time_tick)
        if len(specs) < 2:
            raise PlanValidationError("splitting requires at least two children")
        count = plan.parts.filter(is_leaf=True).count() - 1 + len(specs)
        if count > _leaf_limit(max_leaf_parts):
            raise PartLimitExceededError("split exceeds the active leaf limit")
        existing = set(plan.parts.values_list("part_no", flat=True))
        explicit = [p.part_no for p in specs if p.part_no is not None]
        if (
            any(type(n) is not int or n < 1 for n in explicit)
            or len(set(explicit)) != len(explicit)
            or existing.intersection(explicit)
        ):
            raise PlanValidationError("split child Part numbers must be unique positive integers")
        next_number = max(existing | set(explicit), default=0)
        rows = _part_rows(plan, specs, parent=part)
        for row in rows:
            if row.part_no is None:
                next_number += 1
                row.part_no = next_number
        _save(
            part,
            status=ExportPart.Status.SPLIT,
            is_leaf=False,
            stage="",
            lease_id="",
            lease_until=None,
            heartbeat_at=None,
            finished_at=_now(),
            planning_lease_until=None,
            next_planning_at=None,
        )
        created = ExportPart.objects.bulk_create(rows)
        _save(plan, part_count=count)
        _save(job, state_version=job.state_version + 1)
        return list(created)


def finalize_job_success(job_id, *, plan_version, manifest_object_key, manifest_checksum, expected_state_version=None):
    if not manifest_object_key or not manifest_checksum:
        raise ExportStateError("manifest object and checksum are required")
    with transaction.atomic():
        job = ExportJob.objects.select_for_update().get(pk=job_id)
        _ensure_job_transition(job, ExportJob.Status.SUCCESS)
        if expected_state_version is not None and job.state_version != expected_state_version:
            raise StaleExportUpdateError("Job changed during finalization")
        if job.current_plan_version != plan_version:
            raise StaleExportUpdateError("plan version is no longer current")
        plan = ExportPlan.objects.get(job=job, plan_version=plan_version, status=ExportPlan.Status.READY)
        leaves = plan.parts.filter(is_leaf=True)
        if not plan.part_count or leaves.count() != plan.part_count:
            raise PlanValidationError("active leaf count does not match the persisted plan")
        if leaves.exclude(status=ExportPart.Status.SUCCESS).exists():
            raise InvalidTransitionError("manifest cannot be finalized with unfinished Parts")
        now = _now()
        return _save(
            job,
            status=ExportJob.Status.SUCCESS,
            stage="",
            actual_total=leaves.aggregate(value=Sum("actual_rows"))["value"] or 0,
            manifest_object_key=manifest_object_key,
            manifest_checksum=manifest_checksum,
            error_code="",
            error_detail="",
            next_finalization_at=None,
            completed_at=now,
            expires_at=now + timedelta(hours=24),
            state_version=job.state_version + 1,
        )


def _finish_job(job, status, *, error_code="", error_detail=""):
    _ensure_job_transition(job, status)
    now = _now()
    _save(
        job,
        status=status,
        stage="",
        error_code=error_code,
        error_detail=error_detail,
        completed_at=now,
        planning_lease_until=None,
        next_planning_at=None,
        state_version=job.state_version + 1,
    )
    ExportPart.objects.filter(
        plan__job=job, status__in=[ExportPart.Status.WAITING, ExportPart.Status.DISPATCHED]
    ).update(
        status=ExportPart.Status.CANCELED,
        stage="",
        lease_id="",
        lease_until=None,
        heartbeat_at=None,
        finished_at=now,
        updated_at=now,
    )
    return job


def fail_job(job_id, *, error_code, error_detail=""):
    if not error_code:
        raise ExportStateError("error_code is required")
    with transaction.atomic():
        job = ExportJob.objects.select_for_update().get(pk=job_id)
        return _finish_job(job, ExportJob.Status.FAILED, error_code=error_code, error_detail=error_detail)


def cancel_job(job_id):
    with transaction.atomic():
        job = ExportJob.objects.select_for_update().get(pk=job_id)
        return job if job.status == ExportJob.Status.CANCELED else _finish_job(job, ExportJob.Status.CANCELED)


def recover_expired_part(candidate):
    """The caller has established safe exit; recheck its observed credential."""
    with transaction.atomic():
        part, _, _ = _get_locked_part(candidate.pk)
        fields = ("status", "dispatch_generation", "lease_id", "lease_until")
        if any(getattr(part, field) != getattr(candidate, field) for field in fields):
            return False
        if part.status == ExportPart.Status.DISPATCHED:
            release_dispatch(
                part.pk,
                generation=part.dispatch_generation,
                lease_id=part.lease_id,
                error_code="DISPATCH_LEASE_EXPIRED",
            )
        elif part.status in {ExportPart.Status.RUNNING, ExportPart.Status.UPLOADING}:
            retry_part(
                part.pk,
                generation=part.dispatch_generation,
                lease_id=part.lease_id,
                error_code="WORKER_STOPPED",
                error_detail="",
            )
        else:
            return False
        return True


def fail_exhausted_part(part_id):
    """A recovery scan is only a hint; validate the failed leaf under the Job lock."""
    with transaction.atomic():
        part, plan, job = _get_locked_part(part_id)
        if (
            part.status != ExportPart.Status.FAILED
            or part.error_code == "OVERSIZED"
            or job.status != ExportJob.Status.RUNNING
            or job.current_plan_version != plan.plan_version
            or not part.is_leaf
        ):
            return False
        _finish_job(job, ExportJob.Status.FAILED, error_code="PART_RETRIES_EXHAUSTED")
        return True


def set_parallelism(job_id, parallelism):
    """The API verifies creator authorization before calling this operation."""
    if type(parallelism) is not int or not 1 <= parallelism <= 8:
        raise ExportStateError("parallelism must be between 1 and 8")
    with transaction.atomic():
        job = ExportJob.objects.select_for_update().get(pk=job_id)
        if job.status in {ExportJob.Status.SUCCESS, ExportJob.Status.FAILED, ExportJob.Status.CANCELED}:
            raise InvalidTransitionError("cannot change terminal Job parallelism")
        if job.requested_parallelism == parallelism:
            return job
        return _save(job, requested_parallelism=parallelism, state_version=job.state_version + 1)
