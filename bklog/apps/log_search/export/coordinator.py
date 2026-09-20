"""公平投递、失败即关闭的 Redis 预算、可重放的消息发布与故障恢复。"""

import hashlib
import json
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from uuid import uuid4

from django.conf import settings
from django.db import transaction
from django.db.models import Count, Exists, F, OuterRef, Q
from django.utils import timezone

from apps.log_search.export.models import ExportDispatchGate, ExportJob, ExportPart, ExportPlan
from apps.log_search.export import state
from apps.log_search.export.contracts import SPLITTABLE_PART_ERROR_CODES


INFLIGHT = (ExportPart.Status.DISPATCHED, ExportPart.Status.RUNNING, ExportPart.Status.UPLOADING)


class BudgetUnavailable(Exception):
    """Redis 状态无法安全授权任何新工作。"""


class PublishNotSent(Exception):
    """发布方保证消息未提交；其他异常都视为结果不确定。"""


@dataclass(frozen=True)
class Limits:
    global_limit: int
    index_limit: int
    oversized_limit: int
    lease_seconds: int

    @classmethod
    def configured(cls):
        return cls(
            settings.ASYNC_EXPORT_GLOBAL_LIMIT,
            settings.ASYNC_EXPORT_INDEX_LIMIT,
            settings.ASYNC_EXPORT_OVERSIZED_LIMIT,
            settings.ASYNC_EXPORT_LEASE_SECONDS,
        )


def dimensions(job, oversized):
    # 不能用场景虚拟索引 0、别名，也不能只取联合查询的第一个资源。
    resources = job.resolved_resource_ids
    if not isinstance(resources, list) or not resources or any(not isinstance(r, str) or not r for r in resources):
        raise BudgetUnavailable("complete canonical resource IDs are required")
    values = ["global", f"job:{job.pk}"]
    values.extend(f"index:{hashlib.sha256(r.encode()).hexdigest()}" for r in sorted(set(resources)))
    if oversized:
        values.append("oversized")
    return values


def credential(part):
    return {
        "owner": part.lease_id,
        "generation": str(part.dispatch_generation),
        "expiry": part.lease_until.timestamp() if part.lease_until else 0,
        "dimensions": dimensions(part.plan.job, part.oversized),
    }


class RedisBudget:
    def __init__(self, client, namespace):
        self.client = client
        tag = hashlib.sha256(namespace.encode()).hexdigest()
        self.key = f"bklog:export:{{{tag}}}:ledger"
        self.script = Path(__file__).with_name("lua").joinpath("export_ledger.lua").read_text()

    def execute(self, *args):
        try:
            result = self.client.eval(self.script, 1, self.key, *args)
        except Exception as exc:
            raise BudgetUnavailable("Redis export budget is unavailable") from exc
        if result == -1:
            raise BudgetUnavailable("Redis export ledger must be reconciled")
        return result == 1

    def rebuild(self, parts):
        epoch = uuid4().hex
        entries = {str(part.pk): credential(part) for part in parts}
        self.execute("rebuild", epoch, json.dumps(entries))
        return epoch

    def current_epoch(self):
        try:
            epoch = self.client.hget(self.key, "_epoch")
        except Exception as exc:
            raise BudgetUnavailable("Redis export budget is unavailable") from exc
        return epoch.decode() if isinstance(epoch, bytes) else epoch

    def acquire(self, part, epoch, limits):
        entry = credential(part)
        capacities = {key: limits.index_limit for key in entry["dimensions"]}
        capacities.update(
            {
                "global": limits.global_limit,
                f"job:{part.plan.job_id}": part.plan.job.requested_parallelism,
                "oversized": limits.oversized_limit,
            }
        )
        return self.execute("acquire", str(part.pk), epoch, json.dumps(entry), json.dumps(capacities))

    def release(self, part_id, generation, owner):
        return self.execute("release", str(part_id), owner, str(generation))

    def renew(self, part_id, generation, owner, until):
        return self.execute(
            "renew", str(part_id), owner, str(generation), timezone.now().timestamp(), until.timestamp()
        )


class Coordinator:
    def __init__(self, budget, publish, *, namespace=None, limits=None):
        self.budget, self.publish = budget, publish
        self.namespace = namespace or settings.ASYNC_EXPORT_NAMESPACE
        self.limits = limits or Limits.configured()

    @contextmanager
    def gate(self, phase=None):
        namespace = (
            self.namespace if phase is None else f"scan:{hashlib.sha256(self.namespace.encode()).hexdigest()}:{phase}"
        )
        ExportDispatchGate.objects.get_or_create(namespace=namespace)
        # durable 事务块不接受外层事务，因此返回的投递记录在 deliver 发布前
        # 一定已经提交。
        with transaction.atomic(durable=True):
            yield ExportDispatchGate.objects.select_for_update().get(pk=namespace)

    def reconcile(self):
        """重建预算时保留过期租约与终态 Job 的在途 I/O。"""
        with self.gate():
            return self.budget.rebuild(ExportPart.objects.filter(status__in=INFLIGHT).select_related("plan__job"))

    def reserve(self):
        if self.limits.global_limit <= 0 or self.limits.lease_seconds <= 0:
            return None
        with self.gate() as gate:
            # 同一轮协调复用账本；Redis 丢失时在数据库锁内恢复。
            epoch = self.budget.current_epoch()
            if epoch is None:
                epoch = self.budget.rebuild(ExportPart.objects.filter(status__in=INFLIGHT).select_related("plan__job"))
            jobs = ExportJob.objects.filter(status__in=[ExportJob.Status.READY, ExportJob.Status.RUNNING])
            limit = settings.ASYNC_EXPORT_SCAN_LIMIT
            # aging：长时间没有获得投递的 Job 插队优先，再回到持久化 RR 轮转。
            for job_id in self._aged_job_ids(jobs, limit):
                dispatched = self._dispatch_from_job(job_id, epoch)
                if dispatched is not None:
                    return dispatched
            identifiers = self._round_robin_ids(jobs, gate.cursor, limit)
            for job_id in identifiers:
                gate.cursor = job_id
                gate.save(update_fields=["cursor"])
                dispatched = self._dispatch_from_job(job_id, epoch)
                if dispatched is not None:
                    return dispatched
        return None

    def _dispatch_from_job(self, job_id, epoch):
        job = ExportJob.objects.select_for_update().get(pk=job_id)
        if job.status not in {ExportJob.Status.READY, ExportJob.Status.RUNNING}:
            return None
        waiting = ExportPart.objects.filter(
            plan__job=job,
            plan__plan_version=job.current_plan_version,
            plan__status=ExportPlan.Status.READY,
            status=ExportPart.Status.WAITING,
            is_leaf=True,
        ).filter(Q(next_retry_at__isnull=True) | Q(next_retry_at__lte=timezone.now()))
        candidates = [waiting.filter(oversized=False).order_by("start_time", "part_no").first()]
        if self.limits.oversized_limit > 0:
            candidates.append(waiting.filter(oversized=True).order_by("start_time", "part_no").first())
        candidates = sorted((part for part in candidates if part is not None), key=lambda part: part.start_time)
        for part in candidates:
            part.plan.job = job
            part.lease_id, part.task_id = uuid4().hex, uuid4().hex
            part.dispatch_generation += 1
            part.lease_until = timezone.now() + timedelta(seconds=self.limits.lease_seconds)
            if not self.budget.acquire(part, epoch, self.limits):
                continue
            return state.dispatch_part(
                part.pk, lease_id=part.lease_id, task_id=part.task_id, lease_until=part.lease_until
            )
        return None

    def _aged_job_ids(self, jobs, limit):
        aging_seconds = settings.ASYNC_EXPORT_AGING_SECONDS
        if aging_seconds <= 0:
            return []
        now = timezone.now()
        cutoff = now - timedelta(seconds=aging_seconds)
        has_waiting = ExportPart.objects.filter(
            plan__job_id=OuterRef("pk"),
            plan__plan_version=OuterRef("current_plan_version"),
            plan__status=ExportPlan.Status.READY,
            status=ExportPart.Status.WAITING,
            is_leaf=True,
        ).filter(Q(next_retry_at__isnull=True) | Q(next_retry_at__lte=now))
        aged = (
            jobs.filter(Exists(has_waiting))
            .filter(Q(last_dispatched_at__isnull=True) | Q(last_dispatched_at__lt=cutoff))
            .order_by(F("last_dispatched_at").asc(nulls_first=True), "pk")
        )
        return list(aged.values_list("pk", flat=True)[:limit])

    def deliver(self, part):
        try:
            current = state.replay_dispatched_part(part.pk, generation=part.dispatch_generation, lease_id=part.lease_id)
        except state.ExportStateError:
            return "stale"
        try:
            self.publish(current)
        except PublishNotSent:
            # 在数据库锁内确认 Worker 尚未领取，再撤销投递。
            with self.gate():
                try:
                    state.release_dispatch(part.pk, generation=part.dispatch_generation, lease_id=part.lease_id)
                except state.ExportStateError:
                    return "claimed"
            # 数据库提交成功后才能释放额度；否则事务回滚会造成 Redis 少计。
            self.budget.release(part.pk, part.dispatch_generation, part.lease_id)
            return "not_sent"
        except Exception:
            # broker 超时/断连不能证明消息未投递。
            return "uncertain"
        try:
            state.mark_part_published(part.pk, generation=part.dispatch_generation)
        except state.ExportStateError:
            pass  # Worker 可能已经失败或重试了这一代。
        return "published"

    def tick(self, max_dispatches=100, *, reconcile=True):
        if reconcile:
            self.reconcile()
        sent = []
        for _ in range(max_dispatches):
            part = self.reserve()
            if part is None:
                break
            outcome = self.deliver(part)
            sent.append((part.pk, outcome))
            if outcome in {"uncertain", "not_sent"}:
                break  # 避免 broker 不可用时空转。
        return sent

    @staticmethod
    def _round_robin_ids(queryset, cursor, limit):
        if type(limit) is not int or limit < 1:
            raise ValueError("scan limit must be a positive integer")
        identifiers = list(queryset.filter(pk__gt=cursor).order_by("pk").values_list("pk", flat=True)[:limit])
        if len(identifiers) < limit:
            identifiers += list(
                queryset.filter(pk__lte=cursor).order_by("pk").values_list("pk", flat=True)[: limit - len(identifiers)]
            )
        return identifiers

    def _batch(self, queryset, phase, limit):
        # 每类有界扫描都有独立的持久化游标，首页长期不响应也不会永久
        # 遮住后面的恢复或收尾工作。
        with self.gate(phase) as cursor:
            identifiers = self._round_robin_ids(queryset, cursor.cursor, limit)
            if identifiers:
                cursor.cursor = identifiers[-1]
                cursor.save(update_fields=["cursor"])
            records = {record.pk: record for record in queryset.filter(pk__in=identifiers)}
        return [records[pk] for pk in identifiers if pk in records]

    def replay(self, limit=100, *, reconcile=True):
        if reconcile:
            self.reconcile()
        parts = self._batch(
            ExportPart.objects.filter(status=ExportPart.Status.DISPATCHED, lease_until__gt=timezone.now()),
            "replay",
            limit,
        )
        return [(part.pk, self.deliver(part)) for part in parts]

    def recover_expired(self, limit=100):
        """只回收未被领取的过期投递；运行中的 I/O 保留其预留。"""
        retained = []
        expired = ExportPart.objects.filter(status__in=INFLIGHT).filter(
            Q(lease_until__lte=timezone.now()) | Q(lease_until__isnull=True)
        )
        for candidate in self._batch(expired, "expired", limit):
            if candidate.status != ExportPart.Status.DISPATCHED:
                retained.append(candidate.pk)
                continue
            with self.gate():
                state.recover_expired_part(candidate)
        self.reconcile()
        return retained

    def planning_jobs(self, limit=100):
        now = timezone.now()
        due = (
            ExportJob.objects.filter(status__in=[ExportJob.Status.PENDING, ExportJob.Status.PLANNING])
            .filter(Q(planning_lease_until__isnull=True) | Q(planning_lease_until__lte=now))
            .filter(Q(next_planning_at__isnull=True) | Q(next_planning_at__lte=now))
        )
        return self._batch(due, "planning", limit)

    def failed_parts(self, limit=100):
        now = timezone.now()
        failed = ExportPart.objects.filter(
            status=ExportPart.Status.FAILED,
            is_leaf=True,
            plan__status=ExportPlan.Status.READY,
            plan__job__status=ExportJob.Status.RUNNING,
            plan__plan_version=F("plan__job__current_plan_version"),
        )
        due_split = (Q(planning_lease_until__isnull=True) | Q(planning_lease_until__lte=now)) & (
            Q(next_planning_at__isnull=True) | Q(next_planning_at__lte=now)
        )
        return self._batch(failed.filter(~Q(error_code__in=SPLITTABLE_PART_ERROR_CODES) | due_split), "failed", limit)

    def finalizing_jobs(self, limit=100):
        complete = (
            ExportPlan.objects.filter(
                job_id=OuterRef("pk"),
                plan_version=OuterRef("current_plan_version"),
                status=ExportPlan.Status.READY,
                part_count__gt=0,
            )
            .annotate(
                leaves=Count("parts", filter=Q(parts__is_leaf=True)),
                successes=Count("parts", filter=Q(parts__is_leaf=True, parts__status=ExportPart.Status.SUCCESS)),
            )
            .filter(leaves=F("part_count"), successes=F("part_count"))
        )
        jobs = (
            ExportJob.objects.filter(status=ExportJob.Status.RUNNING)
            .filter(Exists(complete))
            .filter(Q(next_finalization_at__isnull=True) | Q(next_finalization_at__lte=timezone.now()))
        )
        return [job.pk for job in self._batch(jobs, "finalizing", limit)]

    def cleanup_jobs(self, limit=100):
        jobs = ExportJob.objects.filter(
            status__in=[ExportJob.Status.SUCCESS, ExportJob.Status.FAILED, ExportJob.Status.CANCELED],
            artifacts_cleaned_at__isnull=True,
        )
        return [job.pk for job in self._batch(jobs, "artifact_cleanup", limit)]

    def control_work(self, limit=100, *, finalize=False):
        # 先结算已耗尽的失败，再为这些 Job 授权新工作。
        for part in self.failed_parts(limit):
            if part.error_code in SPLITTABLE_PART_ERROR_CODES:
                yield "split", part.pk
            else:
                state.fail_exhausted_part(part.pk)
        for job in self.planning_jobs(limit):
            yield "plan", job.pk
        if finalize:
            for job_id in self.finalizing_jobs(limit):
                yield "finalize", job_id


def renew_worker_lease(budget, part_id, *, generation, lease_id, lease_until, processed_rows=None):
    """Redis 失败即拒绝新的数据库心跳；绝不复活已失去所有权的 Worker。"""
    if not budget.renew(part_id, generation, lease_id, lease_until):
        raise BudgetUnavailable("worker budget ownership was lost")
    return state.heartbeat_part(
        part_id, generation=generation, lease_id=lease_id, lease_until=lease_until, processed_rows=processed_rows
    )
