from datetime import timedelta
from unittest.mock import patch
from uuid import uuid4

from django.test import TestCase, override_settings
from django.utils import timezone

from apps.log_search.export.models import ExportJob, ExportPart
from apps.log_search.export.state import (
    ExportStateError,
    InvalidTransitionError,
    PartSpec,
    PlanValidationError,
    StaleExportUpdateError,
    begin_planning,
    begin_part_upload,
    cancel_job,
    claim_part,
    claim_planning,
    complete_part,
    dispatch_part,
    fail_job,
    finalize_job_success,
    heartbeat_part,
    persist_plan,
    release_dispatch,
    retry_part,
    split_part,
)


class ExportStateTest(TestCase):
    def create_job(self, *, end_time=20):
        return ExportJob.objects.create(
            space_uid="space-a",
            created_by="alice",
            source_app_code="bk_log_search",
            query_kind=ExportJob.QueryKind.SINGLE,
            index_set_ids=[1],
            query_snapshot={"time_field": "dtEventTimeStamp"},
            query_hash="a" * 64,
            start_time=0,
            end_time=end_time,
            time_tick=1,
        )

    def activate_plan(self, job, *, parts=None):
        begin_planning(job.pk)
        return persist_plan(
            job.pk,
            attempt=claim_planning(job_id=job.pk),
            query_hash=job.query_hash,
            target_rows=30_000,
            target_bytes=64 * 1024 * 1024,
            histogram_interval=30_000,
            parts=parts or [PartSpec(1, job.start_time, job.end_time, estimated_rows=10)],
        )

    def dispatch_and_claim(self, part):
        lease_id = uuid4().hex
        dispatch_part(
            part.pk,
            lease_id=lease_id,
            task_id=f"task-{part.pk}",
            lease_until=timezone.now() + timedelta(minutes=5),
        )
        claimed = claim_part(part.pk, lease_id=lease_id)
        return claimed, lease_id

    def complete_claimed_part(self, part, lease_id, *, rows=10):
        begin_part_upload(part.pk, lease_id=lease_id)
        return complete_part(
            part.pk,
            lease_id=lease_id,
            actual_rows=rows,
            actual_bytes=100,
            compressed_bytes=50,
            object_key=f"exports/{part.pk}.tar.gz",
            checksum="b" * 64,
        )

    def test_plan_activation_requires_exact_cover_and_is_atomic(self):
        job = self.create_job()
        begin_planning(job.pk)

        with self.assertRaises(PlanValidationError):
            persist_plan(
                job.pk,
                attempt=claim_planning(job_id=job.pk),
                query_hash=job.query_hash,
                target_rows=1,
                target_bytes=1,
                histogram_interval=1,
                parts=[PartSpec(1, 0, 5), PartSpec(2, 6, 20)],
            )

        self.assertFalse(job.plans.exists())
        self.assertEqual(ExportPart.objects.count(), 0)
        job.refresh_from_db()
        self.assertEqual(job.status, ExportJob.Status.PLANNING)

    def test_part_lifecycle_recomputes_actual_total(self):
        job = self.create_job()
        plan = self.activate_plan(job)
        part = plan.parts.get()
        claimed, lease_id = self.dispatch_and_claim(part)

        self.assertEqual(claimed.attempts, 1)
        heartbeat_part(
            part.pk,
            lease_id=lease_id,
            lease_until=timezone.now() + timedelta(minutes=5),
        )
        self.complete_claimed_part(part, lease_id, rows=10)

        part.refresh_from_db()
        job.refresh_from_db()
        self.assertEqual(part.status, ExportPart.Status.SUCCESS)
        self.assertEqual(job.actual_total, 10)
        with self.assertRaises(StaleExportUpdateError):
            heartbeat_part(
                part.pk,
                lease_id=lease_id,
                lease_until=timezone.now() + timedelta(minutes=5),
            )

    def test_dispatch_failure_releases_without_consuming_attempt(self):
        job = self.create_job()
        plan = self.activate_plan(job)
        part = plan.parts.get()
        lease_id = "lease-dispatch"
        dispatch_part(
            part.pk,
            lease_id=lease_id,
            task_id="task-dispatch",
            lease_until=timezone.now() + timedelta(minutes=5),
        )
        release_dispatch(part.pk, lease_id=lease_id)
        part.refresh_from_db()
        self.assertEqual(part.status, ExportPart.Status.WAITING)
        self.assertEqual(part.attempts, 0)

    def test_retry_is_bounded_to_three_attempts(self):
        job = self.create_job()
        plan = self.activate_plan(job)
        part = plan.parts.get()

        for attempt in range(1, 4):
            claimed, lease_id = self.dispatch_and_claim(part)
            retry_part(
                part.pk,
                lease_id=lease_id,
                error_code="QUERY_FAILED",
                error_detail="transient",
            )
            part.refresh_from_db()
            expected = ExportPart.Status.WAITING if attempt < 3 else ExportPart.Status.FAILED
            self.assertEqual(part.status, expected)
            self.assertEqual(claimed.attempts, attempt)

    @override_settings(ASYNC_EXPORT_MAX_ATTEMPTS=1)
    def test_failed_leaf_can_be_replaced_by_contiguous_children(self):
        job = self.create_job()
        plan = self.activate_plan(job)
        part = plan.parts.get()
        _claimed, lease_id = self.dispatch_and_claim(part)
        retry_part(
            part.pk,
            lease_id=lease_id,
            error_code="OVERSIZED",
            error_detail="cannot finish in one attempt",
        )

        children = split_part(
            part.pk,
            children=[PartSpec(None, 0, 10), PartSpec(None, 10, 20)],
        )

        part.refresh_from_db()
        plan.refresh_from_db()
        self.assertEqual(part.status, ExportPart.Status.SPLIT)
        self.assertFalse(part.is_leaf)
        self.assertEqual(len(children), 2)
        self.assertEqual(plan.part_count, 2)
        self.assertEqual(set(plan.parts.filter(is_leaf=True).values_list("part_no", flat=True)), {2, 3})

    def test_cancel_keeps_running_io_uncancelled_but_blocks_result(self):
        job = self.create_job()
        plan = self.activate_plan(
            job,
            parts=[PartSpec(1, 0, 10), PartSpec(2, 10, 20)],
        )
        running, lease_id = self.dispatch_and_claim(plan.parts.get(part_no=1))
        cancel_job(job.pk)

        running.refresh_from_db()
        waiting = plan.parts.get(part_no=2)
        self.assertEqual(running.status, ExportPart.Status.RUNNING)
        self.assertEqual(waiting.status, ExportPart.Status.CANCELED)
        with self.assertRaises(StaleExportUpdateError):
            self.complete_claimed_part(running, lease_id)

    def test_manifest_success_requires_all_current_leaf_parts(self):
        job = self.create_job()
        plan = self.activate_plan(
            job,
            parts=[PartSpec(1, 0, 10), PartSpec(2, 10, 20)],
        )
        first = plan.parts.get(part_no=1)
        claimed, lease_id = self.dispatch_and_claim(first)
        self.complete_claimed_part(claimed, lease_id)

        with self.assertRaises(InvalidTransitionError):
            finalize_job_success(
                job.pk,
                plan_version=plan.plan_version,
                manifest_object_key="exports/manifest.json",
                manifest_checksum="c" * 64,
            )

        second = plan.parts.get(part_no=2)
        claimed, lease_id = self.dispatch_and_claim(second)
        self.complete_claimed_part(claimed, lease_id, rows=20)
        finalized = finalize_job_success(
            job.pk,
            plan_version=plan.plan_version,
            manifest_object_key="exports/manifest.json",
            manifest_checksum="c" * 64,
        )
        self.assertEqual(finalized.status, ExportJob.Status.SUCCESS)
        self.assertEqual(finalized.actual_total, 30)

    def test_repeated_planning_transition_is_rejected(self):
        job = self.create_job()
        begin_planning(job.pk)
        with self.assertRaises(InvalidTransitionError):
            begin_planning(job.pk)

    def test_planning_failure_is_terminal_and_does_not_leave_dispatchable_parts(self):
        job = self.create_job()
        begin_planning(job.pk)
        failed = fail_job(job.pk, error_code="STATISTICS_FAILED", error_detail="unify query unavailable")
        self.assertEqual(failed.status, ExportJob.Status.FAILED)
        self.assertEqual(failed.error_code, "STATISTICS_FAILED")

    def test_expired_dispatch_cannot_be_created_or_claimed(self):
        job = self.create_job()
        part = self.activate_plan(job).parts.get()
        now = timezone.now()
        with patch("apps.log_search.export.state._now", return_value=now):
            with self.assertRaises(ExportStateError):
                dispatch_part(part.pk, lease_id="lease", task_id="task", lease_until=now)
        part.refresh_from_db()
        self.assertEqual(part.status, ExportPart.Status.WAITING)
        dispatched = dispatch_part(part.pk, lease_id="lease", task_id="task", lease_until=now + timedelta(minutes=1))
        with patch("apps.log_search.export.state._now", return_value=dispatched.lease_until):
            with self.assertRaises(StaleExportUpdateError):
                claim_part(part.pk, lease_id="lease")
        part.refresh_from_db()
        self.assertEqual(part.status, ExportPart.Status.DISPATCHED)
        self.assertEqual(part.attempts, 0)

    def test_expired_worker_cannot_revive_lease_upload_or_commit(self):
        job = self.create_job()
        part, lease_id = self.dispatch_and_claim(self.activate_plan(job).parts.get())
        with patch("apps.log_search.export.state._now", return_value=part.lease_until):
            with self.assertRaises(StaleExportUpdateError):
                heartbeat_part(
                    part.pk,
                    lease_id=lease_id,
                    lease_until=part.lease_until + timedelta(minutes=1),
                )
            with self.assertRaises(StaleExportUpdateError):
                begin_part_upload(part.pk, lease_id=lease_id)
        begin_part_upload(part.pk, lease_id=lease_id)
        with patch("apps.log_search.export.state._now", return_value=part.lease_until):
            with self.assertRaises(StaleExportUpdateError):
                complete_part(
                    part.pk,
                    lease_id=lease_id,
                    actual_rows=999,
                    actual_bytes=100,
                    compressed_bytes=50,
                    object_key="late",
                    checksum="b" * 64,
                )
        part.refresh_from_db()
        job.refresh_from_db()
        self.assertEqual(part.status, ExportPart.Status.UPLOADING)
        self.assertEqual(part.object_key, "")
        self.assertEqual(job.actual_total, 0)

    def test_heartbeat_cannot_shorten_lease(self):
        part, lease_id = self.dispatch_and_claim(self.activate_plan(self.create_job()).parts.get())
        with self.assertRaises(ExportStateError):
            heartbeat_part(
                part.pk,
                lease_id=lease_id,
                lease_until=part.lease_until - timedelta(seconds=1),
            )
        original_expiry = part.lease_until
        part.refresh_from_db()
        self.assertEqual(part.lease_until, original_expiry)

    def test_failed_job_rejects_upload_and_commit_and_does_not_requeue_workers(self):
        job = self.create_job()
        plan = self.activate_plan(job, parts=[PartSpec(1, 0, 10), PartSpec(2, 10, 20)])
        running, lease1 = self.dispatch_and_claim(plan.parts.get(part_no=1))
        uploading, lease2 = self.dispatch_and_claim(plan.parts.get(part_no=2))
        begin_part_upload(uploading.pk, lease_id=lease2)
        fail_job(job.pk, error_code="JOB_TIMEOUT")
        with self.assertRaises(StaleExportUpdateError):
            begin_part_upload(running.pk, lease_id=lease1)
        with self.assertRaises(StaleExportUpdateError):
            complete_part(
                uploading.pk,
                lease_id=lease2,
                actual_rows=999,
                actual_bytes=100,
                compressed_bytes=50,
                object_key="late",
                checksum="b" * 64,
            )
        for part, lease in [(running, lease1), (uploading, lease2)]:
            retry_part(part.pk, lease_id=lease, error_code="STOPPED", error_detail="")
            part.refresh_from_db()
            self.assertEqual(part.status, ExportPart.Status.CANCELED)
            self.assertEqual(part.lease_id, "")
        job.refresh_from_db()
        self.assertEqual(job.status, ExportJob.Status.FAILED)
        self.assertEqual(job.actual_total, 0)

    def test_repeated_cancel_preserves_terminal_metadata_and_running_lease(self):
        job = self.create_job()
        part, lease_id = self.dispatch_and_claim(self.activate_plan(job).parts.get())
        canceled = cancel_job(job.pk)
        again = cancel_job(job.pk)
        self.assertEqual(again.completed_at, canceled.completed_at)
        part.refresh_from_db()
        self.assertEqual(part.lease_id, lease_id)
        # 已开始的 I/O 退出之前，Worker 仍然持有它的预算。
        heartbeat_part(
            part.pk,
            lease_id=lease_id,
            lease_until=part.lease_until + timedelta(seconds=1),
        )
        retry_part(part.pk, lease_id=lease_id, error_code="CANCELED", error_detail="")
        part.refresh_from_db()
        self.assertEqual(part.status, ExportPart.Status.CANCELED)

    def test_plan_rejects_mismatched_query_snapshot(self):
        job = self.create_job()
        begin_planning(job.pk)
        with self.assertRaises(PlanValidationError):
            persist_plan(
                job.pk,
                attempt=claim_planning(job_id=job.pk),
                query_hash="different-query",
                target_rows=1,
                target_bytes=1,
                histogram_interval=1,
                parts=[PartSpec(1, 0, 20)],
            )
        self.assertFalse(job.plans.exists())

    def test_success_lifetime_starts_at_manifest_commit(self):
        job = self.create_job()
        plan = self.activate_plan(job)
        part, lease = self.dispatch_and_claim(plan.parts.get())
        self.complete_claimed_part(part, lease)
        completed_at = timezone.now() + timedelta(hours=2)
        with patch("apps.log_search.export.state._now", return_value=completed_at):
            finalized = finalize_job_success(
                job.pk,
                plan_version=1,
                manifest_object_key="manifest",
                manifest_checksum="c" * 64,
            )
        self.assertEqual(finalized.completed_at, completed_at)
        self.assertEqual(finalized.expires_at, completed_at + timedelta(hours=24))

    @override_settings(ASYNC_EXPORT_MAX_LEAF_PARTS=2, ASYNC_EXPORT_MAX_ATTEMPTS=1)
    def test_split_limit_and_invalid_estimates_roll_back_without_affecting_successful_sibling(self):
        job = self.create_job()
        plan = self.activate_plan(job, parts=[PartSpec(1, 0, 10), PartSpec(2, 10, 20)])
        first, lease = self.dispatch_and_claim(plan.parts.get(part_no=1))
        self.complete_claimed_part(first, lease, rows=7)
        second, lease = self.dispatch_and_claim(plan.parts.get(part_no=2))
        retry_part(second.pk, lease_id=lease, error_code="OVERSIZED", error_detail="")
        for children in [
            [PartSpec(None, 10, 15), PartSpec(None, 15, 20)],
            [PartSpec(None, 10, 15, estimated_rows=-1), PartSpec(None, 15, 20)],
        ]:
            with self.subTest(children=children), self.assertRaises(PlanValidationError):
                split_part(second.pk, children=children)
        second.refresh_from_db()
        plan.refresh_from_db()
        self.assertEqual(second.status, ExportPart.Status.FAILED)
        self.assertTrue(second.is_leaf)
        self.assertEqual(plan.part_count, 2)
        with override_settings(ASYNC_EXPORT_MAX_LEAF_PARTS=3):
            children = split_part(second.pk, children=[PartSpec(None, 10, 15), PartSpec(None, 15, 20)])
        for child in children:
            claimed, lease = self.dispatch_and_claim(child)
            self.complete_claimed_part(claimed, lease, rows=3)
        finalized = finalize_job_success(
            job.pk, plan_version=1, manifest_object_key="manifest", manifest_checksum="c" * 64
        )
        self.assertEqual(finalized.actual_total, 13)
        first.refresh_from_db()
        self.assertEqual(first.attempts, 1)

    def test_duplicate_delivery_and_old_failure_cannot_change_new_attempt(self):
        job = self.create_job()
        part, lease = self.dispatch_and_claim(self.activate_plan(job).parts.get())
        with self.assertRaises(StaleExportUpdateError):
            claim_part(part.pk, lease_id=lease)
        retry_part(part.pk, lease_id=lease, error_code="QUERY_FAILED", error_detail="")
        current, current_lease = self.dispatch_and_claim(part)
        with self.assertRaises(StaleExportUpdateError):
            retry_part(part.pk, lease_id=lease, error_code="LATE", error_detail="")
        part.refresh_from_db()
        self.assertEqual(part.status, ExportPart.Status.RUNNING)
        self.assertEqual(part.attempts, 2)
        self.assertEqual(part.lease_id, current_lease)

    def test_initial_plan_enforces_500_leaf_limit_atomically(self):
        job = self.create_job(end_time=501)
        begin_planning(job.pk)
        kwargs = dict(
            attempt=claim_planning(job_id=job.pk),
            query_hash=job.query_hash,
            target_rows=1,
            target_bytes=1,
            histogram_interval=1,
        )
        with self.assertRaises(PlanValidationError):
            persist_plan(job.pk, parts=[PartSpec(i + 1, i, i + 1) for i in range(501)], **kwargs)
        self.assertFalse(job.plans.exists())
        self.assertFalse(ExportPart.objects.exists())
        plan = persist_plan(
            job.pk,
            parts=[PartSpec(i + 1, i, i + 1) for i in range(499)] + [PartSpec(500, 499, 501)],
            **kwargs,
        )
        self.assertEqual(plan.part_count, 500)
        self.assertEqual(plan.parts.count(), 500)

    def test_plan_rejects_boundaries_that_cannot_be_expressed_at_job_precision(self):
        job = self.create_job()
        job.time_tick = 10
        job.save(update_fields=["time_tick"])
        with self.assertRaises(PlanValidationError):
            self.activate_plan(job, parts=[PartSpec(1, 0, 5), PartSpec(2, 5, 20)])
        self.assertFalse(job.plans.exists())

    @override_settings(ASYNC_EXPORT_MAX_ATTEMPTS=1)
    def test_split_uses_the_same_precision_validation_as_initial_plan(self):
        job = self.create_job()
        job.time_tick = 10
        job.save(update_fields=["time_tick"])
        part = self.activate_plan(job).parts.get()
        _, lease = self.dispatch_and_claim(part)
        retry_part(part.pk, lease_id=lease, error_code="OVERSIZED", error_detail="")
        with self.assertRaises(PlanValidationError):
            split_part(part.pk, children=[PartSpec(None, 0, 5), PartSpec(None, 5, 20)])
        part.refresh_from_db()
        self.assertEqual(part.status, "FAILED")
        self.assertFalse(part.children.exists())
