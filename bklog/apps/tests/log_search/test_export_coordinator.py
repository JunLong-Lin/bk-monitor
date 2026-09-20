import shutil
import subprocess
import tempfile
import time
from contextlib import contextmanager
from datetime import timedelta
from pathlib import Path
from unittest import SkipTest
from unittest.mock import Mock, patch

import redis
from django.test import TestCase, override_settings
from django.utils import timezone

from apps.log_search.export.coordinator import BudgetUnavailable, Coordinator, Limits, PublishNotSent, RedisBudget
from apps.log_search.export.models import ExportJob, ExportPart
from apps.tests.log_search.export_fixtures import create_job
from apps.log_search.export import state


class CoordinatorTest(TestCase):
    @classmethod
    def setUpClass(cls):
        if not shutil.which("redis-server"):
            raise SkipTest("isolated redis-server is required for Lua integration tests")
        super().setUpClass()
        cls.temp = tempfile.TemporaryDirectory(prefix="bklog-export-redis-")
        socket = str(Path(cls.temp.name) / "redis.sock")
        cls.server = subprocess.Popen(
            [
                "redis-server",
                "--port",
                "0",
                "--save",
                "",
                "--appendonly",
                "no",
                "--unixsocket",
                socket,
                "--unixsocketperm",
                "700",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        cls.redis_client = redis.Redis(unix_socket_path=socket, socket_timeout=1)
        for _ in range(100):
            try:
                cls.redis_client.ping()
                break
            except redis.ConnectionError:
                time.sleep(0.02)
        else:
            cls.server.terminate()
            cls.server.wait(timeout=5)
            cls.temp.cleanup()
            raise RuntimeError("isolated Redis did not start")

    @classmethod
    def tearDownClass(cls):
        cls.redis_client.close()
        cls.server.terminate()
        cls.server.wait(timeout=5)
        cls.temp.cleanup()
        super().tearDownClass()

    def setUp(self):
        self.redis_client.flushdb()  # 私有 unix socket，绝不连接部署环境的 Redis。
        self.budget = RedisBudget(self.redis_client, "test-environment")
        self.publish = Mock()
        self.coordinator = Coordinator(
            self.budget, self.publish, namespace="test-environment", limits=Limits(8, 4, 1, 60)
        )

    def ready(self, *, resources=None, parallelism=4, oversized=False):
        job = create_job(
            resolved_resource_ids=resources or ["index:1"],
            requested_parallelism=parallelism,
            end_time=10,
        )
        state.begin_planning(job.pk)
        plan = state.persist_plan(
            job.pk,
            attempt=state.claim_planning(job_id=job.pk),
            query_hash=job.query_hash,
            target_rows=1,
            target_bytes=1,
            histogram_interval=1,
            parts=[state.PartSpec(i + 1, i, i + 1, oversized=oversized) for i in range(10)],
        )
        return job, plan

    def test_round_robin_shared_index_limit_and_job_limit(self):
        first, _ = self.ready(parallelism=8)
        second, _ = self.ready(parallelism=8)
        sent = self.coordinator.tick()
        jobs = [ExportPart.objects.get(pk=pk).plan.job_id for pk, _ in sent]
        self.assertEqual(jobs, [first.pk, second.pk, first.pk, second.pk])
        self.assertEqual(len(sent), 4)

    def test_union_consumes_all_indexes_atomically(self):
        union, _ = self.ready(resources=["index:A", "index:B"])
        single, _ = self.ready(resources=["index:B"])
        other, _ = self.ready(resources=["index:C"])
        self.coordinator.tick()
        self.assertEqual(
            ExportPart.objects.filter(plan__job_id__in=[union.pk, single.pk], status="DISPATCHED").count(), 4
        )
        self.assertEqual(ExportPart.objects.filter(plan__job=other, status="DISPATCHED").count(), 4)

    def test_distinct_indexes_share_global_capacity(self):
        self.ready(resources=["index:1"])
        self.ready(resources=["index:2"])
        self.assertEqual(len(self.coordinator.tick()), 8)

    def test_tick_rebuilds_ledger_once_for_multiple_reservations(self):
        self.ready()
        with patch.object(self.budget, "rebuild", wraps=self.budget.rebuild) as rebuild:
            self.assertEqual(len(self.coordinator.tick(max_dispatches=4)), 4)
        rebuild.assert_called_once()

    def test_oversized_budget_does_not_bypass_global_budget(self):
        self.ready(oversized=True)
        self.assertEqual(len(self.coordinator.tick()), 1)

    def test_redis_loss_rebuild_keeps_running_expired_and_canceled_job_io(self):
        job, _ = self.ready()
        self.coordinator.tick()
        for part in ExportPart.objects.filter(status="DISPATCHED"):
            state.claim_part(part.pk, generation=part.dispatch_generation, lease_id=part.lease_id, worker_id="worker")
        state.cancel_job(job.pk)
        ExportPart.objects.filter(status="RUNNING").update(lease_until=timezone.now() - timedelta(seconds=1))
        self.redis_client.delete(self.budget.key)
        self.ready()
        self.assertEqual(self.coordinator.tick(), [])
        retained = self.coordinator.recover_expired()
        self.assertEqual(len(retained), 4)
        self.assertEqual(self.coordinator.tick(), [])

    def test_expired_running_execution_keeps_its_reservation(self):
        _, plan = self.ready()
        part = self.coordinator.reserve()
        state.claim_part(part.pk, generation=1, lease_id=part.lease_id, worker_id="worker")
        ExportPart.objects.filter(pk=part.pk).update(lease_until=timezone.now() - timedelta(seconds=1))
        self.assertEqual(self.coordinator.recover_expired(), [part.pk])
        part.refresh_from_db()
        self.assertEqual(part.status, "RUNNING")
        self.assertEqual(part.attempts, 1)
        self.assertEqual(part.dispatch_generation, 1)

    def test_expired_unclaimed_delivery_can_be_released_without_io_proof(self):
        self.ready()
        part = self.coordinator.reserve()
        ExportPart.objects.filter(pk=part.pk).update(lease_until=timezone.now() - timedelta(seconds=1))
        self.coordinator.recover_expired()
        part.refresh_from_db()
        self.assertEqual(part.status, "WAITING")
        self.assertEqual(part.attempts, 0)
        with self.assertRaises(state.StaleExportUpdateError):
            state.claim_part(part.pk, generation=1, lease_id="old", worker_id="worker")

    def test_uncertain_broker_publish_replays_same_generation_and_task_id(self):
        self.ready()
        self.publish.side_effect = TimeoutError("broker response lost")
        result = self.coordinator.tick()
        self.assertEqual(result[0][1], "uncertain")
        part = ExportPart.objects.get(pk=result[0][0])
        self.assertIsNone(part.published_at)
        self.publish.side_effect = None
        self.coordinator.replay()
        replayed = self.publish.call_args.args[0]
        self.assertEqual(replayed.task_id, part.task_id)
        self.assertEqual(replayed.dispatch_generation, part.dispatch_generation)

    def test_definite_publish_failure_returns_waiting_without_attempt(self):
        self.ready()
        self.publish.side_effect = PublishNotSent()
        result = self.coordinator.tick()
        part = ExportPart.objects.get(pk=result[0][0])
        self.assertEqual(part.status, "WAITING")
        self.assertEqual(part.attempts, 0)
        self.assertFalse(self.redis_client.hexists(self.budget.key, str(part.pk)))

    def test_publish_failure_does_not_release_budget_before_db_commit(self):
        self.ready()
        part = self.coordinator.reserve()
        self.publish.side_effect = PublishNotSent()
        original_gate = self.coordinator.gate

        @contextmanager
        def failed_commit():
            with original_gate() as gate:
                yield gate
                raise RuntimeError("database commit failed")

        with patch.object(self.coordinator, "gate", failed_commit), self.assertRaises(RuntimeError):
            self.coordinator.deliver(part)
        part.refresh_from_db()
        self.assertEqual(part.status, ExportPart.Status.DISPATCHED)
        self.assertTrue(self.redis_client.hexists(self.budget.key, str(part.pk)))

    def test_crash_after_db_commit_is_discovered_by_replay(self):
        self.ready()
        part = self.coordinator.reserve()
        self.publish.assert_not_called()
        self.coordinator.replay()
        self.assertEqual(self.publish.call_args.args[0].pk, part.pk)

    def test_redis_outage_fails_closed_without_creating_dispatch(self):
        self.ready()
        with (
            patch.object(self.redis_client, "eval", side_effect=redis.ConnectionError),
            self.assertRaises(BudgetUnavailable),
        ):
            self.coordinator.tick()
        self.assertFalse(ExportPart.objects.filter(status="DISPATCHED").exists())
        self.publish.assert_not_called()

    def test_old_owner_cannot_release_new_owner_or_renew_expired_lease(self):
        self.ready()
        part = self.coordinator.reserve()
        self.assertFalse(self.budget.release(part.pk, 2, part.lease_id))
        self.assertFalse(self.budget.release(part.pk, 1, "wrong"))
        self.assertTrue(self.redis_client.hexists(self.budget.key, str(part.pk)))
        future = part.lease_until + timedelta(seconds=1)
        with patch("apps.log_search.export.coordinator.timezone.now", return_value=future):
            self.assertFalse(self.budget.renew(part.pk, 1, part.lease_id, future + timedelta(minutes=1)))

    def test_db_rollback_after_reservation_leaves_no_dispatch_and_rebuild_removes_orphan(self):
        self.ready()
        with patch("apps.log_search.export.coordinator.state.dispatch_part", side_effect=RuntimeError("crash")):
            with self.assertRaises(RuntimeError):
                self.coordinator.reserve()
        self.assertFalse(ExportPart.objects.filter(status="DISPATCHED").exists())
        self.coordinator.reconcile()
        self.assertEqual(self.redis_client.hlen(self.budget.key), 1)  # 只剩 epoch

    def test_parallelism_reduction_waits_for_existing_work(self):
        from apps.log_search.export.state import set_parallelism

        job, _ = self.ready()
        self.coordinator.tick()
        set_parallelism(job.pk, 1)
        self.assertEqual(self.coordinator.tick(), [])
        self.assertEqual(ExportPart.objects.filter(status="DISPATCHED").count(), 4)

    def test_round_robin_position_survives_new_coordinator_instance(self):
        first, _ = self.ready()
        second, _ = self.ready()
        self.assertEqual(self.coordinator.reserve().plan.job_id, first.pk)
        other = Coordinator(self.budget, self.publish, namespace="test-environment", limits=self.coordinator.limits)
        self.assertEqual(other.reserve().plan.job_id, second.pk)

    @override_settings(ASYNC_EXPORT_AGING_SECONDS=30)
    def test_aging_prioritizes_long_waiting_job_over_recently_dispatched(self):
        fresh, _ = self.ready()  # 较小 pk，最近刚投递
        aged, _ = self.ready()  # 较大 pk，但已长时间没有获得投递
        ExportJob.objects.filter(pk=fresh.pk).update(last_dispatched_at=timezone.now())
        ExportJob.objects.filter(pk=aged.pk).update(last_dispatched_at=timezone.now() - timedelta(minutes=2))
        self.assertEqual(self.coordinator.reserve().plan.job_id, aged.pk)

    @override_settings(ASYNC_EXPORT_AGING_SECONDS=30)
    def test_never_dispatched_job_gets_first_dispatch_priority(self):
        dispatched, _ = self.ready()  # 较小 pk，最近刚投递
        waiting, _ = self.ready()  # 较大 pk，从未投递过（last_dispatched_at 为 NULL）
        ExportJob.objects.filter(pk=dispatched.pk).update(last_dispatched_at=timezone.now())
        self.assertEqual(self.coordinator.reserve().plan.job_id, waiting.pk)

    def test_concurrent_lua_acquisition_never_exceeds_shared_index_limit(self):
        from concurrent.futures import ThreadPoolExecutor

        _, plan = self.ready(parallelism=8)
        epoch = self.budget.rebuild([])
        candidates = list(plan.parts.select_related("plan__job"))
        for part in candidates:
            part.lease_id = f"lease-{part.pk}"
            part.dispatch_generation = 1
            part.lease_until = timezone.now() + timedelta(minutes=1)
        with ThreadPoolExecutor(max_workers=10) as pool:
            outcomes = list(
                pool.map(lambda part: self.budget.acquire(part, epoch, self.coordinator.limits), candidates)
            )
        self.assertEqual(sum(outcomes), 4)

    def test_busy_oversized_budget_does_not_block_ordinary_sibling(self):
        _, plan = self.ready(oversized=True)
        ExportPart.objects.filter(plan=plan, part_no=3).update(oversized=False)
        parts = self.coordinator.tick()
        self.assertEqual(len(parts), 2)
        self.assertEqual(ExportPart.objects.filter(status="DISPATCHED", oversized=False).count(), 1)

    def test_recovery_skips_a_candidate_whose_credential_changed(self):
        self.ready()
        part = self.coordinator.reserve()
        stale = ExportPart.objects.get(pk=part.pk)
        ExportPart.objects.filter(pk=part.pk).update(
            dispatch_generation=2, lease_until=timezone.now() + timedelta(minutes=1)
        )
        self.assertFalse(state.recover_expired_part(stale))
        part.refresh_from_db()
        self.assertEqual(part.status, "DISPATCHED")
        self.assertEqual(part.dispatch_generation, 2)

    def test_unconfirmed_execution_does_not_starve_later_expired_dispatch(self):
        self.ready()
        first = self.coordinator.reserve()
        second = self.coordinator.reserve()
        state.claim_part(first.pk, generation=1, lease_id=first.lease_id, worker_id="worker")
        ExportPart.objects.filter(pk__in=[first.pk, second.pk]).update(
            lease_until=timezone.now() - timedelta(seconds=1)
        )
        self.coordinator.recover_expired(limit=1)
        self.coordinator.recover_expired(limit=1)
        second.refresh_from_db()
        self.assertEqual(second.status, "WAITING")
        first.refresh_from_db()
        self.assertEqual(first.status, "RUNNING")

    def test_bounded_replay_rotates_past_a_delivery_that_stays_queued(self):
        self.ready()
        first = self.coordinator.reserve()
        second = self.coordinator.reserve()
        self.coordinator.replay(limit=1)
        self.coordinator.replay(limit=1)
        self.assertEqual([call.args[0].pk for call in self.publish.call_args_list], [first.pk, second.pk])

    def test_planning_scans_rotate_even_when_messages_never_start(self):
        first = create_job()
        second = create_job()
        self.assertEqual([job.pk for job in self.coordinator.planning_jobs(limit=1)], [first.pk])
        other = Coordinator(self.budget, self.publish, namespace="test-environment", limits=self.coordinator.limits)
        self.assertEqual([job.pk for job in other.planning_jobs(limit=1)], [second.pk])

    def test_finalization_scan_filters_incomplete_jobs_before_rotating(self):
        incomplete, _ = self.ready()
        first, first_plan = self.ready()
        second, second_plan = self.ready()
        # 已完成叶子只是供扫描发现用的夹具，不测试 Worker。
        for job, plan in [(first, first_plan), (second, second_plan)]:
            ExportJob.objects.filter(pk=job.pk).update(status="RUNNING")
            plan.parts.update(status="SUCCESS", actual_rows=1)
        ExportJob.objects.filter(pk=incomplete.pk).update(status="RUNNING")
        self.assertEqual(self.coordinator.finalizing_jobs(limit=1), [first.pk])
        self.assertEqual(self.coordinator.finalizing_jobs(limit=1), [second.pk])

    def test_deferred_split_does_not_hide_exhausted_failure_behind_it(self):
        job, plan = self.ready()
        ExportJob.objects.filter(pk=job.pk).update(status="RUNNING")
        first = plan.parts.get(part_no=1)
        second = plan.parts.get(part_no=2)
        ExportPart.objects.filter(pk=first.pk).update(
            status="FAILED", error_code="OVERSIZED", planning_lease_until=timezone.now() + timedelta(minutes=1)
        )
        ExportPart.objects.filter(pk=second.pk).update(status="FAILED", attempts=3, error_code="QUERY_FAILED")
        self.assertEqual([part.pk for part in self.coordinator.failed_parts(limit=1)], [second.pk])
        list(self.coordinator.control_work(limit=1))
        job.refresh_from_db()
        self.assertEqual(job.status, "FAILED")
        self.assertEqual(self.coordinator.tick(), [])


@override_settings(ASYNC_EXPORT_SHARDED_ENABLED=True)
class AdmissionTest(TestCase):
    def test_three_new_jobs_block_old_creation(self):
        from apps.log_search.models import AsyncTask
        from apps.log_search.exceptions import ConcurrentExportLimitException

        for _ in range(3):
            create_job()
        with self.assertRaises(ConcurrentExportLimitException):
            AsyncTask.async_export_task_create_with_running_limit("alice")
        self.assertEqual(AsyncTask.objects.count(), 0)

    def test_new_admission_counts_existing_legacy_jobs_and_preserves_scene_group(self):
        from apps.log_search.models import AsyncTask
        from apps.log_search.constants import ExportType
        from apps.log_search.exceptions import ConcurrentExportLimitException
        from apps.log_search.export.admission import create_job as admit

        for _ in range(3):
            AsyncTask.objects.create(created_by="alice", export_type=ExportType.ASYNC, request_param={})
        values = dict(
            space_uid="space",
            created_by="alice",
            query_kind="single",
            query_hash="a" * 64,
            query_snapshot={"time_units_per_second": 1},
            start_time=0,
            end_time=10,
            time_tick=1,
        )
        with self.assertRaises(ConcurrentExportLimitException):
            admit(**values)
        values["query_kind"] = "scene"
        self.assertEqual(admit(**values).status, ExportJob.Status.PENDING)

    def test_same_request_is_idempotent_even_at_capacity(self):
        from apps.log_search.export.admission import create_job as admit

        values = dict(
            space_uid="space",
            created_by="alice",
            query_kind="single",
            query_hash="a" * 64,
            query_snapshot={"time_units_per_second": 1},
            request_id="same",
            start_time=0,
            end_time=10,
            time_tick=1,
        )
        first = admit(**values)
        create_job()
        create_job()
        self.assertEqual(admit(**values).pk, first.pk)


class ControlTasksTest(TestCase):
    @override_settings(ASYNC_EXPORT_CONTROL_ENABLED=False)
    def test_disabled_control_tasks_do_not_connect_or_publish(self):
        from apps.log_search.tasks.sharded_export import coordinate_sharded_exports, plan_sharded_export

        with (
            patch("apps.log_search.tasks.sharded_export.get_redis_connection") as connect,
            patch("apps.log_search.tasks.sharded_export.adapter_factory") as factory,
        ):
            coordinate_sharded_exports()
            plan_sharded_export(123)
        connect.assert_not_called()
        factory.assert_not_called()

    @override_settings(ASYNC_EXPORT_CONTROL_ENABLED=True, ASYNC_EXPORT_FINALIZE_TASK="")
    def test_incomplete_runtime_configuration_pauses_before_connecting(self):
        from apps.log_search.tasks.sharded_export import coordinate_sharded_exports

        with (
            patch("apps.log_search.tasks.sharded_export.get_redis_connection") as connect,
            patch("apps.log_search.tasks.sharded_export.logger") as logger,
        ):
            coordinate_sharded_exports()
        connect.assert_not_called()
        logger.warning.assert_called_once()

    @override_settings(ASYNC_EXPORT_PART_TASK="worker.part")
    def test_publish_uses_only_part_id_and_generation_headers(self):
        from apps.log_search.tasks.sharded_export import publish_part

        job = create_job()
        state.begin_planning(job.pk)
        plan = state.persist_plan(
            job.pk,
            attempt=state.claim_planning(job_id=job.pk),
            query_hash=job.query_hash,
            target_rows=1,
            target_bytes=1,
            histogram_interval=1,
            parts=[state.PartSpec(1, 0, 60)],
        )
        part = state.dispatch_part(
            plan.parts.get().pk, lease_id="owner", task_id="task", lease_until=timezone.now() + timedelta(minutes=1)
        )
        with patch("apps.log_search.tasks.sharded_export.app.send_task") as send:
            publish_part(part)
        self.assertEqual(send.call_args.kwargs["args"], [part.pk])
        self.assertEqual(send.call_args.kwargs["task_id"], "task")
        self.assertEqual(send.call_args.kwargs["headers"], {"export_generation": 1, "export_lease_id": "owner"})
        self.assertFalse(send.call_args.kwargs["retry"])

    @override_settings(
        ASYNC_EXPORT_SHARDED_ENABLED=False,
        ASYNC_EXPORT_CONTROL_ENABLED=True,
        ASYNC_EXPORT_PART_TASK="worker.part",
        ASYNC_EXPORT_ARTIFACT_STORE_FACTORY="store.factory",
        ASYNC_EXPORT_FINALIZE_TASK="export.finalize",
        ASYNC_EXPORT_GLOBAL_LIMIT=4,
        ASYNC_EXPORT_INDEX_LIMIT=4,
        ASYNC_EXPORT_LEASE_SECONDS=60,
    )
    def test_scanner_keeps_recovering_existing_plans_after_admission_is_disabled(self):
        from apps.log_search.tasks.sharded_export import coordinate_sharded_exports

        job = create_job()
        with (
            patch("apps.log_search.tasks.sharded_export.adapter_factory"),
            patch("apps.log_search.tasks.sharded_export.get_redis_connection"),
            patch("apps.log_search.tasks.sharded_export.Coordinator") as coordinator,
            patch("apps.log_search.tasks.sharded_export.plan_sharded_export.apply_async") as publish,
        ):
            coordinator.return_value.recover_expired.return_value = []
            coordinator.return_value.control_work.return_value = [("plan", job.pk)]
            coordinate_sharded_exports()
        self.assertEqual(publish.call_args.kwargs["args"], [job.pk])
