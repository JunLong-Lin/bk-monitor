from contextlib import contextmanager
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import Mock, patch

from django.test import TestCase, override_settings
from django.utils import timezone

from apps.log_search.export.models import ExportJob, ExportPart
from apps.log_search.export.contracts import PlannerPolicy, PlanningError
from apps.log_search.export.planner import (
    AdaptivePlanner,
    plan_job,
    replan_failed_part,
)
from apps.log_search.export import state
from apps.log_search.export.query import UnifyQueryStatistics
from apps.tests.log_search.export_fixtures import create_job, Distribution


class AdaptivePlannerTest(TestCase):
    def build(self, points, *, end=60, tick=1, start=0, **policy):
        job = SimpleNamespace(
            start_time=start, end_time=end, time_tick=tick, query_snapshot={"time_units_per_second": 1}
        )
        stats = Distribution(points)
        planner = AdaptivePlanner(job, stats, PlannerPolicy(**policy))
        parts, total = planner.build()
        self.assertEqual(parts[0].start_time, start)
        self.assertEqual(parts[-1].end_time, end)
        self.assertTrue(all(a.end_time == b.start_time for a, b in zip(parts, parts[1:])))
        self.assertTrue(all(p.start_time % tick == 0 and p.end_time % tick == 0 for p in parts))
        return parts, total, stats

    def test_dense_ten_seconds_refines_instead_of_fixed_windows(self):
        parts, total, _ = self.build(dict.fromkeys(range(10), 6000), end=10)
        self.assertEqual(total, 60_000)
        self.assertEqual([(p.start_time, p.end_time) for p in parts], [(0, 5), (5, 10)])

    def test_sparse_empty_and_epoch_clipped_ranges(self):
        for points in [{}, {34: 1, 90: 2}]:
            with self.subTest(points=points):
                parts, total, _ = self.build(points, start=4, end=124, tick=2)
                self.assertEqual(len(parts), 1)
                self.assertEqual(total, sum(points.values()))

    def test_uniform_load_and_byte_budget(self):
        parts, _, _ = self.build(dict.fromkeys(range(60), 100), target_rows=100_000, target_bytes=10_000)
        # 字节维度触发递归拆分：每片估算字节不超过递归触发值（2x 软目标）。
        self.assertTrue(all(p.estimated_bytes <= 20_000 for p in parts))
        self.assertGreater(len(parts), 1)

    def test_minimum_tick_hotspot_is_oversized_and_not_merged(self):
        parts, _, _ = self.build({7: 60_000}, end=10)
        hot = [p for p in parts if p.oversized]
        self.assertEqual([(p.start_time, p.end_time) for p in hot], [(7, 8)])
        self.assertTrue(all(p.end_time - p.start_time == 1 for p in hot))

    def test_histogram_requests_are_bounded_windows(self):
        _, _, stats = self.build({1: 1}, end=1000, max_buckets=2)
        calls = [call for call in stats.calls if call[0] == "histogram"]
        self.assertGreater(len(calls), 1)
        self.assertTrue(all(end - start <= 60 for _, start, end, _ in calls))

    def test_quota_sample_call_and_leaf_budgets(self):
        cases = [
            ({0: 101}, {"max_rows": 100}, "QUOTA_EXCEEDED"),
            ({0: 1}, {"sample_bytes": 1}, "SAMPLE_BUDGET_EXCEEDED"),
            ({0: 1}, {"max_calls": 1}, "PLANNING_BUDGET_EXCEEDED"),
            ({0: 10, 2: 10}, {"target_rows": 1, "max_parts": 1}, "PART_LIMIT_EXCEEDED"),
        ]
        for points, policy, code in cases:
            with self.subTest(code=code), self.assertRaises(PlanningError) as raised:
                self.build(points, **policy)
            self.assertEqual(raised.exception.code, code)

    def test_deadline_and_invalid_counts_fail_closed(self):
        job = create_job()
        clock = Mock(side_effect=[0, 0, 121])
        with self.assertRaises(PlanningError):
            AdaptivePlanner(job, Distribution({}), PlannerPolicy(), clock=clock).build()
        with self.assertRaises(PlanningError):
            AdaptivePlanner(job, Mock(count=Mock(return_value=-1)), PlannerPolicy()).build()

    def test_sparse_long_failed_range_splits_without_fragmenting_every_bucket(self):
        job = create_job(end_time=60_000)
        parts, total = AdaptivePlanner(job, Distribution({1: 1}), PlannerPolicy(max_parts=2)).build(force_split=True)
        self.assertEqual([(p.start_time, p.end_time) for p in parts], [(0, 30_000), (30_000, 60_000)])
        self.assertEqual(total, 1)

    def test_empty_long_range_needs_only_count_and_one_confirmation_part(self):
        parts, total, stats = self.build({}, end=60_000_000, max_calls=1)
        self.assertEqual(len(parts), 1)
        self.assertEqual(total, 0)
        self.assertEqual([call[0] for call in stats.calls], ["count"])


class PlanningStateTest(TestCase):
    def test_complete_plan_is_persisted_with_estimate_and_generation(self):
        job = create_job()
        plan = plan_job(job.pk, lambda job: Distribution({1: 60000}))
        job.refresh_from_db()
        self.assertEqual(job.status, ExportJob.Status.READY)
        self.assertEqual(job.estimated_total, 60000)
        self.assertEqual(plan.plan_version, job.planning_generation)
        self.assertIsNone(job.planning_lease_until)
        self.assertTrue(plan.parts.filter(oversized=True).exists())
        self.assertIsNone(plan_job(job.pk, Mock()))

    def test_statistics_failure_retries_boundedly_without_partial_plan(self):
        job = create_job()
        factory = Mock(side_effect=TimeoutError("must not persist credentials"))
        for attempt in range(3):
            ExportJob.objects.filter(pk=job.pk).update(next_planning_at=None)
            plan_job(job.pk, factory)
        job.refresh_from_db()
        self.assertEqual(job.status, ExportJob.Status.FAILED)
        self.assertEqual(job.planning_attempts, 3)
        self.assertEqual(job.error_code, "STATISTICS_FAILED")
        self.assertEqual(job.error_detail, "")
        self.assertFalse(job.plans.exists())

    def test_cancellation_during_statistics_cannot_activate_plan(self):
        job = create_job()
        stats = Distribution({1: 1})

        def factory(current):
            state.cancel_job(current.pk)
            return stats

        self.assertIsNone(plan_job(job.pk, factory))
        self.assertFalse(job.plans.exists())

    def test_expired_planner_reclaimed_and_old_generation_cannot_commit(self):
        job = create_job(
            status=ExportJob.Status.PLANNING,
            planning_generation=1,
            planning_attempts=1,
            planning_lease_until=timezone.now() - timedelta(seconds=1),
        )
        plan = plan_job(job.pk, lambda job: Distribution({}))
        self.assertEqual(plan.plan_version, 2)
        self.assertEqual(job.plans.count(), 1)

    def test_active_planner_and_retry_delay_do_not_duplicate_statistics(self):
        job = create_job(status=ExportJob.Status.PLANNING, planning_lease_until=timezone.now() + timedelta(minutes=1))
        factory = Mock()
        self.assertIsNone(plan_job(job.pk, factory))
        factory.assert_not_called()

    @override_settings(ASYNC_EXPORT_PLANNER_POLICY={"max_rows": 2})
    def test_quota_rejects_before_creating_any_parts(self):
        job = create_job()
        plan_job(job.pk, lambda job: Distribution({1: 3}))
        job.refresh_from_db()
        self.assertEqual(job.error_code, "QUOTA_EXCEEDED")
        self.assertFalse(ExportPart.objects.exists())

    @override_settings(ASYNC_EXPORT_MAX_ATTEMPTS=1)
    def test_failed_leaf_replanning_preserves_successful_siblings(self):
        job = create_job(end_time=10)
        plan = plan_job(job.pk, lambda job: Distribution({1: 1}))
        part = plan.parts.get()
        state.dispatch_part(
            part.pk, lease_id="lease", task_id="task", lease_until=timezone.now() + timedelta(minutes=1)
        )
        state.claim_part(part.pk, lease_id="lease")
        state.retry_part(part.pk, lease_id="lease", error_code="OVERSIZED", error_detail="")
        children = replan_failed_part(part.pk, lambda job: Distribution({1: 1}))
        self.assertEqual(len(children), 2)
        part.refresh_from_db()
        self.assertEqual(part.status, ExportPart.Status.SPLIT)

    def test_superseded_planner_cannot_publish_its_late_statistics(self):
        job = create_job()

        def supersede(current):
            ExportJob.objects.filter(pk=job.pk).update(planning_generation=current.planning_generation + 1)
            return Distribution({1: 1})

        self.assertIsNone(plan_job(job.pk, supersede))
        self.assertFalse(job.plans.exists())

    @override_settings(ASYNC_EXPORT_PLANNING_DEADLINE=1)
    def test_planning_watchdog_fails_jobs_past_overall_deadline(self):
        job = create_job(status="PLANNING", planning_started_at=timezone.now() - timedelta(seconds=2))
        plan_job(job.pk, Mock())
        job.refresh_from_db()
        self.assertEqual(job.error_code, "PLANNING_TIMEOUT")

    @override_settings(ASYNC_EXPORT_MAX_ATTEMPTS=1)
    def test_split_statistics_failure_backs_off_without_failing_job(self):
        job = create_job()
        plan = plan_job(job.pk, lambda job: Distribution({}))
        part = plan.parts.get()
        state.dispatch_part(
            part.pk, lease_id="lease", task_id="task", lease_until=timezone.now() + timedelta(minutes=1)
        )
        state.claim_part(part.pk, lease_id="lease")
        state.retry_part(part.pk, lease_id="lease", error_code="OVERSIZED", error_detail="")
        replan_failed_part(part.pk, Mock(side_effect=TimeoutError))
        job.refresh_from_db()
        part.refresh_from_db()
        self.assertEqual(job.status, "RUNNING")
        self.assertEqual(part.status, "FAILED")
        self.assertIsNotNone(part.next_retry_at)
        self.assertEqual(part.error_code, "STATISTICS_FAILED")
        self.assertEqual(part.attempts, 1)

    def test_query_factory_always_restores_context(self):
        for outcome in ["success", "failure", "cancel"]:
            with self.subTest(outcome=outcome):
                job = create_job()
                events = []

                @contextmanager
                def factory(current):
                    events.append("enter")
                    try:
                        if outcome == "cancel":
                            state.cancel_job(current.pk)
                        if outcome == "failure":
                            yield Mock(count=Mock(side_effect=PlanningError("INVALID_STATISTICS")))
                        else:
                            yield Distribution({})
                    finally:
                        events.append("exit")

                plan_job(job.pk, factory)
                self.assertEqual(events, ["enter", "exit"])
                job.refresh_from_db()
                self.assertEqual(job.status, {"success": "READY", "failure": "FAILED", "cancel": "CANCELED"}[outcome])

    def test_factory_setup_counts_towards_attempt_time_budget(self):
        job = create_job()
        clock = [0]
        statistics = Distribution({})

        @contextmanager
        def factory(current):
            clock[0] = 121
            yield statistics

        with patch("apps.log_search.export.planner.time.monotonic", side_effect=lambda: clock[0]):
            plan_job(job.pk, factory)
        job.refresh_from_db()
        self.assertEqual(job.error_code, "PLANNING_BUDGET_EXCEEDED")
        self.assertEqual(statistics.calls, [])

    @override_settings(ASYNC_EXPORT_PLANNING_DEADLINE=3)
    def test_statistics_timeout_is_capped_by_remaining_overall_deadline(self):
        now = timezone.now()
        job = create_job(planning_started_at=now - timedelta(seconds=2))
        stats = Distribution({})
        with patch("apps.log_search.export.state._now", return_value=now):
            plan_job(job.pk, lambda current: stats)
        self.assertEqual(stats.calls[0][-1], 1)
        job.refresh_from_db()
        self.assertEqual(job.status, "READY")

    def test_expired_planner_failure_cannot_overwrite_job(self):
        job = create_job()
        attempt = state.claim_planning(job_id=job.pk)
        ExportJob.objects.filter(pk=job.pk).update(planning_lease_until=timezone.now() - timedelta(seconds=1))
        attempt.fail(PlanningError("LATE_FAILURE"))
        job.refresh_from_db()
        self.assertEqual(job.status, "PLANNING")
        plan_job(job.pk, lambda current: Distribution({}))
        attempt.fail(PlanningError("EVEN_LATER_FAILURE"))
        job.refresh_from_db()
        self.assertEqual(job.status, "READY")
        self.assertEqual(job.planning_generation, 2)

    def test_stale_planning_commit_is_rejected_even_before_reclaim(self):
        job = create_job()
        attempt = state.claim_planning(job_id=job.pk)
        ExportJob.objects.filter(pk=job.pk).update(planning_lease_until=timezone.now() - timedelta(seconds=1))
        with self.assertRaises(state.StaleExportUpdateError):
            state.persist_plan(
                job.pk,
                attempt=attempt,
                query_hash=job.query_hash,
                target_rows=1,
                target_bytes=1,
                histogram_interval=1,
                parts=[state.PartSpec(1, 0, 60)],
            )
        self.assertFalse(job.plans.exists())

    def test_split_claim_is_fenced_by_next_retry_at(self):
        job = create_job()
        plan = plan_job(job.pk, lambda current: Distribution({}))
        part = plan.parts.get()
        # 一次已结束的 OVERSIZED 执行就是拆分起始状态。
        ExportJob.objects.filter(pk=job.pk).update(status="RUNNING")
        ExportPart.objects.filter(pk=part.pk).update(status="FAILED", attempts=3, error_code="OVERSIZED")
        self.assertIsNotNone(state.begin_split(part.pk))
        # 进行中的拆分不会被重复认领。
        self.assertIsNone(state.begin_split(part.pk))
        # 退避到期后可重新认领。
        ExportPart.objects.filter(pk=part.pk).update(next_retry_at=timezone.now() - timedelta(seconds=1))
        self.assertIsNotNone(state.begin_split(part.pk))


@override_settings(ASYNC_EXPORT_VERIFIED_QUERY_KINDS=["union"], ASYNC_EXPORT_QUERY_END_MODES={"union": "inclusive"})
class UnifyQueryStatisticsTest(TestCase):
    def adapter(self, **overrides):
        query = {
            "query_list": [
                {"reference_name": "a", "table_id": "A", "conditions": {"x": 1}},
                {"reference_name": "b", "table_id": "B"},
            ],
            "metric_merge": "a + b",
            "order_by": ["-time"],
        }
        job = create_job(
            query_kind="union",
            time_tick=1,
            query_snapshot={"unify_query": query, "time_units_per_second": 1000},
        )
        return UnifyQueryStatistics(
            job,
            raw=overrides.get("raw", Mock(return_value={"total": 10, "list": []})),
            reference=overrides.get("reference", Mock(return_value={"series": []})),
            project=overrides.get("project", lambda result: result["list"]),
        )

    def test_union_references_filters_and_inclusive_end_are_preserved(self):
        reference = Mock(return_value={"series": [{"values": [[0, 10]]}]})
        adapter = self.adapter(reference=reference)
        self.assertEqual(adapter.histogram(0, 30000, 30000, timeout=2), {0: 10})
        params = reference.call_args.args[0]
        self.assertEqual(params["metric_merge"], "a + b")
        self.assertEqual([q["reference_name"] for q in params["query_list"]], ["a", "b"])
        self.assertEqual(params["end_time"], "29999")
        self.assertEqual(params["slice_max"], 0)
        self.assertEqual(adapter.base["order_by"], ["-time"])

    def test_sample_uses_bound_projection_and_jsonl_encoding(self):
        adapter = self.adapter(
            raw=Mock(return_value={"list": [{"secret": "raw"}]}), project=lambda result: [{"secret": "***"}]
        )
        self.assertEqual(adapter.sample(0, 1000, 1, timeout=1), [b'{"secret":"***"}\n'])

    def test_partial_statistics_and_unverified_mode_are_rejected(self):
        adapter = self.adapter(raw=Mock(return_value={"total": 1, "partial": True}))
        with self.assertRaises(PlanningError):
            adapter.count(0, 1000, timeout=1)
        with self.assertRaises(PlanningError):
            UnifyQueryStatistics.checked({"result": False, "series": []})
        with override_settings(ASYNC_EXPORT_VERIFIED_QUERY_KINDS=[]), self.assertRaises(PlanningError):
            self.adapter()
