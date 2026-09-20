import json
import tarfile
import tempfile
from contextlib import contextmanager
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from django.test import SimpleTestCase, TestCase, override_settings
from django.utils import timezone

from apps.log_search.export import state
from apps.log_search.export.models import ExportPart
from apps.log_search.export.worker import (
    LocalArtifactStore,
    PartError,
    RawWithScrollReader,
    WorkerPolicy,
    run_part,
)
from apps.tests.log_search.export_fixtures import create_job


def query_with_pages(pages):
    query = Mock()
    query.request.side_effect = lambda start, end: {"start_time": start, "end_time": end}
    query.read.side_effect = pages
    query.project.side_effect = lambda response: response["list"]
    return query


class ReaderTest(SimpleTestCase):
    def reader(self, pages, **policy):
        query = query_with_pages(pages)
        reader = RawWithScrollReader(
            query, SimpleNamespace(start_time=1, end_time=2), WorkerPolicy(**policy), Mock(return_value=10)
        )
        return reader, query

    def test_empty_intermediate_and_nonempty_final_batch(self):
        reader, query = self.reader(
            [
                {"list": [], "done": False},
                {"list": [{"text": "末批"}], "done": True},
            ]
        )
        self.assertEqual(list(reader), [[], [{"text": "末批"}]])
        self.assertTrue(reader.exhausted)
        self.assertEqual([c.args[0]["clear_cache"] for c in query.read.call_args_list], [True, False])
        self.assertTrue(all(c.args[0]["slice_max"] == 0 for c in query.read.call_args_list))

    def test_zero_result_requires_explicit_done(self):
        reader, _ = self.reader([{"list": [], "done": True}])
        self.assertEqual(list(reader), [[]])
        self.assertTrue(reader.exhausted)

    def test_bad_or_partial_response_never_exhausts(self):
        for response in (
            {"list": []},
            {"list": [], "done": "true"},
            {"list": [], "done": True, "partial": True},
            {"list": [], "done": True, "errors": ["failed"]},
            {"list": {}, "done": True},
            {"list": [{}, {}], "done": True},
        ):
            with self.subTest(response=response):
                reader, _ = self.reader([response], batch_rows=1)
                with self.assertRaises(PartError):
                    list(reader)
                self.assertFalse(reader.exhausted)

    def test_empty_and_call_budgets_fail_explicitly(self):
        for pages, policy, code in (
            ([{"list": [], "done": False}] * 3, {}, "SCROLL_NO_PROGRESS"),
            ([{"list": [{}], "done": False}], {"max_calls": 1}, "SCROLL_CALL_BUDGET_EXCEEDED"),
        ):
            reader, _ = self.reader(pages, **policy)
            with self.assertRaisesMessage(PartError, code):
                list(reader)

    def test_network_error_does_not_retry_a_page(self):
        reader, query = self.reader([TimeoutError(), {"list": [], "done": True}])
        with self.assertRaisesMessage(PartError, "QUERY_FAILED"):
            list(reader)
        self.assertEqual(query.read.call_count, 1)

    def test_projection_must_preserve_rows(self):
        reader, query = self.reader([{"list": [{}], "done": True}])
        query.project.side_effect = lambda response: []
        with self.assertRaisesMessage(PartError, "INVALID_PROJECTED_ROWS"):
            list(reader)


@override_settings(ASYNC_EXPORT_LEASE_SECONDS=60)
class PartWorkerTest(TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.store = LocalArtifactStore(self.temp.name)
        self.budget = Mock(renew=Mock(return_value=True))
        self.job = create_job()
        state.begin_planning(self.job.pk)
        self.plan = state.persist_plan(
            self.job.pk,
            attempt=state.claim_planning(job_id=self.job.pk),
            query_hash=self.job.query_hash,
            target_rows=1,
            target_bytes=100,
            histogram_interval=30,
            parts=[state.PartSpec(1, 0, 60, 0, 0)],
        )
        self.part = self.plan.parts.get()
        state.dispatch_part(
            self.part.pk,
            lease_id="owner",
            task_id="task",
            lease_until=timezone.now() + timedelta(seconds=30),
        )
        self.part.refresh_from_db()

    def execute(self, query, **kwargs):
        @contextmanager
        def factory(job):
            try:
                yield query
            finally:
                query.close()

        run_part(
            self.part.pk,
            lease_id="owner",
            query_factory=factory,
            budget=self.budget,
            store=self.store,
            **kwargs,
        )
        self.part.refresh_from_db()

    def test_complete_local_archive_ignores_estimated_row_limit(self):
        query = query_with_pages(
            [
                {"list": [{"log": "one"}], "done": False},
                {"list": [{"log": "末行"}], "done": True},
            ]
        )
        self.execute(query)
        self.assertEqual(self.part.status, ExportPart.Status.SUCCESS)
        self.assertEqual(self.part.actual_rows, 2)
        with tarfile.open(Path(self.temp.name) / self.part.object_key.removeprefix("local:")) as bundle:
            rows = [json.loads(line) for line in bundle.extractfile("logs.log")]
        self.assertEqual(rows, [{"log": "one"}, {"log": "末行"}])
        query.close.assert_called_once()
        self.budget.release.assert_called_once()
        self.job.refresh_from_db()
        self.assertEqual(self.job.status, "RUNNING")  # Manifest 单独处理。
        self.assertEqual(self.job.actual_total, 2)
        self.execute(query)
        self.assertEqual(query.read.call_count, 2)  # 重复投递不发起任何 I/O。

    def test_packaging_stage_is_persisted_before_compression(self):
        original_open = tarfile.open
        observed = []

        def open_archive(*args, **kwargs):
            if kwargs.get("mode") == "w|":
                self.part.refresh_from_db()
                observed.append(self.part.stage)
            return original_open(*args, **kwargs)

        with patch("apps.log_search.export.worker.tarfile.open", side_effect=open_archive):
            self.execute(query_with_pages([{"list": [{"log": "one"}], "done": True}]))
        self.assertEqual(observed, ["PACKAGE"])
        self.assertEqual(self.part.status, ExportPart.Status.SUCCESS)

    def test_query_timeout_retries_part_without_publishing(self):
        query = query_with_pages([TimeoutError()])
        self.execute(query)
        self.assertEqual(self.part.status, ExportPart.Status.WAITING)
        self.assertEqual(self.part.error_code, "QUERY_FAILED")
        self.assertEqual(self.part.lease_id, "")
        self.budget.release.assert_called_once()
        self.assertEqual(list(Path(self.temp.name).rglob("*.tar.gz")), [])
        query.close.assert_called_once()

    def test_resource_limit_goes_directly_to_split_without_repeating_io(self):
        self.execute(query_with_pages([{"list": [{"log": "large"}], "done": True}]), policy=WorkerPolicy(max_bytes=1))
        self.assertEqual(self.part.status, ExportPart.Status.FAILED)
        self.assertEqual(self.part.error_code, "OVERSIZED")
        self.assertEqual(self.part.object_key, "")
        self.assertIsNone(self.part.next_retry_at)
        self.budget.release.assert_called_once()

    def test_lost_redis_ownership_prevents_query(self):
        self.budget.renew.return_value = False
        query = query_with_pages([])
        self.execute(query)
        query.read.assert_not_called()
        self.assertEqual(self.part.status, ExportPart.Status.WAITING)

    def test_cancel_after_response_discards_attempt(self):
        query = query_with_pages([])

        def read(*args, **kwargs):
            state.cancel_job(self.job.pk)
            return {"list": [{"log": "must not commit"}], "done": True}

        query.read.side_effect = read
        self.execute(query)
        self.assertEqual(self.part.status, ExportPart.Status.CANCELED)
        self.assertEqual(list(Path(self.temp.name).rglob("*.tar.gz")), [])

    def test_failed_upload_does_not_commit_part(self):
        self.store.publish = Mock(side_effect=OSError("disk failure"))
        self.execute(query_with_pages([{"list": [], "done": True}]))
        self.assertEqual(self.part.status, ExportPart.Status.WAITING)
        self.assertEqual(self.part.object_key, "")

    def test_lease_expires_during_query_response_is_not_committed(self):
        query = query_with_pages([])

        def read(*args, **kwargs):
            ExportPart.objects.filter(pk=self.part.pk).update(lease_until=timezone.now() - timedelta(seconds=1))
            return {"list": [{}], "done": True}

        query.read.side_effect = read
        self.execute(query)
        self.assertEqual(self.part.status, ExportPart.Status.WAITING)
        self.assertEqual(self.part.object_key, "")
        self.assertEqual(list(Path(self.temp.name).rglob("*.tar.gz")), [])

    def test_missing_store_stops_control_before_reserving(self):
        from apps.log_search.tasks.sharded_export import coordinate_sharded_exports, execute_sharded_export_part

        with (
            override_settings(
                ASYNC_EXPORT_CONTROL_ENABLED=True,
                ASYNC_EXPORT_PART_TASK=execute_sharded_export_part.name,
                ASYNC_EXPORT_ARTIFACT_STORE_FACTORY="",
            ),
            patch("apps.log_search.tasks.sharded_export.get_redis_connection") as connect,
            patch("apps.log_search.tasks.sharded_export.logger") as logger,
        ):
            coordinate_sharded_exports()
            connect.assert_not_called()
            logger.warning.assert_called_once()

    def test_third_execution_failure_is_terminal(self):
        ExportPart.objects.filter(pk=self.part.pk).update(attempts=2)
        self.execute(query_with_pages([{"list": [], "done": False}] * 3))
        self.assertEqual(self.part.status, ExportPart.Status.FAILED)
        self.assertEqual(self.part.attempts, 3)

    def test_expired_generation_does_not_start_io(self):
        ExportPart.objects.filter(pk=self.part.pk).update(lease_until=timezone.now() - timedelta(seconds=1))
        query = query_with_pages([])
        self.execute(query)
        query.read.assert_not_called()
        self.assertEqual(self.part.attempts, 0)
