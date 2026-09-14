import hashlib
import base64
import json
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from contextlib import contextmanager
from datetime import timedelta
from pathlib import Path
from unittest.mock import Mock, patch

from django.test import TestCase, override_settings
from django.utils import timezone
from qcloud_cos.cos_exception import CosServiceError
from qcloud_cos import CosConfig, CosS3Client

from apps.log_search import export_state as state
from apps.log_search.export_finalize import cleanup_export, finalize_export
from apps.log_search.export_models import ExportJob
from apps.log_search.export_storage import CosArtifactStore, artifact_prefix
from apps.log_search.export_worker import Artifact, UnconfirmedQueryExit, run_part
from apps.log_search.tests.export_fixtures import create_job


def cos_error(code=404, name="NoSuchKey"):
    return CosServiceError("HEAD", {"code": name, "message": "fixture"}, code)


class MemoryCos:
    def __init__(self):
        self.objects = {}
        self.puts = 0

    def head_object(self, *, Bucket, Key):
        if Key not in self.objects:
            raise cos_error()
        body, metadata = self.objects[Key]
        return {"Content-Length": str(len(body)), **metadata}

    def put_object(self, *, Bucket, Key, Body, Metadata, **kwargs):
        self.puts += 1
        chunks = []
        while data := Body.read(64 * 1024):
            chunks.append(data)
        self.objects[Key] = (b"".join(chunks), Metadata)
        return {"ETag": "not-a-sha256"}

    def delete_object(self, *, Bucket, Key):
        self.objects.pop(Key, None)


@override_settings(ASYNC_EXPORT_LEASE_SECONDS=60)
class ArtifactFlowTest(TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.client = MemoryCos()
        self.store = CosArtifactStore(lambda timeout: self.client, "bucket", "storage")
        self.job = create_job()
        state.begin_planning(self.job.pk)
        self.plan = state.persist_plan(
            self.job.pk,
            attempt=state.claim_planning(job_id=self.job.pk),
            query_hash=self.job.query_hash,
            target_rows=100,
            target_bytes=100,
            histogram_interval=30,
            parts=[state.PartSpec(1, 0, 30, 1, 10), state.PartSpec(2, 30, 60, 1, 10)],
        )

    def artifact(self, content=b"fixture"):
        path = Path(self.temp.name) / "artifact"
        path.write_bytes(content)
        checksum = hashlib.sha256(content).hexdigest()
        return Artifact(path, 1, len(content), len(content), checksum, checksum)

    def complete_parts(self):
        for part in self.plan.parts.order_by("part_no"):
            state.dispatch_part(
                part.pk, lease_id="owner", task_id="task", lease_until=timezone.now() + timedelta(seconds=60)
            )
            part.refresh_from_db()
            credentials = dict(generation=part.dispatch_generation, lease_id="owner")
            state.claim_part(part.pk, **credentials, worker_id="worker")
            state.begin_part_upload(part.pk, **credentials)
            part.plan.job.refresh_from_db()
            artifact = self.artifact(str(part.pk).encode())
            key = self.store.publish(part, artifact, lambda: 30)
            state.complete_part(
                part.pk,
                **credentials,
                actual_rows=1,
                actual_bytes=artifact.size,
                compressed_bytes=artifact.compressed_size,
                object_key=key,
                checksum=artifact.checksum,
            )
        self.job.refresh_from_db()

    def test_manifest_commit_and_expiry_uses_success_time(self):
        self.complete_parts()
        result = finalize_export(self.job.pk, self.store)
        self.assertEqual(result.status, ExportJob.Status.SUCCESS)
        self.assertEqual(result.expires_at - result.completed_at, timedelta(hours=24))
        manifest = json.loads(self.client.objects[result.manifest_object_key][0])
        self.assertEqual(manifest["actual_total"], 2)
        self.assertEqual([p["start_time"] for p in manifest["parts"]], [0, 30])
        self.assertNotIn("expires_at", manifest)
        self.assertEqual(manifest["expires_after_success_seconds"], 86400)
        puts = self.client.puts
        finalize_export(self.job.pk, self.store)
        self.assertEqual(self.client.puts, puts)
        self.assertEqual(cleanup_export(self.job.pk, self.store), 0)

    def test_existing_immutable_artifact_reused(self):
        self.complete_parts()
        part = self.plan.parts.first()
        artifact = self.artifact(str(part.pk).encode())
        puts = self.client.puts
        self.assertEqual(self.store.publish(part, artifact, lambda: 30), part.object_key)
        self.assertEqual(self.client.puts, puts)

    def test_ready_artifact_stays_recoverable_when_reuse_head_fails(self):
        self.complete_parts()
        part = self.plan.parts.first()
        artifact = self.artifact(str(part.pk).encode())
        with patch.object(self.store, "head", side_effect=TimeoutError()):
            with self.assertRaises(TimeoutError):
                self.store.publish(part, artifact, lambda: 30)
        self.assertEqual(self.job.artifacts.get(object_key=part.object_key).status, "READY")
        self.assertEqual(self.store.publish(part, artifact, lambda: 30), part.object_key)

    def test_cancel_during_manifest_upload_fences_commit_then_cleans(self):
        self.complete_parts()
        publish = self.store.publish_file

        def cancel_after_upload(*args):
            key = publish(*args)
            state.cancel_job(self.job.pk)
            return key

        with patch.object(self.store, "publish_file", side_effect=cancel_after_upload):
            finalize_export(self.job.pk, self.store)
        self.job.refresh_from_db()
        self.assertEqual(self.job.status, ExportJob.Status.CANCELED)
        self.assertFalse(self.job.manifest_object_key)
        self.assertEqual(cleanup_export(self.job.pk, self.store), 3)
        self.assertEqual(self.client.objects, {})

    def test_orphans_only_before_expiry_all_objects_after_expiry(self):
        self.complete_parts()
        artifact = self.artifact(b"orphan")
        self.store.publish_file(self.job, artifact_prefix(self.job) + "unused", artifact, lambda: 30)
        finalize_export(self.job.pk, self.store)
        self.assertEqual(cleanup_export(self.job.pk, self.store), 1)
        self.assertEqual(len(self.client.objects), 3)
        ExportJob.objects.filter(pk=self.job.pk).update(expires_at=timezone.now() - timedelta(seconds=1))
        self.assertEqual(cleanup_export(self.job.pk, self.store), 3)
        self.assertEqual(cleanup_export(self.job.pk, self.store), 0)

    def test_unknown_upload_is_not_cleaned_or_retried_as_idle(self):
        self.complete_parts()
        artifact = self.artifact(b"uncertain")
        with patch.object(self.client, "put_object", side_effect=TimeoutError()):
            with self.assertRaises(UnconfirmedQueryExit):
                self.store.publish_file(self.job, artifact_prefix(self.job) + "uncertain", artifact, lambda: 30)
        self.assertTrue(self.job.artifacts.filter(status="UPLOADING").exists())
        state.cancel_job(self.job.pk)
        self.assertEqual(cleanup_export(self.job.pk, self.store), 0)

    def test_delete_response_loss_retries_without_republishing(self):
        self.complete_parts()
        state.cancel_job(self.job.pk)
        delete = self.client.delete_object

        def lost_response(**kwargs):
            delete(**kwargs)
            raise TimeoutError()

        with patch.object(self.client, "delete_object", side_effect=lost_response):
            self.assertEqual(cleanup_export(self.job.pk, self.store), 0)
        self.assertEqual(self.job.artifacts.filter(status="DELETING").count(), 2)
        self.assertEqual(cleanup_export(self.job.pk, self.store), 2)

    def test_manifest_failure_retries_only_finalization(self):
        self.complete_parts()
        with patch.object(self.store, "verify", side_effect=OSError()):
            finalize_export(self.job.pk, self.store)
        self.assertEqual(list(self.plan.parts.values_list("attempts", flat=True)), [1, 1])
        ExportJob.objects.filter(pk=self.job.pk).update(next_finalization_at=timezone.now())
        self.assertEqual(finalize_export(self.job.pk, self.store).status, "SUCCESS")
        self.assertEqual(list(self.plan.parts.values_list("attempts", flat=True)), [1, 1])

    def test_bad_head_checksum_never_produces_manifest_success(self):
        self.complete_parts()
        part = self.plan.parts.first()
        self.client.objects[part.object_key][1]["x-cos-meta-sha256"] = "bad"
        finalize_export(self.job.pk, self.store)
        self.job.refresh_from_db()
        self.assertEqual(self.job.error_code, "ARTIFACT_VERIFICATION_FAILED")
        self.assertNotEqual(self.job.status, "SUCCESS")

    def test_early_finalize_does_not_consume_retries(self):
        finalize_export(self.job.pk, self.store)
        self.job.refresh_from_db()
        self.assertEqual(self.job.finalization_attempts, 0)

    def test_local_package_change_is_rejected_before_put(self):
        from apps.log_search.export_worker import PartError

        self.complete_parts()
        artifact = self.artifact(b"original")
        artifact.path.write_bytes(b"modified")
        puts = self.client.puts
        with self.assertRaisesMessage(PartError, "LOCAL_ARTIFACT_CHANGED"):
            self.store.publish_file(self.job, artifact_prefix(self.job) + "changed", artifact, lambda: 30)
        self.assertEqual(self.client.puts, puts)

    def test_bounded_service_error_retry_reuses_local_package(self):
        self.complete_parts()
        artifact = self.artifact(b"retry")
        put = self.client.put_object
        calls = 0

        def transient(**kwargs):
            nonlocal calls
            calls += 1
            if calls < 3:
                raise cos_error(503, "SlowDown")
            return put(**kwargs)

        with patch.object(self.client, "put_object", side_effect=transient):
            self.store.publish_file(self.job, artifact_prefix(self.job) + "retry", artifact, lambda: 30)
        self.assertEqual(calls, 3)

    def test_failed_delete_does_not_starve_later_objects(self):
        self.complete_parts()
        state.cancel_job(self.job.pk)
        with patch.object(self.client, "delete_object", side_effect=TimeoutError()):
            self.assertEqual(cleanup_export(self.job.pk, self.store, limit=1), 0)
        self.assertEqual(cleanup_export(self.job.pk, self.store, limit=1), 1)
        self.assertEqual(self.job.artifacts.filter(status="DELETING").count(), 1)

    def test_finalization_attempts_exhaust_without_reexport(self):
        self.complete_parts()
        with patch.object(self.store, "verify", side_effect=OSError()):
            for _ in range(4):
                ExportJob.objects.filter(pk=self.job.pk).update(next_finalization_at=timezone.now())
                finalize_export(self.job.pk, self.store)
        self.job.refresh_from_db()
        self.assertEqual(self.job.status, "FAILED")
        self.assertEqual(self.job.finalization_attempts, 3)
        self.assertEqual(list(self.plan.parts.values_list("attempts", flat=True)), [1, 1])

    def test_sdk_http_stream_md5_metadata_head_and_delete(self):
        self.complete_parts()
        objects = {}
        received = []

        class Endpoint(BaseHTTPRequestHandler):
            def do_HEAD(self):
                found = objects.get(self.path)
                self.send_response(200 if found else 404)
                self.send_header("Content-Length", str(len(found[0])) if found else "0")
                if found:
                    for key, value in found[1].items():
                        self.send_header(key, value)
                self.end_headers()

            def do_PUT(self):
                body = self.rfile.read(int(self.headers["Content-Length"]))
                metadata = {k.lower(): v for k, v in self.headers.items() if k.lower().startswith("x-cos-meta-")}
                objects[self.path] = body, metadata
                received.append((body, self.headers["Content-MD5"]))
                self.send_response(200)
                self.send_header("Content-Length", "0")
                self.end_headers()

            def do_DELETE(self):
                objects.pop(self.path, None)
                self.send_response(204)
                self.end_headers()

            def log_message(self, *args):
                pass

        endpoint = HTTPServer(("127.0.0.1", 0), Endpoint)
        thread = threading.Thread(target=endpoint.serve_forever, daemon=True)
        thread.start()
        try:
            sdk = CosS3Client(
                CosConfig(
                    Region="test",
                    SecretId="fixture",
                    SecretKey="fixture",
                    Scheme="http",
                    IP="127.0.0.1",
                    Port=endpoint.server_port,
                    Timeout=2,
                ),
                retry=0,
            )
            store = CosArtifactStore(lambda timeout: sdk, "fixture-1250000000", "wire")
            artifact = self.artifact(b"sdk-wire-data")
            key = store.publish_file(self.job, artifact_prefix(self.job) + "wire", artifact, lambda: 2)
            record = self.job.artifacts.get(object_key=key)
            store.verify(record, lambda: 2)
            self.assertEqual(
                received, [(b"sdk-wire-data", base64.b64encode(hashlib.md5(b"sdk-wire-data").digest()).decode())]
            )
            store.delete(record, lambda: 2)
            self.assertEqual(objects, {})
        finally:
            endpoint.shutdown()
            endpoint.server_close()
            thread.join(timeout=2)

    def test_worker_cos_path_then_manifest(self):
        @contextmanager
        def query(job):
            yield Mock(
                request=Mock(return_value={}),
                read=Mock(return_value={"list": [{"log": "real package"}], "done": True}),
                project=lambda response: response["list"],
            )

        for part in self.plan.parts.order_by("pk"):
            state.dispatch_part(
                part.pk, lease_id="owner", task_id="task", lease_until=timezone.now() + timedelta(seconds=60)
            )
            part.refresh_from_db()
            run_part(
                part.pk,
                generation=part.dispatch_generation,
                lease_id="owner",
                query_factory=query,
                budget=Mock(renew=Mock(return_value=True)),
                store=self.store,
            )
        self.assertEqual(finalize_export(self.job.pk, self.store).actual_total, 2)

    def test_worker_upload_timeout_retains_part_and_artifact_ownership(self):
        @contextmanager
        def query(job):
            yield Mock(request=Mock(return_value={}), read=Mock(return_value={"list": [], "done": True}))

        part = self.plan.parts.first()
        state.dispatch_part(
            part.pk, lease_id="owner", task_id="task", lease_until=timezone.now() + timedelta(seconds=60)
        )
        part.refresh_from_db()
        budget = Mock(renew=Mock(return_value=True))
        with patch.object(self.client, "put_object", side_effect=TimeoutError()):
            run_part(
                part.pk,
                generation=part.dispatch_generation,
                lease_id="owner",
                query_factory=query,
                budget=budget,
                store=self.store,
            )
        part.refresh_from_db()
        self.assertEqual(part.status, "UPLOADING")
        self.assertEqual(part.error_code, "UPLOAD_EXIT_UNCONFIRMED")
        self.assertEqual(part.lease_id, "owner")
        self.assertEqual(self.job.artifacts.get().status, "UPLOADING")
        budget.release.assert_not_called()
