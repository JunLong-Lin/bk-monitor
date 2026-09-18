import hashlib
import base64
import json
import tempfile
import threading
from contextlib import contextmanager
from datetime import timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from unittest.mock import Mock, patch

import requests
from django.test import TestCase, override_settings
from django.utils import timezone
from qcloud_cos import CosConfig, CosS3Client
from qcloud_cos.cos_exception import CosServiceError

from apps.log_search.export import state
from apps.log_search.export.finalize import cleanup_export, finalize_export
from apps.log_search.export.models import ExportJob
from apps.log_search.export.storage import (
    BKRepoArtifactStore,
    BKRepoHttpClient,
    CosArtifactStore,
    artifact_prefix,
    bkrepo_artifact_store,
)
from apps.log_search.export.worker import Artifact, PartError, UnconfirmedQueryExit, run_part
from apps.tests.log_search.export_fixtures import create_job


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


class MemoryBKRepo:
    def __init__(self):
        self.objects = {}
        self.puts = 0
        self.put_effect = None

    @staticmethod
    def response(status_code, data=None, headers=None):
        response = Mock(status_code=status_code, headers=headers or {})
        response.json.return_value = data if data is not None else {"code": 0, "data": None}
        return response

    def head(self, key, timeout):
        if key not in self.objects:
            return self.response(404)
        body, checksum = self.objects[key]
        return self.response(
            200,
            headers={"Content-Length": str(len(body)), "X-Checksum-Sha256": checksum},
        )

    def put(self, key, stream, size, checksum, timeout):
        self.puts += 1
        if self.put_effect:
            return self.put_effect(key, stream, size, checksum, timeout)
        if key in self.objects:
            return self.response(409, {"code": 250107, "message": "exists"})
        body = stream.read()
        self.objects[key] = body, checksum
        return self.response(200)

    def delete(self, key, timeout):
        if key not in self.objects:
            return self.response(404)
        self.objects.pop(key)
        return self.response(200)


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

    def test_cos_signing_uses_committed_key(self):
        key = "exports/part.tar.gz"
        self.client.get_presigned_download_url = Mock(return_value="https://cos.example.test/signed")
        self.assertEqual(self.store.sign_download(key, 60), "https://cos.example.test/signed")
        self.client.get_presigned_download_url.assert_called_once_with(Bucket="bucket", Key=key, Expired=60)

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
                content_checksum=artifact.content_checksum,
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

    def test_retry_generation_uses_another_object_key(self):
        self.complete_parts()
        part = self.plan.parts.first()
        artifact = self.artifact(str(part.pk).encode())
        original_key = part.object_key
        part.dispatch_generation += 1
        retry_key = self.store.publish(part, artifact, lambda: 30)
        self.assertNotEqual(retry_key, original_key)
        self.assertIn(original_key, self.client.objects)
        self.assertIn(retry_key, self.client.objects)

    def test_ready_artifact_stays_recoverable_when_reuse_head_fails(self):
        self.complete_parts()
        part = self.plan.parts.first()
        artifact = self.artifact(str(part.pk).encode())
        with patch.object(self.store, "head", side_effect=TimeoutError()):
            with self.assertRaises(TimeoutError):
                self.store.publish(part, artifact, lambda: 30)
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
        self.assertEqual(cleanup_export(self.job.pk, self.store), 2)
        self.assertEqual(len(self.client.objects), 1)

    def test_success_objects_are_removed_after_expiry(self):
        self.complete_parts()
        artifact = self.artifact(b"orphan")
        self.store.publish_file(self.job, artifact_prefix(self.job) + "unused", artifact, lambda: 30)
        finalize_export(self.job.pk, self.store)
        self.assertEqual(cleanup_export(self.job.pk, self.store), 0)
        self.assertEqual(len(self.client.objects), 4)
        ExportJob.objects.filter(pk=self.job.pk).update(expires_at=timezone.now() - timedelta(seconds=1))
        self.assertEqual(cleanup_export(self.job.pk, self.store), 3)
        self.assertEqual(len(self.client.objects), 1)
        self.assertEqual(cleanup_export(self.job.pk, self.store), 0)

    def test_expired_cleanup_progresses_in_batches(self):
        self.complete_parts()
        finalize_export(self.job.pk, self.store)
        ExportJob.objects.filter(pk=self.job.pk).update(expires_at=timezone.now() - timedelta(seconds=1))
        self.assertEqual(cleanup_export(self.job.pk, self.store, limit=1), 1)
        self.assertEqual(cleanup_export(self.job.pk, self.store, limit=1), 1)
        self.assertEqual(cleanup_export(self.job.pk, self.store, limit=1), 1)
        self.assertEqual(cleanup_export(self.job.pk, self.store, limit=1), 0)
        self.assertEqual(self.client.objects, {})

    def test_unknown_upload_does_not_create_success_record(self):
        self.complete_parts()
        artifact = self.artifact(b"uncertain")
        with patch.object(self.client, "put_object", side_effect=TimeoutError()):
            with self.assertRaises(UnconfirmedQueryExit):
                self.store.publish_file(self.job, artifact_prefix(self.job) + "uncertain", artifact, lambda: 30)
        state.cancel_job(self.job.pk)
        self.assertEqual(cleanup_export(self.job.pk, self.store), 2)

    def test_delete_response_loss_retries_without_republishing(self):
        self.complete_parts()
        state.cancel_job(self.job.pk)
        delete = self.client.delete_object

        def lost_response(**kwargs):
            delete(**kwargs)
            raise TimeoutError()

        with patch.object(self.client, "delete_object", side_effect=lost_response):
            self.assertEqual(cleanup_export(self.job.pk, self.store), 0)
        self.assertEqual(cleanup_export(self.job.pk, self.store), 2)
        self.job.refresh_from_db()
        self.assertIsNotNone(self.job.artifacts_cleaned_at)

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
        from apps.log_search.export.worker import PartError

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

    def test_failed_delete_is_retried(self):
        self.complete_parts()
        state.cancel_job(self.job.pk)
        with patch.object(self.client, "delete_object", side_effect=TimeoutError()):
            self.assertEqual(cleanup_export(self.job.pk, self.store), 0)
        self.assertEqual(cleanup_export(self.job.pk, self.store), 2)
        self.job.refresh_from_db()
        self.assertIsNotNone(self.job.artifacts_cleaned_at)

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
            from apps.log_search.export.storage import StoredArtifact

            record = StoredArtifact(
                key, artifact.checksum, artifact.content_checksum, artifact.compressed_size, store.storage_id
            )
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

    def test_worker_upload_timeout_retries_part(self):
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
        self.assertEqual(part.status, "WAITING")
        self.assertEqual(part.error_code, "UPLOAD_EXIT_UNCONFIRMED")
        self.assertEqual(part.lease_id, "")
        budget.release.assert_called_once()


@override_settings(
    ASYNC_EXPORT_LEASE_SECONDS=60,
    ASYNC_EXPORT_BKREPO_TIMEOUT=15,
    ASYNC_EXPORT_BKREPO_PUT_ATTEMPTS=3,
)
class BKRepoArtifactStoreTest(TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.client = MemoryBKRepo()
        self.store = BKRepoArtifactStore(self.client, "bkrepo-storage", "export-root")
        self.job = create_job()
        state.begin_planning(self.job.pk)
        self.plan = state.persist_plan(
            self.job.pk,
            attempt=state.claim_planning(job_id=self.job.pk),
            query_hash=self.job.query_hash,
            target_rows=100,
            target_bytes=100,
            histogram_interval=60,
            parts=[state.PartSpec(1, 0, 60, 1, 10)],
        )

    def artifact(self, content=b"bkrepo-fixture"):
        path = Path(self.temp.name) / "artifact"
        path.write_bytes(content)
        checksum = hashlib.sha256(content).hexdigest()
        return Artifact(path, 1, len(content), len(content), checksum, checksum)

    def start_part(self):
        part = self.plan.parts.get()
        state.dispatch_part(
            part.pk, lease_id="owner", task_id="task", lease_until=timezone.now() + timedelta(seconds=60)
        )
        part.refresh_from_db()
        credentials = dict(generation=part.dispatch_generation, lease_id="owner")
        state.claim_part(part.pk, **credentials, worker_id="worker")
        state.begin_part_upload(part.pk, **credentials)
        part.plan.job.refresh_from_db()
        return part, credentials

    def complete_part(self, content=b"bkrepo-fixture"):
        part, credentials = self.start_part()
        artifact = self.artifact(content)
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
        return part, artifact

    @override_settings(ASYNC_EXPORT_ARTIFACT_RETENTION_SECONDS=90)
    def test_part_manifest_and_cleanup_use_bkrepo(self):
        part, artifact = self.complete_part()
        part.refresh_from_db()
        puts = self.client.puts
        self.assertEqual(self.store.publish(part, artifact, lambda: 30), part.object_key)
        self.assertEqual(self.client.puts, puts)

        result = finalize_export(self.job.pk, self.store)
        self.assertEqual(result.status, ExportJob.Status.SUCCESS)
        manifest_key = self.store._key(result.manifest_object_key)
        manifest = json.loads(self.client.objects[manifest_key][0])
        self.assertEqual(manifest["actual_total"], 1)
        self.assertEqual(manifest["expires_after_success_seconds"], 90)
        self.assertLessEqual((result.expires_at - result.completed_at).total_seconds(), 90)
        ExportJob.objects.filter(pk=self.job.pk).update(expires_at=timezone.now() - timedelta(seconds=1))
        self.assertEqual(cleanup_export(self.job.pk, self.store), 2)
        self.assertEqual(self.client.objects, {})

    def test_lost_put_response_is_reconciled_by_head(self):
        part, _credentials = self.start_part()
        artifact = self.artifact()

        def stored_then_timeout(key, stream, size, checksum, timeout):
            self.client.objects[key] = stream.read(), checksum
            raise requests.Timeout()

        self.client.put_effect = stored_then_timeout
        key = self.store.publish(part, artifact, lambda: 30)
        self.assertTrue(key.startswith(artifact_prefix(self.job)))
        self.assertEqual(self.client.puts, 1)

    def test_unconfirmed_put_has_no_persistent_marker(self):
        part, _credentials = self.start_part()
        artifact = self.artifact()

        def timeout(*args):
            raise requests.Timeout()

        self.client.put_effect = timeout
        with self.assertRaisesMessage(UnconfirmedQueryExit, "UPLOAD_EXIT_UNCONFIRMED"):
            self.store.publish(part, artifact, lambda: 30)
        self.assertEqual(self.client.puts, 3)

    def test_existing_wrong_object_is_not_overwritten(self):
        part, _credentials = self.start_part()
        artifact = self.artifact()
        key = f"{artifact_prefix(self.job)}{self.plan.plan_version}/{part.pk}/{part.dispatch_generation}/{artifact.checksum}.tar.gz"
        self.client.objects[self.store._key(key)] = b"wrong", "bad-checksum"
        with self.assertRaisesMessage(PartError, "ARTIFACT_VERIFICATION_FAILED"):
            self.store.publish(part, artifact, lambda: 30)
        self.assertEqual(self.client.puts, 0)
        self.assertEqual(self.client.objects[self.store._key(key)][0], b"wrong")

    @override_settings(
        BKREPO_ENDPOINT_URL="https://repo.example.test",
        BKREPO_USERNAME="user",
        BKREPO_PASSWORD="password",
        BKREPO_PROJECT="project",
        BKREPO_BUCKET="bucket",
        BKREPO_LOCATION="tenant-root",
    )
    def test_factory_reuses_existing_bkrepo_configuration(self):
        store = bkrepo_artifact_store()
        self.assertIsInstance(store.client, BKRepoHttpClient)
        self.assertEqual(store.key_prefix, "tenant-root")
        self.assertEqual(store.client.endpoint_url, "https://repo.example.test")
        self.assertEqual(store.client.project, "project")
        self.assertEqual(store.client.bucket, "bucket")

    def test_bkrepo_signing_uses_temporary_download_token(self):
        session = Mock()
        session.post.return_value = Mock(
            status_code=200,
            json=lambda: {"code": 0, "data": [{"url": "https://repo.example.test/temporary/token"}]},
        )
        client = BKRepoHttpClient("https://repo.example.test", "project", "bucket", "user", "password", session)
        self.assertEqual(client.sign_download("root/file", 60, 5), "https://repo.example.test/temporary/token")
        self.assertEqual(session.post.call_args.kwargs["json"]["expireSeconds"], 60)
        self.assertEqual(session.post.call_args.kwargs["json"]["type"], "DOWNLOAD")
        session.post.return_value = Mock(status_code=404)
        with self.assertRaisesMessage(PartError, "BKREPO_SIGN_FAILED"):
            client.sign_download("root/file", 60, 5)

    def test_http_client_encodes_keys_and_sends_immutable_checksum_headers(self):
        received = []

        class Endpoint(BaseHTTPRequestHandler):
            def do_PUT(self):
                body = self.rfile.read(int(self.headers["Content-Length"]))
                received.append((self.path, body, dict(self.headers)))
                self.send_response(200)
                payload = b'{"code":0,"data":null}'
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, *args):
                pass

        endpoint = HTTPServer(("127.0.0.1", 0), Endpoint)
        thread = threading.Thread(target=endpoint.serve_forever, daemon=True)
        thread.start()
        try:
            client = BKRepoHttpClient(
                f"http://127.0.0.1:{endpoint.server_port}", "project", "bucket", "user", "password"
            )
            response = client.put("space/a file", b"wire", 4, "checksum", 2)
            self.assertEqual(response.json()["code"], 0)
            path, body, headers = received[0]
            self.assertEqual(path, "/generic/project/bucket/space/a%20file")
            self.assertEqual(body, b"wire")
            self.assertEqual(headers["X-BKREPO-OVERWRITE"], "false")
            self.assertEqual(headers["X-BKREPO-SHA256"], "checksum")
            self.assertTrue(headers["Authorization"].startswith("Basic "))
        finally:
            endpoint.shutdown()
            endpoint.server_close()
            thread.join(timeout=2)
