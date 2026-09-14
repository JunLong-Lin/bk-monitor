"""Registered immutable COS artifacts, with no upload-time expiry."""

import base64
import hashlib
import json

from django.conf import settings
from django.db import transaction
from qcloud_cos import CosConfig, CosS3Client
from qcloud_cos.cos_exception import CosServiceError

from apps.log_search.export_models import ExportArtifact, ExportJob
from apps.log_search.export_worker import CheckedFile, PartError, UnconfirmedQueryExit


def artifact_prefix(job):
    scope = json.dumps([settings.ASYNC_EXPORT_NAMESPACE, job.bk_tenant_id, job.space_uid], separators=(",", ":"))
    return f"exports/{hashlib.sha256(scope.encode()).hexdigest()}/{job.pk}/"


class CosArtifactStore:
    def __init__(self, client_factory, bucket, storage_id):
        if any(
            type(value) is not int or value <= 0
            for value in (settings.ASYNC_EXPORT_COS_TIMEOUT, settings.ASYNC_EXPORT_COS_PUT_ATTEMPTS)
        ):
            raise ValueError("COS timeout and attempt budgets must be positive integers")
        self.client_factory, self.bucket, self.storage_id = client_factory, bucket, storage_id

    def head(self, key, guard):
        client = self.client_factory(min(guard(), settings.ASYNC_EXPORT_COS_TIMEOUT))
        try:
            return client.head_object(Bucket=self.bucket, Key=key)
        except CosServiceError as error:
            # HEAD commonly has no XML body, so SDK error code may be Unknown.
            if error.get_status_code() == 404 and error.get_error_code() != "NoSuchBucket":
                return None
            raise

    def verify(self, record, guard):
        if record.storage_id != self.storage_id:
            raise PartError("ARTIFACT_STORAGE_CHANGED")
        metadata = self.head(record.object_key, guard)
        if metadata is None:
            raise PartError("ARTIFACT_MISSING")
        metadata = {k.lower(): str(v) for k, v in metadata.items()}
        if (
            metadata.get("content-length") != str(record.size)
            or metadata.get("x-cos-meta-sha256") != record.checksum
            or metadata.get("x-cos-meta-content-sha256", "") != record.content_checksum
        ):
            raise PartError("ARTIFACT_VERIFICATION_FAILED")

    def publish(self, part, artifact, guard):
        key = f"{artifact_prefix(part.plan.job)}{part.plan.plan_version}/{part.pk}/{artifact.checksum}.tar.gz"
        return self.publish_file(part.plan.job, key, artifact, guard)

    def publish_file(self, job, key, artifact, guard):
        guard()
        with transaction.atomic():
            current = ExportJob.objects.select_for_update().get(pk=job.pk)
            if current.status != ExportJob.Status.RUNNING or not key.startswith(artifact_prefix(current)):
                raise PartError("JOB_STOPPED")
            record, created = ExportArtifact.objects.get_or_create(
                object_key=key,
                defaults=dict(
                    job=current,
                    checksum=artifact.checksum,
                    content_checksum=artifact.content_checksum,
                    size=artifact.compressed_size,
                    storage_id=self.storage_id,
                ),
            )
            if (record.job_id, record.checksum, record.size, record.storage_id, record.content_checksum) != (
                job.pk,
                artifact.checksum,
                artifact.compressed_size,
                self.storage_id,
                artifact.content_checksum,
            ):
                raise PartError("ARTIFACT_IDENTITY_MISMATCH")
            if not created and record.status != ExportArtifact.Status.READY:
                raise PartError("ARTIFACT_UPLOAD_PENDING")
            # READY is immutable and already safe to reuse. Downgrading it
            # before HEAD would make a worker crash turn a valid object into an
            # unrecoverable UPLOADING marker.
        uncertain = False
        try:
            existing = self.head(key, guard)
            if existing is None:
                md5 = hashlib.md5()  # COS transport checksum, not an identity/security hash.
                sha256 = hashlib.sha256()
                with artifact.path.open("rb") as stream:
                    reader = CheckedFile(stream, guard)
                    while block := reader.read(1024 * 1024):
                        md5.update(block)
                        sha256.update(block)
                if sha256.hexdigest() != record.checksum:
                    raise PartError("LOCAL_ARTIFACT_CHANGED")
                for attempt in range(settings.ASYNC_EXPORT_COS_PUT_ATTEMPTS):
                    client = self.client_factory(min(guard(), settings.ASYNC_EXPORT_COS_TIMEOUT))
                    with artifact.path.open("rb") as stream:
                        try:
                            client.put_object(
                                Bucket=self.bucket,
                                Key=key,
                                Body=CheckedFile(stream, guard),
                                ContentLength=str(record.size),
                                ContentMD5=base64.b64encode(md5.digest()).decode(),
                                Metadata={
                                    "x-cos-meta-sha256": record.checksum,
                                    "x-cos-meta-content-sha256": record.content_checksum,
                                },
                            )
                            break
                        except CosServiceError as error:
                            if error.get_status_code() < 500 or attempt + 1 == settings.ASYNC_EXPORT_COS_PUT_ATTEMPTS:
                                raise PartError("COS_UPLOAD_FAILED") from error
                        except Exception as error:
                            uncertain = True
                            raise UnconfirmedQueryExit("UPLOAD_EXIT_UNCONFIRMED") from error
            self.verify(record, guard)
            return key
        finally:
            # A completed or never-started PUT may be cleaned after terminal
            # state. An uncertain PUT retains its durable marker indefinitely.
            if not uncertain:
                ExportArtifact.objects.filter(pk=record.pk, status=ExportArtifact.Status.UPLOADING).update(
                    status=ExportArtifact.Status.READY
                )

    def delete(self, record, guard):
        if record.storage_id != self.storage_id:
            raise PartError("ARTIFACT_STORAGE_CHANGED")
        client = self.client_factory(min(guard(), settings.ASYNC_EXPORT_COS_TIMEOUT))
        client.delete_object(Bucket=self.bucket, Key=record.object_key)


def cos_artifact_store():
    # Reuse the installed COS SDK; the legacy storage wrapper only returns an
    # ETag and cannot enforce this workflow's timeout/metadata/cleanup contract.
    config = dict(settings.ASYNC_EXPORT_COS)
    bucket = config.pop("Bucket")
    identity = hashlib.sha256(json.dumps([config.get("Region"), bucket]).encode()).hexdigest()

    def client(timeout):
        return CosS3Client(CosConfig(**config, Timeout=timeout, Scheme="https"), retry=0)

    return CosArtifactStore(client, bucket, identity)
