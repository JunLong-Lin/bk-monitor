"""Registered immutable export artifacts, with no upload-time expiry."""

import base64
import hashlib
import json
from urllib.parse import quote

import requests
from django.conf import settings
from django.db import transaction
from qcloud_cos import CosConfig, CosS3Client
from qcloud_cos.cos_exception import CosServiceError
from requests.auth import HTTPBasicAuth

from apps.log_search.export_models import ExportArtifact, ExportJob
from apps.log_search.export_worker import CheckedFile, PartError, UnconfirmedQueryExit


def artifact_prefix(job):
    scope = json.dumps([settings.ASYNC_EXPORT_NAMESPACE, job.bk_tenant_id, job.space_uid], separators=(",", ":"))
    return f"exports/{hashlib.sha256(scope.encode()).hexdigest()}/{job.pk}/"


class RegisteredArtifactStore:
    """Storage-independent registration, immutability, and recovery rules."""

    def __init__(self, storage_id):
        self.storage_id = storage_id

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
                md5 = hashlib.md5()  # Transport checksum, not an identity/security hash.
                sha256 = hashlib.sha256()
                with artifact.path.open("rb") as stream:
                    reader = CheckedFile(stream, guard)
                    while block := reader.read(1024 * 1024):
                        md5.update(block)
                        sha256.update(block)
                if sha256.hexdigest() != record.checksum:
                    raise PartError("LOCAL_ARTIFACT_CHANGED")
                self.upload(record, artifact, guard, md5.digest())
            self.verify(record, guard)
            return key
        except UnconfirmedQueryExit:
            uncertain = True
            raise
        finally:
            # A completed or never-started PUT may be cleaned after terminal
            # state. An uncertain PUT retains its durable marker indefinitely.
            if not uncertain:
                ExportArtifact.objects.filter(pk=record.pk, status=ExportArtifact.Status.UPLOADING).update(
                    status=ExportArtifact.Status.READY
                )

    def head(self, key, guard):
        raise NotImplementedError

    def upload(self, record, artifact, guard, md5_digest):
        raise NotImplementedError

    def verify(self, record, guard):
        raise NotImplementedError

    def delete(self, record, guard):
        raise NotImplementedError


class CosArtifactStore(RegisteredArtifactStore):
    def __init__(self, client_factory, bucket, storage_id):
        if any(
            type(value) is not int or value <= 0
            for value in (settings.ASYNC_EXPORT_COS_TIMEOUT, settings.ASYNC_EXPORT_COS_PUT_ATTEMPTS)
        ):
            raise ValueError("COS timeout and attempt budgets must be positive integers")
        self.client_factory, self.bucket = client_factory, bucket
        super().__init__(storage_id)

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

    def upload(self, record, artifact, guard, md5_digest):
        for attempt in range(settings.ASYNC_EXPORT_COS_PUT_ATTEMPTS):
            client = self.client_factory(min(guard(), settings.ASYNC_EXPORT_COS_TIMEOUT))
            with artifact.path.open("rb") as stream:
                try:
                    client.put_object(
                        Bucket=self.bucket,
                        Key=record.object_key,
                        Body=CheckedFile(stream, guard),
                        ContentLength=str(record.size),
                        ContentMD5=base64.b64encode(md5_digest).decode(),
                        Metadata={
                            "x-cos-meta-sha256": record.checksum,
                            "x-cos-meta-content-sha256": record.content_checksum,
                        },
                    )
                    return
                except CosServiceError as error:
                    if error.get_status_code() < 500 or attempt + 1 == settings.ASYNC_EXPORT_COS_PUT_ATTEMPTS:
                        raise PartError("COS_UPLOAD_FAILED") from error
                except Exception as error:
                    raise UnconfirmedQueryExit("UPLOAD_EXIT_UNCONFIRMED") from error

    def delete(self, record, guard):
        if record.storage_id != self.storage_id:
            raise PartError("ARTIFACT_STORAGE_CHANGED")
        client = self.client_factory(min(guard(), settings.ASYNC_EXPORT_COS_TIMEOUT))
        client.delete_object(Bucket=self.bucket, Key=record.object_key)


class BKRepoHttpClient:
    """The installed BKRepo SDK protocol with a per-call timeout."""

    def __init__(self, endpoint_url, project, bucket, username, password, session=None):
        self.endpoint_url = endpoint_url.rstrip("/")
        self.project = project
        self.bucket = bucket
        self.session = session or requests.Session()
        self.session.auth = HTTPBasicAuth(username, password)

    def _url(self, key):
        project = quote(str(self.project), safe="")
        bucket = quote(str(self.bucket), safe="")
        key = quote(key.lstrip("/"), safe="/")
        return f"{self.endpoint_url}/generic/{project}/{bucket}/{key}"

    def head(self, key, timeout):
        return self.session.head(self._url(key), timeout=timeout)

    def put(self, key, stream, size, checksum, timeout):
        return self.session.put(
            self._url(key),
            data=stream,
            timeout=timeout,
            headers={
                "Content-Length": str(size),
                "X-BKREPO-OVERWRITE": "false",
                "X-BKREPO-SHA256": checksum,
            },
        )

    def delete(self, key, timeout):
        return self.session.delete(self._url(key), timeout=timeout)


class BKRepoArtifactStore(RegisteredArtifactStore):
    NOT_FOUND_CODES = {"250102", "251010"}
    OBJECT_EXISTS_CODES = {"250107", "251012"}

    def __init__(self, client, storage_id, key_prefix=""):
        if any(
            type(value) is not int or value <= 0
            for value in (settings.ASYNC_EXPORT_BKREPO_TIMEOUT, settings.ASYNC_EXPORT_BKREPO_PUT_ATTEMPTS)
        ):
            raise ValueError("BKRepo timeout and attempt budgets must be positive integers")
        self.client = client
        self.key_prefix = key_prefix.strip("/")
        super().__init__(storage_id)

    def _key(self, key):
        key = key.lstrip("/")
        return f"{self.key_prefix}/{key}" if self.key_prefix else key

    @staticmethod
    def _data(response):
        try:
            return response.json()
        except ValueError as error:
            raise UnconfirmedQueryExit("BKREPO_RESPONSE_UNCONFIRMED") from error

    @staticmethod
    def _headers(response):
        return {key.lower(): str(value) for key, value in response.headers.items()}

    def _matches(self, response, record):
        headers = self._headers(response)
        return headers.get("content-length") == str(record.size) and headers.get("x-bkrepo-sha256") == record.checksum

    def head(self, key, guard):
        response = self.client.head(self._key(key), min(guard(), settings.ASYNC_EXPORT_BKREPO_TIMEOUT))
        if response.status_code == 200:
            return response
        if response.status_code == 404:
            return None
        raise PartError("BKREPO_HEAD_FAILED")

    def verify(self, record, guard):
        if record.storage_id != self.storage_id:
            raise PartError("ARTIFACT_STORAGE_CHANGED")
        response = self.head(record.object_key, guard)
        if response is None:
            raise PartError("ARTIFACT_MISSING")
        if not self._matches(response, record):
            raise PartError("ARTIFACT_VERIFICATION_FAILED")

    def _reconcile_upload(self, record, guard):
        try:
            response = self.head(record.object_key, guard)
        except Exception as error:
            raise UnconfirmedQueryExit("UPLOAD_EXIT_UNCONFIRMED") from error
        if response is None:
            return False
        if self._matches(response, record):
            return True
        raise UnconfirmedQueryExit("UPLOAD_EXIT_UNCONFIRMED")

    def upload(self, record, artifact, guard, md5_digest):
        del md5_digest
        for attempt in range(settings.ASYNC_EXPORT_BKREPO_PUT_ATTEMPTS):
            try:
                with artifact.path.open("rb") as stream:
                    response = self.client.put(
                        self._key(record.object_key),
                        CheckedFile(stream, guard),
                        record.size,
                        record.checksum,
                        min(guard(), settings.ASYNC_EXPORT_BKREPO_TIMEOUT),
                    )
            # Once requests starts consuming the stream, even a lease guard or
            # local read error cannot prove whether BKRepo persisted the object.
            except Exception as error:
                if self._reconcile_upload(record, guard):
                    return
                if attempt + 1 == settings.ASYNC_EXPORT_BKREPO_PUT_ATTEMPTS:
                    raise UnconfirmedQueryExit("UPLOAD_EXIT_UNCONFIRMED") from error
                continue
            data = self._data(response)
            code = str(data.get("code"))
            if code == "0" or code in self.OBJECT_EXISTS_CODES:
                return
            if response.status_code >= 500 and attempt + 1 < settings.ASYNC_EXPORT_BKREPO_PUT_ATTEMPTS:
                if self._reconcile_upload(record, guard):
                    return
                continue
            raise PartError("BKREPO_UPLOAD_FAILED")

    def delete(self, record, guard):
        if record.storage_id != self.storage_id:
            raise PartError("ARTIFACT_STORAGE_CHANGED")
        response = self.client.delete(self._key(record.object_key), min(guard(), settings.ASYNC_EXPORT_BKREPO_TIMEOUT))
        if response.status_code == 404:
            return
        data = self._data(response)
        if str(data.get("code")) not in {"0", *self.NOT_FOUND_CODES}:
            raise PartError("BKREPO_DELETE_FAILED")


def cos_artifact_store():
    # Reuse the installed COS SDK; the legacy storage wrapper only returns an
    # ETag and cannot enforce this workflow's timeout/metadata/cleanup contract.
    config = dict(settings.ASYNC_EXPORT_COS)
    bucket = config.pop("Bucket")
    identity = hashlib.sha256(json.dumps([config.get("Region"), bucket]).encode()).hexdigest()

    def client(timeout):
        return CosS3Client(CosConfig(**config, Timeout=timeout, Scheme="https"), retry=0)

    return CosArtifactStore(client, bucket, identity)


def bkrepo_artifact_store():
    required = {
        "endpoint_url": settings.BKREPO_ENDPOINT_URL,
        "username": settings.BKREPO_USERNAME,
        "password": settings.BKREPO_PASSWORD,
        "project": settings.BKREPO_PROJECT,
        "bucket": settings.BKREPO_BUCKET,
    }
    if missing := [name for name, value in required.items() if not value]:
        raise ValueError(f"BKRepo configuration is incomplete: {','.join(missing)}")
    key_prefix = str(getattr(settings, "BKREPO_LOCATION", "") or "").strip("/")
    identity = hashlib.sha256(
        json.dumps([required["endpoint_url"].rstrip("/"), required["project"], required["bucket"], key_prefix]).encode()
    ).hexdigest()
    client = BKRepoHttpClient(**required)
    return BKRepoArtifactStore(client, identity, key_prefix)
