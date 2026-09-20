"""分片与清单产物的上传和校验。"""

import base64
import hashlib
import json
from dataclasses import dataclass
from urllib.parse import quote, urlparse

import requests
from django.conf import settings
from qcloud_cos import CosConfig, CosS3Client
from qcloud_cos.cos_exception import CosServiceError
from requests.auth import HTTPBasicAuth

from apps.log_search.export.models import ExportJob
from apps.log_search.export.worker import CheckedFile, PartError, UnconfirmedQueryExit


def artifact_prefix(job):
    scope = json.dumps([settings.ASYNC_EXPORT_NAMESPACE, job.space_uid], separators=(",", ":"))
    return f"exports/{hashlib.sha256(scope.encode()).hexdigest()}/{job.pk}/"


@dataclass(frozen=True)
class StoredArtifact:
    object_key: str
    checksum: str
    size: int
    storage_id: str


class ArtifactStore:
    """与具体存储无关的上传和校验规则。"""

    def __init__(self, storage_id):
        self.storage_id = storage_id

    def publish(self, part, artifact, guard):
        key = (
            f"{artifact_prefix(part.plan.job)}{part.plan.plan_version}/{part.pk}/"
            f"{part.dispatch_generation}/{artifact.checksum}.tar.gz"
        )
        return self.publish_file(part.plan.job, key, artifact, guard)

    def publish_file(self, job, key, artifact, guard):
        guard()
        current = ExportJob.objects.get(pk=job.pk)
        if current.status != ExportJob.Status.RUNNING or not key.startswith(artifact_prefix(current)):
            raise PartError("JOB_STOPPED")
        record = StoredArtifact(key, artifact.checksum, artifact.compressed_size, self.storage_id)
        existing = self.head(key, guard)
        if existing is None:
            md5 = hashlib.md5()  # 传输校验，不用于身份或安全判定。
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

    def head(self, key, guard):
        raise NotImplementedError

    def upload(self, record, artifact, guard, md5_digest):
        raise NotImplementedError

    def verify(self, record, guard):
        raise NotImplementedError

    def delete(self, record, guard):
        raise NotImplementedError

    def sign_download(self, key, expires_in):
        raise NotImplementedError


class CosArtifactStore(ArtifactStore):
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
            # HEAD 通常没有 XML 响应体，SDK 可能给出 Unknown 错误码。
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
        if metadata.get("content-length") != str(record.size) or metadata.get("x-cos-meta-sha256") != record.checksum:
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

    def sign_download(self, key, expires_in):
        client = self.client_factory(settings.ASYNC_EXPORT_COS_TIMEOUT)
        return client.get_presigned_download_url(Bucket=self.bucket, Key=key, Expired=expires_in)


class BKRepoHttpClient:
    """按已安装的 BKRepo SDK 协议实现，并支持单次调用超时。"""

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

    def sign_download(self, key, expires_in, timeout):
        response = self.session.post(
            f"{self.endpoint_url}/generic/temporary/url/create",
            json={
                "projectId": self.project,
                "repoName": self.bucket,
                "fullPathSet": [key],
                "expireSeconds": expires_in,
                "type": "DOWNLOAD",
            },
            timeout=timeout,
        )
        if response.status_code != 200:
            raise PartError("BKREPO_SIGN_FAILED")
        try:
            data = response.json()
            if str(data.get("code")) != "0":
                raise PartError("BKREPO_SIGN_FAILED")
            url = data["data"][0]["url"]
            parsed = urlparse(url)
            if parsed.scheme not in {"http", "https"} or not parsed.netloc or parsed.username or parsed.password:
                raise PartError("BKREPO_SIGN_FAILED")
            return url
        except (ValueError, KeyError, IndexError, TypeError) as error:
            raise PartError("BKREPO_SIGN_FAILED") from error


class BKRepoArtifactStore(ArtifactStore):
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
        checksum = headers.get("x-checksum-sha256") or headers.get("x-bkrepo-sha256")
        return headers.get("content-length") == str(record.size) and checksum == record.checksum

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
            # requests 一旦开始消费数据流，即使是租约校验或本地读取错误，
            # 也无法证明 BKRepo 是否已落盘。
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

    def sign_download(self, key, expires_in):
        return self.client.sign_download(self._key(key), expires_in, settings.ASYNC_EXPORT_BKREPO_TIMEOUT)


def cos_artifact_store():
    # 直接复用已安装的 COS SDK；旧 Storage 包装器只返回 ETag，
    # 无法满足本流程的超时、元数据与清理契约。
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
