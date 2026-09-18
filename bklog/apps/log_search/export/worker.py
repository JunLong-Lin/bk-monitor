"""一次持有租约的 Part 执行：显式 EOF、有界 JSONL 与校验过的 tar.gz。"""

import hashlib
import gzip
import os
import socket
import tarfile
import tempfile
import time
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path

from django.conf import settings
from django.utils import timezone

from apps.log_search.export import state
from apps.log_search.export.contracts import ExportStateError, PlanningError
from apps.log_search.export.coordinator import BudgetUnavailable, renew_worker_lease
from apps.log_search.export.models import ExportJob
from apps.log_search.export.query import encode_export_row
from apps.log_search.export.files import export_temporary_directory
from apps.utils.log import logger


class PartError(Exception):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


class UnconfirmedQueryExit(PartError):
    """HTTP 调用失败不能证明远端查询已经停止。"""


@dataclass(frozen=True)
class WorkerPolicy:
    batch_rows: int = 1000
    max_empty_batches: int = 3
    max_calls: int = 100_000
    max_bytes: int = 1024 * 1024 * 1024
    deadline: int = 600
    request_timeout: int = 15
    scroll: str = "1m"

    def __post_init__(self):
        for name in ("batch_rows", "max_empty_batches", "max_calls", "max_bytes", "deadline", "request_timeout"):
            if type(getattr(self, name)) is not int or getattr(self, name) <= 0:
                raise ValueError(f"{name} must be a positive integer")
        if not isinstance(self.scroll, str) or not self.scroll:
            raise ValueError("scroll must be configured")


class RawWithScrollReader:
    def __init__(self, query, part, policy, guard):
        self.query, self.part, self.policy, self.guard = query, part, policy, guard
        self.exhausted = False

    def __iter__(self):
        empty = 0
        for call in range(self.policy.max_calls):
            timeout = min(self.policy.request_timeout, self.guard())
            params = self.query.request(self.part.start_time, self.part.end_time)
            params.update(limit=self.policy.batch_rows, scroll=self.policy.scroll, clear_cache=call == 0, slice_max=0)
            try:
                response = self.query.read(params, timeout=timeout)
            except Exception as error:
                raise UnconfirmedQueryExit("QUERY_EXIT_UNCONFIRMED") from error
            self.guard()
            if (
                not isinstance(response, dict)
                or response.get("result") is False
                or response.get("partial")
                or response.get("errors")
                or response.get("timed_out")
                or type(response.get("done")) is not bool
                or not isinstance(response.get("list"), list)
                or len(response["list"]) > self.policy.batch_rows
            ):
                raise PartError("INVALID_SCROLL_RESPONSE")
            count = len(response["list"])
            rows = self.query.project(response) if count else []
            if len(rows) != count or any(not isinstance(row, dict) for row in rows):
                raise PartError("INVALID_PROJECTED_ROWS")
            yield rows
            if response["done"]:
                self.exhausted = True
                return
            empty = empty + 1 if not count else 0
            if empty >= self.policy.max_empty_batches:
                raise PartError("SCROLL_NO_PROGRESS")
        raise PartError("SCROLL_CALL_BUDGET_EXCEEDED")


class AttemptGuard:
    def __init__(self, part, budget, policy, credentials):
        self.part, self.budget, self.credentials = part, budget, credentials
        self.deadline = time.monotonic() + policy.deadline
        self.rows = 0

    def __call__(self):
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise PartError("PART_DEADLINE_EXCEEDED")
        lease_seconds = settings.ASYNC_EXPORT_LEASE_SECONDS
        if lease_seconds <= 1:
            raise BudgetUnavailable("bounded worker I/O requires a configured lease")
        if ExportJob.objects.get(pk=self.part.plan.job_id).status != ExportJob.Status.RUNNING:
            raise PartError("JOB_STOPPED")
        renew_worker_lease(
            self.budget,
            self.part.pk,
            **self.credentials,
            lease_until=timezone.now() + timedelta(seconds=lease_seconds),
            processed_rows=self.rows,
        )
        # 留出余量，确保租约到期前还能观察到响应。
        return min(remaining, lease_seconds / 2)


class CheckedFile:
    mode = "rb"

    def __init__(self, stream, guard):
        self.stream, self.guard = stream, guard
        self.unchecked_bytes = 1024 * 1024

    def read(self, size=-1):
        if self.unchecked_bytes >= 1024 * 1024:
            self.guard()
            self.unchecked_bytes = 0
        result = self.stream.read(size)
        self.unchecked_bytes += len(result)
        return result

    def tell(self):
        return self.stream.tell()

    def fileno(self):
        return self.stream.fileno()


def digest_file(path, guard):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        checked = CheckedFile(stream, guard)
        while block := checked.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


@dataclass(frozen=True)
class Artifact:
    path: Path
    rows: int
    size: int
    compressed_size: int
    checksum: str
    content_checksum: str = ""


def package_part(query, part, directory, policy, guard, *, on_packaging=None):
    extension = part.plan.job.query_snapshot.get("export_file_type", "txt")
    if extension not in {"txt", "log"}:
        raise PartError("UNSUPPORTED_EXPORT_FILE_TYPE")
    member_name = f"logs.{extension}"
    payload = directory / "logs.jsonl"
    payload_digest = hashlib.sha256()
    size = rows = 0
    reader = RawWithScrollReader(query, part, policy, guard)
    with payload.open("wb") as stream:
        for batch in reader:
            guard()
            for row in batch:
                data = encode_export_row(row)
                size += len(data)
                if size > policy.max_bytes:
                    raise PartError("OVERSIZED")
                stream.write(data)
                payload_digest.update(data)
                rows += 1
            guard.rows = rows
    if not reader.exhausted:
        raise PartError("SOURCE_NOT_EXHAUSTED")
    guard()
    if on_packaging is not None:
        on_packaging()
    archive = directory / "part.tar.gz"
    with (
        archive.open("wb") as output,
        gzip.GzipFile(filename="", fileobj=output, mode="wb", mtime=0) as compressed,
        tarfile.open(fileobj=compressed, mode="w|") as bundle,
        payload.open("rb") as stream,
    ):
        info = tarfile.TarInfo(member_name)
        info.size, info.mode, info.mtime = size, 0o600, 0
        bundle.addfile(info, CheckedFile(stream, guard))
    # 回读压缩包；必须与完整的 JSONL 字节完全一致才算成功。
    with tarfile.open(archive, "r|gz") as bundle:
        member = bundle.next()
        if member is None or not member.isfile() or member.name != member_name or member.size != size:
            raise PartError("ARCHIVE_VERIFICATION_FAILED")
        digest = hashlib.sha256()
        with bundle.extractfile(member) as stream:
            while block := CheckedFile(stream, guard).read(1024 * 1024):
                digest.update(block)
        if digest.hexdigest() != payload_digest.hexdigest() or bundle.next() is not None:
            raise PartError("ARCHIVE_VERIFICATION_FAILED")
    return Artifact(
        archive, rows, size, archive.stat().st_size, digest_file(archive, guard), payload_digest.hexdigest()
    )


class LocalArtifactStore:
    """仅用于本地开发的产物落盘，不能替代 COS 或共享存储。"""

    def __init__(self, root):
        self.root = Path(root).resolve()

    def publish(self, part, artifact, guard):
        key = f"{part.plan.job_id}/{part.plan.plan_version}/{part.pk}/{artifact.checksum}.tar.gz"
        target = self.root / key
        target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        # 先完整复制并校验，再原子链接；绝不覆盖已有的获胜对象。
        with tempfile.NamedTemporaryFile(dir=target.parent) as temporary, artifact.path.open("rb") as source:
            while block := CheckedFile(source, guard).read(1024 * 1024):
                temporary.write(block)
            temporary.flush()
            os.fsync(temporary.fileno())
            guard()
            try:
                os.link(temporary.name, target)
            except FileExistsError:
                pass
        if target.stat().st_size != artifact.compressed_size or digest_file(target, guard) != artifact.checksum:
            raise PartError("ARTIFACT_VERIFICATION_FAILED")
        return "local:" + key


def local_artifact_store():
    root = settings.ASYNC_EXPORT_LOCAL_ARTIFACT_ROOT
    if os.environ.get("BKPAAS_ENVIRONMENT") != "dev" or not root:
        raise ValueError("local artifacts require dev and an explicit isolated directory")
    return LocalArtifactStore(root)


def run_part(part_id, *, generation, lease_id, query_factory, budget, store, policy=None):
    policy = policy or WorkerPolicy(**settings.ASYNC_EXPORT_WORKER_POLICY)
    credentials = dict(generation=generation, lease_id=lease_id)
    try:
        part = state.claim_part(part_id, **credentials, worker_id=f"{socket.gethostname()}:{os.getpid()}")
    except ExportStateError:
        return  # 重复、已取消或过期的投递不会发起任何查询 I/O。
    guard = AttemptGuard(part, budget, policy, credentials)
    try:
        guard()
        with export_temporary_directory("part") as directory:
            with query_factory(part.plan.job) as query:
                artifact = package_part(
                    query,
                    part,
                    Path(directory),
                    policy,
                    guard,
                    on_packaging=lambda: state.begin_part_package(part.pk, **credentials),
                )
            guard()
            state.begin_part_upload(part.pk, **credentials)
            key = store.publish(part, artifact, guard)
            guard()
            state.complete_part(
                part.pk,
                **credentials,
                actual_rows=artifact.rows,
                actual_bytes=artifact.size,
                compressed_bytes=artifact.compressed_size,
                object_key=key,
                checksum=artifact.checksum,
                content_checksum=artifact.content_checksum,
            )
    except UnconfirmedQueryExit as error:
        if error.code in {"UPLOAD_EXIT_UNCONFIRMED", "BKREPO_RESPONSE_UNCONFIRMED"}:
            # 上传代次使用独立对象键；迟到的旧上传不会覆盖下一次产物。
            state.retry_part(
                part.pk,
                **credentials,
                error_code=error.code,
                error_detail=type(error).__name__,
                next_retry_at=timezone.now() + timedelta(seconds=settings.ASYNC_EXPORT_PART_RETRY_SECONDS),
            )
        else:
            state.note_unconfirmed_part(part.pk, **credentials, error_code=error.code)
            return
    except Exception as error:
        code = error.code if isinstance(error, PartError | PlanningError) else "PART_EXECUTION_FAILED"
        try:
            state.retry_part(
                part.pk,
                **credentials,
                error_code=code,
                error_detail=type(error).__name__,
                next_retry_at=timezone.now() + timedelta(seconds=settings.ASYNC_EXPORT_PART_RETRY_SECONDS),
            )
        except ExportStateError:
            return
    try:
        budget.release(part.pk, generation, lease_id)
    except BudgetUnavailable:
        logger.warning("sharded export completed I/O; ledger release awaits reconciliation")
