"""一次持有租约的 Part 执行：显式 EOF、有界 JSONL 与校验过的 tar.gz。"""

import hashlib
import gzip
import tarfile
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
                raise PartError("QUERY_FAILED") from error
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


def package_part(query, part, directory, policy, guard, *, on_packaging=None):
    member_name = "logs.log"
    payload = directory / "logs.jsonl"
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
                rows += 1
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
    return Artifact(archive, rows, size, archive.stat().st_size, digest_file(archive, guard))


def run_part(part_id, *, lease_id, query_factory, budget, store, policy=None):
    policy = policy or WorkerPolicy(**settings.ASYNC_EXPORT_WORKER_POLICY)
    credentials = dict(lease_id=lease_id)
    try:
        part = state.claim_part(part_id, **credentials)
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
            )
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
        budget.release(part.pk, lease_id)
    except BudgetUnavailable:
        logger.warning("sharded export completed I/O; ledger release awaits reconciliation")
