"""Manifest 提交和已成功产物清理。"""

import hashlib
import json
import time
from datetime import timedelta
from pathlib import Path

from django.conf import settings
from django.db import transaction
from django.utils import timezone

from apps.log_search.export import state
from apps.log_search.export.models import ExportJob, ExportPart, ExportPlan
from apps.log_search.export.storage import StoredArtifact, artifact_prefix
from apps.log_search.export.worker import Artifact, PartError
from apps.log_search.export.files import export_temporary_directory


def manifest_snapshot(job, store_id):
    plan = ExportPlan.objects.get(job=job, plan_version=job.current_plan_version, status=ExportPlan.Status.READY)
    parts = list(plan.parts.filter(is_leaf=True).order_by("start_time", "part_no"))
    if not parts or len(parts) != plan.part_count or any(p.status != ExportPart.Status.SUCCESS for p in parts):
        raise PartError("MANIFEST_PARTS_INCOMPLETE")
    cursor, entries, records = job.start_time, [], []
    for part in parts:
        if part.start_time != cursor or part.end_time <= cursor or part.end_time > job.end_time:
            raise PartError("MANIFEST_BOUNDARY_INVALID")
        cursor = part.end_time
        if any(type(v) is not int or v < 0 for v in (part.actual_rows, part.actual_bytes, part.compressed_bytes)):
            raise PartError("MANIFEST_COUNTS_INVALID")
        if not part.object_key or not part.checksum or not part.object_key.startswith(artifact_prefix(job)):
            raise PartError("MANIFEST_ARTIFACT_INVALID")
        record = StoredArtifact(part.object_key, part.checksum, part.compressed_bytes, store_id)
        records.append(record)
        entries.append(
            dict(
                part_id=part.pk,
                part_no=part.part_no,
                start_time=part.start_time,
                end_time=part.end_time,
                actual_rows=part.actual_rows,
                actual_bytes=part.actual_bytes,
                compressed_bytes=part.compressed_bytes,
                object_key=part.object_key,
                checksum=part.checksum,
                checksum_algorithm="sha256",
            )
        )
    if cursor != job.end_time or sum(p.actual_rows for p in parts) != job.actual_total:
        raise PartError("MANIFEST_TOTAL_INVALID")
    # 绝对到期时间以 Job 为准，在随后的成功事务中计算；上传的文件里
    # 无法给出真实值，因此只记录成功后保留时长。
    return dict(
        schema_version=1,
        job_id=job.pk,
        plan_version=plan.plan_version,
        consistency="weak_snapshot",
        interval="[start,end)",
        time_units_per_second=job.query_snapshot["time_units_per_second"],
        estimated_total=job.estimated_total,
        actual_total=job.actual_total,
        expires_after_success_seconds=settings.ASYNC_EXPORT_ARTIFACT_RETENTION_SECONDS,
        parts=entries,
    ), records


def finalize_export(job_id, store):
    with transaction.atomic():
        job = ExportJob.objects.select_for_update().get(pk=job_id)
        if job.status != ExportJob.Status.RUNNING:
            return
        leaves = ExportPart.objects.filter(plan__job=job, plan__plan_version=job.current_plan_version, is_leaf=True)
        if not leaves.exists() or leaves.exclude(status=ExportPart.Status.SUCCESS).exists():
            return
        now = timezone.now()
        if job.next_finalization_at and job.next_finalization_at > now:
            return
        if job.finalization_attempts >= settings.ASYNC_EXPORT_FINALIZATION_ATTEMPTS:
            state.fail_job(job.pk, error_code="FINALIZATION_RETRIES_EXHAUSTED")
            return
        job.finalization_attempts += 1
        job.next_finalization_at = now + timedelta(seconds=settings.ASYNC_EXPORT_FINALIZATION_DEADLINE)
        job.save(update_fields=["finalization_attempts", "next_finalization_at"])
        attempt = job.finalization_attempts
    deadline = time.monotonic() + settings.ASYNC_EXPORT_FINALIZATION_DEADLINE

    def guard():
        remaining = deadline - time.monotonic()
        current = ExportJob.objects.get(pk=job.pk)
        if (
            remaining <= 0
            or current.status != ExportJob.Status.RUNNING
            or current.state_version != job.state_version
            or current.finalization_attempts != attempt
        ):
            raise PartError("FINALIZATION_STOPPED")
        return remaining

    try:
        guard()
        manifest, records = manifest_snapshot(job, store.storage_id)
        for record in records:
            store.verify(record, guard)
        content = json.dumps(manifest, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
        checksum = hashlib.sha256(content).hexdigest()
        key = f"{artifact_prefix(job)}{job.current_plan_version}/manifest/{attempt}/{checksum}.json"
        with export_temporary_directory("manifest") as directory:
            path = Path(directory) / "manifest.json"
            path.write_bytes(content)
            artifact = Artifact(path, 0, len(content), len(content), checksum)
            store.publish_file(job, key, artifact, guard)
        guard()
        return state.finalize_job_success(
            job.pk,
            plan_version=job.current_plan_version,
            manifest_object_key=key,
            manifest_checksum=checksum,
            manifest_bytes=len(content),
        )
    except Exception as error:
        with transaction.atomic():
            current = ExportJob.objects.select_for_update().get(pk=job.pk)
            if current.status == ExportJob.Status.RUNNING and current.finalization_attempts == attempt:
                current.error_code = error.code if isinstance(error, PartError) else "FINALIZATION_FAILED"
                current.error_detail = type(error).__name__
                current.next_finalization_at = timezone.now() + timedelta(
                    seconds=settings.ASYNC_EXPORT_FINALIZATION_RETRY
                )
                current.save(update_fields=["error_code", "error_detail", "next_finalization_at"])


def cleanup_export(job_id, store, limit=100):
    with transaction.atomic():
        job = ExportJob.objects.select_for_update().get(pk=job_id)
        if job.status not in {ExportJob.Status.SUCCESS, ExportJob.Status.FAILED, ExportJob.Status.CANCELED}:
            return 0
        if ExportPart.objects.filter(plan__job=job, status__in=["DISPATCHED", "RUNNING", "UPLOADING"]).exists():
            return 0
        if job.artifacts_cleaned_at is not None:
            return 0
        if job.status == ExportJob.Status.SUCCESS and (job.expires_at is None or job.expires_at > timezone.now()):
            return 0
        parts = list(ExportPart.objects.filter(plan__job=job).exclude(object_key="").order_by("pk")[:limit])
        records = [
            (
                part,
                StoredArtifact(part.object_key, part.checksum, part.compressed_bytes, store.storage_id),
            )
            for part in parts
        ]
        if len(parts) < limit and job.manifest_object_key:
            records.append(
                (
                    None,
                    StoredArtifact(
                        job.manifest_object_key, job.manifest_checksum, job.manifest_bytes or 0, store.storage_id
                    ),
                )
            )
    deleted = 0
    deadline = time.monotonic() + settings.ASYNC_EXPORT_CLEANUP_DEADLINE

    def guard():
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise PartError("CLEANUP_DEADLINE_EXCEEDED")
        return remaining

    for part, record in records:
        try:
            store.delete(record, guard)
        except Exception:
            # 删除失败时保留对象键，下一轮幂等重试。
            continue
        if part is None:
            ExportJob.objects.filter(pk=job_id, manifest_object_key=record.object_key).update(manifest_object_key="")
        else:
            ExportPart.objects.filter(pk=part.pk, object_key=record.object_key).update(object_key="")
        deleted += 1
    has_parts = ExportPart.objects.filter(plan__job_id=job_id).exclude(object_key="").exists()
    has_manifest = bool(ExportJob.objects.get(pk=job_id).manifest_object_key)
    if not has_parts and not has_manifest:
        ExportJob.objects.filter(pk=job_id, artifacts_cleaned_at__isnull=True).update(
            artifacts_cleaned_at=timezone.now()
        )
    return deleted
