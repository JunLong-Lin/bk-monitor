"""分片导出任务的授权元数据与操作接口。"""

from datetime import timedelta

from django.conf import settings
from django.shortcuts import get_object_or_404
from django.utils import timezone
from django.utils.module_loading import import_string
from rest_framework.exceptions import APIException, PermissionDenied

from apps.iam import ActionEnum, ResourceEnum
from apps.iam.handlers.drf import BusinessActionPermission, IAMPermission
from apps.log_search.export import state
from apps.log_search.export.contracts import InvalidTransitionError
from apps.log_search.export.models import ExportJob, ExportPart
from apps.log_search.models import LogIndexSet, Space
from apps.utils.local import get_request_app_code, get_request_tenant_id, get_request_username


TERMINAL = {ExportJob.Status.SUCCESS, ExportJob.Status.FAILED, ExportJob.Status.CANCELED}
INFLIGHT = {ExportPart.Status.DISPATCHED, ExportPart.Status.RUNNING, ExportPart.Status.UPLOADING}


class ExportConflict(APIException):
    status_code = 409
    default_detail = "EXPORT_INVALID_STATE"
    default_code = "EXPORT_INVALID_STATE"


class ExportExpired(APIException):
    status_code = 410
    default_detail = "EXPORT_FILE_EXPIRED"
    default_code = "EXPORT_FILE_EXPIRED"


class ExportStorageUnavailable(APIException):
    status_code = 503
    default_detail = "EXPORT_STORAGE_UNAVAILABLE"
    default_code = "EXPORT_STORAGE_UNAVAILABLE"


def authorized_job(request, job_id, space_uid, *, operate=False):
    username = get_request_username(default="")
    if not username:
        raise PermissionDenied("EXPORT_IDENTITY_REQUIRED")
    # 先按范围过滤再查询，范围包含中间件注入的来源应用身份。
    job = get_object_or_404(
        ExportJob,
        pk=job_id,
        space_uid=space_uid,
        source_app_code=get_request_app_code(),
    )
    if job.query_kind != ExportJob.QueryKind.SINGLE or len(job.index_set_ids) != 1:
        raise PermissionDenied("QUERY_MODE_NOT_IMPLEMENTED")
    authorized_scope(request, job.space_uid, job.index_set_ids[0])
    if operate and job.created_by != username:
        raise PermissionDenied("EXPORT_CREATOR_REQUIRED")
    return job


def authorized_scope(request, space_uid, index_set_id):
    space = get_object_or_404(Space, space_uid=space_uid, bk_tenant_id=get_request_tenant_id())
    index = get_object_or_404(LogIndexSet, pk=index_set_id, space_uid=space.space_uid)
    # 这里只暴露元数据；查询条件、投影字段、原始错误和对象键
    # 需要另行走产物/数据访问授权。
    permissions = [
        BusinessActionPermission([ActionEnum.VIEW_BUSINESS], space_uid=space.space_uid),
        IAMPermission([ActionEnum.SEARCH_LOG], [ResourceEnum.INDICES.create_instance(index.pk)]),
    ]
    for permission in permissions:
        if not permission.has_permission(request, None):
            raise PermissionDenied()
    return space, index


def job_detail(job_id):
    job = ExportJob.objects.get(pk=job_id)
    parts = list(
        ExportPart.objects.filter(plan__job=job, plan__plan_version=job.current_plan_version, is_leaf=True).values(
            "status", "stage", "actual_rows", "processed_rows"
        )
    )
    completed = sum(part["status"] == ExportPart.Status.SUCCESS for part in parts)
    actual = sum(part["actual_rows"] or 0 for part in parts if part["status"] == ExportPart.Status.SUCCESS)
    active = [part for part in parts if part["status"] in INFLIGHT]
    processed = actual + sum(part["processed_rows"] for part in active)
    success = job.status == ExportJob.Status.SUCCESS
    expired = success and job.expires_at is not None and job.expires_at <= timezone.now()
    stage = ""
    if job.status not in TERMINAL:
        if parts and completed == len(parts):
            stage = ExportJob.Stage.FINALIZING
        else:
            stage = next(
                (value for value in ExportJob.Stage.values if any(part["stage"] == value for part in active)),
                "",
            )
    return {
        "job_id": job.pk,
        "task_model": "ExportJob",
        "status": "EXPIRED" if expired else job.status,
        "execution_status": job.status,
        "stage": stage,
        "estimated_total": job.estimated_total,
        "actual_total": actual,
        "processed_rows": processed,
        "parts_total": len(parts),
        "parts_completed": completed,
        "percent": 100 if success else min(99, completed * 100 // len(parts)) if parts else 0,
        "percent_basis": "completed_parts",
        "requested_parallelism": job.requested_parallelism,
        "configured_parallelism_limit": max(
            0, min(job.requested_parallelism, settings.ASYNC_EXPORT_INDEX_LIMIT, settings.ASYNC_EXPORT_GLOBAL_LIMIT)
        ),
        "inflight_parts": len(active),
        "error_code": job.error_code,
        "created_by": job.created_by,
        "created_at": job.created_at,
        "completed_at": job.completed_at,
        "expires_at": job.expires_at,
        "state_version": job.state_version,
        "can_view": True,
        "can_operate": job.created_by == get_request_username(default="") and job.status not in TERMINAL,
        "poll_after": None if job.status in TERMINAL else 3,
    }


def job_results(job):
    if job.status != ExportJob.Status.SUCCESS:
        raise ExportConflict("EXPORT_NOT_READY")
    if job.expires_at is None or job.expires_at <= timezone.now():
        raise ExportExpired()
    parts = list(
        ExportPart.objects.filter(
            plan__job=job,
            plan__plan_version=job.current_plan_version,
            is_leaf=True,
            status=ExportPart.Status.SUCCESS,
        ).order_by("start_time", "part_no")
    )
    if not parts or not job.manifest_object_key:
        raise ExportConflict("EXPORT_RESULT_INCOMPLETE")
    if not job.manifest_checksum or any(not part.object_key or not part.checksum for part in parts):
        raise ExportConflict("EXPORT_RESULT_INCOMPLETE")
    return {
        "job_id": job.pk,
        "estimated_total": job.estimated_total,
        "actual_total": job.actual_total,
        "expires_at": job.expires_at,
        "manifest": {
            "artifact_id": "manifest",
            "checksum": job.manifest_checksum,
            "compressed_bytes": job.manifest_bytes,
        },
        "parts": [
            {
                "artifact_id": str(part.pk),
                "part_id": part.pk,
                "part_no": part.part_no,
                "start_time": part.start_time,
                "end_time": part.end_time,
                "actual_rows": part.actual_rows,
                "actual_bytes": part.actual_bytes,
                "compressed_bytes": part.compressed_bytes,
                "checksum": part.checksum,
            }
            for part in parts
        ],
    }


def download_link(job, artifact_id):
    # 先验证 Job 和有效期，再从该 Job 当前获胜的 Part 中选择对象。
    job_results(job)
    if artifact_id == "manifest":
        key = job.manifest_object_key
    else:
        part = get_object_or_404(
            ExportPart,
            pk=int(artifact_id),
            plan__job=job,
            plan__plan_version=job.current_plan_version,
            is_leaf=True,
            status=ExportPart.Status.SUCCESS,
        )
        key = part.object_key
    signed_at = timezone.now()
    remaining = int((job.expires_at - signed_at).total_seconds())
    if remaining <= 0:
        raise ExportExpired()
    ttl = min(remaining, settings.ASYNC_EXPORT_SIGNED_URL_SECONDS)
    if ttl <= 0 or not settings.ASYNC_EXPORT_ARTIFACT_STORE_FACTORY:
        raise ExportStorageUnavailable()
    try:
        store = import_string(settings.ASYNC_EXPORT_ARTIFACT_STORE_FACTORY)()
        url = store.sign_download(key, ttl)
    except Exception as error:
        raise ExportStorageUnavailable() from error
    return {"url": url, "expires_at": signed_at + timedelta(seconds=ttl)}


def operate_job(job_id, *, parallelism=None):
    try:
        if parallelism is None:
            state.cancel_job(job_id)
        else:
            state.set_parallelism(job_id, parallelism)
    except InvalidTransitionError as error:
        raise ExportConflict() from error
    return job_detail(job_id)
