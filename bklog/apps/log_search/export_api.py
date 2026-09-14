"""Authorized metadata and control operations for sharded export jobs."""

from django.conf import settings
from django.db import transaction
from django.shortcuts import get_object_or_404
from django.utils import timezone
from rest_framework.exceptions import APIException, PermissionDenied

from apps.iam import ActionEnum, ResourceEnum
from apps.iam.handlers.drf import BusinessActionPermission, IAMPermission
from apps.log_search import export_state
from apps.log_search.export_contracts import InvalidTransitionError
from apps.log_search.export_models import ExportJob, ExportPart
from apps.log_search.models import LogIndexSet, Space
from apps.utils.local import get_request_app_code, get_request_tenant_id, get_request_username


TERMINAL = {ExportJob.Status.SUCCESS, ExportJob.Status.FAILED, ExportJob.Status.CANCELED}
INFLIGHT = {ExportPart.Status.DISPATCHED, ExportPart.Status.RUNNING, ExportPart.Status.UPLOADING}


class ExportConflict(APIException):
    status_code = 409
    default_detail = "EXPORT_INVALID_STATE"
    default_code = "EXPORT_INVALID_STATE"


def authorized_job(request, job_id, space_uid, *, operate=False):
    username = get_request_username(default="")
    if not username:
        raise PermissionDenied("EXPORT_IDENTITY_REQUIRED")
    # Scope before lookup, including application identity supplied by middleware.
    job = get_object_or_404(
        ExportJob,
        pk=job_id,
        bk_tenant_id=get_request_tenant_id(),
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
    # Only metadata is exposed here. Queries, projected fields, raw errors and
    # object keys require the separate artifact/data-access authorization path.
    permissions = [
        BusinessActionPermission([ActionEnum.VIEW_BUSINESS], space_uid=space.space_uid),
        IAMPermission([ActionEnum.SEARCH_LOG], [ResourceEnum.INDICES.create_instance(index.pk)]),
    ]
    for permission in permissions:
        if not permission.has_permission(request, None):
            raise PermissionDenied()
    return space, index


def job_detail(job_id):
    # State writers acquire the same Job lock before changing leaf membership
    # or counters, so one response cannot mix two plans or a split in progress.
    with transaction.atomic():
        job = ExportJob.objects.select_for_update().get(pk=job_id)
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


def operate_job(job_id, *, parallelism=None):
    try:
        if parallelism is None:
            export_state.cancel_job(job_id)
        else:
            export_state.set_parallelism(job_id, parallelism)
    except InvalidTransitionError as error:
        raise ExportConflict() from error
    return job_detail(job_id)
