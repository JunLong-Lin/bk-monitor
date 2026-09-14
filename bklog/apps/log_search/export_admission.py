"""Shared admission for old and new jobs when the sharded route is enabled."""

import hashlib
from contextlib import contextmanager

from django.conf import settings
from django.db import transaction

from apps.log_search.export_models import ExportDispatchGate, ExportJob
from apps.log_search.export_contracts import ExportStateError


def validate_runtime_configuration():
    """Reject activation unless an admitted Job can reach a terminal state."""
    required = {
        "ASYNC_EXPORT_CONTROL_ENABLED": settings.ASYNC_EXPORT_CONTROL_ENABLED,
        "ASYNC_EXPORT_ADAPTER_FACTORY": settings.ASYNC_EXPORT_ADAPTER_FACTORY,
        "ASYNC_EXPORT_PART_TASK": settings.ASYNC_EXPORT_PART_TASK,
        "ASYNC_EXPORT_ARTIFACT_STORE_FACTORY": settings.ASYNC_EXPORT_ARTIFACT_STORE_FACTORY,
        "ASYNC_EXPORT_FINALIZE_TASK": settings.ASYNC_EXPORT_FINALIZE_TASK,
    }
    invalid = [name for name, value in required.items() if not value]
    positive_integers = {
        "ASYNC_EXPORT_GLOBAL_LIMIT": settings.ASYNC_EXPORT_GLOBAL_LIMIT,
        "ASYNC_EXPORT_INDEX_LIMIT": settings.ASYNC_EXPORT_INDEX_LIMIT,
        "ASYNC_EXPORT_LEASE_SECONDS": settings.ASYNC_EXPORT_LEASE_SECONDS,
    }
    invalid.extend(name for name, value in positive_integers.items() if type(value) is not int or value <= 0)
    if invalid:
        raise ExportStateError(f"export runtime is not ready: {','.join(sorted(invalid))}")


@contextmanager
def admission_lock(username, is_scene=False):
    # Match the existing old-export username/group scope, including its
    # cross-space grouping, so new jobs cannot receive three extra slots.
    identity = hashlib.sha256(username.encode()).hexdigest()
    namespace = f"admission:{int(is_scene)}:{identity}"
    ExportDispatchGate.objects.get_or_create(namespace=namespace)
    with transaction.atomic():
        ExportDispatchGate.objects.select_for_update().get(pk=namespace)
        yield


def active_job_count(username, is_scene=False):
    jobs = ExportJob.objects.filter(
        created_by=username,
        status__in=[
            ExportJob.Status.PENDING,
            ExportJob.Status.PLANNING,
            ExportJob.Status.READY,
            ExportJob.Status.RUNNING,
        ],
    )
    jobs = (
        jobs.filter(query_kind=ExportJob.QueryKind.SCENE)
        if is_scene
        else jobs.exclude(query_kind=ExportJob.QueryKind.SCENE)
    )
    return jobs.count()


def create_job(**values):
    """Call after API permission checks and construction of trusted snapshots."""
    if not settings.ASYNC_EXPORT_SHARDED_ENABLED:
        raise ExportStateError("sharded export is disabled")
    from apps.log_search.models import AsyncTask

    with admission_lock(values["created_by"], values["query_kind"] == ExportJob.QueryKind.SCENE):
        if values.get("request_id"):
            existing = ExportJob.objects.filter(
                **{key: values[key] for key in ("bk_tenant_id", "space_uid", "created_by", "request_id")}
            ).first()
            if existing:
                if existing.query_hash != values["query_hash"]:
                    raise ExportStateError("request_id already belongs to a different query")
                return existing
        AsyncTask.check_running_count_by_user(
            values["created_by"], is_scene=values["query_kind"] == ExportJob.QueryKind.SCENE
        )
        job = ExportJob(**values)
        job.full_clean()
        job.save()
        return job
