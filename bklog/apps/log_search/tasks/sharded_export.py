"""Opt-in control tasks; old export queues and entry points are unchanged."""

from blueapps.core.celery.celery import app
from django.conf import settings
from django.utils.module_loading import import_string
from django_redis import get_redis_connection

from apps.log_search import export_admission
from apps.log_search.export_contracts import ExportStateError
from apps.log_search.export_coordinator import BudgetUnavailable, Coordinator, RedisBudget
from apps.log_search.export_planner import plan_job, replan_failed_part
from apps.log_search.export_worker import run_part
from apps.log_search.export_finalize import cleanup_export, finalize_export
from apps.log_search.export_files import cleanup_temporary_files
from apps.utils.log import logger


def publish_part(part):
    # Missing P4 integration is a configuration error, never a queued unknown task.
    if not settings.ASYNC_EXPORT_PART_TASK:
        raise BudgetUnavailable("Part worker task is not configured")
    app.send_task(
        settings.ASYNC_EXPORT_PART_TASK,
        args=[part.pk],
        task_id=part.task_id,
        headers={"export_generation": part.dispatch_generation, "export_lease_id": part.lease_id},
        queue=settings.ASYNC_EXPORT_OVERSIZED_QUEUE if part.oversized else settings.ASYNC_EXPORT_QUEUE,
        retry=False,
    )


def adapter_factory():
    if not settings.ASYNC_EXPORT_ADAPTER_FACTORY:
        raise BudgetUnavailable("authorized query adapter factory is not configured")
    return import_string(settings.ASYNC_EXPORT_ADAPTER_FACTORY)


def check_runtime_configuration():
    try:
        export_admission.validate_runtime_configuration()
    except ExportStateError as error:
        raise BudgetUnavailable(str(error)) from error


@app.task(bind=True, ignore_result=True, queue="sharded_async_export", acks_late=True, reject_on_worker_lost=True)
def execute_sharded_export_part(self, part_id):
    if not settings.ASYNC_EXPORT_CONTROL_ENABLED:
        return
    headers = self.request.headers or {}
    generation, owner = headers.get("export_generation"), headers.get("export_lease_id")
    if type(generation) is not int or generation < 1 or not isinstance(owner, str) or not owner:
        return
    if not settings.ASYNC_EXPORT_ARTIFACT_STORE_FACTORY:
        raise BudgetUnavailable("Part artifact store is not configured")
    cleanup_temporary_files()
    run_part(
        part_id,
        generation=generation,
        lease_id=owner,
        query_factory=adapter_factory(),
        budget=RedisBudget(get_redis_connection("default"), settings.ASYNC_EXPORT_NAMESPACE),
        store=import_string(settings.ASYNC_EXPORT_ARTIFACT_STORE_FACTORY)(),
    )


@app.task(ignore_result=True, queue="sharded_async_export_control")
def plan_sharded_export(job_id):
    if settings.ASYNC_EXPORT_CONTROL_ENABLED:
        plan_job(job_id, adapter_factory())


@app.task(ignore_result=True, queue="sharded_async_export_control")
def split_sharded_export(part_id):
    if settings.ASYNC_EXPORT_CONTROL_ENABLED:
        replan_failed_part(part_id, adapter_factory())


@app.task(ignore_result=True, queue="sharded_async_export_control")
def finalize_sharded_export(job_id):
    if settings.ASYNC_EXPORT_CONTROL_ENABLED:
        cleanup_temporary_files()
        finalize_export(job_id, import_string(settings.ASYNC_EXPORT_ARTIFACT_STORE_FACTORY)())


@app.task(ignore_result=True, queue="sharded_async_export_control")
def cleanup_sharded_export(job_id):
    if settings.ASYNC_EXPORT_CONTROL_ENABLED:
        cleanup_export(job_id, import_string(settings.ASYNC_EXPORT_ARTIFACT_STORE_FACTORY)())


@app.task(ignore_result=True, queue="sharded_async_export_control")
def coordinate_sharded_exports():
    """Install a periodic schedule only after enabling and configuring the route.

    Repeated scans repair missing planning, dispatch, split and finalization
    messages. Workers must use the generation/lease headers, not just part_id.
    """
    if not settings.ASYNC_EXPORT_CONTROL_ENABLED:
        return
    try:
        check_runtime_configuration()
        adapter_factory()
        coordinator = Coordinator(
            RedisBudget(get_redis_connection("default"), settings.ASYNC_EXPORT_NAMESPACE), publish_part
        )
        limit = settings.ASYNC_EXPORT_SCAN_LIMIT
        retained = coordinator.recover_expired(limit=limit)
        if retained:
            logger.warning("sharded export recovery retained %s unconfirmed executions", len(retained))
        for kind, identifier in coordinator.control_work(limit, finalize=bool(settings.ASYNC_EXPORT_FINALIZE_TASK)):
            if kind == "finalize":
                app.send_task(
                    settings.ASYNC_EXPORT_FINALIZE_TASK,
                    args=[identifier],
                    queue=settings.ASYNC_EXPORT_CONTROL_QUEUE,
                    retry=False,
                )
            else:
                task = plan_sharded_export if kind == "plan" else split_sharded_export
                task.apply_async(args=[identifier], queue=settings.ASYNC_EXPORT_CONTROL_QUEUE, retry=False)
        coordinator.replay(limit=limit)
        coordinator.tick(max_dispatches=limit)
        if settings.ASYNC_EXPORT_FINALIZE_TASK:
            for job_id in coordinator.cleanup_jobs(limit):
                cleanup_sharded_export.apply_async(
                    args=[job_id], queue=settings.ASYNC_EXPORT_CONTROL_QUEUE, retry=False
                )
    except BudgetUnavailable:
        logger.warning("sharded export dispatch paused: budget reconciliation unavailable")
