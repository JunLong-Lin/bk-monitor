"""Build trusted single-index snapshots before durable, shared admission."""

import hashlib
import json
from copy import deepcopy
from dataclasses import asdict
from types import SimpleNamespace

from django.conf import settings
from rest_framework.exceptions import PermissionDenied, ValidationError

from apps.log_search import export_admission
from apps.log_search.export_adapter import export_identity, projection_snapshot
from apps.log_search.export_api import ExportConflict, authorized_scope, job_detail
from apps.log_search.export_contracts import ExportStateError, PlannerPolicy
from apps.log_search.export_models import ExportJob
from apps.log_search.handlers.search.search_handlers_esquery import SearchHandler
from apps.log_search.models import AsyncTask
from apps.log_unifyquery.handler.base import UnifyQueryHandler
from apps.utils.local import (
    get_request_app_code,
    get_request_external_username,
    get_request_tenant_id,
    get_request_username,
)


def digest(value):
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    ).hexdigest()


def create_export(request, data):
    username = get_request_username(default="")
    if not username or get_request_external_username():
        raise PermissionDenied("EXPORT_WEB_IDENTITY_REQUIRED")
    if not settings.ASYNC_EXPORT_SHARDED_ENABLED:
        raise ValidationError("EXPORT_ROUTE_DISABLED")
    try:
        export_admission.validate_runtime_configuration()
    except ExportStateError as error:
        raise ValidationError("EXPORT_RUNTIME_NOT_READY") from error
    end_mode = settings.ASYNC_EXPORT_QUERY_END_MODES.get("single")
    if "single" not in settings.ASYNC_EXPORT_VERIFIED_QUERY_KINDS or end_mode not in {"inclusive", "exclusive"}:
        raise ValidationError("QUERY_PROTOCOL_NOT_VERIFIED")
    space, index = authorized_scope(request, data["space_uid"], data["index_set_id"])
    if index.is_group:
        raise ValidationError("QUERY_MODE_NOT_IMPLEMENTED")
    identity = dict(bk_tenant_id=get_request_tenant_id(), space_uid=space.space_uid, created_by=username)
    source_app = get_request_app_code()
    request_hash = digest({"input": data, "source_app_code": source_app})
    # Check before Handler initialization (which can resolve metadata remotely).
    # Final admission rechecks under the same lock after building the snapshot.
    with export_admission.admission_lock(username):
        if data["request_id"]:
            existing = ExportJob.objects.filter(**identity, request_id=data["request_id"]).first()
            if existing:
                if (
                    existing.source_app_code != source_app
                    or existing.query_snapshot.get("request_hash") != request_hash
                ):
                    raise ExportConflict("EXPORT_REQUEST_ID_CONFLICT")
                return job_detail(existing.pk)
        AsyncTask.check_running_count_by_user(username)

    params = {
        key: deepcopy(data[key])
        for key in ("start_time", "end_time", "keyword", "addition", "ip_chooser", "sort_list", "export_fields")
    }
    params.update(index_set_ids=[index.pk], bk_biz_id=space.bk_biz_id, is_desensitize=True, interval="30s")
    context = SimpleNamespace(
        **identity,
        source_app_code=source_app,
        query_snapshot={"search_params": params, "unify_query": {"timezone": data["time_zone"]}},
    )
    with export_identity(context):
        handler = UnifyQueryHandler(deepcopy(params))
        # Freeze the resolved default/user sort so later preferences cannot
        # silently change the task. Validate explicit and resolved sorts alike.
        sort_list = deepcopy(handler.origin_order_by)
        if sort_list:
            handler.check_sort_list(handler.fields()["fields"], sort_list)
        params["sort_list"] = sort_list
        _, _, unit = SearchHandler.init_time_field(index.pk)
        tick = {"second": 1000, "millisecond": 1}.get(unit)
        if tick is None or data["start_time"] % tick or data["end_time"] % tick:
            raise ValidationError("UNSUPPORTED_EXPORT_TIME_PRECISION")
        base = deepcopy(handler.base_dict)
        if not base.get("query_list"):
            raise ValidationError("INCOMPLETE_ROUTING_SNAPSHOT")
        projection = projection_snapshot(handler)
    policy = PlannerPolicy.for_job(SimpleNamespace(policy_snapshot={}))
    query_snapshot = {
        "search_params": params,
        "unify_query": base,
        "time_units_per_second": 1000,
        "export_file_type": data["file_type"],
        "projection": projection,
        "request_hash": request_hash,
    }
    values = dict(
        **identity,
        source_app_code=source_app,
        request_id=data["request_id"],
        query_kind="single",
        index_set_ids=[index.pk],
        resolved_resource_ids=[f"index:{index.pk}"],
        query_snapshot=query_snapshot,
        routing_snapshot={"query_list": deepcopy(base["query_list"])},
        policy_snapshot={"query_end_mode": end_mode, "planner": asdict(policy)},
        start_time=data["start_time"],
        end_time=data["end_time"],
        time_tick=tick,
        requested_parallelism=data["requested_parallelism"],
    )
    values["query_hash"] = digest({key: value for key, value in values.items() if key != "request_id"})
    try:
        job = export_admission.create_job(**values)
    except ExportStateError as error:
        raise ExportConflict("EXPORT_CREATE_CONFLICT") from error
    # PENDING is durable work discovered by Coordinator. No broker publication
    # or count query occurs in this request, including the commit failure path.
    return job_detail(job.pk)
