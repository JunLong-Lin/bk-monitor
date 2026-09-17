"""单索引原生查询绑定，作用域限定在一次规划/执行尝试内。"""

from contextlib import contextmanager
from copy import copy, deepcopy
from types import SimpleNamespace

from django.http import HttpRequest
from django.conf import settings

from apps.api import UnifyQueryApi
from apps.iam import ActionEnum, ResourceEnum
from apps.iam.handlers.drf import PlatformAwareIndexSearchPermission
from apps.log_search.export.contracts import PlanningError
from apps.log_search.export.query import UnifyQueryStatistics
from apps.log_search.models import LogIndexSet, Space
from apps.log_unifyquery.handler.base import UnifyQueryHandler
from apps.utils.local import activate_request, del_local_param, get_local_param, get_request, set_local_param


@contextmanager
def export_identity(job):
    previous = get_request(peaceful=True)
    missing = object()
    previous_timezone = get_local_param("time_zone", missing)
    tenant_id = Space.objects.get(space_uid=job.space_uid).bk_tenant_id
    request = HttpRequest()
    request.method = "POST"
    request.user = SimpleNamespace(username=job.created_by, tenant_id=tenant_id, is_authenticated=True)
    request.META.update(HTTP_X_BK_TENANT_ID=tenant_id, HTTP_BK_APP_CODE=job.source_app_code)
    request.data = deepcopy(job.query_snapshot["search_params"])
    request.data.update(space_uid=job.space_uid)
    request.query_params = {}
    try:
        activate_request(request)
        set_local_param("time_zone", job.query_snapshot["unify_query"].get("timezone", settings.TIME_ZONE))
        yield request
    finally:
        if previous is None:
            del_local_param("request")
        else:
            set_local_param("request", previous)
        if previous_timezone is missing:
            del_local_param("time_zone")
        else:
            set_local_param("time_zone", previous_timezone)


class NativeQuery(UnifyQueryStatistics):
    def __init__(self, job, handler):
        # DataAPI 会在对象上保存单次调用状态；这里按尝试复制副本并关闭重试，
        # 避免改动旧链路共用的 API 单例。
        self.apis = {}
        for name in ("query_ts_raw", "query_ts_reference", "query_ts_raw_with_scroll"):
            api = copy(getattr(UnifyQueryApi, name))
            api.data_api_retry_cls = None
            api.cache_time = 0
            api.use_superuser = False
            self.apis[name] = api
        self.job, self.handler = job, handler
        super().__init__(
            job,
            raw=lambda params, timeout: self.call("query_ts_raw", params, timeout),
            reference=lambda params, timeout: self.call("query_ts_reference", params, timeout),
            project=self.project_rows,
        )

    def call(self, name, params, timeout):
        params = deepcopy(params)
        # 否则 Celery 的 API 预处理会回退成后台管理员账号。
        params.update(bk_username=self.job.created_by, operator=self.job.created_by, no_request=True)
        return self.apis[name](params, timeout=timeout, request_cookies=False)

    def read(self, params, *, timeout):
        return self.call("query_ts_raw_with_scroll", params, timeout)

    def project_rows(self, response):
        return self.handler._deal_query_result(deepcopy(response))["origin_log_list"]


@contextmanager
def native_query_factory(job):
    # 不继承旧联合 Handler 的首索引脱敏逻辑，也不用首个响应的结果表
    # 拼装场景路由。
    if job.query_kind != "single" or len(job.index_set_ids) != 1:
        raise PlanningError("QUERY_MODE_NOT_IMPLEMENTED")
    if job.query_kind not in settings.ASYNC_EXPORT_VERIFIED_QUERY_KINDS:
        raise PlanningError("QUERY_PROTOCOL_NOT_VERIFIED")
    index_id = job.index_set_ids[0]
    params = deepcopy(job.query_snapshot.get("search_params", {}))
    if (
        not job.created_by
        or params.get("index_set_ids") != job.index_set_ids
        or job.resolved_resource_ids != [f"index:{index_id}"]
        or params.get("original_search")
        or params.get("is_desensitize") is False
    ):
        raise PlanningError("INVALID_QUERY_IDENTITY")
    space = Space.objects.get(space_uid=job.space_uid)
    index = LogIndexSet.objects.get(pk=index_id)
    if getattr(index, "is_group", False):
        raise PlanningError("QUERY_MODE_NOT_IMPLEMENTED")
    # 跨空间/跨平台路由在冻结资源契约实现前保持关闭，
    # 绝不能从任意首个索引推断作用范围。
    if index.space_uid != job.space_uid or params.get("bk_biz_id") != space.bk_biz_id:
        raise PlanningError("QUERY_SCOPE_MISMATCH")
    with export_identity(job) as request:
        permission = PlatformAwareIndexSearchPermission(
            [ActionEnum.SEARCH_LOG], ResourceEnum.INDICES, iam_instance_id_key="index_set_id"
        )
        view = SimpleNamespace(kwargs={"index_set_id": index_id})
        if permission.has_permission(request, view) is False:
            raise PlanningError("QUERY_PERMISSION_DENIED")
        handler = UnifyQueryHandler(params)
        # 路由、过滤或排序变化会使冻结的执行失效；重建查询不能
        # 静默改变既有 Job。
        if handler.base_dict != job.query_snapshot.get("unify_query"):
            raise PlanningError("QUERY_SNAPSHOT_CHANGED")
        if "projection" in job.query_snapshot and projection_snapshot(handler) != job.query_snapshot["projection"]:
            raise PlanningError("QUERY_PROJECTION_CHANGED")
        yield NativeQuery(job, handler)


def projection_snapshot(handler):
    """冻结生效的投影配置，包含特权用户的脱敏旁路结果。"""
    return deepcopy(
        {
            "export_fields": handler.export_fields,
            "is_desensitize": handler.is_desensitize,
            "text_fields": handler.text_fields,
            "field_configs": handler.field_configs,
            "text_fields_field_configs": handler.text_fields_field_configs,
        }
    )
