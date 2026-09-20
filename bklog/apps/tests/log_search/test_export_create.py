from copy import deepcopy
from unittest.mock import Mock, patch

from django.conf import settings
from django.test import TestCase, SimpleTestCase, override_settings
from rest_framework.exceptions import PermissionDenied, ValidationError
from rest_framework.test import APIRequestFactory

from apps.log_search.constants import ExportType
from apps.log_search.exceptions import ConcurrentExportLimitException
from apps.log_search.export.adapter import native_query_factory
from apps.log_search.export.api import ExportConflict
from apps.log_search.export.contracts import PlanningError
from apps.log_search.export.create import create_export
from apps.log_search.export.models import ExportJob
from apps.log_search.export.serializers import ExportCreateSerializer
from apps.log_search.models import AsyncTask, LogIndexSet, Space
from apps.log_search.views.export_views import ExportJobViewSet
from apps.log_unifyquery.handler.base import UnifyQueryHandler
from apps.utils.local import get_local_param, get_request


def inputs(**extra):
    serializer = ExportCreateSerializer(
        data={
            "space_uid": "bkcc__2",
            "index_set_id": 1,
            "request_id": "request-1",
            "start_time": 1700000000000,
            "end_time": 1700000060000,
            **extra,
        }
    )
    serializer.is_valid(raise_exception=True)
    return serializer.validated_data


class CreateInputTest(SimpleTestCase):
    def test_ignores_undeclared_parameters(self):
        data = inputs(
            bk_biz_id=2,
            created_by="someone_else",
            is_desensitize=False,
            begin=0,
            addition=[{"field": "service", "operator": "is", "value": "api", "unused": "ignored"}],
        )
        self.assertFalse({"bk_biz_id", "created_by", "is_desensitize", "begin"} & data.keys())
        self.assertEqual(data["addition"], [{"field": "service", "operator": "is", "value": "api"}])

    def test_invalid_ranges_sort_and_fields(self):
        for extra in (
            {"start_time": "now-1h"},
            {"end_time": 0},
            {"start_time": 1700000060000},
            {"sort_list": [["a"]]},
            {"sort_list": [["a", "wrong"]]},
            {"requested_parallelism": 9},
            {"export_fields": [{}]},
            {"addition": [{"field": "x", "operator": "is", "value": {}}]},
        ):
            with self.subTest(extra=extra), self.assertRaises(ValidationError):
                inputs(**extra)


@override_settings(
    ASYNC_EXPORT_SHARDED_ENABLED=True,
    ASYNC_EXPORT_CONTROL_ENABLED=True,
    ASYNC_EXPORT_VERIFIED_QUERY_KINDS=["single"],
    ASYNC_EXPORT_QUERY_END_MODES={"single": "exclusive"},
    ASYNC_EXPORT_ARTIFACT_STORE_FACTORY="store.factory",
    ASYNC_EXPORT_FINALIZE_TASK="export.finalize",
    ASYNC_EXPORT_GLOBAL_LIMIT=4,
    ASYNC_EXPORT_INDEX_LIMIT=4,
    ASYNC_EXPORT_LEASE_SECONDS=60,
    ASYNC_EXPORT_PLANNER_POLICY={"max_rows": 5_000_000},
    MAX_CONCURRENT_EXPORT_TASKS=3,
)
class CreateExportTest(TestCase):
    def setUp(self):
        self.space = Space.objects.create(space_uid="bkcc__2", bk_tenant_id="tenant", bk_biz_id=2)
        self.index = LogIndexSet.objects.create(index_set_id=1, space_uid="bkcc__2")
        self.mocks = {}
        patches = {
            "apps.log_search.export.create.get_request_username": {"return_value": "alice"},
            "apps.log_search.export.create.get_request_external_username": {"return_value": ""},
            "apps.log_search.export.create.get_request_app_code": {"return_value": "app"},
            "apps.log_search.export.api.get_request_tenant_id": {"return_value": "tenant"},
            "apps.log_search.export.api.get_request_username": {"return_value": "alice"},
            "apps.log_search.export.api.BusinessActionPermission.has_permission": {"return_value": True},
            "apps.log_search.export.api.IAMPermission.has_permission": {"return_value": True},
            "apps.log_search.export.create.SearchHandler.init_time_field": {
                "return_value": ("timestamp", "date", "millisecond")
            },
            "apps.log_search.export.create.UnifyQueryHandler": {"side_effect": self.handler},
        }
        for target, kwargs in patches.items():
            patcher = patch(target, **kwargs)
            self.mocks[target] = patcher.start()
            self.addCleanup(patcher.stop)
        self.last_handler = None

    def handler(self, params):
        self.assertEqual(get_request().user.username, "alice")
        self.assertEqual(get_request().META["HTTP_X_BK_TENANT_ID"], "tenant")
        self.assertEqual(get_local_param("time_zone"), settings.TIME_ZONE)
        sort = params.get("sort_list") or [["timestamp", "desc"]]
        self.last_handler = Mock(
            base_dict={
                "query_list": [{"table_id": "fixed-label", "reference_name": "a", "conditions": {}}],
                "start_time": str(params["start_time"]),
                "end_time": str(params["end_time"]),
                "timezone": settings.TIME_ZONE,
                "bk_biz_id": 2,
                "order_by": ["-timestamp"],
            },
            origin_order_by=sort,
            export_fields=params["export_fields"],
            is_desensitize=True,
            text_fields=[],
            field_configs=[{"field_name": "message", "rule_id": 1}],
            text_fields_field_configs=[],
        )
        self.last_handler.fields.return_value = {"fields": [{"field_name": "timestamp", "es_doc_values": True}]}
        return self.last_handler

    def test_real_handler_builds_matching_frozen_request(self):
        self.mocks["apps.log_search.export.create.UnifyQueryHandler"].side_effect = UnifyQueryHandler
        index_info = {
            "index_set_id": 1,
            "scenario_id": "es",
            "origin_scenario_id": "es",
            "origin_indices": "fixed",
            "index_set_obj": self.index,
            "storage_cluster_id": 1,
        }
        with (
            patch.object(UnifyQueryHandler, "_init_index_info_list", return_value=[index_info]),
            patch.object(
                UnifyQueryHandler,
                "_init_sort",
                lambda handler: handler.search_params.get("sort_list") or [["timestamp", "desc"]],
            ),
            patch.object(UnifyQueryHandler, "_init_desensitize", return_value=True),
            patch.object(
                UnifyQueryHandler,
                "fields",
                return_value={"fields": [{"field_name": "timestamp", "es_doc_values": True}]},
            ),
            patch("apps.log_unifyquery.handler.base.BaseIndexSetHandler.get_data_label", return_value="fixed-label"),
            patch(
                "apps.log_unifyquery.handler.base.UnifyQueryMappingHandler.get_all_fields_by_index_id",
                return_value=([], []),
            ),
            patch("apps.log_search.export.adapter.PlatformAwareIndexSearchPermission") as permission,
        ):
            permission.return_value.has_permission.return_value = True
            result = create_export(
                None, inputs(keyword="level:ERROR", addition=[{"field": "service", "operator": "is", "value": "api"}])
            )
            job = ExportJob.objects.get(pk=result["job_id"])
            self.assertEqual(job.query_snapshot["unify_query"]["start_time"], "1700000000000")
            self.assertEqual(job.query_snapshot["unify_query"]["query_list"][0]["query_string"], "level:ERROR")
            self.assertEqual(
                job.query_snapshot["unify_query"]["query_list"][0]["conditions"]["field_list"][0]["value"], ["api"]
            )
            with native_query_factory(job) as query:
                self.assertEqual(query.base, job.query_snapshot["unify_query"])

    def test_creates_durable_pending_snapshot_without_query_or_broker(self):
        data = inputs(export_fields=["message"])
        original = deepcopy(data)
        previous = get_request(peaceful=True)
        result = create_export(None, data)
        job = ExportJob.objects.get(pk=result["job_id"])
        self.assertEqual(data, original)
        self.assertIs(get_request(peaceful=True), previous)
        self.assertEqual((result["status"], result["estimated_total"], result["parts_total"]), ("PENDING", None, 0))
        self.assertEqual(job.query_snapshot["search_params"]["sort_list"], [["timestamp", "desc"]])
        self.assertTrue(job.query_snapshot["search_params"]["is_desensitize"])
        self.assertEqual(job.query_snapshot["projection"]["export_fields"], ["message"])
        self.assertEqual((job.time_tick, job.query_snapshot["time_units_per_second"]), (1, 1000))
        self.last_handler.pre_get_result.assert_not_called()
        self.last_handler.query_ts_raw.assert_not_called()

    def test_repeated_request_at_capacity_reuses_frozen_job_without_rebuilding(self):
        first = create_export(None, inputs())
        for _ in range(2):
            AsyncTask.objects.create(created_by="alice", export_type=ExportType.ASYNC, request_param={})
        builder = self.mocks["apps.log_search.export.create.UnifyQueryHandler"]
        builder.reset_mock()
        with override_settings(ASYNC_EXPORT_PLANNER_POLICY={"max_rows": 1}):
            repeated = create_export(None, inputs())
        self.assertEqual(first["job_id"], repeated["job_id"])
        builder.assert_not_called()
        with self.assertRaises(ExportConflict):
            create_export(None, inputs(keyword="different"))
        self.mocks["apps.log_search.export.create.get_request_app_code"].return_value = "other"
        with self.assertRaises(ExportConflict):
            create_export(None, inputs())

    def test_capacity_rejected_before_query_binding(self):
        for _ in range(3):
            AsyncTask.objects.create(created_by="alice", export_type=ExportType.ASYNC, request_param={})
        with self.assertRaises(ConcurrentExportLimitException):
            create_export(None, inputs())
        self.mocks["apps.log_search.export.create.UnifyQueryHandler"].assert_not_called()
        self.assertFalse(ExportJob.objects.exists())

    def test_quota_rechecked_after_snapshot_creation(self):
        original = self.handler

        def competing(params):
            for _ in range(3):
                AsyncTask.objects.create(created_by="alice", export_type=ExportType.ASYNC, request_param={})
            return original(params)

        self.mocks["apps.log_search.export.create.UnifyQueryHandler"].side_effect = competing
        with self.assertRaises(ConcurrentExportLimitException):
            create_export(None, inputs())
        self.assertFalse(ExportJob.objects.exists())

    def test_disabled_unverified_group_and_external_requests_fail_closed(self):
        for config in (
            {"ASYNC_EXPORT_SHARDED_ENABLED": False},
            {"ASYNC_EXPORT_CONTROL_ENABLED": False},
            {"ASYNC_EXPORT_ARTIFACT_STORE_FACTORY": ""},
            {"ASYNC_EXPORT_FINALIZE_TASK": ""},
            {"ASYNC_EXPORT_GLOBAL_LIMIT": 0},
            {"ASYNC_EXPORT_LEASE_SECONDS": 1},
            {"ASYNC_EXPORT_VERIFIED_QUERY_KINDS": []},
            {"ASYNC_EXPORT_QUERY_END_MODES": {}},
        ):
            with override_settings(**config), self.assertRaises(ValidationError):
                create_export(None, inputs())
        self.index.is_group = True
        self.index.save(update_fields=["is_group"])
        with self.assertRaises(ValidationError):
            create_export(None, inputs())
        self.mocks["apps.log_search.export.create.get_request_external_username"].return_value = "external"
        with self.assertRaises(PermissionDenied):
            create_export(None, inputs())
        self.mocks["apps.log_search.export.create.UnifyQueryHandler"].assert_not_called()

    def test_precision_and_binding_errors_do_not_leave_job(self):
        self.mocks["apps.log_search.export.create.SearchHandler.init_time_field"].return_value = (
            "time",
            "long",
            "second",
        )
        with self.assertRaises(ValidationError):
            create_export(None, inputs(start_time=1700000000001))
        self.mocks["apps.log_search.export.create.UnifyQueryHandler"].side_effect = RuntimeError("metadata failure")
        previous = get_request(peaceful=True)
        with self.assertRaises(RuntimeError):
            create_export(None, inputs())
        self.assertIs(get_request(peaceful=True), previous)
        self.assertFalse(ExportJob.objects.exists())

    def test_created_snapshot_rebinds_and_rejects_projection_drift(self):
        result = create_export(None, inputs())
        job = ExportJob.objects.get(pk=result["job_id"])
        with (
            patch("apps.log_search.export.adapter.UnifyQueryHandler", side_effect=self.handler),
            patch("apps.log_search.export.adapter.PlatformAwareIndexSearchPermission") as permission,
        ):
            permission.return_value.has_permission.return_value = True
            with native_query_factory(job) as query:
                self.assertEqual(query.request(job.start_time, job.end_time)["end_time"], str(job.end_time))
            job.query_snapshot["projection"]["is_desensitize"] = False
            with self.assertRaisesMessage(PlanningError, "QUERY_PROJECTION_CHANGED"):
                with native_query_factory(job):
                    pass

    def test_inclusive_protocol_adjusts_only_reader_end_and_group_drift_is_rejected(self):
        result = create_export(None, inputs())
        job = ExportJob.objects.get(pk=result["job_id"])
        with (
            override_settings(ASYNC_EXPORT_QUERY_END_MODES={"single": "inclusive"}),
            patch("apps.log_search.export.adapter.UnifyQueryHandler", side_effect=self.handler),
            patch("apps.log_search.export.adapter.PlatformAwareIndexSearchPermission") as permission,
        ):
            permission.return_value.has_permission.return_value = True
            with native_query_factory(job) as query:
                self.assertEqual(query.request(job.start_time, job.end_time)["end_time"], str(job.end_time - 1))
                self.assertEqual(query.base["end_time"], str(job.end_time))
            self.index.is_group = True
            self.index.save(update_fields=["is_group"])
            with self.assertRaisesMessage(PlanningError, "QUERY_MODE_NOT_IMPLEMENTED"):
                with native_query_factory(job):
                    pass

    def test_post_route_uses_strict_serializer(self):
        payload = {"space_uid": "bkcc__2", "index_set_id": 1, "start_time": 1700000000000, "end_time": 1700000060000}
        request = APIRequestFactory().post("/", payload, format="json")
        response = ExportJobViewSet.as_view({"post": "create"})(request)
        self.assertEqual(response.data["data"]["status"], "PENDING")
