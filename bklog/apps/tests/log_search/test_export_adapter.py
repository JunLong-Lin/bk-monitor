from copy import deepcopy
import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from types import SimpleNamespace
from unittest.mock import Mock, patch

from django.test import SimpleTestCase, override_settings

from apps.api.modules.unify_query import _UnifyQueryApi
from apps.log_search.export.adapter import NativeQuery, export_identity, native_query_factory
from apps.log_search.export.contracts import PlanningError
from apps.log_unifyquery.handler.base import UnifyQueryHandler
from apps.utils.local import activate_request, del_local_param, get_local_param, get_request, set_local_param


@override_settings(ASYNC_EXPORT_VERIFIED_QUERY_KINDS=["single"])
class NativeAdapterTest(SimpleTestCase):
    def setUp(self):
        self.base = {
            "query_list": [{"reference_name": "a", "table_id": "fixed"}],
            "start_time": "0",
            "end_time": "60",
            "order_by": ["-timestamp"],
            "bk_biz_id": 2,
        }
        self.job = SimpleNamespace(
            query_kind="single",
            index_set_ids=[1],
            created_by="alice",
            space_uid="bkcc__2",
            source_app_code="bk_log",
            resolved_resource_ids=["index:1"],
            time_tick=1,
            policy_snapshot={"query_end_mode": "exclusive"},
            query_snapshot={
                "time_units_per_second": 1,
                "unify_query": self.base,
                "search_params": {"index_set_ids": [1], "bk_biz_id": 2},
            },
            routing_snapshot={"query_list": deepcopy(self.base["query_list"])},
        )
        space = patch(
            "apps.log_search.export.adapter.Space.objects.get",
            return_value=SimpleNamespace(bk_tenant_id="tenant", bk_biz_id=2),
        )
        space.start()
        self.addCleanup(space.stop)

    def test_identity_restored_on_failure_and_absent_context_removed(self):
        previous = get_request(peaceful=True)
        outer = SimpleNamespace(request_id="outer")
        activate_request(outer, "outer")
        try:
            with self.assertRaises(RuntimeError), export_identity(self.job):
                self.assertEqual(get_request().user.username, "alice")
                self.assertEqual(get_request().META["HTTP_X_BK_TENANT_ID"], "tenant")
                raise RuntimeError()
            self.assertIs(get_request(), outer)
            self.assertEqual(outer.request_id, "outer")
            del_local_param("request")
            with export_identity(self.job):
                pass
            self.assertIsNone(get_request(peaceful=True))
        finally:
            if previous is not None:
                activate_request(previous, previous.request_id)

    def test_native_transport_uses_api_tenant_resolution_and_preserves_api_singleton(self):
        apis = _UnifyQueryApi()
        apis.query_ts_raw_with_scroll.data_api_retry_cls = object()
        original_retry = apis.query_ts_raw_with_scroll.data_api_retry_cls
        with patch("apps.log_search.export.adapter.UnifyQueryApi", apis):
            query = NativeQuery(self.job, Mock())
        native = query.apis["query_ts_raw_with_scroll"]
        self.assertIsNone(native.data_api_retry_cls)
        self.assertIs(apis.query_ts_raw_with_scroll.data_api_retry_cls, original_retry)
        response = Mock(is_success=Mock(return_value=True), data={"list": [], "done": True})
        params = query.request(10, 20)
        with export_identity(self.job), patch.object(native, "_send_request", return_value=response) as send:
            self.assertTrue(query.read(params, timeout=2.5)["done"])
            self.assertEqual(send.call_args.args[1], 2.5)
            self.assertEqual(send.call_args.args[-2:], (False, ""))
            self.assertEqual(send.call_args.args[0]["bk_username"], "alice")
        self.assertEqual(params["end_time"], "20")
        self.assertEqual(self.base["end_time"], "60")

    def test_real_data_api_http_preserves_creator_and_scope(self):
        received = []

        class Endpoint(BaseHTTPRequestHandler):
            def do_POST(self):
                received.append((dict(self.headers), json.loads(self.rfile.read(int(self.headers["Content-Length"])))))
                payload = b'{"list": [], "done": true}'
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, *args):
                pass

        server = HTTPServer(("127.0.0.1", 0), Endpoint)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with patch("apps.log_search.export.adapter.UnifyQueryApi", _UnifyQueryApi()):
                query = NativeQuery(self.job, Mock())
            query.apis["query_ts_raw_with_scroll"].url = f"http://127.0.0.1:{server.server_port}/raw_with_scroll/"
            with patch("apps.log_search.models.Space.get_tenant_id", return_value="tenant") as resolve_tenant:
                with export_identity(self.job):
                    result = query.read(query.request(10, 20), timeout=2)
                resolve_tenant.assert_called_with(bk_biz_id=2)
            self.assertEqual(result, {"list": [], "done": True})
            headers, body = received[0]
            headers = {key.lower(): value for key, value in headers.items()}
            self.assertEqual(json.loads(headers["x-bkapi-authorization"])["bk_username"], "alice")
            self.assertEqual(headers["bk-query-source"], "username:alice")
            self.assertEqual(headers["x-bk-scope-space-uid"], "bkcc__2")
            self.assertEqual(headers["x-bk-tenant-id"], "tenant")
            self.assertEqual(body["slice_max"], 0)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

    def test_timezone_is_scoped_to_frozen_query(self):
        previous = get_local_param("time_zone")
        set_local_param("time_zone", "Asia/Shanghai")
        self.job.query_snapshot["unify_query"]["timezone"] = "UTC"
        try:
            with export_identity(self.job):
                self.assertEqual(get_local_param("time_zone"), "UTC")
            self.assertEqual(get_local_param("time_zone"), "Asia/Shanghai")
        finally:
            if previous is None:
                del_local_param("time_zone")
            else:
                set_local_param("time_zone", previous)

    def test_malformed_snapshot_does_not_leak_worker_identity(self):
        previous = get_request(peaceful=True)
        self.job.query_snapshot.pop("unify_query")
        with self.assertRaises(KeyError):
            with export_identity(self.job):
                pass
        self.assertIs(get_request(peaceful=True), previous)

    def factory_patches(self):
        handler = Mock(base_dict=deepcopy(self.base))
        handler._deal_query_result.side_effect = lambda response: {"origin_log_list": [{"log": "masked"}]}
        return patch.multiple(
            "apps.log_search.export.adapter",
            Space=Mock(objects=Mock(get=Mock(return_value=SimpleNamespace(bk_tenant_id="tenant", bk_biz_id=2)))),
            LogIndexSet=Mock(objects=Mock(get=Mock(return_value=SimpleNamespace(space_uid="bkcc__2")))),
            PlatformAwareIndexSearchPermission=Mock(),
            UnifyQueryHandler=Mock(return_value=handler),
        ), handler

    def test_factory_uses_current_authorization_and_shared_projection(self):
        patches, handler = self.factory_patches()
        with patches, native_query_factory(self.job) as query:
            self.assertEqual(query.project({"list": [{"log": "secret"}]}), [{"log": "masked"}])
            self.assertIs(query.handler, handler)

    def test_sample_and_worker_use_existing_desensitization_and_projection(self):
        handler = UnifyQueryHandler.__new__(UnifyQueryHandler)
        handler.field_configs = ["configured"]
        handler.text_fields_field_configs = []
        handler.is_desensitize = True
        handler.index_set_ids = [1]
        handler.export_fields = ["message"]
        handler._log_desensitize = Mock(side_effect=lambda row: {**row, "message": "masked"})
        handler._add_cmdb_fields = lambda row: row
        handler._add_bcs_cluster_fields = lambda row: row
        with patch("apps.log_search.export.adapter.UnifyQueryApi", _UnifyQueryApi()):
            query = NativeQuery(self.job, handler)
        response = {"list": [{"message": "secret", "unselected": "hidden"}]}
        query.raw = Mock(return_value=response)
        sample = query.sample(0, 60, 1, timeout=1)
        projected = query.project(response)
        self.assertEqual(json.loads(sample[0]), {"message": "masked"})
        self.assertEqual(projected, [{"message": "masked"}])
        self.assertEqual(handler._log_desensitize.call_count, 2)
        self.assertEqual(response["list"][0]["message"], "secret")

    def test_changed_route_rejected_and_context_restored(self):
        patches, handler = self.factory_patches()
        handler.base_dict["query_list"] = [{"table_id": "changed"}]
        previous = get_request(peaceful=True)
        with patches, self.assertRaisesMessage(PlanningError, "QUERY_SNAPSHOT_CHANGED"):
            with native_query_factory(self.job):
                self.fail("changed route accepted")
        self.assertIs(get_request(peaceful=True), previous)

    def test_permission_denial_prevents_handler_and_transport(self):
        patches, _ = self.factory_patches()
        with patches, patch("apps.log_search.export.adapter.PlatformAwareIndexSearchPermission") as permission:
            permission.return_value.has_permission.return_value = False
            with self.assertRaisesMessage(PlanningError, "QUERY_PERMISSION_DENIED"):
                with native_query_factory(self.job):
                    self.fail("permission denial ignored")

    def test_scope_mismatch_and_unsupported_modes_fail_closed(self):
        patches, _ = self.factory_patches()
        self.job.query_snapshot["search_params"]["bk_biz_id"] = 3
        with patches, self.assertRaisesMessage(PlanningError, "QUERY_SCOPE_MISMATCH"):
            with native_query_factory(self.job):
                pass
        for kind in ["union", "scene"]:
            self.job.query_kind = kind
            with self.assertRaisesMessage(PlanningError, "QUERY_MODE_NOT_IMPLEMENTED"):
                with native_query_factory(self.job):
                    pass
