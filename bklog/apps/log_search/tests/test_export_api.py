from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import patch

from django.http import Http404
from django.test import TestCase, override_settings
from django.utils import timezone
from rest_framework.exceptions import PermissionDenied, ValidationError
from rest_framework.test import APIRequestFactory
from rest_framework.routers import SimpleRouter

from apps.log_search import export_state as state
from apps.log_search.export_api import ExportConflict, authorized_job, job_detail, operate_job
from apps.log_search.export_models import ExportJob, ExportPart
from apps.log_search.models import LogIndexSet, Space
from apps.log_search.tests.export_fixtures import create_job
from apps.log_search.views.export_views import ExportJobViewSet, ExportParallelismSerializer


@override_settings(ASYNC_EXPORT_INDEX_LIMIT=4, ASYNC_EXPORT_GLOBAL_LIMIT=16)
class ExportAPITest(TestCase):
    def setUp(self):
        self.job = create_job(index_set_ids=[1], source_app_code="app")
        for name, value in [
            ("get_request_username", "alice"),
            ("get_request_tenant_id", "tenant"),
            ("get_request_app_code", "app"),
        ]:
            mock = patch(f"apps.log_search.export_api.{name}", return_value=value)
            setattr(self, name, mock.start())
            self.addCleanup(mock.stop)

    def plan(self):
        state.begin_planning(self.job.pk)
        return state.persist_plan(
            self.job.pk,
            attempt=state.claim_planning(job_id=self.job.pk),
            query_hash=self.job.query_hash,
            target_rows=100,
            target_bytes=100,
            histogram_interval=30,
            parts=[state.PartSpec(1, 0, 30, 5, 10), state.PartSpec(2, 30, 60, 5, 10)],
        )

    def test_progress_uses_current_successful_leaves_and_attempt_rows(self):
        plan = self.plan()
        first, second = list(plan.parts.order_by("part_no"))
        ExportPart.objects.filter(pk=first.pk).update(status="SUCCESS", actual_rows=7, processed_rows=999)
        ExportPart.objects.filter(pk=second.pk).update(status="RUNNING", processed_rows=4, stage="PACKAGE")
        detail = job_detail(self.job.pk)
        self.assertEqual((detail["actual_total"], detail["processed_rows"], detail["percent"]), (7, 11, 50))
        self.assertEqual(detail["stage"], "PACKAGE")
        ExportPart.objects.filter(pk=second.pk).update(status="FAILED")
        self.assertEqual(job_detail(self.job.pk)["processed_rows"], 7)
        ExportPart.objects.filter(pk=second.pk).update(status="SUCCESS", actual_rows=6)
        detail = job_detail(self.job.pk)
        self.assertEqual((detail["actual_total"], detail["percent"], detail["stage"]), (13, 99, "FINALIZING"))

    def test_split_parent_and_inactive_plan_are_excluded(self):
        plan = self.plan()
        parent = plan.parts.first()
        ExportPart.objects.filter(pk=parent.pk).update(status="SPLIT", is_leaf=False, actual_rows=999)
        detail = job_detail(self.job.pk)
        self.assertEqual((detail["parts_total"], detail["actual_total"]), (1, 0))
        ExportJob.objects.filter(pk=self.job.pk).update(current_plan_version=None)
        self.assertEqual(job_detail(self.job.pk)["parts_total"], 0)

    def test_expiry_preserves_execution_status_and_does_not_expose_secrets(self):
        ExportJob.objects.filter(pk=self.job.pk).update(
            status="SUCCESS",
            expires_at=timezone.now() - timedelta(seconds=1),
            manifest_object_key="private-key",
            error_detail="private-query",
        )
        detail = job_detail(self.job.pk)
        self.assertEqual((detail["status"], detail["execution_status"], detail["percent"]), ("EXPIRED", "SUCCESS", 100))
        self.assertIsNone(detail["poll_after"])
        self.assertNotIn("private", str(detail))

    def test_cancel_idempotent_and_terminal_parallelism_conflicts(self):
        first = operate_job(self.job.pk)
        second = operate_job(self.job.pk)
        self.assertEqual(first["state_version"], second["state_version"])
        self.assertEqual(second["status"], "CANCELED")
        with self.assertRaises(ExportConflict):
            operate_job(self.job.pk, parallelism=2)

    def test_parallelism_does_not_claim_to_be_actual_inflight(self):
        detail = operate_job(self.job.pk, parallelism=8)
        self.assertEqual(detail["requested_parallelism"], 8)
        self.assertEqual(detail["configured_parallelism_limit"], 4)
        self.assertEqual(detail["inflight_parts"], 0)

    def test_scope_mismatch_is_not_found_before_iam(self):
        for getter in (self.get_request_tenant_id, self.get_request_app_code):
            original = getter.return_value
            getter.return_value = "other"
            with self.assertRaises(Http404):
                authorized_job(None, self.job.pk, "space")
            getter.return_value = original
        with self.assertRaises(Http404):
            authorized_job(None, self.job.pk, "other-space")

    def test_missing_identity_and_unimplemented_mode_are_denied(self):
        self.get_request_username.return_value = ""
        with self.assertRaises(PermissionDenied):
            authorized_job(None, self.job.pk, "space")
        self.get_request_username.return_value = "alice"
        Space.objects.create(space_uid="space", bk_tenant_id="tenant", bk_biz_id=1)
        ExportJob.objects.filter(pk=self.job.pk).update(query_kind="union")
        with self.assertRaises(PermissionDenied):
            authorized_job(None, self.job.pk, "space")

    def test_shared_metadata_requires_current_permissions_and_creator_for_operations(self):
        self.get_request_username.return_value = "bob"
        Space.objects.create(space_uid="space", bk_tenant_id="tenant", bk_biz_id=1)
        LogIndexSet.objects.create(index_set_id=1, space_uid="space")
        with (
            patch(
                "apps.log_search.export_api.BusinessActionPermission.has_permission", return_value=True
            ) as space_permission,
            patch("apps.log_search.export_api.IAMPermission.has_permission", return_value=True) as permission,
        ):
            self.assertEqual(authorized_job(None, self.job.pk, "space").pk, self.job.pk)
            self.assertFalse(job_detail(self.job.pk)["can_operate"])
            with self.assertRaises(PermissionDenied):
                authorized_job(None, self.job.pk, "space", operate=True)
            permission.return_value = False
            with self.assertRaises(PermissionDenied):
                authorized_job(None, self.job.pk, "space")
            permission.return_value = True
            space_permission.return_value = False
            with self.assertRaises(PermissionDenied):
                authorized_job(None, self.job.pk, "space")

    def test_router_and_request_validation(self):
        router = SimpleRouter()
        router.register("search/export_jobs", ExportJobViewSet, basename="export_jobs")
        self.assertEqual(
            {url.name for url in router.urls},
            {"export_jobs-list", "export_jobs-detail", "export_jobs-cancel", "export_jobs-parallelism"},
        )
        for value in (0, 9, True, "bad", 1.5):
            serializer = ExportParallelismSerializer(data={"space_uid": "space", "requested_parallelism": value})
            self.assertFalse(serializer.is_valid())
        request = APIRequestFactory().get("/", {"space_uid": "space"})
        view = ExportJobViewSet.as_view({"get": "retrieve"})
        with patch("apps.log_search.views.export_views.authorized_job", return_value=self.job):
            response = view(request, pk=str(self.job.pk))
        self.assertEqual(response.data["data"]["job_id"], self.job.pk)

    def test_control_views_validate_and_authorize_before_mutation(self):
        factory = APIRequestFactory()
        request = factory.patch("/", {"space_uid": "space", "requested_parallelism": 8}, format="json")
        view = ExportJobViewSet.as_view({"patch": "parallelism"}, serializer_class=ExportParallelismSerializer)
        with patch("apps.log_search.views.export_views.authorized_job", return_value=self.job) as authorize:
            response = view(request, pk=str(self.job.pk))
        self.assertTrue(authorize.call_args.kwargs["operate"])
        self.assertEqual(response.data["data"]["requested_parallelism"], 8)
        view = ExportJobViewSet()
        view.request = SimpleNamespace(method="POST", data={})
        with self.assertRaises(ValidationError):
            view.cancel(view.request, pk=str(self.job.pk))
