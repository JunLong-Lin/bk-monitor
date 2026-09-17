"""Web 元数据与操作接口；既有 AsyncTask 路由保持原契约。"""

from django.http import Http404
from rest_framework import serializers
from rest_framework.exceptions import PermissionDenied
from rest_framework.response import Response

from apps.generic import APIViewSet
from apps.log_search.export.api import authorized_job, download_link, job_detail, job_results, operate_job
from apps.log_search.export.create import create_export
from apps.log_search.export.models import ExportJob
from apps.log_search.export.serializers import ExportCreateSerializer
from apps.utils.drf import detail_route
from apps.utils.local import get_request_app_code


class ExportScopeSerializer(serializers.Serializer):
    space_uid = serializers.CharField(max_length=256)


class ExportParallelismSerializer(ExportScopeSerializer):
    requested_parallelism = serializers.IntegerField(min_value=1, max_value=8)


class ExportListSerializer(ExportScopeSerializer):
    page = serializers.IntegerField(min_value=1, default=1)
    limit = serializers.IntegerField(min_value=1, max_value=100, default=20)


class ExportLinkSerializer(ExportScopeSerializer):
    artifact_id = serializers.CharField(max_length=32)

    def validate_artifact_id(self, value):
        if value != "manifest" and (not value.isdecimal() or int(value) < 1):
            raise serializers.ValidationError("EXPORT_INVALID_ARTIFACT_ID")
        return value


class ExportJobViewSet(APIViewSet):
    serializer_class = ExportScopeSerializer
    lookup_value_regex = "[0-9]+"

    def list(self, request):
        data = self.valid_serializer(ExportListSerializer).validated_data
        queryset = ExportJob.objects.filter(
            space_uid=data["space_uid"],
            source_app_code=get_request_app_code(),
        ).order_by("-created_at", "-pk")
        offset = (data["page"] - 1) * data["limit"]
        jobs = queryset[offset : offset + data["limit"]]
        # 每条记录重新验证当前空间和索引集权限；无权记录不泄露元数据。
        results = []
        for job in jobs:
            try:
                authorized_job(request, job.pk, data["space_uid"])
            except (Http404, PermissionDenied):
                continue
            results.append(job_detail(job.pk))
        return Response({"page": data["page"], "limit": data["limit"], "results": results})

    def create(self, request):
        data = self.valid_serializer(ExportCreateSerializer).validated_data
        return Response(create_export(request, data))

    def retrieve(self, request, pk=None):
        data = self.valid_serializer(self.get_serializer_class()).validated_data
        job = authorized_job(request, pk, data["space_uid"])
        return Response(job_detail(job.pk))

    @detail_route(methods=["GET"])
    def results(self, request, pk=None):
        data = self.valid_serializer(self.get_serializer_class()).validated_data
        job = authorized_job(request, pk, data["space_uid"])
        return Response(job_results(job))

    @detail_route(methods=["GET"], serializer_class=ExportLinkSerializer)
    def download_link(self, request, pk=None):
        data = self.valid_serializer(self.get_serializer_class()).validated_data
        job = authorized_job(request, pk, data["space_uid"])
        return Response(download_link(job, data["artifact_id"]))

    @detail_route(methods=["POST"])
    def cancel(self, request, pk=None):
        data = self.valid_serializer(self.get_serializer_class()).validated_data
        job = authorized_job(request, pk, data["space_uid"], operate=True)
        return Response(operate_job(job.pk))

    @detail_route(methods=["PATCH"], serializer_class=ExportParallelismSerializer)
    def parallelism(self, request, pk=None):
        data = self.valid_serializer(self.get_serializer_class()).validated_data
        job = authorized_job(request, pk, data["space_uid"], operate=True)
        return Response(operate_job(job.pk, parallelism=data["requested_parallelism"]))
