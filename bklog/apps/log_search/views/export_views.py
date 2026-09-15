"""Web 元数据与操作接口；既有 AsyncTask 路由保持原契约。"""

from rest_framework import serializers
from rest_framework.response import Response

from apps.generic import APIViewSet
from apps.log_search.export.api import authorized_job, job_detail, operate_job
from apps.log_search.export.create import create_export
from apps.log_search.export.serializers import ExportCreateSerializer
from apps.utils.drf import detail_route


class ExportScopeSerializer(serializers.Serializer):
    space_uid = serializers.CharField(max_length=256)


class ExportParallelismSerializer(ExportScopeSerializer):
    requested_parallelism = serializers.IntegerField(min_value=1, max_value=8)


class ExportJobViewSet(APIViewSet):
    serializer_class = ExportScopeSerializer
    lookup_value_regex = "[0-9]+"

    def create(self, request):
        data = self.valid_serializer(ExportCreateSerializer).validated_data
        return Response(create_export(request, data))

    def retrieve(self, request, pk=None):
        data = self.valid_serializer(self.get_serializer_class()).validated_data
        job = authorized_job(request, pk, data["space_uid"])
        return Response(job_detail(job.pk))

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
