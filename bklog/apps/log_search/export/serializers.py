from rest_framework import serializers


class ExportAdditionSerializer(serializers.Serializer):
    field = serializers.CharField()
    operator = serializers.CharField()
    value = serializers.JSONField()

    def validate_value(self, value):
        if not isinstance(value, str | list) or (
            isinstance(value, list) and any(not isinstance(v, str) for v in value)
        ):
            raise serializers.ValidationError("Expected a string or a list of strings.")
        return value


class ExportCreateSerializer(serializers.Serializer):
    space_uid = serializers.CharField(max_length=256)
    index_set_id = serializers.IntegerField(min_value=1)
    request_id = serializers.CharField(max_length=128, required=False, default=None)
    start_time = serializers.IntegerField(min_value=0)
    end_time = serializers.IntegerField(min_value=1)
    keyword = serializers.CharField(default="*", allow_blank=True)
    addition = ExportAdditionSerializer(many=True, default=list)
    ip_chooser = serializers.DictField(default=dict)
    sort_list = serializers.ListField(child=serializers.ListField(child=serializers.CharField()), default=list)
    export_fields = serializers.ListField(child=serializers.CharField(), default=list)
    requested_parallelism = serializers.IntegerField(min_value=1, max_value=8, default=4)

    def validate_sort_list(self, value):
        if any(len(item) != 2 or item[1] not in {"asc", "desc"} for item in value):
            raise serializers.ValidationError("Expected [field, asc|desc] pairs.")
        return value

    def validate(self, attrs):
        if attrs["end_time"] <= attrs["start_time"]:
            raise serializers.ValidationError("EXPORT_INVALID_TIME_RANGE")
        return attrs
