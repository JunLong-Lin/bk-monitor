from contextlib import AbstractContextManager

from apps.log_search.export.models import ExportJob


def create_job(**extra):
    values = dict(
        space_uid="space",
        created_by="alice",
        query_kind="single",
        query_snapshot={"time_units_per_second": 1},
        query_hash="a" * 64,
        start_time=0,
        end_time=60,
        time_tick=1,
        resolved_resource_ids=["index:1"],
    )
    values.update(extra)
    return ExportJob.objects.create(**values)


class Distribution(AbstractContextManager):
    def __init__(self, points, row_bytes=10):
        self.points, self.row_bytes = points, row_bytes
        self.calls = []

    def __exit__(self, *args):
        return False

    def count(self, start, end, *, timeout):
        self.calls.append(("count", start, end, timeout))
        return sum(count for timestamp, count in self.points.items() if start <= timestamp < end)

    def histogram(self, start, end, interval, *, timeout):
        self.calls.append(("histogram", start, end, timeout))
        buckets = {}
        for timestamp, count in self.points.items():
            if start <= timestamp < end:
                key = timestamp // interval * interval
                buckets[key] = buckets.get(key, 0) + count
        return buckets

    def sample(self, start, end, limit, *, timeout):
        self.calls.append(("sample", start, end, timeout))
        return [b"x" * self.row_bytes]
