from django.core.management.base import BaseCommand

from apps.log_search.export.files import cleanup_temporary_files


class Command(BaseCommand):
    help = "清理本机残留的分片导出临时文件（进程锁会保护正在执行的尝试）。"

    def handle(self, *args, **options):
        self.stdout.write(f"Removed {cleanup_temporary_files()} abandoned export directories")
