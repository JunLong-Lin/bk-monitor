from django.core.management.base import BaseCommand

from apps.log_search.export_files import cleanup_temporary_files


class Command(BaseCommand):
    help = "Remove abandoned sharded-export files on this worker host (process locks protect active attempts)."

    def handle(self, *args, **options):
        self.stdout.write(f"Removed {cleanup_temporary_files()} abandoned export directories")
