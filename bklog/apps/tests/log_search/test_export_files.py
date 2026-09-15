import os
import tempfile
import time
from pathlib import Path

from django.test import SimpleTestCase, override_settings

from apps.log_search.export.files import cleanup_temporary_files, export_temporary_directory, temporary_root


class TemporaryFileTest(SimpleTestCase):
    def test_active_attempt_is_protected_and_abandoned_directory_is_removed(self):
        with tempfile.TemporaryDirectory() as root, override_settings(ASYNC_EXPORT_TEMP_ROOT=root):
            old = time.time() - 7200
            abandoned = temporary_root() / "part-abandoned"
            abandoned.mkdir()
            marker = abandoned / ".active"
            marker.touch()
            (abandoned / "partial.tar.gz").write_bytes(b"partial")
            os.utime(marker, (old, old))
            with export_temporary_directory("part") as active:
                os.utime(active / ".active", (old, old))
                self.assertEqual(cleanup_temporary_files(), 1)
                self.assertTrue(active.exists())
                self.assertFalse(abandoned.exists())
            self.assertFalse(active.exists())

    def test_cleanup_skips_unrelated_and_symlinked_directories(self):
        with tempfile.TemporaryDirectory() as root, override_settings(ASYNC_EXPORT_TEMP_ROOT=root):
            unrelated = Path(root) / "user-data"
            unrelated.mkdir()
            (unrelated / "keep").touch()
            (temporary_root() / "part-link").symlink_to(unrelated, target_is_directory=True)
            self.assertEqual(cleanup_temporary_files(), 0)
            self.assertTrue((unrelated / "keep").exists())
