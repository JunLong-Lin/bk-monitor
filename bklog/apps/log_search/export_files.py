"""Local temporary files with process locks, including crash cleanup."""

import fcntl
import hashlib
import os
import shutil
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path

from django.conf import settings


def temporary_root():
    namespace = hashlib.sha256(settings.ASYNC_EXPORT_NAMESPACE.encode()).hexdigest()[:24]
    root = Path(settings.ASYNC_EXPORT_TEMP_ROOT or tempfile.gettempdir()) / f"bklog-export-{namespace}"
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    return root


@contextmanager
def export_temporary_directory(kind):
    with tempfile.TemporaryDirectory(prefix=f"{kind}-", dir=temporary_root()) as directory:
        with open(Path(directory) / ".active", "xb") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            yield Path(directory)


def cleanup_temporary_files(limit=100):
    """Age is only a candidate filter; a held OS lock always prevents deletion.

    Run on every dedicated worker host. This must use local disk, not a shared
    filesystem whose locking/ownership semantics have not been established.
    """
    cutoff = time.time() - max(60, settings.ASYNC_EXPORT_TEMP_RETENTION_SECONDS)
    deadline = time.monotonic() + 2
    deleted = 0
    for path in temporary_root().iterdir():
        if deleted >= limit or time.monotonic() >= deadline:
            break
        if path.is_symlink() or not path.is_dir() or not path.name.startswith(("part-", "manifest-")):
            continue
        try:
            if (path / ".active").stat().st_mtime > cutoff:
                continue
            fd = os.open(path / ".active", os.O_RDONLY | os.O_NOFOLLOW)
            with os.fdopen(fd, "rb") as lock:
                try:
                    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    continue
                shutil.rmtree(path)
                deleted += 1
        except FileNotFoundError:
            continue
    return deleted
