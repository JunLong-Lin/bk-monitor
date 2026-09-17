"""本机临时文件与进程锁，含进程被强杀后的残留清理。"""

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
    """目录年龄只用于筛选候选，持有操作系统文件锁的目录永远不会被删除。

    需要在每台专用 Worker 主机上执行，且必须使用本地磁盘；不能用于锁语义
    和归属尚未验证的共享文件系统。
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
