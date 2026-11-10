"""Coordinate Yahoo-layout writers with the source handoff owner.

Ordinary writers hold a shared lock, so this does not serialize downloads.
The existing migration owner takes the same inode exclusively when switching
the workspace. A writer that waited on the old inode must retry instead of
writing to a path which now identifies a different workspace.
"""
from __future__ import annotations

from contextlib import contextmanager, ExitStack
import fcntl
import os
from pathlib import Path
import stat
import time

from downloader.dataset_lock import DatasetLockPathChanged, DatasetLockTimeout

REPO_ROOT = Path(__file__).resolve().parents[2]
YAHOO_ROOT = REPO_ROOT / 'data_yahoo'
PEPPERSTONE_ROOT = REPO_ROOT / 'data_peperstone'


def _uses_root(paths, root: Path) -> bool:
    root = Path(os.path.abspath(root))
    resolved_root = root.resolve(strict=False)
    for value in paths:
        if value is None:
            continue
        path = Path(os.path.abspath(os.fspath(value)))
        if path == root or path.is_relative_to(root):
            return True
        resolved = path.resolve(strict=False)
        if resolved == resolved_root or resolved.is_relative_to(resolved_root):
            return True
    return False


@contextmanager
def _writer_guard(root: Path, timeout_seconds: float):
    from stockagent.data_sync.workspace_source_handoff import guard_locked_path, guard_path

    root = Path(os.path.abspath(root))
    # The first guard prevents creating a C fallback when an enrolled E mount
    # is unavailable. Recheck after acquiring the actual file description.
    guard_path(root)
    root.mkdir(parents=True, exist_ok=True)
    if root.is_symlink() or not root.is_dir():
        raise DatasetLockPathChanged('market workspace root is redirected')
    path = root / '.download.lock'
    fd = os.open(path, os.O_CREAT | os.O_RDWR | os.O_CLOEXEC | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, 'r+') as handle:
        deadline = time.monotonic() + timeout_seconds
        while True:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_SH | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise DatasetLockTimeout('market workspace handoff owner did not release its lock')
                time.sleep(min(0.25, remaining))
        try:
            try:
                held, current = os.fstat(handle.fileno()), path.lstat()
            except OSError as error:
                raise DatasetLockPathChanged('market workspace lock disappeared while waiting') from error
            if (not stat.S_ISREG(current.st_mode)
                    or (held.st_dev, held.st_ino) != (current.st_dev, current.st_ino)):
                raise DatasetLockPathChanged('market workspace lock changed while waiting')
            guard_locked_path(root, handle)
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


@contextmanager
def yahoo_writer_guard(*paths, timeout_seconds: float = 180.0):
    """Fence writes to each enrolled Yahoo-layout workspace in a stable order."""
    roots = sorted({Path(os.path.abspath(root)) for root in (YAHOO_ROOT, PEPPERSTONE_ROOT)
                    if _uses_root(paths, root)})
    if not roots:
        yield
        return
    if not 0 <= timeout_seconds < float('inf'):
        raise ValueError('market workspace lock timeout must be finite and nonnegative')
    with ExitStack() as locks:
        for root in roots:
            locks.enter_context(_writer_guard(root, timeout_seconds))
        yield
