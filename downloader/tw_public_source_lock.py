"""Serialize standalone TW-public supplemental writers with the canonical source root."""

from __future__ import annotations

from contextlib import contextmanager
import fcntl
from pathlib import Path


@contextmanager
def source_update_lock(root: Path, *, already_held: bool = False):
    if already_held:
        yield
        return
    path = root.parent / ".locks" / "tw-public-refresh.lock"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError("canonical Taiwan public source writer is active") from exc
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
