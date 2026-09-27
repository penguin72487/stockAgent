"""Bounded, fail-closed exclusive locks for canonical downloader workspaces."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import errno
import json
import math
from pathlib import Path
import sys
import time
from typing import Iterator

try:
    import fcntl
except ImportError:  # pragma: no cover - exercised by the unavailable-backend test
    fcntl = None


DEFAULT_LOCK_TIMEOUT_SECONDS = 180.0


class DatasetLockTimeout(TimeoutError):
    """The existing writer did not release its lock within the allowed wait."""


@dataclass(frozen=True)
class DatasetLockAcquisition:
    wait_seconds: float


def parse_lock_timeout_seconds(value: str | float) -> float:
    seconds = float(value)
    if not math.isfinite(seconds) or seconds < 0:
        raise ValueError("lock timeout must be finite and nonnegative")
    return seconds


@contextmanager
def exclusive_dataset_lock(
    path: Path,
    *,
    provider: str,
    timeout_seconds: float = DEFAULT_LOCK_TIMEOUT_SECONDS,
    poll_interval_seconds: float = 0.25,
) -> Iterator[DatasetLockAcquisition]:
    """Hold one writer lock for the entire body, including error unwinding.

    The lock file is never deleted: replacing its inode would let a second
    writer acquire a different lock. Only this coordination file (and its
    parent when new) is created before acquisition; callers publish no source
    artifacts until this context yields.
    """

    timeout_seconds = parse_lock_timeout_seconds(timeout_seconds)
    poll_interval_seconds = float(poll_interval_seconds)
    if not math.isfinite(poll_interval_seconds) or poll_interval_seconds <= 0:
        raise ValueError("lock polling interval must be finite and positive")
    started = time.monotonic()
    deadline = started + timeout_seconds

    def emit(state: str, reason: str) -> float:
        waited = max(0.0, time.monotonic() - started)
        print(
            json.dumps({
                "event": "dataset_lock", "provider": provider,
                "state": state, "reason": reason, "path": str(path),
                "lock_wait_seconds": round(waited, 6),
                "lock_timeout_seconds": timeout_seconds,
            }, sort_keys=True),
            file=sys.stderr,
            flush=True,
        )
        return waited

    def timed_out() -> None:
        waited = emit("timeout", "dataset_writer_lock_timeout")
        raise DatasetLockTimeout(
            f"{provider} dataset writer lock timed out after {waited:.3f}s: {path}"
        )

    if fcntl is None:
        emit("unsupported", "exclusive_file_lock_unavailable")
        raise RuntimeError("Exclusive dataset locking is unavailable; refusing to write")

    path.parent.mkdir(parents=True, exist_ok=True)
    emit("waiting", "acquiring_exclusive_dataset_lock")
    with path.open("a+", encoding="utf-8") as handle:
        first_attempt = True
        next_report = started + 30.0
        while True:
            if not first_attempt and time.monotonic() >= deadline:
                timed_out()
            first_attempt = False
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except OSError as exc:
                if exc.errno not in {errno.EACCES, errno.EAGAIN}:
                    emit("error", "exclusive_file_lock_failed")
                    raise
            now = time.monotonic()
            remaining = deadline - now
            if remaining <= 0:
                timed_out()
            if now >= next_report:
                emit("waiting", "dataset_writer_lock_busy")
                next_report = now + 30.0
            time.sleep(min(poll_interval_seconds, remaining))
        waited = emit("acquired", "exclusive_dataset_lock_acquired")
        try:
            yield DatasetLockAcquisition(wait_seconds=waited)
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
