"""Coordinate deliberate paper-service cutovers with unattended recovery.

Only a held OS lock defers repair. Metadata (including stale crash metadata)
never disables recovery, and the kernel releases the lease if its owner dies.
The guardian also holds this lock during its check/repair cycle, so a cutover
cannot race an already-started repair.
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
import json
import os
from pathlib import Path
import time


CONTROL_LOCK = (Path(__file__).resolve().parents[2]
                / "artifacts/operations/tw_day_trade_service_control.lock")


@contextmanager
def control_lease(*, owner: str | None = None, timeout_seconds: float = 0.,
                  path: Path = CONTROL_LOCK):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+", encoding="utf-8") as handle:
        deadline = time.monotonic() + timeout_seconds
        while True:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    handle.seek(0)
                    try:
                        metadata = json.load(handle)
                    except (ValueError, OSError):
                        metadata = {}
                    if owner is not None:
                        raise RuntimeError("paper service control is held by another coordinator")
                    yield {"acquired": False, "active": True, "lock_path": str(path),
                           "holder": metadata, "authority": "held_os_advisory_lock"}
                    return
                time.sleep(.05)
        if owner is not None:
            handle.seek(0)
            handle.truncate()
            json.dump({"owner": owner, "pid": os.getpid(),
                       "started_at": datetime.now(timezone.utc).isoformat()}, handle)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            yield {"acquired": True, "active": False, "lock_path": str(path),
                   "authority": "held_os_advisory_lock"}
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
