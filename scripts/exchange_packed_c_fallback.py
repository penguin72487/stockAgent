#!/usr/bin/env python3
"""Atomically replace penguin's C cold path with a fail-closed empty mountpoint."""

from __future__ import annotations

import ctypes
import os
from pathlib import Path
import sys
import time

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json  # noqa: E402

CANONICAL = Path("/srv/stockagent-packed")
PREPARED = Path("/srv/stockagent-packed-fallback-swap-20260925")
PRESERVED = Path("/srv/stockagent-packed-c-pre-d-20260925")
SENTINEL = ".stockagent-d-mount-required"
RECEIPT = Path("/var/lib/stockagent-d-cold-migration/c-exchange.json")
AT_FDCWD = -100
RENAME_EXCHANGE = 2


def _exchange(first: Path, second: Path) -> None:
    libc = ctypes.CDLL(None, use_errno=True)
    result = libc.renameat2(
        ctypes.c_int(AT_FDCWD),
        os.fsencode(first),
        ctypes.c_int(AT_FDCWD),
        os.fsencode(second),
        ctypes.c_uint(RENAME_EXCHANGE),
    )
    if result != 0:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error), str(first))


def _only_sentinel(path: Path) -> bool:
    return (
        path.is_dir()
        and not path.is_symlink()
        and {item.name for item in path.iterdir()} == {SENTINEL}
        and (path / SENTINEL).is_file()
        and not (path / SENTINEL).is_symlink()
    )


def run() -> dict[str, object]:
    if PRESERVED.exists() or PRESERVED.is_symlink():
        raise SnapshotError("preserved former C path already exists")
    if not _only_sentinel(PREPARED):
        raise SnapshotError(
            "prepared fallback must contain only the mount-required sentinel"
        )
    if CANONICAL.is_symlink() or not CANONICAL.is_dir() or os.path.ismount(CANONICAL):
        raise SnapshotError("canonical C cold root is redirected or mounted")
    if CANONICAL.stat().st_dev != PREPARED.stat().st_dev:
        raise SnapshotError("fallback exchange would cross filesystems")
    if (CANONICAL / ".local-state/node-id").read_text().strip() != "penguin":
        raise SnapshotError("canonical C cold root has the wrong node identity")
    _exchange(CANONICAL, PREPARED)
    if (
        not _only_sentinel(CANONICAL)
        or not (PREPARED / ".local-state/node-id").is_file()
    ):
        raise SnapshotError("atomic C fallback exchange did not produce expected paths")
    os.rename(PREPARED, PRESERVED)
    result: dict[str, object] = {
        "state": "c_preserved_fallback_ready",
        "former_c": str(PRESERVED),
        "canonical_fallback": str(CANONICAL),
        "swapped_at": time.time(),
        "former_c_device": PRESERVED.stat().st_dev,
        "former_c_inode": PRESERVED.stat().st_ino,
    }
    atomic_write_json(RECEIPT, result)
    return result


if __name__ == "__main__":
    try:
        print(run())
    except (OSError, ValueError, SnapshotError) as exc:
        print({"error": str(exc)}, file=sys.stderr)
        raise SystemExit(2) from exc
