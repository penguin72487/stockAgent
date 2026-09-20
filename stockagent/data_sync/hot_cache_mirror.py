"""Retire only a verified hard-linked cache *name* from hot transport.

The local derived cache remains intact.  This is deliberately independent of
packed cold retention: it does not delete a cache payload's last link.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import stat
import uuid
from pathlib import Path
from typing import Any

from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync.materialized_cache import process_references


def _inventory(local_cache: Path, hot_cache: Path) -> dict[str, Any]:
    digest = hashlib.sha256()
    files = logical_bytes = 0
    if not hot_cache.exists():
        return {"files": 0, "logical_bytes": 0, "fingerprint": "absent"}
    if hot_cache.is_symlink() or not hot_cache.is_dir():
        raise SnapshotError("hot cache is not a real directory")
    for directory, dirnames, filenames in os.walk(hot_cache, followlinks=False):
        dirnames.sort()
        filenames.sort()
        for name in dirnames:
            path = Path(directory) / name
            if path.is_symlink() or not path.is_dir():
                raise SnapshotError(f"unsafe hot cache directory: {path}")
        for name in filenames:
            hot_file = Path(directory) / name
            local_file = local_cache / hot_file.relative_to(hot_cache)
            hot_stat = hot_file.lstat()
            try:
                local_stat = local_file.lstat()
            except FileNotFoundError as exc:
                raise SnapshotError(f"hot cache has no local counterpart: {hot_file}") from exc
            if not stat.S_ISREG(hot_stat.st_mode) or not stat.S_ISREG(local_stat.st_mode):
                raise SnapshotError(f"cache mirror contains a non-regular file: {hot_file}")
            if (hot_stat.st_dev, hot_stat.st_ino) != (
                local_stat.st_dev,
                local_stat.st_ino,
            ):
                raise SnapshotError(f"hot cache is not a hard link to local: {hot_file}")
            relative = hot_file.relative_to(hot_cache).as_posix()
            digest.update(
                f"{relative}\0{hot_stat.st_dev}:{hot_stat.st_ino}:"
                f"{hot_stat.st_size}:{hot_stat.st_mtime_ns}:"
                f"{hot_stat.st_ctime_ns}\n".encode()
            )
            files += 1
            logical_bytes += hot_stat.st_size
    return {
        "files": files,
        "logical_bytes": logical_bytes,
        "fingerprint": digest.hexdigest(),
    }


def plan_hot_cache_mirror(
    local_root: Path,
    hot_root: Path,
    *,
    bridge_inactive: bool,
) -> dict[str, Any]:
    local_root = local_root.resolve()
    hot_root = hot_root.resolve()
    if local_root == hot_root or local_root in hot_root.parents or hot_root in local_root.parents:
        raise SnapshotError("local and hot roots must be disjoint")
    if not (hot_root / ".stignore").is_file() or "(?d)/cache" not in (
        hot_root / ".stignore"
    ).read_text(encoding="utf-8").splitlines():
        raise SnapshotError("hot Syncthing folder does not ignore /cache")
    local_cache = local_root / "cache"
    hot_cache = hot_root / "cache"
    if local_cache.is_symlink() or not local_cache.is_dir():
        raise SnapshotError("local cache is not a real directory")
    inventory = _inventory(local_cache, hot_cache)
    references = process_references(hot_cache) if hot_cache.exists() else []
    blockers = []
    if not bridge_inactive:
        blockers.append("hot-bridge-active")
    if references:
        blockers.append("hot-cache-has-process-references")
    if not inventory["files"]:
        blockers.append("no-hot-cache-files")
    return {
        "local_cache": str(local_cache),
        "hot_cache": str(hot_cache),
        "files": inventory["files"],
        "logical_bytes": inventory["logical_bytes"],
        "physical_reclaim_estimate_bytes": 0,
        "plan_fingerprint": inventory["fingerprint"],
        "blockers": blockers,
        "apply_ready": not blockers,
    }


def apply_hot_cache_mirror(
    local_root: Path,
    hot_root: Path,
    quarantine_root: Path,
    *,
    expected_fingerprint: str,
    bridge_inactive: bool,
) -> dict[str, Any]:
    plan = plan_hot_cache_mirror(
        local_root, hot_root, bridge_inactive=bridge_inactive
    )
    if plan["plan_fingerprint"] != expected_fingerprint:
        raise SnapshotError("hot cache mirror plan changed; run dry-run again")
    if plan["blockers"]:
        raise SnapshotError("hot cache mirror blocked: " + ", ".join(plan["blockers"]))
    quarantine_root.mkdir(parents=True, exist_ok=True)
    if quarantine_root.is_symlink() or (
        quarantine_root.stat().st_dev != Path(plan["hot_cache"]).stat().st_dev
    ):
        raise SnapshotError("hot cache quarantine must be a real same-filesystem directory")
    if any(quarantine_root.iterdir()):
        raise SnapshotError("unfinished hot cache quarantine requires audit")
    moved = quarantine_root / f"cache-{uuid.uuid4().hex}"
    os.replace(plan["hot_cache"], moved)
    try:
        observed = _inventory(Path(plan["local_cache"]), moved)
        if observed["fingerprint"] != expected_fingerprint:
            raise SnapshotError("hot cache mirror changed during quarantine")
    except Exception:
        os.replace(moved, plan["hot_cache"])
        raise
    shutil.rmtree(moved)
    return {**plan, "action": "removed-hot-cache-names", "deleted_hot_files": plan["files"]}
