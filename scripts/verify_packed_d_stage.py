#!/usr/bin/env python3
"""Resume-safe SHA-256 verification of the D-backed packed staging volume.

This verifies stored objects, not source completeness or service cutover.
"""

from __future__ import annotations

import argparse
from dataclasses import replace
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import time

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.audit_packed_cold_store import audit  # noqa: E402
from stockagent.data_sync.desync_snapshots import (  # noqa: E402
    SnapshotError, atomic_write_json, sha256_file,
)
from stockagent.data_sync.packed_backup import (  # noqa: E402
    BackupConfig, current_heads_unchanged, current_inventory, inventory,
    same_file_signature, signature,
)

STAGE_ROOT = Path("/srv/stockagent-packed-d-stage")
IMAGE = Path("/mnt/d/stockagent-cold-primary/packed.ext4.img")
EXPECTED_UUID = "7371835a-d0d1-442a-a50f-9292f79c3c3c"
EXPECTED_VOLUME_ID = "9ba6ab87-3889-40e7-90e4-9597dc88abaf"
VOLUME_MARKER = Path("/mnt/d/stockagent-backup/backup-volume.json")
STATE_DIR = Path("/var/lib/stockagent-d-cold-migration")


def _output(*args: str) -> str:
    return subprocess.run(args, check=True, capture_output=True, text=True).stdout.strip()


def guard_stage() -> None:
    if _output("findmnt", "-n", "-o", "SOURCE", "-T", "/mnt/d") != "D:\\":
        raise SnapshotError("D drive is not mounted at /mnt/d")
    try:
        marker = json.loads(VOLUME_MARKER.read_text())
    except (OSError, ValueError) as exc:
        raise SnapshotError("D volume marker is unreadable") from exc
    if marker.get("volume_id") != EXPECTED_VOLUME_ID:
        raise SnapshotError("D volume marker changed")
    if IMAGE.is_symlink() or not IMAGE.is_file():
        raise SnapshotError("D cold image is missing or not a regular file")
    target = _output("findmnt", "-n", "-o", "TARGET", "-T", str(STAGE_ROOT))
    fstype = _output("findmnt", "-n", "-o", "FSTYPE", "-T", str(STAGE_ROOT))
    device = _output("findmnt", "-n", "-o", "SOURCE", "-T", str(STAGE_ROOT))
    if target != str(STAGE_ROOT) or fstype != "ext4" or not device.startswith("/dev/loop"):
        raise SnapshotError("D staging root is not the expected ext4 loop mount")
    if _output("blkid", "-s", "UUID", "-o", "value", device) != EXPECTED_UUID:
        raise SnapshotError("D staging filesystem UUID changed")
    if not _output("losetup", "-j", str(IMAGE)).startswith(f"{device}:"):
        raise SnapshotError("D staging loop is not backed by the enrolled image")


def _same_receipt(recorded: str, observed: tuple[int, ...]) -> bool:
    try:
        return same_file_signature(json.loads(recorded), observed)
    except (TypeError, ValueError):
        return False


def verify_one(
    connection: sqlite3.Connection, root: Path, item: dict[str, object]
) -> tuple[bool, int]:
    relative = str(item["relative"])
    digest = str(item["sha256"])
    path = root / relative
    before = signature(path)
    if before[2] != int(item["bytes"]):
        raise SnapshotError(f"object size changed: {relative}")
    row = connection.execute(
        "SELECT digest,signature FROM verified WHERE path=?", (relative,)
    ).fetchone()
    if row is not None and row[0] == digest and _same_receipt(row[1], before):
        return False, before[2]
    if sha256_file(path) != digest or signature(path) != before:
        raise SnapshotError(f"object SHA-256 or identity mismatch: {relative}")
    connection.execute(
        "INSERT INTO verified(path,digest,signature,checked_at) VALUES(?,?,?,?) "
        "ON CONFLICT(path) DO UPDATE SET digest=excluded.digest, "
        "signature=excluded.signature, checked_at=excluded.checked_at",
        (relative, digest, json.dumps(before), time.time()),
    )
    return True, before[2]


def run(scope: str, max_objects: int | None = None) -> dict[str, object]:
    guard_stage()
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    status_path = STATE_DIR / "verify-status.json"
    backup_cfg = BackupConfig.load(REPO_ROOT / "configs/data_sync/packed_backup.json")
    cfg = replace(backup_cfg, source=STAGE_ROOT)
    current, errors, _manifests, heads = current_inventory(cfg)
    if errors:
        raise SnapshotError(f"current head inventory incomplete: {errors[:3]}")
    if scope == "current":
        selected = current
    else:
        selected, errors = inventory(cfg)
        if errors:
            raise SnapshotError(f"object namespace invalid: {errors[:3]}")
    selected.sort(key=lambda row: str(row["relative"]))
    total = len(selected)
    if max_objects is not None:
        selected = selected[:max_objects]
    started = time.time()
    status: dict[str, object] = {
        "state": "checking", "scope": scope, "root": str(STAGE_ROOT),
        "selected_objects": total, "max_objects": max_objects,
        "checked_objects": 0, "newly_hashed_bytes": 0,
    }
    atomic_write_json(status_path, status)
    connection = sqlite3.connect(STATE_DIR / "verified.sqlite3")
    connection.execute(
        "CREATE TABLE IF NOT EXISTS verified (path TEXT PRIMARY KEY, "
        "digest TEXT NOT NULL, signature TEXT NOT NULL, checked_at REAL NOT NULL)"
    )
    newly_hashed = 0
    checked_bytes = 0
    try:
        for index, item in enumerate(selected, 1):
            hashed, size = verify_one(connection, STAGE_ROOT, item)
            checked_bytes += size
            if hashed:
                newly_hashed += size
            if index % 32 == 0 or index == len(selected):
                connection.commit()
                status.update(
                    checked_objects=index, checked_bytes=checked_bytes,
                    newly_hashed_bytes=newly_hashed,
                    elapsed_seconds=round(time.time() - started, 3),
                )
                atomic_write_json(status_path, status)
        if not current_heads_unchanged(cfg, heads):
            raise SnapshotError("current heads changed during D stage verification")
        reachability = audit(STAGE_ROOT, fast=True)
        if reachability["errors"] or reachability["missing_current_object_count"]:
            raise SnapshotError("D stage reachability is not valid for current heads")
        status.update(
            state=("partial" if max_objects is not None and len(selected) < total
                   else "verified_current" if scope == "current" else "verified_present_objects"),
            historical_missing_objects=reachability["missing_referenced_object_count"],
            historical_completeness="not_proven",
            finished_at=time.time(),
        )
        atomic_write_json(status_path, status)
        return status
    except Exception as exc:
        connection.commit()
        status.update(state="failed", error=str(exc), finished_at=time.time())
        atomic_write_json(status_path, status)
        raise
    finally:
        connection.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scope", choices=("current", "all"), default="current")
    parser.add_argument("--max-objects", type=int)
    args = parser.parse_args()
    if args.max_objects is not None and args.max_objects <= 0:
        parser.error("--max-objects must be positive")
    try:
        result = run(args.scope, args.max_objects)
    except (OSError, ValueError, SnapshotError, subprocess.SubprocessError) as exc:
        print(json.dumps({"error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 2
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
