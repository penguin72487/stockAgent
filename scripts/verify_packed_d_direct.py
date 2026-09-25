#!/usr/bin/env python3
"""Verify every existing D cold object using fresh SHA receipts or readback."""

from __future__ import annotations

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
from scripts.copy_packed_d_streaming import list_object_names  # noqa: E402
from stockagent.data_sync.desync_snapshots import (  # noqa: E402
    SnapshotError,
    atomic_write_json,
    sha256_file,
)
from stockagent.data_sync.packed_backup import (  # noqa: E402
    BackupConfig,
    same_file_signature,
    signature,
)

VOLUME = Path("/srv/stockagent-d-volume")
RAW_ROOT = VOLUME / "stockagent-backup/packed"
PRIMARY_ROOT = VOLUME / "stockagent-cold-primary/packed"
STATE_DIR = Path("/var/lib/stockagent-d-cold-migration")


def _root() -> Path:
    mounts = subprocess.run(
        [
            "/bin/bash",
            str(REPO_ROOT / "scripts/mount_packed_d_cold.sh"),
            "--check-volume",
        ],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    if mounts.returncode != 0:
        raise SnapshotError("D primary volume guard failed: " + mounts.stderr.strip())
    present = [
        path
        for path in (RAW_ROOT, PRIMARY_ROOT)
        if path.is_dir() and not path.is_symlink()
    ]
    if len(present) != 1:
        raise SnapshotError(
            "expected exactly one D packed namespace before/after rename"
        )
    return present[0]


def _trusted_backup_receipt(
    backup: sqlite3.Connection,
    relative: str,
    digest: str,
    observed: tuple[int, ...],
    *,
    cutoff: float,
) -> bool:
    row = backup.execute(
        "SELECT digest,target_sig,checked FROM verified WHERE path=?", (relative,)
    ).fetchone()
    if row is None or row[0] != digest or float(row[2]) < cutoff:
        return False
    try:
        return same_file_signature(json.loads(row[1]), observed)
    except (TypeError, ValueError):
        return False


def _head_bytes(root: Path) -> dict[str, bytes]:
    result = {}
    for path in sorted((root / "heads").glob("*/*.json")):
        if path.is_symlink():
            raise SnapshotError(f"D head is redirected: {path}")
        result[path.relative_to(root).as_posix()] = path.read_bytes()
    if not result:
        raise SnapshotError("D packed namespace has no current heads")
    return result


def run() -> dict[str, object]:
    root = _root()
    cfg = BackupConfig.load(REPO_ROOT / "configs/data_sync/packed_backup.json")
    if root == RAW_ROOT:
        status = json.loads((cfg.state_dir / "status.json").read_text())
        if (
            status.get("state") != "up_to_date"
            or status.get("current_heads_complete") is not True
            or status.get("pending_objects") != 0
            or status.get("pending_heads") != 0
            or status.get("error_count") != 0
        ):
            raise SnapshotError("C-to-D current-head backup is not complete")
    heads = _head_bytes(root)
    objects = list_object_names(root)
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    status_path = STATE_DIR / "direct-verify-status.json"
    result: dict[str, object] = {
        "state": "checking",
        "root": str(root),
        "objects_total": len(objects),
        "objects_checked": 0,
        "bytes_checked": 0,
        "receipt_trusted_objects": 0,
        "newly_hashed_objects": 0,
        "started_at": time.time(),
    }
    atomic_write_json(status_path, result)
    direct = sqlite3.connect(STATE_DIR / "direct-verified.sqlite3")
    backup = sqlite3.connect(
        f"file:{cfg.state_dir / 'verified.sqlite3'}?mode=ro", uri=True
    )
    direct.execute(
        "CREATE TABLE IF NOT EXISTS verified (path TEXT PRIMARY KEY, "
        "digest TEXT NOT NULL, signature TEXT NOT NULL, checked_at REAL NOT NULL)"
    )
    cutoff = time.time() - cfg.checksum_recheck_days * 86400
    try:
        for index, item in enumerate(objects, 1):
            relative = str(item["relative"])
            digest = str(item["sha256"])
            path = root / relative
            before = signature(path)
            known = direct.execute(
                "SELECT digest,signature,checked_at FROM verified WHERE path=?",
                (relative,),
            ).fetchone()
            try:
                direct_trusted = (
                    known is not None
                    and known[0] == digest
                    and float(known[2]) >= cutoff
                    and same_file_signature(json.loads(known[1]), before)
                )
            except (TypeError, ValueError):
                direct_trusted = False
            if direct_trusted:
                trusted = True
            elif _trusted_backup_receipt(
                backup, relative, digest, before, cutoff=cutoff
            ):
                trusted = True
            else:
                if sha256_file(path) != digest or signature(path) != before:
                    raise SnapshotError(
                        f"D object SHA-256 or identity mismatch: {relative}"
                    )
                trusted = False
            direct.execute(
                "INSERT INTO verified(path,digest,signature,checked_at) VALUES(?,?,?,?) "
                "ON CONFLICT(path) DO UPDATE SET digest=excluded.digest, "
                "signature=excluded.signature, checked_at=excluded.checked_at",
                (relative, digest, json.dumps(before), time.time()),
            )
            result["objects_checked"] = index
            result["bytes_checked"] = int(result["bytes_checked"]) + before[2]
            key = "receipt_trusted_objects" if trusted else "newly_hashed_objects"
            result[key] = int(result[key]) + 1
            if index % 32 == 0 or index == len(objects):
                direct.commit()
                atomic_write_json(status_path, result)
        if _head_bytes(root) != heads:
            raise SnapshotError("D current heads changed during direct verification")
        reachability = audit(root, fast=True)
        if reachability["errors"] or reachability["missing_current_object_count"]:
            raise SnapshotError("D current-head reachability failed")
        result.update(
            state="verified_all_present_objects",
            current_heads=len(heads),
            current_object_count=reachability["current_unique_object_count"],
            historical_missing_objects=reachability["missing_referenced_object_count"],
            historical_completeness="known_incomplete"
            if reachability["missing_referenced_object_count"]
            else "present_complete",
            finished_at=time.time(),
        )
        atomic_write_json(status_path, result)
        return result
    except Exception as exc:
        direct.commit()
        result.update(state="failed", error=str(exc), finished_at=time.time())
        atomic_write_json(status_path, result)
        raise
    finally:
        direct.close()
        backup.close()


if __name__ == "__main__":
    try:
        print(json.dumps(run(), indent=2, sort_keys=True))
    except (OSError, ValueError, SnapshotError, subprocess.SubprocessError) as exc:
        print(json.dumps({"error": str(exc)}), file=sys.stderr)
        raise SystemExit(2) from exc
