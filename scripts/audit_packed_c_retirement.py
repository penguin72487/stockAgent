#!/usr/bin/env python3
"""Read-only proof that every former C cold byte is preserved in D primary."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import time

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.copy_packed_d_streaming import list_object_names  # noqa: E402
from scripts.verify_packed_d_direct import PRIMARY_ROOT as D_ROOT, STATE_DIR  # noqa: E402
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

C_ROOT = Path("/srv/stockagent-packed-c-pre-d-20260925")


def _distinct_filesystems(former: Path, primary: Path) -> bool:
    return former.stat().st_dev != primary.stat().st_dev


def _check_d_primary() -> None:
    result = subprocess.run(
        ["/bin/bash", str(REPO_ROOT / "scripts/mount_packed_d_cold.sh"), "--check"],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    if result.returncode != 0:
        raise SnapshotError("D cold primary is unavailable: " + result.stderr.strip())


def _trusted_receipt(
    connection: sqlite3.Connection,
    relative: str,
    digest: str,
    observed: tuple[int, ...],
    *,
    signature_column: str,
    time_column: str,
    cutoff: float,
) -> bool:
    if signature_column not in {"signature", "source_sig"} or time_column not in {
        "checked_at",
        "checked",
    }:
        raise ValueError("unsupported receipt column")
    row = connection.execute(
        f"SELECT digest,{signature_column},{time_column} FROM verified WHERE path=?",
        (relative,),
    ).fetchone()
    if row is None or row[0] != digest:
        return False
    try:
        return float(row[2]) >= cutoff and same_file_signature(
            json.loads(row[1]), observed
        )
    except (TypeError, ValueError):
        return False


def _verify_metadata() -> tuple[int, int]:
    heads = manifests = 0
    for namespace in ("heads", "manifests"):
        for former in sorted((C_ROOT / namespace).rglob("*.json")):
            relative = former.relative_to(C_ROOT)
            current = D_ROOT / relative
            if former.is_symlink() or current.is_symlink():
                raise SnapshotError(f"cold metadata is redirected: {relative}")
            original = former.read_bytes()
            if not current.is_file() or current.read_bytes() != original:
                if namespace != "heads":
                    raise SnapshotError(f"D manifest differs from former C: {relative}")
                digest = hashlib.sha256(original).hexdigest()
                archived = (
                    D_ROOT
                    / "head-history"
                    / relative.with_suffix("")
                    / f"{digest}.json"
                )
                if (
                    archived.is_symlink()
                    or not archived.is_file()
                    or archived.read_bytes() != original
                ):
                    raise SnapshotError(
                        f"D current/head-history lacks former C head: {relative}"
                    )
            if namespace == "heads":
                heads += 1
            else:
                manifests += 1
    return heads, manifests


def run() -> dict[str, object]:
    _check_d_primary()
    if C_ROOT.is_symlink() or not C_ROOT.is_dir():
        raise SnapshotError("former C cold root is missing or redirected")
    if not _distinct_filesystems(C_ROOT, D_ROOT):
        raise SnapshotError("former C and D primary are on the same filesystem")
    if (C_ROOT / ".local-state/node-id").read_text().strip() != "penguin":
        raise SnapshotError("former C release node identity differs")
    if (D_ROOT / ".local-state/node-id").read_text().strip() != "penguin":
        raise SnapshotError("D release node identity differs")
    d_status = json.loads((STATE_DIR / "direct-verify-status.json").read_text())
    if d_status.get("state") != "verified_all_present_objects":
        raise SnapshotError("D existing objects have not all been verified")

    selected = list_object_names(C_ROOT)
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    status_path = STATE_DIR / "c-retirement-audit.json"
    status: dict[str, object] = {
        "state": "checking",
        "c_root": str(C_ROOT),
        "d_root": str(D_ROOT),
        "total_objects": len(selected),
        "checked_objects": 0,
        "checked_bytes": 0,
        "c_receipt_trusted_objects": 0,
        "c_newly_hashed_objects": 0,
        "started_at": time.time(),
    }
    atomic_write_json(status_path, status)
    cfg = BackupConfig.load(REPO_ROOT / "configs/data_sync/packed_backup.json")
    backup = sqlite3.connect(
        f"file:{cfg.state_dir / 'verified.sqlite3'}?mode=ro", uri=True
    )
    ddb = sqlite3.connect(
        f"file:{STATE_DIR / 'direct-verified.sqlite3'}?mode=ro", uri=True
    )
    cdb = sqlite3.connect(STATE_DIR / "c-verified.sqlite3")
    cdb.execute(
        "CREATE TABLE IF NOT EXISTS verified (path TEXT PRIMARY KEY, "
        "digest TEXT NOT NULL, signature TEXT NOT NULL, checked_at REAL NOT NULL)"
    )
    cutoff = time.time() - cfg.checksum_recheck_days * 86400
    try:
        for index, item in enumerate(selected, 1):
            relative = str(item["relative"])
            digest = str(item["sha256"])
            source_file = C_ROOT / relative
            target_file = D_ROOT / relative
            source_sig = signature(source_file)
            target_sig = signature(target_file)
            if not _trusted_receipt(
                ddb,
                relative,
                digest,
                target_sig,
                signature_column="signature",
                time_column="checked_at",
                cutoff=cutoff,
            ):
                raise SnapshotError(
                    f"D primary lacks the verified C object: {relative}"
                )
            trusted = _trusted_receipt(
                cdb,
                relative,
                digest,
                source_sig,
                signature_column="signature",
                time_column="checked_at",
                cutoff=cutoff,
            ) or _trusted_receipt(
                backup,
                relative,
                digest,
                source_sig,
                signature_column="source_sig",
                time_column="checked",
                cutoff=cutoff,
            )
            if not trusted and (
                sha256_file(source_file) != digest
                or signature(source_file) != source_sig
            ):
                raise SnapshotError(
                    f"former C object differs from D digest: {relative}"
                )
            cdb.execute(
                "INSERT INTO verified(path,digest,signature,checked_at) VALUES(?,?,?,?) "
                "ON CONFLICT(path) DO UPDATE SET digest=excluded.digest, "
                "signature=excluded.signature, checked_at=excluded.checked_at",
                (relative, digest, json.dumps(source_sig), time.time()),
            )
            status["checked_objects"] = index
            status["checked_bytes"] = int(status["checked_bytes"]) + source_sig[2]
            key = "c_receipt_trusted_objects" if trusted else "c_newly_hashed_objects"
            status[key] = int(status[key]) + 1
            if index % 32 == 0 or index == len(selected):
                cdb.commit()
                atomic_write_json(status_path, status)
        heads, manifests = _verify_metadata()
        status.update(
            state="verified_c_subset_of_d",
            heads_verified=heads,
            manifests_verified=manifests,
            finished_at=time.time(),
        )
        atomic_write_json(status_path, status)
        return status
    except Exception as exc:
        cdb.commit()
        status.update(state="failed", error=str(exc), finished_at=time.time())
        atomic_write_json(status_path, status)
        raise
    finally:
        cdb.close()
        ddb.close()
        backup.close()


if __name__ == "__main__":
    try:
        print(json.dumps(run(), indent=2, sort_keys=True))
    except (OSError, ValueError, SnapshotError, subprocess.SubprocessError) as exc:
        print(json.dumps({"error": str(exc)}), file=sys.stderr)
        raise SystemExit(2) from exc
