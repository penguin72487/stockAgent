"""Explicitly recover an absent CEX writer workspace from one fixed cold release.

Dry-run by default. This is not an automatic cold-to-hot downloader hook. Full
canonical recovery/verification precedes an atomic link change under the same
producer/publication lock. Cold objects, existing directories and partial
attempts are never deleted. New observations belong to the mutable live root,
not the evictable immutable materialized cache.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import fcntl
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json
from scripts.deliver_vast_futures_preparation import rename_no_replace
from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync.materialized_cache import _atomic_symlink
from stockagent.data_sync.packed_snapshots import (
    fetch_packed_snapshot, resolve_packed_snapshot_id, verify_packed_snapshot,
)

COLD_ROOT = Path("/srv/stockagent-packed")
MATERIALIZED_ROOT = Path("/srv/stockagent-packed-materialized")
LIVE_ROOT = Path("/srv/stockagent-live")
RESERVE_BYTES = 32 * 1024**3


def link_identity(path: Path) -> dict:
    info = path.lstat()
    return {"target": os.readlink(path), "generation": [info.st_dev, info.st_ino,
            info.st_size, info.st_mtime_ns, info.st_ctime_ns]}


def recover(*, dataset: str, snapshot_id: str, manifest_sha256: str,
            receipt_path: Path, apply: bool = False) -> dict:
    if dataset not in {"bybit", "okx", "binance"}:
        raise SnapshotError("only the three existing CEX sources are enrolled")
    if LIVE_ROOT.is_symlink() or LIVE_ROOT.resolve() != LIVE_ROOT or not LIVE_ROOT.is_dir():
        raise SnapshotError("canonical mutable live root is not available")
    receipt_path = receipt_path.absolute()
    if any(receipt_path == root or root in receipt_path.parents
           for root in (COLD_ROOT, MATERIALIZED_ROOT, LIVE_ROOT)):
        raise SnapshotError("engineering receipt must be outside data roots")
    source = ROOT / f"data_{dataset}"
    expected = MATERIALIZED_ROOT / dataset / snapshot_id
    if not source.is_symlink() or source.exists() or os.readlink(source) != str(expected):
        raise SnapshotError("source is not the expected missing immutable-cache link")
    before = link_identity(source)
    destination = LIVE_ROOT / f"data_{dataset}"
    resume = None
    if os.path.lexists(destination):
        if destination.is_symlink() or not destination.is_dir():
            raise SnapshotError("mutable workspace must be a real directory")
        if receipt_path.is_file() and not receipt_path.is_symlink():
            with receipt_path.open("rb") as handle:
                resume = json.loads(handle.read(1024**2 + 1))
        if not (isinstance(resume, dict) and resume.get("workspace_installed") is True
                and resume.get("contract") == "missing_crypto_mutable_workspace_recovery_v1"
                and resume.get("original_link") == before
                and resume.get("snapshot_id") == snapshot_id
                and resume.get("manifest_sha256") == manifest_sha256
                and resume.get("destination") == str(destination)):
            raise SnapshotError("refusing to replace an existing mutable workspace")
    resolved = resolve_packed_snapshot_id(COLD_ROOT, dataset, snapshot_id)
    if resolved.manifest_sha256 != manifest_sha256:
        raise SnapshotError("fixed cold release manifest SHA differs")
    required = int(resolved.manifest["source"]["logical_bytes"])
    free = shutil.disk_usage(LIVE_ROOT).free
    if free < (0 if resume else required) + RESERVE_BYTES:
        raise SnapshotError("full reconstruction would breach the 32 GiB storage reserve")
    evidence = {"contract": "missing_crypto_mutable_workspace_recovery_v1",
                "state": "planned", "dataset": dataset, "snapshot_id": snapshot_id,
                "manifest_sha256": manifest_sha256, "original_link": before,
                "destination": str(destination), "source_inventory": resolved.manifest["source"],
                "free_bytes_before": free, "reserve_bytes": RESERVE_BYTES,
                "cold_objects_modified": False, "partial_attempts_deleted": False,
                "historical_baseline_only": True, "provider_calls": 0,
                "workspace_installed": bool(resume),
                "observed_at_utc": datetime.now(timezone.utc).isoformat()}
    if not apply:
        atomic_write_json(receipt_path, evidence)
        return evidence
    lock_path = ROOT / "artifacts/daily_downloader" / f"{dataset}_source_publish.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        if (link_identity(source) != before or source.exists()
                or (not resume and os.path.lexists(destination))):
            raise SnapshotError("source/destination changed before recovery admission")
        scratch = (Path(resume["scratch_root"]) if resume else
                   Path(tempfile.mkdtemp(prefix=f".{dataset}-source-recovery-", dir=LIVE_ROOT)))
        evidence.update(state="reconstructing", scratch_root=str(scratch))
        atomic_write_json(receipt_path, evidence)
        try:
            if resume:
                verify_packed_snapshot(COLD_ROOT, resolved, materialized_path=destination)
                evidence.update(workspace_installed=True, resumed_verified_install=True)
            else:
                restored = fetch_packed_snapshot(COLD_ROOT, scratch, resolved)
            if (link_identity(source) != before or source.exists()
                    or (not resume and os.path.lexists(destination))):
                raise SnapshotError("source/destination changed during full reconstruction")
            current = resolve_packed_snapshot_id(COLD_ROOT, dataset, snapshot_id)
            if current.manifest_sha256 != manifest_sha256:
                raise SnapshotError("fixed cold manifest changed during reconstruction")
            if not resume:
                rename_no_replace(restored, destination)
            evidence.update(state="verified_workspace_installed", workspace_installed=True,
                            full_canonical_recovery_verified=True)
            atomic_write_json(receipt_path, evidence)
            _atomic_symlink(source, destination)
            for parent in (destination.parent, source.parent):
                descriptor = os.open(parent, os.O_RDONLY | os.O_DIRECTORY)
                try:
                    os.fsync(descriptor)
                finally:
                    os.close(descriptor)
            evidence.update(state="restored_mutable_workspace", new_link=link_identity(source),
                            recovered_files=int(resolved.manifest["source"]["files"]),
                            recovered_logical_bytes=required,
                            finished_at_utc=datetime.now(timezone.utc).isoformat())
            atomic_write_json(receipt_path, evidence)
        except BaseException as error:
            evidence.update(state="failed_retained", error_type=type(error).__name__, error=str(error))
            atomic_write_json(receipt_path, evidence)
            raise
    return evidence


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", choices=("bybit", "okx", "binance"), required=True)
    parser.add_argument("--snapshot-id", required=True)
    parser.add_argument("--manifest-sha256", required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    print(json.dumps(recover(dataset=args.dataset, snapshot_id=args.snapshot_id,
                             manifest_sha256=args.manifest_sha256,
                             receipt_path=args.receipt, apply=args.apply)))


if __name__ == "__main__":
    main()
