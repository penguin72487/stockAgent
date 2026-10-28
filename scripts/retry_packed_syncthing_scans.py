#!/usr/bin/env python3
"""Retry enrolled cold-store and optional NAS notifications without source rebuilds.

A scan API acknowledgement is not peer convergence or release verification.
The original publisher owns source validation and immutable head publication;
this job only replays the paths already recorded by that publisher.
"""

from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
from pathlib import Path
import subprocess
import sys
import time


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.data_sync.desync_snapshots import (  # noqa: E402
    SnapshotError,
    atomic_write_json,
    validate_slug,
)
from stockagent.data_sync.syncthing_scan import scan_after_publish, scan_folder_state  # noqa: E402


SYNC_ROOT = Path("/srv/stockagent-packed")
RECEIPT_PATH = Path("/var/lib/stockagent-d-cold-scan-retry/latest.json")
MOUNT_CHECK = REPO_ROOT / "scripts/mount_packed_d_cold.sh"
BACKUP_CONFIG = REPO_ROOT / "configs/data_sync/backup_stream.json"


def retry_backup_transport() -> dict[str, object]:
    """Use the original queue/owner, independently of a full backup cycle.

    The deadline bounds network requests only. The original notification
    transaction still owns publication recovery, ledger rebasing and durability;
    its existing journal preserves work across service interruption. No catalog,
    source export, capacity scan, acceptance ingestion or retirement is started.
    """
    from stockagent.data_sync.backup_stream import BackupStream
    from scripts.configure_artifact_ingress_syncthing import request

    if BACKUP_CONFIG.is_symlink() or not BACKUP_CONFIG.is_file():
        raise SnapshotError("canonical backup notification config is missing or redirected")
    configuration = json.loads(BACKUP_CONFIG.read_bytes())
    timeout = configuration.get("scan_timeout_seconds", 30)
    if type(timeout) not in (int, float) or not 1 <= timeout <= 1800:
        raise SnapshotError("backup transport scan timeout must be bounded")
    # Do not modify the enrolled policy. This timer only has a short retry
    # budget; large hashes retain their pending dispatch for the next attempt.
    queue = BackupStream({**configuration, "scan_timeout_seconds": min(timeout, 30)})
    base, key, folder_id = queue.paired_transport()
    status = request(base, key, "/rest/db/status", {"folder": folder_id},
                     timeout=min(timeout, 30))
    state = status.get("state") if isinstance(status, dict) else None
    if state == "scanning" or not isinstance(state, str) or not state:
        # notify_transport is a durable owner transaction and recovers prepared
        # publications. Do not reread/rewrite its potentially large ledger just
        # to record another busy observation; no pending work was acknowledged.
        return {
            "state": ("deferred_existing_folder_scan" if state == "scanning"
                      else "deferred_invalid_folder_state"),
            "observed_folder_state": state, "batch_scan_receipts": [],
            "ledger_read": False, "ledger_written": False, "NAS_backup_verified": False,
        }
    try:
        result = queue.notify_transport(wait_for_owner=False)
    except BlockingIOError:
        result = {"state": "existing_source_owner_busy", "NAS_backup_verified": False}
    return result


def _check_mount() -> None:
    result = subprocess.run(
        ["/bin/bash", str(MOUNT_CHECK), "--check"],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    if result.returncode != 0:
        raise SnapshotError(
            "D-primary mount check failed: "
            + (result.stderr.strip() or result.stdout.strip() or str(result.returncode))
        )


def _pending_datasets() -> list[str]:
    directory = SYNC_ROOT / ".local-state/scan-pending"
    if directory.is_symlink():
        raise SnapshotError("pending scan directory must not be a symlink")
    if not directory.exists():
        return []
    if not directory.is_dir():
        raise SnapshotError("pending scan path is not a directory")
    names: list[str] = []
    for path in directory.iterdir():
        if path.suffix != ".json":
            continue
        if path.is_symlink() or not path.is_file():
            raise SnapshotError(f"pending scan receipt is not a regular file: {path.name}")
        names.append(validate_slug(path.stem, "dataset"))
    return sorted(names)


def retry_one(
    dataset: str | None = None, *, batch_object_paths: bool = False,
) -> dict[str, object]:
    if type(batch_object_paths) is not bool:
        raise SnapshotError("batch_object_paths must be a boolean")
    _check_mount()
    pending = _pending_datasets()
    if dataset is not None:
        dataset = validate_slug(dataset, "dataset")
        if dataset not in pending:
            return {"status": "idle_no_pending", "dataset": dataset,
                    "pending_before": len(pending), "pending_after": len(pending),
                    "peer_convergence": "not_checked", "release_verification": "not_checked"}
    elif not pending:
        return {"status": "idle_no_pending", "pending_before": 0, "pending_after": 0}
    else:
        dataset = pending[0]
    folder_state = scan_folder_state()
    if folder_state == "scanning":
        # A client timeout does not prove that Syncthing stopped its previous
        # scan. Leave every generation intact and let the original timer retry
        # after the current scan, rather than queueing another blocking POST.
        return {
            "status": "deferred_existing_folder_scan", "dataset": dataset,
            "observed_folder_state": folder_state,
            "pending_before": len(pending), "pending_after": len(_pending_datasets()),
            "scan_requested": False, "peer_convergence": "not_checked",
            "release_verification": "not_checked",
        }
    try:
        acknowledged = scan_after_publish(
            SYNC_ROOT, dataset, retry_full=True,
            **({"batch_object_paths": True} if batch_object_paths else {}),
        )
    except FileNotFoundError:
        # Another retry may finish and atomically remove the same pending
        # receipt after our directory listing. A failed scan keeps it.
        if (SYNC_ROOT / ".local-state/scan-pending" / f"{dataset}.json").exists():
            raise
        acknowledged = False
    if not acknowledged:
        if dataset in _pending_datasets():
            raise SnapshotError(f"pending scan was not acknowledged: {dataset}")
        return {
            "status": "concurrent_scan_completed",
            "dataset": dataset,
            "pending_before": len(pending),
            "pending_after": len(_pending_datasets()),
            "peer_convergence": "not_checked",
            "release_verification": "not_checked",
        }
    return {
        "status": "scan_request_acknowledged",
        "dataset": dataset,
        "pending_before": len(pending),
        "pending_after": len(_pending_datasets()),
        "peer_convergence": "not_checked",
        "release_verification": "not_checked",
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", help="Drain only this already-pending dataset.")
    parser.add_argument("--receipt", type=Path, default=RECEIPT_PATH)
    parser.add_argument(
        "--batch-object-paths", action="store_true",
        help="Opt in to bounded multi-sub object scans before separate manifest/head scans.",
    )
    parser.add_argument(
        "--backup-transport", action="store_true",
        help="Also retry the enrolled NAS queue through its original notification owner.",
    )
    args = parser.parse_args(argv)
    started = time.monotonic()
    receipt: dict[str, object] = {
        "schema_version": 1,
        "observed_at_utc": datetime.now(UTC).isoformat(),
        "scope": "one_existing_D_primary_pending_scan_receipt",
        # A requested policy is not evidence that any pending work existed.
        "requested_scan_policy": {"batch_object_paths": args.batch_object_paths},
    }
    if args.backup_transport:
        receipt["scope"] = "one_pending_D_primary_scan_and_enrolled_NAS_notifications"
        receipt["requested_scan_policy"]["backup_transport"] = True
    try:
        receipt.update(retry_one(
            args.dataset,
            **({"batch_object_paths": True} if args.batch_object_paths else {}),
        ))
        exit_code = 0
    except (OSError, ValueError, SnapshotError, subprocess.TimeoutExpired) as exc:
        receipt.update({"status": "retry_failed", "error": f"{type(exc).__name__}: {exc}"})
        exit_code = 1
    if args.backup_transport:
        # Neither folder's failure should starve the other folder's retry.
        try:
            notification = retry_backup_transport()
            receipt["backup_transport"] = notification
            if notification.get("error_type") or notification.get("state") == "retryable_scan_error":
                exit_code = 1
        except (OSError, ValueError, KeyError, TypeError, RuntimeError) as exc:
            receipt["backup_transport"] = {
                "state": "notification_retry_failed", "error_type": type(exc).__name__,
                "NAS_backup_verified": False,
            }
            exit_code = 1
    receipt["elapsed_seconds"] = round(time.monotonic() - started, 6)
    atomic_write_json(args.receipt, receipt)
    print(json.dumps(receipt, ensure_ascii=False, sort_keys=True), flush=True)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
