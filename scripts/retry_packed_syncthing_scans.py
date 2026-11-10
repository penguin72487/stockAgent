#!/usr/bin/env python3
"""Retry enrolled cold-store and optional NAS notifications without source rebuilds.

A scan API acknowledgement is not peer convergence or release verification.
The original publisher owns source validation and immutable head publication;
this job only replays the paths already recorded by that publisher.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
from datetime import UTC, datetime
import fcntl
import json
import os
from pathlib import Path
import stat
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


def backup_owner_busy(configuration: dict) -> bool:
    """Observe the existing publication lock without changing its inode.

    A migration may pause the paired folder while it holds this owner. Check
    the lock before inspecting the folder so that an expected owner boundary
    is not reported as a broken pairing. The real pairing is still checked by
    the original owner whenever the lock becomes available.
    """
    root = configuration.get("state_root")
    if root is None:
        return False  # Keep the canonical configuration validation downstream.
    if not isinstance(root, str) or not Path(root).is_absolute():
        raise SnapshotError("invalid backup owner state root")
    lock = Path(root) / "owner.lock"
    try:
        fd = os.open(lock, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK)
    except FileNotFoundError:
        return False
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or info.st_nlink != 1:
            raise SnapshotError("backup owner lock is not owned regular metadata")
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return True
        else:
            fcntl.flock(fd, fcntl.LOCK_UN)
            return False
    finally:
        os.close(fd)


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
    if backup_owner_busy(configuration):
        return {"state": "existing_source_owner_busy", "NAS_backup_verified": False}
    # Do not modify the enrolled policy. This timer only has a short retry
    # budget; large hashes retain their pending dispatch for the next attempt.
    queue = BackupStream({**configuration, "scan_timeout_seconds": min(timeout, 30)})
    base, key, folder_id = queue.paired_transport()
    status = request(base, key, "/rest/db/status", {"folder": folder_id},
                     timeout=min(timeout, 30))
    state = status.get("state") if isinstance(status, dict) else None
    if not isinstance(state, str) or not state or state in {"scanning", "scan-waiting"}:
        # notify_transport is a durable owner transaction and recovers prepared
        # publications. Do not reread/rewrite its potentially large ledger just
        # to record another busy observation; no pending work was acknowledged.
        return {
            "state": ("deferred_existing_folder_scan" if isinstance(state, str)
                      and state in {"scanning", "scan-waiting"}
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
    if folder_state in {"scanning", "scan-waiting"}:
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
            **({"batch_object_paths": True, "request_timeout_seconds": 480}
               if batch_object_paths else {}),
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


def retry_batch(*, max_datasets: int, budget_seconds: int, batch_object_paths: bool,
                after_failed: str | None = None) -> dict[str, object]:
    """Drain a bounded FIFO snapshot without overlapping the folder's scans.

    The time budget limits *starting* another dataset, not the duration of a
    Syncthing request already in flight. Every dataset retains the existing
    per-generation lock, acknowledgement and timeout rules in ``retry_one``.
    """
    if type(max_datasets) is not int or not 1 <= max_datasets <= 32:
        raise SnapshotError("max_datasets must be between 1 and 32")
    if type(budget_seconds) is not int or not 1 <= budget_seconds <= 240:
        raise SnapshotError("budget_seconds must be between 1 and 240")
    _check_mount()
    pending = _pending_datasets()
    directory = SYNC_ROOT / ".local-state/scan-pending"
    ordered: list[tuple[int, str]] = []
    for dataset in pending:
        try:
            info = (directory / f"{dataset}.json").lstat()
        except FileNotFoundError:
            continue  # Another owner already completed this generation.
        if not stat.S_ISREG(info.st_mode):
            raise SnapshotError("pending scan receipt changed into a nonregular file")
        ordered.append((info.st_mtime_ns, dataset))
    ordered.sort()
    if after_failed is not None:
        after_failed = validate_slug(after_failed, 'retry cursor')
        # A prior repeatedly failing prefix must not indefinitely starve later
        # datasets. The exact failed intent remains untouched in its owner
        # queue, and rotation wraps so that it will be retried in a later run.
        position = next((i for i, (_, name) in enumerate(ordered) if name == after_failed), None)
        if position is not None:
            ordered = ordered[position+1:] + ordered[:position+1]

    started = time.monotonic()
    results: list[dict[str, object]] = []
    failures = 0
    last_failed = None
    for _, dataset in ordered[:max_datasets]:
        if results and time.monotonic() - started >= budget_seconds:
            break
        try:
            result = retry_one(dataset, batch_object_paths=batch_object_paths)
        except (OSError, ValueError, SnapshotError, subprocess.TimeoutExpired) as exc:
            # Retain the exact failed intent, but let a broken dataset stop
            # neither unrelated datasets nor the independent NAS notifier.
            result = {"dataset": dataset, "status": "retry_failed",
                      "error_type": type(exc).__name__}
            failures += 1
            last_failed = dataset
        results.append(result)
        if result.get("status") == "deferred_existing_folder_scan":
            break  # A previous owner still has this folder; do not queue POSTs.

    if failures:
        status = "retry_partial_failure"
    elif any(item.get("status") == "deferred_existing_folder_scan" for item in results):
        status = "deferred_existing_folder_scan"
    elif results:
        status = "scan_batch_processed"
    else:
        status = "idle_no_pending"
    return {
        "status": status,
        "pending_before": len(pending),
        "pending_after": len(_pending_datasets()),
        "attempted_count": len(results),
        "acknowledged_count": sum(item.get("status") == "scan_request_acknowledged" for item in results),
        "failed_count": failures,
        "retry_cursor": last_failed,
        "datasets": results,
        "peer_convergence": "not_checked",
        "release_verification": "not_checked",
    }


def _previous_failure_cursor(path: Path) -> str | None:
    """Read only the last bounded batch's private fairness cursor."""
    try:
        if path.is_symlink() or not path.is_file() or path.stat().st_size > 1024*1024:
            return None
        value = json.loads(path.read_bytes())
        if (not isinstance(value, dict) or value.get('schema_version') != 1
                or value.get('status') != 'retry_partial_failure'
                or not isinstance(value.get('retry_cursor'), str)):
            return None
        return validate_slug(value.get('retry_cursor'), 'retry cursor')
    except (OSError, ValueError, TypeError, SnapshotError):
        return None


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
    parser.add_argument("--max-datasets", type=int, default=1,
                        help="Retry at most this many pending datasets, oldest first (1-32).")
    parser.add_argument("--budget-seconds", type=int, default=40,
                        help="Do not start another dataset after this many seconds (1-240).")
    args = parser.parse_args(argv)
    if not 1 <= args.max_datasets <= 32 or not 1 <= args.budget_seconds <= 240:
        parser.error("--max-datasets must be 1-32 and --budget-seconds must be 1-240")
    if args.dataset and args.max_datasets != 1:
        parser.error("--dataset cannot be combined with --max-datasets greater than 1")
    started = time.monotonic()
    receipt: dict[str, object] = {
        "schema_version": 1,
        "observed_at_utc": datetime.now(UTC).isoformat(),
        "scope": "one_existing_D_primary_pending_scan_receipt",
        # A requested policy is not evidence that any pending work existed.
        "requested_scan_policy": {"batch_object_paths": args.batch_object_paths},
    }
    if args.batch_object_paths:
        receipt["requested_scan_policy"]["request_timeout_seconds"] = 480
    if args.backup_transport:
        receipt["scope"] = "one_pending_D_primary_scan_and_enrolled_NAS_notifications"
        receipt["requested_scan_policy"]["backup_transport"] = True
    if args.max_datasets > 1:
        receipt["scope"] = "bounded_pending_D_primary_scans_and_optional_NAS_notifications"
        receipt["requested_scan_policy"].update({"max_datasets": args.max_datasets,
                                                  "budget_seconds": args.budget_seconds})
    # These are different Syncthing folders with independent canonical owners.
    # A slow D metadata scan must not delay an otherwise-ready NAS notification.
    # The original per-folder locks, generations and acknowledgement rules stay
    # unchanged; this does not start a second backup writer or prove delivery.
    pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="nas-scan-retry") if args.backup_transport else nullcontext()
    with pool as executor:
        backup_future = executor.submit(retry_backup_transport) if executor else None
        try:
            if args.max_datasets > 1:
                receipt.update(retry_batch(max_datasets=args.max_datasets,
                                           budget_seconds=args.budget_seconds,
                                           batch_object_paths=args.batch_object_paths,
                                           after_failed=_previous_failure_cursor(args.receipt)))
            else:
                receipt.update(retry_one(
                    args.dataset,
                    **({"batch_object_paths": True} if args.batch_object_paths else {}),
                ))
            exit_code = int(receipt.get("status") == "retry_partial_failure")
        except (OSError, ValueError, SnapshotError, subprocess.TimeoutExpired) as exc:
            receipt.update({"status": "retry_failed", "error": f"{type(exc).__name__}: {exc}"})
            exit_code = 1
        if backup_future is not None:
            try:
                notification = backup_future.result()
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
