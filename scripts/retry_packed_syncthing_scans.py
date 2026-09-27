#!/usr/bin/env python3
"""Retry one D-primary cold-store scan receipt without rebuilding its source.

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
from stockagent.data_sync.syncthing_scan import scan_after_publish  # noqa: E402


SYNC_ROOT = Path("/srv/stockagent-packed")
RECEIPT_PATH = Path("/var/lib/stockagent-d-cold-scan-retry/latest.json")
MOUNT_CHECK = REPO_ROOT / "scripts/mount_packed_d_cold.sh"


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
    args = parser.parse_args(argv)
    started = time.monotonic()
    receipt: dict[str, object] = {
        "schema_version": 1,
        "observed_at_utc": datetime.now(UTC).isoformat(),
        "scope": "one_existing_D_primary_pending_scan_receipt",
        # A requested policy is not evidence that any pending work existed.
        "requested_scan_policy": {"batch_object_paths": args.batch_object_paths},
    }
    try:
        receipt.update(retry_one(
            args.dataset,
            **({"batch_object_paths": True} if args.batch_object_paths else {}),
        ))
        exit_code = 0
    except (OSError, ValueError, SnapshotError, subprocess.TimeoutExpired) as exc:
        receipt.update({"status": "retry_failed", "error": f"{type(exc).__name__}: {exc}"})
        exit_code = 1
    receipt["elapsed_seconds"] = round(time.monotonic() - started, 6)
    atomic_write_json(args.receipt, receipt)
    print(json.dumps(receipt, ensure_ascii=False, sort_keys=True), flush=True)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
