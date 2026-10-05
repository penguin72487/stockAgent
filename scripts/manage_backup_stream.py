#!/usr/bin/env python3
"""Manage the canonical packed-to-NAS incremental backup queue."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from stockagent.data_sync.backup_stream import BackupStream, capture_catalog  # noqa: E402
from stockagent.data_sync.offhost_backup import private_json  # noqa: E402
from stockagent.data_sync.desync_snapshots import SnapshotError  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=ROOT / "configs/data_sync/backup_stream.json")
    parser.add_argument("action", choices=("inventory", "cycle", "status", "register-baseline", "capture-auxiliary",
                                           "publish-recovery-requests"))
    parser.add_argument("--output", type=Path)
    parser.add_argument("--inspect-only", action="store_true")
    parser.add_argument("--wait-for-owner", action="store_true")
    parser.add_argument("--report", type=Path)
    parser.add_argument("--delivery", type=Path)
    args = parser.parse_args()
    config = json.loads(args.config.read_bytes())
    if args.action == "inventory":
        if not args.output or args.output.exists():
            parser.error("inventory needs a fresh output receipt")
        queue = BackupStream(config)
        queue.storage_guard()
        result = capture_catalog(queue.backup_read_root(), Path(config["publication_catalog"]),
                                 Path(config["history_disposition"]) if config.get("history_disposition") else None,
                                 include_unreferenced_objects=config.get("include_unreferenced_cold_objects", False))
        private_json(args.output, result)
        print(json.dumps({"state": "catalog_captured", "identity_sha256": result["identity_sha256"],
            "releases": len(result["releases"]), "files": len(result["files"]),
            "missing_objects": len(result["missing_objects"]), "metadata_errors": len(result["metadata_errors"])}))
    elif args.action == "status":
        print((Path(config["state_root"]) / "status.json").read_text())
    elif args.action == "register-baseline":
        if not args.report or not args.delivery:
            parser.error("baseline registration needs the relayed report and fixed source delivery")
        print(json.dumps(BackupStream(config).register_baseline(args.report, args.delivery)))
    elif args.action == "capture-auxiliary":
        print(json.dumps(BackupStream(config).refresh_auxiliary(wait_for_owner=args.wait_for_owner)))
    elif args.action == "publish-recovery-requests":
        print(json.dumps(BackupStream(config).publish_recovery_requests(wait_for_owner=args.wait_for_owner)))
    else:
        try:
            result = BackupStream(config).cycle(publish=not args.inspect_only)
        except SnapshotError as error:
            if isinstance(error.__cause__, BlockingIOError):
                print(json.dumps({"state": "existing_source_owner_busy", "backup_completed": False}))
                raise SystemExit(75) from None
            raise
        print(json.dumps(result, ensure_ascii=False))
        if result["receipt_errors"] or result["auxiliary_error"] or result.get("cold_transport_cache", {}).get("errors"):
            raise SystemExit(1)


if __name__ == "__main__":
    main()
