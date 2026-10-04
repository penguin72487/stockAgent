#!/usr/bin/env python3
"""Capture exact backup inputs, use restic, and verify an isolated restored copy.

Target enrollment and NAS mount checks are separate from byte restoration.
This tool never deletes sources or prunes repository history.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from stockagent.data_sync.offhost_backup import (  # noqa: E402
    ResticBackup, capture_plan, export_delivery, private_json, verify_restore,
)
from stockagent.data_sync.nas_target import NasTarget  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="operation", required=True)
    capture = sub.add_parser("capture")
    capture.add_argument("--cold-root", type=Path, required=True)
    capture.add_argument("--release", action="append", nargs=2, metavar=("DATASET", "SNAPSHOT_ID"))
    capture.add_argument("--control-receipt", type=Path)
    capture.add_argument("--all-history", action="store_true", help="include every retained manifest observed in this capture; refuse missing objects")
    capture.add_argument("--output", type=Path, required=True)
    export = sub.add_parser("export")
    export.add_argument("--plan", type=Path, required=True)
    export.add_argument("--destination", type=Path, required=True)
    export.add_argument("--output", type=Path, required=True)
    for operation in ("init", "backup", "check", "restore"):
        command = sub.add_parser(operation)
        command.add_argument("--restic", type=Path, required=True)
        command.add_argument("--repository", required=True)
        command.add_argument("--password-file", type=Path, required=True)
        command.add_argument("--cache", type=Path, required=True)
        command.add_argument("--output", type=Path, required=True)
        command.add_argument("--nas-config", type=Path, help="require the configured NAS mount before and after the operation")
        if operation == "backup":
            command.add_argument("--plan", type=Path, required=True)
        if operation == "restore":
            command.add_argument("--snapshot-id", required=True)
            command.add_argument("--destination", type=Path, required=True)
        if operation == "check":
            command.add_argument("--read-data", action="store_true")
    verify = sub.add_parser("verify-restore")
    verify.add_argument("--plan", type=Path, required=True)
    verify.add_argument("--restored-root", type=Path, required=True)
    verify.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    os.umask(0o077)
    if args.output.exists():
        parser.error("each receipt requires a fresh output path")
    if args.operation == "capture":
        result = capture_plan(args.cold_root, selectors=args.release, control_receipt=args.control_receipt,
                              all_history=args.all_history)
    elif args.operation == "export":
        result = export_delivery(json.loads(args.plan.read_bytes()), args.destination)
    elif args.operation == "verify-restore":
        result = verify_restore(json.loads(args.plan.read_bytes()), args.restored_root, record_plan_path=args.plan)
    else:
        target = NasTarget(args.nas_config) if args.nas_config else None
        before = target.check() if target else None
        if target and args.repository != str(target.repository):
            parser.error("repository must be the exact configured NAS user directory")
        client = ResticBackup(args.restic, args.repository, args.password_file, cache=args.cache)
        if args.operation == "init":
            result = {"state": "repository_initialized", "result": client.initialize(),
                      "durable_off_host_backup_verified": False}
        elif args.operation == "backup":
            result = client.backup(json.loads(args.plan.read_bytes()), args.plan)
        elif args.operation == "check":
            client.run(["check", *(["--read-data"] if args.read_data else [])])
            result = {"state": "repository_checked", "read_all_data": args.read_data,
                      "durable_off_host_backup_verified": False}
        else:
            result = client.restore(args.snapshot_id, args.destination)
        if target:
            result["nas_target_before"] = before
            result["nas_target_after"] = target.unchanged(before)
    private_json(args.output, result)
    # Details remain in the owner-only receipt. Never echo repository credentials.
    print(json.dumps({key: result[key] for key in (
        "state", "identity_sha256", "snapshot_id", "files_verified", "bytes_verified",
        "durable_off_host_backup_verified") if key in result}))


if __name__ == "__main__":
    main()
