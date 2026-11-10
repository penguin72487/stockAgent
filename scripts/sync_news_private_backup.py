#!/usr/bin/env python3
"""Copy a fixed, explicitly authorized news version to the original NAS relay."""
from __future__ import annotations

import argparse
import fcntl
import json
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from stockagent.data_sync.backup_stream import BackupStream
from stockagent.data_sync.desync_snapshots import atomic_write_bytes
from stockagent.data_sync.news_private_backup import (
    NewsCapacityDeferred, capture_plan, encoded, fixed_bytes, publish_plan, reconcile_plan, validate_plan, validate_policy,
)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--policy", type=Path, default=ROOT / "configs/data_sync/news_private_backup.json")
    parser.add_argument("--backup-config", type=Path, default=ROOT / "configs/data_sync/backup_stream.json")
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--progress", type=Path, required=True)
    parser.add_argument("--publish", action="store_true")
    parser.add_argument('--receipts-only', action='store_true', help='reconcile fixed NAS ACKs without copying')
    parser.add_argument('--yield-owner-between-waves', action='store_true',
                        help='yield the original owner and re-admit capacity after each closed wave')
    parser.add_argument("--wait-seconds", type=int, default=1200)
    args = parser.parse_args()
    if args.publish and args.receipts_only:
        parser.error('choose publication or receipt reconciliation')
    os.umask(0o077)
    if not 0 <= args.wait_seconds <= 1800:
        parser.error("wait is bounded to 0..1800 seconds")
    configuration = json.loads(fixed_bytes(args.backup_config.absolute()))
    policy = json.loads(fixed_bytes(args.policy.absolute()))
    validate_policy(policy, configuration)
    root = ROOT / policy["source_root"]
    from stockagent.data_sync.workspace_source_handoff import guard_path, guard_locked_path
    guard_path(root)
    for path in (root, args.plan.absolute(), args.progress.absolute()):
        if any(p.is_symlink() for p in (path, *path.parents)):
            raise ValueError("private news roots are redirected")
    if args.plan.absolute() == args.progress.absolute() or any(
            p.absolute().is_relative_to(root) for p in (args.plan, args.progress)):
        raise ValueError("private evidence must not mutate the original source namespace")
    if args.publish or args.receipts_only:
        from stockagent.runtime_identity import runtime_identity, validate_runtime_lock
        lock = json.loads(fixed_bytes(Path(os.environ["STOCKAGENT_BACKUP_RUNTIME_LOCK"])))
        if validate_runtime_lock(lock, runtime_identity()):
            raise ValueError("existing backup runtime lock differs")
        if os.readlink("/proc/self/ns/mnt") != os.readlink("/proc/1/ns/mnt"):
            raise ValueError("use the original authority mount namespace")
    with (root / ".build.lock").open("a") as owner:
        try:
            fcntl.flock(owner, fcntl.LOCK_SH | fcntl.LOCK_NB)
        except BlockingIOError:
            print(json.dumps({'state': 'original_news_builder_busy', 'retryable': True}), flush=True)
            raise SystemExit(75) from None
        guard_locked_path(root, owner, lock_name='.build.lock')
        if args.plan.exists():
            plan = json.loads(fixed_bytes(args.plan.absolute()))
            validate_plan(plan, policy, configuration)
        else:
            plan = capture_plan(ROOT, policy, configuration)
            atomic_write_bytes(args.plan.absolute(), encoded(plan), mode=0o600)
        print(json.dumps({"state": "fixed_private_news_plan", "files": plan["files"],
                          "source_bytes": plan["source_bytes"], "identity_sha256": plan["identity_sha256"]}), flush=True)
        if args.publish or args.receipts_only:
            queue = BackupStream({**configuration, "scan_timeout_seconds": 10})
            queue.storage_guard()
            try:
                if args.receipts_only:
                    print(json.dumps(reconcile_plan(queue, plan, args.progress.absolute(),
                                                    wait_seconds=args.wait_seconds)), flush=True)
                else:
                    publish_plan(queue, ROOT, plan, args.progress.absolute(), wait_seconds=args.wait_seconds,
                                 yield_owner_between_waves=args.yield_owner_between_waves)
            except (BlockingIOError, NewsCapacityDeferred) as error:
                print(json.dumps({'state': 'fixed_news_plan_deferred', 'retryable': True,
                                  'reason': str(error), 'source_files_deleted': 0}), flush=True)
                raise SystemExit(75) from None


if __name__ == "__main__":
    main()
