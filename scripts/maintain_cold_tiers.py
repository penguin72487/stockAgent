#!/usr/bin/env python3
"""Bounded daily stable-cold consolidation; no source/CAS deletion."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from stockagent.data_sync.cold_tiers import LOCAL_POLICY, cycle, enqueue, is_work_window, load_policy, read_document, status_record
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json


def migration_owner_active():
    """Observe canonical oneshot owners without starting or stopping them."""
    try:
        result = subprocess.run(['systemctl', 'show', '--property=ActiveState', '--value',
            'stockagent-source-local-retirement.service', 'stockagent-source-local-repack.service',
            'stockagent-prepared-stream-install.service'], capture_output=True, text=True, timeout=3, check=False)
        return result.returncode == 0 and any(line.strip() in {'active', 'activating'}
                                               for line in result.stdout.splitlines())
    except (OSError, subprocess.SubprocessError):
        return False


def migration_priority_active():
    """Defer optional LZX work while a fresh pressured retirement queue runs."""
    from scripts.run_source_local_retirement_queue import _retirement_work_pending
    from stockagent.data_sync import retirement_turn, workspace_volume
    from stockagent.data_sync.cold_primary import _check_d_primary_mount
    try:
        config = workspace_volume.configuration()
        workspace_volume.check(c=config)
        _check_d_primary_mount(Path(config['cold_root']))
        root = Path(config['mount_point'])/'migration/source-local-exact-repack'
        stat = os.statvfs(config['cold_root'])
        return (stat.f_bavail*stat.f_frsize < retirement_turn.PRESSURE_BYTES
                and (_retirement_work_pending(root) or migration_owner_active()))
    except (OSError, ValueError, KeyError, TypeError, SnapshotError):
        return False  # The original cycle still checks all of its own owners.


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--policy", type=Path, default=LOCAL_POLICY)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--force-window", action="store_true", help="Explicit one-shot acceptance outside the daily window")
    parser.add_argument("--seed-existing", action="store_true", help="Resume bounded metadata-only inventory of old blobs")
    parser.add_argument("--yield-to-migration", action="store_true", help="Let active D retirement run first under capacity pressure")
    parser.add_argument("--enqueue-object", action="append", help="Exact CAS relative blob path, never a source directory")
    parser.add_argument("--output", type=Path, default=Path("/var/lib/stockagent-cold-tiers/status.json"))
    args = parser.parse_args()
    policy = load_policy(args.policy)
    if args.enqueue_object:
        if not args.apply:
            parser.error("enqueuing is an explicit metadata mutation; use --apply")
        enqueue(Path(policy["sync_root"]), "manual-exact-cohort", args.enqueue_object, policy_path=args.policy)
    try:
        if (args.apply and args.yield_to_migration and (args.force_window or is_work_window(policy, time.time()))
                and migration_priority_active()):
            result = {"state": "waiting_migration_priority", "apply": True,
                      "observed_at_epoch": time.time(), "original_objects_deleted": 0,
                      "payload_copies_created": 0, "objects_completed": 0,
                      "logical_bytes_processed": 0, "allocation_bytes_saved": 0,
                      "NAS_compression_claim": False, "failed_intents": 0, "intent_waves_attempted": 0}
        else:
            result = cycle(policy, policy_path=args.policy, apply=args.apply,
                           force_window=args.force_window, seed=args.seed_existing)
    except BlockingIOError:
        result = {"state": "existing_tier_owner_busy", "original_objects_deleted": 0}
    if args.apply:
        try:
            previous = read_document(args.output.absolute())
        except (OSError, ValueError, SnapshotError):
            previous = {}
        result = status_record(result, policy, previous)
        atomic_write_json(args.output, result)
    print(json.dumps(result))
    return 75 if result.get("failed_intents") else 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, KeyError, TypeError, SnapshotError) as error:
        print(json.dumps({"state": "cold_tier_rejected", "error_type": type(error).__name__,
                          "reason": str(error)[:500],
                          "original_objects_deleted": 0}), file=sys.stderr)
        raise SystemExit(65)
