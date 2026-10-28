#!/usr/bin/env python3
"""Bounded daily stable-cold consolidation; no source/CAS deletion."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from stockagent.data_sync.cold_tiers import LOCAL_POLICY, cycle, enqueue, load_policy, read_document, status_record
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--policy", type=Path, default=LOCAL_POLICY)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--force-window", action="store_true", help="Explicit one-shot acceptance outside the daily window")
    parser.add_argument("--seed-existing", action="store_true", help="Resume bounded metadata-only inventory of old blobs")
    parser.add_argument("--enqueue-object", action="append", help="Exact CAS relative blob path, never a source directory")
    parser.add_argument("--output", type=Path, default=Path("/var/lib/stockagent-cold-tiers/status.json"))
    args = parser.parse_args()
    policy = load_policy(args.policy)
    if args.enqueue_object:
        if not args.apply:
            parser.error("enqueuing is an explicit metadata mutation; use --apply")
        enqueue(Path(policy["sync_root"]), "manual-exact-cohort", args.enqueue_object, policy_path=args.policy)
    try:
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
