#!/usr/bin/env python3
"""Reference-aware, explicitly authorized D-primary testing-payload discard."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.ingest_remote_cold_artifacts import ingress_owner, PUBLICATION_OWNER
from stockagent.data_sync.artifact_discard import DEFAULT_POLICY, load_policy, cold_discard_plan, apply_cold_discard
from stockagent.data_sync.cold_primary import _check_d_primary_mount
from stockagent.data_sync.desync_snapshots import atomic_write_json, SnapshotError


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
    p.add_argument("--receipt", type=Path, required=True)
    p.add_argument("--apply", action="store_true")
    args = p.parse_args()
    if args.receipt.exists() or args.receipt.with_suffix(".dry.json").exists():
        p.error("fresh private receipts required")
    cold = Path("/srv/stockagent-packed")
    materialized = Path("/srv/stockagent-packed-materialized")
    _check_d_primary_mount(cold)
    policy = load_policy(args.policy)
    started = time.monotonic()
    plan = cold_discard_plan(cold, policy, materialized_root=materialized)
    atomic_write_json(args.receipt.with_suffix(".dry.json"), plan)
    print(json.dumps({"phase": "reference-graph-audited", "objects": len(plan["objects"]),
                      "bytes": plan["reclaimable_allocated_bytes"], "blockers": plan["blockers"]}), flush=True)
    if args.apply:
        with ingress_owner(PUBLICATION_OWNER):
            _check_d_primary_mount(cold)
            if load_policy(args.policy) != policy:
                raise RuntimeError("user disposition changed during owner wait")
            try:
                result = apply_cold_discard(cold, policy, plan, materialized_root=materialized,
                                           receipt=args.receipt, _audit_started_monotonic=started)
            except SnapshotError as error:
                if args.receipt.exists() or "cold manifest generation changed" not in str(error):
                    raise
                # Other owners may publish while our complete audit waits in
                # the lock queue. Rebuild only this metadata reference graph
                # under the existing owner; never reuse stale reachability.
                started = time.monotonic()
                plan = cold_discard_plan(cold, policy, materialized_root=materialized)
                atomic_write_json(args.receipt.with_suffix(".owner-refresh.dry.json"), plan)
                result = apply_cold_discard(cold, policy, plan, materialized_root=materialized,
                                           receipt=args.receipt, _audit_started_monotonic=started)
    else:
        result = {"state": "cold-testing-discard-dry-run", "plan_identity_sha256": plan["identity_sha256"]}
        atomic_write_json(args.receipt, result)
    print(json.dumps({k: v for k, v in result.items() if k != "deleted_objects"}))


if __name__ == "__main__":
    main()
