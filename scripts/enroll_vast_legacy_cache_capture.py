#!/usr/bin/env python3
"""Enroll only this measured, inventoried legacy cache cohort for retries."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import socket
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.capture_inactive_training_cache import POLICY, portable_fingerprint
from stockagent.data_sync.desync_snapshots import sha256_file
from stockagent.data_sync.offhost_backup import private_json


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inventory", required=True, type=Path)
    parser.add_argument("--measurement", required=True, type=Path)
    parser.add_argument("--evidence", required=True, type=Path)
    args = parser.parse_args()
    if os.geteuid() != 0 or socket.gethostname() != "penguin" or POLICY.exists() or args.evidence.exists():
        parser.error("penguin root, uninitialized private enrollment and fresh evidence required")
    inventory = json.loads(args.inventory.read_text())
    measurement = json.loads(args.measurement.read_text())
    if (inventory.get("training_only_role_verified") is not True
            or measurement.get("state") != "measured_remote_transport_accepted"
            or any(r.get("state") != "accepted" or r.get("all_original_sha_metadata_verified") is not True
                   for r in measurement["runs"]) or len(measurement["runs"]) < 9):
        raise ValueError("actual role and repeated complete-workflow measurement required")
    benchmark_root = next(r for r in inventory["caches"] if r["name"] == measurement["cache_root"])
    if (benchmark_root["fingerprint"] != measurement["source_fingerprint"]
            or inventory["node_profile"]["limits"]["cpu_worker_budget"] < measurement["selected_threads"]):
        raise ValueError("measurement source or actual resource budget changed")
    # The current training's cache is never a legacy enrollment. New namespaces
    # and changed generations do not inherit this one-shot authorization.
    roots = {r["name"]: portable_fingerprint(r["rows"]) for r in inventory["caches"]
             if not r["process_references"] and not r["service_references"] and not r["service_error"]}
    policy = {"schema_version": 1, "one_shot": True, "authority_node_id": "penguin", "origin_node_id": "vastai1T",
              "roots": roots, "maximum_cohort_bytes": 256 * 1024**3,
              "measurement_receipt": str(args.measurement.resolve()), "measurement_sha256": sha256_file(args.measurement),
              "node_machine_sha256": inventory["node_profile"]["machine_sha256"]}
    private_json(POLICY, policy)
    receipt = {"state": "exact_legacy_cache_retry_cohort_enrolled", "policy_path": str(POLICY),
               "policy_sha256": sha256_file(POLICY), "root_count": len(roots),
               "inventory_sha256": sha256_file(args.inventory), "measurement_sha256": policy["measurement_sha256"],
               "selected_threads": measurement["selected_threads"], "source_deleted": False,
               "future_cache_generations_enrolled": False}
    private_json(args.evidence, receipt)
    print(json.dumps(receipt))


if __name__ == "__main__":
    main()
