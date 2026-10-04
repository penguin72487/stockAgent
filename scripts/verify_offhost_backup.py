#!/usr/bin/env python3
"""Exercise a real pinned release through encrypted backup and exact recovery.

This local pilot prepares a closed lab203 delivery. It never claims NAS access,
off-host durability, all history, provider completeness or cleanup permission.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import secrets
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from stockagent.data_sync.desync_snapshots import scan_tree  # noqa: E402
from stockagent.data_sync.offhost_backup import (  # noqa: E402
    ResticBackup, capture_plan, export_delivery, private_json, verify_restore,
)
from stockagent.data_sync.packed_snapshots import (  # noqa: E402
    fetch_packed_snapshot, resolve_packed_snapshot_id, verify_packed_snapshot,
)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cold-root", type=Path, required=True)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--snapshot-id", required=True)
    parser.add_argument("--restic", type=Path, required=True)
    parser.add_argument("--control-receipt", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    os.umask(0o077)
    output = args.output.absolute()
    if output.exists() or any(p.is_symlink() for p in (output, *output.parents)):
        parser.error("pilot requires a fresh private output root")
    output.mkdir(mode=0o700, parents=True)
    started = time.perf_counter()
    selections = [(args.dataset, args.snapshot_id)]
    plan = capture_plan(args.cold_root, selectors=selections, control_receipt=args.control_receipt)
    recorded = output / "source-plan.json"
    private_json(recorded, plan)
    password = output / "local-pilot.password"
    with password.open("x") as stream:
        stream.write(secrets.token_hex(32) + "\n")
    password.chmod(0o600)
    client = ResticBackup(args.restic, str(output / "encrypted-repository"), password, cache=output / "cache")
    initialized = client.initialize()
    backed_up = client.backup(plan, recorded)
    private_json(output / "backup-receipt.json", backed_up)
    client.run(["check", "--read-data"])
    restored = output / "restored"
    client.restore(backed_up["snapshot_id"], restored)
    recovery = verify_restore(plan, restored, record_plan_path=recorded)
    private_json(output / "restore-receipt.json", recovery)
    cold_copy = restored / Path(plan["cold_root"]).as_posix().lstrip("/")
    selected = resolve_packed_snapshot_id(cold_copy, args.dataset, args.snapshot_id)
    materialized = fetch_packed_snapshot(cold_copy, output / "materialized", selected)
    reconstructed = verify_packed_snapshot(cold_copy, selected, materialized_path=materialized)
    fingerprint = scan_tree(materialized)["portable_fingerprint_sha256"]
    if fingerprint != selected.manifest["source"]["portable_fingerprint_sha256"]:
        raise ValueError("independent materialization differs from canonical source fingerprint")
    # Public release delivery excludes the private control DB dump and key.
    delivery_plan = capture_plan(args.cold_root, selectors=selections)
    delivery = export_delivery(delivery_plan, output / "lab203-ready-delivery")
    private_json(output / "delivery-receipt.json", delivery)
    result = {"state": "local_backup_recovery_accepted", "dataset": args.dataset,
              "source_snapshot_id": args.snapshot_id, "source_manifest_sha256": selected.manifest_sha256,
              "source_plan_identity_sha256": plan["identity_sha256"], "restic_snapshot_id": backed_up["snapshot_id"],
              "repository_id": initialized["id"], "objects_verified": recovery["files_verified"],
              "bytes_verified": recovery["bytes_verified"], "materialized_proof": reconstructed,
              "portable_source_fingerprint_sha256": fingerprint, "lab203_delivery": delivery,
              "control_archive_bytes_verified": recovery["control_archive_bytes_verified"],
              "control_database_restore_verified": False, "durable_off_host_backup_verified": False,
              "all_history_verified": False, "source_deleted": False,
              "complete_wall_seconds": time.perf_counter() - started,
              "scope": "one pinned real source release, optional consistent control bytes, complete encrypted local backup/check/restore/materialization and closed delivery export"}
    private_json(output / "acceptance.json", result)
    print(json.dumps({k: result[k] for k in ("state", "dataset", "objects_verified", "bytes_verified",
                                           "complete_wall_seconds", "durable_off_host_backup_verified")}))


if __name__ == "__main__":
    main()
