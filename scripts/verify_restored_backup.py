#!/usr/bin/env python3
"""Canonical reconstruction/SQL proof on independently restored delivery files.

The caller must retain the fixed NAS snapshot restore receipt separately. This
verifier does not promote local files into NAS provenance or full-history proof.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.verify_backup_delivery import verify  # noqa: E402
from stockagent.data_sync.backup_auxiliary import verified_working_tree, verified_control  # noqa: E402
from stockagent.data_sync.desync_snapshots import scan_tree  # noqa: E402
from stockagent.data_sync.offhost_backup import private_json  # noqa: E402
from stockagent.data_sync.packed_snapshots import (  # noqa: E402
    fetch_packed_snapshot, resolve_packed_snapshot_id, verify_packed_snapshot,
)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--delivery", type=Path, required=True)
    parser.add_argument("--expected-envelope-sha256", required=True)
    parser.add_argument("--output", type=Path, required=True)
    sub = parser.add_subparsers(dest="mode", required=True)
    packed = sub.add_parser("packed")
    packed.add_argument("--dataset", required=True)
    packed.add_argument("--snapshot-id", required=True)
    packed.add_argument("--materialized-root", type=Path, required=True)
    auxiliary = sub.add_parser("auxiliary")
    auxiliary.add_argument("--logical-restore-root", type=Path)
    auxiliary.add_argument("--pg-bin", type=Path)
    args = parser.parse_args()
    if args.output.exists() or args.output.absolute().is_relative_to(args.delivery.absolute()):
        parser.error("keep a fresh acceptance receipt outside the restored closed delivery")
    transport = verify(args.delivery, args.expected_envelope_sha256)
    result = {"transport": transport, "nas_provenance_verified_by_this_tool": False,
              "full_history_backup_verified": False}
    if args.mode == "packed":
        cold = args.delivery / "cold"
        selected = resolve_packed_snapshot_id(cold, args.dataset, args.snapshot_id)
        archive = verify_packed_snapshot(cold, selected)
        materialized = fetch_packed_snapshot(cold, args.materialized_root, selected)
        proof = verify_packed_snapshot(cold, selected, materialized_path=materialized)
        fingerprint = scan_tree(materialized)["portable_fingerprint_sha256"]
        if fingerprint != selected.manifest["source"]["portable_fingerprint_sha256"]:
            raise ValueError("independent reconstructed source fingerprint differs")
        result.update(state="packed_source_reconstruction_verified", canonical_archive=archive,
                      materialized_proof=proof, portable_source_fingerprint_sha256=fingerprint,
                      source_snapshot_id=args.snapshot_id, source_manifest_sha256=selected.manifest_sha256)
    else:
        result.update(state="code_and_control_bytes_verified", code=verified_working_tree(args.delivery / "code"))
        control, _, _ = verified_control(args.delivery / "control")
        result["control_logical_state_identity_sha256"] = control["logical_state_identity_sha256"]
        if args.logical_restore_root:
            from stockagent.control.recovery import restore_control
            result["control"] = restore_control(args.delivery / "control", args.logical_restore_root, pg_bin=args.pg_bin)
            result["state"] = "code_and_control_logical_restore_verified"
    private_json(args.output, result)
    print(json.dumps({k: result[k] for k in ("state", "nas_provenance_verified_by_this_tool", "full_history_backup_verified")}))


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(type(error).__name__, file=sys.stderr)
        raise SystemExit(1)
