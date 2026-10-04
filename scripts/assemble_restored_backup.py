#!/usr/bin/env python3
"""Reconstruct a fixed release from a list of independently restored waves."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from stockagent.data_sync.backup_recovery import assemble_release  # noqa: E402
from stockagent.data_sync.desync_snapshots import scan_tree  # noqa: E402
from stockagent.data_sync.offhost_backup import private_json  # noqa: E402
from stockagent.data_sync.packed_snapshots import fetch_packed_snapshot, resolve_packed_snapshot_id, verify_packed_snapshot  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--restored-inputs", type=Path, required=True,
        help="private JSON list of root, envelope_identity_sha256 and envelope_file_sha256 for fixed snapshot restores")
    parser.add_argument("--cold-root", type=Path, required=True)
    parser.add_argument("--materialized-root", type=Path, required=True)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--snapshot-id", required=True)
    parser.add_argument("--manifest-sha256", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if (args.output.exists() or args.materialized_root.exists()
            or any(args.output.absolute().is_relative_to(p.absolute()) for p in (args.cold_root, args.materialized_root))):
        parser.error("use fresh independent cold/materialized roots and an external evidence file")
    result = assemble_release(json.loads(args.restored_inputs.read_bytes()), args.cold_root,
        dataset=args.dataset, snapshot_id=args.snapshot_id, manifest_sha256=args.manifest_sha256)
    resolved = resolve_packed_snapshot_id(args.cold_root, args.dataset, args.snapshot_id)
    materialized = fetch_packed_snapshot(args.cold_root, args.materialized_root, resolved)
    result["materialized"] = verify_packed_snapshot(args.cold_root, resolved, materialized_path=materialized)
    fingerprint = scan_tree(materialized)["portable_fingerprint_sha256"]
    if fingerprint != result["portable_source_fingerprint_sha256"]:
        raise ValueError("reconstructed source fingerprint differs")
    result["state"] = "exact_packed_source_from_restored_waves_verified"
    private_json(args.output, result)
    print(json.dumps({k: result[k] for k in ("state", "copied_files", "nas_provenance_verified_by_this_tool")}))


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(type(error).__name__, file=sys.stderr)
        raise SystemExit(1)
