#!/usr/bin/env python3
"""Explicit exact-release recovery into a NEW path; never activate a model."""
import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from stockagent.data_sync.bulk_archive import (
    PRESERVATION_CONTRACTS, ROLE, SYNC_ROOT, canonical_blobs, native_guard,
    preservation_payload, restore_original_root,
)
from stockagent.data_sync.cold_primary import d_primary_read_alias
from stockagent.data_sync.desync_snapshots import SnapshotError, sha256_file
from stockagent.data_sync.windows_cold_io import hash_file, metadata_path
from stockagent.data_sync.packed_snapshots import resolve_packed_snapshot_id, verify_packed_snapshot


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--dataset", required=True)
    p.add_argument("--release-id", required=True)
    p.add_argument("--relative-root", required=True)
    p.add_argument("--destination", type=Path, required=True)
    p.add_argument("--apply", action="store_true")
    args = p.parse_args()
    native_guard()
    resolved = resolve_packed_snapshot_id(SYNC_ROOT, args.dataset, args.release_id)
    metadata = resolved.manifest.get("metadata", {})
    if metadata.get("preservation_contract") not in PRESERVATION_CONTRACTS or metadata.get("transport_role") != ROLE:
        raise SnapshotError("selected release is not compressed legacy preservation")
    files = canonical_blobs(SYNC_ROOT, resolved)
    aliases = {name: d_primary_read_alias(path) for name, path in files.items()}
    if hash_file(aliases["member_inventory.json"]) != metadata["member_inventory_sha256"]:
        raise SnapshotError("cold original-member index differs")
    index = json.loads(metadata_path(aliases["member_inventory.json"]).read_text())
    if not args.apply:
        print(json.dumps({"state": "explicit_restore_plan", "relative_root": args.relative_root,
                          "destination": str(args.destination), "release_id": args.release_id,
                          "activate_model": False, "cold_deleted": False}), flush=True)
        return 0
    verify_packed_snapshot(SYNC_ROOT, resolved, d_primary_native_blob_reads=True)
    result = restore_original_root(preservation_payload(aliases), index, args.relative_root, args.destination)
    print(json.dumps(result), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
