#!/usr/bin/env python3
"""Verify the private FinLab research release and write a public-safe audit receipt."""

from __future__ import annotations

import argparse
from datetime import UTC, datetime
import gzip
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.snapshot_finlab_quota import _atomic_json  # noqa: E402
from scripts.stage_tw_public_research_release import _formal_inventory_hashes  # noqa: E402
from stockagent.data_sync.desync_snapshots import sha256_file  # noqa: E402
from stockagent.data_sync.packed_snapshots import (  # noqa: E402
    SnapshotError, resolve_latest_packed, verify_packed_snapshot,
)

DATASET = "tw-public-research-finlab-2014-v4"


def audit(sync_root: Path, staged_root: Path) -> dict:
    release = json.loads((staged_root / "release_receipt.json").read_text(encoding="utf-8"))
    if release.get("status") != "complete" or release.get("private_personal_research") is not True:
        raise RuntimeError("staged research release is not complete/private")
    resolved = resolve_latest_packed(sync_root, DATASET)
    proof = verify_packed_snapshot(sync_root, resolved)
    inventory_info = resolved.manifest["archive"]["inventory"]
    with gzip.open(sync_root / inventory_info["relpath"], "rt", encoding="utf-8") as handle:
        inventory = {row["path"]: row["sha256"] for row in map(json.loads, handle) if row.get("kind") == "file"}
    expected = {**release["staged_files_sha256"], **release["formal_action_members_sha256"]}
    expected["release_receipt.json"] = None
    if any(inventory.get(path) != digest for path, digest in expected.items() if digest is not None):
        raise RuntimeError("verified cold inventory differs from staged research hashes")
    if "release_receipt.json" not in inventory:
        raise RuntimeError("cold release receipt missing from inventory")
    base_snapshot_id = None
    base_cold_feature_match = False
    try:
        base = resolve_latest_packed(sync_root, "tw-public", require_objects=False)
        base_snapshot_id = str(base.manifest["snapshot_id"])
        base_hashes = _formal_inventory_hashes(
            base_snapshot_id, sync_root,
            required_members=("features/tw_public_stock_daily.parquet",),
        )
        base_cold_feature_match = (
            base_hashes["features/tw_public_stock_daily.parquet"] == release["base_feature_sha256"]
        )
    except (OSError, ValueError, RuntimeError, SnapshotError):
        # FinLab cold integrity and official-base synchronization are separate.
        # Never turn an unverified base into a green private release status.
        pass
    return {
        "schema_version": 1,
        "observed_at_utc": datetime.now(UTC).isoformat(),
        "dataset": DATASET,
        "snapshot_id": proof["snapshot_id"],
        "manifest_sha256": proof["manifest_sha256"],
        "inventory_sha256": proof["inventory_sha256"],
        "cold_objects_verified": True,
        "verified_object_bytes": proof["verified_object_bytes"],
        "feature_rows": release["feature_rows"],
        "model_channels": release["model_channels"],
        "base_feature_sha256": release["base_feature_sha256"],
        "staged_receipt_sha256": sha256_file(staged_root / "release_receipt.json"),
        "base_snapshot_id": base_snapshot_id,
        "base_cold_feature_match": base_cold_feature_match,
        "remote_materialization": "not_verified",
        "historical_point_in_time": False,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sync-root", type=Path, default=Path("/srv/stockagent-packed"))
    parser.add_argument("--staged-root", type=Path, default=Path("/srv/stockagent-live/data_tw_public_research_finlab_2014_v4"))
    parser.add_argument("--output", type=Path, default=ROOT / "artifacts/live/finlab/training_delivery.json")
    args = parser.parse_args(argv)
    receipt = audit(args.sync_root, args.staged_root)
    _atomic_json(args.output, receipt)
    print(json.dumps({"event": "finlab_research_cold_verified", "snapshot_id": receipt["snapshot_id"], "verified_object_bytes": receipt["verified_object_bytes"]}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
