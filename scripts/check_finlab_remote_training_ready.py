#!/usr/bin/env python3
"""Fail closed unless the exact FinLab research ABI and TW base are present."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from stockagent.config import load_config  # noqa: E402
from stockagent.data_sync.desync_snapshots import sha256_file  # noqa: E402

DEFAULT_CONFIG = ROOT / "configs/deployments/tw_public_finlab_research_2014_v4_remote.yaml"


def check(config_path: Path) -> dict:
    config = load_config(config_path)
    feature = Path(config.data.tw_public_feature_path)
    stock_root = Path(config.data.parquet_root)
    release_root = feature.parent.parent
    receipt = json.loads((release_root / "release_receipt.json").read_text(encoding="utf-8"))
    if receipt.get("status") != "complete" or receipt.get("research_only") is not True:
        raise RuntimeError("FinLab research release is not complete")
    if receipt.get("historical_point_in_time") is not False or receipt.get("model_channels") != 636:
        raise RuntimeError("FinLab research ABI or PIT contract changed")
    expected = receipt.get("staged_files_sha256", {}).get(f"features/{feature.name}")
    if not expected or sha256_file(feature) != expected:
        raise RuntimeError("FinLab feature table hash differs from exact release")
    if pq.ParquetFile(feature).metadata.num_rows != receipt.get("feature_rows"):
        raise RuntimeError("FinLab feature table row count differs from receipt")
    base = stock_root.parent / "features/tw_public_stock_daily.parquet"
    if sha256_file(base) != receipt.get("base_feature_sha256"):
        raise RuntimeError("TW public base does not match the FinLab research build")
    if not stock_root.is_dir() or not any(stock_root.glob("*.parquet")):
        raise RuntimeError("TW price panel parquet root is missing")
    for member, expected_hash in receipt.get("formal_action_members_sha256", {}).items():
        if sha256_file(release_root / member) != expected_hash:
            raise RuntimeError(f"formal action companion mismatch: {member}")
    return {
        "status": "inputs_exact_hash_verified",
        "feature_rows": receipt["feature_rows"],
        "model_channels": receipt["model_channels"],
        "research_only": True,
        "historical_point_in_time": False,
        "cold_ready_proof": "run stockagent-data use --verify on both datasets separately",
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    args = parser.parse_args(argv)
    print(json.dumps(check(args.config), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
