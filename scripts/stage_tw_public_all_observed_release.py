#!/usr/bin/env python3
"""Stage the exact 428-channel all-observed TW research ABI for cold release."""

from __future__ import annotations

import argparse
import fcntl
import json
from pathlib import Path
import sys

import pyarrow.compute as pc
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.stage_tw_public_research_release import (  # noqa: E402
    FORMAL_FEATURE,
    LIVE_ROOT,
    LOCK,
    _formal_inventory_hashes,
    _required_formal_members,
    _stage_copy,
)
from stockagent.data_sync.desync_snapshots import sha256_file  # noqa: E402


SOURCE = ROOT / "artifacts/research_features/tw_public_research_all_2014_v3.parquet"
SOURCE_RECEIPT = SOURCE.with_suffix(".all_features.json")
WIDE_SOURCE = ROOT / "artifacts/research_features/tw_public_research_wide_2014_taifex_v2.parquet"
WIDE_RECEIPT = WIDE_SOURCE.with_suffix(".taifex_research.json")
STAGED_ROOT = Path("/srv/stockagent-live/data_tw_public_research_all_observed_2014_v3")
PUBLIC_VALUE_CHANNELS = 204
STOCK_VALUE_CHANNELS = 20
MODEL_CHANNELS = STOCK_VALUE_CHANNELS + PUBLIC_VALUE_CHANNELS * 2


def _validated_contract() -> tuple[dict, pq.FileMetaData, str, str]:
    receipt = json.loads(SOURCE_RECEIPT.read_text(encoding="utf-8"))
    if int(receipt.get("contract_version", -1)) != 2:
        raise RuntimeError("unexpected all-observed feature contract version")
    source_hash = sha256_file(SOURCE)
    if source_hash != receipt.get("output_sha256"):
        raise RuntimeError("all-observed receipt and feature table hash differ")
    wide_hash = sha256_file(WIDE_SOURCE)
    if wide_hash != receipt.get("inputs", {}).get("wide", {}).get("sha256"):
        raise RuntimeError("all-observed receipt and TAIFEX v2 input hash differ")
    schema = pq.read_schema(SOURCE)
    public_names = [name for name in schema.names if name.startswith("twpub_")]
    if len(public_names) != PUBLIC_VALUE_CHANNELS or len(set(public_names)) != len(public_names):
        raise RuntimeError(
            f"expected {PUBLIC_VALUE_CHANNELS} unique public values, got {len(public_names)}"
        )
    metadata = pq.ParquetFile(SOURCE).metadata
    if metadata.num_rows <= 0 or not {"date", "symbol"}.issubset(schema.names):
        raise RuntimeError("empty or malformed all-observed research table")
    return receipt, metadata, source_hash, wide_hash


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-snapshot-id", required=True)
    parser.add_argument("--sync-root", type=Path, default=Path("/srv/stockagent-packed"))
    parser.add_argument("--staged-root", type=Path, default=STAGED_ROOT)
    args = parser.parse_args()
    LOCK.parent.mkdir(parents=True, exist_ok=True)
    with LOCK.open("a+") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        receipt, metadata, source_hash, wide_hash = _validated_contract()
        entitlement_summary = json.loads(
            (LIVE_ROOT / "tw_corporate_action_entitlements.summary.json").read_text(
                encoding="utf-8"
            )
        )
        required_members = _required_formal_members(entitlement_summary, LIVE_ROOT)
        inventory_hashes = _formal_inventory_hashes(
            args.base_snapshot_id, args.sync_root, required_members=required_members
        )
        formal_hash = sha256_file(FORMAL_FEATURE)
        receipt_formal_hash = receipt.get("inputs", {}).get("official", {}).get("sha256")
        if not (
            formal_hash
            == receipt_formal_hash
            == inventory_hashes["features/tw_public_stock_daily.parquet"]
        ):
            raise RuntimeError(
                "all-observed/formal lineage mismatch: rebuild from the current "
                "published tw-public base before staging"
            )
        end_date = str(pc.max(pq.read_table(SOURCE, columns=["date"])["date"]).as_py())
        staged_files = (
            (SOURCE, Path("features") / SOURCE.name, source_hash),
            (
                SOURCE_RECEIPT,
                Path("features") / SOURCE_RECEIPT.name,
                sha256_file(SOURCE_RECEIPT),
            ),
            (WIDE_SOURCE, Path("features") / WIDE_SOURCE.name, wide_hash),
            (
                WIDE_RECEIPT,
                Path("features") / WIDE_RECEIPT.name,
                sha256_file(WIDE_RECEIPT),
            ),
        )
        staged_hashes: dict[str, str] = {}
        for source, relative, expected in staged_files:
            _stage_copy(source, args.staged_root / relative, expected)
            staged_hashes[relative.as_posix()] = expected
        action_hashes: dict[str, str] = {}
        for name in required_members[1:]:
            source = LIVE_ROOT / name
            expected = inventory_hashes[name]
            if sha256_file(source) != expected:
                raise RuntimeError(f"formal action receipt changed after publication: {name}")
            _stage_copy(source, args.staged_root / name, expected)
            action_hashes[name] = expected
        release = {
            "schema_version": 1,
            "status": "complete",
            "research_only": True,
            "historical_vintage_verified": False,
            "end_date": end_date,
            "base_dataset": "tw-public",
            "base_snapshot_id": args.base_snapshot_id,
            "base_feature_sha256": formal_hash,
            "feature_rows": int(metadata.num_rows),
            "public_value_channels": PUBLIC_VALUE_CHANNELS,
            "stock_value_channels": STOCK_VALUE_CHANNELS,
            "model_value_channels": STOCK_VALUE_CHANNELS + PUBLIC_VALUE_CHANNELS,
            "model_availability_channels": PUBLIC_VALUE_CHANNELS,
            "model_channels": MODEL_CHANNELS,
            "staged_files_sha256": staged_hashes,
            "formal_action_members_sha256": action_hashes,
        }
        target = args.staged_root / "release_receipt.json"
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_suffix(".json.tmp")
        try:
            temporary.write_text(
                json.dumps(release, sort_keys=True, indent=2) + "\n",
                encoding="utf-8",
            )
            temporary.chmod(0o600)
            temporary.replace(target)
        finally:
            temporary.unlink(missing_ok=True)
        print(json.dumps(release, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
