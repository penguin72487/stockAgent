#!/usr/bin/env python3
"""Stage one hash-linked, private TW public research table for cold release."""

from __future__ import annotations

import argparse
import fcntl
import fnmatch
import gzip
import hashlib
import json
import os
from pathlib import Path
import shutil
import sys
import uuid

import pyarrow.compute as pc
import pyarrow.parquet as pq
import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from stockagent.data_sync.desync_snapshots import sha256_file  # noqa: E402
from stockagent.data_sync.packed_snapshots import (  # noqa: E402
    resolve_latest_packed,
    resolve_packed_snapshot_id,
)

LIVE_ROOT = Path("/srv/stockagent-live/data_tw_public")
SOURCE = LIVE_ROOT / "features/tw_public_research_wide_2014_v1.parquet"
RESEARCH_RECEIPT = SOURCE.with_suffix(".research.json")
FORMAL_FEATURE = LIVE_ROOT / "features/tw_public_stock_daily.parquet"
STAGED_ROOT = Path("/srv/stockagent-live/data_tw_public_research_wide_2014_v1")
LOCK = LIVE_ROOT.parent / ".locks/tw-public-refresh.lock"
SELECTOR = ROOT / "configs/markets/tw_public_preopen_wide_research_2014_v1.yaml"


FORMAL_MEMBERS = (
    "features/tw_public_stock_daily.parquet",
    "tw_corporate_action_reference.parquet",
    "tw_corporate_action_reference.summary.json",
    "tw_corporate_action_entitlements.parquet",
    "tw_corporate_action_entitlements.summary.json",
)


def _required_formal_members(entitlement_summary: dict, live_root: Path) -> tuple[str, ...]:
    manifest_receipt = entitlement_summary.get("raw_receipt_manifest")
    if not isinstance(manifest_receipt, dict):
        raise RuntimeError("formal entitlement raw manifest receipt is missing")
    manifest_relative = str(manifest_receipt.get("relative_path", "")).strip()
    manifest_source = (live_root / manifest_relative).resolve()
    if (
        not manifest_relative
        or not manifest_source.is_relative_to(live_root.resolve())
        or manifest_source.suffix != ".jsonl"
    ):
        raise RuntimeError("invalid formal entitlement raw manifest path")
    manifest_member = manifest_source.relative_to(live_root.resolve()).as_posix()
    return (*FORMAL_MEMBERS, manifest_member)


def _formal_inventory_hashes(
    snapshot_id: str, sync_root: Path, *, required_members: tuple[str, ...]
) -> dict[str, str]:
    latest = resolve_latest_packed(sync_root, "tw-public", require_objects=False)
    if latest.manifest["snapshot_id"] != snapshot_id:
        raise RuntimeError("formal base snapshot is not the current tw-public head")
    resolved = resolve_packed_snapshot_id(
        sync_root, "tw-public", snapshot_id, require_objects=False
    )
    info = resolved.manifest["archive"]["inventory"]
    path = sync_root / info["relpath"]
    if sha256_file(path) != info["sha256"]:
        raise RuntimeError("formal snapshot inventory hash mismatch")
    hashes: dict[str, str] = {}
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        for line in handle:
            row = json.loads(line)
            if row.get("path") in required_members:
                hashes[str(row["path"])] = str(row["sha256"])
    missing = set(required_members) - hashes.keys()
    if missing:
        raise RuntimeError(f"formal snapshot lacks required members: {sorted(missing)}")
    return hashes


def _stage_copy(source: Path, target: Path, expected_hash: str) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(target.name + ".tmp." + uuid.uuid4().hex)
    try:
        shutil.copyfile(source, temporary)
        os.chmod(temporary, 0o600)
        if sha256_file(temporary) != expected_hash:
            raise RuntimeError(f"source changed while copying {source}")
        os.replace(temporary, target)
    finally:
        temporary.unlink(missing_ok=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-snapshot-id", required=True)
    parser.add_argument("--sync-root", type=Path, default=Path("/srv/stockagent-packed"))
    parser.add_argument("--staged-root", type=Path, default=STAGED_ROOT)
    args = parser.parse_args()
    LOCK.parent.mkdir(parents=True, exist_ok=True)
    with LOCK.open("a+") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        receipt = json.loads(RESEARCH_RECEIPT.read_text(encoding="utf-8"))
        if int(receipt.get("contract_version", -1)) != 5:
            raise RuntimeError("unexpected research table contract version")
        entitlement_summary = json.loads(
            (LIVE_ROOT / "tw_corporate_action_entitlements.summary.json").read_text(
                encoding="utf-8"
            )
        )
        required_members = _required_formal_members(entitlement_summary, LIVE_ROOT)
        formal_hash = sha256_file(FORMAL_FEATURE)
        inventory_hashes = _formal_inventory_hashes(
            args.base_snapshot_id, args.sync_root, required_members=required_members
        )
        if not (
            formal_hash
            == inventory_hashes["features/tw_public_stock_daily.parquet"]
            == receipt.get("base_sha256")
        ):
            raise RuntimeError(
                "research/formal lineage mismatch: rebuild research from the "
                "published formal base before staging"
            )
        research_hash = sha256_file(SOURCE)
        if research_hash != receipt.get("output_sha256"):
            raise RuntimeError("research receipt and feature table hash differ")
        selector = yaml.safe_load(SELECTOR.read_text(encoding="utf-8"))
        selected = selector["data"]["feature_include"]
        if len(selected) != 93 or len(selected) != len(set(selected)):
            raise RuntimeError("research selector is not the audited 93 value channels")
        schema = pq.read_schema(SOURCE)
        columns = set(schema.names)
        missing = [name for name in selected if name not in columns and name not in {
            "open_raw", "high_raw", "low_raw", "close_raw", "trading_volume_raw",
        }]
        if missing:
            raise RuntimeError(f"selected research fields absent: {missing}")
        patterns = selector["data"]["feature_availability_indicators"]
        availability = [
            name for name in selected
            if any(fnmatch.fnmatchcase(name, pattern) for pattern in patterns)
        ]
        if len(availability) != 50:
            raise RuntimeError(f"expected 50 availability channels, got {len(availability)}")
        metadata = pq.ParquetFile(SOURCE).metadata
        if metadata.num_rows <= 0 or "date" not in columns or "symbol" not in columns:
            raise RuntimeError("empty or malformed research table")
        end_date = str(pc.max(pq.read_table(SOURCE, columns=["date"])["date"]).as_py())
        if end_date != str(receipt.get("inputs", {}).get("end_date", end_date)):
            raise RuntimeError("research receipt end date differs from table")
        output = args.staged_root / "features" / SOURCE.name
        _stage_copy(SOURCE, output, research_hash)
        receipt_hash = sha256_file(RESEARCH_RECEIPT)
        _stage_copy(RESEARCH_RECEIPT, args.staged_root / "features" / RESEARCH_RECEIPT.name, receipt_hash)
        action_hashes = {}
        for name in required_members[1:]:
            source = LIVE_ROOT / name
            expected = inventory_hashes[name]
            if sha256_file(source) != expected:
                raise RuntimeError(f"formal action receipt changed after publication: {name}")
            _stage_copy(source, args.staged_root / name, expected)
            action_hashes[name] = expected
        staged = {
            "schema_version": 1,
            "status": "complete",
            "research_only": True,
            "historical_vintage_verified": False,
            "end_date": end_date,
            "base_dataset": "tw-public",
            "base_snapshot_id": args.base_snapshot_id,
            "base_feature_sha256": formal_hash,
            "feature_sha256": research_hash,
            "research_receipt_sha256": receipt_hash,
            "formal_action_members_sha256": action_hashes,
            "feature_rows": metadata.num_rows,
            "model_value_channels": len(selected),
            "model_availability_channels": len(availability),
            "model_channels": len(selected) + len(availability),
        }
        target = args.staged_root / "release_receipt.json"
        temporary = target.with_name(target.name + ".tmp." + uuid.uuid4().hex)
        try:
            temporary.write_text(json.dumps(staged, sort_keys=True, indent=2) + "\n", encoding="utf-8")
            os.chmod(temporary, 0o600)
            os.replace(temporary, target)
        finally:
            temporary.unlink(missing_ok=True)
        print(json.dumps(staged, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
