#!/usr/bin/env python3
"""Stage the verified FinLab-enriched research feature ABI for private cold delivery.

The account-scoped raw data_finlab tree is deliberately not part of this release.
This is a current-revision research table, never a historical PIT or live model.
"""

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
    LIVE_ROOT, LOCK, _required_formal_members, _stage_copy,
)
from scripts.finlab_release_gate import catalog_readiness  # noqa: E402
from scripts.download_finlab_history import safe_stem  # noqa: E402
from stockagent.data_sync.desync_snapshots import sha256_file  # noqa: E402

SOURCE = ROOT / "artifacts/research_features/tw_public_research_finlab_2014_v4.parquet"
BASE = ROOT / "artifacts/research_features/tw_public_research_all_2014_v3.parquet"
STAGED_ROOT = Path("/srv/stockagent-live/data_tw_public_research_finlab_2014_v4")
STOCK_CHANNELS = 20


def validated_source(source: Path, base: Path, live_root: Path) -> tuple[dict, dict, str, int, int, int, str]:
    receipt = json.loads(source.with_suffix(".finlab_research.json").read_text(encoding="utf-8"))
    base_receipt = json.loads(base.with_suffix(".all_features.json").read_text(encoding="utf-8"))
    if receipt.get("research_only") is not True or receipt.get("historical_point_in_time") is not False:
        raise RuntimeError("FinLab overlay is not the expected research-only contract")
    source_hash = sha256_file(source)
    if source_hash != receipt.get("output_sha256"):
        raise RuntimeError("FinLab overlay/receipt hash mismatch")
    base_hash = sha256_file(base)
    if base_hash != base_receipt.get("output_sha256") or base_hash != receipt.get("inputs", {}).get("base_sha256"):
        raise RuntimeError("FinLab overlay/all-observed base lineage mismatch")
    formal_hash = sha256_file(live_root / "features/tw_public_stock_daily.parquet")
    if formal_hash != base_receipt.get("inputs", {}).get("official", {}).get("sha256"):
        raise RuntimeError("formal TW feature source changed since research overlay build")
    schema = pq.read_schema(source)
    public = [name for name in schema.names if name.startswith("twpub_")]
    finlab = [name for name in schema.names if name.startswith("twfl_")]
    if len(public) != 204 or len(finlab) != len(receipt.get("feature_columns") or ()) or len(finlab) != 104:
        raise RuntimeError("research channel schema does not match expected 204+104 ABI")
    if set(finlab) != set(receipt["feature_columns"]):
        raise RuntimeError("FinLab channel names differ from source receipt")
    metadata = pq.ParquetFile(source).metadata
    if metadata.num_rows != int(receipt.get("rows", -1)) or metadata.num_rows <= 0:
        raise RuntimeError("research row count does not match source receipt")
    if not {"date", "symbol"}.issubset(schema.names):
        raise RuntimeError("research table lacks date/symbol keys")
    end_date = str(pc.max(pq.read_table(source, columns=["date"])["date"]).as_py())
    return receipt, base_receipt, source_hash, len(public), len(finlab), metadata.num_rows, end_date


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=SOURCE)
    parser.add_argument("--base", type=Path, default=BASE)
    parser.add_argument("--live-root", type=Path, default=LIVE_ROOT)
    parser.add_argument("--staged-root", type=Path, default=STAGED_ROOT)
    parser.add_argument("--finlab-root", type=Path, default=ROOT / "data_finlab")
    args = parser.parse_args(argv)
    LOCK.parent.mkdir(parents=True, exist_ok=True)
    with LOCK.open("a+") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        readiness = catalog_readiness(args.finlab_root, verify_hashes=True)
        if not readiness["ready"]:
            raise RuntimeError(
                "FinLab catalog not current; private packaging blocked "
                f"(missing={readiness['missing']}, stale={readiness['stale']}, "
                f"invalid={readiness['invalid']}, catalog_fresh={readiness['catalog_fresh']})"
            )
        source_receipt, base_receipt, source_hash, public_count, finlab_count, rows, end_date = validated_source(
            args.source, args.base, args.live_root
        )
        for key, source in (source_receipt.get("inputs", {}).get("sources") or {}).items():
            current_path = args.finlab_root / "receipts" / f"{safe_stem(key)}.json"
            try:
                current = json.loads(current_path.read_text(encoding="utf-8"))
            except (OSError, ValueError) as exc:
                raise RuntimeError("FinLab research input receipt is missing") from exc
            if current.get("dataset") != key or current.get("sha256") != source.get("sha256"):
                raise RuntimeError("FinLab research overlay predates a current source revision")
        entitlement_summary = json.loads(
            (args.live_root / "tw_corporate_action_entitlements.summary.json").read_text(encoding="utf-8")
        )
        required = _required_formal_members(entitlement_summary, args.live_root)
        action_hashes = {member: sha256_file(args.live_root / member) for member in required[1:]}
        relative_feature = Path("features") / args.source.name
        relative_receipt = Path("features") / args.source.with_suffix(".finlab_research.json").name
        receipt_hash = sha256_file(args.source.with_suffix(".finlab_research.json"))
        previous_path = args.staged_root / "release_receipt.json"
        try:
            previous = json.loads(previous_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            previous = {}
        if (
            previous.get("status") == "complete"
            and previous.get("catalog_latest_verified") is True
            and previous.get("source_hashes_verified") is True
            and previous.get("staged_files_sha256") == {
                relative_feature.as_posix(): source_hash,
                relative_receipt.as_posix(): receipt_hash,
            }
            and previous.get("formal_action_members_sha256") == action_hashes
            and previous.get("base_feature_sha256") == base_receipt["inputs"]["official"]["sha256"]
            and previous.get("source_receipts_sha256") == readiness["source_receipts_sha256"]
            and (args.staged_root / relative_feature).is_file()
            and (args.staged_root / relative_receipt).is_file()
            and all((args.staged_root / member).is_file() for member in action_hashes)
            and sha256_file(args.staged_root / relative_feature) == source_hash
            and sha256_file(args.staged_root / relative_receipt) == receipt_hash
            and all(sha256_file(args.staged_root / member) == expected for member, expected in action_hashes.items())
        ):
            print(json.dumps({"event": "finlab_research_stage_unchanged", "rows": rows, "channels": previous["model_channels"]}))
            return 0
        staged_hashes = {}
        for original, relative in (
            (args.source, relative_feature),
            (args.source.with_suffix(".finlab_research.json"), relative_receipt),
        ):
            expected = source_hash if original == args.source else receipt_hash
            _stage_copy(original, args.staged_root / relative, expected)
            staged_hashes[relative.as_posix()] = expected
        for member, expected in action_hashes.items():
            original = args.live_root / member
            _stage_copy(original, args.staged_root / member, expected)
        release = {
            "schema_version": 1,
            "status": "complete",
            "research_only": True,
            "historical_point_in_time": False,
            "strict_training_eligible": False,
            "private_personal_research": True,
            "end_date": end_date,
            "base_dataset": "tw-public",
            "base_feature_sha256": base_receipt["inputs"]["official"]["sha256"],
            "base_research_sha256": source_receipt["inputs"]["base_sha256"],
            "feature_rows": rows,
            "public_value_channels": public_count,
            "finlab_value_channels": finlab_count,
            "stock_value_channels": STOCK_CHANNELS,
            "model_channels": STOCK_CHANNELS + 2 * (public_count + finlab_count),
            "catalog_latest_verified": True,
            "catalog_total": readiness["catalog_total"],
            "catalog_checked_at_utc": readiness["checked_at_utc"],
            "source_receipts_sha256": readiness["source_receipts_sha256"],
            "source_hashes_verified": readiness["hashes_verified"],
            "staged_files_sha256": staged_hashes,
            "formal_action_members_sha256": action_hashes,
            "source_revision_policy": "current_revision_only",
            "use_restriction": "owner_private_noncommercial_research_only_no_public_raw_delivery",
        }
        target = args.staged_root / "release_receipt.json"
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_suffix(".json.tmp")
        try:
            temporary.write_text(json.dumps(release, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
            temporary.chmod(0o600)
            temporary.replace(target)
        finally:
            temporary.unlink(missing_ok=True)
        print(json.dumps({"event": "finlab_research_staged", "rows": rows, "channels": release["model_channels"], "end_date": end_date}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
