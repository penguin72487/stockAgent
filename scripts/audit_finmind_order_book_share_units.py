#!/usr/bin/env python3
"""Independently audit local FinMind order-book unit migration; never fetch data."""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
import hashlib
import json
from pathlib import Path
import sys

import pyarrow as pa
import pyarrow.parquet as pq


ROOT = Path(__file__).resolve().parents[1]
DATASET = "TaiwanStockStatisticsOfOrderBookAndTrade"
MULTIPLIERS = {
    "TotalBuyVolume": ("TotalBuyVolume_shares", 1000),
    "TotalSellVolume": ("TotalSellVolume_shares", 1000),
    "TotalDealVolume": ("TotalDealVolume_shares", 1000),
    "TotalDealMoney": ("TotalDealMoney_twd", 1_000_000),
}
MIGRATION_KEYS = {
    "volume_units", "parquet_size_bytes", "sha256", "unit_normalized_at_utc",
    "unit_normalization_previous_source", "unit_normalization_recovery",
}


def _verify_quantities(table: pa.Table) -> None:
    """Use independent decimal arithmetic on every original source value."""

    for raw_name, (canonical, multiplier) in MULTIPLIERS.items():
        if raw_name not in table.column_names or canonical not in table.column_names:
            raise ValueError(f"missing quantity column: {raw_name}/{canonical}")
        dtype = pa.float64() if canonical.endswith("_twd") else pa.int64()
        if table.schema.field(canonical).type != dtype:
            raise ValueError(f"canonical dtype mismatch: {canonical}")
        raw_values = table[raw_name].to_pylist()
        canonical_values = table[canonical].to_pylist()
        for index, (raw, actual) in enumerate(zip(raw_values, canonical_values, strict=True)):
            if raw is None or isinstance(raw, bool) or actual is None:
                raise ValueError(f"missing/boolean quantity: {canonical} row {index}")
            try:
                expected = Decimal(str(raw)) * multiplier
            except (InvalidOperation, ValueError) as exc:
                raise ValueError(f"invalid numeric source: {raw_name} row {index}") from exc
            if not expected.is_finite() or expected < 0:
                raise ValueError(f"nonfinite/negative quantity: {raw_name} row {index}")
            if canonical.endswith("_shares"):
                if expected != expected.to_integral_value() or expected > 2**63 - 1:
                    raise ValueError(f"source cannot resolve to Int64 shares: {raw_name} row {index}")
                if actual != int(expected):
                    raise ValueError(f"share quantity mismatch: {canonical} row {index}")
            elif actual != float(expected) or not Decimal(str(actual)).is_finite():
                raise ValueError(f"TWD quantity mismatch: {canonical} row {index}")


def audit(root: Path, *, expected_receipts: int | None = None, progress: bool = False) -> dict:
    root = root.resolve()
    receipt_root = root / "receipts" / DATASET
    paths = sorted(receipt_root.glob("*.json"))
    signatures: dict[Path, tuple[int, int, int]] = {}
    totals, statuses, grains = Counter(), Counter(), Counter()
    records, errors = [], []

    def checked_bytes(path: Path) -> bytes:
        path = path.resolve()
        if not path.is_relative_to(root):
            raise ValueError("source path escapes FinMind root")
        stat = path.stat()
        signature = stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns
        if path in signatures and signatures[path] != signature:
            raise ValueError("source changed during audit")
        signatures[path] = signature
        return path.read_bytes()

    def load_receipt(path: Path) -> tuple[dict, str]:
        payload = checked_bytes(path)
        value = json.loads(payload)
        if not isinstance(value, dict):
            raise ValueError("receipt is not an object")
        return value, hashlib.sha256(payload).hexdigest()

    def checked_table(path: Path, receipt: dict, expected_sha: str) -> pa.Table:
        payload = checked_bytes(path)
        if (hashlib.sha256(payload).hexdigest() != expected_sha
                or receipt.get("sha256") != expected_sha):
            raise ValueError("Parquet SHA-256 mismatch")
        if len(payload) != receipt.get("parquet_size_bytes"):
            raise ValueError("Parquet byte-size mismatch")
        table = pq.ParquetFile(pa.BufferReader(payload)).read()
        if table.num_rows != receipt.get("rows"):
            raise ValueError("Parquet row-count mismatch")
        return table

    for number, path in enumerate(paths, 1):
        row = {"receipt": str(path.relative_to(root)), "errors": []}
        records.append(row)
        try:
            day = date.fromisoformat(path.stem)
            current, receipt_sha = load_receipt(path)
            source = root / "market_intraday" / DATASET / f"year={day.year}" / f"date={day}.parquet"
            if (current.get("dataset") != DATASET or current.get("date") != str(day)
                    or current.get("parquet_path") != str(source.relative_to(root))):
                raise ValueError("current receipt date/dataset/path mismatch")
            previous = current["unit_normalization_previous_source"]
            prior_sha = previous["sha256"]
            if (not isinstance(prior_sha, str) or len(prior_sha) != 64
                    or any(char not in "0123456789abcdef" for char in prior_sha)):
                raise ValueError("invalid archived source digest")
            version_root = root / "versions" / DATASET / str(day)
            old_source, old_receipt_path = version_root / f"{prior_sha}.parquet", version_root / f"{prior_sha}.json"
            if (previous.get("parquet_path") != str(old_source.relative_to(root))
                    or previous.get("receipt_path") != str(old_receipt_path.relative_to(root))):
                raise ValueError("archived source/receipt path mismatch")
            original, original_receipt_sha = load_receipt(old_receipt_path)
            if (original.get("dataset") != DATASET or original.get("date") != str(day)
                    or original.get("parquet_path") != str(source.relative_to(root))):
                raise ValueError("archived receipt identity mismatch")
            before = checked_table(old_source, original, prior_sha)
            after = checked_table(source, current, current["sha256"])
            canonical = {name for name, _ in MULTIPLIERS.values()}
            raw_columns = [name for name in before.column_names if name not in canonical]
            if (set(after.column_names) != set(raw_columns) | canonical
                    or not before.select(raw_columns).equals(after.select(raw_columns), check_metadata=True)):
                raise ValueError("migration changed original provider arrays, dtypes, or metadata")
            original_metadata = {key: value for key, value in original.items() if key not in MIGRATION_KEYS}
            current_metadata = {key: value for key, value in current.items() if key not in MIGRATION_KEYS}
            if original_metadata != current_metadata:
                raise ValueError("migration changed original receipt metadata/status/grain/fetched_at")
            recovery = current.get("unit_normalization_recovery")
            if recovery is not None and (
                not isinstance(recovery, dict)
                or recovery.get("reason") != "interrupted_parquet_receipt_commit"
                or recovery.get("reconstructed_from_sha256") != prior_sha
                or recovery.get("verified_new_sha256") != current["sha256"]
                or recovery.get("canonical_table_equal") is not True
            ):
                raise ValueError("unit-normalization recovery proof does not match archived/current source")
            units = current.get("volume_units") or {}
            if (units.get("contract_version") != 1 or units.get("normalization_valid") is not True
                    or units.get("invalid_rows") != 0 or units.get("invalid_fields") != {}
                    or units.get("multipliers") != {key: value[1] for key, value in MULTIPLIERS.items()}
                    or units.get("market_scope") != "twse_regular_trading_only"):
                raise ValueError("current unit receipt is invalid or has different scope/multipliers")
            _verify_quantities(after)
            totals.update(verified_receipts=1, rows=after.num_rows,
                          original_parquet_bytes=original["parquet_size_bytes"],
                          current_parquet_bytes=current["parquet_size_bytes"])
            statuses[str(current.get("status"))] += 1
            grains[str(current.get("observed_grain"))] += 1
            row.update(date=str(day), rows=after.num_rows, current_sha256=current["sha256"],
                       original_sha256=prior_sha, receipt_sha256=receipt_sha,
                       archived_receipt_sha256=original_receipt_sha)
        except (OSError, ValueError, KeyError, TypeError, pa.ArrowException) as exc:
            message = f"{type(exc).__name__}: {exc}"
            row["errors"].append(message)
            errors.append({"receipt": row["receipt"], "error": message})
        if progress and number % 100 == 0:
            print(f"[finmind-unit-audit] checked={number} failures={len(errors)}", file=sys.stderr, flush=True)
    if expected_receipts is not None and len(paths) != expected_receipts:
        errors.append({"error": f"expected {expected_receipts} current receipts, found {len(paths)}"})
    if not paths:
        errors.append({"error": "no current order-book receipts"})
    if sorted(receipt_root.glob("*.json")) != paths:
        errors.append({"error": "current receipt set changed during audit"})
    for path, expected in signatures.items():
        try:
            stat = path.stat()
            if (stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns) != expected:
                errors.append({"error": f"source changed during audit: {path}"})
        except OSError as exc:
            errors.append({"error": f"source disappeared: {path}: {exc}"})
    return {
        "schema_version": 1, "checked_at_utc": datetime.now(UTC).isoformat(),
        "dataset": DATASET, "source_root": str(root), "passed": not errors,
        "current_receipts": len(paths), "totals": dict(totals),
        "preserved_status_counts": dict(statuses), "preserved_grain_counts": dict(grains),
        "checked_files": len(signatures), "errors": errors, "partitions": records,
        "scope": "local quantity migration integrity; does not establish completeness, PIT or training eligibility",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT / "data_finmind")
    parser.add_argument("--expected-receipts", type=int)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.output is not None and args.output.resolve().is_relative_to(args.root.resolve()):
        parser.error("audit output must be outside the source root")
    result = audit(args.root, expected_receipts=args.expected_receipts, progress=True)
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({key: value for key, value in result.items() if key != "partitions"}, ensure_ascii=False, indent=2))
    raise SystemExit(0 if result["passed"] else 1)


if __name__ == "__main__":
    main()
