#!/usr/bin/env python3
"""Verify one TW minute session against receipt-backed Shioaji chunks.

Physical absence is not a missing trade. Repair only rows that are provably
different from the retained source, never invent a zero-volume or carried bar.
"""

from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import fcntl
import json
import os
from pathlib import Path
import sys
import tempfile
from typing import Any
from zoneinfo import ZoneInfo

import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from downloader.download_shioaji_tw_minute_kbars import minute_receipt_valid  # noqa: E402
from scripts.audit_shioaji_tw_minute_dataset import audit_frame  # noqa: E402
from scripts.build_shioaji_tw_minute_dataset import (  # noqa: E402
    EXECUTOR_ONLY_COLUMNS,
    FEATURE_STATISTICS_CONTRACT,
    MODEL_FEATURE_COLUMNS,
    SCHEMA_VERSION,
    _available_collection_symbols,
    _feature_statistics,
    _read_json,
    _sha256,
    _validate_collection_gate,
    build_research_frame,
)
from scripts.audit_tw_stock_minute_coverage import _expected_by_date  # noqa: E402

RAW_COLUMNS = (
    "symbol", "date", "ts", "market", "contract_unit", "Open", "High",
    "Low", "Close", "Volume", "Amount",
)


def compare_raw_frames(source: pl.DataFrame, derived: pl.DataFrame) -> dict[str, int | bool]:
    """Exact source-to-derived row parity, including raw numerical values."""

    source = source.select(RAW_COLUMNS).with_columns(
        pl.col("ts").cast(pl.Datetime("ns"))
    )
    derived = derived.select(RAW_COLUMNS).with_columns(
        pl.col("ts").cast(pl.Datetime("ns"))
    )
    keys = ["symbol", "ts"]
    source_duplicate_keys = source.group_by(keys).len().filter(pl.col("len") > 1).height
    derived_duplicate_keys = derived.group_by(keys).len().filter(pl.col("len") > 1).height
    missing = source.join(derived.select(keys), on=keys, how="anti").height
    extra = derived.join(source.select(keys), on=keys, how="anti").height
    if source_duplicate_keys or derived_duplicate_keys:
        # A duplicate key could make the join multiplicative; first repair the
        # key invariant rather than allocating a potentially huge cross product.
        mismatched = 0
    else:
        joined = source.join(derived, on=keys, how="inner", suffix="_derived")
        mismatched = joined.filter(pl.any_horizontal(*[
            ~pl.col(name).eq_missing(pl.col(f"{name}_derived"))
            for name in RAW_COLUMNS if name not in keys
        ])).height
    return {
        "source_rows": source.height,
        "derived_rows": derived.height,
        "source_duplicate_keys": source_duplicate_keys,
        "derived_duplicate_keys": derived_duplicate_keys,
        "source_rows_missing_from_derived": missing,
        "derived_rows_without_source": extra,
        "raw_value_mismatches": mismatched,
        "raw_value_comparison_skipped": bool(
            source_duplicate_keys or derived_duplicate_keys
        ),
    }


def validate_source_day(frame: pl.DataFrame, trade_date: date) -> dict[str, int]:
    """Reject malformed source values before any repair or completeness claim."""

    if frame.is_empty():
        return {"invalid_source_rows": 0}
    time = (pl.col("ts").dt.hour().cast(pl.Int16) * 60
            + pl.col("ts").dt.minute().cast(pl.Int16))
    prices = ["Open", "High", "Low", "Close"]
    invalid = frame.filter(
        (pl.col("date") != trade_date)
        | (pl.col("ts").dt.date() != trade_date)
        | (time < 9 * 60 + 1) | (time > 13 * 60 + 30)
        | (pl.col("ts").dt.second() != 0)
        | (pl.col("ts").dt.nanosecond() != 0)
        | pl.any_horizontal(*[
            pl.col(name).is_null() | ~pl.col(name).is_finite() | (pl.col(name) <= 0)
            for name in (*prices, "contract_unit")
        ])
        | pl.any_horizontal(*[
            pl.col(name).is_null() | ~pl.col(name).is_finite() | (pl.col(name) < 0)
            for name in ("Volume", "Amount")
        ])
        | (pl.col("High") < pl.max_horizontal(*prices))
        | (pl.col("Low") > pl.min_horizontal(*prices))
        | ((pl.col("Volume") == 0.0) & (pl.col("Amount") != 0.0))
        | ((pl.col("Volume") > 0.0) & (pl.col("Amount") <= 0.0))
    ).height
    return {"invalid_source_rows": invalid}


def _source_paths(
    input_root: Path, symbols: set[str], trade_date: date, *, simulation: bool
) -> tuple[list[Path], list[str], list[str]]:
    paths: list[Path] = []
    gaps: list[str] = []
    failures: list[str] = []
    for symbol in sorted(symbols):
        manifest = input_root / "symbols" / f"{symbol}.manifest.json"
        try:
            payload = _read_json(manifest)
            matches = [chunk for chunk in payload.get("chunks", [])
                       if chunk.get("start_date", "") <= trade_date.isoformat()
                       <= chunk.get("end_date", "")]
            if len(matches) != 1 or payload.get("symbol") != symbol:
                raise ValueError("missing or overlapping source chunk")
            chunk = matches[0]
            start = date.fromisoformat(chunk["start_date"])
            end = date.fromisoformat(chunk["end_date"])
            receipt = Path(str(chunk["receipt_path"]))
            if not minute_receipt_valid(
                receipt, symbol=symbol, start=start, end=end, simulation=simulation
            ):
                raise ValueError("source receipt or parquet SHA invalid")
            receipt_payload = _read_json(receipt)
            output_receipt = receipt_payload.get("output_receipt") or {}
            if (chunk.get("data_sha256") != output_receipt.get("sha256")
                    or chunk.get("source_gap_dates", [])
                    != receipt_payload.get("source_gap_dates", [])):
                raise ValueError("manifest and source receipt disagree")
            if chunk.get("data_path") and (
                str(chunk["data_path"]) != str(output_receipt.get("path"))
            ):
                raise ValueError("manifest and source parquet path disagree")
            if trade_date.isoformat() in chunk.get("source_gap_dates", []):
                gaps.append(symbol)
            if chunk.get("data_path"):
                paths.append(Path(str(chunk["data_path"])))
        except (OSError, KeyError, TypeError, ValueError) as exc:
            failures.append(f"{symbol}: {exc}")
    return paths, gaps, failures


def _day_summary(day: pl.DataFrame, trade_date: date,
                 source_summary: dict[str, Any], sha: str) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "feature_statistics_contract": FEATURE_STATISTICS_CONTRACT,
        "status": "ok", "source": "shioaji_kbars_1m",
        "trade_date": trade_date.isoformat(),
        "input_chunk_start": source_summary.get("input_chunk_start"),
        "input_chunk_end": source_summary.get("input_chunk_end"),
        "rows": day.height, "symbols": day["symbol"].n_unique(),
        "bars": day["ts"].n_unique(),
        "feature_valid_rows": int(day["feature_valid"].sum()),
        "label_valid_rows": int(day["label_valid_1m"].sum()),
        "model_feature_columns": list(MODEL_FEATURE_COLUMNS),
        "executor_only_columns": list(EXECUTOR_ONLY_COLUMNS),
        "output": f"trade_date={trade_date.isoformat()}/data.parquet",
        "output_sha256": sha,
        **_feature_statistics(day),
    }


def reconcile(input_root: Path, output_root: Path, trade_date: date,
              *, stock_root: Path = Path("data_tw_public/stocks"),
              repair: bool = False) -> dict[str, Any]:
    if trade_date >= datetime.now(ZoneInfo("Asia/Taipei")).date():
        raise ValueError("only completed trading sessions can be reconciled")
    if input_root.is_symlink() or output_root.is_symlink():
        raise RuntimeError("mutable source and research roots must not be symlinks")
    source_summary_path = input_root / "download_summary.json"
    source_summary = _validate_collection_gate(
        source_summary_path, selected_symbols=None, subset_requested=False
    )
    if not (date.fromisoformat(source_summary["start_date"]) <= trade_date
            <= date.fromisoformat(source_summary["end_date"])):
        raise ValueError("date outside terminal source catalog")
    symbols = _available_collection_symbols(source_summary_path, source_summary)
    paths, gaps, failures = _source_paths(
        input_root, symbols, trade_date, simulation=bool(source_summary["simulation"])
    )
    partition_dir = output_root / f"trade_date={trade_date.isoformat()}"
    partition = partition_dir / "data.parquet"
    receipt_path = partition_dir / "summary.json"
    manifest_path = output_root / "manifest.json"
    old_receipt = _read_json(receipt_path)
    manifest = _read_json(manifest_path)
    if (old_receipt.get("output_sha256") != _sha256(partition)
            or manifest.get("status") != "research_ready"
            or trade_date.isoformat() not in manifest.get("dates", [])):
        raise RuntimeError("derived partition or root manifest is not verified")
    if failures:
        return {"trade_date": trade_date.isoformat(), "status": "source_unassessable",
                "source_failures": failures, "source_gap_symbols": gaps}
    if not paths:
        raise RuntimeError("no verified source chunks for selected session")
    try:
        source = (pl.scan_parquet([str(p) for p in paths])
                  .filter(pl.col("date") == trade_date)
                  .select(RAW_COLUMNS).collect(engine="streaming"))
    except (OSError, pl.exceptions.PolarsError) as exc:
        return {"trade_date": trade_date.isoformat(), "status": "source_unassessable",
                "source_failures": [str(exc)], "source_gap_symbols": gaps}
    derived = pl.read_parquet(partition)
    source_checks = validate_source_day(source, trade_date)
    parity = compare_raw_frames(source, derived)
    try:
        derived_checks = audit_frame(derived, trade_date=trade_date)
        derived_audit_error = None
    except RuntimeError as exc:
        derived_checks = None
        derived_audit_error = str(exc)
    _, official_by_date = _expected_by_date(stock_root, trade_date, trade_date)
    positive_volume_symbols = set(official_by_date.get(trade_date, {}))
    observed_symbols = set(source["symbol"].to_list())
    unavailable_positive_volume = sorted(positive_volume_symbols - symbols)
    source_missing_positive_volume = sorted(
        (positive_volume_symbols & symbols) - observed_symbols
    )
    defects = sum(parity[key] for key in (
        "source_duplicate_keys", "derived_duplicate_keys",
        "source_rows_missing_from_derived", "derived_rows_without_source",
        "raw_value_mismatches",
    ))
    if source_checks["invalid_source_rows"] or parity["source_duplicate_keys"]:
        status = "invalid_source"
    elif defects or derived_audit_error:
        status = "derived_mismatch"
    elif gaps or unavailable_positive_volume or source_missing_positive_volume:
        status = "source_coverage_gap"
    else:
        status = "source_parity_verified"
    report = {
        "trade_date": trade_date.isoformat(),
        "status": status,
        "source_gap_symbols": gaps,
        "contract_unavailable_symbols": int(source_summary["contract_unavailable_symbols"]),
        "contract_unavailable_positive_volume_symbols": unavailable_positive_volume,
        "source_missing_positive_volume_symbols": source_missing_positive_volume,
        **source_checks, **parity,
        "derived_audit": derived_checks,
        "derived_audit_error": derived_audit_error,
        "complete_trade_tape_verified": False,
    }
    if not repair or (not defects and derived_audit_error is None):
        return report
    if source_checks["invalid_source_rows"] or parity["source_duplicate_keys"]:
        raise RuntimeError("refusing repair from invalid or duplicate source rows")
    if parity["derived_rows_without_source"]:
        raise RuntimeError("refusing to remove derived rows without source evidence")
    if datetime.now(ZoneInfo("Asia/Taipei")).time().isoformat() < "14:31:00" and datetime.now(ZoneInfo("Asia/Taipei")).time().isoformat() >= "07:45:00":
        raise RuntimeError("refusing research partition repair in live-priority window")
    rebuilt = build_research_frame(source.lazy()).collect(engine="streaming")
    audit_frame(rebuilt, trade_date=trade_date)
    lock_path = output_root / ".minute_repair.lock"
    with lock_path.open("a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if _sha256(partition) != old_receipt["output_sha256"]:
            raise RuntimeError("derived partition changed during reconciliation")
        if {path.name for path in partition_dir.iterdir()} != {
            "data.parquet", "summary.json"
        }:
            raise RuntimeError("refusing repair of partition with unexpected files")
        entries = manifest.get("partitions", [])
        if sum(entry.get("trade_date") == trade_date.isoformat()
               for entry in entries) != 1:
            raise RuntimeError("date missing or duplicated in root partition manifest")
        stage = Path(tempfile.mkdtemp(prefix=".minute-repair-", dir=output_root))
        staged_partition = stage / "data.parquet"
        rebuilt.write_parquet(staged_partition, compression="zstd",
                              compression_level=7, statistics=True,
                              row_group_size=128_000)
        new_receipt = _day_summary(rebuilt, trade_date, old_receipt,
                                   _sha256(staged_partition))
        (stage / "summary.json").write_text(
            json.dumps(new_receipt, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        quarantine = (output_root.parent / "quarantine" /
                      f"minute-repair-{trade_date}-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')}")
        quarantine.parent.mkdir(parents=True, exist_ok=True)
        os.replace(partition_dir, quarantine)
        os.replace(stage, partition_dir)
        for index, entry in enumerate(entries):
            if entry.get("trade_date") == trade_date.isoformat():
                entries[index] = new_receipt
                break
        manifest["written_at_utc"] = datetime.now(timezone.utc).isoformat()
        temporary = manifest_path.with_suffix(".json.tmp")
        temporary.write_text(json.dumps(manifest, ensure_ascii=False, indent=2,
                                        sort_keys=True) + "\n", encoding="utf-8")
        os.replace(temporary, manifest_path)
        report.update(status="locally_repaired", quarantined_original=str(quarantine),
                      repaired_rows=rebuilt.height)
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trade-date", required=True, type=date.fromisoformat)
    parser.add_argument("--input-root", type=Path, default=Path("data_tw_minute/shioaji_1m"))
    parser.add_argument("--output-root", type=Path, default=Path("data_tw_minute/research_dataset"))
    parser.add_argument("--stock-root", type=Path, default=Path("data_tw_public/stocks"))
    parser.add_argument("--repair", action="store_true")
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    result = reconcile(args.input_root, args.output_root, args.trade_date,
                       stock_root=args.stock_root, repair=args.repair)
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        temporary = args.report.with_suffix(args.report.suffix + ".tmp")
        temporary.write_text(json.dumps(result, ensure_ascii=False, indent=2,
                                        sort_keys=True) + "\n", encoding="utf-8")
        os.replace(temporary, args.report)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
