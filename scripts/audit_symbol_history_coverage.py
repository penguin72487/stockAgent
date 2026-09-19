#!/usr/bin/env python3
"""Audit manifest symbols against stored Parquet footers without downloading data.

The base and hot tail overlap, so their row counts are deliberately separate.
This report proves storage/schema observations, not provider availability,
historical completeness, publication time, or trainability.
"""

from __future__ import annotations

import argparse
import csv
from datetime import UTC, datetime
import io
import json
from pathlib import Path
import sys


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from downloader.artifact_io import atomic_write_text  # noqa: E402


SOURCES = (
    ("tw_official", "data_tw_public/stocks", "code", None, False),
    ("yahoo_us", "data_yahoo/us_stocks", "code", None, False),
    ("binance_usdm", "data_binance/1m", "code", "onboard_time", True),
    ("okx_swap", "data_okx/1m", "code", "list_time", True),
    ("bybit_perp", "data_bybit/1m", "code", "launch_time", True),
)

TW_REQUIRED_BAR_FIELDS = frozenset({
    "date", "open", "max", "min", "close", "Trading_Volume", "adjclose",
})
TW_EVENT_ONLY_FIELDS = frozenset({
    "fallback_reason", "raw_ohlc_scale_factor", "raw_ohlc_scale_reference_date",
    "adjustment_reference_date", "adjustment_reference_price",
    "adjustment_reference_kind", "ohlc_normalization",
    "return_quarantine_reason", "official_listing_evidence",
})
TW_TRAINING_FEATURE_PATH = "data_tw_public/features/tw_public_stock_daily.parquet"


def _field_interpretation(source: str, field: str, state: str) -> str:
    if source == "tw_official" and field in TW_REQUIRED_BAR_FIELDS:
        return "required_bar_gap" if state != "all_present" else "required_bar_present"
    if source == "tw_official" and field in TW_EVENT_ONLY_FIELDS:
        return "conditional_evidence_not_a_required_value"
    if source == "tw_public_training" and state == "all_null":
        return "no_point_in_time_value_in_current_build"
    if source == "tw_public_training" and state == "partial":
        return "sparse_requires_source_and_population_review"
    return "inspect_source_grain_and_history" if state != "all_present" else "present"


def _verified_stats(path: Path, cache: dict) -> tuple[dict | None, str]:
    if not path.is_file():
        return None, "missing_file"
    entry = cache.get(str(path))
    if not isinstance(entry, dict):
        return None, "not_scanned"
    stat = path.stat()
    if entry.get("size") != stat.st_size or entry.get("mtime_ns") != stat.st_mtime_ns:
        return None, "cache_stale"
    stats = entry.get("stats")
    if not isinstance(stats, dict):
        return None, "invalid_parquet"
    return stats, "footer_verified"


def _calendar_gap_hours(start_text: str | None, first_text: str | None) -> float | None:
    if not start_text or not first_text:
        return None
    try:
        start = datetime.fromisoformat(start_text.replace(" ", "T"))
        first = datetime.fromisoformat(first_text.replace("Z", "+00:00"))
        if start.tzinfo is None:
            start = start.replace(tzinfo=UTC)
        if first.tzinfo is None:
            first = first.replace(tzinfo=UTC)
        return round(max(0.0, (first - start).total_seconds() / 3600), 3)
    except ValueError:
        return None


def build_audit(repo_root: Path, cache_payload: dict) -> tuple[list[dict], list[dict], dict]:
    cache = cache_payload.get("files", {})
    schemas = cache_payload.get("schemas", {})
    if not isinstance(cache, dict) or not isinstance(schemas, dict):
        raise ValueError("invalid record inventory cache")
    symbol_rows: list[dict] = []
    field_rows: list[dict] = []
    source_summary: dict[str, dict] = {}
    for source, relative_dir, code_key, listed_key, has_tail in SOURCES:
        directory = repo_root / relative_dir
        manifest = directory / "symbols.csv"
        if not manifest.is_file():
            source_summary[source] = {"state": "manifest_missing"}
            continue
        with manifest.open(newline="", encoding="utf-8") as handle:
            records = list(csv.DictReader(handle))
        by_code = {
            str(record.get(code_key) or "").strip(): record
            for record in records if str(record.get(code_key) or "").strip()
        }
        stored_codes = {
            path.name.removesuffix("_features.parquet")
            for path in directory.glob("*_features.parquet")
        }
        for code in sorted(set(by_code) | stored_codes):
            record = by_code.get(code, {})
            base_path = directory / f"{code}_features.parquet"
            tail_path = directory / "_hot_tail" / base_path.name
            base, base_state = _verified_stats(base_path, cache)
            tail, tail_state = (
                _verified_stats(tail_path, cache)
                if has_tail else (None, "not_applicable")
            )
            parts = (("base", base_path, base, base_state),)
            if has_tail:
                parts += (("hot_tail", tail_path, tail, tail_state),)
            for part, path, stats, state in parts:
                if stats is None:
                    continue
                fields = schemas.get(stats.get("schema_id"), [])
                values = stats.get("non_null", [])
                if not isinstance(fields, list) or not isinstance(values, list) or len(fields) != len(values):
                    continue
                for (field, dtype), non_null in zip(fields, values, strict=True):
                    field_state = (
                        "unknown_null_count" if non_null is None
                        else "all_null" if non_null == 0
                        else "partial" if non_null < int(stats.get("count") or 0)
                        else "all_present"
                    )
                    field_rows.append({
                        "source": source, "symbol": code, "part": part,
                        "field": field, "dtype": dtype,
                        "rows": stats.get("count"), "non_null": non_null,
                        "state": field_state,
                        "interpretation": _field_interpretation(source, field, field_state),
                    })
            first = base.get("first") if base else None
            latest = max(
                (part.get("last") for part in (base, tail) if part and part.get("last")),
                default=None,
            )
            listed = str(record.get(listed_key) or "") if listed_key else ""
            symbol_rows.append({
                "source": source, "symbol": code,
                "manifest_state": "current_manifest" if code in by_code else "stored_not_current_manifest",
                "listed_at_utc": listed, "base_state": base_state,
                "tail_state": tail_state, "base_rows": base.get("count") if base else None,
                "tail_physical_rows": tail.get("count") if tail else None,
                "first_stored": first, "base_last": base.get("last") if base else None,
                "tail_last": tail.get("last") if tail else None,
                "latest_stored": latest,
                "listing_to_first_stored_hours": _calendar_gap_hours(listed, first),
            })
        source_rows = [row for row in symbol_rows if row["source"] == source]
        source_summary[source] = {
            "manifest_symbols": len(by_code),
            "stored_not_current_manifest": sum(row["manifest_state"] != "current_manifest" for row in source_rows),
            "base_verified": sum(row["base_state"] == "footer_verified" for row in source_rows),
            "base_missing": sum(row["base_state"] == "missing_file" for row in source_rows),
            "base_unverified": sum(row["base_state"] not in {"footer_verified", "missing_file"} for row in source_rows),
            "tail_verified": sum(row["tail_state"] == "footer_verified" for row in source_rows),
            "listing_head_gap_candidates": sum(
                isinstance(row["listing_to_first_stored_hours"], float)
                and row["listing_to_first_stored_hours"] > 1.0
                for row in source_rows
            ),
            "required_bar_field_gaps": sum(
                row["source"] == source and row["part"] == "base"
                and row["interpretation"] == "required_bar_gap"
                for row in field_rows
            ) if source == "tw_official" else None,
        }
    feature_path = repo_root / TW_TRAINING_FEATURE_PATH
    feature_stats, feature_state = _verified_stats(feature_path, cache)
    if feature_stats is not None:
        feature_fields = schemas.get(feature_stats.get("schema_id"), [])
        values = feature_stats.get("non_null", [])
        if isinstance(feature_fields, list) and isinstance(values, list) and len(feature_fields) == len(values):
            total_rows = int(feature_stats.get("count") or 0)
            for (field, dtype), non_null in zip(feature_fields, values, strict=True):
                state = (
                    "unknown_null_count" if non_null is None
                    else "all_null" if non_null == 0
                    else "partial" if non_null < total_rows
                    else "all_present"
                )
                field_rows.append({
                    "source": "tw_public_training", "symbol": "__PANEL__",
                    "part": "stock_and_market", "field": field, "dtype": dtype,
                    "rows": total_rows, "non_null": non_null, "state": state,
                    "interpretation": _field_interpretation("tw_public_training", field, state),
                })
    feature_rows = [row for row in field_rows if row["source"] == "tw_public_training"]
    source_summary["tw_public_training"] = {
        "file_state": feature_state,
        "physical_rows": feature_stats.get("count") if feature_stats else None,
        "first_stored": feature_stats.get("first") if feature_stats else None,
        "latest_stored": feature_stats.get("last") if feature_stats else None,
        "fields": len(feature_rows),
        "all_null_fields": sum(row["state"] == "all_null" for row in feature_rows),
        "basis": "Physical stock and market rows are mixed; null fractions are not symbol coverage.",
    }
    summary = {
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "sources": source_summary,
        "symbol_rows": len(symbol_rows), "field_rows": len(field_rows),
        "basis": (
            "Parquet footer and current symbol manifests only. Hot-tail physical rows overlap "
            "base rows; no unique total or source-history completeness is claimed. "
            "Listing-to-first gaps are candidates, not proof of recoverable candles."
        ),
    }
    return symbol_rows, field_rows, summary


def _csv_text(rows: list[dict], columns: tuple[str, ...]) -> str:
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=columns)
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--output-dir", type=Path, default=Path("artifacts/data_completeness/latest"))
    args = parser.parse_args()
    root = args.repo_root.resolve()
    cache_path = root / "artifacts/live/data_monitor/record_inventory_cache.json"
    cache_payload = json.loads(cache_path.read_text(encoding="utf-8"))
    symbols, fields, summary = build_audit(root, cache_payload)
    output_dir = args.output_dir if args.output_dir.is_absolute() else root / args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    atomic_write_text(output_dir / "symbols.csv", _csv_text(symbols, tuple(symbols[0]) if symbols else ("source", "symbol")))
    atomic_write_text(output_dir / "fields.csv", _csv_text(fields, tuple(fields[0]) if fields else ("source", "symbol", "field")))
    atomic_write_text(output_dir / "summary.json", json.dumps(summary, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps(summary, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
