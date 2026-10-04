#!/usr/bin/env python3
"""Extend the curated feature matrix with explicitly estimated release clocks.

Never edits source data, the strict dataset, raw publication receipts or a live
config. No downloads, fitting, normalization, labels, training or publication.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import UTC, date, datetime, timedelta
import json
import os
from pathlib import Path
import sys

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json
from scripts.prepare_tw_day_trade_feature_catalog import sha256, write_csv
from scripts.curate_tw_day_trade_training_dataset import validate_matrix
from stockagent.data.finlab_research_overlay import _verified_source, _wide_stock, _wide_market
from stockagent.data.tw_day_trade_feature_admission import classify_candidate, CATEGORY_LABELS
from stockagent.data.tw_public_release_schedule import (
    CONTRACT, align_observations, feature_name, feature_category, next_session, rule_for,
    rule_manifest, schedule_lookup, apply_known_release_overrides, max_carry_days,
)


def read_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def normalize_source(path: Path, key: str, sessions: list[date], symbols: list[str],
                     uploads: pl.DataFrame | None = None, *,
                     publication_bounds: pl.DataFrame | None = None,
                     quality_rows: list[dict] | None = None):
    rule, feature = rule_for(key), feature_name(key)
    if rule is None:
        raise ValueError("no source-family rule")
    schema = pl.scan_parquet(path).collect_schema()
    if "source_index" not in schema:
        raise ValueError("no period/index column")
    fields = [n for n in schema if n != "source_index"]
    if not fields or any(not (schema[n].is_numeric() or schema[n] == pl.Null) for n in fields):
        raise ValueError("non-numeric or ambiguous source schema; requires a dedicated adapter")
    indexes = pl.read_parquet(path, columns=["source_index"]).get_column("source_index").to_list()
    if len(indexes) != len(set(indexes)) or any(x is None for x in indexes):
        raise ValueError("duplicate/null source index")
    lookup = apply_known_release_overrides(schedule_lookup(indexes, rule, sessions), key, sessions)
    usable = lookup.filter(pl.col("date").is_not_null()).select("source_index", "date")
    if rule.kind == "quarter" and uploads is not None:
        usable = lookup.select("source_index", pl.col("date").fill_null(sessions[-1] + timedelta(days=1)))
    if rule.scope == "market":
        table = _wide_market(path, feature, sessions, date_lookup=usable)
    else:
        table = _wide_stock(path, feature, sessions, date_lookup=usable, keep_source_index=True,
                            defer_collapse=True)
        table = table.filter(pl.col("symbol").is_in(symbols))
        table = table.join(lookup.select("source_index", "estimated_published_on"),
                           on="source_index", how="left", validate="m:1")
        if rule.kind in {"quarter", "revenue"}:
            # TDR foreign reporting periods/deadlines do not obey this envelope.
            table = table.filter(~pl.col("symbol").str.starts_with("91"))
        if rule.kind == "quarter" and uploads is not None:
            table = table.join(uploads, on=["source_index", "symbol"], how="left", validate="m:1")
            late = {d: next_session(d, sessions) for d in table["known_upload_on"].drop_nulls().unique()}
            table = table.with_columns(pl.col("known_upload_on").replace_strict(
                late, default=None, return_dtype=pl.Date).alias("known_usable_on"))
            # A known publication outside our calendar must remain unavailable.
            table = table.filter(pl.col("known_upload_on").is_null() | pl.col("known_usable_on").is_not_null())
            table = table.with_columns(
                pl.coalesce("known_usable_on", "date").alias("date"),
                pl.coalesce("known_upload_on", "estimated_published_on").alias("estimated_published_on"))
            if table.filter(pl.col("date") <= pl.col("known_upload_on")).height:
                raise ValueError("observation precedes known provider upload")
        if publication_bounds is not None:
            bounds = publication_bounds.filter(pl.col("dataset") == key).drop("dataset")
            table = table.join(bounds, on=["source_index", "symbol"], how="left", validate="m:1")
            delayed = {d: next_session(d, sessions) for d in table["known_published_on"].drop_nulls().unique()}
            table = table.with_columns(pl.col("known_published_on").replace_strict(
                delayed, default=None, return_dtype=pl.Date).alias("_bound_session"))
            table = table.filter(pl.col("known_published_on").is_null() | pl.col("_bound_session").is_not_null())
            table = table.with_columns(pl.max_horizontal("date", "_bound_session").alias("date"),
                pl.max_horizontal("estimated_published_on", "known_published_on").alias("estimated_published_on"))
        table = table.filter(pl.col("date") <= sessions[-1])
        if table.filter(pl.col("estimated_published_on").is_null() |
                        (pl.col("date") <= pl.col("estimated_published_on"))).height:
            raise ValueError("observation precedes estimated publication boundary")
        # A late restatement of an old quarter must not replace a newer quarter.
        if rule.kind in {"quarter", "revenue"}:
            table = table.sort("symbol", "date", "source_index").with_columns(
                pl.col("source_index").rank("dense").over("symbol").alias("_period_order"))
            table = table.with_columns(pl.col("_period_order").cum_max().over("symbol").alias("_newest"))
            table = table.filter(pl.col("_period_order") == pl.col("_newest"))
        table = table.sort("date", "symbol", "source_index").unique(
            ["date", "symbol"], keep="last", maintain_order=True)
    # Keep row-level clocks only in audit observations, never in model X.
    clock_columns = [c for c in ("source_index", "estimated_published_on", "known_upload_on", "known_published_on") if c in table.columns]
    table = table.select("date", "symbol", feature, *clock_columns)
    if key == "etl:inventory:大於四百張佔比" or (key.startswith("foreign_investors_shareholding:") and "比率" in key):
        invalid = table.filter(~pl.col(feature).is_between(0, 100))
        if quality_rows is not None:
            quality_rows.extend({"dataset": key, "feature": feature,
                "date": str(row["date"]), "symbol": row["symbol"], "source_index": row["source_index"],
                "original_value": row[feature], "reason": "percentage_outside_0_100"}
                for row in invalid.to_dicts())
        # Retain the invalid observation's key as a NULL barrier. Dropping it
        # would silently carry a previous valid state across the bad report.
        table = table.with_columns(pl.when(pl.col(feature).is_between(0, 100))
                                   .then(pl.col(feature)).otherwise(None).alias(feature))
    if key == "tw_etf_nav_daily:折溢價(%)":
        invalid = table.filter(pl.col(feature) <= -100)
        if quality_rows is not None:
            quality_rows.extend({"dataset": key, "feature": feature, "date": str(row["date"]),
                "symbol": row["symbol"], "source_index": row["source_index"],
                "original_value": row[feature], "reason": "discount_impossible_for_positive_price_and_nav"}
                for row in invalid.to_dicts())
        table = table.with_columns(pl.when(pl.col(feature) > -100).then(pl.col(feature)).otherwise(None).alias(feature))
    return table, lookup


def retain_invalid_barriers(original: pl.DataFrame, repaired: pl.DataFrame, feature: str):
    """An unresolved invalid report must interrupt state carry after repair."""
    keys = ["date", "symbol"]
    invalid = original.filter(pl.col(feature).is_null())
    unresolved = invalid.join(repaired.filter(pl.col(feature).is_not_null()).select(keys), on=keys, how="anti")
    repaired = repaired.join(unresolved.select(keys), on=keys, how="anti")
    return pl.concat([repaired, unresolved], how="diagonal_relaxed").sort(keys)


def load_uploads(finlab: Path):
    found = _verified_source(finlab, "financial_statements_upload_detail:upload_date")
    if found is None:
        return None, None
    path, receipt = found
    frame = pl.read_parquet(path)
    fields = [c for c in frame.columns if c != "source_index"]
    table = frame.unpivot(index="source_index", on=fields, variable_name="symbol", value_name="known_upload_on")
    table = table.with_columns(pl.col("known_upload_on").cast(pl.Date, strict=True)).drop_nulls()
    if table.select(pl.struct("source_index", "symbol").is_duplicated().any()).item():
        raise ValueError("ambiguous upload-date keys")
    return table, {"path": str(path.resolve()), "sha256": receipt["sha256"],
                   "evidence": "provider_upload_lower_bound_not_exact_original_version"}


def validate_base_unchanged(base: Path, candidate: Path, columns: list[str]) -> int:
    names = ["date", "symbol", *columns, *[n + "__available" for n in columns]]
    a = pq.ParquetFile(base).iter_batches(batch_size=65536, columns=names)
    b = pq.ParquetFile(candidate).iter_batches(batch_size=65536, columns=names)
    rows = 0
    from itertools import zip_longest
    for x, y in zip_longest(a, b):
        if x is None or y is None or not x.equals(y):
            raise ValueError("strict base columns/order/quality masks changed")
        rows += len(x)
    return rows


def notebook(out: Path):
    code = f'''from pathlib import Path
import sys, json
sys.path.insert(0, {str(ROOT)!r})
from scripts.build_tw_release_schedule_dataset import validate_base_unchanged
from scripts.curate_tw_day_trade_training_dataset import validate_matrix
from scripts.prepare_tw_day_trade_feature_catalog import sha256
root = Path({str(out)!r})
m = json.loads((root / 'dataset_manifest.json').read_text())
schema = json.loads((root / 'feature_columns.json').read_text())
assert sha256(root / 'model_inputs.parquet') == m['matrix']['sha256']
assert sha256(root / 'feature_columns.json') == m['feature_schema_sha256']
assert not m['historical_point_in_time'] and m['publication_time_estimated']
stats, annual = validate_matrix(root / 'model_inputs.parquet', schema['value_features'])
assert stats['rows'] == m['matrix']['rows']
validate_base_unchanged(Path(m['base_matrix']), root / 'model_inputs.parquet', schema['strict_base_features'])
print({{k:v for k,v in stats.items() if k != 'features'}})
'''
    atomic_write_json(out / "verify_dataset.ipynb", {"nbformat": 4, "nbformat_minor": 5,
        "metadata": {}, "cells": [
            {"cell_type": "markdown", "id": "scope", "metadata": {}, "source": [
                "# 研究發布時程資料集\n只推估公布時間，沒有補造數值。當期修訂版不等於歷史原始版本；不是實盤 PIT 驗證。\n"]},
            {"cell_type": "code", "id": "verify", "metadata": {}, "execution_count": None,
             "outputs": [], "source": code.splitlines(keepends=True)}]})


def build(*, base: Path, finlab: Path, calendar: Path, catalog: Path, out: Path,
          cross_source_bundle: Path | None = None):
    out = out.resolve()
    if out.exists():
        raise FileExistsError("use a new versioned output directory; never overwrite a prior build")
    implementation = {p: sha256(ROOT / p) for p in [
        "scripts/build_tw_release_schedule_dataset.py", "stockagent/data/tw_public_release_schedule.py",
        "stockagent/data/finlab_research_overlay.py", "stockagent/data/tw_day_trade_feature_admission.py"]}
    manifest = read_json(base / "dataset_manifest.json")
    matrix = base / "model_inputs.parquet"
    if not manifest.get("training_features_ready") or sha256(matrix) != manifest["matrix"]["sha256"]:
        raise ValueError("strict base matrix not verified")
    base_names = read_json(base / "feature_columns.json")["value_features"]
    calendar_sha, catalog_sha = sha256(calendar), sha256(catalog)
    sessions = sorted(set(pl.read_parquet(calendar, columns=["date"])["date"].cast(pl.Date).to_list()))
    keys = pl.read_parquet(matrix, columns=["date", "symbol"])
    if not set(keys["date"]) <= set(sessions):
        raise ValueError("calendar does not contain every strict base decision date")
    symbols = sorted(keys["symbol"].unique())
    out.mkdir(parents=True)
    normalized = out / "observations"
    normalized.mkdir()
    uploads, upload_receipt = load_uploads(finlab)
    overlay, overlay_receipt, publication_bounds = {}, None, None
    if cross_source_bundle is not None:
        bundle_path = cross_source_bundle / "bundle_manifest.json"
        overlay_receipt = {"path": str(bundle_path.resolve()), "sha256": sha256(bundle_path)}
        bundle = read_json(bundle_path)
        if bundle.get("contract") != "tw_cross_source_missing_only_v1":
            raise ValueError("unsupported cross-source bundle ABI")
        overlay = bundle["overrides"]
        boundary = bundle.get("publication_lower_bounds")
        if boundary:
            if sha256(Path(boundary["path"])) != boundary["sha256"]:
                raise ValueError("publication bounds changed")
            publication_bounds = pl.read_parquet(boundary["path"])
    classified = [classify_candidate(r) for r in pl.read_csv(catalog, infer_schema_length=15000).to_dicts()]
    definitions, receipts, clock_rows, decisions, quality_rows = [], [], [], [], []
    for row in classified:
        key = row["dataset_id"]
        if row["provider"] != "FinLab" or row["decision"] != "quarantined_pit" or rule_for(key) is None:
            decisions.append(row)
            continue
        rule, feature = rule_for(key), feature_name(key)
        try:
            found = _verified_source(finlab, key)
            if found is None:
                raise ValueError("no source receipt")
            path, receipt = found
            input_path = path
            extra = overlay.get(key)
            if extra:
                if (extra["primary_sha256"] != receipt["sha256"] or
                    sha256(Path(extra["path"])) != extra["sha256"]):
                    raise RuntimeError("cross-source overlay does not match pinned primary source")
                input_path = Path(extra["path"])
            table, lookup = normalize_source(input_path, key, sessions, symbols, uploads,
                publication_bounds=publication_bounds, quality_rows=quality_rows)
            if extra and (key == "etl:inventory:大於四百張佔比" or
                          (key.startswith("foreign_investors_shareholding:") and "比率" in key)):
                original_masks = []
                original, _ = normalize_source(path, key, sessions, symbols, uploads,
                    publication_bounds=publication_bounds, quality_rows=original_masks)
                table = retain_invalid_barriers(original, table, feature)
                bad_keys = set(table.filter(pl.col(feature).is_null()).select("date", "symbol").iter_rows())
                quality_rows.extend(r for r in original_masks
                    if (date.fromisoformat(r["date"]), r["symbol"]) in bad_keys)
            table = table.filter(pl.col("date") <= keys["date"].max())
            if not table.height or table[feature].n_unique() < 2:
                raise ValueError("empty or constant observed source in selected universe")
            target = normalized / f"{feature}.parquet"
            table.write_parquet(target, compression="zstd")
            receipts.append({"dataset": key, "path": str(path.resolve()), "sha256": receipt["sha256"],
                "receipt": receipt, "cross_source_overlay": extra,
                "normalized_path": str(target), "normalized_sha256": sha256(target)})
            for item in lookup.to_dicts():
                clock_rows.append({"dataset": key, "rule": rule.name, **item})
            definitions.append({"feature": feature, "dataset": key,
                "category": feature_category(key), "category_zh": CATEGORY_LABELS[feature_category(key)],
                "rule": rule.name, "scope": rule.scope,
                "carry_days": rule.carry_days, "unit": "provider_native_unit_no_conversion",
                "older_carry_days": rule.older_carry_days, "cadence_change_on": rule.cadence_change_on,
                "publication_time_estimated": True, "value_vintage": "current_provider_revision",
                "source_observations": table.height, "first_available": str(table["date"].min()),
                "last_available": str(table["date"].max()), "rule_sources": " | ".join(rule.urls)})
            row.update(decision="research_selected_estimated_publication", selected_canonical_feature=feature,
                       category=feature_category(key), category_zh=CATEGORY_LABELS[feature_category(key)],
                       reason=rule.name + "; current revision, not verified historical PIT")
            print(f"[release-schedule] {key}: {table.height:,} observations", flush=True)
        except ValueError as exc:
            row.update(decision="quarantined_research_adapter", reason=str(exc))
            print(f"[release-schedule] QUARANTINE {key}: {exc}", flush=True)
        decisions.append(row)
    names = [d["feature"] for d in definitions]
    if not names or len(names) != len(set(names)) or set(names) & set(base_names):
        raise ValueError("empty or duplicate feature ABI")
    atomic_write_json(out / "publication_rules.json", rule_manifest())
    atomic_write_json(out / "source_receipts.json", {"sources": receipts, "upload_dates": upload_receipt,
        "cross_source_bundle": overlay_receipt,
        "calendar": str(calendar.resolve()), "calendar_sha256": calendar_sha,
        "base_manifest": str((base / "dataset_manifest.json").resolve()),
        "base_manifest_sha256": sha256(base / "dataset_manifest.json"), "catalog_sha256": catalog_sha})
    write_csv(out / "release_schedule.csv", clock_rows)
    atomic_write_json(out / "quality_masks.json", quality_rows)
    write_csv(out / "quality_masks.csv", quality_rows)
    write_csv(out / "feature_classification.csv", decisions)
    write_csv(out / "remaining_timing_worklist.csv", [r for r in decisions
        if r["decision"] in {"quarantined_pit", "quarantined_research_adapter"}])
    write_csv(out / "research_feature_dictionary.csv", definitions)
    all_names = base_names + names
    columns = {"contract": CONTRACT, "value_features": all_names, "strict_base_features": base_names,
        "estimated_publication_features": names, "availability_features": [n + "__available" for n in all_names],
        "model_features": all_names + [n + "__available" for n in all_names]}
    atomic_write_json(out / "feature_columns.json", columns)
    target, temporary = out / "model_inputs.parquet", out / "model_inputs.parquet.tmp"
    writer = None
    try:
        for year in sorted(keys["date"].dt.year().unique()):
            frame = pl.scan_parquet(matrix).filter(pl.col("date").dt.year() == year).collect()
            year_keys = frame.select("date", "symbol")
            start, end = year_keys["date"].min(), year_keys["date"].max()
            added = []
            for definition in definitions:
                feature, rule = definition["feature"], rule_for(definition["dataset"])
                observations = pl.scan_parquet(normalized / f"{feature}.parquet").filter(
                    pl.col("date").is_between(start - timedelta(days=max_carry_days(rule)), end)).collect()
                added.append(align_observations(year_keys, observations, feature, rule))
            frame = frame.with_columns(added).with_columns(
                [pl.col(n).is_not_null().alias(n + "__available") for n in names])
            frame = frame.select("date", "symbol", *all_names, *columns["availability_features"])
            arrow = frame.to_arrow().cast(pa.schema([pa.field("date", pa.date32()), pa.field("symbol", pa.string()),
                *[pa.field(n, pa.float32()) for n in all_names],
                *[pa.field(n + "__available", pa.bool_()) for n in all_names]]))
            if writer is None:
                writer = pq.ParquetWriter(temporary, arrow.schema, compression="zstd")
            writer.write_table(arrow, row_group_size=65536)
            print(f"[release-schedule] joined {year}: {frame.height:,} rows / {len(all_names)} values", flush=True)
        if writer is not None:
            writer.close()
            writer = None
        stats, annual = validate_matrix(temporary, all_names)
        preserved = validate_base_unchanged(matrix, temporary, base_names)
        for r in receipts + ([upload_receipt] if upload_receipt else []):
            if sha256(Path(r["path"])) != r["sha256"]:
                raise ValueError(f"source bytes changed during build: {r['path']}")
        for r in list(overlay.values()) + ([overlay_receipt] if overlay_receipt else []):
            if sha256(Path(r["path"])) != r["sha256"]:
                raise ValueError("cross-source input changed during build")
        if sha256(calendar) != calendar_sha or sha256(catalog) != catalog_sha or sha256(matrix) != manifest["matrix"]["sha256"]:
            raise ValueError("base/calendar/catalog changed during build")
        if implementation != {p: sha256(ROOT / p) for p in implementation}:
            raise ValueError("implementation changed during build; use a fresh version")
        os.replace(temporary, target)
    finally:
        if writer is not None:
            writer.close()
    atomic_write_json(out / "validation.json", {**stats, "strict_base_rows_unchanged": preserved})
    write_csv(out / "annual_coverage.csv", annual)
    notebook(out)
    result = {"contract": CONTRACT, "created_at_utc": datetime.now(UTC).isoformat(),
        "research_training_features_ready": True, "strict_training_eligible": False,
        "historical_point_in_time": False, "publication_time_estimated": True,
        "live_eligible": False, "day_trade_execution_training_ready": False,
        "base_matrix": str(matrix.resolve()), "base_matrix_sha256": manifest["matrix"]["sha256"],
        "extra_conservative_delay_calendar_days": 1,
        "cross_source_bundle": overlay_receipt,
        "matrix": {"path": str(target), "sha256": sha256(target), "rows": stats["rows"]},
        "strict_value_features": len(base_names), "research_value_features": len(names),
        "total_model_channels": 2 * len(all_names),
        "source_field_decisions": dict(Counter(r["decision"] for r in decisions)),
        "feature_schema_sha256": sha256(out / "feature_columns.json"),
        "rules_sha256": sha256(out / "publication_rules.json"),
        "source_receipts_sha256": sha256(out / "source_receipts.json"),
        "validation_sha256": sha256(out / "validation.json"),
        "quality_masks_sha256": sha256(out / "quality_masks.json"),
        "invalid_source_cells_masked": len(quality_rows),
        "implementation": implementation,
        "limitations": ["estimated release clocks may differ from late/special actual filings",
            "current revised provider values, not historical original-vintage data",
            "not a minute-execution/return-label dataset; no performance claim",
            "normalization and predictive selection must be fitted on each training fold only",
            "housing, metadata, unadapted sources and missing numbers remain excluded/NULL"]}
    atomic_write_json(out / "dataset_manifest.json", result)
    return result


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--base-dataset", type=Path, default=ROOT / "artifacts/datasets/tw_day_trade_curated_20260928_v3")
    p.add_argument("--finlab-root", type=Path, default=ROOT / "data_finlab")
    p.add_argument("--calendar", type=Path, default=ROOT / "data_tw_public/twse_taiex_ohlc.parquet")
    p.add_argument("--catalog", type=Path, default=ROOT / "artifacts/data_quality/downloader_integrity_20260928/feature_candidates_corrected.csv")
    p.add_argument("--output-dir", type=Path, required=True)
    p.add_argument("--cross-source-bundle", type=Path)
    p.add_argument("--accept-estimated-publication", action="store_true", required=True)
    args = p.parse_args()
    result = build(base=args.base_dataset, finlab=args.finlab_root, calendar=args.calendar,
                   catalog=args.catalog, out=args.output_dir, cross_source_bundle=args.cross_source_bundle)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
