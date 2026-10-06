#!/usr/bin/env python3
"""Build a receipt-pinned missing-only overlay from canonical FinMind downloads.

This does not fetch again, mutate raw providers, train, or publish. The resulting
wide tables are inputs to build_tw_release_schedule_dataset, not model channels.
"""
from __future__ import annotations

import argparse
from dataclasses import asdict
from datetime import UTC, datetime
import json
from pathlib import Path
import sys

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json
from scripts.prepare_tw_day_trade_feature_catalog import sha256, write_csv
from stockagent.data.finlab_research_overlay import _verified_source
from stockagent.data.tw_public_release_schedule import feature_name
from stockagent.data.tw_public_cross_source_fill import (
    CONTRACT, MAPPINGS, Mapping, missing_only, normalize_finmind, period_key, read_finmind,
)


def revenue_candidates(frame):
    """Exact adjacent period arithmetic, never a positional shift across gaps."""
    raw, audit = normalize_finmind(frame, MAPPINGS["monthly_revenue:當月營收"])
    raw = raw.with_columns(pl.col("period").str.to_date("%Y-%m").alias("_month"))
    prev = raw.select("symbol", pl.col("_month").dt.offset_by("1mo"), pl.col("value").alias("_previous"))
    lastyear = raw.select("symbol", pl.col("_month").dt.offset_by("1y"), pl.col("value").alias("_year_ago"))
    table = raw.join(prev, on=["symbol", "_month"], how="left", validate="1:1").join(
        lastyear, on=["symbol", "_month"], how="left", validate="1:1")
    table = table.sort("symbol", "_month").with_columns(pl.col("_month").dt.year().alias("_year"))
    table = table.with_columns(pl.col("value").cum_sum().over("symbol", "_year").alias("_cumulative"),
        pl.col("value").cum_count().over("symbol", "_year").alias("_count"))
    table = table.with_columns(pl.when(pl.col("_count") == pl.col("_month").dt.month())
        .then(pl.col("_cumulative")).otherwise(None).alias("_cumulative"))
    prev_cum = table.select("symbol", pl.col("_month").dt.offset_by("1y"),
                            pl.col("_cumulative").alias("_previous_cumulative"))
    table = table.join(prev_cum, on=["symbol", "_month"], how="left", validate="1:1")
    pct = lambda a, b: pl.when(pl.col(b) != 0).then((pl.col(a) / pl.col(b) - 1) * 100).otherwise(None)
    expressions = {"當月營收": pl.col("value"), "上月營收": pl.col("_previous"),
        "去年當月營收": pl.col("_year_ago"), "當月累計營收": pl.col("_cumulative"),
        "去年累計營收": pl.col("_previous_cumulative"),
        "上月比較增減(%)": pct("value", "_previous"), "去年同月增減(%)": pct("value", "_year_ago"),
        "前期比較增減(%)": pct("_cumulative", "_previous_cumulative")}
    return {"monthly_revenue:" + k: table.select("period", "symbol", v.alias("value"))
        .filter(pl.col("value").is_finite()) for k, v in expressions.items()}, audit


def build(*, finmind, finlab, previous, out, source_manifest=None, dataset_prefixes=()):
    if out.exists():
        raise FileExistsError("use a new versioned output directory")
    out = out.resolve()
    code = {str(p): sha256(ROOT / p) for p in [
        "stockagent/data/tw_public_cross_source_fill.py", "scripts/build_tw_cross_source_fill_bundle.py"]}
    if source_manifest is not None:
        from scripts.prepare_tw_day_trade_mixed_frequency import verify_sources
        fixed = verify_sources(source_manifest.parent)
        definitions = [s for s in fixed['feature_specs'] if s.get('source') == 'FinLab']
        symbols = sorted(p.name.removesuffix('_features.parquet') for p in (source_manifest.parent / 'stocks').glob('*_features.parquet'))
    else:
        definitions = pl.read_csv(previous / "research_feature_dictionary.csv").to_dicts()
        symbols = pl.read_parquet(previous / "model_inputs.parquet", columns=["symbol"])["symbol"].unique().to_list()
    if dataset_prefixes:
        definitions = [d for d in definitions if d['dataset'].startswith(tuple(dataset_prefixes))]
    if not definitions or not symbols:
        raise ValueError('empty explicitly selected source/universe scope')
    out.mkdir(parents=True)
    (out / "wide").mkdir()
    (out / "fills").mkdir()
    (out / "conflicts").mkdir()
    sources, frames, results, overrides, bounds = {}, {}, [], {}, []
    requested = sorted({m.dataset for k, m in MAPPINGS.items() if k in {d['dataset'] for d in definitions}})
    for dataset in requested:
        print(f"[crossfill] verify current receipts: {dataset}", flush=True)
        frame, receipt = read_finmind(finmind, dataset)
        frame = frame.filter(pl.col("stock_id").is_in(symbols))
        frames[dataset] = frame
        sources[dataset] = receipt
    revenues, revenue_audit = ({}, {})
    if 'TaiwanStockMonthRevenue' in frames:
        revenues, revenue_audit = revenue_candidates(frames["TaiwanStockMonthRevenue"])
    # create_time is provider ingestion (NOT company publication), available
    # only since 2026-04-21. That day's bootstrap timestamps are not historical
    # release evidence. Use later genuine observations only as a lower bound;
    # old missing timestamps retain the explicitly approved schedule proxy.
    rev_dates = None
    if 'TaiwanStockMonthRevenue' in frames:
        rev_dates = frames["TaiwanStockMonthRevenue"].filter(pl.col("country") == "Taiwan").select(
            pl.date(pl.col("revenue_year"), pl.col("revenue_month"), 1).dt.strftime("%Y-%m").alias("period"),
            pl.col("stock_id").alias("symbol"),
            pl.col("create_time").str.slice(0, 10).str.to_date(strict=False).alias("known_published_on")
        ).with_columns(pl.when(pl.col("known_published_on") > pl.date(2026, 4, 21))
            .then(pl.col("known_published_on")).otherwise(None).alias("known_published_on"))
        rev_dates = rev_dates.group_by("period", "symbol").agg(pl.col("known_published_on").max())
    for definition in definitions:
        key = definition["dataset"]
        spec = MAPPINGS.get(key)
        if key in revenues:
            spec = Mapping("TaiwanStockMonthRevenue", key.split(":", 1)[1], "revenue", .001, "exact_month_arithmetic")
            candidate, source_audit = revenues[key], revenue_audit
        elif spec:
            candidate, source_audit = normalize_finmind(frames[spec.dataset], spec)
        else:
            results.append({"dataset": key, "state": "unmapped_requires_semantic_adapter",
                "filled_keys": 0, "online_exhausted": False})
            continue
        found = _verified_source(finlab, key)
        if found is None:
            raise ValueError(f"primary source not verified: {key}")
        path, receipt = found
        schema = pl.scan_parquet(path).collect_schema()
        cols = [c for c in schema if c in symbols]
        wide = pl.read_parquet(path, columns=["source_index", *cols])
        primary = wide.unpivot(index="source_index", on=cols, variable_name="symbol", value_name="value")
        primary = period_key(primary, spec.kind)
        primary = primary.with_columns(pl.col("value").cast(pl.Float64))
        invalid = primary.filter(pl.col("value").is_not_null() & ~pl.col("value").is_finite()).height
        if key == "etl:inventory:大於四百張佔比" or (key.startswith("foreign_") and "比率" in key):
            invalid += primary.filter(pl.col("value").is_finite() & ~pl.col("value").is_between(0, 100)).height
            primary = primary.with_columns(pl.when(pl.col("value").is_between(0, 100))
                .then(pl.col("value")).otherwise(None).alias("value"))
        primary = primary.with_columns(pl.when(pl.col("value").is_finite()).then(pl.col("value")).otherwise(None))
        # Missing columns for historically absent symbols are genuine candidates;
        # final stock-day key universe/lifecycle is still owned by the strict base.
        candidate = candidate.filter(pl.col("symbol").is_in(symbols))
        fills, audit, conflicts = missing_only(primary.select("period", "symbol", "value"), candidate)
        audit.update(dataset=key, state=audit.pop("reason"), mapping=asdict(spec),
            invalid_primary_observations=invalid, online_exhausted=False, **source_audit)
        results.append(audit)
        print(f"[crossfill] {key}: overlap={audit['overlap']:,} agreement={audit['agreement']:.6f} fills={fills.height:,}", flush=True)
        name = feature_name(key)
        if conflicts.height:
            conflicts.write_parquet(out / "conflicts" / f"{name}.parquet", compression="zstd")
        if not fills.height:
            continue
        index_map = primary.select("period", "source_index").unique()
        if index_map["period"].n_unique() != index_map.height:
            raise ValueError(f"ambiguous provider period index: {key}")
        fills = fills.join(index_map, on="period", how="left", validate="m:1")
        if spec.kind == "quarter":
            generated = pl.col("period").str.replace("Q", "-Q")
        elif spec.kind == "revenue":
            # New months without the primary mapped clock are not guessed here.
            fills = fills.filter(pl.col("source_index").is_not_null())
            generated = pl.col("source_index")
        else:
            generated = pl.col("period") + pl.lit(" 00:00:00")
        fills = fills.with_columns(pl.coalesce("source_index", generated).alias("source_index"))
        if spec.kind == "revenue":
            bounded = fills.join(rev_dates, on=["period", "symbol"], how="left", validate="m:1")
            bounds.append(bounded.filter(pl.col("known_published_on").is_not_null()).select(
                pl.lit(key).alias("dataset"), "source_index", "symbol", "known_published_on"))
            fills = bounded.drop("known_published_on")
        audit["filled_keys"] = fills.height
        if not fills.height:
            continue
        fill_path = out / "fills" / f"{name}.parquet"
        fills.with_columns(pl.lit(spec.dataset).alias("source_dataset")).write_parquet(fill_path, compression="zstd")
        # Primary finite cells retain priority. Actual invalid cells were already
        # masked explicitly and are separately counted, not silently overwritten.
        merged = pl.concat([primary.filter(pl.col("value").is_finite()).select("source_index", "symbol", "value"),
                            fills.select("source_index", "symbol", "value")])
        if merged.select(pl.struct("source_index", "symbol").is_duplicated().any()).item():
            raise ValueError("merge changed an observed primary key")
        merged = merged.pivot(index="source_index", on="symbol", values="value").sort("source_index")
        # Keep original row dates, even all-NULL rows, to preserve schema/calendar.
        merged = wide.select("source_index").join(merged, on="source_index", how="full", coalesce=True).sort("source_index")
        target = out / "wide" / f"{name}.parquet"
        merged.write_parquet(target, compression="zstd")
        overrides[key] = {"path": str(target), "sha256": sha256(target),
            "primary_path": str(path.resolve()), "primary_sha256": receipt["sha256"],
            "fills_path": str(fill_path), "fills_sha256": sha256(fill_path), "filled_observations": fills.height}
    if bounds:
        target = out / "publication_lower_bounds.parquet"
        pl.concat(bounds).write_parquet(target, compression="zstd")
        bounds_receipt = {"path": str(target), "sha256": sha256(target)}
    else:
        bounds_receipt = None
    atomic_write_json(out / "mapping_audit.json", results)
    write_csv(out / "mapping_audit.csv", [{k: (json.dumps(v, ensure_ascii=False) if isinstance(v, dict) else v)
        for k, v in r.items()} for r in results])
    atomic_write_json(out / "source_receipts.json", sources)
    result = {"contract": CONTRACT, "created_at_utc": datetime.now(UTC).isoformat(),
        "overrides": overrides, "publication_lower_bounds": bounds_receipt,
        "filled_observations": sum(r["filled_keys"] for r in results),
        "fields_with_fills": len(overrides), "all_missing_resolved": False, "online_exhausted": False,
        "source_receipts_sha256": sha256(out / "source_receipts.json"),
        "mapping_audit_sha256": sha256(out / "mapping_audit.json"),
        "implementation": code, "historical_point_in_time": False,
        "limitations": ["No source claims complete per-security observations",
            "Missing/inapplicable/conflicting cells remain NULL, not invented zero",
            "ToAlpha bulk training needs separate written authorization; not used",
            "Unmapped feature families and original financial vintages remain unresolved"]}
    result['selected_dataset_prefixes'] = list(dataset_prefixes)
    if source_manifest is not None:
        result['source_manifest_sha256'] = sha256(source_manifest)
    if code != {p: sha256(ROOT / p) for p in code}:
        raise ValueError("implementation changed during build")
    for item in overrides.values():
        if sha256(Path(item["primary_path"])) != item["primary_sha256"]:
            raise ValueError("primary changed during build")
    atomic_write_json(out / "bundle_manifest.json", result)
    return result


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--finmind-root", type=Path, default=ROOT / "data_finmind/sponsor")
    p.add_argument("--finlab-root", type=Path, default=ROOT / "data_finlab")
    p.add_argument("--previous-dataset", type=Path, default=ROOT / "artifacts/datasets/tw_day_trade_release_schedule_research_20260928_v3")
    p.add_argument("--output-dir", type=Path, required=True)
    p.add_argument('--source-manifest', type=Path, help='Fixed native source projection instead of a legacy model matrix')
    p.add_argument('--dataset-prefix', action='append', default=[], help='Limit actual source reads to selected feature families')
    a = p.parse_args()
    m = build(finmind=a.finmind_root, finlab=a.finlab_root, previous=a.previous_dataset, out=a.output_dir,
              source_manifest=a.source_manifest, dataset_prefixes=a.dataset_prefix)
    print(json.dumps({k:v for k,v in m.items() if k != "overrides"}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
