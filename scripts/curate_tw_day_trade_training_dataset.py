#!/usr/bin/env python3
"""Classify sources and export a clean, receipt-audited 09:00 feature matrix.

The canonical panel owns transformations, publication shifts and masks. This
script does not invent a trainer, labels, executable prices, or source data.
Run prepare, the printed canonical source audit, then finalize. A failed audit
cannot publish a ready dataset manifest.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import UTC, datetime
from dataclasses import asdict, replace
import hashlib
import json
from pathlib import Path
import sys
import subprocess

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json, atomic_write_text
from scripts.build_tw_feature_admission_bundle import csv_rows, export_masked_features
from scripts.stage_tw_public_research_release import _required_formal_members, _stage_copy
from scripts.prepare_tw_day_trade_feature_catalog import read_json, sha256, write_csv
from scripts.audit_tw_public_data_layer import _load_or_build_panel
from stockagent.config import load_config
from stockagent.data.panel_cache import array_content_fingerprint
from stockagent.data.tw_public_features import (
    _build_margin_features, _build_institutional_features, _build_valuation_features,
    _next_exchange_session_lookup, _source_content_receipts,
)
from stockagent.data.tw_day_trade_feature_admission import (
    CATEGORY_LABELS, CONTRACT_VERSION, VALUE_FEATURES, SHIFT_FEATURES,
    classify_candidate, category_for, feature_clock, validate_feature_names,
)


def feature_columns() -> list[str]:
    return list(VALUE_FEATURES) + [name + "__available" for name in VALUE_FEATURES]


def implementation_receipts() -> dict:
    return {"builder_sha256": sha256(Path(__file__)),
            "admission_sha256": sha256(ROOT / "stockagent/data/tw_day_trade_feature_admission.py"),
            "auditor_sha256": sha256(ROOT / "scripts/audit_tw_public_data_layer.py")}


def verify_source_bundle(source: Path) -> dict:
    manifest_path = source / "source_build_receipt.json"
    if not manifest_path.exists():
        manifest_path = source / "dataset_manifest.json"
    receipt = read_json(manifest_path)
    path = source / "features/tw_public_stock_daily.parquet"
    if (not (receipt.get("core_feature_ready") or receipt.get("kind") == "canonical_build_pending_audit")
            or sha256(path) != receipt["core_feature_table_sha256"]):
        raise ValueError("source bundle feature receipt does not match; rebuild the source bundle")
    return {"feature_path": str(path), "feature_sha256": sha256(path),
            "manifest_path": str(manifest_path), "manifest_sha256": sha256(manifest_path)}


def rebuild_sources(out: Path, public: Path, end_date: str) -> dict:
    """Refresh derived inputs through existing producers, never receipt relabeling."""
    if (out / "dataset_manifest.json").exists():
        raise FileExistsError("ready dataset exists; use a new versioned output directory")
    source = out / "source_inputs"
    source.mkdir(parents=True, exist_ok=True)
    actions = read_json(public / "tw_corporate_action_entitlements.summary.json")
    hashes = {}
    for member in _required_formal_members(actions, public)[1:]:
        digest = sha256(public / member)
        _stage_copy(public / member, source / member, digest)
        hashes[member] = digest
    commands = [
        [sys.executable, str(ROOT / "scripts/build_tw_official_symbol_parquets.py"),
         "--input-dir", str(public), "--output-dir", str(source / "stocks"),
         "--workers", "4", "--end-date", end_date],
        [sys.executable, str(ROOT / "scripts/build_tw_public_training_features.py"),
         "--input-dir", str(public), "--symbols-root", str(source / "stocks"),
         "--output-path", str(source / "features/tw_public_stock_daily.parquet"),
         "--end-date", end_date],
    ]
    for command in commands:
        print("[curated-sources] " + " ".join(command), flush=True)
        subprocess.run(command, cwd=ROOT, check=True)
    receipt = {"kind": "canonical_build_pending_audit", "core_feature_ready": False,
               "end_date": end_date, "commands": commands, "action_dependencies": hashes,
               "core_feature_table_sha256": sha256(source / "features/tw_public_stock_daily.parquet"),
               "symbol_build_summary_sha256": sha256(source / "stocks/official_symbol_build_summary.json")}
    atomic_write_json(source / "source_build_receipt.json", receipt)
    return receipt


def conflicting_feature_cells(frame, names: list[str], family: str) -> list[dict]:
    """Compare canonical pre-merge observations, never pick a venue by row order."""
    import polars as pl
    if frame.is_empty() or not names:
        return []
    frame = frame.select("date", "symbol", *names).filter(
        pl.any_horizontal(pl.col(name).is_not_null() for name in names)
    )
    duplicates = frame.filter(frame.select("date", "symbol").is_duplicated())
    if duplicates.is_empty():
        return []
    grouped = duplicates.group_by("date", "symbol").agg(
        pl.col(name).drop_nulls().unique().sort().alias(name) for name in names
    )
    result = []
    for row in grouped.iter_rows(named=True):
        for name in names:
            values = row[name]
            if len(values) > 1:
                result.append({"date": str(row["date"]), "symbol": row["symbol"],
                               "feature": name, "source_family": family,
                               "conflicting_values": json.dumps(values),
                               "reason": "conflicting_official_observations_before_columnwise_merge"})
    return result


def build_quality_masks(public: Path, source_feature: Path) -> list[dict]:
    """Reuse canonical parsers/clocks; quarantine only disagreeing value cells."""
    expected = read_json(source_feature.with_suffix(".summary.json"))["source_receipts"]
    before = _source_content_receipts(public)
    if before != expected:
        raise ValueError("quality-mask sources differ from the pinned canonical feature build")
    results = []
    for family, builder, needs_shift in [
        ("margin", _build_margin_features, False),
        ("institutional", _build_institutional_features, False),
        ("valuation", _build_valuation_features, True),
    ]:
        frame = builder(public)
        names = [name for name in VALUE_FEATURES if name in frame.columns]
        rows = conflicting_feature_cells(frame, names, family)
        if needs_shift and rows:
            # Daily valuation is shifted by the panel, whereas chip builders
            # already emit next-session decision dates. Never shift twice.
            lookup = _next_exchange_session_lookup(public)
            mapping = {str(a): str(b) for a, b in lookup.iter_rows()}
            rows = [{**row, "date": mapping[row["date"]]}
                    for row in rows if row["date"] in mapping]
        results.extend(rows)
        del frame
    if _source_content_receipts(public) != before:
        raise ValueError("source changed while building quality masks")
    return sorted(results, key=lambda row: (row["date"], row["symbol"], row["feature"]))


def apply_quality_masks(panel, rows: list[dict]):
    """Keep cached canonical tensors immutable and mark ambiguity unobserved."""
    dates = {str(d)[:10]: i for i, d in enumerate(panel.dates)}
    symbols = {s: i for i, s in enumerate(panel.symbols)}
    features = {name: i for i, name in enumerate(panel.feature_names)}
    changed = panel.features.copy()
    active, outside = 0, 0
    for row in rows:
        if row["feature"] not in VALUE_FEATURES:
            raise ValueError("quality mask targets a non-selected feature")
        if row["date"] not in dates or row["symbol"] not in symbols:
            outside += 1
            continue
        di, si = dates[row["date"]], symbols[row["symbol"]]
        if not panel.alive_mask[di, si]:
            outside += 1
            continue
        changed[di, si, features[row["feature"]]] = 0
        changed[di, si, features[row["feature"] + "__available"]] = 0
        active += 1
    return replace(panel, features=changed), {"masked_export_cells": active,
                                            "outside_export_scope": outside,
                                            "reason": "conflicting_official_observations"}


def prepare(source: Path, catalog: Path, out: Path, public: Path | None = None) -> dict:
    if (out / "dataset_manifest.json").exists():
        raise FileExistsError("ready dataset exists; use a new versioned output directory")
    pinned = verify_source_bundle(source)
    out.mkdir(parents=True, exist_ok=True)
    rows = csv_rows(catalog)
    ids = [row["catalog_id"] for row in rows]
    if len(set(ids)) != len(ids):
        raise ValueError("duplicate source catalog IDs")
    classified = [classify_candidate(row) for row in rows]
    write_csv(out / "feature_classification.csv", classified)
    write_csv(out / "excluded_features.csv", [r for r in classified if r["decision"] != "canonical_feature_selected"])
    counts = Counter((r["category"], r["decision"]) for r in classified)
    write_csv(out / "category_summary.csv", [
        {"category": c, "category_zh": CATEGORY_LABELS[c], "decision": d, "source_fields": n}
        for (c, d), n in sorted(counts.items())
    ])
    # Feature-audit configuration deliberately has no day-trade fill claims.
    # Do not hand this naive-label config to train.py as an execution backtest.
    config = {
        "base_config": str(ROOT / "configs/markets/tw_public_preopen_pit.yaml"),
        "experiment_name": f"tw-day-trade-curated-features-20260928-v{CONTRACT_VERSION}",
        "runner": {"output_dir": str(out / "unused_feature_audit_runner"), "resume": False,
                   "post_train_infer": False},
        "data": {"parquet_root": str(source / "stocks"),
                 "tw_public_feature_path": pinned["feature_path"],
                 "panel_start_date": "2014-01-01", "panel_cache_root": str(out / "panel"),
                 "panel_load_workers": 4, "feature_include": list(VALUE_FEATURES),
                 "feature_exclude": [], "feature_zero_fill": [],
                 "feature_availability_indicators": ["*"],
                 "feature_shift_next_session": list(SHIFT_FEATURES)},
        "walk_forward": {"expected_first_year": 2014},
    }
    atomic_write_text(out / "feature_audit.yaml",
                      "# FEATURE AUDIT ONLY: not a train.py day-trade execution configuration.\n"
                      + json.dumps(config, ensure_ascii=False, indent=2) + "\n")
    columns = feature_columns()
    validate_feature_names(columns)
    atomic_write_json(out / "feature_columns.json", {"value_features": list(VALUE_FEATURES),
                     "availability_features": columns[len(VALUE_FEATURES):], "model_inputs": columns,
                     "key_columns_not_model_inputs": ["date", "symbol"]})
    quality_masks = build_quality_masks(public or ROOT / "data_tw_public", Path(pinned["feature_path"]))
    atomic_write_json(out / "quality_masks.json", {"cells": quality_masks})
    write_csv(out / "quality_masks.csv", quality_masks)
    prepared = {"contract_version": CONTRACT_VERSION, "source": pinned,
                "catalog_path": str(catalog), "catalog_sha256": sha256(catalog),
                "config_sha256": sha256(out / "feature_audit.yaml"),
                "feature_schema_sha256": sha256(out / "feature_columns.json"),
                "classification_sha256": sha256(out / "feature_classification.csv"),
                "quality_masks_sha256": sha256(out / "quality_masks.json"),
                "implementation": implementation_receipts(),
                "catalog_rows": len(rows),
                "decisions": dict(Counter(r["decision"] for r in classified)),
                "raw_source_mutated": False, "feature_ready": False}
    atomic_write_json(out / "preparation_receipt.json", prepared)
    return prepared


def validate_matrix(path: Path, names: list[str], *, dates=None, symbols=None) -> tuple[dict, list[dict]]:
    """Read-back validation at stock/session grain, including NULL != zero.

    Fixed semantic selection precedes profiling; no test-set performance or
    whole-history correlation/normalization is used to choose columns.
    """
    columns = names + [name + "__available" for name in names]
    parquet = pq.ParquetFile(path)
    expected = ["date", "symbol", *columns]
    if parquet.schema_arrow.names != expected:
        raise ValueError("feature matrix contains extra/missing columns or wrong column order")
    if (parquet.schema_arrow.field("date").type != pa.date32()
            or parquet.schema_arrow.field("symbol").type != pa.string()):
        raise ValueError("primary keys must be date32 and string")
    for name in names:
        if parquet.schema_arrow.field(name).type != pa.float32():
            raise ValueError(f"not float32 feature: {name}")
        if parquet.schema_arrow.field(name + "__available").type != pa.bool_():
            raise ValueError(f"not bool availability: {name}")
    if dates is None or symbols is None:
        import polars as pl
        axes = pl.read_parquet(path, columns=["date", "symbol"])
        if axes["date"].null_count() or axes["symbol"].null_count():
            raise ValueError("null primary key")
        dates = np.array(sorted(axes["date"].unique()), dtype="datetime64[D]")
        symbols = sorted(axes["symbol"].unique())
    dates = np.asarray(dates, dtype="datetime64[D]")
    if not len(dates) or not len(symbols):
        raise ValueError("empty matrix axes")
    day_lookup = {int(d): i for i, d in enumerate(dates.astype(np.int64))}
    symbol_lookup = {s: i for i, s in enumerate(symbols)}
    seen = np.zeros(len(dates) * len(symbols), dtype=bool)
    totals, annual = {}, {}
    row_count = 0
    first, last = None, None
    for batch in parquet.iter_batches(batch_size=65536):
        date_array = np.asarray(batch.column("date"), dtype="datetime64[D]")
        symbols_array = batch.column("symbol").to_pylist()
        if np.isnat(date_array).any() or any(s is None or not s for s in symbols_array):
            raise ValueError("null/empty primary key")
        try:
            di = np.fromiter((day_lookup[int(d)] for d in date_array.astype(np.int64)), dtype=np.int64)
            si = np.fromiter((symbol_lookup[s] for s in symbols_array), dtype=np.int64)
        except KeyError as error:
            raise ValueError("noncanonical session or symbol") from error
        encoded = di * len(symbols) + si
        if len(np.unique(encoded)) != len(encoded) or seen[encoded].any():
            raise ValueError("duplicate date/symbol primary key")
        seen[encoded] = True
        row_count += len(batch)
        first = str(date_array.min()) if first is None else min(first, str(date_array.min()))
        last = str(date_array.max()) if last is None else max(last, str(date_array.max()))
        years = date_array.astype("datetime64[Y]").astype(np.int64) + 1970
        year_masks = {int(y): years == y for y in np.unique(years)}
        for name in names:
            array = batch.column(name)
            flags = batch.column(name + "__available")
            if flags.null_count:
                raise ValueError(f"null availability: {name}")
            values = array.to_numpy(zero_copy_only=False)
            valid = array.is_valid().to_numpy(zero_copy_only=False)
            available = flags.to_numpy(zero_copy_only=False)
            if not np.array_equal(valid, available) or not np.isfinite(values[valid]).all():
                raise ValueError(f"NULL/availability or finite-value contract failed: {name}")
            observed = values[valid]
            t = totals.setdefault(name, {"observed": 0, "missing": 0, "observed_zero": 0,
                                          "min": None, "max": None})
            t["observed"] += int(valid.sum())
            t["missing"] += int((~valid).sum())
            t["observed_zero"] += int((observed == 0).sum())
            if observed.size:
                lo, hi = float(observed.min()), float(observed.max())
                t["min"] = lo if t["min"] is None else min(t["min"], lo)
                t["max"] = hi if t["max"] is None else max(t["max"], hi)
            for year, mask in year_masks.items():
                a = annual.setdefault((year, name), {"year": year, "feature": name,
                                                     "rows": 0, "observed": 0, "observed_zero": 0})
                a["rows"] += int(mask.sum())
                a["observed"] += int((mask & valid).sum())
                a["observed_zero"] += int((mask & valid & (values == 0)).sum())
    if not row_count or row_count != parquet.metadata.num_rows:
        raise ValueError("empty or truncated matrix")
    if not seen.reshape(len(dates), len(symbols)).any(axis=1).all():
        raise ValueError("entire canonical session missing from matrix")
    all_null = [n for n, t in totals.items() if not t["observed"]]
    if all_null:
        raise ValueError(f"all-null selected feature(s): {all_null}")
    annual_rows = []
    for _, a in sorted(annual.items()):
        a["missing"] = a["rows"] - a["observed"]
        a["observed_fraction"] = a["observed"] / a["rows"]
        annual_rows.append(a)
    return {"rows": row_count, "value_channels": len(names), "availability_channels": len(names),
            "first_session": first, "last_session": last,
            "sessions": int(seen.reshape(len(dates), len(symbols)).any(axis=1).sum()),
            "symbols": int(seen.reshape(len(dates), len(symbols)).any(axis=0).sum()),
            "duplicate_keys": 0, "noncanonical_keys": 0, "nonfinite_observed_values": 0,
            "null_mask_mismatches": 0, "all_null_features": [],
            "constant_observed_features": [n for n, t in totals.items() if t["min"] == t["max"]],
            "features": totals}, annual_rows


def feature_dictionary(stats: dict) -> list[dict]:
    result = []
    for index, name in enumerate(VALUE_FEATURES):
        category = category_for("", name)
        unit = "dimensionless_canonical_transform"
        if name.endswith("_pct_raw"):
            unit = "percentage_points"
        elif name in {"twpub_pe_raw", "twpub_pb_raw"}:
            unit = "ratio"
        elif name == "twpub_twse_taiex_raw":
            unit = "index_points_market_context"
        elif name.endswith("_lots_raw"):
            unit = "board_lots_1000_shares"
        elif name == "twpub_official_trading_volume_raw":
            unit = "shares"
        elif name == "twpub_official_trading_value_raw":
            unit = "TWD"
        elif name == "twpub_official_trades_raw":
            unit = "trade_count"
        elif name.endswith("_log"):
            unit = "canonical_log_transformed_source_unit"
        result.append({"input_index": index, "feature": name, "category": category,
                       "category_zh": CATEGORY_LABELS[category], "unit": unit,
                       "clock": feature_clock(name), "availability_column": name + "__available",
                       "source_builder": "stockagent.data.panel + stockagent.data.tw_public_features",
                       **stats["features"][name]})
    return result


def write_notebook(out: Path) -> None:
    code = (
        "from pathlib import Path\nimport json\nimport sys\n"
        f"sys.path.insert(0, {str(ROOT)!r})\n"
        "from scripts.curate_tw_day_trade_training_dataset import validate_matrix\n"
        "from scripts.prepare_tw_day_trade_feature_catalog import sha256\n"
        f"root = Path({str(out)!r})\n"
        "manifest = json.loads((root / 'dataset_manifest.json').read_text())\n"
        "columns = json.loads((root / 'feature_columns.json').read_text())\n"
        "assert sha256(root / 'feature_columns.json') == manifest['feature_schema_sha256']\n"
        "assert sha256(root / 'model_inputs.parquet') == manifest['matrix']['sha256']\n"
        "assert sha256(root / 'quality_masks.json') == manifest['quality_masks_sha256']\n"
        "quality, annual = validate_matrix(root / 'model_inputs.parquet', columns['value_features'])\n"
        "assert quality['rows'] == manifest['matrix']['rows']\n"
        "assert not quality['all_null_features']\n"
        "print({k: v for k, v in quality.items() if k != 'features'})\n"
    )
    notebook = {"nbformat": 4, "nbformat_minor": 5, "metadata": {}, "cells": [
        {"cell_type": "markdown", "id": "scope", "metadata": {}, "source": [
            "# 當沖模型輸入驗證\n在專案 fintech 環境執行；這是特徵資料，不含可成交價格或報酬標籤。\n",
            "缺值保留 NULL，availability 區分未知與真實零；未以完整歷史估計標準化或篩選預測效力。"]},
        {"cell_type": "code", "id": "readback", "metadata": {}, "execution_count": None,
         "outputs": [], "source": code.splitlines(keepends=True)},
    ]}
    atomic_write_json(out / "verify_dataset.ipynb", notebook)


def finalize(out: Path) -> dict:
    if (out / "dataset_manifest.json").exists():
        raise FileExistsError("ready dataset exists; validate it or build a new version")
    prepared = read_json(out / "preparation_receipt.json")
    if prepared["contract_version"] != CONTRACT_VERSION:
        raise ValueError("preparation contract changed; use a new dataset version")
    if prepared.get("implementation") != implementation_receipts():
        raise ValueError("preparation implementation changed; rerun prepare and verify matching source audit")
    for path, digest in [(out / "feature_audit.yaml", prepared["config_sha256"]),
                         (out / "feature_columns.json", prepared["feature_schema_sha256"]),
                         (out / "feature_classification.csv", prepared["classification_sha256"]),
                         (out / "quality_masks.json", prepared["quality_masks_sha256"]),
                         (Path(prepared["catalog_path"]), prepared["catalog_sha256"]),
                         (Path(prepared["source"]["manifest_path"]), prepared["source"]["manifest_sha256"]),
                         (Path(prepared["source"]["feature_path"]), prepared["source"]["feature_sha256"])]:
        if sha256(path) != digest:
            raise ValueError(f"pinned input changed: {path}")
    audit = read_json(out / "audit/summary.json")
    if not audit.get("model_safe") or Path(audit["config"]).resolve() != out / "feature_audit.yaml":
        raise ValueError("matching canonical source/feature audit must pass first")
    config = load_config(out / "feature_audit.yaml")
    resolved_sha = hashlib.sha256(json.dumps(asdict(config), sort_keys=True, default=str).encode("utf-8")).hexdigest()
    if audit.get("resolved_config_sha256") != resolved_sha:
        raise ValueError("audit configuration changed, including inherited settings; rerun source audit")
    panel, _ = _load_or_build_panel(config, Path(config.data.parquet_root),
                                    Path(config.data.tw_public_feature_path), build_if_missing=True,
                                    panel_cache_root=out / "panel")
    validate_feature_names(list(panel.feature_names))
    if array_content_fingerprint(panel.features) != audit.get("panel_feature_fingerprint"):
        raise ValueError("model input bytes differ from audited panel; rerun source audit")
    panel, quality_mask_summary = apply_quality_masks(panel, read_json(out / "quality_masks.json")["cells"])
    exported = export_masked_features(panel, out / "model_inputs.parquet", list(VALUE_FEATURES))
    quality, annual = validate_matrix(out / "model_inputs.parquet", list(VALUE_FEATURES),
                                     dates=panel.dates, symbols=panel.symbols)
    dictionary = feature_dictionary(quality)
    write_csv(out / "feature_dictionary.csv", dictionary)
    write_csv(out / "annual_coverage.csv", annual)
    atomic_write_json(out / "validation.json", quality)
    by_category = Counter(row["category_zh"] for row in dictionary)
    result = {
        "contract_version": CONTRACT_VERSION, "created_at_utc": datetime.now(UTC).isoformat(),
        "training_features_ready": True, "day_trade_execution_training_ready": False,
        "scope": "09:00 Asia/Taipei causal feature dataset; no labels, no execution price claim, no training started",
        "matrix": {**{k: v for k, v in exported.items() if k not in {"missing_cells", "observed_zero_cells"}},
                   "sessions": quality["sessions"], "symbols": quality["symbols"],
                   "panel_universe_symbols": len(panel.symbols)},
        "feature_categories": dict(by_category), "source_field_decisions": prepared["decisions"],
        "key_columns_not_model_inputs": ["date", "symbol"],
        "feature_schema_sha256": sha256(out / "feature_columns.json"),
        "source": prepared["source"], "catalog_sha256": prepared["catalog_sha256"],
        "config_sha256": prepared["config_sha256"], "audit_sha256": sha256(out / "audit/summary.json"),
        "validation_sha256": sha256(out / "validation.json"),
        "quality_masks_sha256": prepared["quality_masks_sha256"],
        "quality_mask_summary": quality_mask_summary,
        "pre_mask_audited_panel_fingerprint": audit["panel_feature_fingerprint"],
        "curated_panel_fingerprint": array_content_fingerprint(panel.features),
        "implementation": prepared["implementation"],
        "forbidden_in_model": ["housing", "identifiers", "sha256", "paths", "receipt_metadata", "future_labels"],
        "excluded_derived_families": ["TDCC invalid aggregate and unverified snapshots",
                                       "FinLab original vintage/publication not verified",
                                       "XBRL theoretical publication and revised macro research tables"],
        "normalization": "not fitted; fit only within each annual training fold",
        "split_policy": "canonical expanding annual walk-forward; never random stock-day split",
        "null_policy": "NULL until canonical masked tensor loading; zero never means source observed",
        "do_not": ["train with feature_audit.yaml's inherited naive close labels",
                   "shift this decision-dated matrix another session",
                   "describe audited inputs as live trade or minute execution readiness"],
    }
    write_notebook(out)
    # Commit readiness only after all required data, checks and companion
    # artifacts exist; an interrupted build leaves no ready marker.
    atomic_write_json(out / "dataset_manifest.json", result)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=["rebuild-sources", "prepare", "finalize", "validate"])
    parser.add_argument("--source-bundle", type=Path,
                        default=ROOT / "artifacts/datasets/tw_day_trade_features_20260928_v1")
    parser.add_argument("--catalog", type=Path,
                        default=ROOT / "artifacts/data_quality/downloader_integrity_20260928/feature_candidates_corrected.csv")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--public-dir", type=Path, default=ROOT / "data_tw_public")
    parser.add_argument("--end-date", default="2026-09-24")
    args = parser.parse_args()
    out = args.output_dir.resolve()
    if args.phase == "rebuild-sources":
        result = rebuild_sources(out, args.public_dir.resolve(), args.end_date)
    elif args.phase == "prepare":
        result = prepare(args.source_bundle.resolve(), args.catalog.resolve(), out, args.public_dir.resolve())
    elif args.phase == "finalize":
        result = finalize(out)
    else:
        manifest = read_json(out / "dataset_manifest.json")
        if (not manifest.get("training_features_ready")
                or sha256(out / "model_inputs.parquet") != manifest["matrix"]["sha256"]
                or sha256(out / "feature_columns.json") != manifest["feature_schema_sha256"]
                or sha256(out / "quality_masks.json") != manifest["quality_masks_sha256"]):
            raise ValueError("ready manifest/data/schema digest mismatch")
        validate_feature_names(read_json(out / "feature_columns.json")["model_inputs"])
        result, _ = validate_matrix(out / "model_inputs.parquet", list(VALUE_FEATURES))
        result = {k: v for k, v in result.items() if k != "features"}
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
