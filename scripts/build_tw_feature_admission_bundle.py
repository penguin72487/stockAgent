#!/usr/bin/env python3
"""Prepare local feature-only inputs and a dated admission report.

This is not a new trainer, execution model, publisher, or live deployment.
The canonical panel owns shifts, missingness, calendars and corporate actions.
"""
from __future__ import annotations

import argparse
from collections import Counter
import csv
from datetime import datetime, UTC
import json
from pathlib import Path
import sys

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json, atomic_write_text
from scripts.prepare_tw_day_trade_feature_catalog import read_json, sha256, write_csv
from scripts.stage_tw_public_research_release import _required_formal_members, _stage_copy
from scripts.audit_tw_public_data_layer import _load_or_build_panel
from stockagent.config import load_config
from stockagent.data.panel_cache import load_panel_cache_v2


def csv_rows(path: Path) -> list[dict]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def stage_inputs(out: Path, public: Path) -> dict:
    receipt = read_json(public / "tw_corporate_action_entitlements.summary.json")
    hashes = {}
    for relative in _required_formal_members(receipt, public)[1:]:
        source = public / relative
        digest = sha256(source)
        target = out / relative
        if not target.is_file() or sha256(target) != digest:
            _stage_copy(source, target, digest)
        hashes[relative] = digest
    core = {
        "base_config": str(ROOT / "configs/markets/tw_public_preopen_raw_2014_v1.yaml"),
        "experiment_name": "tw-preopen-feature-admission-20260928-v1",
        "runner": {"output_dir": str(out / "unused_feature_audit_runner"), "resume": False, "post_train_infer": False},
        "data": {"parquet_root": str(out / "stocks"),
                 "tw_public_feature_path": str(out / "features/tw_public_stock_daily.parquet"),
                 "panel_cache_root": str(out / "core_panel"), "panel_load_workers": 4},
    }
    warning = "# Feature preparation/audit only. Inherited close-proxy labels are NOT day-trade fills.\n"
    atomic_write_text(out / "core_feature_audit.yaml", warning + json.dumps(core, ensure_ascii=False, indent=2) + "\n")
    masked = {"base_config": str(out / "core_feature_audit.yaml"),
              "data": {"feature_availability_indicators": ["*"], "panel_cache_root": str(out / "core_masked_panel")}}
    atomic_write_text(out / "core_masked_feature_audit.yaml", warning + json.dumps(masked, indent=2) + "\n")
    research = {
        "base_config": str(ROOT / "configs/markets/tw_public_preopen_finlab_research_2014_v4.yaml"),
        "experiment_name": "tw-preopen-local-research-features-20260928-v1",
        "runner": {"output_dir": str(out / "unused_research_audit_runner"), "resume": False, "post_train_infer": False},
        "data": {"parquet_root": str(out / "stocks"), "panel_load_workers": 4,
                 "tw_public_feature_path": str(out / "research_features.parquet"),
                 "panel_cache_root": str(out / "research_panel")},
    }
    atomic_write_text(out / "research_feature_audit.yaml", warning + json.dumps(research, ensure_ascii=False, indent=2) + "\n")
    atomic_write_json(out / "action_dependency_receipt.json", hashes)
    return hashes


def export_masked_features(panel, target: Path, core_names: list[str]) -> dict:
    """Export model inputs only; never export future prices/labels as features."""
    indices = {name: i for i, name in enumerate(panel.feature_names)}
    if len(indices) != len(panel.feature_names):
        raise ValueError("duplicate panel feature names")
    expected = set(core_names) | {name + "__available" for name in core_names}
    if set(indices) != expected:
        raise ValueError("masked panel ABI mismatch")
    schema = pa.schema([pa.field("date", pa.date32()), pa.field("symbol", pa.string()),
                        *[pa.field(name, pa.float32()) for name in core_names],
                        *[pa.field(name + "__available", pa.bool_()) for name in core_names]])
    temp = target.with_suffix(".parquet.tmp")
    rows = 0
    missing = Counter()
    observed_zero = Counter()
    symbols = np.asarray(panel.symbols)
    try:
        with pq.ParquetWriter(temp, schema, compression="zstd") as writer:
            for begin in range(0, len(panel.dates), 32):
                stop = min(begin + 32, len(panel.dates))
                # Historical alive mask is row scope, not a feature or a claim
                # that an opening order could execute at this timestamp.
                active = np.asarray(panel.alive_mask[begin:stop], dtype=bool)
                di, si = np.nonzero(active)
                arrays = [pa.array(np.asarray(panel.dates[begin:stop])[di].astype("datetime64[D]")),
                          pa.array(symbols[si])]
                indicators = []
                for name in core_names:
                    values = np.asarray(panel.features[begin:stop, :, indices[name]])[di, si]
                    available = np.asarray(panel.features[begin:stop, :, indices[name + "__available"]])[di, si]
                    if not np.isfinite(values).all() or not np.isin(available, [0, 1]).all():
                        raise ValueError(f"invalid masked panel input: {name}")
                    mask = available.astype(bool)
                    missing[name] += int((~mask).sum())
                    observed_zero[name] += int((mask & (values == 0)).sum())
                    arrays.append(pa.array(values, type=pa.float32(), mask=~mask))
                    indicators.append(pa.array(mask))
                writer.write_table(pa.Table.from_arrays([*arrays, *indicators], schema=schema))
                rows += len(di)
        temp.replace(target)
    finally:
        temp.unlink(missing_ok=True)
    return {"path": str(target), "sha256": sha256(target), "rows": rows,
            "missing_cells": dict(missing), "observed_zero_cells": dict(observed_zero),
            "value_channels": len(core_names), "availability_channels": len(core_names),
            "first_session": str(panel.dates[0])[:10], "last_session": str(panel.dates[-1])[:10],
            "sessions": len(panel.dates), "symbols": len(panel.symbols)}


def finalize(out: Path, audit: Path) -> dict:
    accepted = read_json(audit / "accepted_core_audit/summary.json")
    if not accepted.get("model_safe"):
        raise RuntimeError("core source/feature audit failed; cannot declare training feature readiness")
    config = load_config(out / "core_masked_feature_audit.yaml")
    core_names = list(config.data.feature_include)
    panel, _ = _load_or_build_panel(config, Path(config.data.parquet_root), Path(config.data.tw_public_feature_path),
                                    build_if_missing=True, panel_cache_root=out / "core_masked_panel")
    audited = load_panel_cache_v2(out / "core_panel")
    if list(audited["feature_names"]) != core_names or not np.array_equal(audited["dates"], panel.dates):
        raise ValueError("masked/accepted core axes differ")
    if list(audited["symbols"]) != list(panel.symbols):
        raise ValueError("masked/accepted symbol axes differ")
    for begin in range(0, len(panel.dates), 32):
        if not np.array_equal(panel.features[begin:begin + 32, :, :len(core_names)], audited["features"][begin:begin + 32]):
            raise ValueError("adding missingness indicators changed an accepted core value")
    export = export_masked_features(panel, out / "preopen_core_features.parquet", core_names)
    research_receipt = read_json(out / "research_features.finlab_research.json")
    if sha256(out / "research_features.parquet") != research_receipt["output_sha256"]:
        raise ValueError("research receipt SHA-256 mismatch")
    # Preserve the small receipt chain and prove the research formal base is
    # exactly the rebuilt core, even when only old build receipts were stale.
    base_path = ROOT / "artifacts/research_features/tw_public_research_all_2014_v3.parquet"
    base_receipt_path = base_path.with_suffix(".all_features.json")
    base_receipt = read_json(base_receipt_path)
    if sha256(base_path) != research_receipt["inputs"]["base_sha256"] or base_receipt["output_sha256"] != research_receipt["inputs"]["base_sha256"]:
        raise ValueError("research base changed or its receipt is invalid")
    if base_receipt["inputs"]["official"]["sha256"] != sha256(out / "features/tw_public_stock_daily.parquet"):
        raise ValueError("research table does not use the accepted official feature bytes")
    _stage_copy(base_receipt_path, out / "lineage/research_base.all_features.json", sha256(base_receipt_path))
    profile_summary = read_json(audit / "research_feature_profile/summary.json")
    stat = (out / "research_features.parquet").stat()
    if profile_summary["file_signature"] != {"inode": stat.st_ino, "size": stat.st_size, "mtime_ns": stat.st_mtime_ns}:
        raise ValueError("research profile is stale")
    if profile_summary["findings"]:
        raise ValueError("research table structural or numerical findings remain")
    research = csv_rows(audit / "research_feature_profile/feature_inventory.csv")
    core_profiles = {r["feature"]: r for r in csv_rows(audit / "accepted_core_audit/feature_profiles.csv")}
    rows = []
    for name in core_names:
        p = core_profiles[name]
        rows.append({"layer": "core", "feature": name, "admission": "accepted_preopen_feature",
                     "first": export["first_session"], "last": export["last_session"],
                     "count": export["rows"] - export["missing_cells"][name],
                     "missing_cells": export["missing_cells"][name],
                     "observed_zero_cells": export["observed_zero_cells"][name],
                     "scope": p["scope"], "reason": "source_receipts_and_0900_clock_audited; not_execution_certification",
                     "source": "TWSE/TPEx/CBC/DGBAS official", "path": export["path"]})
    mappings = research_receipt.get("futures_field_mapping", {}) | research_receipt.get("etf_aggregate_field_mapping", {})
    for p in research:
        name = p["feature"]
        source = next((key for key, value in research_receipt["inputs"]["sources"].items()
                       if name in value["feature_columns"]), "TW public/TAIFEX/MOPS research union")
        late = p["first"] >= "2026-01-01"
        rows.append({"layer": "research", **p, "admission": "research_only_late_history" if late else "research_only",
                     "reason": "current_revision_or_proxy_or_unverified_clock; never_strict_live",
                     "source": source, "dimension": mappings.get(name, ""),
                     "constant_when_observed": p["min"] == p["max"],
                     "path": str(out / "research_features.parquet")})
    write_csv(audit / "model_feature_admission.csv", rows)
    annual = csv_rows(audit / "research_feature_profile/annual_feature_inventory.csv")
    missing_years = [r for r in annual if int(r["year"]) >= 2014 and int(r["raw_non_null"]) == 0]
    write_csv(audit / "research_missing_feature_years.csv", missing_years)
    catalogue = read_json(audit / "catalog_summary.json")
    result = {
        "schema_version": 1, "prepared_at_utc": datetime.now(UTC).isoformat(),
        "scope": "local feature preparation; no broker, no model training, no remote delivery",
        "core_feature_ready": True, "day_trade_execution_training_ready": False,
        "execution_reason": "feature-clock validation uses existing preopen audit contract; no claim about same-close proxy labels or minute fills",
        "core": export, "core_values_equal_audited_panel": True,
        "core_audit_sha256": sha256(audit / "accepted_core_audit/summary.json"),
        "core_config_sha256": sha256(out / "core_masked_feature_audit.yaml"),
        "core_feature_table_sha256": sha256(out / "features/tw_public_stock_daily.parquet"),
        "research": {"path": str(out / "research_features.parquet"), "sha256": research_receipt["output_sha256"],
                     "rows": research_receipt["rows"], "external_value_channels": len(research),
                     "base_stock_channels": 20, "availability_channels": len(research),
                     "configured_total_channels": 20 + 2 * len(research),
                     "strict_training_eligible": False, "publishable": False,
                     "features_starting_2026": sum(r["first"] >= "2026-01-01" for r in research)},
        "registered_sources": catalogue["registered_sources"],
        "source_failure_count": catalogue["source_failure_count"],
        "admission_counts": dict(Counter(r["admission"] for r in rows)),
        "limitations": ["field_presence_is_not_all_symbol_day_completeness", "research_versions_not_historical_vintages",
                        "new_raw_sources_without_adapters_are_catalogued_not_auto_joined", "fold_transform_fitting_must_use_training_years_only",
                        "live_sources_may_advance_after_receipt; revalidate_before_new_builds"],
    }
    atomic_write_json(out / "dataset_manifest.json", result)
    return result


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--output-dir", type=Path, required=True)
    p.add_argument("--audit-dir", type=Path, required=True)
    p.add_argument("--public-dir", type=Path, default=ROOT / "data_tw_public")
    p.add_argument("--stage-only", action="store_true")
    args = p.parse_args()
    out, audit = args.output_dir.resolve(), args.audit_dir.resolve()
    out.mkdir(parents=True, exist_ok=True)
    if args.stage_only:
        print(json.dumps({"action_dependencies": stage_inputs(out, args.public_dir.resolve())}, indent=2))
    else:
        print(json.dumps(finalize(out, audit), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
