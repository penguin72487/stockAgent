#!/usr/bin/env python3
"""Report every inventoried field against the exact mixed-source admission.

Classification is not another dataset, an acquisition-failure verdict, or PIT
certification. Source counts and eventual model dimensions remain separate.
"""
from __future__ import annotations

import argparse
from collections import Counter
import csv
from datetime import UTC, datetime
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from downloader.artifact_io import atomic_write_json
from scripts.prepare_tw_day_trade_feature_catalog import sha256, write_csv
from stockagent.data.tw_day_trade_feature_admission import classify_candidate, category_for, CATEGORY_LABELS
from stockagent.data.tw_public_cross_source_fill import MAPPINGS


def read_csv(path):
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def coverage_fields(name: str, proof: dict, definition: dict, quality_counts: Counter) -> dict:
    """Row-weighted availability, not a corruption or applicable-issuer rate."""
    denominator = int(proof["decision_feature_rows"])
    cells = int(proof["feature_available_cells"][name])
    if denominator <= 0 or not 0 <= cells <= denominator:
        raise ValueError("invalid feature availability counts")
    return {"observations": definition["observations"],
            "first_usable": definition["first_usable"], "last_usable": definition["last_usable"],
            "unmapped_release_periods": definition.get("unmapped_release_periods", 0),
            "available_stock_decision_cells": cells,
            "decision_stock_rows_denominator": denominator,
            "availability_ratio_over_all_stock_rows": cells/denominator,
            "quality_barrier_observation_rows": quality_counts[name],
            "coverage_basis": "all_stock_decision_rows_not_applicable_issuer_or_source_period_denominator"}


def classify(row: dict, specs: list[dict], finlab: dict[str, dict]) -> dict:
    result = classify_candidate(row)
    # A legacy curated-table recommendation is not admission to this new ABI.
    if result["decision"] == "canonical_feature_selected":
        result.update(decision="excluded_legacy_representation",
                      selected_canonical_feature="", reason="new_abi_uses_explicit_source_specs_not_legacy_allowlist")
    selected = None
    if row["provider"] == "FinLab":
        previous = finlab.get(row["catalog_id"])
        if previous:
            result.update(previous)
        selected = next((s for s in specs if s.get("dataset") == row["dataset_id"] and s["source"] == "FinLab"), None)
    elif row["dataset_id"] == "physical:tw-public:training-features":
        selected = next((s for s in specs if s.get("source_column") == row["field"]), None)
    if selected:
        result.update(decision="selected_private_research_observation",
                      selected_canonical_feature=selected["feature"],
                      reason="explicit_registered_clock_and_pinned_source; eventual_validity_and_variation_gate_required",
                      update_frequency=selected["rule"]["kind"], carry_days=selected["rule"]["carry_days"],
                      scope=selected["rule"]["scope"], publication_clock=selected["clock"],
                      publication_time_estimated=selected["publication_time_estimated"],
                      original_historical_vintage_verified=False)
    elif row["provider"] == "FinMind" and not result["decision"].startswith("excluded_"):
        dataset = row["dataset_id"].split(":", 1)[-1]
        mappings = [m for m in MAPPINGS.values() if m.dataset == dataset]
        if mappings:
            result.update(decision="quarantined_missing_only_adapter",
                          reason="registered_economic_mapping_exists; needs_current_period_unit_overlap_conflict_and_primary_hash_acceptance",
                          update_frequency="|".join(sorted({m.kind for m in mappings})))
    # A TDCC investor count is a measure, not an entity ID; its tier is the key.
    if row["provider"] == "FinMind" and row["field"] == "investors" and "HoldingSharesPer" in row["dataset_id"]:
        result.update(decision="quarantined_adapter", reason="holder_count_requires_tier_aware_pivot_and_publication_clock_not_an_identifier")
    result.update(source_first=row.get("source_first"), source_last=row.get("source_last"),
                  bounds_basis=row.get("bounds_basis"), source_schema_verified=row.get("schema_verified"),
                  source_content_sha_verified=row.get("sha256_verified"),
                  admitted_as_model_value=False)
    return result


def report(catalog: Path, source: Path, out: Path, view: Path | None = None) -> dict:
    if out.exists():
        raise FileExistsError("report at a fresh versioned root")
    out.mkdir(parents=True)
    manifest_path = source/"source_manifest.json"
    manifest = json.loads(manifest_path.read_text())
    specs = manifest["feature_specs"]
    finlab = {r["catalog_id"]: r for r in read_csv(source/"finlab_admission.csv")}
    classified = [classify(r, specs, finlab) for r in read_csv(catalog/"feature_candidates.csv")]
    admitted_names = set()
    definitions, quality_counts = {}, Counter()
    view_proof = None
    if view is not None:
        view_proof = json.loads((view/"dataset_manifest.json").read_text())
        if view_proof["source_manifest_sha256"] != sha256(manifest_path):
            raise ValueError("remote view does not bind this source release")
        definition_rows = read_csv(view/"feature_dictionary.csv")
        definitions = {r["feature"]: r for r in definition_rows}
        admitted_names = set(definitions)
        if (len(definitions) != len(definition_rows)
                or len(definitions) != view_proof["value_features"]
                or set(view_proof["feature_available_cells"]) != admitted_names
                or not admitted_names <= {s["feature"] for s in specs}):
            raise ValueError("remote feature dictionary/count contract mismatch")
        quality_counts = Counter(r["feature"] for r in json.loads((view/"quality_masks.json").read_text()))
        rejected = {r["feature"]: r["reason"] for r in read_csv(view/"excluded_features.csv")}
        for row in classified:
            name = row["selected_canonical_feature"]
            row["admitted_as_model_value"] = name in admitted_names
            if name in rejected:
                row.update(decision="excluded_no_variable_observation", reason=rejected[name])
    write_csv(out/"feature_classification.csv", classified)
    write_csv(out/"excluded_features.csv", [r for r in classified if r["decision"].startswith("excluded_") or r["decision"] == "execution_only"])
    write_csv(out/"remaining_feature_worklist.csv", [r for r in classified if r["decision"].startswith("quarantined_")])
    values = [{"feature": s["feature"], "source": s["source"],
               "dataset": s.get("dataset", s.get("source_column")),
               "category": s.get("category") or category_for("tw-public", s.get("source_column", "")),
               "update_frequency": s["rule"]["kind"], "carry_days": s["rule"]["carry_days"],
               "scope": s["rule"]["scope"], "clock": s["clock"],
               "publication_time_estimated": s["publication_time_estimated"],
               "value_vintage": s["value_vintage"], "admitted_as_model_value": s["feature"] in admitted_names}
              for s in specs]
    for r in values:
        r["category_zh"] = CATEGORY_LABELS.get(r["category"], r["category"])
        if r["feature"] in admitted_names:
            r.update(coverage_fields(r["feature"], view_proof, definitions[r["feature"]], quality_counts))
    write_csv(out/"selected_source_features.csv", values)
    summary = {"created_at_utc": datetime.now(UTC).isoformat(),
               "catalog_summary": json.loads((catalog/"catalog_summary.json").read_text()),
               "classified_semantic_rows": len(classified), "decisions": dict(Counter(r["decision"] for r in classified)),
               "categories": dict(Counter(r["category"] for r in classified)),
               "selected_source_value_features": len(specs), "source_end_date": manifest["end_date"],
               "selected_sources": dict(Counter(s["source"] for s in specs)),
               "selected_frequency_kinds": dict(Counter(s["rule"]["kind"] for s in specs)),
               "source_manifest_sha256": sha256(manifest_path),
               "feature_view_verified": bool(view_proof), "remote_view": view_proof,
               "admitted_model_value_features": len(admitted_names),
               "actual_model_channels": view_proof.get("model_channels") if view_proof else None,
               "admitted_frequency_kinds": dict(Counter(s["rule"]["kind"] for s in specs if s["feature"] in admitted_names)),
               "reporter_sha256": sha256(Path(__file__)),
               "training_ready": bool(view_proof and view_proof.get("training_ready")),
               "historical_point_in_time": False,
               "limitations": ["schema/footer/receipt audit is not all-byte or all-security-cell completeness",
                               "cycle or nonapplicability is not corruption; unadapted is not all-null",
                               "FinMind missing-only mappings not promoted without economic-key comparison",
                               "TEJ query previews not certified history; snapshot/unregistered clocks remain excluded",
                               "current revised values and estimated release clocks remain private research"]}
    atomic_write_json(out/"admission_summary.json", summary)
    return {k: v for k, v in summary.items() if k not in ("catalog_summary", "remote_view")}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", type=Path, required=True)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--remote-view", type=Path)
    args = parser.parse_args()
    print(json.dumps(report(args.catalog, args.source_root, args.output_dir, args.remote_view), ensure_ascii=False))


if __name__ == "__main__":
    main()
