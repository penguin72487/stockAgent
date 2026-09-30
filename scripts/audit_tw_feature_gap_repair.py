#!/usr/bin/env python3
"""Readback audit of the declared one-day policy and unresolved-feature scope."""
from __future__ import annotations

import argparse
from collections import Counter
import json
from pathlib import Path
import sys

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from downloader.artifact_io import atomic_write_bytes, atomic_write_json
from scripts.prepare_tw_day_trade_feature_catalog import sha256, write_csv
from stockagent.data.tw_public_release_schedule import next_session


def publish_semantic_audit(dataset, out):
    """Refresh the user's worklist only from a complete, accepted semantic ABI.

    Old adapter-status counts are historical evidence, not corruption verdicts.
    Preserve their exact bytes before publishing the replacement and receipt.
    """
    read = lambda p: json.loads(p.read_text())
    m = read(dataset / "dataset_manifest.json")
    acceptance = read(dataset / "independent_acceptance.json")
    if m["contract"] != "tw_feature_semantics_research_v1":
        raise ValueError("unexpected semantic contract")
    if any(m[k] for k in ("historical_point_in_time", "strict_training_eligible", "live_eligible")):
        raise ValueError("research data incorrectly promoted")
    if acceptance.get("status") != "passed" or acceptance["matrix_sha256"] != m["matrix"]["sha256"]:
        raise ValueError("semantic dataset has no matching independent acceptance")
    for filename, expected in (("model_inputs.parquet", m["matrix"]["sha256"]),
                               ("feature_columns.json", m["feature_schema_sha256"]),
                               ("validation.json", m["validation_sha256"]),
                               ("remaining_feature_worklist.csv", m["worklist_sha256"]),
                               ("missingness_causes.csv", m["missingness_causes_sha256"])):
        if sha256(dataset / filename) != expected:
            raise ValueError(f"semantic artifact changed: {filename}")
    schema = read(dataset / "feature_columns.json")
    stats = read(dataset / "validation.json")
    work = pl.read_csv(dataset / "remaining_feature_worklist.csv", schema_overrides={"remaining_missing": pl.UInt64})
    reasons = pl.read_csv(dataset / "missingness_causes.csv", schema_overrides={"cells": pl.UInt64})
    if work.height != len(schema["value_features"]) or set(work["feature"]) != set(schema["value_features"]):
        raise ValueError("semantic worklist schema mismatch")
    for r in work.iter_rows(named=True):
        missing = stats["features"][r["feature"]]["missing"]
        if r["remaining_missing"] != missing or reasons.filter(pl.col("feature") == r["feature"])["cells"].sum() != missing:
            raise ValueError("semantic diagnosis does not reconcile")
    remaining = work["remaining_missing"].sum()
    if acceptance["missingness_partition_total"] != remaining or acceptance["rows_compared"] != m["matrix"]["rows"]:
        raise ValueError("semantic acceptance count mismatch")
    out.mkdir(parents=True, exist_ok=True)
    archived = []
    for filename in ("remaining_feature_worklist.csv", "missingness_causes.csv", "acceptance.json"):
        old = out / filename
        if old.exists():
            archive = out / "history" / f"{old.stem}.{sha256(old)}{old.suffix}"
            if not archive.exists():
                atomic_write_bytes(archive, old.read_bytes(), durable=True)
            archived.append(str(archive.resolve()))
    for filename in ("remaining_feature_worklist.csv", "missingness_causes.csv"):
        atomic_write_bytes(out / filename, (dataset / filename).read_bytes(), durable=True)
    result = {**acceptance, "dataset": str(dataset.resolve()), "contract": m["contract"],
        "dataset_manifest_sha256": sha256(dataset / "dataset_manifest.json"),
        "independent_acceptance_sha256": sha256(dataset / "independent_acceptance.json"),
        "worklist_sha256": m["worklist_sha256"], "missingness_causes_sha256": m["missingness_causes_sha256"],
        "remaining_missing": remaining, "prior_audits_preserved": archived,
        "online_exhausted": False, "source_corruption_not_inferred_from_missingness": True,
        "note": "Replaces adapter-only statuses with temporal, scope and semantic causes; not a declaration that every unknown source is healthy."}
    atomic_write_json(out / "acceptance.json", result)
    return result


def audit(dataset, previous, research_bundle, daily_bundle, out):
    read = lambda p:json.loads(p.read_text())
    m = read(dataset / "dataset_manifest.json")
    if m.get("contract") == "tw_feature_semantics_research_v1":
        return publish_semantic_audit(dataset, out)
    stats = read(dataset / "validation.json")
    schema = read(dataset / "feature_columns.json")
    parent = Path(m["lineage"]["parent"])
    pm = read(parent / "dataset_manifest.json")
    inputs = read(parent / "source_receipts.json")
    rules = read(parent / "publication_rules.json")
    if m["historical_point_in_time"] or m["live_eligible"] or m["strict_training_eligible"]:
        raise ValueError("research data incorrectly promoted")
    if sha256(dataset / "model_inputs.parquet") != m["matrix"]["sha256"]:
        raise ValueError("final matrix fingerprint mismatch")
    if sha256(Path(pm["base_matrix"])) != pm["base_matrix_sha256"]:
        raise ValueError("strict base bytes changed")
    sessions = pl.read_parquet(inputs["calendar"], columns=["date"])["date"].cast(pl.Date).sort().unique(maintain_order=True).to_list()
    if sha256(Path(inputs["calendar"])) != inputs["calendar_sha256"]:
        raise ValueError("source calendar changed since construction")
    clocks = pl.read_csv(parent / "release_schedule.csv", schema_overrides={"source_index":pl.String})
    clocks = clocks.with_columns([pl.col(c).str.to_date() for c in ["date", "estimated_published_on", "safety_ready_on"]])
    wrong = clocks.filter((pl.col("extra_delay_days") != 1) |
        ((pl.col("safety_ready_on") - pl.col("estimated_published_on")).dt.total_days() != 1) |
        (pl.col("date") < pl.col("safety_ready_on")))
    if wrong.height or rules["extra_conservative_delay_calendar_days"] != 1:
        raise ValueError("extra safety delay contract failed")
    checked, known = 0, 0
    for receipt in inputs["sources"]:
        path = Path(receipt["normalized_path"])
        if sha256(path) != receipt["normalized_sha256"]:
            raise ValueError("normalized observations changed")
        names = pl.scan_parquet(path).collect_schema().names()
        if "estimated_published_on" not in names:
            continue  # market clocks validated in the schedule above
        table = pl.read_parquet(path, columns=[n for n in ["date", "estimated_published_on", "known_upload_on", "known_published_on"] if n in names])
        pubdays = table["estimated_published_on"].unique().to_list()
        table = table.with_columns(pl.col("estimated_published_on").replace_strict(
            {d:next_session(d,sessions) for d in pubdays}, default=None, return_dtype=pl.Date).alias("_expected"))
        if table.filter(pl.col("_expected").is_null() | (pl.col("date") != pl.col("_expected"))).height:
            raise ValueError("stock observations do not use the first post-publication market session")
        for name in [n for n in ("known_upload_on", "known_published_on") if n in names]:
            known += table[name].is_not_null().sum()
            if table.filter(pl.col(name).is_not_null() & (pl.col("date") <= pl.col(name))).height:
                raise ValueError("known clock lower bound violated")
        checked += table.height
    definitions = {r["feature"]:r for r in pl.read_csv(parent / "research_feature_dictionary.csv").to_dicts()}
    raw_audits = {r["dataset"]:r for r in read(research_bundle / "mapping_audit.json")}
    daily_audits = {r["feature"]:r for r in read(daily_bundle / "mapping_audit.json")}
    old = read(previous / "validation.json")["features"]
    worklist = []
    for name in schema["value_features"]:
        d = definitions.get(name,{})
        a = raw_audits.get(d.get("dataset"), daily_audits.get(name,{}))
        remaining = stats["features"][name]["missing"]
        status = a.get("state", a.get("reason", "unmapped_or_undefined_formula_requires_audit"))
        if not remaining:
            status = "no_missing_in_selected_key_universe"
        worklist.append({"feature":name,"source_dataset":d.get("dataset",a.get("source","canonical_base")),
            "category":d.get("category","canonical_base"),"old_missing":old[name]["missing"],
            "remaining_missing":remaining,"net_missing_reduction_including_clock_changes":old[name]["missing"]-remaining,
            "status":status,"source_overlap_checked":a.get("overlap"),"source_conflicts":a.get("conflicts"),
            "online_exhausted":False,"next_action":"none_in_selected_scope" if not remaining else
            "verify_original_definition_applicability_and_authorized_alternate_observations"})
    out.mkdir(parents=True,exist_ok=True)
    write_csv(out / "remaining_feature_worklist.csv",worklist)
    result = {"matrix_sha256":m["matrix"]["sha256"],"rows":stats["rows"],
        "declared_release_schedule_rows_checked":clocks.height,
        "normalized_stock_clock_rows_checked":checked,"known_clock_lower_bounds_checked":known,
        "declared_clock_policy_violations":0,"existing_observed_values_changed_by_daily_patch":stats["existing_observed_values_changed"],
        "old_missing":sum(v["missing"] for v in old.values()),
        "remaining_missing":sum(v["missing"] for v in stats["features"].values()),
        "status_feature_counts":dict(Counter(r["status"] for r in worklist)),
        "all_missing_resolved":False,"online_exhausted":False,
        "historical_publication_dates_proven":False,
        "note":"Zero violations validates the DECLARED estimated policy, not true original release times."}
    atomic_write_json(out / "acceptance.json",result)
    return result


if __name__ == "__main__":
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--dataset",type=Path,required=True)
    p.add_argument("--previous",type=Path,default=ROOT / "artifacts/datasets/tw_day_trade_release_schedule_research_20260928_v3")
    p.add_argument("--research-bundle",type=Path,default=ROOT / "artifacts/data_quality/tw_cross_source_fill_20260928_v2")
    p.add_argument("--daily-bundle",type=Path,default=ROOT / "artifacts/data_quality/tw_daily_feature_fill_20260928_v3")
    p.add_argument("--output-dir",type=Path,required=True)
    a=p.parse_args()
    print(json.dumps(audit(a.dataset,a.previous,a.research_bundle,a.daily_bundle,a.output_dir),ensure_ascii=False,indent=2))
