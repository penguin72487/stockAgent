"""Independently compare a prior-exchange liquidity candidate with its parent.

This is an observational readback, not publication or training admission.  It
compares the complete selected daily rows and independently rejoins every
changed liquidity observation to the same physical life on the preceding real
exchange session.  The caller supplies immutable parent/candidate directories.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import UTC, datetime
import json
from pathlib import Path

import polars as pl

from downloader.artifact_io import atomic_write_json, sha256_file


ALLOWED = {
    "previous_volume", "previous_open_interest", "taifex_volume_logret_1d",
    "taifex_open_interest_change_asinh",
}


def verified_materialization(root: Path) -> tuple[dict, dict[str, str]]:
    stage = json.loads((root / "preparation_stage.json").read_text())
    if stage.get("status") != "selected_materialization_ready_not_published":
        raise ValueError("not a completed, unpublished candidate preparation")
    manifest_path = root / "materialization/manifest.json"
    terms_manifest_path = root / "terms/manifest.json"
    if (sha256_file(manifest_path) != stage["materialization_manifest_sha256"]
            or sha256_file(terms_manifest_path) != stage["execution_terms_manifest_sha256"]):
        raise ValueError("prepared manifest fingerprint differs from its stage")
    manifest = json.loads(manifest_path.read_text())
    for name, item in manifest["outputs"].items():
        if sha256_file(root / "materialization" / name) != item["sha256"]:
            raise ValueError("materialized output fingerprint mismatch: " + name)
    terms = json.loads(terms_manifest_path.read_text())
    if sha256_file(root / "terms/rules.parquet") != terms["outputs"]["rules"]["sha256"]:
        raise ValueError("selected account rule fingerprint mismatch")
    identities = {
        "preparation_stage": sha256_file(root / "preparation_stage.json"),
        "materialization_manifest": sha256_file(manifest_path),
        "daily": manifest["outputs"]["continuous_daily.parquet"]["sha256"],
        "dependencies": manifest["outputs"]["execution_dependencies.parquet"]["sha256"],
        "terms_manifest": sha256_file(terms_manifest_path),
        "rules": terms["outputs"]["rules"]["sha256"],
    }
    return manifest, identities


def audit(parent: Path, candidate: Path) -> dict:
    old_manifest, old_hash = verified_materialization(parent)
    new_manifest, new_hash = verified_materialization(candidate)
    if (new_manifest.get("prior_exchange_liquidity_contract")
            != "prior_exchange_session_liquidity_v1"):
        raise ValueError("candidate does not declare the corrected liquidity contract")
    if (old_hash["dependencies"] != new_hash["dependencies"]
            or old_hash["rules"] != new_hash["rules"]):
        raise ValueError("accounting dependencies or selected rule bytes changed")
    if (old_manifest["rows"] != new_manifest["rows"]
            or old_manifest["products"] != new_manifest["products"]
            or set(old_manifest["requested_products"]) != set(new_manifest["requested_products"])
            or set(old_manifest["zero_print_products"]) != set(new_manifest["zero_print_products"])):
        raise ValueError("candidate changed the selected financial universe")
    old = pl.read_parquet(parent / "materialization/continuous_daily.parquet")
    new = pl.read_parquet(candidate / "materialization/continuous_daily.parquet")
    if set(new.columns) - set(old.columns) != {"previous_exchange_session_date"}:
        raise ValueError("candidate has unexpected new or missing fields")
    if set(old.columns) - set(new.columns):
        raise ValueError("candidate removed existing fields")
    keys = ["date", "physical_contract"]
    if old.select(keys).is_duplicated().any() or new.select(keys).is_duplicated().any():
        raise ValueError("selected account tape has duplicate dated physical lives")
    if not old.select(keys).equals(new.select(keys)):
        old, new = old.sort(keys), new.sort(keys)
        if not old.select(keys).equals(new.select(keys)):
            raise ValueError("selected account rows differ")
    differences = []
    for name in old.columns:
        if not old[name].equals(new[name]):
            if name not in ALLOWED:
                raise ValueError("unexpected changed daily column: " + name)
            changed = (~old[name].eq_missing(new[name])).sum()
            differences.append({"column": name, "rows": int(changed)})
    volume_change = ~old["previous_volume"].eq_missing(new["previous_volume"])
    interest_change = ~old["previous_open_interest"].eq_missing(new["previous_open_interest"])
    changed = volume_change | interest_change
    touched = new.filter(changed).select(
        "date", "product", "physical_contract", "previous_market_date", "previous_exchange_session_date",
        "previous_volume", "previous_open_interest", "executable", "same_contract_as_previous_session",
    )
    calendar = pl.read_parquet(candidate / "accounting_event_calendar.parquet").select(
        pl.col("date").alias("previous_market_date"), "accounting_only_date")
    touched = touched.join(calendar, on="previous_market_date", how="left", validate="m:1")
    if touched.filter(pl.col("accounting_only_date") != True).height or touched["accounting_only_date"].null_count():
        raise ValueError("liquidity changed outside the session immediately after an accounting-only date")
    # The pre-selection source includes an older physical row even when that
    # row is outside the selected publication horizon; never join by product
    # or delivery month alone.
    source = (pl.scan_parquet(candidate / "frame.parquet")
              .select(pl.col("date").alias("previous_exchange_session_date"), "physical_contract",
                      pl.col("volume").alias("source_volume"),
                      pl.col("open_interest").alias("source_open_interest"))
              .collect())
    checked = touched.join(source, on=["previous_exchange_session_date", "physical_contract"],
                           how="left", validate="m:1")
    if (not checked["previous_volume"].equals(checked["source_volume"])
            or not checked["previous_open_interest"].equals(checked["source_open_interest"])):
        raise ValueError("liquidity does not match the exact preceding physical exchange row")
    if checked.filter(pl.col("previous_exchange_session_date") >= pl.col("date")).height:
        raise ValueError("a purported prior exchange observation is not earlier")
    capable_before = old["executable"] & old["same_contract_as_previous_session"] & (old["previous_volume"] >= 2).fill_null(False)
    capable_after = new["executable"] & new["same_contract_as_previous_session"] & (new["previous_volume"] >= 2).fill_null(False)
    dates = Counter(str(x) for x in touched["date"].to_list())
    if verified_materialization(parent)[1] != old_hash or verified_materialization(candidate)[1] != new_hash:
        raise ValueError("source changed while the readback was running")
    return {
        "contract": "prior_exchange_full_rebuild_comparison_v1",
        "observed_at_utc": datetime.now(UTC).isoformat(),
        "status": "independently_compared_not_published",
        "parent": str(parent), "candidate": str(candidate),
        "parent_identity": old_hash, "candidate_identity": new_hash,
        "accounting_event_calendar_sha256": sha256_file(candidate / "accounting_event_calendar.parquet"),
        "candidate_source_frame_sha256": sha256_file(candidate / "frame.parquet"),
        "rows_compared": old.height, "products_with_selected_financial_component": old_manifest["products"],
        "zero_print_products": sorted(old_manifest["zero_print_products"]),
        "changed_columns": differences, "liquidity_changed_rows": touched.height,
        "liquidity_changed_dates": dict(sorted(dates.items())),
        "prior_exchange_source_equality_verified": True,
        "all_other_existing_daily_fields_exactly_equal": True,
        "same_physical_life_join_verified": True,
        "unchanged_accounting_rule_and_dependency_bytes": True,
        "new_one_lot_prior_capacity_at_50pct_rows": int((~capable_before & capable_after).sum()),
        "lost_one_lot_prior_capacity_at_50pct_rows": int((capable_before & ~capable_after).sum()),
        "this_is_not_a_fill": True, "full_history_or_source_complete": False,
        "financial_release_accepted": False, "gpu_training_started_for_candidate": False,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("parent", type=Path)
    parser.add_argument("candidate", type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    result = audit(args.parent.resolve(), args.candidate.resolve())
    result["audit_script_sha256"] = sha256_file(Path(__file__))
    atomic_write_json(args.output, result)
    print(json.dumps({k: result[k] for k in ("status", "rows_compared", "changed_columns",
          "liquidity_changed_rows", "liquidity_changed_dates", "new_one_lot_prior_capacity_at_50pct_rows")}), flush=True)


if __name__ == "__main__":
    main()
