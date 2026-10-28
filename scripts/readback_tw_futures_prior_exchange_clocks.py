"""Independently validate four rebuilt futures clock and reference tapes.

The verified source functions are loaded from an immutable code release.  This
checks output values and bindings, not missing-source or training readiness.
"""
from __future__ import annotations

import argparse
from datetime import UTC, datetime
import inspect
import json
from pathlib import Path

import numpy as np
import polars as pl

from downloader.artifact_io import atomic_write_json, sha256_file
from stockagent.data.tw_futures_decision_clock import (
    bind_entry_specifications,
    load_entry_specifications,
    reprice_margin_entries,
    validate_clock_source,
    validate_opening_reference_source,
)
from stockagent.runtime_identity import verify_release_bundles, verify_source_release


def _same_tape(old: pl.DataFrame, new: pl.DataFrame, label: str) -> None:
    if old.schema != new.schema or not old.equals(new):
        raise ValueError(f"{label} values or ordering changed during the clock rebuild")


def _entry_quote_differences(old: pl.DataFrame, new: pl.DataFrame) -> pl.DataFrame:
    """Permit only one-ULP positive observed VWAP differences; retain rows."""
    if old.schema != new.schema or not old.drop("entry_quote").equals(new.drop("entry_quote")):
        raise ValueError("entry axes, volume, provenance or other non-price facts changed")
    compared = old.with_columns(new["entry_quote"].alias("_new_entry_quote"))
    changed = compared.filter(pl.col("entry_quote").ne_missing(pl.col("_new_entry_quote")))
    if changed.is_empty():
        return changed
    a, b = (changed[name].to_numpy() for name in ("entry_quote", "_new_entry_quote"))
    if (changed.filter(pl.col("status") != "observed_minute").height
            or not np.all(np.isfinite(a) & np.isfinite(b) & (a > 0) & (b > 0))):
        raise ValueError("a non-observed or non-finite entry quote changed")
    ai, bi = a.astype(np.float64).view(np.uint64), b.astype(np.float64).view(np.uint64)
    if np.any(np.maximum(ai, bi) - np.minimum(ai, bi) > 1):
        raise ValueError("entry quote changed by more than one FP64 ULP")
    if not np.array_equal(a.astype(np.float32), b.astype(np.float32)):
        raise ValueError("entry quote changes its stored FP32 value")
    return changed


def _verify_repricing(changed: pl.DataFrame, *, daily: Path, rules: Path,
                      directory: Path, manifest: dict) -> int:
    """Check changed quotes through real dated rules, limits and ledger ABI."""
    if changed.is_empty():
        return 0
    keys = ["date", "physical_contract"]
    selected = changed.select(keys)
    dates = changed["date"].unique().to_list()
    base = (pl.scan_parquet(daily)
            .filter(pl.col("date").is_in(dates))
            .select(*keys, "symbol", "open", "contract_multiplier", "asset_class", "product", "previous_volume")
            .collect().join(selected, on=keys, how="inner", validate="1:1"))
    rule_rows = (pl.scan_parquet(rules).filter(pl.col("date").is_in(dates)).collect()
                 .join(selected, on=keys, how="inner", validate="1:1"))
    source = base.join(rule_rows.drop("contract_multiplier"), on=keys, how="inner", validate="1:1")
    source = source.join(changed.drop("symbol"), on=keys, how="inner", validate="1:1")
    source = bind_entry_specifications(source, load_entry_specifications(directory, manifest))
    if source.height != changed.height:
        raise ValueError("changed quote lacks an exact financial, specification or rule row")
    tape = np.zeros((source.height, 33), dtype=np.float32)
    tape[:, 1] = 1
    tape[:, 4] = source["settlement_contract_value_twd"].to_numpy()
    tape[:, 8] = np.floor(np.nan_to_num(source["previous_volume"].to_numpy(), nan=0.) * .5)
    comparisons = 0
    for policy in ("previous_session", "single_contract_probe"):
        for participation in (.5, 1.):
            old = reprice_margin_entries(tape, source, participation=participation,
                                         broker_multiplier=1., asset_scope="all_twd", capacity_policy=policy)
            replacement = source.with_columns(pl.col("_new_entry_quote").alias("entry_quote"))
            new = reprice_margin_entries(tape, replacement, participation=participation,
                                         broker_multiplier=1., asset_scope="all_twd", capacity_policy=policy)
            if not np.array_equal(old, new, equal_nan=True):
                raise ValueError("one-ULP entry quote changes priced ledger, tax, margin or limit permission")
            comparisons += 1
    return comparisons


def readback(*, receipt: Path, preflight: Path, old_release: Path, new_release: Path,
             old_clock: Path, new_clock: Path, output: Path) -> dict:
    if output.exists():
        raise FileExistsError("preserve the existing independent clock readback")
    code = receipt.parent / "build-source"
    identity = verify_source_release(receipt, code)
    verify_release_bundles(receipt)
    imported = {}
    for function in (validate_clock_source, validate_opening_reference_source,
                     bind_entry_specifications, load_entry_specifications, reprice_margin_entries,
                     verify_source_release, verify_release_bundles):
        loaded = Path(inspect.getsourcefile(function)).resolve()
        if not loaded.is_relative_to(code.resolve()):
            raise ValueError("clock readback imported outside the frozen release: " + str(loaded))
        imported[function.__name__] = str(loaded.relative_to(code.resolve()))

    old_daily, old_rules = old_release / "daily/continuous_daily.parquet", old_release / "rules/rules.parquet"
    new_daily, new_rules = new_release / "daily/continuous_daily.parquet", new_release / "rules/rules.parquet"
    required = (receipt, preflight, old_daily, old_rules, new_daily, new_rules)
    pins = {str(path): sha256_file(path) for path in required}
    prior = json.loads(preflight.read_text())
    if (prior.get("contract") != "prior_exchange_clock_preflight_v1"
            or prior.get("clock_input_columns_exactly_equal") is not True
            or prior.get("old_daily_sha256") != pins[str(old_daily)]
            or prior.get("new_daily_sha256") != pins[str(new_daily)]
            or prior.get("old_rules_sha256") != pins[str(old_rules)]
            or prior.get("new_rules_sha256") != pins[str(new_rules)]
            or prior.get("source_release", {}).get("source_sha256") != identity["source_sha256"]):
        raise ValueError("prior clock preflight does not bind these exact source files")
    results = []
    for window, subdir in ((1, Path()), (300, Path("entry_window_300m"))):
        for clock, decision in (("0845", "08:45"), ("0900", "09:00")):
            old_dir, new_dir = old_clock / subdir / clock, new_clock / subdir / clock
            old_entry, new_entry = old_dir / "entries.parquet", new_dir / "entries.parquet"
            old_ref, new_ref = old_dir / "opening_reference.parquet", new_dir / "opening_reference.parquet"
            local_files = (old_entry, new_entry, old_ref, new_ref,
                           old_dir / "manifest.json", new_dir / "manifest.json",
                           old_dir / "opening_reference_manifest.json",
                           new_dir / "opening_reference_manifest.json")
            pins.update({str(path): sha256_file(path) for path in local_files})
            old_entries, old_manifest = validate_clock_source(
                old_entry, daily_path=old_daily, rules_path=old_rules, decision_time=decision)
            new_entries, new_manifest = validate_clock_source(
                new_entry, daily_path=new_daily, rules_path=new_rules, decision_time=decision)
            if (old_manifest["clock"]["window_minutes"] != window
                    or new_manifest["clock"]["window_minutes"] != window
                    or new_manifest["clock"]["asset_scope"] != "all_twd"):
                raise ValueError("clock scope or execution window differs from the request")
            changed = _entry_quote_differences(old_entries, new_entries)
            comparisons = _verify_repricing(changed, daily=new_daily, rules=new_rules,
                                            directory=new_dir, manifest=new_manifest)
            old_refs, _ = validate_opening_reference_source(
                old_ref, daily_path=old_daily, decision_time=decision, entry_manifest=old_manifest)
            new_refs, _ = validate_opening_reference_source(
                new_ref, daily_path=new_daily, decision_time=decision, entry_manifest=new_manifest)
            _same_tape(old_refs, new_refs, f"{clock}/{window} reference")
            if not old_entries.select("date", "physical_contract", "symbol").equals(
                    old_refs.select("date", "physical_contract", "symbol")):
                raise ValueError("opening reference and entry axes differ")
            statuses = {row[0]: row[1] for row in new_entries.group_by("status").len().iter_rows()}
            kinds = {row[0]: row[1] for row in new_refs.group_by("reference_kind").len().iter_rows()}
            results.append({"decision_time": decision, "window_minutes": window,
                            "entry_rows": new_entries.height, "entry_statuses": statuses,
                            "opening_reference_kinds": kinds,
                            "old_and_new_entries_exactly_equal": changed.is_empty(),
                            "one_ulp_vwap_rows": changed.height,
                            "one_ulp_vwap_changes": changed.select(
                                "date", "physical_contract", "entry_quote", "_new_entry_quote"
                            ).with_columns(pl.col("date").cast(pl.String)).to_dicts(),
                            "dated_repricing_policy_comparisons": comparisons,
                            "old_and_new_references_exactly_equal": True,
                            "pair_identity": new_manifest["pair_identity"]})
            print(json.dumps({"stage": "readback_pair_complete", **results[-1]}), flush=True)
            del old_entries, new_entries, old_refs, new_refs
    if any(sha256_file(Path(path)) != digest for path, digest in pins.items()):
        raise ValueError("source or clock bytes changed during independent readback")
    verify_source_release(receipt, code)
    proof = {"contract": "prior_exchange_clock_frozen_readback_v2",
             "completed_at_utc": datetime.now(UTC).isoformat(),
             "source_release": identity, "imported_functions_from_verified_release": imported,
             "pairs": results, "input_pins": pins,
             "quarantined_gaps_not_repaired": True,
             "financial_point_in_time_verified": False, "source_coverage_complete": False,
             "end_to_end_training_verified": False, "actual_broker_fills_verified": False,
             "script_sha256": sha256_file(Path(__file__))}
    atomic_write_json(output, proof)
    return proof


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("receipt", "preflight", "old-release", "new-release", "old-clock", "new-clock", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    args = parser.parse_args()
    readback(**{name: getattr(args, name).resolve() for name in
                ("receipt", "preflight", "old_release", "new_release", "old_clock", "new_clock", "output")})


if __name__ == "__main__":
    main()
