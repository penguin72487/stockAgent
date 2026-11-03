"""Check whether a corrected financial release can reuse a verified clock core.

The canonical clock builder still rechecks the original source objects and
rebuilds all public outputs.  This preflight merely proves identical clock
inputs and binds the existing, exact missing-execution allowlist to the new
financial release; it never admits a new observation or changes a fill.
"""
from __future__ import annotations

import argparse
from datetime import UTC, date, datetime
import hashlib
import inspect
import json
from pathlib import Path

import polars as pl

from downloader.artifact_io import atomic_write_json, sha256_file
from scripts.prepare_tw_futures_decision_clocks import _load_clock_history
from stockagent.data.tw_futures_decision_clock import (
    bind_entry_specifications, validate_execution_quarantine,
)
from stockagent.data.tw_futures_margin_release import read_bound_output
from stockagent.runtime_identity import verify_source_release


def history(path: Path, end: date) -> pl.DataFrame:
    scan = pl.scan_parquet(path)
    names = scan.collect_schema().names()
    aliases = ["shioaji_roots"] if "shioaji_roots" in names else []
    return _load_clock_history(
        scan, end, aliases,
        ["observed_open", "observed_close", "source_row_observed", "official_source_sha256"],
        ["outright_volume", "tenor_rank", "official_expiry"],
    )


def validate_rule_delta_for_clock_reuse(*, old_rules: Path, new_rules: Path,
                                        publication: dict, readback: Path | None) -> dict:
    """Admit an audited expiry-value-only change without changing clock inputs.

    The previous strict behavior remains the default. A changed rule tape is
    accepted only with a matching immutable publication and independent
    selected-tape readback, followed by a fresh complete column comparison.
    This never admits a price, a new trade, or an accounting calendar.
    """
    old_sha, new_sha = sha256_file(old_rules), sha256_file(new_rules)
    if old_sha == new_sha:
        return {"contract": "identical_financial_rule_bytes_v1", "changed_columns": {}}
    if (readback is None
            or publication.get("contract") != "terminal_rounding_v22_research_publication_v1"
            or publication.get("selected_readback_sha256") != sha256_file(readback)):
        raise ValueError("changed financial rules require the matching audited expiry-only publication")
    audit = json.loads(readback.read_text())
    if (audit.get("contract") != "index_terminal_rounding_v22_selected_independent_readback_v1"
            or audit.get("candidate") != publication.get("candidate")
            or audit.get("old_candidate_identity", {}).get("rules") != old_sha
            or audit.get("candidate_identity", {}).get("rules") != new_sha
            or audit.get("selected_daily_values_equal") is not True
            or audit.get("selected_dependency_bytes_equal") is not True
            or audit.get("financial_release_accepted") is not False
            or audit.get("training_admitted") is not False
            or publication.get("research_only") is not True
            or publication.get("training_started_for_candidate") is not False):
        raise ValueError("expiry-only rule readback is not bound to these research releases")
    left, right = pl.read_parquet(old_rules), pl.read_parquet(new_rules)
    keys = ["date", "physical_contract"]
    if left.schema != right.schema or left.height != right.height:
        raise ValueError("changed rule tape has a different schema or physical-day count")
    if not left.select(keys).equals(right.select(keys)):
        left, right = left.sort(keys), right.sort(keys)
    if not left.select(keys).equals(right.select(keys)) or left.select(keys).is_duplicated().any():
        raise ValueError("changed rule tape has a different or ambiguous physical-day axis")
    differences = {column: int(left[column].ne_missing(right[column]).sum())
                   for column in left.columns if not left[column].equals(right[column])}
    if (set(differences) != {"terminal_contract_value_twd"}
            or differences != audit.get("selected_rule_changed_columns")):
        raise ValueError(f"changed rule tape affects more than the audited expiry values: {differences}")
    if sha256_file(old_rules) != old_sha or sha256_file(new_rules) != new_sha:
        raise ValueError("financial rule tape changed during clock delta verification")
    return {"contract": "audited_expiry_only_rule_delta_v1", "changed_columns": differences,
            "selected_readback_sha256": sha256_file(readback),
            "old_rules_sha256": old_sha, "new_rules_sha256": new_sha}


def validate_checkpoint_rebuild_policy(*, checkpoint: Path, document: dict,
                                       old_source_hash: str, new_source_hash: str,
                                       rebuild_from_originals: bool) -> bool:
    """Permit a different input identity only for a mandatory raw rebuild."""
    identity = document.get("identity", {})
    expected_name = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
    if (not isinstance(rebuild_from_originals, bool)
            or document.get("contract") != "verified_bulk_window_checkpoint_v1"
            or checkpoint.name != expected_name or new_source_hash != old_source_hash
            or identity.get("asset_scope") != "all_twd"
            or identity.get("entry_windows") != [1, 300]):
        raise ValueError("canonical checkpoint identity or selected clock scope is invalid")
    matches = identity.get("source_frame_sha256") == old_source_hash
    if not matches and not rebuild_from_originals:
        raise ValueError("canonical checkpoint cannot be reused for these exact clock inputs")
    return matches


def preflight(*, old_daily: Path, old_rules: Path, new_daily: Path, new_rules: Path,
              old_quarantine: Path, gaps: Path, spec_path: Path, checkpoint: Path,
              frozen_receipt: Path, publication: Path, output_quarantine: Path,
              report: Path, start: date, end: date, rule_delta_readback: Path | None = None,
              rebuild_from_originals: bool = False) -> dict:
    if output_quarantine.exists() or report.exists():
        raise FileExistsError("preserve existing clock control and preflight evidence")
    if start > end:
        raise ValueError("clock horizon is reversed")
    source_root = frozen_receipt.parent / "build-source"
    source_proof = verify_source_release(frozen_receipt, source_root)
    for function in (_load_clock_history, bind_entry_specifications, read_bound_output,
                     validate_execution_quarantine, verify_source_release):
        loaded = Path(inspect.getsourcefile(function)).resolve()
        if not loaded.is_relative_to(source_root.resolve()):
            raise ValueError("clock preflight imported code outside the frozen source: " + str(loaded))
    published = json.loads(publication.read_text())
    new_daily_sha, new_rules_sha = sha256_file(new_daily), sha256_file(new_rules)
    old_daily_sha, old_rules_sha = sha256_file(old_daily), sha256_file(old_rules)
    if (published.get("contract") not in {"prior_exchange_candidate_publication_readback_v1",
                                         "terminal_rounding_v22_research_publication_v1"}
            or published.get("daily_sha256") != new_daily_sha
            or published.get("rules_sha256") != new_rules_sha
            or published.get("financial_point_in_time_verified") is not False
            or published.get("all_history_or_products_training_ready") is not False):
        raise ValueError("new release lacks matching, bounded publication readback")
    rules_comparison = validate_rule_delta_for_clock_reuse(
        old_rules=old_rules, new_rules=new_rules, publication=published, readback=rule_delta_readback)
    pins = {str(path): sha256_file(path) for path in (
        old_daily, old_rules, new_daily, new_rules, old_quarantine, gaps, spec_path,
        spec_path.with_name("manifest.json"), checkpoint / "manifest.json", frozen_receipt, publication,
    )}
    if rule_delta_readback is not None:
        pins[str(rule_delta_readback)] = sha256_file(rule_delta_readback)
    old_history, new_history = history(old_daily, end), history(new_daily, end)
    if old_history.schema != new_history.schema or not old_history.equals(new_history):
        raise ValueError("published releases differ in clock or reference history inputs")
    specs, spec_proof = read_bound_output(spec_path)
    if spec_proof.get("dataset") != "taifex_dated_product_specifications":
        raise ValueError("clock reuse requires the original dated specifications")
    old_source = bind_entry_specifications(old_history.filter(pl.col("date") >= start), specs)
    new_source = bind_entry_specifications(new_history.filter(pl.col("date") >= start), specs)
    if not old_source.equals(new_source):
        raise ValueError("dated clock source differs after specification binding")
    old_source_hash = hashlib.sha256(old_source.serialize()).hexdigest()
    new_source_hash = hashlib.sha256(new_source.serialize()).hexdigest()
    checkpoint_doc = json.loads((checkpoint / "manifest.json").read_text())
    identity = checkpoint_doc.get("identity", {})
    checkpoint_matches = validate_checkpoint_rebuild_policy(checkpoint=checkpoint,
        document=checkpoint_doc, old_source_hash=old_source_hash, new_source_hash=new_source_hash,
        rebuild_from_originals=rebuild_from_originals)
    previous = json.loads(old_quarantine.read_text())
    missing = pl.read_parquet(gaps)
    validate_execution_quarantine(previous, missing, daily_sha256=old_daily_sha, rules_sha256=old_rules_sha)
    rebound = {
        **previous, "created_at_utc": datetime.now(UTC).isoformat(),
        "source_daily_sha256": new_daily_sha, "source_rules_sha256": new_rules_sha,
        "rebound_from_quarantine_sha256": pins[str(old_quarantine)],
        "rebound_from_verified_financial_readback_sha256": pins[str(publication)],
        "rebind_only_exact_previous_contract_days": True,
        "new_gaps_require_independent_review": True,
    }
    validate_execution_quarantine(rebound, missing, daily_sha256=new_daily_sha, rules_sha256=new_rules_sha)
    if any(sha256_file(Path(path)) != digest for path, digest in pins.items()):
        raise ValueError("clock source changed during preflight")
    atomic_write_json(output_quarantine, rebound)
    result = {
        "contract": "prior_exchange_clock_preflight_v1",
        "completed_at_utc": datetime.now(UTC).isoformat(),
        "old_daily_sha256": old_daily_sha, "new_daily_sha256": new_daily_sha,
        "old_rules_sha256": old_rules_sha, "new_rules_sha256": new_rules_sha,
        "financial_rules_comparison": rules_comparison,
        "history_input_rows": old_history.height, "execution_input_rows": old_source.height,
        "clock_input_columns_exactly_equal": True,
        "checkpoint_source_frame_sha256": old_source_hash if checkpoint_matches else None,
        "current_source_frame_sha256": old_source_hash,
        "prior_checkpoint_source_frame_sha256": identity.get("source_frame_sha256"),
        "prior_checkpoint_source_frame_exact_match": checkpoint_matches,
        "bulk_checkpoint_reuse_approved": not rebuild_from_originals,
        "rebuild_from_originals_required": rebuild_from_originals,
        "checkpoint_manifest_sha256": pins[str(checkpoint / "manifest.json")],
        "source_release": source_proof, "new_quarantine_sha256": sha256_file(output_quarantine),
        "quarantined_contract_days": missing.height, "new_gaps_require_independent_review": True,
        "canonical_clock_rebuild_pending": True, "original_raw_sha_revalidation_pending": True,
        "financial_point_in_time_verified": False, "training_started": False,
        "script_sha256": sha256_file(Path(__file__)),
    }
    atomic_write_json(report, result)
    print(json.dumps({"stage": "clock_inputs_verified", "rows": old_source.height,
                      "quarantined": missing.height, "report": str(report)}), flush=True)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("old-daily", "old-rules", "new-daily", "new-rules", "old-quarantine", "gaps",
                 "spec-path", "checkpoint", "frozen-receipt", "publication", "output-quarantine", "report"):
        parser.add_argument("--" + name, required=True, type=Path)
    parser.add_argument("--start", required=True, type=date.fromisoformat)
    parser.add_argument("--end", required=True, type=date.fromisoformat)
    parser.add_argument("--rule-delta-readback", type=Path,
                        help="Independent expiry-only research comparison; omitted for identical rules")
    parser.add_argument("--rebuild-from-originals", action="store_true",
                        help="Rebind the exact quarantine but require raw-source rebuild, never checkpoint reuse")
    preflight(**{key: value.resolve() if isinstance(value, Path) else value for key, value in vars(parser.parse_args()).items()})


if __name__ == "__main__":
    main()
