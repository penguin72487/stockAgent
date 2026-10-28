"""Publish a source-bound prior-exchange candidate through the shared owner.

This admits only selected research components.  It does not assert complete
historical point-in-time coverage, create execution clocks, or start training.
"""
from __future__ import annotations

import argparse
from datetime import UTC, datetime
import inspect
import json
from pathlib import Path
import shutil

from downloader.artifact_io import atomic_write_json, sha256_file
from stockagent.data.tw_futures_margin import validate_margin_rule_source
from stockagent.data.tw_futures_margin_release import publish_all_twd_margin_release
from stockagent.runtime_identity import verify_release_bundles, verify_source_release


def publish(receipt: Path, candidate: Path, audit_path: Path, audit_script: Path,
            final_settlement: Path, output: Path, report: Path) -> dict:
    code = receipt.parent / "build-source"
    identity = verify_source_release(receipt, code)
    verify_release_bundles(receipt)
    for function in (publish_all_twd_margin_release, validate_margin_rule_source,
                     verify_source_release, verify_release_bundles):
        loaded = Path(inspect.getsourcefile(function)).resolve()
        if not loaded.is_relative_to(code.resolve()):
            raise ValueError("publisher imported code outside the frozen source: " + str(loaded))
    release = json.loads(receipt.read_text())
    overlay = release.get("source_overlay", {})
    expected = {"scripts/build_project_release.py", "scripts/prepare_tw_futures_margin_training.py",
                "stockagent/data/tw_futures_margin_release.py"}
    if (overlay.get("base_source_sha256") != "3b31101d252c96a513972fbf6f0afbb9fd5db5d9bdd0538384056b4cd8132261"
            or overlay.get("unselected_workspace_changes_promoted") is not False
            or set(overlay.get("overrides", {})) != expected):
        raise ValueError("candidate code is not the audited frozen overlay")
    audit = json.loads(audit_path.read_text())
    audit_hash = sha256_file(audit_path)
    if (audit.get("contract") != "prior_exchange_full_rebuild_comparison_v1"
            or audit.get("status") != "independently_compared_not_published"
            or audit.get("candidate") != str(candidate)
            or audit.get("audit_script_sha256") != sha256_file(audit_script)
            or audit.get("prior_exchange_source_equality_verified") is not True
            or audit.get("same_physical_life_join_verified") is not True
            or audit.get("all_other_existing_daily_fields_exactly_equal") is not True
            or audit.get("unchanged_accounting_rule_and_dependency_bytes") is not True
            or audit.get("financial_release_accepted") is not False):
        raise ValueError("candidate lacks a matching independent comparison")
    pins = {
        "preparation_stage": candidate / "preparation_stage.json",
        "materialization_manifest": candidate / "materialization/manifest.json",
        "daily": candidate / "materialization/continuous_daily.parquet",
        "dependencies": candidate / "materialization/execution_dependencies.parquet",
        "terms_manifest": candidate / "terms/manifest.json",
        "rules": candidate / "terms/rules.parquet",
    }
    if any(sha256_file(path) != audit["candidate_identity"][key] for key, path in pins.items()):
        raise ValueError("candidate changed after its independent comparison")
    if sha256_file(candidate / "frame.parquet") != audit["candidate_source_frame_sha256"]:
        raise ValueError("candidate physical source changed after comparison")
    if sha256_file(candidate / "accounting_event_calendar.parquet") != audit["accounting_event_calendar_sha256"]:
        raise ValueError("candidate account calendar changed after comparison")
    if output.exists() or report.exists():
        raise FileExistsError("preserve the existing candidate release or acceptance report")
    if shutil.disk_usage(candidate).free < 25 * 1024**3:
        raise RuntimeError("candidate host lacks the 25 GiB release reserve")
    final_sha = sha256_file(final_settlement)
    print(json.dumps({"stage": "source_audit_accepted", "audit_sha256": audit_hash,
                      "candidate": str(candidate), "output": str(output)}, ensure_ascii=False), flush=True)
    result = publish_all_twd_margin_release(
        materialization=candidate / "materialization", execution_terms=candidate / "terms/rules.parquet",
        final_settlement=final_settlement, output=output,
    )
    daily, rules = output / "daily/continuous_daily.parquet", output / "rules/rules.parquet"
    validated_rules, rule_manifest = validate_margin_rule_source(rules, daily)
    daily_manifest = json.loads((output / "daily/manifest.json").read_text())
    if (daily_manifest.get("materialization_version") != 3
            or daily_manifest.get("prior_exchange_liquidity_contract") != "prior_exchange_session_liquidity_v1"
            or daily_manifest.get("research_only") is not True
            or daily_manifest.get("all_products_training_ready") is not False
            or daily_manifest.get("financial_point_in_time_verified") is not False
            or daily_manifest.get("execution_rules_sha256") != sha256_file(rules)
            or rule_manifest.get("source_daily_sha256") != sha256_file(daily)
            or len(validated_rules) != daily_manifest["rows"]):
        raise ValueError("published candidate failed independent daily/rule readback")
    if (any(sha256_file(path) != audit["candidate_identity"][key] for key, path in pins.items())
            or sha256_file(final_settlement) != final_sha or sha256_file(audit_path) != audit_hash):
        raise ValueError("publication inputs changed during validation")
    verify_source_release(receipt, code)
    proof = {
        "contract": "prior_exchange_candidate_publication_readback_v1",
        "completed_at_utc": datetime.now(UTC).isoformat(),
        "candidate": str(candidate), "source_release": identity,
        "independent_audit_sha256": audit_hash, "final_settlement_sha256": final_sha,
        "publisher_result": result, "daily_manifest_sha256": sha256_file(output / "daily/manifest.json"),
        "rule_manifest_sha256": sha256_file(output / "rules/manifest.json"),
        "daily_sha256": sha256_file(daily), "rules_sha256": sha256_file(rules),
        "selected_research_components": daily_manifest["products"],
        "selected_account_rows": daily_manifest["rows"],
        "research_only": True, "financial_point_in_time_verified": False,
        "all_history_or_products_training_ready": False,
        "execution_clocks_rebound": False, "training_started_for_candidate": False,
        "publication_script_sha256": sha256_file(Path(__file__)),
    }
    atomic_write_json(report, proof)
    print(json.dumps({"stage": "research_candidate_published_and_read_back", "report": str(report),
                      "products": proof["selected_research_components"],
                      "rows": proof["selected_account_rows"], "training_started": False}), flush=True)
    return proof


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("receipt", "candidate", "audit", "audit-script", "final-settlement", "output", "report"):
        parser.add_argument("--" + name, type=Path, required=True)
    args = parser.parse_args()
    publish(*(getattr(args, name.replace("-", "_")).resolve()
              for name in ("receipt", "candidate", "audit", "audit-script", "final-settlement", "output", "report")))


if __name__ == "__main__":
    main()
