"""Revalidate a published research release with its verified frozen code.

This readback does not modify the release and does not assert complete source
coverage, historical point-in-time proof, or end-to-end training readiness.
"""
from __future__ import annotations

import argparse
from datetime import UTC, datetime
import inspect
import json
from pathlib import Path

import polars as pl

from downloader.artifact_io import atomic_write_json, sha256_file
from stockagent.data.tw_futures_margin import (
    corporate_margin_base_values, load_margin_terminal_components, validate_margin_rule_source,
)
from stockagent.data.tw_futures_margin_release import validate_accounting_continuation
from stockagent.runtime_identity import verify_release_bundles, verify_source_release


def readback(receipt: Path, publication: Path, final_settlement: Path, output: Path) -> dict:
    if output.exists():
        raise FileExistsError("preserve the existing independent release readback")
    code = receipt.parent / "build-source"
    identity = verify_source_release(receipt, code)
    verify_release_bundles(receipt)
    loaded = {}
    for function in (corporate_margin_base_values, load_margin_terminal_components,
                     validate_margin_rule_source, validate_accounting_continuation,
                     verify_source_release, verify_release_bundles):
        path = Path(inspect.getsourcefile(function)).resolve()
        if not path.is_relative_to(code.resolve()):
            raise ValueError("release readback imported code outside the frozen source: " + str(path))
        loaded[function.__name__] = str(path.relative_to(code.resolve()))
    previous = json.loads(publication.read_text())
    candidate = Path(previous["candidate"])
    release = candidate / "release"
    daily = release / "daily/continuous_daily.parquet"
    rules = release / "rules/rules.parquet"
    daily_manifest = release / "daily/manifest.json"
    rules_manifest = release / "rules/manifest.json"
    files = (receipt, publication, final_settlement, daily, rules, daily_manifest, rules_manifest)
    pins = {str(path): sha256_file(path) for path in files}
    if (previous.get("contract") != "prior_exchange_candidate_publication_readback_v1"
            or previous["source_release"]["source_sha256"] != identity["source_sha256"]
            or previous["daily_sha256"] != pins[str(daily)]
            or previous["rules_sha256"] != pins[str(rules)]
            or previous["daily_manifest_sha256"] != pins[str(daily_manifest)]
            or previous["rule_manifest_sha256"] != pins[str(rules_manifest)]
            or previous["final_settlement_sha256"] != pins[str(final_settlement)]):
        raise ValueError("published release no longer matches its recorded source and output pins")
    manifest = json.loads(daily_manifest.read_text())
    if (manifest.get("status") != "complete" or manifest.get("materialization_version") != 3
            or manifest.get("prior_exchange_liquidity_contract") != "prior_exchange_session_liquidity_v1"
            or manifest.get("research_only") is not True
            or manifest.get("financial_point_in_time_verified") is not False
            or manifest.get("all_products_training_ready") is not False):
        raise ValueError("release is not the declared bounded prior-exchange research candidate")
    print(json.dumps({"stage": "frozen_code_and_release_pins_verified"}), flush=True)
    rules_frame, rule_proof = validate_margin_rule_source(rules, daily)
    frame = pl.read_parquet(daily)
    validate_accounting_continuation(frame, rules_frame)
    terms, rights = load_margin_terminal_components(rules, rule_proof)
    corporate_margin_base_values(frame, rules_frame, final_settlement,
        terminal_component_terms=terms, terminal_subscription_values=rights)
    if (frame.height != manifest["rows"] or rules_frame.height != frame.height
            or "previous_exchange_session_date" not in frame.columns):
        raise ValueError("daily/rule counts or corrected liquidity axis differ")
    if any(sha256_file(Path(path)) != digest for path, digest in pins.items()):
        raise ValueError("release or code receipt changed during the frozen readback")
    verify_source_release(receipt, code)
    proof = {
        "contract": "prior_exchange_frozen_publication_readback_v1",
        "completed_at_utc": datetime.now(UTC).isoformat(),
        "source_release": identity, "imported_functions_from_verified_release": loaded,
        "validated_rows": frame.height, "validated_products": frame["product"].n_unique(),
        "daily_rule_and_accounting_continuation_verified": True,
        "terminal_and_corporate_margin_values_verified": True,
        "research_only": True, "financial_point_in_time_verified": False,
        "source_coverage_complete": False, "end_to_end_training_verified": False,
        "input_pins": pins, "script_sha256": sha256_file(Path(__file__)),
    }
    atomic_write_json(output, proof)
    print(json.dumps({"stage": "frozen_release_readback_complete", "rows": frame.height,
                      "products": proof["validated_products"], "output": str(output)}), flush=True)
    return proof


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("receipt", "publication", "final-settlement", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    args = parser.parse_args()
    readback(*(getattr(args, name.replace("-", "_")).resolve()
               for name in ("receipt", "publication", "final-settlement", "output")))


if __name__ == "__main__":
    main()
