#!/usr/bin/env python3
"""Classify the 2014+ TW public feature ABI using a measured table inventory.

An observed non-null cell is never by itself a historical publication proof.
This report separates the existing strict PIT model selection from the user's
research acceptance policy, which also allows current revised values with an
official or explicitly inferred publication date.
"""

from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from stockagent.config import load_config
from stockagent.data.tw_public_features import FEATURE_COLUMNS


SNAPSHOT_FIRST = FEATURE_COLUMNS.index("twpub_tdcc_retail_holder_ratio")
SNAPSHOT_LAST = FEATURE_COLUMNS.index("twpub_short_sale_available_log")
SNAPSHOT_FEATURES = frozenset(FEATURE_COLUMNS[SNAPSHOT_FIRST: SNAPSHOT_LAST + 1])


def classify(name: str, count: int, selected: set[str]) -> tuple[str, str]:
    if name in selected:
        return "selected_original_pit", "Original value; release/session clock and source lineage are required by the strict 2014 audit."
    if count == 0:
        return "all_null", "No finite cell exists in the measured canonical table; cannot train this field."
    if name == "twpub_official_trading_volume_raw":
        return "redundant_raw", "The base training panel already selects trading_volume_raw; do not double-weight the same volume family."
    if name == "twpub_dividend_per_share_raw":
        return "partial_market_raw", "The observed original daily source covers TPEx but not the full TWSE universe; historical TWSE original values need another source."
    if name in SNAPSHOT_FEATURES:
        return "snapshot_no_old_vintage", "Current/forward snapshots begin in 2026; their old values and historical publication versions cannot be inferred from today's response."
    if name.startswith("twpub_taifex_"):
        return "late_taifex_capture", "Local TAIFEX OpenAPI captures are 2025/2026-only and do not cover the 2014-2026 training horizon."
    if name.startswith("twpub_mof_"):
        return "bulk_no_original_values", "Current revised bulk values are first-observed in 2026; original MOF release PDF values and complete index history are not yet parsed and verified."
    if name in {
        "twpub_usdtwd_raw", "twpub_usdtwd_log", "twpub_cbc_overnight_rate",
        "twpub_cbc_m1b_raw", "twpub_cbc_m1b_log", "twpub_cbc_m2_raw",
        "twpub_cbc_m2_log", "twpub_dgbas_cpi_log", "twpub_dgbas_gdp_log",
    }:
        return "bulk_no_original_values", "Historical bulk levels exist, but older original release values are not proved; only first-observed 2026 cells enter the strict table."
    return "derived_or_duplicate_not_selected", "A transform, ratio, or duplicate of another source; excluded by the original-value training request, not evidence that its source has no history."


def research_acceptance(name: str, count: int, decision: str) -> tuple[str, str, str]:
    """Value and date evidence are separate; this is a policy, not a PIT claim."""

    if count == 0:
        return "unavailable", "unavailable", "no"
    if decision == "selected_original_pit":
        return "original_release_or_official_daily", "original_or_official_date", "yes"
    if decision in {"snapshot_no_old_vintage", "late_taifex_capture"}:
        return "current_captured_version", "first_observed_at_only", "yes_from_first_observation"
    if decision == "bulk_no_original_values":
        return "current_revised_version", "official_announcement_else_inferred", "yes_after_date_mapping"
    if decision == "partial_market_raw":
        return "official_source_partial_market", "official_source_date", "yes_for_covered_market"
    if decision == "redundant_raw":
        return "official_source_duplicate", "official_source_date", "no_duplicate_input"
    return "derived_or_duplicate", "underlying_source_date", "no_raw_only_request"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inventory-dir", required=True, type=Path)
    parser.add_argument("--config", default="configs/markets/tw_public_preopen_raw_2014_v1.yaml")
    args = parser.parse_args()
    config = load_config(args.config)
    selected = set(config.data.feature_include)
    with (args.inventory_dir / "feature_inventory.csv").open(newline="", encoding="utf-8") as handle:
        inventory = list(csv.DictReader(handle))
    annual: dict[str, dict[int, int]] = defaultdict(dict)
    with (args.inventory_dir / "annual_feature_inventory.csv").open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            annual[row["feature"]][int(row["year"])] = int(row["raw_non_null"])
    if [row["feature"] for row in inventory] != list(FEATURE_COLUMNS):
        raise RuntimeError("inventory feature ABI differs from current code; rebuild the inventory")
    output = args.inventory_dir / "2014_training_feature_decisions.csv"
    fieldnames = ["feature", "decision", "reason", "research_value_basis",
                  "research_publication_basis", "research_acceptance", "first", "last", "event_cells_total",
                  "event_cells_2014_2026", "years_with_event_2014_2026", "missing_event_years_2014_2025"]
    with output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in inventory:
            name = row["feature"]
            count = int(row["count"] or 0)
            status, reason = classify(name, count, selected)
            value_basis, publication_basis, acceptance = research_acceptance(name, count, status)
            yearly = annual[name]
            writer.writerow({
                "feature": name, "decision": status, "reason": reason,
                "research_value_basis": value_basis,
                "research_publication_basis": publication_basis,
                "research_acceptance": acceptance,
                "first": row["first"], "last": row["last"],
                "event_cells_total": count,
                "event_cells_2014_2026": sum(yearly.get(y, 0) for y in range(2014, 2027)),
                "years_with_event_2014_2026": sum(yearly.get(y, 0) > 0 for y in range(2014, 2027)),
                "missing_event_years_2014_2025": ";".join(
                    str(y) for y in range(2014, 2026) if not yearly.get(y, 0)
                ),
            })
    print(output)


if __name__ == "__main__":
    main()
