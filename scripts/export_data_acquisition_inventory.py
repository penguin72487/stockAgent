#!/usr/bin/env python3
"""Export every registered monitor source without contacting any provider.

The monitor snapshot is the existing source registry projection. This export
does not claim a source is complete merely because a job or file exists.
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
import sys
from typing import Any, Mapping


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SNAPSHOT = ROOT / "artifacts/live/data_monitor/public_status.json"
FIELDS = (
    "id", "provider", "title", "market_category", "scope", "acquisition_role",
    "update_owner", "cadence", "operation_state", "status", "status_label",
    "coverage_current", "coverage_total", "coverage_unit", "record_count",
    "first_observed", "last_observed", "record_evidence", "source_link",
    "snapshot_at_utc",
)


def acquisition_role(source: Mapping[str, Any]) -> str:
    """Only classify source authority where this repo has an explicit rule."""
    name = str(source.get("id") or "")
    provider = str(source.get("provider") or "")
    if name == "finmind:sponsor:TaiwanStockInstitutionalInvestorsBuySellWide":
        return "derived_from_finmind_long_no_api"
    if name == "finmind:sponsor:TaiwanStockPrice":
        return "early_history_and_gap_fill_then_validation"
    if name == "finlab:price:收盤價":
        return "first_acquisition_then_surplus_validation"
    if provider.startswith(("TWSE", "TPEx", "MOPS", "TAIFEX", "TDCC")):
        return "official_primary_for_own_fields"
    if provider == "永豐 Shioaji":
        return "broker_primary_for_own_grain"
    if provider == "FinLab":
        return "independent_research_source_unreconciled"
    if provider == "FinMind":
        return "independent_source_or_gap_fill_unreconciled"
    return "independent_source_unreconciled"


def rows_from_snapshot(snapshot: Mapping[str, Any]) -> list[dict[str, Any]]:
    sources = snapshot.get("sources")
    if not isinstance(sources, list):
        raise ValueError("monitor snapshot has no source registry")
    seen: set[str] = set()
    rows: list[dict[str, Any]] = []
    for source in sources:
        if not isinstance(source, Mapping) or not isinstance(source.get("id"), str):
            raise ValueError("invalid source registry item")
        source_id = source["id"]
        if source_id in seen:
            raise ValueError(f"duplicate source id: {source_id}")
        seen.add(source_id)
        coverage = source.get("coverage") if isinstance(source.get("coverage"), Mapping) else {}
        records = source.get("record_stats") if isinstance(source.get("record_stats"), Mapping) else {}
        rows.append({
            "id": source_id, "provider": source.get("provider"), "title": source.get("title"),
            "market_category": source.get("market_category"), "scope": source.get("scope"),
            "acquisition_role": acquisition_role(source), "update_owner": source.get("update_owner"),
            "cadence": source.get("cadence"), "operation_state": source.get("operation_state"),
            "status": source.get("status"), "status_label": source.get("status_label"),
            "coverage_current": coverage.get("current"), "coverage_total": coverage.get("total"),
            "coverage_unit": coverage.get("unit"), "record_count": records.get("count"),
            "first_observed": records.get("first"), "last_observed": records.get("last"),
            "record_evidence": records.get("state"), "source_link": source.get("detail_link"),
            "snapshot_at_utc": snapshot.get("generated_at_utc"),
        })
    return rows


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", type=Path, default=DEFAULT_SNAPSHOT)
    args = parser.parse_args(argv)
    snapshot = json.loads(args.snapshot.read_text(encoding="utf-8"))
    rows = rows_from_snapshot(snapshot)
    writer = csv.DictWriter(sys.stdout, fieldnames=FIELDS, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
