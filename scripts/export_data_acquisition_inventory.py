#!/usr/bin/env python3
"""Export every registered monitor source without contacting any provider.

The monitor snapshot is the existing source registry projection. This export
does not claim a source is complete merely because a job or file exists.
"""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import csv
import json
from pathlib import Path
import sys
from typing import Any, Mapping


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.acquisition_policy import (
    ACQUISITION_OWNERS, FACT_IDENTITY, evaluate_secondary_admission,
    source_admission_class, source_blocker,
)
from downloader.artifact_io import atomic_write_json

DEFAULT_SNAPSHOT = ROOT / "artifacts/live/data_monitor/public_status.json"
FIELDS = (
    "id", "endpoint_id", "registry_alias", "provider", "title", "market_category", "scope",
    "in_active_scope", "acquisition_enabled", "automation_eligible", "publishable",
    "acquisition_role", "work_class", "acquisition_blocker", "quota_scope",
    "dedup_identity", "dedup_evidence", "acquisition_owner_endpoints",
    "dispatch_order", "validation_policy", "validation_admission",
    "update_owner", "cadence", "operation_state", "status", "status_label",
    "coverage_current", "coverage_total", "coverage_unit", "record_count",
    "first_observed", "last_observed", "record_evidence", "source_link",
    "snapshot_at_utc",
)


def quota_scope(source: Mapping[str, Any]) -> str:
    """Routing labels, not invented limits or an alternative limiter registry."""
    provider = str(source.get("provider") or "")
    name = str(source.get("id") or "")
    if provider.startswith("FinLab") or name.startswith("finlab:"):
        return "finlab_account_daily_bytes; observed_provider_reset"
    if provider.startswith("FinMind") or name.startswith("finmind:"):
        return "finmind-v4-data; shared_token_hourly_and_provider_cooldown"
    if "Shioaji" in provider:
        return "shioaji_person_connections_quote_requests_daily_bytes"
    if provider.startswith("OpenBB") or name.startswith("openbb:"):
        return "underlying_provider_bucket; OpenBB_is_not_independent_quota"
    if provider == "Binance":
        return "binance_IP_request_weight_and_endpoint_bucket"
    if provider == "OKX":
        return "okx_endpoint_IP_or_IP_instrument_bucket"
    if provider == "Bybit":
        return "bybit_IP_plus_endpoint_response_limits"
    return "existing_provider_named_limiter; numeric_limit_not_inferred"


def validation_admission(source: Mapping[str, Any]) -> str:
    name = str(source.get("id") or "")
    if name.startswith("finmind:sponsor:"):
        return "enforced_in_Sponsor_next_for_priority8_API_only"
    if name.startswith("finlab:"):
        return "enforced_in_FinLab_sync_for_secondary_price_refresh_only"
    return "not_wired_as_optional_validation; independent_required_acquisition"


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
            "id": source_id, "endpoint_id": source.get("endpoint_id"),
            "registry_alias": source.get("registry_alias"),
            "provider": source.get("provider"), "title": source.get("title"),
            "market_category": source.get("market_category"), "scope": source.get("scope"),
            "in_active_scope": source.get("in_active_scope"),
            "acquisition_enabled": source.get("acquisition_enabled"),
            "automation_eligible": source.get("automation_eligible"),
            "publishable": source.get("publishable"),
            "acquisition_role": acquisition_role(source), "update_owner": source.get("update_owner"),
            "work_class": source_admission_class(source),
            "acquisition_blocker": source_blocker(source),
            "quota_scope": quota_scope(source), "dedup_identity": FACT_IDENTITY,
            "dedup_evidence": "explicit_ownership_only; same_name_is_not_equivalence_proof",
            "acquisition_owner_endpoints": "|".join(ACQUISITION_OWNERS.get(source_id, ())),
            "dispatch_order": "live_and_due_incremental_reserved > missing_history > optional_tick > API_validation",
            "validation_policy": "all_required_acquisition_gate + local_queue + shared_surplus; local_file_audit_no_API",
            "validation_admission": validation_admission(source),
            "cadence": source.get("cadence"), "operation_state": source.get("operation_state"),
            "status": source.get("status"), "status_label": source.get("status_label"),
            "coverage_current": coverage.get("current"), "coverage_total": coverage.get("total"),
            "coverage_unit": coverage.get("unit"), "record_count": records.get("count"),
            "first_observed": records.get("first"), "last_observed": records.get("last"),
            "record_evidence": records.get("state"), "source_link": source.get("detail_link"),
            "snapshot_at_utc": snapshot.get("generated_at_utc"),
        })
    return rows


def policy_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    providers: dict[str, Counter] = defaultdict(Counter)
    for row in rows:
        counts = providers[str(row.get("provider") or "unclassified")]
        counts["registered_rows"] += 1
        counts[str(row["work_class"])] += 1
        if row.get("acquisition_blocker"):
            counts["required_endpoints_not_ready"] += 1
    return {
        "snapshot_at_utc": rows[0].get("snapshot_at_utc") if rows else None,
        "registered_rows": len(rows),
        "providers": {key: dict(value) for key, value in sorted(providers.items())},
        "interpretation": "Registry rows are not task/symbol/field counts. Unreconciled sources are not deduplicated or declared complete.",
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", type=Path, default=DEFAULT_SNAPSHOT)
    parser.add_argument("--output", type=Path, help="Write CSV here instead of stdout")
    parser.add_argument("--policy-output", type=Path,
                        help="Optional local provider/admission summary JSON; no provider API")
    args = parser.parse_args(argv)
    snapshot = json.loads(args.snapshot.read_text(encoding="utf-8"))
    rows = rows_from_snapshot(snapshot)
    if args.policy_output:
        summary = policy_summary(rows)
        summary["api_validation_admission"] = evaluate_secondary_admission(snapshot_path=args.snapshot)
        atomic_write_json(args.policy_output, summary)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with args.output.open("w", encoding="utf-8", newline="") as target:
            writer = csv.DictWriter(target, fieldnames=FIELDS, lineterminator="\n")
            writer.writeheader()
            writer.writerows(rows)
    else:
        writer = csv.DictWriter(sys.stdout, fieldnames=FIELDS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
