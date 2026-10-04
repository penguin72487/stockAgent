"""Bounded, read-only provider views of the existing data-monitor snapshot.

``provider`` is the registry's source-owner label, not necessarily one legal
entity: some labels describe a composite source or a credential gate.  Keep
that distinction visible instead of inventing a second provider registry.
"""

from __future__ import annotations

from collections import Counter
from typing import Any, Mapping


MARKET_ORDER = {name: rank for rank, name in enumerate((
    "taiwan_equity", "taiwan_derivatives", "taiwan_public", "global_equity",
    "forex", "macro", "cross_market", "configuration", "crypto",
))}
OPERATION_ORDER = {name: rank for rank, name in enumerate((
    "catching_up", "streaming", "waiting_publication", "complete", "unable", "deferred", "control", "reference",
))}


SOURCE_KEYS = (
    "id", "endpoint_id", "title", "detail", "scope", "registry_alias",
    "in_active_scope", "market_category", "market_category_label", "granularity",
    "operation_state", "operation_label", "operation_reason", "execution_state",
    "status", "status_label", "update_owner", "cadence", "data_through",
    "last_verified_at_utc", "warnings", "detail_link", "sort_index",
)
NESTED_KEYS = {
    "record_stats": ("state", "count", "first", "last", "files_total",
                     "files_inspected", "invalid_files", "verified_partial_count", "basis"),
    "acquisition_progress": ("state", "label", "current", "total", "unit", "ratio",
                             "basis", "preparing_for_date", "evidence_coverage"),
    "eta": ("state", "remaining_seconds", "estimated_complete_at_utc", "basis"),
    "automation": ("schedule_state", "schedule_label", "next_run_at_utc",
                   "next_run_basis", "job_running", "stream_window"),
    "publication": ("schedule_label", "detected_at_utc", "observed_at_utc",
                    "applied_at_utc", "basis", "exact_time_declared"),
    "freshness": ("age_seconds", "state", "basis"),
}


def project_provider_detail(
    snapshot: Mapping[str, Any], provider: str, *, offset: int = 0, limit: int = 30,
    search: str = "", operation: str = "all", market: str = "all",
) -> dict[str, Any] | None:
    """Return one owner-label page without exposing unrelated source rows."""
    all_rows = snapshot.get("sources")
    if not isinstance(all_rows, list):
        raise ValueError("data-monitor source registry missing")
    rows = [row for row in all_rows if isinstance(row, Mapping)
            and str(row.get("provider") or "其他") == provider]
    if not rows:
        return None
    active = [row for row in rows if row.get("in_active_scope") is True]
    states = Counter(str(row.get("operation_state") or "unable") for row in rows)
    active_states = Counter(str(row.get("operation_state") or "unable") for row in active)
    inventory = Counter(str((row.get("record_stats") or {}).get("state") or "unverified")
                        for row in rows)
    categories = Counter(str(row.get("market_category_label") or "其他") for row in rows)
    market_options = {str(row.get("market_category") or "cross_market"):
                      str(row.get("market_category_label") or "其他") for row in rows}
    aliases = sum(row.get("registry_alias") is True for row in rows)
    summarized = next((item for item in snapshot.get("provider_summaries", [])
                       if isinstance(item, Mapping) and item.get("provider") == provider), {})
    needle = search.casefold()
    matched = [row for row in rows if
               (operation == "all" or row.get("operation_state") == operation)
               and (market == "all" or row.get("market_category") == market)
               and (not needle or any(needle in str(row.get(key) or "").casefold()
                                      for key in ("title", "id", "endpoint_id", "update_owner", "detail")))]
    matched.sort(key=lambda row: (
        MARKET_ORDER.get(str(row.get("market_category")), 99),
        OPERATION_ORDER.get(str(row.get("operation_state")), 99),
        str(row.get("title") or row.get("id") or "").casefold(),
        str(row.get("id") or ""),
    ))
    page = matched[offset:offset + limit]
    projected = []
    for row in page:
        item = {key: row[key] for key in SOURCE_KEYS if key in row}
        for key, keys in NESTED_KEYS.items():
            value = row.get(key)
            if isinstance(value, Mapping):
                item[key] = {part: value[part] for part in keys if part in value}
        projected.append(item)
    result = {
        "schema_version": 1,
        "generated_at_utc": snapshot.get("generated_at_utc"),
        "read_only": True,
        "production_control_possible": False,
        "provider": provider,
        "summary": {
            "registered": len(rows),
            "active_endpoints": len(active),
            "registry_aliases": aliases,
            "operation_state_counts": dict(states),
            "active_operation_state_counts": dict(active_states),
            "inventory_state_counts": dict(inventory),
            "market_categories": dict(categories),
            "market_category_options": market_options,
            "source_status": summarized.get("status"),
            "active_current_ratio": (
                (active_states["complete"] + active_states["streaming"]) / len(active)
                if active else None
            ),
            "ratio_basis": "已完成或串流中的主動端點／主動端點；非歷史筆數完整率",
        },
        "page": {"offset": offset, "limit": limit, "matched_total": len(matched),
                 "has_more": offset + len(page) < len(matched)},
        "sources": projected,
    }
    if provider == "FinLab" and isinstance(snapshot.get("finlab_acquisition"), Mapping):
        acquisition = snapshot["finlab_acquisition"]
        result["finlab_acquisition"] = {
            key: acquisition[key] for key in (
                "state", "downloaded", "catalog_total", "ratio", "not_downloaded",
                "not_downloaded_by_reason", "quota_remaining_mb", "quota_limit_mb",
                "quota_observed_at_utc", "next_run_at_utc", "last_receipt_at_utc",
            ) if key in acquisition
        }
    return result
