"""Receipt-backed FinLab collection modes and research-priority inventory.

No SDK, credential loading, raw-data reads or scheduler mutation. Importance
is a reversible *view* preference; the canonical collector retains its market,
missing-history, ageing, retry and residual-quota ordering.
"""
from __future__ import annotations

from collections import Counter
from statistics import fmean


INVENTORY_CONTRACT_VERSION = 1
BATCH_ETA_MEASUREMENT_VERSION = 2
WINDOWED_FAMILIES = frozenset({"tw_tick", "tw_minute", "tw_futures_minute"})
GROUPS = {
    "prices": (1, "行情與交易資格"),
    "fundamentals": (2, "營收與財報"),
    "flows": (3, "籌碼、信用與借券"),
    "derivatives": (4, "期貨、選擇權與保證金"),
    "macro": (5, "總經與跨市場"),
    "funds": (6, "ETF與可轉債"),
    "events": (7, "企業事件、治理與新聞"),
    "other": (8, "其他特徵"),
    "labels": (9, "來源標籤"),
    "intraday": (10, "逐筆與合成分鐘（剩餘配額）"),
}
PRICE_CATEGORIES = frozenset({
    "price", "rotc_price", "reference_price", "taiex_total_index", "stock_index_price",
    "security_categories", "company_basic_info", "trading_halt", "trading_attention",
    "disposal_information", "delayed_open_close", "delisting_announcements",
    "delisted_companies", "delisted_companies_tse", "delisted_companies_otc",
    "security_termination_events", "margin_short_sale_suspension", "day_trade_short_suspension",
})
FLOW_CATEGORIES = frozenset({
    "broker_transactions", "rotc_broker_transactions", "inventory", "etl",
    "foreign_investors_shareholding", "institutional_investors_trading_summary",
    "institutional_investors_trading_all_market_summary", "qfii_top_and_industry",
    "margin_balance", "margin_transactions", "security_lending", "security_lending_sell",
    "short_sell_value", "sellable_lending_quota", "block_trade", "intraday_trading",
    "intraday_trading_stat", "market_transaction_info", "ceil_floor_unfilled",
})


def estimate_attempt_batch(durations: list[float], *, limit: int,
                           since_completion_seconds: float) -> dict:
    """Remaining wall time from completed end-to-end attempt cycles.

    Costs add, so use an arithmetic mean, not the fast-path median. The last
    64 cycles include quota checks, candidate scanning, validation, persistence
    and periodic checkpoints, while excluding one-off pre-batch bootstrap.
    Elapsed time in the current cycle is subtracted; an in-flight attempt is
    not counted as completed. This never predicts unpublished source supply.
    """
    samples = durations[-64:]
    average = fmean(samples) if len(samples) >= 3 else None
    remaining = max(0, limit - len(durations))
    return {
        "eta_measurement_contract_version": BATCH_ETA_MEASUREMENT_VERSION,
        "eta_measurement_basis": "rolling_mean_end_to_end_attempt_cycle",
        "sample_count": len(durations), "sample_window_count": len(samples),
        "completed_attempts": len(durations), "seconds_per_attempt_estimate": average,
        "remaining_batch_seconds_estimate": max(
            0.0, average * remaining - max(0.0, since_completion_seconds)
        ) if average is not None else None,
    }


def feature_group(key: str, *, record_unit: str | None = None) -> dict:
    """Classify every key, including new or unrecognised catalogue additions."""
    category = key.split(":", 1)[0]
    if category in WINDOWED_FAMILIES:
        group = "intraday"
    elif record_unit == "metadata_values":
        group = "labels"
    elif category in PRICE_CATEGORIES:
        group = "prices"
    elif category.startswith(("financial_statement", "monthly_revenue", "rotc_monthly_revenue")) or category in {
            "fundamental_features", "price_earning_ratio", "financial_statements_upload_detail",
            "industry_eps_summary", "operating_analysis_summary"}:
        group = "fundamentals"
    elif category.startswith(("tw_taifex", "futures_", "tw_option")) or category == "single_stock_futures_and_equity_options_underlying":
        group = "derivatives"
    elif category.startswith(("tw_business", "tw_total_", "tw_industry_", "tw_monetary", "tw_gov_bond")) or category in {
            "world_index", "stock_index_vol", "benchmark_return", "national_security_fund"}:
        group = "macro"
    elif category in FLOW_CATEGORIES or category.startswith(("margin_", "tw_credit", "internal_equity", "intraday_", "after_market_")):
        group = "flows"
    elif category.startswith(("tw_etf", "tw_active_etf", "tw_tpex_etf", "cb_", "etf_", "fund_dividend")):
        group = "funds"
    elif category.startswith(("dividend", "director", "shareholder", "management_", "capital_", "par_value", "tw_news")) or category in {
            "important_info_announcement", "board_dividend_announcement", "treasury_stock",
            "investors_conference", "lawsuit_info", "insider_shareholding_transfer_declaration"}:
        group = "events"
    else:
        group = "other"
    priority, label = GROUPS[group]
    return {"feature_group": group, "feature_group_label": label, "importance": priority}


def collection_mode(row: dict, *, workload_current: bool) -> str:
    if row["key"].split(":", 1)[0] in WINDOWED_FAMILIES:
        return "windowed_backfill"
    work = row.get("workload") or {}
    if not workload_current:
        return "unverified"
    # Existing immutable provider objects, not a count of catalogue names,
    # establish a baseline. This is not per-symbol/PIT completeness.
    if work.get("downloaded") is True and not row.get("history_scope_warning"):
        return "incremental"
    if work.get("downloaded") is True:
        return "incremental_with_history_review"
    if work.get("blocked_reason") or row.get("state") in {
            "provider_empty", "vip_only", "authentication_failed"}:
        return "source_wait"
    return "history_backfill"


def annotate_inventory(datasets: list[dict], *, workload_current: bool) -> dict:
    counts = Counter()
    grouped: dict[str, dict] = {}
    for row in datasets:
        row.update(feature_group(row["key"], record_unit=(row.get("workload") or {}).get("record_unit")))
        row["collection_mode"] = collection_mode(row, workload_current=workload_current)
        if row["collection_mode"] == "windowed_backfill":
            continue  # exemplar catalogue keys are not a market denominator
        counts[row["collection_mode"]] += 1
        group = grouped.setdefault(row["feature_group"], {
            "id": row["feature_group"], "label": row["feature_group_label"],
            "importance": row["importance"], "total_keys": 0, "incremental_keys": 0,
            "backfill_keys": 0, "source_wait_keys": 0, "due_keys": 0, "unverified_keys": 0,
        })
        group["total_keys"] += 1
        if row["collection_mode"].startswith("incremental"):
            group["incremental_keys"] += 1
            group["due_keys"] += (row.get("workload") or {}).get("needs_refresh") is True
        elif row["collection_mode"] == "history_backfill":
            group["backfill_keys"] += 1
        elif row["collection_mode"] == "source_wait":
            group["source_wait_keys"] += 1
        else:
            group["unverified_keys"] += 1
    return {
        "contract_version": INVENTORY_CONTRACT_VERSION,
        "state": "available" if workload_current else "unverified",
        "general_total": sum(counts.values()), "mode_counts": dict(counts),
        "groups": sorted(grouped.values(), key=lambda item: item["importance"]),
        "basis": "完整來源物件已取得後使用既有SDK增量追新；不是逐股票歷史缺口、PIT或每個資料格完整的證明。",
        "sort_scope": "view_only_canonical_scheduler_unchanged",
    }
