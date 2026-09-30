"""Semantic admission for a curated 09:00 TW equity feature dataset.

This is an allowlist, not a correlation/importance screen. Raw provider fields
are never admitted just because they are numeric or have non-null observations.
Hashes and clocks remain in receipts, never in the model's numeric channels.
"""
from __future__ import annotations

import re

CONTRACT_VERSION = 3

CATEGORY_LABELS = {
    "price_technical": "行情與技術形態",
    "liquidity": "成交量與流動性",
    "microstructure": "委託簿、逐筆成交與微結構",
    "valuation": "估值與殖利率",
    "institutional": "法人買賣與籌碼",
    "margin_lending": "融資融券與借券",
    "shareholding": "股權與持股分布",
    "fundamentals": "財報與營收",
    "corporate_events": "公司事件與股利",
    "company_profile": "公司屬性、治理與掛牌生命週期",
    "macro": "總經、景氣與政策",
    "fx_rates": "匯率、利率與債券",
    "derivatives": "期貨、選擇權與部位",
    "funds": "基金與 ETF",
    "cross_market": "海外市場與商品",
    "crypto": "加密資產與鏈上資料",
    "housing": "房屋、不動產與住宅統計（排除）",
    "execution_rules": "交易規則與標籤（非模型輸入）",
    "metadata": "識別、來源與管理資訊（非模型輸入）",
    "unclassified": "待辨識來源語意",
}

TECHNICAL_FEATURES = (
    "open_logret_1d", "max_logret_1d", "min_logret_1d", "close_logret_1d",
    "trading_volume_logret_1d", "signed_vol", "body_ratio", "signed_body_ratio",
    "delta_body_ratio", "clv", "clv_centered", "delta_clv", "upper_shadow",
    "lower_shadow", "shadow_imbalance",
)
DAILY_PUBLIC_FEATURES = (
    "twpub_official_close_logret_1d", "twpub_official_trading_volume_raw",
    "twpub_official_trading_value_raw", "twpub_official_trades_raw",
    "twpub_official_turnover_ratio", "twpub_official_intraday_range",
    "twpub_official_close_to_high", "twpub_official_close_to_low",
    "twpub_pe_raw", "twpub_pb_raw", "twpub_dividend_yield_pct_raw",
    "twpub_twse_taiex_raw",
)
CHIP_FEATURES = (
    "twpub_margin_balance_lots_raw", "twpub_margin_balance_chg",
    "twpub_margin_buy_flow", "twpub_margin_sell_flow", "twpub_short_balance_lots_raw",
    "twpub_short_balance_chg", "twpub_short_buy_flow", "twpub_short_sell_flow",
    "twpub_foreign_net_buy_flow", "twpub_investment_trust_net_buy_flow",
    "twpub_dealer_net_buy_flow", "twpub_institutional_net_buy_flow",
)
RELEASE_FEATURES = (
    "twpub_cbc_fx_reserves_log", "twpub_cbc_fx_reserves_chg",
    "twpub_cbc_m1b_yoy_pct_raw", "twpub_cbc_m2_yoy_pct_raw",
    "twpub_dgbas_cpi_yoy_pct_raw", "twpub_dgbas_unemployment_pct_raw",
    "twpub_dgbas_gdp_yoy_pct_raw",
)
VALUE_FEATURES = TECHNICAL_FEATURES + DAILY_PUBLIC_FEATURES + CHIP_FEATURES + RELEASE_FEATURES
SHIFT_FEATURES = TECHNICAL_FEATURES + DAILY_PUBLIC_FEATURES
LEGACY_POSITIVE_ONLY_LOG_FEATURES = frozenset({
    "twpub_official_trading_volume_log", "twpub_official_trading_value_log",
    "twpub_official_trades_log", "twpub_margin_balance_log", "twpub_short_balance_log",
})

# These names have a publication/range defect, irrespective of storage dtype.
SNAPSHOT_PREFIXES = (
    "twpub_monthly_revenue_", "twpub_cumulative_revenue_yoy", "twpub_financial_",
    "twpub_insider_", "twpub_borrow_", "twpub_sbl_", "twpub_short_sale_available_",
    "twpub_tdcc_", "twpub_company_",
)
HOUSING_CENSUS = frozenset({"hv", "mhs", "mhs2", "resconst", "ressales", "vip"})
HOUSING = re.compile(
    r"housing|house[_ -]?prices?|home[_ -]?(?:sales|prices|ownership)|mortgage|"
    r"real[_ -]?estate|residential|building[_ -]?permits?|"
    r"(?:^|[_ :])(?:houst|permit|mspus|csushpinsa|rent)(?:$|[_ :])|"
    r"房屋|房價|住宅|不動產|房地產|房貸|租金|實價登錄|地價|土地移轉|建物|建築",
    re.IGNORECASE,
)
METADATA = re.compile(
    r"(?:^|[_ :])(?:sha\d*|md5|hash|checksum|path|url|uri|filename|file_name|"
    r"receipt|downloaded|retrieved|fetched|ingested|payload|archive|document|"
    r"source|provider|schema|request|query|error|warning|issue|row_index|"
    r"table_index|table_title|page|status|address|email|fax|phone|chairman|"
    r"accounting_firm|spokesperson|underwriter|published|observed|retrieved|"
    r"available_at|strict_available|context_ref|dimensions_json|decimals)(?:$|[_ :])|"
    r"資料來源|下載|網址|檔案|備註|錯誤|註記|說明|名稱|姓名|住址|地址|電話|傳真|發言人",
    re.IGNORECASE,
)
IDENTIFIER = re.compile(
    r"(?:^|[_ :])(?:id|code|symbol|name|date|time|timestamp|datetime|year|month|"
    r"quarter|period|unit|currency|country|market|exchange|type|season|category|"
    r"contract|ticker|index|rank)(?:$|[_ :])|"
    r"^(?:Date|Year|Season|CompanyName|SecuritiesCompanyCode|LineNumber|TimePeriod|SeriesCode)$|"
    r"日期|年月|年度|季別|代號|編號|持股分級|市場別|證券類別|公司簡稱|股票簡稱",
    re.IGNORECASE,
)
EXECUTION_NAMES = frozenset({
    "adjustment_reference_price", "raw_ohlc_scale_factor", "return_1d",
    "adj_close", "adjusted_close", "tradable", "alive_mask", "force_exit_mask",
    "can_buy_mask", "can_sell_mask", "can_short_open_mask", "target", "label",
    "next_session_open_gap_logret",
})


def housing_reason(row: dict) -> str:
    provider = str(row.get("provider", "")).lower()
    dataset = str(row.get("dataset_id", ""))
    field = str(row.get("field", ""))
    if provider == "moi":
        return "user_excluded_moi_real_estate_transactions"
    if provider == "census" and dataset.rsplit(":", 1)[-1] in HOUSING_CENSUS:
        return "user_excluded_census_housing_or_mixed_construction"
    if HOUSING.search(dataset + " " + field):
        return "user_excluded_housing_related_measure"
    return ""


def is_execution_field(field: str) -> bool:
    return field.startswith(("_twpub_", "target_", "label_", "future_", "execution_", "exit_")) or field.lower() in EXECUTION_NAMES


def category_for(dataset: str, field: str, provider: str = "") -> str:
    if housing_reason({"provider": provider, "dataset_id": dataset, "field": field}):
        return "housing"
    if is_execution_field(field):
        return "execution_rules"
    text = (dataset + " " + field).lower()
    if field in TECHNICAL_FEATURES:
        return "liquidity" if field in {"signed_vol", "trading_volume_logret_1d"} else "price_technical"
    if field.startswith("twpub_official_"):
        return "liquidity" if any(x in field for x in ("volume", "value", "trades", "turnover")) else "price_technical"
    if any(x in text for x in ("binance", "bybit", "okx", "crypto", "bitcoin", "ethereum", "defi", "dune", "stablecoin", "onchain", "coinmetrics")):
        return "crypto"
    if any(x in text for x in ("microstructure", "bidask", "bid_depth", "ask_depth", "book_imbalance", "odd_lot", "ceil_floor_unfilled", "揭示買", "揭示賣")):
        return "microstructure"
    if any(x in text for x in ("tw-minute-train", "kbar", "ticks", "quote")):
        return "price_technical"
    if any(x in text for x in ("taifex", "futures", "options", "futopt", "tw-futures", "tw-options", "cftc", "open_interest", "期貨", "選擇權")):
        return "derivatives"
    if any(x in text for x in ("positions_", "pct_of_oi", "_traders_", "traders_", "change_in_", "commercial_", "reportable_", "concentration_")):
        return "derivatives"
    if re.search(r"t187ap(?:06|07)_", text):
        return "fundamentals"
    if re.search(r"t187ap(?:08|09|10|11|12|13)_", text):
        return "shareholding"
    if any(x in text for x in ("tdcc", "shareholding", "holder", "insider", "集保", "持股", "股權", "大股東")):
        return "shareholding"
    if any(x in text for x in ("margin", "lending", "borrow", "sbl_", "loancollateral", "securedloan", "short_balance", "short_buy", "short_sell", "short_sale", "融資", "融券", "借券")):
        return "margin_lending"
    if any(x in text for x in ("institution", "3insti", "qfii", "foreign_net_buy", "trust_net_buy", "dealer_net_buy", "broker_transactions", "法人", "買賣超", "外資", "投信", "自營商")):
        return "institutional"
    if any(x in text for x in ("pe_raw", "pe_log", "pb_raw", "pb_log", "valuation", "dividend_yield", "本益比", "股價淨值比", "殖利率")):
        return "valuation"
    if any(x in text for x in ("financial", "xbrl", "fundamental", "revenue", "mopsfin", "營收", "財報", "財務", "資產", "負債", "損益", "現金流")):
        return "fundamentals"
    if any(x in text for x in ("dividend", "exdiv", "exright", "entitlement", "material", "attention", "disposal", "conference", "capital_reduction", "股利", "除權", "除息", "注意", "處置", "重大訊息", "減資")):
        return "corporate_events"
    if any(x in text for x in ("company", "companies", "listed", "listing", "governance", "board_election", "chairman", "compensation", "director", "suspension", "change_transaction", "basic_info", "董", "監察", "產業", "治理")):
        return "company_profile"
    if any(x in text for x in ("etf", "fund", "基金")):
        return "funds"
    if any(x in text for x in ("fx_reserve", "m1b", "m2_", "cpi", "gdp", "unemployment", "dgbas", "mof_", "fred", "public_economic", "pmi", "business_indicator", "總經", "景氣", "貨幣", "物價", "失業")):
        return "macro"
    if any(x in text for x in ("exchange_rate", "exchangerate", "forex", "usdtwd", "interest_rate", "interestrate", "governmentbond", "convertiblebond", "cb_", "treasury", "overnight", "匯率", "利率", "公債", "債券")):
        return "fx_rates"
    if any(x in text for x in ("crudeoil", "goldprice", "commodity", "commodities", "usstock", "europestock", "japanstock", "ukstock", "yahoo", "黃金", "原油")):
        return "cross_market"
    if any(x in text for x in ("price", "ohlcv", "taiex", "stock-features", "stockprice", "收盤", "開盤", "最高", "最低", "成交價", "股價", "指數")):
        return "price_technical"
    if any(x in text for x in ("volume", "turnover", "trading_value", "trades", "成交量", "成交股數", "成交金額", "成交筆數")):
        return "liquidity"
    if "openbb" in text:
        return "cross_market"
    return "unclassified"


def classify_candidate(row: dict) -> dict:
    """Classify every source field; output contains no provenance hashes/paths."""
    field, dataset = str(row["field"]), str(row["dataset_id"])
    provider = str(row.get("provider", ""))
    category = category_for(dataset, field, provider)
    housing = housing_reason(row)
    semantic_field = field.rsplit(":", 1)[-1] if provider == "FinLab" else field
    # Source APIs mix snake_case, CamelCase and CJK labels.
    semantic_field = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", semantic_field)
    old = row.get("admission", "")
    if housing:
        decision, reason = "excluded_housing", housing
    elif field == "next_session_open_gap_logret":
        decision, reason = "separate_open_quote_feature", "valid_after_open_quote_only_in_phase_aware_abi_not_this_preopen_matrix"
    elif is_execution_field(field):
        decision, reason = "execution_only", "keep_for_execution_or_labels_never_in_feature_matrix"
    elif METADATA.search(semantic_field) or semantic_field.startswith("_"):
        decision, reason, category = "excluded_metadata", "provenance_or_operational_metadata_not_a_signal", "metadata"
    elif IDENTIFIER.search(semantic_field):
        decision, reason = "excluded_identifier", "entity_time_unit_or_categorical_key_requires_explicit_encoding"
        if category == "unclassified":
            category = "metadata"
    elif old in {"missing_values", "upstream_all_null"}:
        decision, reason = "excluded_all_null", "no_verified_observed_value_in_selected_source"
    elif old in {"incomplete_source", "legacy_derived_table_needs_scoped_rebuild"}:
        decision, reason = "quarantined_quality", "source_or_derived_table_not_currently_validated"
    elif field.startswith("twpub_tdcc_"):
        decision, reason = "quarantined_quality", "tdcc_aggregate_range_and_tier_semantics_require_repair"
    elif field in LEGACY_POSITIVE_ONLY_LOG_FEATURES:
        decision, reason = "excluded_lossy_transform", "positive_only_log1p_discards_valid_zero_use_canonical_raw_measure"
    elif field in VALUE_FEATURES and dataset == "physical:tw-public:training-features":
        decision, reason = "canonical_feature_selected", "use_pinned_audited_canonical_table_not_raw_catalog_values"
    elif field in VALUE_FEATURES:
        decision, reason = "excluded_duplicate_representation", "same_named_signal_uses_canonical_equity_source_not_futures_projection"
    elif provider == "FinLab" or old == "research_only_needs_adapter" or field.startswith(("twfl_", "twpub_xbrl_", *SNAPSHOT_PREFIXES)):
        decision, reason = "quarantined_pit", "historical_publication_time_or_original_vintage_unverified"
    elif row.get("role") == "key_or_provenance":
        decision, reason = "excluded_identifier", "catalog_identifies_non_signal_key_or_unit"
    else:
        # A numeric string is not meaningless. Keep it in the worklist rather
        # than pretending that a generic cast proves its units/clock/entity.
        decision, reason = "quarantined_adapter", "needs_source_specific_entity_unit_publication_and_numeric_adapter"
    return {
        "catalog_id": row["catalog_id"], "provider": provider, "dataset_id": dataset,
        "field": field, "category": category, "category_zh": CATEGORY_LABELS[category],
        "decision": decision, "included_as_raw_column": False,
        "selected_canonical_feature": field if decision == "canonical_feature_selected" else "",
        "reason": reason, "previous_admission": old,
    }


def validate_feature_names(names: list[str]) -> None:
    expected = list(VALUE_FEATURES) + [f + "__available" for f in VALUE_FEATURES]
    if names != expected:
        raise ValueError("curated feature ABI/order mismatch; refusing extra, missing or duplicate inputs")
    for name in VALUE_FEATURES:
        if housing_reason({"field": name}) or is_execution_field(name) or METADATA.search(name):
            raise ValueError(f"forbidden curated input: {name}")


def feature_clock(name: str) -> str:
    if name in SHIFT_FEATURES:
        return "previous_completed_exchange_session; canonical_panel_shift_once"
    if name in CHIP_FEATURES:
        return "post_close_source_mapped_to_next_exchange_session; no_second_panel_shift"
    if name in RELEASE_FEATURES:
        return "verified_original_release_available_by_0900; carry_released_state_only"
    raise ValueError(f"feature has no clock contract: {name}")
