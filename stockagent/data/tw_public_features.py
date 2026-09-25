from __future__ import annotations

from dataclasses import dataclass, field
from contextvars import ContextVar
from datetime import date, timedelta
import hashlib
import json
import os
from pathlib import Path
import time
from typing import Callable, Iterable

import polars as pl

from stockagent.data.tw_delisting_rules import (
    classify_delisting_notice,
    split_announcement_symbols,
)


DEFAULT_MARKET_SYMBOL = "__MARKET__"
DAY_TRADE_REGIME_START = date(2014, 1, 6)
DAY_TRADE_SELL_FIRST_START = date(2014, 6, 30)
DAY_TRADE_SUSPENSION_COLUMN = "暫停現股賣出後現款買進當沖註記"
# Both official balance products report margin balances/limits in board lots
# (張/交易單位).  The standard listed/OTC board lot is 1,000 shares, so convert
# the exchange-wide next-session headroom before exposing it to an exact-share
# execution ledger.  Unknown source schemas never reach this conversion.
TW_MARGIN_BOARD_LOT_SHARES = 1_000.0

STOCK_FEATURE_COLUMNS = (
    "twpub_official_trading_volume_raw",
    "twpub_official_trading_value_raw",
    "twpub_official_trades_raw",
    "twpub_pe_raw",
    "twpub_pb_raw",
    "twpub_dividend_yield_pct_raw",
    "twpub_dividend_per_share_raw",
    "twpub_margin_balance_lots_raw",
    "twpub_short_balance_lots_raw",
    "twpub_margin_buy_lots_raw",
    "twpub_margin_sell_lots_raw",
    "twpub_short_sell_lots_raw",
    "twpub_short_buy_lots_raw",
    "twpub_foreign_net_buy_shares_raw",
    "twpub_investment_trust_net_buy_shares_raw",
    "twpub_dealer_net_buy_shares_raw",
    "twpub_institutional_net_buy_shares_raw",
    "twpub_official_close_logret_1d",
    "twpub_official_trading_volume_log",
    "twpub_official_trading_value_log",
    "twpub_official_trades_log",
    "twpub_official_turnover_ratio",
    "twpub_official_intraday_range",
    "twpub_official_close_to_high",
    "twpub_official_close_to_low",
    "twpub_pe_log",
    "twpub_pb_log",
    "twpub_dividend_yield",
    "twpub_dividend_per_share_log",
    "twpub_margin_balance_log",
    "twpub_margin_balance_chg",
    "twpub_short_balance_log",
    "twpub_short_balance_chg",
    "twpub_margin_buy_flow",
    "twpub_margin_sell_flow",
    "twpub_short_sell_flow",
    "twpub_short_buy_flow",
    "twpub_foreign_net_buy_flow",
    "twpub_investment_trust_net_buy_flow",
    "twpub_dealer_net_buy_flow",
    "twpub_institutional_net_buy_flow",
    "twpub_tdcc_retail_holder_ratio",
    "twpub_tdcc_large_holder_ratio",
    "twpub_tdcc_holder_count_log",
    "twpub_company_paidin_capital_log",
    "twpub_company_issued_shares_log",
    "twpub_company_private_shares_log",
    "twpub_company_age_years",
    "twpub_company_listed_age_years",
    "twpub_company_par_value_log",
    "twpub_company_industry_code",
    "twpub_company_is_foreign",
    "twpub_company_has_preferred_stock",
    "twpub_dividend_cash_per_share",
    "twpub_dividend_stock_per_share",
    "twpub_dividend_total_cash_log",
    "twpub_dividend_total_stock_log",
    "twpub_dividend_confirmed",
    "twpub_dividend_board_approved",
    "twpub_exdiv_known",
    "twpub_exdiv_cash_dividend",
    "twpub_exdiv_stock_dividend_ratio",
    "twpub_exdiv_subscription_ratio",
    "twpub_exdiv_subscription_price_log",
    "twpub_material_event_count_log",
    "twpub_material_clause_log",
    "twpub_material_fact_lag_days",
    "twpub_attention_count_log",
    "twpub_attention_close_log",
    "twpub_attention_pe_log",
    "twpub_disposal_count_log",
    "twpub_monthly_revenue_log",
    "twpub_monthly_revenue_mom",
    "twpub_monthly_revenue_yoy",
    "twpub_cumulative_revenue_yoy",
    "twpub_financial_eps",
    "twpub_financial_revenue_log",
    "twpub_financial_net_income_asinh",
    "twpub_financial_gross_margin",
    "twpub_financial_operating_margin",
    "twpub_financial_net_margin",
    "twpub_financial_assets_log",
    "twpub_financial_debt_ratio",
    "twpub_financial_current_ratio",
    "twpub_financial_equity_ratio",
    "twpub_financial_book_value_per_share_log",
    "twpub_insider_holdings_log",
    "twpub_insider_pledge_ratio",
    "twpub_insider_transfer_shares_log",
    "twpub_borrow_available_log",
    "twpub_sbl_balance_log",
    "twpub_short_sale_available_log",
)

MARKET_FEATURE_COLUMNS = (
    "twpub_twse_taiex_raw",
    "twpub_usdtwd_raw",
    "twpub_cbc_fx_reserves_usd_billion_raw",
    "twpub_cbc_m1b_raw",
    "twpub_cbc_m1b_yoy_pct_raw",
    "twpub_cbc_m2_raw",
    "twpub_cbc_m2_yoy_pct_raw",
    "twpub_dgbas_cpi_yoy_pct_raw",
    "twpub_dgbas_unemployment_pct_raw",
    "twpub_dgbas_gdp_yoy_pct_raw",
    "twpub_twse_taiex_log",
    "twpub_twse_taiex_logret_1d",
    "twpub_twse_taiex_pct",
    "twpub_usdtwd_log",
    "twpub_usdtwd_logret_1d",
    "twpub_cbc_overnight_rate",
    "twpub_cbc_overnight_rate_chg",
    "twpub_cbc_fx_reserves_log",
    "twpub_cbc_fx_reserves_chg",
    "twpub_cbc_m1b_log",
    "twpub_cbc_m1b_yoy",
    "twpub_cbc_m2_log",
    "twpub_cbc_m2_yoy",
    "twpub_dgbas_cpi_log",
    "twpub_dgbas_cpi_yoy",
    "twpub_dgbas_unemployment_rate",
    "twpub_dgbas_gdp_log",
    "twpub_dgbas_gdp_yoy",
    "twpub_mof_export_log",
    "twpub_mof_import_log",
    "twpub_mof_trade_balance_asinh",
    "twpub_mof_tax_total_log",
    "twpub_mof_securities_tax_log",
    "twpub_mof_futures_tax_log",
    "twpub_mof_business_tax_log",
    "twpub_taifex_tx_volume_log",
    "twpub_taifex_tx_open_interest_log",
    "twpub_taifex_tx_settlement_logret_1d",
    "twpub_taifex_txo_call_volume_log",
    "twpub_taifex_txo_put_volume_log",
    "twpub_taifex_txo_put_call_volume_ratio",
    "twpub_taifex_txo_call_oi_log",
    "twpub_taifex_txo_put_oi_log",
    "twpub_taifex_txo_put_call_oi_ratio",
    "twpub_taifex_dealer_net_oi_asinh",
    "twpub_taifex_foreign_net_oi_asinh",
    "twpub_taifex_trust_net_oi_asinh",
    "twpub_taifex_tx_top5_long_ratio",
    "twpub_taifex_tx_top10_long_ratio",
    "twpub_taifex_tx_large_oi_log",
    "twpub_taifex_tx_final_settlement_logret",
)

FEATURE_COLUMNS = (*STOCK_FEATURE_COLUMNS, *MARKET_FEATURE_COLUMNS)
POST_CLOSE_CHIP_FEATURE_COLUMNS = (
    "twpub_margin_balance_lots_raw",
    "twpub_short_balance_lots_raw",
    "twpub_margin_buy_lots_raw",
    "twpub_margin_sell_lots_raw",
    "twpub_short_sell_lots_raw",
    "twpub_short_buy_lots_raw",
    "twpub_foreign_net_buy_shares_raw",
    "twpub_investment_trust_net_buy_shares_raw",
    "twpub_dealer_net_buy_shares_raw",
    "twpub_institutional_net_buy_shares_raw",
    "twpub_margin_balance_log",
    "twpub_margin_balance_chg",
    "twpub_short_balance_log",
    "twpub_short_balance_chg",
    "twpub_margin_buy_flow",
    "twpub_margin_sell_flow",
    "twpub_short_sell_flow",
    "twpub_short_buy_flow",
    "twpub_foreign_net_buy_flow",
    "twpub_investment_trust_net_buy_flow",
    "twpub_dealer_net_buy_flow",
    "twpub_institutional_net_buy_flow",
)
RULE_COLUMNS = (
    "_twpub_official_traded",
    "_twpub_delisted",
    "_twpub_tpex_next_limit_up_ret",
    "_twpub_tpex_next_limit_down_ret",
    "_twpub_dividend_year",
    "_twpub_attention_flag",
    "_twpub_disposal_flag",
    "_twpub_short_open_ban",
    "_twpub_margin_short_evidence_next_session",
    "_twpub_short_capacity_shares_next_session",
    "_twpub_force_short_cover",
    "_twpub_force_cover_lead_sessions",
    "_twpub_force_cover_anchor_ordinal",
    "_twpub_force_cover_cancel_ordinal",
    "_twpub_trading_halt",
    "_twpub_day_trade_eligible",
    "_twpub_day_trade_short_open",
)
OUTPUT_COLUMNS = (*FEATURE_COLUMNS, *RULE_COLUMNS)
KEY_COLUMNS = ("date", "symbol")
TW_PUBLIC_FEATURE_AVAILABILITY_CONTRACT_VERSION = 10
AVAILABILITY_POLICY = {
    "historical_daily": "official session/trading date with source-specific publication timing",
    "post_close_chip_daily": (
        "official margin and institutional values for session t are published after "
        "the close and mapped to the next source-verified exchange session before model use"
    ),
    "tdcc_shareholding": "first observed response time at the 09:00 Asia/Taipei session boundary unless a verified release receipt is available",
    "monthly_macro": "period end is not publication; unverified historical release clocks require a separate vintage/calendar receipt",
    "quarterly_macro": "period end is not publication; unverified historical release clocks require a separate vintage/calendar receipt",
    "dgbas_official_release": "original document clocks take precedence; date-only 2012-05-07..2017-06-30 CPI, unemployment and GDP advance estimates use the official 08:30 schedule; other date-only releases enter the next verified session",
    "cbc_official_fx_release": "dated original foreign-reserve press-release values enter the first verified exchange session after the date-only posting; current bulk values remain separately first-observed",
    "snapshot_openapi": "announcement timestamp where supplied; otherwise response completion at the 09:00 Asia/Taipei session boundary",
    "future_event_snapshot": "response completion clock and explicit future effective date",
    "delisting_short_rules": "explicit effective date not before publication; otherwise next calendar day after notice",
    "margin_short_capacity": (
        "official end-of-session margin-short balance and next-business-day "
        "limit; shifted exactly one exchange session by the panel before use"
    ),
    "day_trade_eligibility": (
        "official TWSE/TPEx exchange-session list for the exact trade date; "
        "missing membership and unknown sell-first markers fail closed"
    ),
}


@dataclass(slots=True)
class TwPublicFeatureBuildResult:
    output_path: Path
    rows: int
    feature_count: int
    stock_rows: int
    market_rows: int
    market_symbol: str
    source_files: list[str]
    source_receipts: list[dict[str, str | int]]
    output_receipt: dict[str, str | int]
    symbol_universe_receipt: dict[str, str | int | bool]
    requested_end_date: str | None = None
    allow_daily_publication_lag: bool = False
    build_mode: str = "full"
    incremental_start_date: str | None = None
    reused_rows: int = 0
    incremental_fallback_reason: str | None = None
    stage_elapsed_seconds: dict[str, float] = field(default_factory=dict)


# Completed-session publication only changes a short daily tail, but the raw
# TWSE/TPEx archives contain roughly ten million rows.  Keep the filter scoped
# to date-normalized append/overlap datasets; event/snapshot sources are still
# read in full so an old announcement whose effective range reaches the live
# tail cannot disappear.  ContextVar keeps direct helper calls and concurrent
# test workers isolated.
_INCREMENTAL_READ_RANGE: ContextVar[tuple[date, date] | None] = ContextVar(
    "tw_public_incremental_read_range",
    default=None,
)
_BUILD_SESSION_DATES: ContextVar[tuple[Path, pl.DataFrame] | None] = ContextVar(
    "tw_public_build_session_dates", default=None
)
_INCREMENTAL_DATE_DATASETS = frozenset(
    {
        "twse_daily_ohlcv",
        "tpex_daily_ohlcv",
        "twse_daily_valuation",
        "tpex_daily_valuation",
        "twse_margin_balance",
        "tpex_margin_balance",
        "twse_institutional_trades",
        "tpex_institutional_trades",
        "twse_market_index",
        "twse_taiex_ohlc",
        "twse_day_trade_eligibility",
        "tpex_day_trade_eligibility",
    }
)

# These five exact source files are read only by the market-row TAIFEX
# builders below. They neither define stock features nor the verified equity
# session calendar. Other market-looking sources may be shared with stock
# builders or availability rules and remain on the full-rebuild path.
_MARKET_ONLY_SOURCE_NAMES = frozenset({
    "taifex_daily_futures.parquet",
    "taifex_daily_options.parquet",
    "taifex_institutional_total.parquet",
    "taifex_large_trader_futures_oi.parquet",
    "taifex_final_settlement_price.parquet",
})


def _file_content_receipt(path: Path) -> dict[str, str | int]:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return {
        "name": path.name,
        "size": int(path.stat().st_size),
        "sha256": digest.hexdigest(),
    }


def _source_content_receipts(input_dir: Path) -> list[dict[str, str | int]]:
    # These are raw-archive indexes, not inputs to the current feature ABI.
    # Keep their bytes in the cold release without forcing a full 9.5M-row
    # feature rebuild whenever an unrelated GCIS/FSC CSV is captured.
    background_catalogs = {
        "gcis_open_data_catalog.parquet",
        "fsc_open_data_catalog.parquet",
    }
    receipts = [
        _release_feature_content_receipt(path)
        if path.name in {
            "dgbas_release_vintages.parquet",
            "cbc_fx_reserve_release_vintages.parquet",
            "cbc_money_release_vintages.parquet",
        }
        else _file_content_receipt(path)
        for path in sorted(input_dir.glob("*.parquet"))
        if path.name not in background_catalogs
    ]
    supplemental = input_dir / "supplemental/cbc_overnight_official_pages.parquet"
    if supplemental.is_file():
        # The acceptance gate resolves receipt names relative to input_dir.
        # Keeping only the basename made a valid supplemental source appear
        # missing at input_dir/cbc_overnight_official_pages.parquet and caused
        # a full feature rebuild on every 08:30 gate retry, including 09:00.
        receipt = _file_content_receipt(supplemental)
        receipt["name"] = supplemental.relative_to(input_dir).as_posix()
        receipts.append(receipt)
    return receipts


def _release_feature_content_receipt(path: Path) -> dict[str, str | int]:
    """Hash exact model inputs, not a release page's cosmetic HTML revision.

    Raw page/attachment hashes remain in the archive and its independent
    integrity audit. A PDF value, publication clock, source identity, or
    evidence-eligibility change *must* still invalidate the feature table.
    """

    frame = pl.read_parquet(path)
    if path.name == "dgbas_release_vintages.parquet":
        columns = (
            "source", "period", "published_on", "release_id", "release_kind",
            "metric", "value_pct", "value_evidence", "published_time_precision",
            "published_clock_taipei", "html_sha256",
        )
        ordering = ("source", "release_id")
    elif path.name == "cbc_money_release_vintages.parquet":
        columns = ("period", "published_on", "release_url", "metric",
                   "value_pct", "value_evidence", "html_sha256")
        ordering = ("period", "published_on", "release_url", "metric")
    else:
        columns = ("period", "published_on", "metric", "value", "value_evidence", "html_sha256")
        ordering = ("period", "published_on", "metric", "value")
    if not set(columns) <= set(frame.columns):
        # Unknown historical schema: never accidentally ignore a revision.
        return _file_content_receipt(path)
    semantic = frame.select(
        [pl.col(name) for name in columns if name != "html_sha256"]
        + [pl.col("html_sha256").is_not_null().alias("has_original_html")]
    ).sort(list(ordering))
    payload = json.dumps(
        semantic.to_dicts(), ensure_ascii=False, sort_keys=True,
        separators=(",", ":"), allow_nan=False,
    ).encode("utf-8")
    return {
        "name": path.name,
        "size": len(payload),
        "sha256": hashlib.sha256(payload).hexdigest(),
        "basis": "feature_semantics_v1",
    }


def _symbol_universe_receipt(symbols_root: str | Path | None) -> dict[str, str | int | bool]:
    if symbols_root is None or str(symbols_root).strip() == "":
        names: list[str] = []
        available = False
    else:
        root = Path(symbols_root)
        available = root.exists()
        names = sorted(path.name for path in root.glob("*_features.parquet")) if available else []
    payload = "\n".join(names).encode("utf-8")
    return {
        "available": available,
        "file_count": len(names),
        "names_sha256": hashlib.sha256(payload).hexdigest(),
    }


def _stable_file_identity(path: Path) -> tuple[int, int, int, int]:
    stat = path.stat()
    return (
        int(stat.st_ino),
        int(stat.st_size),
        int(stat.st_mtime_ns),
        int(stat.st_ctime_ns),
    )


def _incremental_base_is_compatible(
    output_path: Path,
    summary_path: Path,
    *,
    market_symbol: str,
    source_receipts: list[dict[str, str | int]],
    symbol_universe_receipt: dict[str, str | int | bool],
    allow_daily_publication_lag: bool = False,
    allowed_changed_source_names: frozenset[str] = frozenset(),
) -> bool:
    """Reuse a prefix only when its complete dependency bytes are unchanged.

    A rewritten source parquet may correct an arbitrarily old observation or
    effective-date rule. Without a source-partition receipt proving the changed
    rows are confined to the tail, reusing any historical prefix is unsafe.
    A caller may separately allow a precisely named market-only dependency
    change while reusing stock rows, but every other source and the entire old
    output receipt must still match exactly.
    """

    try:
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        schema = pl.read_parquet_schema(output_path)
    except (OSError, UnicodeError, json.JSONDecodeError):
        return False
    if not isinstance(summary, dict):
        return False
    if list(schema) != [*KEY_COLUMNS, *OUTPUT_COLUMNS]:
        return False
    if summary.get("feature_columns") != list(FEATURE_COLUMNS):
        return False
    if summary.get("rule_columns") != list(RULE_COLUMNS):
        return False
    if summary.get("market_symbol") != market_symbol:
        return False
    previous_receipts = summary.get("source_receipts")
    if allowed_changed_source_names:
        if not isinstance(previous_receipts, list):
            return False
        previous_by_name = {
            item.get("name"): item for item in previous_receipts
            if isinstance(item, dict) and isinstance(item.get("name"), str)
        }
        current_by_name = {item["name"]: item for item in source_receipts}
        if (
            len(previous_by_name) != len(previous_receipts)
            or previous_by_name.keys() != current_by_name.keys()
        ):
            return False
        changed = {
            name for name in current_by_name
            if current_by_name[name] != previous_by_name[name]
        }
        if not changed or not changed <= allowed_changed_source_names:
            return False
    elif previous_receipts != source_receipts:
        return False
    if summary.get("symbol_universe_receipt") != symbol_universe_receipt:
        return False
    if summary.get("allow_daily_publication_lag") is not allow_daily_publication_lag:
        return False
    if (
        int(summary.get("availability_contract_version") or -1)
        != TW_PUBLIC_FEATURE_AVAILABILITY_CONTRACT_VERSION
    ):
        return False
    expected = summary.get("output_receipt")
    if not isinstance(expected, dict):
        return False
    try:
        return expected == _file_content_receipt(output_path)
    except OSError:
        return False


def build_tw_public_training_features(
    input_dir: str | Path = "data_tw_public",
    output_path: str | Path = "data_tw_public/features/tw_public_stock_daily.parquet",
    *,
    symbols_root: str | Path | None = "data_yahoo/tw_stocks",
    market_symbol: str = DEFAULT_MARKET_SYMBOL,
    summary_path: str | Path | None = None,
    end_date: date | None = None,
    allow_daily_publication_lag: bool = False,
    incremental_start_date: date | None = None,
    incremental_context_days: int = 45,
) -> TwPublicFeatureBuildResult:
    input_dir = Path(input_dir)
    output_path = Path(output_path)
    resolved_summary_path = Path(
        summary_path or output_path.with_suffix(".summary.json")
    )
    if incremental_context_days < 1:
        raise ValueError("incremental_context_days must be positive")
    if incremental_start_date is not None and end_date is None:
        raise ValueError("incremental_start_date requires end_date")
    if (
        incremental_start_date is not None
        and end_date is not None
        and incremental_start_date > end_date
    ):
        raise ValueError("incremental_start_date must not be after end_date")

    build_started = time.perf_counter()
    source_receipts = _source_content_receipts(input_dir)
    symbol_universe_receipt = _symbol_universe_receipt(symbols_root)
    base_compatible = bool(
        output_path.is_file()
        and _incremental_base_is_compatible(
            output_path,
            resolved_summary_path,
            market_symbol=market_symbol,
            source_receipts=source_receipts,
            symbol_universe_receipt=symbol_universe_receipt,
            allow_daily_publication_lag=allow_daily_publication_lag,
        )
    )
    market_only_summary: dict[str, object] = {}
    if (
        not base_compatible
        and output_path.is_file()
        and end_date is not None
        and _incremental_base_is_compatible(
            output_path,
            resolved_summary_path,
            market_symbol=market_symbol,
            source_receipts=source_receipts,
            symbol_universe_receipt=symbol_universe_receipt,
            allow_daily_publication_lag=allow_daily_publication_lag,
            allowed_changed_source_names=_MARKET_ONLY_SOURCE_NAMES,
        )
    ):
        market_only_summary = json.loads(resolved_summary_path.read_text(encoding="utf-8"))
        if market_only_summary.get("requested_end_date") != end_date.isoformat():
            market_only_summary = {}
    use_market_only = bool(market_only_summary)
    source_proof_done = time.perf_counter()
    if base_compatible:
        summary = json.loads(resolved_summary_path.read_text(encoding="utf-8"))
        if summary.get("requested_end_date") == (
            end_date.isoformat() if end_date is not None else None
        ):
            if source_receipts != _source_content_receipts(input_dir):
                raise RuntimeError("TW public source parquet changed while verifying unchanged features")
            if symbol_universe_receipt != _symbol_universe_receipt(symbols_root):
                raise RuntimeError("TW symbol universe changed while verifying unchanged features")
            # Repeated 08:30 acceptance retries must not rewrite the 1+ GiB
            # feature table when every input, semantic option and output hash
            # already verifies. A needless replace invalidates the opening
            # panel cache even though the model-visible values are identical.
            return TwPublicFeatureBuildResult(
                output_path=output_path,
                rows=int(summary["rows"]),
                feature_count=int(summary["feature_count"]),
                stock_rows=int(summary["stock_rows"]),
                market_rows=int(summary["market_rows"]),
                market_symbol=market_symbol,
                source_files=list(summary["source_files"]),
                source_receipts=source_receipts,
                output_receipt=dict(summary["output_receipt"]),
                symbol_universe_receipt=symbol_universe_receipt,
                requested_end_date=summary.get("requested_end_date"),
                allow_daily_publication_lag=allow_daily_publication_lag,
                build_mode="unchanged_verified",
                reused_rows=int(summary["rows"]),
                stage_elapsed_seconds={
                    "source_proof": round(source_proof_done - build_started, 3),
                    "total_before_summary": round(time.perf_counter() - build_started, 3),
                },
            )
    use_incremental = incremental_start_date is not None and base_compatible
    incremental_fallback_reason = (
        "base_contract_not_verified"
        if incremental_start_date is not None and not base_compatible and not use_market_only
        else None
    )
    existing_identity = (
        _stable_file_identity(output_path)
        if use_incremental or use_market_only else None
    )
    symbols = _load_symbol_filter(symbols_root)
    feature_stage_seconds: dict[str, float] = {}

    def timed_frame(
        name: str, builder: Callable[..., pl.DataFrame], *args: object, **kwargs: object
    ) -> pl.DataFrame:
        started = time.perf_counter()
        frame = builder(*args, **kwargs)
        feature_stage_seconds[name] = round(time.perf_counter() - started, 3)
        return frame

    read_token = None
    if use_incremental:
        assert incremental_start_date is not None
        assert end_date is not None
        read_token = _INCREMENTAL_READ_RANGE.set(
            (
                incremental_start_date
                - timedelta(days=int(incremental_context_days)),
                end_date,
            )
        )
    session_token = None
    try:
        exchange_sessions = _exchange_session_dates(input_dir)
        session_token = _BUILD_SESSION_DATES.set((input_dir.resolve(), exchange_sessions))
        if use_market_only:
            # These TAIFEX sources cannot affect stock rows: each is consumed
            # exclusively by its corresponding market-row builder below.
            # The old table and every other source were hash-verified above.
            stock_features = pl.DataFrame()
        else:
            stock_frames = [
                timed_frame("stock_official_ohlcv", _build_official_ohlcv_features, input_dir),
                timed_frame("stock_delisted_rules", _build_delisted_company_rules, input_dir),
                timed_frame("stock_valuation", _build_valuation_features, input_dir),
                timed_frame("stock_margin", _build_margin_features, input_dir),
                timed_frame("stock_institutional", _build_institutional_features, input_dir),
                timed_frame("stock_tdcc", _build_tdcc_features, input_dir),
                timed_frame("stock_company_basic", _build_company_basic_features, input_dir),
                timed_frame("stock_dividend", _build_dividend_features, input_dir),
                timed_frame("stock_ex_dividend_preview", _build_ex_dividend_preview_features, input_dir),
                timed_frame("stock_material_info", _build_material_info_features, input_dir),
                timed_frame("stock_attention_disposal", _build_attention_disposal_features, input_dir),
                timed_frame("stock_financial", _build_model_useful_financial_features, input_dir),
                timed_frame("stock_ownership", _build_model_useful_ownership_features, input_dir),
                timed_frame("stock_shorting", _build_model_useful_shorting_features, input_dir),
                timed_frame("stock_rule", _build_model_useful_rule_features, input_dir),
                timed_frame("stock_day_trade_rule", _build_day_trade_rule_features,
                    input_dir,
                    symbols=symbols,
                    end_date=end_date,
                    allow_missing_latest_session=allow_daily_publication_lag,
                ),
            ]
            # Every model-facing row must be keyed by an exchange session.
            stock_mapping_started = time.perf_counter()
            if not exchange_sessions.is_empty():
                stock_frames = [
                    _map_available_dates_to_sessions(frame, input_dir, sessions=exchange_sessions)
                    for frame in stock_frames
                ]
            feature_stage_seconds["stock_session_mapping"] = round(
                time.perf_counter() - stock_mapping_started, 3
            )
            if use_incremental:
                assert incremental_start_date is not None
                assert end_date is not None
                stock_frames = [
                    _slice_feature_frame(frame, incremental_start_date, end_date)
                    for frame in stock_frames
                ]
            stock_merge_started = time.perf_counter()
            stock_features = _merge_feature_frames(
                stock_frames, stage_seconds=feature_stage_seconds, stage_prefix="stock_join"
            )
            feature_stage_seconds["stock_merge"] = round(
                time.perf_counter() - stock_merge_started, 3
            )
            if symbols is not None and not stock_features.is_empty():
                stock_features = stock_features.filter(
                    pl.col("symbol").is_in(sorted(symbols))
                )
        stock_build_done = time.perf_counter()

        market_frames = [
                timed_frame("market_twse_index", _build_twse_market_index_features,
                    input_dir,
                    market_symbol=market_symbol,
                ),
                timed_frame("market_usdtwd", _build_usdtwd_features,
                    input_dir,
                    market_symbol=market_symbol,
                ),
                timed_frame("market_cbc_overnight", _build_cbc_overnight_rate_features,
                    input_dir,
                    market_symbol=market_symbol,
                ),
                timed_frame("market_cbc_monthly", _build_cbc_monthly_macro_features,
                    input_dir,
                    market_symbol=market_symbol,
                ),
                timed_frame("market_dgbas", _build_dgbas_macro_features,
                    input_dir,
                    market_symbol=market_symbol,
                ),
                timed_frame("market_mof", _build_mof_macro_features,
                    input_dir,
                    market_symbol=market_symbol,
                ),
                timed_frame("market_taifex_tx", _build_taifex_tx_features,
                    input_dir,
                    market_symbol=market_symbol,
                ),
                timed_frame("market_taifex_options", _build_taifex_options_features,
                    input_dir,
                    market_symbol=market_symbol,
                ),
                timed_frame("market_taifex_institutional", _build_taifex_institutional_features,
                    input_dir,
                    market_symbol=market_symbol,
                ),
                timed_frame("market_taifex_large_trader", _build_taifex_large_trader_features,
                    input_dir,
                    market_symbol=market_symbol,
                ),
                timed_frame("market_taifex_settlement", _build_taifex_final_settlement_features,
                    input_dir,
                    market_symbol=market_symbol,
                ),
            ]
        market_mapping_started = time.perf_counter()
        if not exchange_sessions.is_empty():
            market_frames = [
                _map_available_dates_to_sessions(frame, input_dir, sessions=exchange_sessions)
                for frame in market_frames
            ]
        feature_stage_seconds["market_session_mapping"] = round(
            time.perf_counter() - market_mapping_started, 3
        )
        if use_incremental:
            assert incremental_start_date is not None
            assert end_date is not None
            market_frames = [
                _slice_feature_frame(frame, incremental_start_date, end_date)
                for frame in market_frames
            ]
        market_merge_started = time.perf_counter()
        market_features = _merge_feature_frames(
            market_frames, stage_seconds=feature_stage_seconds, stage_prefix="market_join"
        )
        feature_stage_seconds["market_merge"] = round(
            time.perf_counter() - market_merge_started, 3
        )
        market_build_done = time.perf_counter()
    finally:
        if session_token is not None:
            _BUILD_SESSION_DATES.reset(session_token)
        if read_token is not None:
            _INCREMENTAL_READ_RANGE.reset(read_token)

    frames = [frame for frame in (stock_features, market_features) if not frame.is_empty()]
    if frames:
        output = pl.concat(frames, how="diagonal_relaxed")
        if end_date is not None:
            output = output.filter(pl.col("date") <= pl.lit(end_date))
        if use_incremental:
            assert incremental_start_date is not None
            output = output.filter(
                pl.col("date") >= pl.lit(incremental_start_date)
            )
        output = _ensure_feature_columns(output).sort(["date", "symbol"])
    else:
        output = pl.DataFrame(
            {
                "date": pl.Series([], dtype=pl.Date),
                "symbol": pl.Series([], dtype=pl.Utf8),
                **{name: pl.Series([], dtype=pl.Float64) for name in OUTPUT_COLUMNS},
            }
        )
    assembly_done = time.perf_counter()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_output = output_path.with_suffix(output_path.suffix + ".tmp")
    reused_rows = 0
    if use_market_only:
        if not output.is_empty() and output.filter(pl.col("symbol") != market_symbol).height:
            raise RuntimeError("market-only feature rebuild produced stock rows")
        stock_base = pl.scan_parquet(output_path).filter(
            pl.col("symbol") != market_symbol
        )
        stock_rows = int(stock_base.select(pl.len()).collect(engine="streaming").item())
        if stock_rows != int(market_only_summary["stock_rows"]):
            raise RuntimeError("market-only feature base stock row count changed")
        combined = pl.concat([stock_base, output.lazy()], how="vertical").sort(
            ["date", "symbol"]
        )
        combined.sink_parquet(
            temporary_output,
            compression="snappy",
            statistics=True,
            row_group_size=64_000,
            maintain_order=True,
            engine="streaming",
        )
        reused_rows = stock_rows
        market_rows = int(output.height)
        rows = stock_rows + market_rows
    elif use_incremental:
        assert incremental_start_date is not None
        prefix = pl.scan_parquet(output_path).filter(
            pl.col("date") < pl.lit(incremental_start_date)
        )
        prefix_counts = prefix.select(
            pl.len().alias("rows"),
            (pl.col("symbol") != market_symbol).sum().alias("stock_rows"),
            (pl.col("symbol") == market_symbol).sum().alias("market_rows"),
        ).collect(engine="streaming").row(0, named=True)
        reused_rows = int(prefix_counts["rows"] or 0)
        combined = pl.concat([prefix, output.lazy()], how="vertical")
        combined.sink_parquet(
            temporary_output,
            compression="snappy",
            statistics=True,
            row_group_size=64_000,
            maintain_order=True,
            engine="streaming",
        )
        stock_rows = int(prefix_counts["stock_rows"] or 0) + int(
            output.filter(pl.col("symbol") != market_symbol).height
        )
        market_rows = int(prefix_counts["market_rows"] or 0) + int(
            output.filter(pl.col("symbol") == market_symbol).height
        )
        rows = reused_rows + int(output.height)
    else:
        output.write_parquet(
            temporary_output,
            compression="snappy",
            statistics=True,
            row_group_size=64_000,
        )
        rows = int(output.height)
        stock_rows = (
            int(output.filter(pl.col("symbol") != market_symbol).height)
            if not output.is_empty()
            else 0
        )
        market_rows = (
            int(output.filter(pl.col("symbol") == market_symbol).height)
            if not output.is_empty()
            else 0
        )
    if source_receipts != _source_content_receipts(input_dir):
        temporary_output.unlink(missing_ok=True)
        raise RuntimeError("TW public source parquet changed while features were being built")
    if symbol_universe_receipt != _symbol_universe_receipt(symbols_root):
        temporary_output.unlink(missing_ok=True)
        raise RuntimeError("TW symbol universe changed while public features were being built")
    if (use_incremental or use_market_only) and existing_identity != _stable_file_identity(output_path):
        temporary_output.unlink(missing_ok=True)
        raise RuntimeError("TW public feature base changed during incremental build")
    os.replace(temporary_output, output_path)

    source_files = sorted(str(path) for path in input_dir.glob("*.parquet"))
    output_receipt = _file_content_receipt(output_path)
    committed_done = time.perf_counter()
    result = TwPublicFeatureBuildResult(
        output_path=output_path,
        rows=rows,
        feature_count=len(FEATURE_COLUMNS),
        stock_rows=stock_rows,
        market_rows=market_rows,
        market_symbol=market_symbol,
        source_files=source_files,
        source_receipts=source_receipts,
        output_receipt=output_receipt,
        symbol_universe_receipt=symbol_universe_receipt,
        requested_end_date=end_date.isoformat() if end_date is not None else None,
        allow_daily_publication_lag=allow_daily_publication_lag,
        build_mode=(
            "market_only_rebuild" if use_market_only
            else "incremental_tail" if use_incremental else "full"
        ),
        incremental_start_date=(
            incremental_start_date.isoformat() if use_incremental else None
        ),
        reused_rows=reused_rows,
        incremental_fallback_reason=incremental_fallback_reason,
        stage_elapsed_seconds={
            "source_proof": round(source_proof_done - build_started, 3),
            "stock_build": round(stock_build_done - source_proof_done, 3),
            "market_build": round(market_build_done - stock_build_done, 3),
            **feature_stage_seconds,
            "output_assembly": round(assembly_done - market_build_done, 3),
            "parquet_write_and_proof": round(committed_done - assembly_done, 3),
            "total_before_summary": round(committed_done - build_started, 3),
        },
    )
    _write_summary(resolved_summary_path, result)
    return result


def _write_summary(path: str | Path, result: TwPublicFeatureBuildResult) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "output_path": str(result.output_path),
        "rows": result.rows,
        "feature_count": result.feature_count,
        "stock_rows": result.stock_rows,
        "market_rows": result.market_rows,
        "source_files": result.source_files,
        "source_receipts": result.source_receipts,
        "output_receipt": result.output_receipt,
        "symbol_universe_receipt": result.symbol_universe_receipt,
        "feature_columns": list(FEATURE_COLUMNS),
        "rule_columns": list(RULE_COLUMNS),
        "market_symbol": result.market_symbol,
        "requested_end_date": result.requested_end_date,
        "allow_daily_publication_lag": result.allow_daily_publication_lag,
        "build_mode": result.build_mode,
        "incremental_start_date": result.incremental_start_date,
        "reused_rows": result.reused_rows,
        "incremental_fallback_reason": result.incremental_fallback_reason,
        "stage_elapsed_seconds": result.stage_elapsed_seconds,
        "availability_contract_version": TW_PUBLIC_FEATURE_AVAILABILITY_CONTRACT_VERSION,
        "availability_policy": AVAILABILITY_POLICY,
    }
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def _load_symbol_filter(symbols_root: str | Path | None) -> set[str] | None:
    if symbols_root is None or str(symbols_root).strip() == "":
        return None
    root = Path(symbols_root)
    if not root.exists():
        return None
    return {path.name.removesuffix("_features.parquet").upper() for path in root.glob("*_features.parquet")}


def _read_date_bounded_parquet(
    path: Path,
    *,
    dataset: str,
    columns: list[str] | None = None,
) -> pl.DataFrame:
    read_range = _INCREMENTAL_READ_RANGE.get()
    lazy = pl.scan_parquet(path)
    if read_range is not None and dataset in _INCREMENTAL_DATE_DATASETS:
        start, end = read_range
        schema = pl.read_parquet_schema(path)
        date_dtype = schema.get("date")
        if date_dtype == pl.String:
            predicate = pl.col("date").is_between(
                pl.lit(start.isoformat()),
                pl.lit(end.isoformat()),
                closed="both",
            )
        else:
            predicate = pl.col("date").cast(pl.Date, strict=False).is_between(
                start,
                end,
                closed="both",
            )
        lazy = lazy.filter(predicate)
    if columns is not None:
        lazy = lazy.select(columns)
    return lazy.collect(engine="streaming")


def _read_optional(input_dir: Path, name: str) -> pl.DataFrame:
    path = input_dir / f"{name}.parquet"
    if not path.exists():
        return pl.DataFrame()
    return _read_date_bounded_parquet(path, dataset=name)


def _exchange_session_dates(input_dir: Path) -> pl.DataFrame:
    """Historical index sessions plus a jointly published future rule session."""
    cached = _BUILD_SESSION_DATES.get()
    if cached is not None and cached[0] == input_dir.resolve():
        return cached[1]
    calendar = _read_optional(input_dir, "twse_taiex_ohlc")
    if not calendar.is_empty() and "date" in calendar.columns:
        sessions = calendar.select(_date_column_expr("date").alias("_session_date"))
    else:
        # Minimal/older official archives may have OHLCV before the separate
        # index table was collected. A nonempty official daily trading table
        # still proves that its exchange session occurred.
        for name in (
            "twse_daily_ohlcv", "tpex_daily_ohlcv",
            "twse_daily_valuation", "tpex_daily_valuation",
        ):
            path = input_dir / f"{name}.parquet"
            if path.exists():
                sessions = pl.scan_parquet(path).select(
                    _date_column_expr("date").alias("_session_date")
                ).collect()
                break
        else:
            return pl.DataFrame(schema={"_session_date": pl.Date})
    sessions = sessions.drop_nulls("_session_date").unique("_session_date")
    latest = sessions.get_column("_session_date").max()
    future: list[pl.DataFrame] = []
    for name in ("twse_day_trade_eligibility", "tpex_day_trade_eligibility"):
        path = input_dir / f"{name}.parquet"
        if not path.exists():
            break
        future.append(
            pl.scan_parquet(path)
            .select(_date_column_expr("date").alias("_session_date"))
            .filter(pl.col("_session_date") > latest)
            .drop_nulls("_session_date")
            .unique("_session_date")
            .collect()
        )
    if len(future) == 2:
        jointly_published = future[0].join(
            future[1], on="_session_date", how="inner", validate="1:1"
        )
        sessions = pl.concat([sessions, jointly_published])
    return sessions.unique("_session_date").sort("_session_date")


def _next_exchange_session_lookup(input_dir: Path) -> pl.DataFrame:
    """Map a completed session to the next source-verified exchange session."""

    sessions = _exchange_session_dates(input_dir).rename(
        {"_session_date": "_source_date"}
    )
    if sessions.height < 2:
        return pl.DataFrame(
            schema={"_source_date": pl.Date, "_available_date": pl.Date}
        )
    return (
        sessions.with_columns(
            pl.col("_source_date").shift(-1).alias("_available_date")
        )
        .drop_nulls("_available_date")
        .select("_source_date", "_available_date")
    )


def _map_available_dates_to_sessions(
    frame: pl.DataFrame, input_dir: Path, *, sessions: pl.DataFrame | None = None
) -> pl.DataFrame:
    """A Saturday or holiday release enters the first verified open session."""

    if frame.is_empty() or "date" not in frame.columns:
        return frame
    if sessions is None:
        sessions = _exchange_session_dates(input_dir)
    if sessions.is_empty():
        return frame.head(0)
    if frame.schema["date"] == pl.Date and frame.height >= 100_000:
        # Official daily panels already use verified session dates. Checking
        # their date column is linear; sorting and as-of joining every wide
        # stock row again is substantially more expensive. A holiday or null
        # date takes the original mapping path, so this never changes which
        # session owns an announcement.
        already_aligned = frame.select(
            pl.col("date")
            .is_in(sessions.get_column("_session_date"))
            .fill_null(False)
            .all()
        ).item()
        if already_aligned:
            return frame
    return (
        frame.drop_nulls("date").sort("date")
        .join_asof(
            sessions, left_on="date", right_on="_session_date", strategy="forward"
        )
        .filter(pl.col("_session_date").is_not_null())
        .drop("date")
        .rename({"_session_date": "date"})
    )


def _shift_post_close_features_to_next_session(
    frame: pl.DataFrame,
    session_lookup: pl.DataFrame,
) -> pl.DataFrame:
    """Move post-close model features without moving same-source rule evidence."""

    feature_columns = [
        name for name in POST_CLOSE_CHIP_FEATURE_COLUMNS if name in frame.columns
    ]
    if frame.is_empty() or session_lookup.is_empty() or not feature_columns:
        return pl.DataFrame()
    return (
        frame.select(
            [
                _date_column_expr("date").alias("_source_date"),
                _symbol_expr("symbol").alias("symbol"),
                *[pl.col(name) for name in feature_columns],
            ]
        )
        .join(session_lookup, on="_source_date", how="inner", validate="m:1")
        .select(
            [
                pl.col("_available_date").alias("date"),
                pl.col("symbol"),
                *[pl.col(name) for name in feature_columns],
            ]
        )
    )


def _slice_feature_frame(
    frame: pl.DataFrame,
    start: date,
    end: date,
) -> pl.DataFrame:
    if frame.is_empty() or "date" not in frame.columns:
        return frame
    return frame.filter(
        _date_column_expr("date").is_between(start, end, closed="both")
    )


def _merge_feature_frames(
    frames: Iterable[pl.DataFrame], *,
    stage_seconds: dict[str, float] | None = None,
    stage_prefix: str = "join",
) -> pl.DataFrame:
    cleaned: list[pl.DataFrame] = []
    for index, frame in enumerate(frames):
        if frame is None or frame.is_empty():
            continue
        prepared_at = time.perf_counter()
        prepared = _finalize_feature_frame(frame)
        if stage_seconds is not None:
            stage_seconds[f"{stage_prefix}_prepare_{index}"] = round(
                time.perf_counter() - prepared_at, 3
            )
        if not prepared.is_empty():
            cleaned.append(prepared)
    if not cleaned:
        return pl.DataFrame()
    if cleaned[0].height >= 100_000 and len(cleaned) >= 3:
        # A sequence of wide full joins rebuilds and coalesces the growing
        # key relation at every step. Form its union once, then attach each
        # unique-key feature relation with a cheaper left join. Both plans
        # retain every key and the first frame's value for repeated columns.
        keys_at = time.perf_counter()
        merged = pl.concat(
            [frame.select(list(KEY_COLUMNS)) for frame in cleaned],
            how="vertical",
        ).unique(subset=list(KEY_COLUMNS))
        if stage_seconds is not None:
            stage_seconds[f"{stage_prefix}_keys"] = round(
                time.perf_counter() - keys_at, 3
            )
        join_frames = enumerate(cleaned)
        join_how = "left"
    else:
        merged = cleaned[0]
        join_frames = enumerate(cleaned[1:], start=1)
        join_how = "full"
    for index, frame in join_frames:
        joined_at = time.perf_counter()
        merged = merged.join(
            frame, on=list(KEY_COLUMNS), how=join_how,
            coalesce=True,
        )
        if stage_seconds is not None:
            stage_seconds[f"{stage_prefix}_{index}"] = round(
                time.perf_counter() - joined_at, 3
            )
    # _finalize_feature_frame has already made every input unique by key and
    # normalized key/value dtypes. Either join plan remains unique by key,
    # so regrouping all 9M+ wide rows here is redundant. Keep
    # the same first-frame-wins handling of suffixed duplicate feature names.
    return merged.select(
        [*KEY_COLUMNS, *[name for name in merged.columns if name in OUTPUT_COLUMNS]]
    )


def _finalize_feature_frame(frame: pl.DataFrame) -> pl.DataFrame:
    if frame.is_empty():
        return frame
    output_cols = [col for col in frame.columns if col not in KEY_COLUMNS and col in OUTPUT_COLUMNS]
    if not output_cols:
        return pl.DataFrame()
    date_expr = (
        pl.col("date") if frame.schema["date"] == pl.Date
        else _date_column_expr("date")
    )
    frame = (
        frame.select(
            [
                date_expr.alias("date"),
                _symbol_expr("symbol").alias("symbol"),
                *[
                    (pl.col(col) if frame.schema[col] == pl.Float64 else
                     pl.col(col).cast(pl.Float64, strict=False)).alias(col)
                    for col in output_cols
                ],
            ]
        )
        .drop_nulls(["date", "symbol"])
        .filter(pl.col("symbol") != "")
    )
    if frame.is_empty():
        return frame
    if frame.height >= 100_000 and not frame.select(
        pl.struct(list(KEY_COLUMNS)).is_duplicated().any()
    ).item():
        # The group-by below exists to collapse duplicate source keys. If a
        # large frame has none, the same normalized rows are already final.
        # On the real official OHLCV frame this checks 9.7M keys in ~0.6s;
        # grouping every feature column is orders of magnitude costlier.
        return frame
    return frame.group_by(["date", "symbol"]).agg([pl.col(col).drop_nulls().last().alias(col) for col in output_cols])


def _ensure_feature_columns(frame: pl.DataFrame) -> pl.DataFrame:
    columns = set(frame.columns)
    expressions = []
    for name in OUTPUT_COLUMNS:
        if name in columns:
            expressions.append(pl.col(name).cast(pl.Float64, strict=False).alias(name))
        else:
            expressions.append(pl.lit(None, dtype=pl.Float64).alias(name))
    return frame.select([pl.col("date"), pl.col("symbol"), *expressions])


def _metadata_columns() -> set[str]:
    return {
        "_dataset",
        "_source",
        "_downloaded_at_utc",
        "_payload_sha256",
        "_url",
        "_resource",
        "_data_gov_id",
        "_data_gov_title",
        "_table_title",
        "_table_index",
        "_row_index",
    }


def _lit_null_float():
    return pl.lit(None, dtype=pl.Float64)


def _text_expr(name: str):
    return pl.col(name).cast(pl.Utf8, strict=False).str.strip_chars()


def _date_from_text_expr(expr):
    text = expr.cast(pl.Utf8, strict=False).str.strip_chars()
    iso = text.str.slice(0, 10).str.strptime(pl.Date, "%Y-%m-%d", strict=False)
    slash = text.str.strptime(pl.Date, "%Y/%m/%d", strict=False)
    digits = text.str.replace_all(r"[^0-9]", "")
    first4 = digits.str.slice(0, 4).cast(pl.Int32, strict=False)
    ymd = (
        pl.when((digits.str.len_chars() == 8) & (first4 >= 1900))
        .then(digits.str.strptime(pl.Date, "%Y%m%d", strict=False))
        .otherwise(None)
    )
    roc7_year = digits.str.slice(0, 3).cast(pl.Int32, strict=False) + 1911
    roc7 = (
        pl.when(digits.str.len_chars() == 7)
        .then(
            (
                roc7_year.cast(pl.Utf8)
                + "-"
                + digits.str.slice(3, 2)
                + "-"
                + digits.str.slice(5, 2)
            ).str.strptime(pl.Date, "%Y-%m-%d", strict=False)
        )
        .otherwise(None)
    )
    roc6_year = digits.str.slice(0, 2).cast(pl.Int32, strict=False) + 1911
    roc6 = (
        pl.when(digits.str.len_chars() == 6)
        .then(
            (
                roc6_year.cast(pl.Utf8)
                + "-"
                + digits.str.slice(2, 2)
                + "-"
                + digits.str.slice(4, 2)
            ).str.strptime(pl.Date, "%Y-%m-%d", strict=False)
        )
        .otherwise(None)
    )
    return pl.coalesce([iso, slash, ymd, roc7, roc6])


def _date_column_expr(name: str):
    return _date_from_text_expr(pl.col(name))


def _yyyymmdd_expr(name: str):
    return _date_column_expr(name)


def _period_expr(name: str):
    return _month_period_available_expr(name, lag_days=0)


def _month_period_available_expr(name: str, *, lag_days: int):
    text = _text_expr(name)
    year_m = text.str.extract(r"^(\d{4})M(\d{1,2})$", 1)
    month_m = text.str.extract(r"^\d{4}M(\d{1,2})$", 1)
    year_sep = text.str.extract(r"^(\d{4})[./-](\d{1,2})$", 1)
    month_sep = text.str.extract(r"^\d{4}[./-](\d{1,2})$", 1)
    year = pl.coalesce([year_m, year_sep])
    month = pl.coalesce([month_m, month_sep])
    month_start = (year + "-" + month + "-01").str.strptime(pl.Date, "%Y-%m-%d", strict=False)
    return month_start.dt.offset_by("1mo").dt.offset_by("-1d").dt.offset_by(f"{int(lag_days)}d")


def _quarter_period_available_expr(name: str, *, lag_days: int):
    text = _text_expr(name)
    year = text.str.extract(r"^(\d{4})Q([1-4])$", 1)
    quarter = text.str.extract(r"^\d{4}Q([1-4])$", 1).cast(pl.Int32, strict=False)
    month = (quarter * 3).cast(pl.Utf8)
    quarter_end = (year + "-" + month + "-01").str.strptime(pl.Date, "%Y-%m-%d", strict=False)
    return quarter_end.dt.offset_by("1mo").dt.offset_by("-1d").dt.offset_by(f"{int(lag_days)}d")


def _roc_year_month_available_expr(year_col: str, month_col: str, *, lag_days: int):
    roc_year = _num_expr(year_col).cast(pl.Int32, strict=False) + 1911
    month = _num_expr(month_col).cast(pl.Int32, strict=False)
    month_start = (
        roc_year.cast(pl.Utf8) + "-" + month.cast(pl.Utf8) + "-01"
    ).str.strptime(pl.Date, "%Y-%m-%d", strict=False)
    return month_start.dt.offset_by("1mo").dt.offset_by("-1d").dt.offset_by(f"{int(lag_days)}d")


def _roc_tax_period_available_expr(name: str, *, lag_days: int):
    text = _text_expr(name)
    roc_year = text.str.extract(r"(\d{2,3})年", 1).cast(pl.Int32, strict=False) + 1911
    month = text.str.extract(r"年\s*(\d{1,2})月", 1).cast(pl.Int32, strict=False)
    month_start = (
        roc_year.cast(pl.Utf8) + "-" + month.cast(pl.Utf8) + "-01"
    ).str.strptime(pl.Date, "%Y-%m-%d", strict=False)
    return month_start.dt.offset_by("1mo").dt.offset_by("-1d").dt.offset_by(f"{int(lag_days)}d")


def _days_between_expr(later, earlier):
    return later.cast(pl.Int32, strict=False) - earlier.cast(pl.Int32, strict=False)


def _symbol_expr(name: str):
    return pl.col(name).cast(pl.Utf8, strict=False).str.strip_chars().str.to_uppercase()


def _num_expr(name: str):
    return (
        pl.col(name)
        .cast(pl.Utf8, strict=False)
        .str.strip_chars()
        .str.replace_all(",", "")
        .str.replace_all("%", "")
        .replace(["", "-", "--", "—", "N/A", "NA", "NULL", "null", "None"], None)
        .cast(pl.Float64, strict=False)
    )


def _num_from_text_digits_expr(name: str):
    return (
        pl.col(name)
        .cast(pl.Utf8, strict=False)
        .str.extract(r"(-?\d+(?:\.\d+)?)", 1)
        .cast(pl.Float64, strict=False)
    )


def _optional_num_expr(columns: set[str], name: str):
    if not name or name not in columns:
        return pl.lit(None, dtype=pl.Float64)
    return _num_expr(name)


def _first_existing(columns: set[str], names: Iterable[str]) -> str | None:
    for name in names:
        if name in columns:
            return name
    return None


def _first_num_expr(columns: set[str], names: Iterable[str]):
    exprs = [_num_expr(name) for name in names if name in columns]
    return pl.coalesce(exprs) if exprs else _lit_null_float()


def _sum_num_expr(columns: set[str], names: Iterable[str]):
    expr = _lit_null_float()
    started = False
    for name in names:
        if name not in columns:
            continue
        value = _num_expr(name).fill_null(0.0)
        expr = value if not started else expr + value
        started = True
    return expr if started else _lit_null_float()


def _safe_ratio(numerator, denominator):
    return pl.when(denominator.is_finite() & (denominator != 0.0)).then(numerator / denominator).otherwise(None)


def _safe_log_ratio(numerator, denominator):
    ratio = _safe_ratio(numerator, denominator)
    return _safe_log(ratio)


def _binary_contains_expr(columns: set[str], name: str, pattern: str):
    if name not in columns:
        return _lit_null_float()
    return pl.col(name).cast(pl.Utf8, strict=False).str.contains(pattern).fill_null(False).cast(pl.Float64)


def _positive_log1p(expr):
    return pl.when(expr.is_finite() & (expr > 0.0)).then((expr + 1.0).log()).otherwise(None)


def _safe_log(expr):
    return pl.when(expr.is_finite() & (expr > 0.0)).then(expr.log()).otherwise(None)


def _signed_asinh(expr, scale: float = 1000.0):
    x = expr / float(scale)
    return pl.when(expr.is_finite()).then((x + ((x * x) + 1.0).sqrt()).log()).otherwise(None)


def _positive_log(expr):
    return pl.when(expr.is_finite() & (expr > 0.0)).then(expr.log()).otherwise(None)


def _build_official_ohlcv_features(input_dir: Path) -> pl.DataFrame:
    frames: list[pl.DataFrame] = []
    twse = _read_optional(input_dir, "twse_daily_ohlcv")
    if not twse.is_empty():
        close = _num_expr("收盤價")
        high = _num_expr("最高價")
        low = _num_expr("最低價")
        volume = _num_expr("成交股數")
        value = _num_expr("成交金額")
        trades = _num_expr("成交筆數")
        frames.append(
            twse.select(
                [
                    _date_column_expr("date").alias("date"),
                    _symbol_expr("證券代號").alias("symbol"),
                    pl.lit(1.0).alias("_twpub_official_traded"),
                    close.alias("_close"),
                    volume.alias("twpub_official_trading_volume_raw"),
                    value.alias("twpub_official_trading_value_raw"),
                    trades.alias("twpub_official_trades_raw"),
                    _positive_log1p(volume).alias("twpub_official_trading_volume_log"),
                    _positive_log1p(value).alias("twpub_official_trading_value_log"),
                    _positive_log1p(trades).alias("twpub_official_trades_log"),
                    _safe_ratio(high - low, close).alias("twpub_official_intraday_range"),
                    _safe_ratio(close, high).alias("twpub_official_close_to_high"),
                    _safe_ratio(close, low).alias("twpub_official_close_to_low"),
                ]
            )
        )
    tpex = _read_optional(input_dir, "tpex_daily_ohlcv")
    if not tpex.is_empty():
        close = _num_expr("收盤")
        high = _num_expr("最高")
        low = _num_expr("最低")
        volume = _num_expr("成交股數")
        value = _num_expr("成交金額(元)")
        trades = _num_expr("成交筆數")
        shares = _num_expr("發行股數")
        frames.append(
            tpex.select(
                [
                    _date_column_expr("date").alias("date"),
                    _symbol_expr("代號").alias("symbol"),
                    pl.lit(1.0).alias("_twpub_official_traded"),
                    close.alias("_close"),
                    volume.alias("twpub_official_trading_volume_raw"),
                    value.alias("twpub_official_trading_value_raw"),
                    trades.alias("twpub_official_trades_raw"),
                    _positive_log1p(volume).alias("twpub_official_trading_volume_log"),
                    _positive_log1p(value).alias("twpub_official_trading_value_log"),
                    _positive_log1p(trades).alias("twpub_official_trades_log"),
                    _safe_ratio(volume, shares).alias("twpub_official_turnover_ratio"),
                    _safe_ratio(high - low, close).alias("twpub_official_intraday_range"),
                    _safe_ratio(close, high).alias("twpub_official_close_to_high"),
                    _safe_ratio(close, low).alias("twpub_official_close_to_low"),
                    _safe_log_ratio(_num_expr("次日漲停價"), close).alias("_twpub_tpex_next_limit_up_ret"),
                    _safe_log_ratio(_num_expr("次日跌停價"), close).alias("_twpub_tpex_next_limit_down_ret"),
                ]
            )
        )
    if not frames:
        return pl.DataFrame()
    frame = pl.concat(frames, how="diagonal_relaxed").drop_nulls(["date", "symbol"])
    if frame.is_empty():
        return frame
    return (
        frame.sort(["symbol", "date"])
        .with_columns(_safe_log(pl.col("_close") / pl.col("_close").shift(1).over("symbol")).alias("twpub_official_close_logret_1d"))
        .drop("_close")
    )


def _build_delisted_company_rules(input_dir: Path) -> pl.DataFrame:
    # TPEx's official "delisted" lifecycle table also records same-symbol
    # transfers to the TWSE.  Those are venue migrations, not terminal asset
    # exits: the TWSE new-listing table identifies them explicitly as 櫃轉市.
    # Treating them as terminal would fabricate a sale, fee and portfolio reset
    # even though the security continues trading under the same symbol.
    twse_new_listings = _read_optional(input_dir, "twse_api_company_newlisting")
    tpex_to_twse_symbols: set[str] = set()
    if (
        not twse_new_listings.is_empty()
        and {"Code", "Note"}.issubset(twse_new_listings.columns)
    ):
        tpex_to_twse_symbols = {
            str(symbol).strip().upper()
            for symbol in (
                twse_new_listings.filter(
                    pl.col("Note")
                    .cast(pl.Utf8, strict=False)
                    .fill_null("")
                    .str.contains("櫃轉市", literal=True)
                )
                .get_column("Code")
                .cast(pl.Utf8, strict=False)
                .drop_nulls()
                .to_list()
            )
            if str(symbol).strip()
        }

    frames: list[pl.DataFrame] = []
    for dataset in ("twse_delisted_company", "tpex_delisted_company"):
        frame = _read_optional(input_dir, dataset)
        if frame.is_empty() or not {"date", "symbol"}.issubset(frame.columns):
            continue
        if dataset == "tpex_delisted_company" and tpex_to_twse_symbols:
            frame = frame.filter(
                ~_symbol_expr("symbol").is_in(sorted(tpex_to_twse_symbols))
            )
        frames.append(
            frame.select(
                [
                    _date_column_expr("date").alias("date"),
                    _symbol_expr("symbol").alias("symbol"),
                    pl.lit(1.0).alias("_twpub_delisted"),
                ]
            ).drop_nulls(["date", "symbol"])
        )
    if not frames:
        return pl.DataFrame()
    return pl.concat(frames, how="diagonal_relaxed").unique(["date", "symbol"], keep="last")


def _snapshot_date_expr(columns: set[str]):
    explicit = _first_existing(columns, ("出表日期", "發言日期", "Date", "日期", "date"))
    declared = _date_column_expr(explicit) if explicit else pl.lit(None, dtype=pl.Date)
    observed = _observed_available_date_expr(columns)
    if observed is not None:
        # The report date describes the subject, not necessarily publication.
        # An observation made at 09:00 or later belongs to the next session.
        return (
            pl.when(observed.is_not_null())
            .then(pl.max_horizontal(declared, observed))
            .otherwise(None)
        )
    return declared


def _snapshot_not_before_expr(columns: set[str], event_date):
    published = _snapshot_date_expr(columns)
    return (
        pl.when(published.is_not_null())
        .then(pl.max_horizontal(event_date, published))
        .otherwise(None)
    )


def _observed_available_date_expr(columns: set[str]):
    if "_downloaded_at_utc" in columns:
        local = (
            pl.col("_downloaded_at_utc")
            .cast(pl.Utf8, strict=False)
            .str.to_datetime(time_zone="UTC", strict=False)
            .dt.convert_time_zone("Asia/Taipei")
        )
        candidate = (
            pl.when(local.dt.hour() < 9)
            .then(local.dt.date())
            .otherwise(local.dt.date().dt.offset_by("1d"))
        )
        if "_as_of_date" in columns:
            # A legacy/partial row with an unparseable response clock cannot
            # be promoted to the snapshot day before opening.
            return pl.coalesce(
                [candidate, _date_column_expr("_as_of_date").dt.offset_by("1d")]
            )
        return candidate
    if "_as_of_date" in columns:
        return _date_column_expr("_as_of_date").dt.offset_by("1d")
    return None


def _periodic_available_date_expr(columns: set[str], period_end):
    """Never manufacture a historical release from a statistical period end.

    Current-only open-data resources contain later revisions of old months.
    Without a source-timestamped historical value vintage, the first archived
    response is the earliest defensible time for those exact values.
    """

    observed = _observed_available_date_expr(columns)
    if observed is None:
        return pl.lit(None, dtype=pl.Date)
    return (
        pl.when(observed.is_not_null() & period_end.is_not_null())
        .then(pl.max_horizontal(period_end.dt.offset_by("1d"), observed))
        .otherwise(None)
    )


def _material_info_available_date_expr(columns: set[str]):
    """Use MOPS's row-level speaking time, never the fact/event date."""

    fallback = (
        _snapshot_date_expr(columns)
        if "_downloaded_at_utc" in columns or "_as_of_date" in columns
        else pl.lit(None, dtype=pl.Date)
    )
    if "發言日期" not in columns or "發言時間" not in columns:
        return fallback
    announced = _date_column_expr("發言日期")
    raw_clock = _text_expr("發言時間")
    compact = raw_clock.str.zfill(6)
    colon_clock = raw_clock.str.contains(r"^\d{1,2}:\d{2}(?::\d{2})?$")
    compact_clock = raw_clock.str.contains(r"^\d{4,6}$")
    hour = pl.coalesce(
        [raw_clock.str.extract(r"^(\d{1,2}):", 1), compact.str.slice(0, 2)]
    ).cast(pl.Int32, strict=False)
    minute = pl.coalesce(
        [raw_clock.str.extract(r"^\d{1,2}:(\d{2})", 1), compact.str.slice(2, 2)]
    ).cast(pl.Int32, strict=False)
    second = pl.coalesce(
        [raw_clock.str.extract(r"^\d{1,2}:\d{2}:(\d{2})$", 1),
         pl.when(colon_clock).then(pl.lit("0")).otherwise(compact.str.slice(4, 2))]
    ).cast(pl.Int32, strict=False)
    valid = (
        announced.is_not_null()
        & (colon_clock | compact_clock)
        & hour.is_between(0, 23)
        & minute.is_between(0, 59)
        & second.is_between(0, 59)
    )
    release_date = (
        pl.when(hour < 9)
        .then(announced)
        .otherwise(announced.dt.offset_by("1d"))
    )
    return pl.when(valid).then(release_date).otherwise(fallback)


def _snapshot_symbol_expr(columns: set[str]):
    name = _first_existing(columns, ("公司代號", "SecuritiesCompanyCode", "Code", "TWSECode"))
    return _symbol_expr(name) if name else pl.lit("")


def _read_glob(input_dir: Path, pattern: str) -> list[pl.DataFrame]:
    return [pl.read_parquet(path) for path in sorted(input_dir.glob(pattern))]


def _build_model_useful_financial_features(input_dir: Path) -> pl.DataFrame:
    monthly_frames: list[pl.DataFrame] = []
    income_frames: list[pl.DataFrame] = []
    balance_frames: list[pl.DataFrame] = []
    for frame in _read_glob(input_dir, "*_api_*t187ap05_[lo].parquet"):
        columns = set(frame.columns)
        revenue = _first_num_expr(columns, ("營業收入-當月營收",))
        monthly_frames.append(
            frame.select(
                _snapshot_date_expr(columns).alias("date"),
                _snapshot_symbol_expr(columns).alias("symbol"),
                _positive_log1p(revenue).alias("twpub_monthly_revenue_log"),
                (_first_num_expr(columns, ("營業收入-上月比較增減(%)",)) / 100.0).alias("twpub_monthly_revenue_mom"),
                (_first_num_expr(columns, ("營業收入-去年同月增減(%)",)) / 100.0).alias("twpub_monthly_revenue_yoy"),
                (_first_num_expr(columns, ("累計營業收入-前期比較增減(%)",)) / 100.0).alias("twpub_cumulative_revenue_yoy"),
            )
        )

    for frame in _read_glob(input_dir, "*_api_*t187ap06_*parquet"):
        columns = set(frame.columns)
        revenue = _first_num_expr(columns, ("營業收入", "收益", "利息淨收益"))
        gross = _first_num_expr(columns, ("營業毛利（毛損）淨額", "營業毛利（毛損）"))
        operating = _first_num_expr(columns, ("營業利益（損失）", "繼續營業單位稅前淨利（淨損）"))
        net_income = _first_num_expr(columns, ("本期淨利（淨損）", "本期稅後淨利（淨損）"))
        income_frames.append(
            frame.select(
                _snapshot_date_expr(columns).alias("date"),
                _snapshot_symbol_expr(columns).alias("symbol"),
                _first_num_expr(columns, ("基本每股盈餘（元）", "基本每股盈餘")).alias("twpub_financial_eps"),
                _positive_log1p(revenue).alias("twpub_financial_revenue_log"),
                _signed_asinh(net_income, scale=100000.0).alias("twpub_financial_net_income_asinh"),
                _safe_ratio(gross, revenue).alias("twpub_financial_gross_margin"),
                _safe_ratio(operating, revenue).alias("twpub_financial_operating_margin"),
                _safe_ratio(net_income, revenue).alias("twpub_financial_net_margin"),
            )
        )

    for frame in _read_glob(input_dir, "*_api_*t187ap07_*parquet"):
        columns = set(frame.columns)
        assets = _first_num_expr(columns, ("資產總額", "資產總計"))
        liabilities = _first_num_expr(columns, ("負債總額", "負債總計"))
        current_assets = _first_num_expr(columns, ("流動資產",))
        current_liabilities = _first_num_expr(columns, ("流動負債",))
        equity = _first_num_expr(columns, ("權益總額", "權益總計"))
        balance_frames.append(
            frame.select(
                _snapshot_date_expr(columns).alias("date"),
                _snapshot_symbol_expr(columns).alias("symbol"),
                _positive_log1p(assets).alias("twpub_financial_assets_log"),
                _safe_ratio(liabilities, assets).alias("twpub_financial_debt_ratio"),
                _safe_ratio(current_assets, current_liabilities).alias("twpub_financial_current_ratio"),
                _safe_ratio(equity, assets).alias("twpub_financial_equity_ratio"),
                _positive_log(_first_num_expr(columns, ("每股參考淨值",))).alias("twpub_financial_book_value_per_share_log"),
            )
        )
    groups = [
        pl.concat(group, how="diagonal_relaxed")
        for group in (monthly_frames, income_frames, balance_frames)
        if group
    ]
    return _merge_feature_frames(groups)


def _build_model_useful_ownership_features(input_dir: Path) -> pl.DataFrame:
    holding_frames: list[pl.DataFrame] = []
    transfer_frames: list[pl.DataFrame] = []
    for frame in _read_glob(input_dir, "*_api_*t187ap11_[lo].parquet"):
        columns = set(frame.columns)
        holding_frames.append(
            frame.select(
                _snapshot_date_expr(columns).alias("date"),
                _snapshot_symbol_expr(columns).alias("symbol"),
                _num_expr("目前持股").alias("holdings"),
                _num_expr("設質股數").alias("pledged"),
            ).group_by(["date", "symbol"]).agg(
                _positive_log1p(pl.col("holdings").sum()).alias("twpub_insider_holdings_log"),
                _safe_ratio(pl.col("pledged").sum(), pl.col("holdings").sum()).alias("twpub_insider_pledge_ratio"),
            )
        )
    for frame in _read_glob(input_dir, "*_api_*t187ap12_[lo].parquet"):
        columns = set(frame.columns)
        transfer_frames.append(
            frame.select(
                _snapshot_date_expr(columns).alias("date"),
                _snapshot_symbol_expr(columns).alias("symbol"),
                _num_expr("預定轉讓方式及股數-轉讓股數").alias("shares"),
            ).group_by(["date", "symbol"]).agg(
                _positive_log1p(pl.col("shares").sum()).alias("twpub_insider_transfer_shares_log")
            )
        )
    groups = [
        pl.concat(group, how="diagonal_relaxed")
        for group in (holding_frames, transfer_frames)
        if group
    ]
    return _merge_feature_frames(groups)


def _build_model_useful_shorting_features(input_dir: Path) -> pl.DataFrame:
    frames: list[pl.DataFrame] = []
    tpex = _read_optional(input_dir, "tpex_api_tpex_margin_sbl")
    if not tpex.is_empty():
        columns = set(tpex.columns)
        frames.append(
            tpex.select(
                _snapshot_date_expr(columns).alias("date"),
                _snapshot_symbol_expr(columns).alias("symbol"),
                _positive_log1p(_num_expr("SaleAvailableVolumes")).alias("twpub_short_sale_available_log"),
                _positive_log1p(_num_expr("SecuritiesBorrowingBalanceOfTheMarketDay")).alias("twpub_sbl_balance_log"),
                _positive_log1p(_num_expr("AvailableVolumesForSBLShortSale")).alias("twpub_borrow_available_log"),
            )
        )
    twse = _read_optional(input_dir, "twse_api_sbl_twt96u")
    if not twse.is_empty():
        for code, volume in (("TWSECode", "TWSEAvailableVolume"), ("GRETAICode", "GRETAIAvailableVolume")):
            frames.append(
                twse.select(
                    _snapshot_date_expr(set(twse.columns)).alias("date"),
                    _symbol_expr(code).alias("symbol"),
                    _positive_log1p(_num_expr(volume)).alias("twpub_borrow_available_log"),
                )
            )
    return _finalize_feature_frame(pl.concat(frames, how="diagonal_relaxed")) if frames else pl.DataFrame()


def _expand_rule_range(
    frame: pl.DataFrame,
    *,
    symbol: str,
    start: str,
    end: str | None,
    rule: str,
    end_exclusive: bool = False,
) -> pl.DataFrame:
    columns = set(frame.columns)
    start_expr = _date_column_expr(start)
    if end and end in columns:
        parsed_end = _date_column_expr(end)
        end_expr = pl.coalesce([parsed_end.dt.offset_by("-1d") if end_exclusive else parsed_end, start_expr])
    else:
        end_expr = start_expr
    return (
        frame.select(
            _symbol_expr(symbol).alias("symbol"),
            pl.date_ranges(start_expr, end_expr, interval="1d", closed="both").alias("date"),
        )
        .explode("date", empty_as_null=True)
        .drop_nulls(["date", "symbol"])
        .with_columns(pl.lit(1.0).alias(rule))
    )


def _build_model_useful_rule_features(input_dir: Path) -> pl.DataFrame:
    frames: list[pl.DataFrame] = []
    specs = (
        ("twse_api_exchangereport_bfi84u", "Code", "StartDate", "EndDate", "_twpub_short_open_ban", False),
        ("tpex_api_tpex_margin_trading_term", "SecuritiesCompanyCode", "ShortSaleSuspensionStartDate", "ShortSaleSuspensionEndDate", "_twpub_short_open_ban", False),
        ("tpex_short_sale_suspension_terms", "symbol", "short_open_ban_date", "short_open_resume_date", "_twpub_short_open_ban", False),
        ("twse_api_exchangereport_twtawu", "Code", "TradingHaltDate", "TradingResumptionDate", "_twpub_trading_halt", True),
        ("tpex_api_tpex_spendi_history", "SecuritiesCompanyCode", "DateOfSuspendedTrading", "DateOfResumedTrading", "_twpub_trading_halt", True),
    )
    for dataset, symbol, start, end, rule, end_exclusive in specs:
        frame = _read_optional(input_dir, dataset)
        if not frame.is_empty() and {symbol, start}.issubset(frame.columns):
            frames.append(
                _expand_rule_range(
                    frame,
                    symbol=symbol,
                    start=start,
                    end=end,
                    rule=rule,
                    end_exclusive=end_exclusive,
                )
            )
    cmode = _read_optional(input_dir, "tpex_api_tpex_cmode")
    if not cmode.is_empty():
        frames.append(
            cmode.filter(_text_expr("SuspensionOfTrading").is_in(["Y", "Ｙ", "是"]))
            .select(
                _date_column_expr("Date").alias("date"),
                _symbol_expr("SecuritiesCompanyCode").alias("symbol"),
                pl.lit(1.0).alias("_twpub_trading_halt"),
            )
        )
    announcement_rules = _build_delisting_announcement_rules(input_dir)
    if not announcement_rules.is_empty():
        frames.append(announcement_rules)
    return _finalize_feature_frame(pl.concat(frames, how="diagonal_relaxed")) if frames else pl.DataFrame()


def _normalize_day_trade_source(
    input_dir: Path,
    *,
    dataset: str,
) -> pl.DataFrame:
    path = input_dir / f"{dataset}.parquet"
    if not path.exists():
        return pl.DataFrame()
    required = {"date", "證券代號", "證券名稱"}
    schema_columns = set(pl.read_parquet_schema(path))
    missing = sorted(required - schema_columns)
    if missing:
        raise ValueError(
            f"{dataset}.parquet is missing required day-trade columns: {missing}"
        )

    has_suspension_marker = DAY_TRADE_SUSPENSION_COLUMN in schema_columns
    projected_columns = ["date", "證券代號"]
    if has_suspension_marker:
        projected_columns.append(DAY_TRADE_SUSPENSION_COLUMN)
    frame = _read_date_bounded_parquet(
        path,
        dataset=dataset,
        columns=projected_columns,
    )
    normalized = frame.select(
        _date_column_expr("date").alias("date"),
        _symbol_expr("證券代號").alias("symbol"),
        (
            pl.col(DAY_TRADE_SUSPENSION_COLUMN)
            .cast(pl.Utf8, strict=False)
            .str.strip_chars()
            .str.to_uppercase()
            if has_suspension_marker
            else pl.lit(None, dtype=pl.Utf8)
        ).alias("_sell_first_suspension"),
    ).drop_nulls(["date", "symbol"])
    normalized = normalized.filter(
        (pl.col("date") >= pl.lit(DAY_TRADE_REGIME_START))
        & (pl.col("symbol") != "")
    )
    if normalized.is_empty():
        raise ValueError(
            f"{dataset}.parquet has no rows in the day-trade regime"
        )

    post_sell_first_missing = normalized.filter(
        (pl.col("date") >= pl.lit(DAY_TRADE_SELL_FIRST_START))
        & pl.col("_sell_first_suspension").is_null()
    )
    if not post_sell_first_missing.is_empty():
        raise ValueError(
            f"{dataset}.parquet has missing sell-first suspension markers on or "
            f"after {DAY_TRADE_SELL_FIRST_START.isoformat()}"
        )
    normalized = normalized.with_columns(
        pl.col("_sell_first_suspension").fill_null("")
    )
    unknown_markers = (
        normalized.filter(
            ~pl.col("_sell_first_suspension").is_in(["", "Y", "*", "＊"])
        )
        .select("_sell_first_suspension")
        .unique()
        .sort("_sell_first_suspension")
    )
    if not unknown_markers.is_empty():
        raise ValueError(
            f"{dataset}.parquet has unknown sell-first suspension markers: "
            f"{unknown_markers.to_series().to_list()[:5]}"
        )

    duplicates = (
        normalized.group_by(["date", "symbol"])
        .agg(pl.len().alias("_rows"))
        .filter(pl.col("_rows") != 1)
    )
    if not duplicates.is_empty():
        raise ValueError(
            f"{dataset}.parquet has duplicate date-symbol eligibility rows: "
            f"{duplicates.head(5).to_dicts()}"
        )
    return normalized.with_columns(
        pl.lit(1.0).alias("_eligible_source")
    )


def _normalize_day_trade_universe(
    input_dir: Path,
    *,
    dataset: str,
    symbol_column: str,
) -> pl.DataFrame:
    path = input_dir / f"{dataset}.parquet"
    if not path.exists():
        return pl.DataFrame()
    required = {"date", symbol_column}
    schema_columns = set(pl.read_parquet_schema(path))
    missing = sorted(required - schema_columns)
    if missing:
        raise ValueError(
            f"{dataset}.parquet is missing required universe columns: {missing}"
        )
    frame = _read_date_bounded_parquet(
        path,
        dataset=dataset,
        columns=["date", symbol_column],
    )
    return (
        frame.select(
            _date_column_expr("date").alias("date"),
            _symbol_expr(symbol_column).alias("symbol"),
        )
        .drop_nulls(["date", "symbol"])
        .filter(
            (pl.col("date") >= pl.lit(DAY_TRADE_REGIME_START))
            & (pl.col("symbol") != "")
        )
        .unique()
        .sort(["date", "symbol"])
    )


def _require_day_trade_date_coverage(
    *,
    market: str,
    universe: pl.DataFrame,
    eligible: pl.DataFrame,
    allow_missing_latest_session: bool = False,
) -> pl.DataFrame:
    universe_dates = universe.select("date").unique()
    eligible_dates = eligible.select("date").unique()
    missing = universe_dates.join(eligible_dates, on="date", how="anti").sort("date")
    extra = eligible_dates.join(universe_dates, on="date", how="anti").sort("date")
    if allow_missing_latest_session and extra.is_empty() and missing.height == 1:
        missing_date = missing.item()
        latest_universe_date = universe_dates.select(pl.col("date").max()).item()
        if missing_date == latest_universe_date:
            return universe.filter(pl.col("date") != pl.lit(missing_date))
    if not missing.is_empty() or not extra.is_empty():
        raise ValueError(
            f"{market} day-trade eligibility coverage does not match official "
            "OHLCV sessions: "
            f"missing={missing.head(5).to_series().to_list()} "
            f"extra={extra.head(5).to_series().to_list()}"
        )
    return universe


def _build_day_trade_rule_features(
    input_dir: Path,
    *,
    symbols: set[str] | None = None,
    end_date: date | None = None,
    allow_missing_latest_session: bool = False,
) -> pl.DataFrame:
    """Build exact-session TWSE/TPEx day-trade masks with explicit negatives.

    The official pages publish only members.  A sparse ``1``-only table is not
    enough for point-in-time correctness because removal from the list would be
    indistinguishable from a missing receipt.  The receipt-certified daily
    OHLCV rows provide each venue's observed security universe, so this builder
    emits an explicit 0/1 for every observed date-symbol key.
    """

    twse_eligible = _normalize_day_trade_source(
        input_dir,
        dataset="twse_day_trade_eligibility",
    )
    tpex_eligible = _normalize_day_trade_source(
        input_dir,
        dataset="tpex_day_trade_eligibility",
    )
    if twse_eligible.is_empty() and tpex_eligible.is_empty():
        return pl.DataFrame()
    if twse_eligible.is_empty() or tpex_eligible.is_empty():
        raise ValueError(
            "day-trade rule build requires both TWSE and TPEx point-in-time "
            "eligibility histories"
        )

    twse_universe = _normalize_day_trade_universe(
        input_dir,
        dataset="twse_daily_ohlcv",
        symbol_column="證券代號",
    )
    tpex_universe = _normalize_day_trade_universe(
        input_dir,
        dataset="tpex_daily_ohlcv",
        symbol_column="代號",
    )
    if twse_universe.is_empty() or tpex_universe.is_empty():
        raise ValueError(
            "day-trade rule build requires receipt-certified TWSE and TPEx "
            "daily OHLCV universes"
        )

    # A next-session rule master is intentionally published before that
    # session has an official close.  It must remain in the mutable rule
    # archive for live execution, while a feature build ending at the prior
    # close must compare only dates inside its declared build horizon.  Keep
    # the default strict for direct callers without an explicit horizon so a
    # genuinely corrupt extra date is not silently hidden.
    if end_date is not None:
        horizon = pl.lit(end_date)
        twse_eligible = twse_eligible.filter(pl.col("date") <= horizon)
        tpex_eligible = tpex_eligible.filter(pl.col("date") <= horizon)
        twse_universe = twse_universe.filter(pl.col("date") <= horizon)
        tpex_universe = tpex_universe.filter(pl.col("date") <= horizon)

    frames: list[pl.DataFrame] = []
    for market, universe, eligible in (
        ("TWSE", twse_universe, twse_eligible),
        ("TPEx", tpex_universe, tpex_eligible),
    ):
        universe = _require_day_trade_date_coverage(
            market=market,
            universe=universe,
            eligible=eligible,
            allow_missing_latest_session=allow_missing_latest_session,
        )
        if symbols is not None:
            symbol_filter = sorted(symbols)
            universe = universe.filter(pl.col("symbol").is_in(symbol_filter))
            eligible = eligible.filter(pl.col("symbol").is_in(symbol_filter))
        if universe.is_empty():
            continue
        frames.append(
            universe.join(
                eligible.select(
                    "date",
                    "symbol",
                    "_sell_first_suspension",
                    "_eligible_source",
                ),
                on=["date", "symbol"],
                how="left",
            )
            .with_columns(
                [
                    pl.col("_eligible_source")
                    .fill_null(0.0)
                    .alias("_twpub_day_trade_eligible"),
                    pl.when(
                        pl.col("_eligible_source").is_not_null()
                        & (pl.col("date") >= pl.lit(DAY_TRADE_SELL_FIRST_START))
                        & (pl.col("_sell_first_suspension") == "")
                    )
                    .then(1.0)
                    .otherwise(0.0)
                    .alias("_twpub_day_trade_short_open"),
                ]
            )
            .select(
                "date",
                "symbol",
                "_twpub_day_trade_eligible",
                "_twpub_day_trade_short_open",
            )
        )
    return (
        pl.concat(frames, how="vertical").sort(["date", "symbol"])
        if frames
        else pl.DataFrame()
    )


def _build_delisting_announcement_rules(input_dir: Path) -> pl.DataFrame:
    """Turn point-in-time delisting notices into executable short rules.

    Explicit effective dates are authoritative but are never applied before the
    announcement.  When a delisting notice has no explicit short-sale start,
    the conservative rule starts on the next calendar day; the panel later
    aligns calendar dates to actual sessions. Explicit cover dates are emitted
    directly; an announcement that explicitly requires cover N trading sessions
    before delisting emits a point-in-time relative marker for the panel to
    resolve against its actual exchange-session calendar.
    """
    announcements = _read_optional(input_dir, "tw_delisting_short_sale_announcements")
    if announcements.is_empty() or not {"announcement_date", "symbols"}.issubset(announcements.columns):
        return pl.DataFrame()

    columns = set(announcements.columns)

    def date_expr(name: str):
        return _date_column_expr(name) if name in columns else pl.lit(None, dtype=pl.Date)

    normalized = announcements.select(
        date_expr("announcement_date").alias("announcement_date"),
        (
            pl.col("market")
            .cast(pl.Utf8, strict=False)
            .fill_null("")
            .str.to_lowercase()
            if "market" in columns
            else pl.lit("")
        ).alias("market"),
        pl.col("symbols").cast(pl.Utf8, strict=False).fill_null("").alias("symbols"),
        date_expr("short_open_ban_date").alias("short_open_ban_date"),
        date_expr("short_open_resume_date").alias("short_open_resume_date"),
        date_expr("short_cover_deadline").alias("short_cover_deadline"),
        date_expr("short_cover_anchor_date").alias("short_cover_anchor_date"),
        date_expr("delisting_date").alias("delisting_date"),
        (
            pl.col("short_cover_lead_trading_days").cast(pl.Int64, strict=False)
            if "short_cover_lead_trading_days" in columns
            else pl.lit(None, dtype=pl.Int64)
        ).alias("short_cover_lead_trading_days"),
        (
            pl.col("short_cover_anchor_lead_trading_days").cast(pl.Int64, strict=False)
            if "short_cover_anchor_lead_trading_days" in columns
            else pl.lit(None, dtype=pl.Int64)
        ).alias("short_cover_anchor_lead_trading_days"),
        (
            pl.col("article_78_exempt").cast(pl.Boolean, strict=False).fill_null(False)
            if "article_78_exempt" in columns
            else pl.lit(False)
        ).alias("article_78_exempt"),
        (
            pl.col("technical_share_replacement").cast(pl.Boolean, strict=False).fill_null(False)
            if "technical_share_replacement" in columns
            else pl.lit(False)
        ).alias("technical_share_replacement"),
        (
            pl.col("delisting_cancelled").cast(pl.Boolean, strict=False).fill_null(False)
            if "delisting_cancelled" in columns
            else pl.lit(False)
        ).alias("delisting_cancelled"),
        (
            pl.col("continues_short_open_ban").cast(pl.Boolean, strict=False).fill_null(False)
            if "continues_short_open_ban" in columns
            else pl.lit(False)
        ).alias("continues_short_open_ban"),
        (
            pl.col("subject").cast(pl.Utf8, strict=False).fill_null("")
            if "subject" in columns
            else pl.lit("")
        ).alias("subject"),
        (
            pl.col("body_text").cast(pl.Utf8, strict=False).fill_null("")
            if "body_text" in columns
            else pl.lit("")
        ).alias("body_text"),
    )
    horizon_values = [
        value
        for name in ("announcement_date", "short_open_resume_date", "delisting_date")
        for value in normalized[name].drop_nulls().to_list()
    ]
    # An explicit ban is persistent state. The absence of newer announcement
    # rows is not a resume event, so an open-ended ban must survive through the
    # build date unless a real resume/delisting transition closes it.
    rule_horizon = max([date.today(), *horizon_values])

    rows = normalized.iter_rows(named=True)
    resume_events: dict[tuple[str, str], list[date]] = {}
    cancellation_events: dict[tuple[str, str], list[date]] = {}
    delisting_cancellation_events: dict[tuple[str, str], list[date]] = {}
    for row in rows:
        resume_date = row["short_open_resume_date"]
        announcement_date = row["announcement_date"]
        if announcement_date is None:
            continue
        symbols = split_announcement_symbols(row["symbols"])
        market = row["market"]
        if resume_date is not None:
            for symbol in symbols:
                resume_events.setdefault((market, symbol), []).append(
                    max(resume_date, announcement_date)
                )
        classification = classify_delisting_notice(
            f"{row['subject']} {row['body_text']}"
        )
        if classification.restriction_cancelled:
            for symbol in symbols:
                cancellation_events.setdefault((market, symbol), []).append(
                    announcement_date
                )
        if bool(row["delisting_cancelled"]) or classification.delisting_cancelled:
            for symbol in symbols:
                delisting_cancellation_events.setdefault(
                    (market, symbol), []
                ).append(announcement_date)
    for dates in resume_events.values():
        dates.sort()
    for dates in cancellation_events.values():
        dates.sort()
    for dates in delisting_cancellation_events.values():
        dates.sort()

    output: list[dict[str, object]] = []
    for row in normalized.iter_rows(named=True):
        announcement_date = row["announcement_date"]
        if announcement_date is None:
            continue
        text = f"{row['subject']} {row['body_text']}"
        classification = classify_delisting_notice(text)
        article_78_exempt = bool(row["article_78_exempt"]) or classification.article_78_exempt
        technical_share_replacement = bool(
            row["technical_share_replacement"]
        ) or classification.technical_share_replacement
        delisting_cancelled = bool(
            row["delisting_cancelled"]
        ) or classification.delisting_cancelled
        continues_short_open_ban = bool(
            row["continues_short_open_ban"]
        ) or classification.continues_short_open_ban
        delisting_date = row["delisting_date"]
        inferred_delisting_rule = bool(
            classification.is_delisting_notice
            and delisting_date is not None
            and not article_78_exempt
            and not technical_share_replacement
            and not delisting_cancelled
            and not classification.restriction_cancelled
        )
        market = row["market"]
        symbols = split_announcement_symbols(row["symbols"])
        for symbol in symbols:
            event_key = (market, symbol)
            explicit_start = (
                None
                if classification.restriction_cancelled
                else row["short_open_ban_date"]
            )
            if explicit_start is None and continues_short_open_ban:
                explicit_start = announcement_date
            if explicit_start is not None:
                # A cached notice must never alter dates before its publication.
                start = max(explicit_start, announcement_date)
            elif inferred_delisting_rule:
                start = announcement_date + timedelta(days=1)
            else:
                start = None

            if start is not None:
                next_cancellation = next(
                    (
                        value
                        for value in cancellation_events.get(event_key, ())
                        if value >= announcement_date
                    ),
                    None,
                )
                next_delisting_cancellation = next(
                    (
                        value
                        for value in delisting_cancellation_events.get(
                            event_key, ()
                        )
                        if value >= announcement_date
                    ),
                    None,
                )
                if next_cancellation is not None and next_cancellation <= start:
                    start = None
                if (
                    inferred_delisting_rule
                    and next_delisting_cancellation is not None
                    and next_delisting_cancellation <= start
                ):
                    start = None
            else:
                next_delisting_cancellation = next(
                    (
                        value
                        for value in delisting_cancellation_events.get(
                            event_key, ()
                        )
                        if value >= announcement_date
                    ),
                    None,
                )

            # Company terminal events are sourced from the authoritative TWSE
            # / TPEx delisted-company datasets.  Those datasets do not include
            # ETF beneficiary certificates, so for leading-zero fund symbols
            # use the official, uncancelled delisting notice itself.  Keeping
            # this fund-only avoids replacing observed company outcomes with a
            # merely scheduled announcement.
            if (
                symbol.startswith("0")
                and inferred_delisting_rule
                and delisting_date is not None
                and (
                    next_delisting_cancellation is None
                    or next_delisting_cancellation > delisting_date
                )
            ):
                output.append(
                    {
                        "date": delisting_date,
                        "symbol": symbol,
                        "_twpub_delisted": 1.0,
                    }
                )

            if start is not None:
                end_candidates = [
                    value
                    for value in (delisting_date,)
                    if value is not None
                    and value >= start
                    and classification.is_delisting_notice
                    and not delisting_cancelled
                ]
                next_resume = next(
                    (
                        value
                        for value in resume_events.get(event_key, ())
                        if value > start
                    ),
                    None,
                )
                if next_resume is not None:
                    end_candidates.append(next_resume - timedelta(days=1))
                if next_cancellation is not None and next_cancellation > start:
                    end_candidates.append(next_cancellation - timedelta(days=1))
                if (
                    inferred_delisting_rule
                    and next_delisting_cancellation is not None
                    and next_delisting_cancellation > start
                ):
                    end_candidates.append(
                        next_delisting_cancellation - timedelta(days=1)
                    )
                # A ban is a state, not a one-day event.  In the absence of an
                # explicit resume/delisting transition, keep it active through
                # the latest date known by this point-in-time announcement set.
                end = min(end_candidates) if end_candidates else max(start, rule_horizon or start)
                cursor = start
                while cursor <= end:
                    output.append({"date": cursor, "symbol": symbol, "_twpub_short_open_ban": 1.0})
                    cursor += timedelta(days=1)

            deadline = row["short_cover_deadline"]
            deadline_cancelled = bool(
                next_delisting_cancellation is not None
                and deadline is not None
                and next_delisting_cancellation <= deadline
            )
            if (
                deadline is not None
                and deadline >= announcement_date
                and not deadline_cancelled
            ):
                output.append({"date": deadline, "symbol": symbol, "_twpub_force_short_cover": 1.0})

            # Some TPEx notices key the mandatory short close to the first
            # stop-transfer date (six exchange sessions before it), rather
            # than to delisting (the usual ten-session rule). The panel resolves
            # this explicit anchor against its own observed exchange sessions.
            anchor_date = row["short_cover_anchor_date"]
            anchor_lead_sessions = row["short_cover_anchor_lead_trading_days"]
            if anchor_date is not None and anchor_lead_sessions is not None:
                cover_anchor = anchor_date
                lead_sessions = anchor_lead_sessions
            else:
                cover_anchor = delisting_date
                lead_sessions = row["short_cover_lead_trading_days"]
                if lead_sessions is None and classification.requires_relative_cover:
                    lead_sessions = 10
            if (
                inferred_delisting_rule
                and cover_anchor is not None
                and lead_sessions is not None
                and int(lead_sessions) > 0
            ):
                output.append(
                    {
                        "date": announcement_date,
                        "symbol": symbol,
                        "_twpub_force_cover_lead_sessions": float(lead_sessions),
                        "_twpub_force_cover_anchor_ordinal": float(
                            cover_anchor.toordinal()
                        ),
                        "_twpub_force_cover_cancel_ordinal": (
                            float(next_delisting_cancellation.toordinal())
                            if next_delisting_cancellation is not None
                            else None
                        ),
                    }
                )

    return pl.DataFrame(output, infer_schema_length=None) if output else pl.DataFrame()


def _build_twse_market_index_features(input_dir: Path, *, market_symbol: str) -> pl.DataFrame:
    """Build one PIT TAIEX series from the complete monthly archive plus IND.

    ``MI_5MINS_HIST`` is the complete official base (available from 1999), while
    the daily ``MI_INDEX?type=IND`` table starts much later.  Same-day IND rows
    have higher priority, but overlapping closes must agree first so a stale or
    semantically mismatched TWSE payload cannot silently replace the archive.
    """

    archive = _read_optional(input_dir, "twse_taiex_ohlc")
    if not archive.is_empty():
        required = {"date", "closing_index"}
        missing = sorted(required - set(archive.columns))
        if missing:
            raise ValueError(
                f"twse_taiex_ohlc.parquet is missing required columns: {missing}"
            )
        archive_taiex = (
            archive.select(
                [
                    _date_column_expr("date").alias("date"),
                    _num_expr("closing_index").alias("_taiex"),
                ]
            )
            .drop_nulls(["date", "_taiex"])
            .group_by("date")
            .agg(pl.col("_taiex").last())
            .sort("date")
            .with_columns(
                [
                    pl.lit(None, dtype=pl.Float64).alias("_reported_pct"),
                    pl.lit(0, dtype=pl.Int8).alias("_source_priority"),
                ]
            )
        )
    else:
        archive_taiex = pl.DataFrame()

    index_frame = _read_optional(input_dir, "twse_market_index")
    if not index_frame.is_empty() and "指數" in index_frame.columns:
        index_name = pl.col("指數").cast(pl.Utf8, strict=False)
        ind_taiex = (
            index_frame.filter(index_name.str.contains("發行量加權股價指數"))
            .select(
                [
                    _date_column_expr("date").alias("date"),
                    _num_expr("收盤指數").alias("_taiex"),
                    (_num_expr("漲跌百分比(%)") / 100.0).alias("_reported_pct"),
                ]
            )
            .drop_nulls(["date", "_taiex"])
            .group_by("date")
            .agg(
                [
                    pl.col("_taiex").last(),
                    pl.col("_reported_pct").drop_nulls().last(),
                ]
            )
            .sort("date")
            .with_columns(pl.lit(1, dtype=pl.Int8).alias("_source_priority"))
        )
    else:
        ind_taiex = pl.DataFrame()

    if not archive_taiex.is_empty() and not ind_taiex.is_empty():
        overlap = archive_taiex.select(
            ["date", pl.col("_taiex").alias("_archive_close")]
        ).join(
            ind_taiex.select(["date", pl.col("_taiex").alias("_ind_close")]),
            on="date",
            how="inner",
        )
        mismatched = overlap.filter(
            (pl.col("_archive_close") - pl.col("_ind_close")).abs() > 0.011
        )
        if not mismatched.is_empty():
            examples = mismatched.head(5).to_dicts()
            raise ValueError(
                "TWSE TAIEX close mismatch between MI_5MINS_HIST and "
                f"MI_INDEX IND; examples={examples}"
            )

    sources = [
        frame for frame in (archive_taiex, ind_taiex) if not frame.is_empty()
    ]
    if not sources:
        return pl.DataFrame()
    taiex = (
        pl.concat(sources, how="diagonal_relaxed")
        .sort(["date", "_source_priority"])
        .group_by("date", maintain_order=True)
        .agg(
            [
                pl.col("_taiex").drop_nulls().last().alias("_taiex"),
                pl.col("_reported_pct").drop_nulls().last().alias("_reported_pct"),
            ]
        )
        .sort("date")
        .with_columns(pl.col("_taiex").shift(1).alias("_previous_taiex"))
        .with_columns(
            pl.coalesce(
                [
                    pl.col("_reported_pct"),
                    pl.when(pl.col("_previous_taiex") > 0.0)
                    .then(pl.col("_taiex") / pl.col("_previous_taiex") - 1.0)
                    .otherwise(None),
                ]
            ).alias("twpub_twse_taiex_pct")
        )
    )
    return taiex.with_columns(
        [
            pl.lit(market_symbol).alias("symbol"),
            # This is the receipt-backed exchange-session marker consumed by
            # panel construction.  Keep it on the TAIEX market row only: dates
            # from FX, macro, or futures features are not evidence that the
            # cash-equity market traded.
            pl.lit(1.0).alias("_twpub_official_traded"),
            pl.col("_taiex").alias("twpub_twse_taiex_raw"),
            _positive_log(pl.col("_taiex")).alias("twpub_twse_taiex_log"),
            _safe_log(pl.col("_taiex") / pl.col("_previous_taiex")).alias(
                "twpub_twse_taiex_logret_1d"
            ),
        ]
    ).select(
        [
            "date",
            "symbol",
            "_twpub_official_traded",
            "twpub_twse_taiex_raw",
            "twpub_twse_taiex_log",
            "twpub_twse_taiex_logret_1d",
            "twpub_twse_taiex_pct",
        ]
    )


def _build_valuation_features(input_dir: Path) -> pl.DataFrame:
    frames: list[pl.DataFrame] = []
    for name, code_col, dividend_col in (
        ("twse_daily_valuation", "證券代號", None),
        ("tpex_daily_valuation", "股票代號", "每股股利"),
    ):
        frame = _read_optional(input_dir, name)
        if frame.is_empty():
            continue
        columns = set(frame.columns)
        pe = _num_expr("本益比")
        pb = _num_expr("股價淨值比")
        dividend_yield_pct = _num_expr("殖利率(%)")
        dividend = _optional_num_expr(columns, dividend_col) if dividend_col else pl.lit(None, dtype=pl.Float64)
        frames.append(
            frame.select(
                [
                    _date_column_expr("date").alias("date"),
                    _symbol_expr(code_col).alias("symbol"),
                    pe.alias("twpub_pe_raw"),
                    pb.alias("twpub_pb_raw"),
                    dividend_yield_pct.alias("twpub_dividend_yield_pct_raw"),
                    dividend.alias("twpub_dividend_per_share_raw"),
                    _positive_log1p(pe).alias("twpub_pe_log"),
                    _positive_log1p(pb).alias("twpub_pb_log"),
                    (dividend_yield_pct / 100.0).alias("twpub_dividend_yield"),
                    _positive_log1p(dividend).alias("twpub_dividend_per_share_log"),
                ]
            )
        )
    return pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()


def _build_margin_features(input_dir: Path) -> pl.DataFrame:
    frames: list[pl.DataFrame] = []
    session_lookup = _next_exchange_session_lookup(input_dir)
    twse = _read_optional(input_dir, "twse_margin_balance")
    if not twse.is_empty():
        columns = set(twse.columns)
        margin_prev = _num_expr("前日餘額")
        margin_today = _num_expr("今日餘額")
        short_prev = _num_expr("前日餘額_2")
        short_today = _num_expr("今日餘額_2")
        short_limit_next = _optional_num_expr(columns, "次一營業日限額_2")
        has_margin_schema = {
            "今日餘額_2",
            "次一營業日限額_2",
            "註記",
        }.issubset(columns)
        short_note = (
            _text_expr("註記").fill_null("")
            if "註記" in columns
            else pl.lit(None, dtype=pl.Utf8)
        )
        valid_short_evidence = (
            short_today.is_finite()
            & short_limit_next.is_finite()
            & (short_today >= 0.0)
            & (short_limit_next >= 0.0)
            & (short_today == short_today.round(0))
            & (short_limit_next == short_limit_next.round(0))
            & ~short_note.str.contains("X", literal=True).fill_null(True)
        ).fill_null(False) if has_margin_schema else pl.lit(False)
        short_capacity_shares = (
            pl.when(valid_short_evidence)
            .then(
                (short_limit_next - short_today)
                .clip(lower_bound=0.0)
                * TW_MARGIN_BOARD_LOT_SHARES
            )
            .otherwise(0.0)
        )
        source = twse.select(
            [
                _date_column_expr("date").alias("date"),
                _symbol_expr("代號").alias("symbol"),
                margin_today.alias("twpub_margin_balance_lots_raw"),
                short_today.alias("twpub_short_balance_lots_raw"),
                _num_expr("買進").alias("twpub_margin_buy_lots_raw"),
                _num_expr("賣出").alias("twpub_margin_sell_lots_raw"),
                _num_expr("賣出_2").alias("twpub_short_sell_lots_raw"),
                _num_expr("買進_2").alias("twpub_short_buy_lots_raw"),
                _positive_log1p(margin_today).alias("twpub_margin_balance_log"),
                _signed_asinh(margin_today - margin_prev).alias("twpub_margin_balance_chg"),
                _positive_log1p(short_today).alias("twpub_short_balance_log"),
                _signed_asinh(short_today - short_prev).alias("twpub_short_balance_chg"),
                _signed_asinh(_num_expr("買進")).alias("twpub_margin_buy_flow"),
                _signed_asinh(_num_expr("賣出")).alias("twpub_margin_sell_flow"),
                _signed_asinh(_num_expr("賣出_2")).alias("twpub_short_sell_flow"),
                _signed_asinh(_num_expr("買進_2")).alias("twpub_short_buy_flow"),
                valid_short_evidence.cast(pl.Float64).alias(
                    "_twpub_margin_short_evidence_next_session"
                ),
                short_capacity_shares.alias(
                    "_twpub_short_capacity_shares_next_session"
                ),
            ]
        )
        frames.append(
            _shift_post_close_features_to_next_session(source, session_lookup)
        )
        frames.append(
            source.select(
                "date",
                "symbol",
                "_twpub_margin_short_evidence_next_session",
                "_twpub_short_capacity_shares_next_session",
            )
        )

    tpex = _read_optional(input_dir, "tpex_margin_balance")
    if not tpex.is_empty():
        columns = set(tpex.columns)
        margin_prev = _num_expr("前資餘額(張)")
        margin_today = _num_expr("資餘額")
        short_prev = _num_expr("前券餘額(張)")
        short_today = _num_expr("券餘額")
        short_limit_next = _optional_num_expr(columns, "券限額")
        has_margin_schema = {
            "券餘額",
            "券限額",
            "備註",
        }.issubset(columns)
        short_note = (
            _text_expr("備註").fill_null("")
            if "備註" in columns
            else pl.lit(None, dtype=pl.Utf8)
        )
        valid_short_evidence = (
            short_today.is_finite()
            & short_limit_next.is_finite()
            & (short_today >= 0.0)
            & (short_limit_next >= 0.0)
            & (short_today == short_today.round(0))
            & (short_limit_next == short_limit_next.round(0))
            & ~short_note.str.contains("X", literal=True).fill_null(True)
        ).fill_null(False) if has_margin_schema else pl.lit(False)
        short_capacity_shares = (
            pl.when(valid_short_evidence)
            .then(
                (short_limit_next - short_today)
                .clip(lower_bound=0.0)
                * TW_MARGIN_BOARD_LOT_SHARES
            )
            .otherwise(0.0)
        )
        source = tpex.select(
            [
                _date_column_expr("date").alias("date"),
                _symbol_expr("代號").alias("symbol"),
                margin_today.alias("twpub_margin_balance_lots_raw"),
                short_today.alias("twpub_short_balance_lots_raw"),
                _num_expr("資買").alias("twpub_margin_buy_lots_raw"),
                _num_expr("資賣").alias("twpub_margin_sell_lots_raw"),
                _num_expr("券賣").alias("twpub_short_sell_lots_raw"),
                _num_expr("券買").alias("twpub_short_buy_lots_raw"),
                _positive_log1p(margin_today).alias("twpub_margin_balance_log"),
                _signed_asinh(margin_today - margin_prev).alias("twpub_margin_balance_chg"),
                _positive_log1p(short_today).alias("twpub_short_balance_log"),
                _signed_asinh(short_today - short_prev).alias("twpub_short_balance_chg"),
                _signed_asinh(_num_expr("資買")).alias("twpub_margin_buy_flow"),
                _signed_asinh(_num_expr("資賣")).alias("twpub_margin_sell_flow"),
                _signed_asinh(_num_expr("券賣")).alias("twpub_short_sell_flow"),
                _signed_asinh(_num_expr("券買")).alias("twpub_short_buy_flow"),
                valid_short_evidence.cast(pl.Float64).alias(
                    "_twpub_margin_short_evidence_next_session"
                ),
                short_capacity_shares.alias(
                    "_twpub_short_capacity_shares_next_session"
                ),
            ]
        )
        frames.append(
            _shift_post_close_features_to_next_session(source, session_lookup)
        )
        frames.append(
            source.select(
                "date",
                "symbol",
                "_twpub_margin_short_evidence_next_session",
                "_twpub_short_capacity_shares_next_session",
            )
        )
    nonempty = [frame for frame in frames if not frame.is_empty()]
    return pl.concat(nonempty, how="diagonal_relaxed") if nonempty else pl.DataFrame()


def _build_institutional_features(input_dir: Path) -> pl.DataFrame:
    frames: list[pl.DataFrame] = []
    session_lookup = _next_exchange_session_lookup(input_dir)
    twse = _read_optional(input_dir, "twse_institutional_trades")
    if not twse.is_empty():
        # T86 changed its historical foreign-investor header.  Each source
        # row has one of these columns, not both; selecting only the modern
        # name silently erased most TWSE values before 2018.
        foreign_net = pl.coalesce(
            _optional_num_expr(set(twse.columns), "外陸資買賣超股數(不含外資自營商)"),
            _optional_num_expr(set(twse.columns), "外資買賣超股數"),
        )
        source = twse.select(
            [
                _date_column_expr("date").alias("date"),
                _symbol_expr("證券代號").alias("symbol"),
                foreign_net.alias(
                    "twpub_foreign_net_buy_shares_raw"
                ),
                _num_expr("投信買賣超股數").alias(
                    "twpub_investment_trust_net_buy_shares_raw"
                ),
                _num_expr("自營商買賣超股數").alias(
                    "twpub_dealer_net_buy_shares_raw"
                ),
                _num_expr("三大法人買賣超股數").alias(
                    "twpub_institutional_net_buy_shares_raw"
                ),
                _signed_asinh(foreign_net).alias("twpub_foreign_net_buy_flow"),
                _signed_asinh(_num_expr("投信買賣超股數")).alias(
                    "twpub_investment_trust_net_buy_flow"
                ),
                _signed_asinh(_num_expr("自營商買賣超股數")).alias(
                    "twpub_dealer_net_buy_flow"
                ),
                _signed_asinh(_num_expr("三大法人買賣超股數")).alias(
                    "twpub_institutional_net_buy_flow"
                ),
            ]
        )
        frames.append(
            _shift_post_close_features_to_next_session(source, session_lookup)
        )

    tpex = _read_optional(input_dir, "tpex_institutional_trades")
    if not tpex.is_empty():
        columns = set(tpex.columns)
        total_col = (
            "三大法人買賣超股數合計"
            if "三大法人買賣超股數合計" in columns
            else "三大法人買賣超股數"
        )
        source = tpex.select(
            [
                _date_column_expr("date").alias("date"),
                _symbol_expr("代號").alias("symbol"),
                _num_expr("外資及陸資淨買股數").alias(
                    "twpub_foreign_net_buy_shares_raw"
                ),
                _num_expr("投信淨買股數").alias(
                    "twpub_investment_trust_net_buy_shares_raw"
                ),
                _num_expr("自營淨買股數").alias(
                    "twpub_dealer_net_buy_shares_raw"
                ),
                _num_expr(total_col).alias(
                    "twpub_institutional_net_buy_shares_raw"
                ),
                _signed_asinh(_num_expr("外資及陸資淨買股數")).alias(
                    "twpub_foreign_net_buy_flow"
                ),
                _signed_asinh(_num_expr("投信淨買股數")).alias(
                    "twpub_investment_trust_net_buy_flow"
                ),
                _signed_asinh(_num_expr("自營淨買股數")).alias(
                    "twpub_dealer_net_buy_flow"
                ),
                _signed_asinh(_num_expr(total_col)).alias(
                    "twpub_institutional_net_buy_flow"
                ),
            ]
        )
        frames.append(
            _shift_post_close_features_to_next_session(source, session_lookup)
        )
    nonempty = [frame for frame in frames if not frame.is_empty()]
    return pl.concat(nonempty, how="diagonal_relaxed") if nonempty else pl.DataFrame()


def _build_tdcc_features(input_dir: Path) -> pl.DataFrame:
    normalized: list[pl.DataFrame] = []
    # data.gov.tw resolves to TDCC's maintained opendata resource and is the
    # canonical daily source.  The direct openapi-t snapshot remains useful for
    # older rows that the current snapshot no longer carries.
    for priority, name in enumerate(
        (
            "tdcc_shareholding_distribution",
            "data_gov_tdcc_shareholding_distribution",
        )
    ):
        frame = _read_optional(input_dir, name)
        if frame.is_empty():
            continue
        columns = set(frame.columns)
        date_columns = [
            candidate
            for candidate in ("資料日期", "\ufeff資料日期")
            if candidate in columns
        ]
        if not date_columns:
            continue
        source_date = pl.coalesce(
            [_yyyymmdd_expr(candidate) for candidate in date_columns]
        )
        availability_date = _periodic_available_date_expr(columns, source_date)
        normalized.append(
            frame.select(
                [
                    source_date.alias("_source_date"),
                    availability_date.alias("date"),
                    _symbol_expr("證券代號").alias("symbol"),
                    _num_expr("持股分級").alias("_tier"),
                    (_num_expr("占集保庫存數比例%") / 100.0).alias("_ratio"),
                    _num_expr("人數").alias("_holders"),
                    pl.lit(priority, dtype=pl.Int8).alias("_source_priority"),
                    _text_expr("_downloaded_at_utc").alias("_observed_at")
                    if "_downloaded_at_utc" in columns
                    else pl.lit("").alias("_observed_at"),
                ]
            )
        )
    if not normalized:
        return pl.DataFrame()
    frame = _map_available_dates_to_sessions(
        pl.concat(normalized, how="vertical_relaxed"), input_dir
    )
    if frame.is_empty():
        return pl.DataFrame()
    return (
        frame.drop_nulls(["_source_date", "date", "symbol", "_tier"])
        .sort(["date", "symbol", "_source_date", "_tier", "_observed_at", "_source_priority"])
        .unique(
            subset=["date", "_source_date", "symbol", "_tier"],
            keep="last",
            maintain_order=True,
        )
        .join(
            frame.group_by(["date", "symbol"]).agg(
                pl.col("_source_date").max().alias("_latest_source_date")
            ),
            on=["date", "symbol"],
            how="left",
            validate="m:1",
        )
        .filter(pl.col("_source_date") == pl.col("_latest_source_date"))
        .group_by(["date", "symbol"])
        .agg(
            [
                pl.when(pl.col("_tier") <= 5).then(pl.col("_ratio")).otherwise(0.0).sum().alias(
                    "twpub_tdcc_retail_holder_ratio"
                ),
                pl.when(pl.col("_tier") >= 13).then(pl.col("_ratio")).otherwise(0.0).sum().alias(
                    "twpub_tdcc_large_holder_ratio"
                ),
                _positive_log1p(pl.col("_holders").sum()).alias("twpub_tdcc_holder_count_log"),
            ]
        )
    )


def _build_company_basic_features(input_dir: Path) -> pl.DataFrame:
    frames: list[pl.DataFrame] = []
    specs = [
        (
            "twse_listed_company_basic",
            "公司代號",
            "出表日期",
            "實收資本額",
            "已發行普通股數或TDR原股發行股數",
            "私募股數",
            "成立日期",
            "上市日期",
            "普通股每股面額",
            "產業別",
            "外國企業註冊地國",
            "特別股",
        ),
        (
            "tpex_basic_company",
            "SecuritiesCompanyCode",
            "Date",
            "Paidin.Capital.NTDollars",
            "IssueShares",
            "PrivateStock.shares",
            "DateOfIncorporation",
            "DateOfListing",
            "ParValueOfCommonStock",
            "SecuritiesIndustryCode",
            "Registration",
            "PreferredStock.shares",
        ),
    ]
    for (
        name,
        symbol_col,
        report_date_col,
        capital_col,
        shares_col,
        private_col,
        incorporation_col,
        listing_col,
        par_col,
        industry_col,
        foreign_col,
        preferred_col,
    ) in specs:
        frame = _read_optional(input_dir, name)
        if frame.is_empty():
            continue
        columns = set(frame.columns)
        report_date = _snapshot_date_expr(columns)
        incorporation = _date_column_expr(incorporation_col)
        listing = _date_column_expr(listing_col)
        selected = frame.select(
            [
                report_date.alias("date"),
                _symbol_expr(symbol_col).alias("symbol"),
                _positive_log1p(_optional_num_expr(columns, capital_col)).alias("twpub_company_paidin_capital_log"),
                _positive_log1p(_optional_num_expr(columns, shares_col)).alias("twpub_company_issued_shares_log"),
                _positive_log1p(_optional_num_expr(columns, private_col)).alias("twpub_company_private_shares_log"),
                incorporation.alias("_incorporation_date"),
                listing.alias("_listing_date"),
                _positive_log1p(_optional_num_expr(columns, par_col)).alias("twpub_company_par_value_log"),
                _optional_num_expr(columns, industry_col).alias("twpub_company_industry_code"),
                pl.when(pl.col(foreign_col).cast(pl.Utf8, strict=False).str.strip_chars() != "")
                .then(1.0)
                .otherwise(0.0)
                .alias("twpub_company_is_foreign")
                if foreign_col in columns
                else _lit_null_float().alias("twpub_company_is_foreign"),
                pl.when(_optional_num_expr(columns, preferred_col).fill_null(0.0) > 0.0)
                .then(1.0)
                .otherwise(0.0)
                .alias("twpub_company_has_preferred_stock"),
            ]
        )
        frames.append(
            selected.with_columns(
                [
                    (
                        (pl.col("date").dt.year() - pl.col("_incorporation_date").dt.year()).cast(pl.Float64)
                    ).alias("twpub_company_age_years"),
                    (
                        (pl.col("date").dt.year() - pl.col("_listing_date").dt.year()).cast(pl.Float64)
                    ).alias("twpub_company_listed_age_years"),
                ]
            ).drop(["_incorporation_date", "_listing_date"])
        )
    return pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()


def _build_dividend_features(input_dir: Path) -> pl.DataFrame:
    frames: list[pl.DataFrame] = []
    twse = _read_optional(input_dir, "twse_listed_dividend")
    if not twse.is_empty():
        columns = set(twse.columns)
        cash_per_share = _sum_num_expr(
            columns,
            [
                "股東配發-盈餘分配之現金股利(元/股)",
                "股東配發-法定盈餘公積發放之現金(元/股)",
                "股東配發-資本公積發放之現金(元/股)",
            ],
        )
        stock_per_share = _sum_num_expr(
            columns,
            [
                "股東配發-盈餘轉增資配股(元/股)",
                "股東配發-法定盈餘公積轉增資配股(元/股)",
                "股東配發-資本公積轉增資配股(元/股)",
            ],
        )
        frames.append(
            twse.select(
                [
                    _snapshot_not_before_expr(
                        columns,
                        pl.coalesce(
                            [
                                _date_column_expr("董事會（擬議）股利分派日"),
                                _date_column_expr("股東會日期"),
                                _date_column_expr("出表日期"),
                                _date_column_expr("date"),
                            ]
                        ),
                    ).alias("date"),
                    _symbol_expr("公司代號").alias("symbol"),
                    cash_per_share.alias("twpub_dividend_cash_per_share"),
                    stock_per_share.alias("twpub_dividend_stock_per_share"),
                    _positive_log1p(_optional_num_expr(columns, "股東配發-股東配發之現金(股利)總金額(元)")).alias(
                        "twpub_dividend_total_cash_log"
                    ),
                    _positive_log1p(_optional_num_expr(columns, "股東配發-股東配股總股數(股)")).alias(
                        "twpub_dividend_total_stock_log"
                    ),
                    _binary_contains_expr(columns, "決議（擬議）進度", "股東會").alias("twpub_dividend_confirmed"),
                    _binary_contains_expr(columns, "決議（擬議）進度", "董事會").alias("twpub_dividend_board_approved"),
                    _optional_num_expr(columns, "股利年度").alias("_twpub_dividend_year"),
                ]
            )
        )
    tpex = _read_optional(input_dir, "tpex_dividend")
    if not tpex.is_empty():
        columns = set(tpex.columns)
        cash_per_share = _sum_num_expr(
            columns,
            [
                "股東配發內容-盈餘分配之現金股利(元/股)",
                "股東配發內容-法定盈餘公積、資本公積發放之現金(元/股)",
            ],
        )
        stock_per_share = _sum_num_expr(
            columns,
            [
                "股東配發內容-盈餘轉增資配股(元/股)",
                "股東配發內容-法定盈餘公積、資本公積轉增資配股(元/股)",
            ],
        )
        frames.append(
            tpex.select(
                [
                    _snapshot_not_before_expr(
                        columns,
                        pl.coalesce(
                            [
                                _date_column_expr("董事會決議通過股利分派日"),
                                _date_column_expr("股東會日期配盈餘/待彌補虧損(元)"),
                                _date_column_expr("出表日期"),
                                _date_column_expr("date"),
                            ]
                        ),
                    ).alias("date"),
                    _symbol_expr("公司代號").alias("symbol"),
                    cash_per_share.alias("twpub_dividend_cash_per_share"),
                    stock_per_share.alias("twpub_dividend_stock_per_share"),
                    _positive_log1p(_optional_num_expr(columns, "股東配發內容-股東配發之現金(股利)總金額(元)")).alias(
                        "twpub_dividend_total_cash_log"
                    ),
                    _positive_log1p(_optional_num_expr(columns, "股東配發內容-股東配股總股數(股)")).alias(
                        "twpub_dividend_total_stock_log"
                    ),
                    pl.lit(None, dtype=pl.Float64).alias("twpub_dividend_confirmed"),
                    pl.lit(1.0).alias("twpub_dividend_board_approved"),
                    _optional_num_expr(columns, "股利年度").alias("_twpub_dividend_year"),
                ]
            )
        )
    return pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()


def _build_ex_dividend_preview_features(input_dir: Path) -> pl.DataFrame:
    frame = _read_optional(input_dir, "twse_ex_dividend_preview")
    if frame.is_empty():
        return pl.DataFrame()
    columns = set(frame.columns)
    stock_dividend = _sum_num_expr(columns, ["StockDividendRatio", "SubscriptionRatio"])
    return frame.select(
        [
            _snapshot_date_expr(columns).alias("date"),
            _symbol_expr("Code").alias("symbol"),
            pl.lit(1.0).alias("twpub_exdiv_known"),
            _optional_num_expr(columns, "CashDividend").alias("twpub_exdiv_cash_dividend"),
            stock_dividend.alias("twpub_exdiv_stock_dividend_ratio"),
            _optional_num_expr(columns, "SubscriptionRatio").alias("twpub_exdiv_subscription_ratio"),
            _positive_log1p(_optional_num_expr(columns, "SubscriptionPricePerShare")).alias(
                "twpub_exdiv_subscription_price_log"
            ),
        ]
    )


def _build_material_info_features(input_dir: Path) -> pl.DataFrame:
    frame = _read_optional(input_dir, "twse_listed_material_info")
    if frame.is_empty():
        return pl.DataFrame()
    columns = set(frame.columns)
    event_date = _date_column_expr("事實發生日") if "事實發生日" in columns else _date_column_expr("發言日期")
    base = frame.select(
        [
            _material_info_available_date_expr(columns).alias("date"),
            _symbol_expr("公司代號").alias("symbol"),
            _num_from_text_digits_expr("符合條款").alias("_clause")
            if "符合條款" in columns
            else _lit_null_float().alias("_clause"),
            event_date.alias("_event_date"),
        ]
    ).drop_nulls(["date", "symbol"])
    if base.is_empty():
        return base
    base = _map_available_dates_to_sessions(base, input_dir)
    return base.group_by(["date", "symbol"]).agg(
        [
            (pl.len().cast(pl.Float64) + 1.0).log().alias("twpub_material_event_count_log"),
            _positive_log1p(pl.col("_clause").max()).alias("twpub_material_clause_log"),
            _days_between_expr(pl.col("date").max(), pl.col("_event_date").min()).cast(pl.Float64).alias(
                "twpub_material_fact_lag_days"
            ),
        ]
    )


def _build_attention_disposal_features(input_dir: Path) -> pl.DataFrame:
    frames: list[pl.DataFrame] = []
    specs = [
        ("twse_notice_stock", "Code", "Date", "ClosingPrice", "PE", True),
        ("tpex_attention_stock", "SecuritiesCompanyCode", "Date", "ClosePrice", "PriceEarningRatio", True),
        ("twse_disposal_stock", "Code", "Date", None, None, False),
        ("tpex_disposal_stock", "SecuritiesCompanyCode", "Date", None, None, False),
    ]
    for name, symbol_col, date_col, close_col, pe_col, is_attention in specs:
        frame = _read_optional(input_dir, name)
        if frame.is_empty():
            continue
        columns = set(frame.columns)
        symbol = _symbol_expr(symbol_col)
        base = frame.select(
            [
                _snapshot_date_expr(columns).alias("date"),
                symbol.alias("symbol"),
                _optional_num_expr(columns, close_col).alias("_close"),
                _optional_num_expr(columns, pe_col).alias("_pe"),
            ]
        ).drop_nulls(["date", "symbol"]).filter(pl.col("symbol") != "")
        if base.is_empty():
            continue
        if is_attention:
            frames.append(
                base.group_by(["date", "symbol"]).agg(
                    [
                        pl.lit(1.0).alias("_twpub_attention_flag"),
                        (pl.len().cast(pl.Float64) + 1.0).log().alias("twpub_attention_count_log"),
                        _positive_log1p(pl.col("_close").max()).alias("twpub_attention_close_log"),
                        _positive_log1p(pl.col("_pe").max()).alias("twpub_attention_pe_log"),
                    ]
                )
            )
        else:
            frames.append(
                base.group_by(["date", "symbol"]).agg(
                    [
                        pl.lit(1.0).alias("_twpub_disposal_flag"),
                        (pl.len().cast(pl.Float64) + 1.0).log().alias("twpub_disposal_count_log"),
                    ]
                )
            )
    return pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()


def _build_usdtwd_features(input_dir: Path, *, market_symbol: str) -> pl.DataFrame:
    frame = _read_optional(input_dir, "cbc_usdtwd_closing_rate")
    if frame.is_empty() or "日期" not in frame.columns or "NTD/USD" not in frame.columns:
        return pl.DataFrame()
    rate = _num_expr("NTD/USD")
    return (
        _macro_release_rows(
            frame, input_dir, _yyyymmdd_expr("日期"), [rate.alias("_rate")]
        )
        .drop_nulls(["date"])
        .group_by("date")
        .agg(pl.col("_rate").drop_nulls().last().alias("_rate"))
        .sort("date")
        .with_columns(
            [
                pl.lit(market_symbol).alias("symbol"),
                pl.col("_rate").alias("twpub_usdtwd_raw"),
                _safe_log(pl.col("_rate")).alias("twpub_usdtwd_log"),
                _safe_log(pl.col("_rate") / pl.col("_rate").shift(1)).alias("twpub_usdtwd_logret_1d"),
            ]
        )
        .select(["date", "symbol", "twpub_usdtwd_raw", "twpub_usdtwd_log", "twpub_usdtwd_logret_1d"])
    )


def _build_cbc_overnight_rate_features(input_dir: Path, *, market_symbol: str) -> pl.DataFrame:
    sources: list[pl.DataFrame] = []
    bulk = _read_optional(input_dir, "cbc_overnight_rate")
    if not bulk.is_empty() and {"日期", "利率[%]"} <= set(bulk.columns):
        sources.append(bulk)
    pages_path = input_dir / "supplemental/cbc_overnight_official_pages.parquet"
    if pages_path.is_file():
        pages = pl.read_parquet(pages_path)
        if {"subject_date", "rate_pct", "status", "observed_at_utc"} <= set(pages.columns):
            pages = pages.filter(pl.col("status") == "ok").select(
                pl.col("subject_date").alias("日期"),
                pl.col("rate_pct").alias("利率[%]"),
                pl.col("observed_at_utc").alias("_downloaded_at_utc"),
            )
            sources.append(pages)
    events: list[pl.DataFrame] = []
    for priority, frame in enumerate(sources):
        values = (
            _macro_release_rows(
                frame, input_dir, _date_column_expr("日期"),
                [(_num_expr("利率[%]") / 100.0).alias("_rate")],
            )
            .drop_nulls(["date", "_rate"])
            .group_by("date")
            .agg(pl.col("_rate").last())
            .sort("date")
            .with_columns(
                pl.col("_rate").alias("twpub_cbc_overnight_rate"),
                (pl.col("_rate") - pl.col("_rate").shift(1)).alias("twpub_cbc_overnight_rate_chg"),
                pl.lit(priority).alias("_priority"),
            )
            .drop("_rate")
        )
        if not values.is_empty():
            events.append(values)
    if not events:
        return pl.DataFrame()
    return (
        pl.concat(events)
        .sort(["date", "_priority"])
        .group_by("date")
        .agg(
            pl.col("twpub_cbc_overnight_rate").last(),
            pl.col("twpub_cbc_overnight_rate_chg").last(),
        )
        .with_columns(pl.lit(market_symbol).alias("symbol"))
        .select(["date", "symbol", "twpub_cbc_overnight_rate", "twpub_cbc_overnight_rate_chg"])
    )


def _macro_release_rows(
    frame: pl.DataFrame,
    input_dir: Path,
    period_end,
    values: list[pl.Expr],
) -> pl.DataFrame:
    """Keep period ordering separate from release/session ordering."""

    rows = frame.select(
        [
            period_end.alias("_period"),
            _periodic_available_date_expr(set(frame.columns), period_end).alias("date"),
            _text_expr("_downloaded_at_utc").alias("_observed_at")
            if "_downloaded_at_utc" in frame.columns
            else pl.lit("").alias("_observed_at"),
            *values,
        ]
    ).drop_nulls(["_period", "date"])
    if rows.is_empty():
        return rows.drop("_period", "_observed_at")
    return (
        _map_available_dates_to_sessions(rows, input_dir)
        .sort(["date", "_period", "_observed_at"])
        .drop("_period", "_observed_at")
    )


def _dgbas_headline_release_rows(
    archive: pl.DataFrame,
    input_dir: Path,
    *,
    source: str,
    metric: str,
    value_column: str,
    percent_as_fraction: bool = True,
) -> pl.DataFrame:
    """Use original release values, never a current table relabelled as old."""

    required = {"source", "period", "published_on", "release_id", "release_kind",
                "metric", "value_pct", "value_evidence", "html_sha256"}
    if archive.is_empty() or not required <= set(archive.columns):
        return pl.DataFrame(schema={"date": pl.Date, value_column: pl.Float64})
    rows = archive.filter(
        (pl.col("source") == source)
        & (pl.col("metric") == metric)
        & pl.col("value_evidence").is_in(
            ["official_release_headline", "original_attachment_cpi_text",
             "original_attachment_unemployment_text"]
        )
        & pl.col("value_pct").is_not_null()
        & ((pl.col("release_kind") == "direct_attachment") | pl.col("html_sha256").is_not_null())
    )
    if rows.is_empty():
        return pl.DataFrame(schema={"date": pl.Date, value_column: pl.Float64})
    conflicting = (
        rows.group_by(["period", "published_on"])
        .agg(pl.col("value_pct").n_unique().alias("distinct_values"))
        .filter(pl.col("distinct_values") > 1)
    )
    if not conflicting.is_empty():
        raise ValueError(f"conflicting same-day DGBAS {source} release headlines")
    # Original document clocks override the historical official schedule.
    # DGBAS moved CPI, employment and GDP advance estimates to 08:30 on
    # 2012-05-07, then moved all statistics to 16:00 on 2017-07-01.
    # GDP preliminary releases were explicitly exempt from the 08:30 era.
    # https://www.stat.gov.tw/News_Content.aspx?n=3703&s=23721
    # https://www.stat.gov.tw/News_Content.aspx?n=2642&s=97506
    published_day = pl.col("published_on").str.strptime(pl.Date, "%Y-%m-%d", strict=False)
    is_gdp_advance = (
        pl.col("title").cast(pl.Utf8, strict=False).str.contains("概估")
        if "title" in rows.columns else pl.lit(False)
    )
    scheduled_preopen = published_day.is_between(
        date(2012, 5, 7), date(2017, 6, 30), closed="both"
    ) & (
        pl.lit(source in {"cpi", "unemployment"})
        | (pl.lit(source == "gdp") & is_gdp_advance)
    )
    if {"published_time_precision", "published_clock_taipei"} <= set(rows.columns):
        before_open = pl.when(
            pl.col("published_time_precision") == "official_document_time"
        ).then(
            pl.col("published_clock_taipei") < "09:00:00"
        ).otherwise(scheduled_preopen)
    else:
        before_open = scheduled_preopen
    available_day = pl.when(before_open).then(published_day).otherwise(
        published_day.dt.offset_by("1d")
    )
    release_rows = rows.select(
        available_day.alias("date"),
        pl.col("period").alias("_period"),
        pl.col("release_id").alias("_release_id"),
        (
            pl.col("value_pct").cast(pl.Float64, strict=False)
            / (100.0 if percent_as_fraction else 1.0)
        ).alias(value_column),
    ).drop_nulls(["date", value_column])
    if release_rows.is_empty():
        return pl.DataFrame(schema={"date": pl.Date, value_column: pl.Float64})
    return (
        _map_available_dates_to_sessions(release_rows, input_dir)
        .sort(["date", "_period", "_release_id"])
        .group_by("date")
        .agg(pl.col(value_column).last())
    )


def _merge_bulk_and_official_release_values(
    bulk: pl.DataFrame, official: pl.DataFrame, value_column: str
) -> pl.DataFrame:
    frames = []
    for priority, frame in enumerate((bulk, official)):
        if not frame.is_empty():
            frames.append(frame.with_columns(pl.lit(priority).alias("_priority")))
    if not frames:
        return pl.DataFrame(schema={"date": pl.Date, value_column: pl.Float64})
    return (
        pl.concat(frames)
        .sort(["date", "_priority"])
        .group_by("date")
        .agg(pl.col(value_column).drop_nulls().last())
    )


def _cbc_fx_release_rows(input_dir: Path) -> pl.DataFrame:
    archive = _read_optional(input_dir, "cbc_fx_reserve_release_vintages")
    value_column = "_fx_reserves"
    required = {"period", "published_on", "metric", "value", "value_evidence", "html_sha256"}
    if archive.is_empty() or not required <= set(archive.columns):
        return pl.DataFrame(schema={"date": pl.Date, value_column: pl.Float64})
    rows = archive.filter(
        (pl.col("metric") == "fx_reserves_usd_100m")
        & (pl.col("value_evidence") == "original_press_release_text")
        & pl.col("value").is_not_null()
        & pl.col("html_sha256").is_not_null()
    )
    if rows.is_empty():
        return pl.DataFrame(schema={"date": pl.Date, value_column: pl.Float64})
    conflicting = (
        rows.group_by(["period", "published_on"])
        .agg(pl.col("value").n_unique().alias("distinct_values"))
        .filter(pl.col("distinct_values") > 1)
    )
    if not conflicting.is_empty():
        raise ValueError("conflicting same-day CBC foreign-reserve release values")
    # The release page states a date but not a proven intra-day timestamp.
    # One 億 USD is 0.1 billion USD, the unit used by cbc_fx_reserves.
    release_rows = rows.select(
        pl.col("published_on").str.strptime(pl.Date, "%Y-%m-%d", strict=False)
        .dt.offset_by("1d").alias("date"),
        pl.col("period").alias("_period"),
        (pl.col("value").cast(pl.Float64, strict=False) / 10.0).alias(value_column),
    ).drop_nulls(["date", value_column])
    return (
        _map_available_dates_to_sessions(release_rows, input_dir)
        .sort(["date", "_period"])
        .group_by("date")
        .agg(pl.col(value_column).last())
    )


def _cbc_money_release_rows(input_dir: Path) -> pl.DataFrame:
    """Use only M1B/M2 year-on-year values stated in dated original releases."""

    schema = {"date": pl.Date, "_m1b_yoy": pl.Float64, "_m2_yoy": pl.Float64}
    archive = _read_optional(input_dir, "cbc_money_release_vintages")
    required = {"period", "published_on", "release_url", "metric", "value_pct",
                "value_evidence", "html_sha256"}
    if archive.is_empty() or not required <= set(archive.columns):
        return pl.DataFrame(schema=schema)
    rows = archive.filter(
        pl.col("metric").is_in(["m1b_yoy_pct", "m2_yoy_pct"])
        & (pl.col("value_evidence") == "original_press_release_text")
        & pl.col("value_pct").is_not_null()
        & pl.col("html_sha256").is_not_null()
    )
    if rows.is_empty():
        return pl.DataFrame(schema=schema)
    conflicting = (
        rows.group_by(["period", "published_on", "metric"])
        .agg(pl.col("value_pct").n_unique().alias("distinct_values"))
        .filter(pl.col("distinct_values") > 1)
    )
    if not conflicting.is_empty():
        raise ValueError("conflicting same-day CBC money-supply release values")
    # The original article gives a posting date; its historical intra-day
    # publication clock is not independently proven. The next verified market
    # session is therefore the earliest model-safe session.
    releases = rows.select(
        pl.col("published_on").str.strptime(pl.Date, "%Y-%m-%d", strict=False)
        .dt.offset_by("1d").alias("date"),
        pl.col("period").alias("_period"),
        pl.col("release_url").alias("_release_url"),
        pl.when(pl.col("metric") == "m1b_yoy_pct")
        .then(pl.col("value_pct")).otherwise(None).alias("_m1b_yoy"),
        pl.when(pl.col("metric") == "m2_yoy_pct")
        .then(pl.col("value_pct")).otherwise(None).alias("_m2_yoy"),
    ).drop_nulls(["date"])
    return (
        _map_available_dates_to_sessions(releases, input_dir)
        .sort(["date", "_period", "_release_url"])
        .group_by("date")
        .agg(pl.col("_m1b_yoy").drop_nulls().last(),
             pl.col("_m2_yoy").drop_nulls().last())
    )


def _build_cbc_monthly_macro_features(input_dir: Path, *, market_symbol: str) -> pl.DataFrame:
    frames: list[pl.DataFrame] = []
    official_fx = _cbc_fx_release_rows(input_dir)
    fx = _read_optional(input_dir, "cbc_fx_reserves")
    bulk_fx = pl.DataFrame(schema={"date": pl.Date, "_fx_reserves": pl.Float64})
    if not fx.is_empty() and "日期" in fx.columns and "金額" in fx.columns:
        bulk_fx = (
            _macro_release_rows(
                fx, input_dir, _month_period_available_expr("日期", lag_days=0),
                [_num_expr("金額").alias("_fx_reserves")],
            )
            .drop_nulls(["date"])
            .group_by("date")
            .agg(pl.col("_fx_reserves").drop_nulls().last().alias("_fx_reserves"))
        )
    reserves = _merge_bulk_and_official_release_values(bulk_fx, official_fx, "_fx_reserves")
    if not reserves.is_empty():
        frames.append(
            reserves.sort("date")
            .with_columns(
                [
                    pl.lit(market_symbol).alias("symbol"),
                    pl.col("_fx_reserves").alias("twpub_cbc_fx_reserves_usd_billion_raw"),
                    _positive_log(pl.col("_fx_reserves")).alias("twpub_cbc_fx_reserves_log"),
                    _safe_log(pl.col("_fx_reserves") / pl.col("_fx_reserves").shift(1)).alias(
                        "twpub_cbc_fx_reserves_chg"
                    ),
                ]
            )
            .select(["date", "symbol", "twpub_cbc_fx_reserves_usd_billion_raw", "twpub_cbc_fx_reserves_log", "twpub_cbc_fx_reserves_chg"])
        )
    money = _read_optional(input_dir, "cbc_money_aggregates")
    money_rows: list[pl.DataFrame] = []
    if not money.is_empty() and "期間" in money.columns:
        columns = set(money.columns)
        money_rows.append(
            _macro_release_rows(
                money, input_dir, _month_period_available_expr("期間", lag_days=0),
                [
                    _first_num_expr(columns, ["貨幣總計數-Ｍ１Ｂ-原始值", "貨幣總計數 -Ｍ１Ｂ-原始值"]).alias("_m1b"),
                    _first_num_expr(columns, ["貨幣總計數-Ｍ１Ｂ-年增率", "貨幣總計數 -Ｍ１Ｂ-年增率"]).alias("_m1b_yoy"),
                    _first_num_expr(columns, ["貨幣總計數-Ｍ２-原始值", "貨幣總計數 -Ｍ２-原始值"]).alias("_m2"),
                    _first_num_expr(columns, ["貨幣總計數-Ｍ２-年增率", "貨幣總計數 -Ｍ２-年增率"]).alias("_m2_yoy"),
                ]
            )
            .with_columns(pl.lit(0).alias("_priority"))
        )
    official_money = _cbc_money_release_rows(input_dir)
    if not official_money.is_empty():
        money_rows.append(official_money.with_columns(pl.lit(1).alias("_priority")))
    if money_rows:
        money_columns = ("_m1b", "_m1b_yoy", "_m2", "_m2_yoy")
        combined = pl.concat(money_rows, how="diagonal_relaxed")
        combined = combined.with_columns([
            pl.lit(None, dtype=pl.Float64).alias(name)
            for name in money_columns if name not in combined.columns
        ])
        combined_money = (
            combined
            .drop_nulls(["date"])
            .sort(["date", "_priority"])
            .group_by("date")
            .agg([pl.col(name).drop_nulls().last().alias(name)
                  for name in money_columns])
        )
        frames.append(
            combined_money.with_columns(
                [
                    pl.lit(market_symbol).alias("symbol"),
                    pl.col("_m1b").alias("twpub_cbc_m1b_raw"),
                    pl.col("_m1b_yoy").alias("twpub_cbc_m1b_yoy_pct_raw"),
                    pl.col("_m2").alias("twpub_cbc_m2_raw"),
                    pl.col("_m2_yoy").alias("twpub_cbc_m2_yoy_pct_raw"),
                    _positive_log(pl.col("_m1b")).alias("twpub_cbc_m1b_log"),
                    (pl.col("_m1b_yoy") / 100.0).alias("twpub_cbc_m1b_yoy"),
                    _positive_log(pl.col("_m2")).alias("twpub_cbc_m2_log"),
                    (pl.col("_m2_yoy") / 100.0).alias("twpub_cbc_m2_yoy"),
                ]
            )
            .select(["date", "symbol", "twpub_cbc_m1b_raw", "twpub_cbc_m1b_yoy_pct_raw", "twpub_cbc_m2_raw", "twpub_cbc_m2_yoy_pct_raw", "twpub_cbc_m1b_log", "twpub_cbc_m1b_yoy", "twpub_cbc_m2_log", "twpub_cbc_m2_yoy"])
        )
    return pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()


def _build_dgbas_macro_features(input_dir: Path, *, market_symbol: str) -> pl.DataFrame:
    frames: list[pl.DataFrame] = []
    release_archive = _read_optional(input_dir, "dgbas_release_vintages")
    # Preserve the numbers printed in the original release. The older
    # fraction-valued channels remain available for checkpoint compatibility.
    for source, metric, raw_name in (
        ("cpi", "cpi_yoy_pct", "twpub_dgbas_cpi_yoy_pct_raw"),
        ("unemployment", "unemployment_rate_pct", "twpub_dgbas_unemployment_pct_raw"),
        ("gdp", "gdp_yoy_pct", "twpub_dgbas_gdp_yoy_pct_raw"),
    ):
        original = _dgbas_headline_release_rows(
            release_archive, input_dir, source=source, metric=metric,
            value_column=raw_name, percent_as_fraction=False,
        )
        if not original.is_empty():
            frames.append(
                original.with_columns(pl.lit(market_symbol).alias("symbol"))
                .select("date", "symbol", raw_name)
            )
    official_cpi_yoy = _dgbas_headline_release_rows(
        release_archive, input_dir, source="cpi", metric="cpi_yoy_pct",
        value_column="_cpi_yoy",
    )
    official_unemployment = _dgbas_headline_release_rows(
        release_archive, input_dir, source="unemployment",
        metric="unemployment_rate_pct",
        value_column="twpub_dgbas_unemployment_rate",
    )
    official_gdp_yoy = _dgbas_headline_release_rows(
        release_archive, input_dir, source="gdp", metric="gdp_yoy_pct",
        value_column="_gdp_yoy",
    )
    cpi = _read_optional(input_dir, "dgbas_cpi_basic")
    if not cpi.is_empty() and {"Item", "TIME_PERIOD", "TYPE", "Item_VALUE"} <= set(cpi.columns):
        base = cpi.filter(pl.col("Item").cast(pl.Utf8, strict=False).str.contains("總指數"))
        cpi_level = (
            base.filter(pl.col("TYPE").cast(pl.Utf8, strict=False).str.contains("原始值"))
            .pipe(
                _macro_release_rows, input_dir,
                _month_period_available_expr("TIME_PERIOD", lag_days=0),
                [_num_expr("Item_VALUE").alias("_cpi")],
            )
            .drop_nulls(["date"])
            .group_by("date")
            .agg(pl.col("_cpi").drop_nulls().last().alias("_cpi"))
        )
        cpi_yoy = (
            base.filter(pl.col("TYPE").cast(pl.Utf8, strict=False).str.contains("年增率"))
            .pipe(
                _macro_release_rows, input_dir,
                _month_period_available_expr("TIME_PERIOD", lag_days=0),
                [(_num_expr("Item_VALUE") / 100.0).alias("_cpi_yoy")],
            )
            .drop_nulls(["date"])
            .group_by("date")
            .agg(pl.col("_cpi_yoy").drop_nulls().last().alias("_cpi_yoy"))
        )
        cpi_yoy = _merge_bulk_and_official_release_values(
            cpi_yoy,
            official_cpi_yoy,
            "_cpi_yoy",
        )
        frames.append(
            cpi_level.join(cpi_yoy, on="date", how="full", coalesce=True)
            .with_columns(
                [
                    pl.lit(market_symbol).alias("symbol"),
                    _positive_log(pl.col("_cpi")).alias("twpub_dgbas_cpi_log"),
                    pl.col("_cpi_yoy").alias("twpub_dgbas_cpi_yoy"),
                ]
            )
            .select(["date", "symbol", "twpub_dgbas_cpi_log", "twpub_dgbas_cpi_yoy"])
        )
    elif not official_cpi_yoy.is_empty():
        frames.append(
            official_cpi_yoy.with_columns(
                pl.lit(market_symbol).alias("symbol"),
                pl.col("_cpi_yoy").alias("twpub_dgbas_cpi_yoy"),
            ).select(["date", "symbol", "twpub_dgbas_cpi_yoy"])
        )
    unemp = _read_optional(input_dir, "dgbas_unemployment_rate")
    if not unemp.is_empty() and "年月別_Year_and_month" in unemp.columns and "總計_Total_百分比" in unemp.columns:
        bulk_unemployment = (
            _macro_release_rows(
                unemp, input_dir,
                _month_period_available_expr("年月別_Year_and_month", lag_days=0),
                [(_num_expr("總計_Total_百分比") / 100.0).alias("twpub_dgbas_unemployment_rate")],
            )
            .drop_nulls(["date"])
            .group_by("date")
            .agg(pl.col("twpub_dgbas_unemployment_rate").drop_nulls().last())
        )
        frames.append(
            _merge_bulk_and_official_release_values(
                bulk_unemployment,
                official_unemployment,
                "twpub_dgbas_unemployment_rate",
            )
            .with_columns(pl.lit(market_symbol).alias("symbol"))
            .select(["date", "symbol", "twpub_dgbas_unemployment_rate"])
        )
    elif not official_unemployment.is_empty():
        frames.append(
            official_unemployment.with_columns(pl.lit(market_symbol).alias("symbol"))
            .select(["date", "symbol", "twpub_dgbas_unemployment_rate"])
        )
    gdp = _read_optional(input_dir, "dgbas_gdp_expenditure_sa")
    if not gdp.is_empty() and {"Item", "TIME_PERIOD", "TYPE", "Item_VALUE"} <= set(gdp.columns):
        base = gdp.filter(pl.col("Item").cast(pl.Utf8, strict=False).str.contains("國內生產毛額"))
        level = (
            base.filter(pl.col("TYPE").cast(pl.Utf8, strict=False).str.contains("原始值"))
            .pipe(
                _macro_release_rows, input_dir,
                _quarter_period_available_expr("TIME_PERIOD", lag_days=0),
                [_num_expr("Item_VALUE").alias("_gdp")],
            )
            .drop_nulls(["date"])
            .group_by("date")
            .agg(pl.col("_gdp").drop_nulls().last().alias("_gdp"))
        )
        yoy = (
            base.filter(pl.col("TYPE").cast(pl.Utf8, strict=False).str.contains("年增率"))
            .pipe(
                _macro_release_rows, input_dir,
                _quarter_period_available_expr("TIME_PERIOD", lag_days=0),
                [(_num_expr("Item_VALUE") / 100.0).alias("_gdp_yoy")],
            )
            .drop_nulls(["date"])
            .group_by("date")
            .agg(pl.col("_gdp_yoy").drop_nulls().last().alias("_gdp_yoy"))
        )
        yoy = _merge_bulk_and_official_release_values(
            yoy, official_gdp_yoy, "_gdp_yoy"
        )
        frames.append(
            level.join(yoy, on="date", how="full", coalesce=True)
            .with_columns(
                [
                    pl.lit(market_symbol).alias("symbol"),
                    _positive_log(pl.col("_gdp")).alias("twpub_dgbas_gdp_log"),
                    pl.col("_gdp_yoy").alias("twpub_dgbas_gdp_yoy"),
                ]
            )
            .select(["date", "symbol", "twpub_dgbas_gdp_log", "twpub_dgbas_gdp_yoy"])
        )
    elif not official_gdp_yoy.is_empty():
        frames.append(
            official_gdp_yoy.with_columns(
                pl.lit(market_symbol).alias("symbol"),
                pl.col("_gdp_yoy").alias("twpub_dgbas_gdp_yoy"),
            ).select(["date", "symbol", "twpub_dgbas_gdp_yoy"])
        )
    return _finalize_feature_frame(pl.concat(frames, how="diagonal_relaxed")) if frames else pl.DataFrame()


def _build_mof_macro_features(input_dir: Path, *, market_symbol: str) -> pl.DataFrame:
    frames: list[pl.DataFrame] = []
    trade = _read_optional(input_dir, "mof_customs_trade")
    if not trade.is_empty() and {"年度", "月份"} <= set(trade.columns):
        columns = set(trade.columns)
        exports = _first_num_expr(columns, ["出口總值(新臺幣千元)", "出口(新臺幣千元)"])
        imports = _first_num_expr(columns, ["進口總值(新臺幣千元)", "進口(新臺幣千元)"])
        frames.append(
            _macro_release_rows(
                trade, input_dir,
                _roc_year_month_available_expr("年度", "月份", lag_days=0),
                [
                    exports.alias("_exports"),
                    imports.alias("_imports"),
                    _optional_num_expr(columns, "出入超(新臺幣千元)").alias("_balance"),
                ]
            )
            .drop_nulls(["date"])
            .group_by("date")
            .agg(
                [
                    pl.col("_exports").drop_nulls().last().alias("_exports"),
                    pl.col("_imports").drop_nulls().last().alias("_imports"),
                    pl.col("_balance").drop_nulls().last().alias("_balance"),
                ]
            )
            .with_columns(
                [
                    pl.lit(market_symbol).alias("symbol"),
                    _positive_log1p(pl.col("_exports")).alias("twpub_mof_export_log"),
                    _positive_log1p(pl.col("_imports")).alias("twpub_mof_import_log"),
                    _signed_asinh(pl.col("_balance"), scale=1_000_000.0).alias("twpub_mof_trade_balance_asinh"),
                ]
            )
            .select(["date", "symbol", "twpub_mof_export_log", "twpub_mof_import_log", "twpub_mof_trade_balance_asinh"])
        )
    tax = _read_optional(input_dir, "mof_tax_revenue")
    if not tax.is_empty() and "稅目別" in tax.columns:
        columns = set(tax.columns)
        frames.append(
            _macro_release_rows(
                tax, input_dir, _roc_tax_period_available_expr("稅目別", lag_days=0),
                [
                    _optional_num_expr(columns, "總計").alias("_total"),
                    _optional_num_expr(columns, "證券交易稅").alias("_securities_tax"),
                    _optional_num_expr(columns, "期貨交易稅").alias("_futures_tax"),
                    _optional_num_expr(columns, "營業稅").alias("_business_tax"),
                ]
            )
            .drop_nulls(["date"])
            .group_by("date")
            .agg(
                [
                    pl.col("_total").drop_nulls().last().alias("_total"),
                    pl.col("_securities_tax").drop_nulls().last().alias("_securities_tax"),
                    pl.col("_futures_tax").drop_nulls().last().alias("_futures_tax"),
                    pl.col("_business_tax").drop_nulls().last().alias("_business_tax"),
                ]
            )
            .with_columns(
                [
                    pl.lit(market_symbol).alias("symbol"),
                    _positive_log1p(pl.col("_total")).alias("twpub_mof_tax_total_log"),
                    _positive_log1p(pl.col("_securities_tax")).alias("twpub_mof_securities_tax_log"),
                    _positive_log1p(pl.col("_futures_tax")).alias("twpub_mof_futures_tax_log"),
                    _positive_log1p(pl.col("_business_tax")).alias("twpub_mof_business_tax_log"),
                ]
            )
            .select(
                [
                    "date",
                    "symbol",
                    "twpub_mof_tax_total_log",
                    "twpub_mof_securities_tax_log",
                    "twpub_mof_futures_tax_log",
                    "twpub_mof_business_tax_log",
                ]
            )
        )
    return pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()


def _build_taifex_tx_features(input_dir: Path, *, market_symbol: str) -> pl.DataFrame:
    frame = _read_optional(input_dir, "taifex_daily_futures")
    if frame.is_empty() or "Date" not in frame.columns or "Contract" not in frame.columns:
        return pl.DataFrame()
    columns = set(frame.columns)
    session_filter = pl.lit(True)
    if "TradingSession" in columns:
        session_filter = pl.col("TradingSession").cast(pl.Utf8, strict=False).str.contains("一般").fill_null(False)
    tx = (
        frame.filter((_symbol_expr("Contract") == "TX") & session_filter)
        .select(
            [
                _yyyymmdd_expr("Date").alias("date"),
                pl.col("ContractMonth(Week)").cast(pl.Utf8, strict=False).alias("_month"),
                _num_expr("Volume").alias("_volume"),
                _num_expr("OpenInterest").alias("_open_interest"),
                _num_expr("SettlementPrice").alias("_settlement"),
            ]
        )
        .drop_nulls(["date"])
    )
    if tx.is_empty():
        return pl.DataFrame()
    totals = tx.group_by("date").agg(
        [
            pl.col("_volume").sum().alias("_volume"),
            pl.col("_open_interest").sum().alias("_open_interest"),
        ]
    )
    front = tx.sort(["date", "_month"]).group_by("date").agg(pl.col("_settlement").drop_nulls().first().alias("_settlement"))
    return (
        totals.join(front, on="date", how="full", coalesce=True)
        .sort("date")
        .with_columns(
            [
                pl.lit(market_symbol).alias("symbol"),
                _positive_log1p(pl.col("_volume")).alias("twpub_taifex_tx_volume_log"),
                _positive_log1p(pl.col("_open_interest")).alias("twpub_taifex_tx_open_interest_log"),
                _safe_log(pl.col("_settlement") / pl.col("_settlement").shift(1))
                .fill_null(0.0)
                .alias("twpub_taifex_tx_settlement_logret_1d"),
            ]
        )
        .select(
            [
                "date",
                "symbol",
                "twpub_taifex_tx_volume_log",
                "twpub_taifex_tx_open_interest_log",
                "twpub_taifex_tx_settlement_logret_1d",
            ]
        )
    )


def _build_taifex_options_features(input_dir: Path, *, market_symbol: str) -> pl.DataFrame:
    frame = _read_optional(input_dir, "taifex_daily_options")
    if frame.is_empty() or "Date" not in frame.columns or "Contract" not in frame.columns:
        return pl.DataFrame()
    columns = set(frame.columns)
    session_filter = pl.lit(True)
    if "TradingSession" in columns:
        session_filter = pl.col("TradingSession").cast(pl.Utf8, strict=False).str.contains("一般").fill_null(False)
    txo = (
        frame.filter((_symbol_expr("Contract") == "TXO") & session_filter)
        .select(
            [
                _yyyymmdd_expr("Date").alias("date"),
                pl.col("CallPut").cast(pl.Utf8, strict=False).str.to_uppercase().alias("_cp"),
                _num_expr("Volume").alias("_volume"),
                _num_expr("OpenInterest").alias("_oi"),
            ]
        )
        .drop_nulls(["date"])
    )
    if txo.is_empty():
        return pl.DataFrame()
    agg = txo.group_by("date").agg(
        [
            pl.when(pl.col("_cp").str.contains("買|C")).then(pl.col("_volume")).otherwise(0.0).sum().alias("_call_volume"),
            pl.when(pl.col("_cp").str.contains("賣|P")).then(pl.col("_volume")).otherwise(0.0).sum().alias("_put_volume"),
            pl.when(pl.col("_cp").str.contains("買|C")).then(pl.col("_oi")).otherwise(0.0).sum().alias("_call_oi"),
            pl.when(pl.col("_cp").str.contains("賣|P")).then(pl.col("_oi")).otherwise(0.0).sum().alias("_put_oi"),
        ]
    )
    return agg.with_columns(
        [
            pl.lit(market_symbol).alias("symbol"),
            _positive_log1p(pl.col("_call_volume")).alias("twpub_taifex_txo_call_volume_log"),
            _positive_log1p(pl.col("_put_volume")).alias("twpub_taifex_txo_put_volume_log"),
            _safe_ratio(pl.col("_put_volume"), pl.col("_call_volume")).alias("twpub_taifex_txo_put_call_volume_ratio"),
            _positive_log1p(pl.col("_call_oi")).alias("twpub_taifex_txo_call_oi_log"),
            _positive_log1p(pl.col("_put_oi")).alias("twpub_taifex_txo_put_oi_log"),
            _safe_ratio(pl.col("_put_oi"), pl.col("_call_oi")).alias("twpub_taifex_txo_put_call_oi_ratio"),
        ]
    ).select(
        [
            "date",
            "symbol",
            "twpub_taifex_txo_call_volume_log",
            "twpub_taifex_txo_put_volume_log",
            "twpub_taifex_txo_put_call_volume_ratio",
            "twpub_taifex_txo_call_oi_log",
            "twpub_taifex_txo_put_oi_log",
            "twpub_taifex_txo_put_call_oi_ratio",
        ]
    )


def _build_taifex_institutional_features(input_dir: Path, *, market_symbol: str) -> pl.DataFrame:
    frame = _read_optional(input_dir, "taifex_institutional_total")
    if frame.is_empty() or "Date" not in frame.columns or "Item" not in frame.columns:
        return pl.DataFrame()
    base = frame.select(
        [
            _yyyymmdd_expr("Date").alias("date"),
            pl.col("Item").cast(pl.Utf8, strict=False).alias("_item"),
            _num_expr("OpenInterest(Net)").alias("_net_oi"),
        ]
    ).drop_nulls(["date"])
    frames: list[pl.DataFrame] = []
    for item, feature in (
        ("自營商", "twpub_taifex_dealer_net_oi_asinh"),
        ("外資及陸資", "twpub_taifex_foreign_net_oi_asinh"),
        ("投信", "twpub_taifex_trust_net_oi_asinh"),
    ):
        part = (
            base.filter(pl.col("_item") == item)
            .group_by("date")
            .agg(_signed_asinh(pl.col("_net_oi").sum(), scale=1000.0).alias(feature))
            .with_columns(pl.lit(market_symbol).alias("symbol"))
            .select(["date", "symbol", feature])
        )
        if not part.is_empty():
            frames.append(part)
    return _merge_feature_frames(frames) if frames else pl.DataFrame()


def _build_taifex_large_trader_features(input_dir: Path, *, market_symbol: str) -> pl.DataFrame:
    frame = _read_optional(input_dir, "taifex_large_trader_futures_oi")
    if frame.is_empty() or "Date" not in frame.columns or "Contract" not in frame.columns:
        return pl.DataFrame()
    tx = (
        frame.filter(_symbol_expr("Contract") == "TX")
        .select(
            [
                _yyyymmdd_expr("Date").alias("date"),
                _num_expr("Top5Buy").alias("_top5_buy"),
                _num_expr("Top5Sell").alias("_top5_sell"),
                _num_expr("Top10Buy").alias("_top10_buy"),
                _num_expr("Top10Sell").alias("_top10_sell"),
                _num_expr("OIOfMarket").alias("_market_oi"),
            ]
        )
        .drop_nulls(["date"])
    )
    if tx.is_empty():
        return pl.DataFrame()
    agg = tx.group_by("date").agg(
        [
            pl.col("_top5_buy").sum().alias("_top5_buy"),
            pl.col("_top5_sell").sum().alias("_top5_sell"),
            pl.col("_top10_buy").sum().alias("_top10_buy"),
            pl.col("_top10_sell").sum().alias("_top10_sell"),
            pl.col("_market_oi").max().alias("_market_oi"),
        ]
    )
    return agg.with_columns(
        [
            pl.lit(market_symbol).alias("symbol"),
            _safe_ratio(pl.col("_top5_buy"), pl.col("_top5_buy") + pl.col("_top5_sell")).alias(
                "twpub_taifex_tx_top5_long_ratio"
            ),
            _safe_ratio(pl.col("_top10_buy"), pl.col("_top10_buy") + pl.col("_top10_sell")).alias(
                "twpub_taifex_tx_top10_long_ratio"
            ),
            _positive_log1p(pl.col("_market_oi")).alias("twpub_taifex_tx_large_oi_log"),
        ]
    ).select(
        [
            "date",
            "symbol",
            "twpub_taifex_tx_top5_long_ratio",
            "twpub_taifex_tx_top10_long_ratio",
            "twpub_taifex_tx_large_oi_log",
        ]
    )


def _build_taifex_final_settlement_features(input_dir: Path, *, market_symbol: str) -> pl.DataFrame:
    frame = _read_optional(input_dir, "taifex_final_settlement_price")
    if frame.is_empty() or "商品代號" not in frame.columns:
        return pl.DataFrame()
    tx = (
        frame.filter(pl.col("商品代號").cast(pl.Utf8, strict=False).str.contains(r"(^|/)TX(/|$)"))
        .select([_date_column_expr("最後結算日").alias("date"), _num_expr("最後結算價").alias("_settlement")])
        .drop_nulls(["date"])
        .group_by("date")
        .agg(pl.col("_settlement").drop_nulls().last().alias("_settlement"))
        .sort("date")
    )
    if tx.is_empty():
        return pl.DataFrame()
    return tx.with_columns(
        [
            pl.lit(market_symbol).alias("symbol"),
            _safe_log(pl.col("_settlement") / pl.col("_settlement").shift(1)).alias(
                "twpub_taifex_tx_final_settlement_logret"
            ),
        ]
    ).select(["date", "symbol", "twpub_taifex_tx_final_settlement_logret"])
