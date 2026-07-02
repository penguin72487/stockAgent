from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Iterable

import polars as pl


DEFAULT_MARKET_SYMBOL = "__MARKET__"

STOCK_FEATURE_COLUMNS = (
    "twpub_official_close_logret_1d",
    "twpub_official_trading_volume_log",
    "twpub_official_trading_value_log",
    "twpub_official_trades_log",
    "twpub_official_turnover_ratio",
    "twpub_official_intraday_range",
    "twpub_official_close_to_high",
    "twpub_official_close_to_low",
    "twpub_tpex_next_limit_up_ret",
    "twpub_tpex_next_limit_down_ret",
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
    "twpub_dividend_year",
    "twpub_exdiv_known",
    "twpub_exdiv_cash_dividend",
    "twpub_exdiv_stock_dividend_ratio",
    "twpub_exdiv_subscription_ratio",
    "twpub_exdiv_subscription_price_log",
    "twpub_material_event_count_log",
    "twpub_material_clause_log",
    "twpub_material_fact_lag_days",
    "twpub_attention_flag",
    "twpub_attention_count_log",
    "twpub_attention_close_log",
    "twpub_attention_pe_log",
    "twpub_disposal_flag",
    "twpub_disposal_count_log",
)

MARKET_FEATURE_COLUMNS = (
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
KEY_COLUMNS = ("date", "symbol")
AVAILABILITY_POLICY = {
    "historical_daily": "official session/trading date; usable for next-session labels",
    "tdcc_shareholding": "TDCC data date plus 7 calendar days as a conservative availability date",
    "monthly_macro": "period end plus 45 calendar days when no explicit release date is provided",
    "quarterly_macro": "quarter end plus 90 calendar days when no explicit release date is provided",
    "snapshot_openapi": "announcement/report date when present; otherwise downloader as-of date",
    "future_event_snapshot": "downloader as-of date for known future-event snapshot rows",
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


def build_tw_public_training_features(
    input_dir: str | Path = "data_tw_public",
    output_path: str | Path = "data_tw_public/features/tw_public_stock_daily.parquet",
    *,
    symbols_root: str | Path | None = "data_yahoo/tw_stocks",
    market_symbol: str = DEFAULT_MARKET_SYMBOL,
    summary_path: str | Path | None = None,
) -> TwPublicFeatureBuildResult:
    input_dir = Path(input_dir)
    output_path = Path(output_path)
    symbols = _load_symbol_filter(symbols_root)
    stock_frames = [
        _build_official_ohlcv_features(input_dir),
        _build_valuation_features(input_dir),
        _build_margin_features(input_dir),
        _build_institutional_features(input_dir),
        _build_tdcc_features(input_dir),
        _build_company_basic_features(input_dir),
        _build_dividend_features(input_dir),
        _build_ex_dividend_preview_features(input_dir),
        _build_material_info_features(input_dir),
        _build_attention_disposal_features(input_dir),
    ]
    stock_features = _merge_feature_frames(stock_frames)
    if symbols is not None and not stock_features.is_empty():
        stock_features = stock_features.filter(pl.col("symbol").is_in(sorted(symbols)))

    market_features = _merge_feature_frames(
        [
            _build_twse_market_index_features(input_dir, market_symbol=market_symbol),
            _build_usdtwd_features(input_dir, market_symbol=market_symbol),
            _build_cbc_overnight_rate_features(input_dir, market_symbol=market_symbol),
            _build_cbc_monthly_macro_features(input_dir, market_symbol=market_symbol),
            _build_dgbas_macro_features(input_dir, market_symbol=market_symbol),
            _build_mof_macro_features(input_dir, market_symbol=market_symbol),
            _build_taifex_tx_features(input_dir, market_symbol=market_symbol),
            _build_taifex_options_features(input_dir, market_symbol=market_symbol),
            _build_taifex_institutional_features(input_dir, market_symbol=market_symbol),
            _build_taifex_large_trader_features(input_dir, market_symbol=market_symbol),
            _build_taifex_final_settlement_features(input_dir, market_symbol=market_symbol),
        ]
    )

    frames = [frame for frame in (stock_features, market_features) if not frame.is_empty()]
    if frames:
        output = pl.concat(frames, how="diagonal_relaxed")
        output = _ensure_feature_columns(output).sort(["date", "symbol"])
    else:
        output = pl.DataFrame(
            {
                "date": pl.Series([], dtype=pl.Date),
                "symbol": pl.Series([], dtype=pl.Utf8),
                **{name: pl.Series([], dtype=pl.Float64) for name in FEATURE_COLUMNS},
            }
        )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output.write_parquet(output_path, compression="snappy", statistics=True)

    source_files = sorted(str(path) for path in input_dir.glob("*.parquet"))
    result = TwPublicFeatureBuildResult(
        output_path=output_path,
        rows=int(output.height),
        feature_count=len(FEATURE_COLUMNS),
        stock_rows=int(output.filter(pl.col("symbol") != market_symbol).height) if not output.is_empty() else 0,
        market_rows=int(output.filter(pl.col("symbol") == market_symbol).height) if not output.is_empty() else 0,
        market_symbol=market_symbol,
        source_files=source_files,
    )
    _write_summary(summary_path or output_path.with_suffix(".summary.json"), result)
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
        "feature_columns": list(FEATURE_COLUMNS),
        "market_symbol": result.market_symbol,
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


def _read_optional(input_dir: Path, name: str) -> pl.DataFrame:
    path = input_dir / f"{name}.parquet"
    if not path.exists():
        return pl.DataFrame()
    return pl.read_parquet(path)


def _merge_feature_frames(frames: Iterable[pl.DataFrame]) -> pl.DataFrame:
    cleaned = [_finalize_feature_frame(frame) for frame in frames if frame is not None and not frame.is_empty()]
    cleaned = [frame for frame in cleaned if not frame.is_empty()]
    if not cleaned:
        return pl.DataFrame()
    merged = cleaned[0]
    for frame in cleaned[1:]:
        merged = merged.join(frame, on=list(KEY_COLUMNS), how="full", coalesce=True)
    return _finalize_feature_frame(merged)


def _finalize_feature_frame(frame: pl.DataFrame) -> pl.DataFrame:
    if frame.is_empty():
        return frame
    feature_cols = [col for col in frame.columns if col not in KEY_COLUMNS and col in FEATURE_COLUMNS]
    if not feature_cols:
        return pl.DataFrame()
    frame = (
        frame.select(
            [
                _date_column_expr("date").alias("date"),
                _symbol_expr("symbol").alias("symbol"),
                *[pl.col(col).cast(pl.Float64, strict=False).alias(col) for col in feature_cols],
            ]
        )
        .drop_nulls(["date", "symbol"])
        .filter(pl.col("symbol") != "")
    )
    if frame.is_empty():
        return frame
    return frame.group_by(["date", "symbol"]).agg([pl.col(col).drop_nulls().last().alias(col) for col in feature_cols])


def _ensure_feature_columns(frame: pl.DataFrame) -> pl.DataFrame:
    columns = set(frame.columns)
    expressions = []
    for name in FEATURE_COLUMNS:
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
                    close.alias("_close"),
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
                    close.alias("_close"),
                    _positive_log1p(volume).alias("twpub_official_trading_volume_log"),
                    _positive_log1p(value).alias("twpub_official_trading_value_log"),
                    _positive_log1p(trades).alias("twpub_official_trades_log"),
                    _safe_ratio(volume, shares).alias("twpub_official_turnover_ratio"),
                    _safe_ratio(high - low, close).alias("twpub_official_intraday_range"),
                    _safe_ratio(close, high).alias("twpub_official_close_to_high"),
                    _safe_ratio(close, low).alias("twpub_official_close_to_low"),
                    _safe_log_ratio(_num_expr("次日漲停價"), close).alias("twpub_tpex_next_limit_up_ret"),
                    _safe_log_ratio(_num_expr("次日跌停價"), close).alias("twpub_tpex_next_limit_down_ret"),
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


def _build_twse_market_index_features(input_dir: Path, *, market_symbol: str) -> pl.DataFrame:
    frame = _read_optional(input_dir, "twse_market_index")
    if frame.is_empty() or "指數" not in frame.columns:
        return pl.DataFrame()
    index_name = pl.col("指數").cast(pl.Utf8, strict=False)
    taiex = (
        frame.filter(index_name.str.contains("發行量加權股價指數"))
        .select(
            [
                _date_column_expr("date").alias("date"),
                _num_expr("收盤指數").alias("_taiex"),
                (_num_expr("漲跌百分比(%)") / 100.0).alias("twpub_twse_taiex_pct"),
            ]
        )
        .drop_nulls(["date"])
        .group_by("date")
        .agg(
            [
                pl.col("_taiex").drop_nulls().last().alias("_taiex"),
                pl.col("twpub_twse_taiex_pct").drop_nulls().last().alias("twpub_twse_taiex_pct"),
            ]
        )
        .sort("date")
    )
    if taiex.is_empty():
        return pl.DataFrame()
    total_ret = (
        frame.filter(index_name.str.contains("發行量加權報酬指數"))
        .select([_date_column_expr("date").alias("date"), _num_expr("報酬指數").alias("_return_index")])
        .drop_nulls(["date"])
        .group_by("date")
        .agg(pl.col("_return_index").drop_nulls().last().alias("_return_index"))
        .sort("date")
    )
    output = taiex.join(total_ret, on="date", how="left").sort("date")
    return output.with_columns(
        [
            pl.lit(market_symbol).alias("symbol"),
            _positive_log(pl.col("_taiex")).alias("twpub_twse_taiex_log"),
            _safe_log(pl.col("_taiex") / pl.col("_taiex").shift(1)).alias("twpub_twse_taiex_logret_1d"),
        ]
    ).select(
        [
            "date",
            "symbol",
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
        dividend_yield = _num_expr("殖利率(%)") / 100.0
        dividend = _optional_num_expr(columns, dividend_col) if dividend_col else pl.lit(None, dtype=pl.Float64)
        frames.append(
            frame.select(
                [
                    _date_column_expr("date").alias("date"),
                    _symbol_expr(code_col).alias("symbol"),
                    _positive_log1p(pe).alias("twpub_pe_log"),
                    _positive_log1p(pb).alias("twpub_pb_log"),
                    dividend_yield.alias("twpub_dividend_yield"),
                    _positive_log1p(dividend).alias("twpub_dividend_per_share_log"),
                ]
            )
        )
    return pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()


def _build_margin_features(input_dir: Path) -> pl.DataFrame:
    frames: list[pl.DataFrame] = []
    twse = _read_optional(input_dir, "twse_margin_balance")
    if not twse.is_empty():
        margin_prev = _num_expr("前日餘額")
        margin_today = _num_expr("今日餘額")
        short_prev = _num_expr("前日餘額_2")
        short_today = _num_expr("今日餘額_2")
        frames.append(
            twse.select(
                [
                    _date_column_expr("date").alias("date"),
                    _symbol_expr("代號").alias("symbol"),
                    _positive_log1p(margin_today).alias("twpub_margin_balance_log"),
                    _signed_asinh(margin_today - margin_prev).alias("twpub_margin_balance_chg"),
                    _positive_log1p(short_today).alias("twpub_short_balance_log"),
                    _signed_asinh(short_today - short_prev).alias("twpub_short_balance_chg"),
                    _signed_asinh(_num_expr("買進")).alias("twpub_margin_buy_flow"),
                    _signed_asinh(_num_expr("賣出")).alias("twpub_margin_sell_flow"),
                    _signed_asinh(_num_expr("賣出_2")).alias("twpub_short_sell_flow"),
                    _signed_asinh(_num_expr("買進_2")).alias("twpub_short_buy_flow"),
                ]
            )
        )

    tpex = _read_optional(input_dir, "tpex_margin_balance")
    if not tpex.is_empty():
        margin_prev = _num_expr("前資餘額(張)")
        margin_today = _num_expr("資餘額")
        short_prev = _num_expr("前券餘額(張)")
        short_today = _num_expr("券餘額")
        frames.append(
            tpex.select(
                [
                    _date_column_expr("date").alias("date"),
                    _symbol_expr("代號").alias("symbol"),
                    _positive_log1p(margin_today).alias("twpub_margin_balance_log"),
                    _signed_asinh(margin_today - margin_prev).alias("twpub_margin_balance_chg"),
                    _positive_log1p(short_today).alias("twpub_short_balance_log"),
                    _signed_asinh(short_today - short_prev).alias("twpub_short_balance_chg"),
                    _signed_asinh(_num_expr("資買")).alias("twpub_margin_buy_flow"),
                    _signed_asinh(_num_expr("資賣")).alias("twpub_margin_sell_flow"),
                    _signed_asinh(_num_expr("券賣")).alias("twpub_short_sell_flow"),
                    _signed_asinh(_num_expr("券買")).alias("twpub_short_buy_flow"),
                ]
            )
        )
    return pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()


def _build_institutional_features(input_dir: Path) -> pl.DataFrame:
    frames: list[pl.DataFrame] = []
    twse = _read_optional(input_dir, "twse_institutional_trades")
    if not twse.is_empty():
        frames.append(
            twse.select(
                [
                    _date_column_expr("date").alias("date"),
                    _symbol_expr("證券代號").alias("symbol"),
                    _signed_asinh(_num_expr("外陸資買賣超股數(不含外資自營商)")).alias("twpub_foreign_net_buy_flow"),
                    _signed_asinh(_num_expr("投信買賣超股數")).alias("twpub_investment_trust_net_buy_flow"),
                    _signed_asinh(_num_expr("自營商買賣超股數")).alias("twpub_dealer_net_buy_flow"),
                    _signed_asinh(_num_expr("三大法人買賣超股數")).alias("twpub_institutional_net_buy_flow"),
                ]
            )
        )

    tpex = _read_optional(input_dir, "tpex_institutional_trades")
    if not tpex.is_empty():
        columns = set(tpex.columns)
        total_col = "三大法人買賣超股數合計" if "三大法人買賣超股數合計" in columns else "三大法人買賣超股數"
        frames.append(
            tpex.select(
                [
                    _date_column_expr("date").alias("date"),
                    _symbol_expr("代號").alias("symbol"),
                    _signed_asinh(_num_expr("外資及陸資淨買股數")).alias("twpub_foreign_net_buy_flow"),
                    _signed_asinh(_num_expr("投信淨買股數")).alias("twpub_investment_trust_net_buy_flow"),
                    _signed_asinh(_num_expr("自營淨買股數")).alias("twpub_dealer_net_buy_flow"),
                    _signed_asinh(_num_expr(total_col)).alias("twpub_institutional_net_buy_flow"),
                ]
            )
        )
    return pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()


def _build_tdcc_features(input_dir: Path) -> pl.DataFrame:
    frames = [
        frame
        for frame in (
            _read_optional(input_dir, "tdcc_shareholding_distribution"),
            _read_optional(input_dir, "data_gov_tdcc_shareholding_distribution"),
        )
        if not frame.is_empty()
    ]
    if not frames:
        return pl.DataFrame()
    frame = pl.concat(frames, how="diagonal_relaxed")
    if frame.is_empty():
        return pl.DataFrame()
    columns = set(frame.columns)
    date_col = "\ufeff資料日期" if "\ufeff資料日期" in columns else "資料日期"
    if date_col not in columns:
        return pl.DataFrame()
    tier = _num_expr("持股分級")
    ratio = _num_expr("占集保庫存數比例%") / 100.0
    holder_count = _num_expr("人數")
    return (
        frame.select(
            [
                _yyyymmdd_expr(date_col).dt.offset_by("7d").alias("date"),
                _symbol_expr("證券代號").alias("symbol"),
                tier.alias("_tier"),
                ratio.alias("_ratio"),
                holder_count.alias("_holders"),
            ]
        )
        .drop_nulls(["date", "symbol", "_tier"])
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
        report_date = pl.coalesce([_date_column_expr(report_date_col), _date_column_expr("date")])
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
                    pl.coalesce(
                        [
                            _date_column_expr("董事會（擬議）股利分派日"),
                            _date_column_expr("股東會日期"),
                            _date_column_expr("出表日期"),
                            _date_column_expr("date"),
                        ]
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
                    _optional_num_expr(columns, "股利年度").alias("twpub_dividend_year"),
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
                    pl.coalesce(
                        [
                            _date_column_expr("董事會決議通過股利分派日"),
                            _date_column_expr("股東會日期配盈餘/待彌補虧損(元)"),
                            _date_column_expr("出表日期"),
                            _date_column_expr("date"),
                        ]
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
                    _optional_num_expr(columns, "股利年度").alias("twpub_dividend_year"),
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
            _date_column_expr("date").alias("date"),
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
            pl.coalesce([_date_column_expr("發言日期"), _date_column_expr("出表日期"), _date_column_expr("date")]).alias("date"),
            _symbol_expr("公司代號").alias("symbol"),
            _num_from_text_digits_expr("符合條款").alias("_clause")
            if "符合條款" in columns
            else _lit_null_float().alias("_clause"),
            event_date.alias("_event_date"),
        ]
    ).drop_nulls(["date", "symbol"])
    if base.is_empty():
        return base
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
                pl.coalesce([_date_column_expr(date_col), _date_column_expr("date")]).alias("date"),
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
                        pl.lit(1.0).alias("twpub_attention_flag"),
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
                        pl.lit(1.0).alias("twpub_disposal_flag"),
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
        frame.select([_yyyymmdd_expr("日期").alias("date"), rate.alias("_rate")])
        .drop_nulls(["date"])
        .group_by("date")
        .agg(pl.col("_rate").drop_nulls().last().alias("_rate"))
        .sort("date")
        .with_columns(
            [
                pl.lit(market_symbol).alias("symbol"),
                _safe_log(pl.col("_rate")).alias("twpub_usdtwd_log"),
                _safe_log(pl.col("_rate") / pl.col("_rate").shift(1)).alias("twpub_usdtwd_logret_1d"),
            ]
        )
        .select(["date", "symbol", "twpub_usdtwd_log", "twpub_usdtwd_logret_1d"])
    )


def _build_cbc_overnight_rate_features(input_dir: Path, *, market_symbol: str) -> pl.DataFrame:
    frame = _read_optional(input_dir, "cbc_overnight_rate")
    if frame.is_empty() or "日期" not in frame.columns:
        return pl.DataFrame()
    rate_col = "利率[%]" if "利率[%]" in frame.columns else None
    if rate_col is None:
        return pl.DataFrame()
    return (
        frame.select([_date_column_expr("日期").alias("date"), (_num_expr(rate_col) / 100.0).alias("_rate")])
        .drop_nulls(["date"])
        .group_by("date")
        .agg(pl.col("_rate").drop_nulls().last().alias("_rate"))
        .sort("date")
        .with_columns(
            [
                pl.lit(market_symbol).alias("symbol"),
                pl.col("_rate").alias("twpub_cbc_overnight_rate"),
                (pl.col("_rate") - pl.col("_rate").shift(1)).alias("twpub_cbc_overnight_rate_chg"),
            ]
        )
        .select(["date", "symbol", "twpub_cbc_overnight_rate", "twpub_cbc_overnight_rate_chg"])
    )


def _build_cbc_monthly_macro_features(input_dir: Path, *, market_symbol: str) -> pl.DataFrame:
    frames: list[pl.DataFrame] = []
    fx = _read_optional(input_dir, "cbc_fx_reserves")
    if not fx.is_empty() and "日期" in fx.columns and "金額" in fx.columns:
        frames.append(
            fx.select(
                [
                    _month_period_available_expr("日期", lag_days=45).alias("date"),
                    _num_expr("金額").alias("_fx_reserves"),
                ]
            )
            .drop_nulls(["date"])
            .group_by("date")
            .agg(pl.col("_fx_reserves").drop_nulls().last().alias("_fx_reserves"))
            .sort("date")
            .with_columns(
                [
                    pl.lit(market_symbol).alias("symbol"),
                    _positive_log(pl.col("_fx_reserves")).alias("twpub_cbc_fx_reserves_log"),
                    _safe_log(pl.col("_fx_reserves") / pl.col("_fx_reserves").shift(1)).alias(
                        "twpub_cbc_fx_reserves_chg"
                    ),
                ]
            )
            .select(["date", "symbol", "twpub_cbc_fx_reserves_log", "twpub_cbc_fx_reserves_chg"])
        )
    money = _read_optional(input_dir, "cbc_money_aggregates")
    if not money.is_empty() and "期間" in money.columns:
        columns = set(money.columns)
        frames.append(
            money.select(
                [
                    _month_period_available_expr("期間", lag_days=45).alias("date"),
                    _first_num_expr(columns, ["貨幣總計數-Ｍ１Ｂ-原始值", "貨幣總計數 -Ｍ１Ｂ-原始值"]).alias("_m1b"),
                    _first_num_expr(columns, ["貨幣總計數-Ｍ１Ｂ-年增率", "貨幣總計數 -Ｍ１Ｂ-年增率"]).alias("_m1b_yoy"),
                    _first_num_expr(columns, ["貨幣總計數-Ｍ２-原始值", "貨幣總計數 -Ｍ２-原始值"]).alias("_m2"),
                    _first_num_expr(columns, ["貨幣總計數-Ｍ２-年增率", "貨幣總計數 -Ｍ２-年增率"]).alias("_m2_yoy"),
                ]
            )
            .drop_nulls(["date"])
            .group_by("date")
            .agg(
                [
                    pl.col("_m1b").drop_nulls().last().alias("_m1b"),
                    pl.col("_m1b_yoy").drop_nulls().last().alias("_m1b_yoy"),
                    pl.col("_m2").drop_nulls().last().alias("_m2"),
                    pl.col("_m2_yoy").drop_nulls().last().alias("_m2_yoy"),
                ]
            )
            .with_columns(
                [
                    pl.lit(market_symbol).alias("symbol"),
                    _positive_log(pl.col("_m1b")).alias("twpub_cbc_m1b_log"),
                    (pl.col("_m1b_yoy") / 100.0).alias("twpub_cbc_m1b_yoy"),
                    _positive_log(pl.col("_m2")).alias("twpub_cbc_m2_log"),
                    (pl.col("_m2_yoy") / 100.0).alias("twpub_cbc_m2_yoy"),
                ]
            )
            .select(["date", "symbol", "twpub_cbc_m1b_log", "twpub_cbc_m1b_yoy", "twpub_cbc_m2_log", "twpub_cbc_m2_yoy"])
        )
    return pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()


def _build_dgbas_macro_features(input_dir: Path, *, market_symbol: str) -> pl.DataFrame:
    frames: list[pl.DataFrame] = []
    cpi = _read_optional(input_dir, "dgbas_cpi_basic")
    if not cpi.is_empty() and {"Item", "TIME_PERIOD", "TYPE", "Item_VALUE"} <= set(cpi.columns):
        base = cpi.filter(pl.col("Item").cast(pl.Utf8, strict=False).str.contains("總指數"))
        cpi_level = (
            base.filter(pl.col("TYPE").cast(pl.Utf8, strict=False).str.contains("原始值"))
            .select([_month_period_available_expr("TIME_PERIOD", lag_days=45).alias("date"), _num_expr("Item_VALUE").alias("_cpi")])
            .drop_nulls(["date"])
            .group_by("date")
            .agg(pl.col("_cpi").drop_nulls().last().alias("_cpi"))
        )
        cpi_yoy = (
            base.filter(pl.col("TYPE").cast(pl.Utf8, strict=False).str.contains("年增率"))
            .select([_month_period_available_expr("TIME_PERIOD", lag_days=45).alias("date"), (_num_expr("Item_VALUE") / 100.0).alias("_cpi_yoy")])
            .drop_nulls(["date"])
            .group_by("date")
            .agg(pl.col("_cpi_yoy").drop_nulls().last().alias("_cpi_yoy"))
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
    unemp = _read_optional(input_dir, "dgbas_unemployment_rate")
    if not unemp.is_empty() and "年月別_Year_and_month" in unemp.columns and "總計_Total_百分比" in unemp.columns:
        frames.append(
            unemp.select(
                [
                    _month_period_available_expr("年月別_Year_and_month", lag_days=45).alias("date"),
                    (_num_expr("總計_Total_百分比") / 100.0).alias("twpub_dgbas_unemployment_rate"),
                ]
            )
            .drop_nulls(["date"])
            .group_by("date")
            .agg(pl.col("twpub_dgbas_unemployment_rate").drop_nulls().last())
            .with_columns(pl.lit(market_symbol).alias("symbol"))
            .select(["date", "symbol", "twpub_dgbas_unemployment_rate"])
        )
    gdp = _read_optional(input_dir, "dgbas_gdp_expenditure_sa")
    if not gdp.is_empty() and {"Item", "TIME_PERIOD", "TYPE", "Item_VALUE"} <= set(gdp.columns):
        base = gdp.filter(pl.col("Item").cast(pl.Utf8, strict=False).str.contains("國內生產毛額"))
        level = (
            base.filter(pl.col("TYPE").cast(pl.Utf8, strict=False).str.contains("原始值"))
            .select([_quarter_period_available_expr("TIME_PERIOD", lag_days=90).alias("date"), _num_expr("Item_VALUE").alias("_gdp")])
            .drop_nulls(["date"])
            .group_by("date")
            .agg(pl.col("_gdp").drop_nulls().last().alias("_gdp"))
        )
        yoy = (
            base.filter(pl.col("TYPE").cast(pl.Utf8, strict=False).str.contains("年增率"))
            .select([_quarter_period_available_expr("TIME_PERIOD", lag_days=90).alias("date"), (_num_expr("Item_VALUE") / 100.0).alias("_gdp_yoy")])
            .drop_nulls(["date"])
            .group_by("date")
            .agg(pl.col("_gdp_yoy").drop_nulls().last().alias("_gdp_yoy"))
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
    return pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()


def _build_mof_macro_features(input_dir: Path, *, market_symbol: str) -> pl.DataFrame:
    frames: list[pl.DataFrame] = []
    trade = _read_optional(input_dir, "mof_customs_trade")
    if not trade.is_empty() and {"年度", "月份"} <= set(trade.columns):
        columns = set(trade.columns)
        exports = _first_num_expr(columns, ["出口總值(新臺幣千元)", "出口(新臺幣千元)"])
        imports = _first_num_expr(columns, ["進口總值(新臺幣千元)", "進口(新臺幣千元)"])
        frames.append(
            trade.select(
                [
                    _roc_year_month_available_expr("年度", "月份", lag_days=45).alias("date"),
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
            tax.select(
                [
                    _roc_tax_period_available_expr("稅目別", lag_days=45).alias("date"),
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
