from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Iterable

import polars as pl


DEFAULT_MARKET_SYMBOL = "__MARKET__"

STOCK_FEATURE_COLUMNS = (
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
)

MARKET_FEATURE_COLUMNS = (
    "twpub_usdtwd_log",
    "twpub_usdtwd_logret_1d",
    "twpub_taifex_tx_volume_log",
    "twpub_taifex_tx_open_interest_log",
    "twpub_taifex_tx_settlement_logret_1d",
)

FEATURE_COLUMNS = (*STOCK_FEATURE_COLUMNS, *MARKET_FEATURE_COLUMNS)
KEY_COLUMNS = ("date", "symbol")


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
        _build_valuation_features(input_dir),
        _build_margin_features(input_dir),
        _build_institutional_features(input_dir),
        _build_tdcc_features(input_dir),
    ]
    stock_features = _merge_feature_frames(stock_frames)
    if symbols is not None and not stock_features.is_empty():
        stock_features = stock_features.filter(pl.col("symbol").is_in(sorted(symbols)))

    market_features = _merge_feature_frames(
        [
            _build_usdtwd_features(input_dir, market_symbol=market_symbol),
            _build_taifex_tx_features(input_dir, market_symbol=market_symbol),
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


def _date_column_expr(name: str):
    return pl.col(name).cast(pl.Utf8, strict=False).str.slice(0, 10).str.strptime(pl.Date, "%Y-%m-%d", strict=False)


def _yyyymmdd_expr(name: str):
    text = pl.col(name).cast(pl.Utf8, strict=False).str.replace_all(r"[^0-9]", "")
    return pl.when(text.str.len_chars() == 8).then(text.str.strptime(pl.Date, "%Y%m%d", strict=False)).otherwise(None)


def _period_expr(name: str):
    text = pl.col(name).cast(pl.Utf8, strict=False)
    return (
        pl.when(text.str.contains(r"^\d{4}M\d{2}$"))
        .then((text.str.slice(0, 4) + "-" + text.str.slice(5, 2) + "-01").str.strptime(pl.Date, "%Y-%m-%d", strict=False))
        .otherwise(None)
    )


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


def _optional_num_expr(columns: set[str], name: str):
    if name not in columns:
        return pl.lit(None, dtype=pl.Float64)
    return _num_expr(name)


def _positive_log1p(expr):
    return pl.when(expr.is_finite() & (expr > 0.0)).then((expr + 1.0).log()).otherwise(None)


def _safe_log(expr):
    return pl.when(expr.is_finite() & (expr > 0.0)).then(expr.log()).otherwise(None)


def _signed_asinh(expr, scale: float = 1000.0):
    x = expr / float(scale)
    return pl.when(expr.is_finite()).then((x + ((x * x) + 1.0).sqrt()).log()).otherwise(None)


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
    frame = _read_optional(input_dir, "tdcc_shareholding_distribution")
    if frame.is_empty():
        frame = _read_optional(input_dir, "data_gov_tdcc_shareholding_distribution")
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
                _yyyymmdd_expr(date_col).alias("date"),
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
                _safe_log(pl.col("_settlement") / pl.col("_settlement").shift(1)).alias(
                    "twpub_taifex_tx_settlement_logret_1d"
                ),
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
