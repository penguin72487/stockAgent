"""Build causal daily public features for midnight UTC Bybit decisions."""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import sys

import numpy as np
import polars as pl
import pyarrow.parquet as pq

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.data.crypto_public_web import (
    COINGECKO_ASSET_FEATURES,
    COINGECKO_MARKET_FEATURES,
    COINMETRICS_FEATURES,
    ETF_ISSUER_FEATURES,
    FRED_FEATURES,
    SEC_ASSET_FEATURES,
    SEC_MARKET_FEATURES,
    coingecko_snapshot_rows,
    coinmetrics_vintage_rows,
    etf_issuer_snapshot_rows,
    fred_macro_rows,
    sec_etf_filing_rows,
)


BOUNDARY_MINUTES_UTC = 0
MARKET_SYMBOL = "__MARKET__"

OKX_FEATURES = (
    "crypto_okx_mark_return_1d",
    "crypto_okx_index_return_1d",
    "crypto_okx_mark_range_log_1d",
    "crypto_okx_index_range_log_1d",
    "crypto_okx_contract_mark_basis_log",
    "crypto_okx_mark_index_basis_log",
    "crypto_okx_funding_realized_rate",
    "crypto_okx_funding_realized_annualized",
    "crypto_okx_funding_age_hours",
    "crypto_okx_funding_available",
    "crypto_okx_open_interest_usd_log_change_1d",
    "crypto_okx_open_interest_usd_to_volume_log",
    "crypto_okx_taker_imbalance_1d",
    "crypto_okx_long_short_account_ratio_log",
    "crypto_okx_top_trader_account_ratio_log",
    "crypto_okx_top_trader_position_ratio_log",
    "crypto_okx_available",
    "crypto_okx_positioning_available",
    "crypto_okx_taker_available",
    "crypto_okx_session_coverage",
)

BINANCE_FEATURES = (
    "crypto_binance_mark_return_1d",
    "crypto_binance_index_return_1d",
    "crypto_binance_mark_range_log_1d",
    "crypto_binance_index_range_log_1d",
    "crypto_binance_contract_mark_basis_log",
    "crypto_binance_mark_index_basis_log",
    "crypto_binance_funding_realized_rate",
    "crypto_binance_funding_realized_annualized",
    "crypto_binance_funding_age_hours",
    "crypto_binance_funding_available",
    "crypto_binance_open_interest_usd_log_change_1d",
    "crypto_binance_open_interest_usd_to_volume_log",
    "crypto_binance_taker_imbalance_1d",
    "crypto_binance_global_long_short_account_ratio_log",
    "crypto_binance_top_trader_account_ratio_log",
    "crypto_binance_top_trader_position_ratio_log",
    "crypto_binance_quote_volume_log1p",
    "crypto_binance_trade_count_log1p",
    "crypto_binance_core_available",
    "crypto_binance_positioning_available",
    "crypto_binance_session_coverage",
)

BYBIT_FEATURES = (
    "crypto_bybit_funding_rate_sum_1d",
    "crypto_bybit_funding_realized_annualized",
    "crypto_bybit_funding_cashflow_coefficient_1d",
    "crypto_bybit_funding_last_rate",
    "crypto_bybit_funding_age_hours",
    "crypto_bybit_funding_event_count_1d",
    "crypto_bybit_funding_available",
)

FREE_MARKET_SERIES = (
    (
        "crypto_public_fear_greed_index",
        "alternative_me_fear_greed",
        "fear_greed_index",
        "BTC",
        False,
    ),
    (
        "crypto_public_defi_open_interest_log1p",
        "defillama_open_interest",
        "open_interest_at_end_usd",
        "all",
        True,
    ),
    (
        "crypto_public_options_notional_volume_log1p",
        "defillama_options_notional_volume",
        "daily_options_notional_volume_usd",
        "all",
        True,
    ),
    (
        "crypto_public_dex_volume_log1p",
        "defillama_dex_volume",
        "daily_dex_volume_usd",
        "all",
        True,
    ),
    (
        "crypto_public_protocol_fees_log1p",
        "defillama_protocol_fees",
        "daily_protocol_fees_usd",
        "all",
        True,
    ),
    (
        "crypto_public_protocol_revenue_log1p",
        "defillama_protocol_revenue",
        "daily_protocol_revenue_usd",
        "all",
        True,
    ),
)

FREE_SNAPSHOT_SERIES = (
    (
        "crypto_public_btc_fastest_fee_log1p",
        "bitcoin_mempool_fees",
        "fastest_fee",
        True,
    ),
    ("crypto_public_btc_hour_fee_log1p", "bitcoin_mempool_fees", "hour_fee", True),
    (
        "crypto_public_btc_mempool_vsize_log1p",
        "bitcoin_mempool_state",
        "virtual_size",
        True,
    ),
    (
        "crypto_public_btc_unconfirmed_txs_log1p",
        "bitcoin_mempool_state",
        "unconfirmed_tx_count",
        True,
    ),
    (
        "crypto_public_btc_difficulty_change_pct",
        "bitcoin_difficulty_adjustment",
        "estimated_change_pct",
        False,
    ),
)

FREE_SUM_SNAPSHOT_SERIES = (
    (
        "crypto_public_defi_tvl_log1p",
        "defillama_chains",
        "tvl_usd",
        True,
    ),
    (
        "crypto_public_stablecoin_supply_log1p",
        "defillama_stablecoins",
        "circulating",
        True,
    ),
    (
        "crypto_public_yield_tvl_log1p",
        "defillama_yields",
        "tvl_usd",
        True,
    ),
)

FREE_LAST_SNAPSHOT_SERIES = (
    (
        "crypto_public_eth_gas_fast_gwei_log1p",
        "blockscout_ethereum_gas",
        "gas_price_fast_gwei",
        True,
        1.0,
    ),
    (
        "crypto_public_eth_network_utilization",
        "blockscout_ethereum_gas",
        "network_utilization_pct",
        False,
        0.01,
    ),
    (
        "crypto_public_btc_hashrate_log1p",
        "bitcoin_hashrate_history",
        "hashrate",
        True,
        1.0,
    ),
)

HYPERLIQUID_METRICS = {
    "funding_rate": "crypto_public_hyperliquid_funding_rate",
    "premium": "crypto_public_hyperliquid_premium",
    "open_interest": "crypto_public_hyperliquid_open_interest_log1p",
    "day_notional_volume": "crypto_public_hyperliquid_day_volume_log1p",
}

FREE_FEATURES = (
    *(spec[0] for spec in FREE_MARKET_SERIES),
    *(spec[0] for spec in FREE_SNAPSHOT_SERIES),
    *(spec[0] for spec in FREE_SUM_SNAPSHOT_SERIES),
    *(spec[0] for spec in FREE_LAST_SNAPSHOT_SERIES),
    *HYPERLIQUID_METRICS.values(),
    "crypto_public_market_available",
    "crypto_public_hyperliquid_available",
)

OUTPUT_FEATURES = tuple(
    dict.fromkeys(
        (
            *BYBIT_FEATURES,
            *BINANCE_FEATURES,
            *OKX_FEATURES,
            *FREE_FEATURES,
            *FRED_FEATURES,
            *SEC_MARKET_FEATURES,
            *SEC_ASSET_FEATURES,
            *COINMETRICS_FEATURES,
            *COINGECKO_MARKET_FEATURES,
            *COINGECKO_ASSET_FEATURES,
            *ETF_ISSUER_FEATURES,
        )
    )
)


@dataclass(slots=True)
class SymbolCoverage:
    symbol: str
    base_coin: str
    okx_code: str | None
    status: str
    rows: int
    first_date: str | None
    last_date: str | None
    message: str | None = None
    okx_mapping_basis: str | None = None
    okx_status: str = "unmapped"
    okx_rows: int = 0
    binance_code: str | None = None
    binance_mapping_basis: str | None = None
    binance_status: str = "unmapped"
    binance_rows: int = 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bybit-daily-dir", default="data_bybit/perpetual_daily")
    parser.add_argument("--okx-dir", default="data_okx/1m")
    parser.add_argument("--binance-dir", default="data_binance/1m")
    parser.add_argument(
        "--free-public-path", default="data_free_public/observations.parquet"
    )
    parser.add_argument(
        "--fred-macro-path", default="data_fred_crypto_macro/observations.parquet"
    )
    parser.add_argument(
        "--coinmetrics-vintage-dir", default="data_coinmetrics_community/vintages"
    )
    parser.add_argument("--crypto-etf-dir", default="data_crypto_etf")
    parser.add_argument("--crypto-reference-dir", default="data_crypto_reference")
    parser.add_argument(
        "--output-path",
        default="data_bybit/public_features/bybit_crypto_public_daily.parquet",
    )
    parser.add_argument("--symbols", nargs="*", default=None)
    return parser.parse_args()


def _parse_utc_expr(column: str, schema: pl.Schema) -> pl.Expr:
    return (
        pl.col(column).str.to_datetime(strict=False, time_zone="UTC")
        if schema[column] == pl.String
        else pl.col(column).cast(pl.Datetime("us", "UTC"), strict=False)
    )


def _write_parquet_atomic(frame: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        pq.write_table(
            frame.to_arrow(), temporary, compression="snappy", write_statistics=True
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _ensure_output_feature_schema(frame: pl.DataFrame) -> pl.DataFrame:
    missing = [name for name in OUTPUT_FEATURES if name not in frame.columns]
    if missing:
        frame = frame.with_columns(
            *(pl.lit(None, dtype=pl.Float64).alias(name) for name in missing)
        )
    return frame.select("date", "symbol", *OUTPUT_FEATURES)


def _write_text_atomic(text: str, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(text, encoding="utf-8")
    os.replace(temporary, path)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1 << 20):
            digest.update(chunk)
    return digest.hexdigest()


def _instrument_rows(
    bybit_daily_dir: Path, symbols: set[str]
) -> list[dict[str, object]]:
    path = bybit_daily_dir / "symbols.csv"
    if not path.is_file():
        raise FileNotFoundError(f"missing materialized Bybit symbol receipt: {path}")
    frame = pl.read_csv(path, infer_schema_length=10_000)
    required = {"code", "base_coin"}
    if missing := required - set(frame.columns):
        raise ValueError(f"Bybit symbol receipt missing columns: {sorted(missing)}")
    if symbols:
        frame = frame.filter(pl.col("code").str.to_uppercase().is_in(symbols))
        missing_symbols = symbols - set(frame["code"].str.to_uppercase().to_list())
        if missing_symbols:
            raise ValueError(
                f"requested symbols unavailable: {sorted(missing_symbols)}"
            )
    return frame.select("code", "base_coin").unique().sort("code").to_dicts()


def _okx_base_map(okx_dir: Path) -> dict[str, str]:
    path = okx_dir / "symbols.csv"
    if not path.is_file():
        return {}
    frame = pl.read_csv(path, infer_schema_length=10_000).filter(
        (pl.col("settle_ccy") == "USDT")
        & (pl.col("ct_type") == "linear")
        & (pl.col("state") == "live")
    )
    family_column = "inst_family" if "inst_family" in frame.columns else None
    symbol_column = next(
        (name for name in ("okx_symbol", "name") if name in frame.columns), None
    )
    if family_column is None and symbol_column is None:
        return {}
    mapping: dict[str, str] = {}
    columns = ["code", family_column or symbol_column]
    for row in frame.select(*columns).to_dicts():
        family = str(row[family_column or symbol_column] or "")
        if family_column is None and family.endswith("-SWAP"):
            family = family.removesuffix("-SWAP")
        if not family.endswith("-USDT"):
            continue
        base = family.removesuffix("-USDT").upper()
        code = str(row["code"])
        if (okx_dir / f"{code}_features.parquet").is_file():
            mapping.setdefault(base, code)
    return mapping


def _binance_base_map(binance_dir: Path) -> dict[str, str]:
    path = binance_dir / "symbols.csv"
    if not path.is_file():
        return {}
    frame = pl.read_csv(path, infer_schema_length=10_000).filter(
        (pl.col("quote_asset") == "USDT")
        & (pl.col("margin_asset") == "USDT")
        & (pl.col("contract_type") == "PERPETUAL")
    )
    mapping: dict[str, str] = {}
    for row in frame.select("binance_symbol", "base_asset").to_dicts():
        base = str(row["base_asset"] or "").upper()
        code = str(row["binance_symbol"] or "")
        if base and code and (binance_dir / f"{code}_features.parquet").is_file():
            mapping.setdefault(base, code)
    return mapping


def _resolve_okx_mapping(
    base_coin: str, mapping: dict[str, str]
) -> tuple[str | None, str | None]:
    return _resolve_base_mapping(base_coin, mapping)


def _resolve_base_mapping(
    base_coin: str, mapping: dict[str, str]
) -> tuple[str | None, str | None]:
    base = str(base_coin).strip().upper()
    if base in mapping:
        return mapping[base], "exact_base_coin"
    for multiplier in ("1000000", "10000", "1000"):
        stripped = base.removeprefix(multiplier)
        if stripped != base and stripped in mapping:
            return mapping[stripped], f"denomination_{multiplier}_to_1"
        scaled = f"{multiplier}{base}"
        if scaled in mapping:
            return mapping[scaled], f"denomination_1_to_{multiplier}"
    return None, None


def _bybit_funding_features(path: Path, symbol: str) -> pl.DataFrame:
    frame = pl.read_parquet(path)
    source_columns = {
        "funding_rate_sum_previous_session": "crypto_bybit_funding_rate_sum_1d",
        "funding_cashflow_coefficient_previous_session": "crypto_bybit_funding_cashflow_coefficient_1d",
        "funding_last_rate_previous_session": "crypto_bybit_funding_last_rate",
        "funding_age_hours_at_decision": "crypto_bybit_funding_age_hours",
        "funding_event_count_previous_session": "crypto_bybit_funding_event_count_1d",
    }
    missing = {"date", *source_columns} - set(frame.columns)
    if missing:
        raise ValueError(
            f"{path.name} missing causal Bybit funding features: {sorted(missing)}"
        )
    return frame.select(
        pl.col("date").cast(pl.String),
        pl.lit(symbol).alias("symbol"),
        *[
            pl.col(source).cast(pl.Float64).alias(target)
            for source, target in source_columns.items()
        ],
        (pl.col("funding_rate_sum_previous_session").cast(pl.Float64) * 365.0).alias(
            "crypto_bybit_funding_realized_annualized"
        ),
        (
            pl.col("funding_rate_sum_previous_session")
            .cast(pl.Float64)
            .is_finite()
            .fill_null(False)
            .cast(pl.Float64)
            .alias("crypto_bybit_funding_available")
        ),
    ).select("date", "symbol", *BYBIT_FEATURES)


def _positive_log_ratio(last: str, first: str, alias: str) -> pl.Expr:
    return (
        pl.when((pl.col(last) > 0) & (pl.col(first) > 0))
        .then((pl.col(last) / pl.col(first)).log())
        .otherwise(None)
        .alias(alias)
    )


def _okx_daily(path: Path, symbol: str) -> pl.DataFrame:
    frame = pl.read_parquet(path)
    required = {
        "date",
        "okx_mark_open",
        "okx_mark_high",
        "okx_mark_low",
        "okx_mark_close",
        "okx_index_open",
        "okx_index_high",
        "okx_index_low",
        "okx_index_close",
    }
    if missing := required - set(frame.columns):
        raise ValueError(f"{path.name} missing OKX public columns: {sorted(missing)}")
    timestamps = frame.select(_parse_utc_expr("date", frame.schema).alias("__ts"))
    positive_deltas = (
        timestamps.select(pl.col("__ts").diff().dt.total_minutes().drop_nulls())
        .to_series()
        .to_numpy()
    )
    positive_deltas = positive_deltas[positive_deltas > 0]
    if positive_deltas.size == 0:
        raise ValueError(f"{path.name} cannot infer completed-bar interval")
    values, counts = np.unique(positive_deltas, return_counts=True)
    interval_minutes = int(values[int(np.argmax(counts))])
    if interval_minutes < 1 or interval_minutes > 60:
        raise ValueError(f"unsupported OKX bar interval: {interval_minutes} minutes")
    normalized = (
        frame.with_columns(
            _parse_utc_expr("date", frame.schema).alias("__bar_open_utc")
        )
        .with_columns(
            (
                pl.col("okx_funding_age_hours").is_not_null()
                & (
                    pl.col("okx_funding_age_hours")
                    < pl.col("okx_funding_age_hours").shift(1)
                )
            )
            .fill_null(False)
            .alias("__funding_event")
            if "okx_funding_age_hours" in frame.columns
            else pl.lit(False).alias("__funding_event")
        )
        .with_columns(
            (pl.col("__bar_open_utc") + pl.duration(minutes=interval_minutes)).alias(
                "__available_utc"
            )
        )
        .with_columns(
            (
                (
                    pl.col("__available_utc")
                    - pl.duration(minutes=BOUNDARY_MINUTES_UTC)
                    - pl.duration(microseconds=1)
                )
                .dt.date()
                .dt.offset_by("1d")
            ).alias("__session_date")
        )
    )

    def first_non_null(column: str, alias: str) -> pl.Expr:
        return pl.col(column).drop_nulls().first().alias(alias)

    def last_non_null(column: str, alias: str) -> pl.Expr:
        return pl.col(column).drop_nulls().last().alias(alias)

    aggregations: list[pl.Expr] = [
        first_non_null("okx_mark_open", "__mark_open"),
        pl.col("okx_mark_high").max().alias("__mark_high"),
        pl.col("okx_mark_low").min().alias("__mark_low"),
        last_non_null("okx_mark_close", "__mark_close"),
        first_non_null("okx_index_open", "__index_open"),
        pl.col("okx_index_high").max().alias("__index_high"),
        pl.col("okx_index_low").min().alias("__index_low"),
        last_non_null("okx_index_close", "__index_close"),
        pl.col("okx_mark_close").is_not_null().sum().alias("__mark_rows"),
        pl.col("okx_index_close").is_not_null().sum().alias("__index_rows"),
        pl.col("__available_utc").min().alias("__first_available_utc"),
        pl.col("__available_utc").max().alias("crypto_okx_source_available_at_utc"),
        pl.len().alias("__source_rows"),
        pl.col("__available_utc").n_unique().alias("__unique_available_rows"),
        (
            ((pl.col("__available_utc").dt.minute() % interval_minutes) == 0)
            & (pl.col("__available_utc").dt.second() == 0)
            & (pl.col("__available_utc").dt.microsecond() == 0)
        )
        .all()
        .alias("__availability_grid_ok"),
    ]
    last_columns = {
        "okx_contract_mark_basis_log": "crypto_okx_contract_mark_basis_log",
        "okx_mark_index_basis_log": "crypto_okx_mark_index_basis_log",
        "okx_funding_age_hours": "crypto_okx_funding_age_hours",
        "okx_open_interest_usd_to_volume_log": "crypto_okx_open_interest_usd_to_volume_log",
        "okx_long_short_account_ratio_log": "crypto_okx_long_short_account_ratio_log",
        "okx_top_trader_account_ratio_log": "crypto_okx_top_trader_account_ratio_log",
        "okx_top_trader_position_ratio_log": "crypto_okx_top_trader_position_ratio_log",
    }
    for source, target in last_columns.items():
        aggregations.append(
            last_non_null(source, target)
            if source in normalized.columns
            else pl.lit(None, dtype=pl.Float64).alias(target)
        )
    if "okx_funding_realized_rate" in normalized.columns:
        aggregations.extend(
            [
                pl.col("okx_funding_realized_rate")
                .filter(pl.col("__funding_event"))
                .sum()
                .alias("crypto_okx_funding_realized_rate"),
                pl.col("okx_funding_realized_rate")
                .is_not_null()
                .sum()
                .alias("__funding_rows"),
            ]
        )
    else:
        aggregations.extend(
            [
                pl.lit(None, dtype=pl.Float64).alias(
                    "crypto_okx_funding_realized_rate"
                ),
                pl.lit(0, dtype=pl.UInt32).alias("__funding_rows"),
            ]
        )
    if "okx_open_interest_usd" in normalized.columns:
        aggregations.extend(
            [
                first_non_null("okx_open_interest_usd", "__oi_first"),
                last_non_null("okx_open_interest_usd", "__oi_last"),
            ]
        )
    else:
        aggregations.extend(
            [
                pl.lit(None, dtype=pl.Float64).alias("__oi_first"),
                pl.lit(None, dtype=pl.Float64).alias("__oi_last"),
            ]
        )
    for source, alias in (
        ("okx_taker_buy_volume_contracts", "__taker_buy"),
        ("okx_taker_sell_volume_contracts", "__taker_sell"),
    ):
        aggregations.append(
            pl.col(source).sum().alias(alias)
            if source in normalized.columns
            else pl.lit(None, dtype=pl.Float64).alias(alias)
        )
        aggregations.append(
            pl.col(source).is_not_null().sum().alias(f"__count_{source}")
            if source in normalized.columns
            else pl.lit(0, dtype=pl.UInt32).alias(f"__count_{source}")
        )
    positioning_columns = (
        "okx_open_interest_usd",
        "okx_long_short_account_ratio_log",
        "okx_top_trader_account_ratio_log",
        "okx_top_trader_position_ratio_log",
    )
    for column in positioning_columns:
        aggregations.append(
            pl.col(column).is_not_null().sum().alias(f"__count_{column}")
            if column in normalized.columns
            else pl.lit(0, dtype=pl.UInt32).alias(f"__count_{column}")
        )
    expected_rows = 1440 // interval_minutes
    first_available_offset_minutes = (
        BOUNDARY_MINUTES_UTC // interval_minutes + 1
    ) * interval_minutes
    grouped = (
        normalized.group_by("__session_date", maintain_order=True)
        .agg(aggregations)
        .with_columns(
            (pl.col("__source_rows") / float(expected_rows)).alias(
                "crypto_okx_session_coverage"
            ),
            pl.col("__session_date")
            .cast(pl.Datetime("us"))
            .dt.replace_time_zone("UTC")
            .alias("__boundary"),
        )
        .filter(
            (pl.col("__source_rows") == expected_rows)
            & (pl.col("__unique_available_rows") == expected_rows)
            & pl.col("__availability_grid_ok")
            & (
                pl.col("__first_available_utc")
                == pl.col("__boundary")
                - pl.duration(days=1)
                + pl.duration(minutes=first_available_offset_minutes)
            )
            & (pl.col("crypto_okx_source_available_at_utc") == pl.col("__boundary"))
        )
        .with_columns(
            (
                (pl.col("__mark_rows") == expected_rows)
                & (pl.col("__index_rows") == expected_rows)
            ).alias("__core_complete"),
            pl.min_horizontal(
                *[pl.col(f"__count_{column}") for column in positioning_columns]
            ).alias("__positioning_rows"),
            pl.min_horizontal(
                pl.col("__count_okx_taker_buy_volume_contracts"),
                pl.col("__count_okx_taker_sell_volume_contracts"),
            ).alias("__taker_rows"),
        )
    )
    grouped = grouped.with_columns(
        *[
            pl.when(pl.col("__core_complete"))
            .then(_positive_log_ratio(last, first, "__unused"))
            .otherwise(None)
            .alias(name)
            for name, last, first in (
                ("crypto_okx_mark_return_1d", "__mark_close", "__mark_open"),
                ("crypto_okx_index_return_1d", "__index_close", "__index_open"),
                ("crypto_okx_mark_range_log_1d", "__mark_high", "__mark_low"),
                ("crypto_okx_index_range_log_1d", "__index_high", "__index_low"),
            )
        ],
        pl.when(pl.col("__positioning_rows") == expected_rows)
        .then(_positive_log_ratio("__oi_last", "__oi_first", "__unused"))
        .otherwise(None)
        .alias("crypto_okx_open_interest_usd_log_change_1d"),
        pl.when(
            (pl.col("__taker_rows") == expected_rows)
            & ((pl.col("__taker_buy") + pl.col("__taker_sell")) > 0)
        )
        .then(
            (pl.col("__taker_buy") - pl.col("__taker_sell"))
            / (pl.col("__taker_buy") + pl.col("__taker_sell"))
        )
        .otherwise(None)
        .alias("crypto_okx_taker_imbalance_1d"),
        (pl.col("crypto_okx_funding_realized_rate") * 365.0).alias(
            "crypto_okx_funding_realized_annualized"
        ),
        (pl.col("__funding_rows") == expected_rows)
        .cast(pl.Float64)
        .alias("crypto_okx_funding_available"),
        pl.col("__core_complete").cast(pl.Float64).alias("crypto_okx_available"),
        (pl.col("__positioning_rows") == expected_rows)
        .cast(pl.Float64)
        .alias("crypto_okx_positioning_available"),
        (pl.col("__taker_rows") == expected_rows)
        .cast(pl.Float64)
        .alias("crypto_okx_taker_available"),
        pl.lit(symbol).alias("symbol"),
    )
    grouped = grouped.with_columns(
        *[
            pl.when(pl.col("__core_complete"))
            .then(pl.col(name))
            .otherwise(None)
            .alias(name)
            for name in (
                "crypto_okx_contract_mark_basis_log",
                "crypto_okx_mark_index_basis_log",
            )
        ],
        *[
            pl.when(pl.col("__positioning_rows") == expected_rows)
            .then(pl.col(name))
            .otherwise(None)
            .alias(name)
            for name in (
                "crypto_okx_open_interest_usd_to_volume_log",
                "crypto_okx_long_short_account_ratio_log",
                "crypto_okx_top_trader_account_ratio_log",
                "crypto_okx_top_trader_position_ratio_log",
            )
        ],
        pl.when(pl.col("__funding_rows") == expected_rows)
        .then(pl.col("crypto_okx_funding_age_hours"))
        .otherwise(None)
        .alias("crypto_okx_funding_age_hours"),
    )
    return grouped.select(
        pl.col("__session_date").cast(pl.String).alias("date"),
        "symbol",
        *OKX_FEATURES,
        "crypto_okx_source_available_at_utc",
    )


def _binance_daily(path: Path, symbol: str) -> pl.DataFrame:
    frame = pl.read_parquet(path)
    required = {
        "date",
        "binance_volume_quote",
        "binance_trade_count",
        "binance_taker_buy_quote_volume",
        "binance_mark_open",
        "binance_mark_high",
        "binance_mark_low",
        "binance_mark_close",
        "binance_index_open",
        "binance_index_high",
        "binance_index_low",
        "binance_index_close",
        "binance_funding_rate",
        "binance_funding_age_hours",
    }
    if missing := required - set(frame.columns):
        raise ValueError(
            f"{path.name} missing Binance public columns: {sorted(missing)}"
        )
    timestamps = frame.select(_parse_utc_expr("date", frame.schema).alias("__ts"))
    positive_deltas = (
        timestamps.select(pl.col("__ts").diff().dt.total_minutes().drop_nulls())
        .to_series()
        .to_numpy()
    )
    positive_deltas = positive_deltas[positive_deltas > 0]
    if positive_deltas.size == 0:
        raise ValueError(f"{path.name} cannot infer completed-bar interval")
    values, counts = np.unique(positive_deltas, return_counts=True)
    interval_minutes = int(values[int(np.argmax(counts))])
    if interval_minutes < 1 or interval_minutes > 60:
        raise ValueError(
            f"unsupported Binance bar interval: {interval_minutes} minutes"
        )
    normalized = (
        frame.with_columns(
            _parse_utc_expr("date", frame.schema).alias("__bar_open_utc")
        )
        .with_columns(
            (
                pl.col("binance_funding_age_hours").is_not_null()
                & (
                    pl.col("binance_funding_age_hours")
                    < pl.col("binance_funding_age_hours").shift(1)
                )
            )
            .fill_null(False)
            .alias("__funding_event")
        )
        .with_columns(
            (pl.col("__bar_open_utc") + pl.duration(minutes=interval_minutes)).alias(
                "__available_utc"
            )
        )
        .with_columns(
            (
                (
                    pl.col("__available_utc")
                    - pl.duration(minutes=BOUNDARY_MINUTES_UTC)
                    - pl.duration(microseconds=1)
                )
                .dt.date()
                .dt.offset_by("1d")
            ).alias("__session_date")
        )
    )

    def first_non_null(column: str, alias: str) -> pl.Expr:
        return pl.col(column).drop_nulls().first().alias(alias)

    def last_non_null(column: str, alias: str) -> pl.Expr:
        return pl.col(column).drop_nulls().last().alias(alias)

    aggregations: list[pl.Expr] = [
        first_non_null("binance_mark_open", "__mark_open"),
        pl.col("binance_mark_high").max().alias("__mark_high"),
        pl.col("binance_mark_low").min().alias("__mark_low"),
        last_non_null("binance_mark_close", "__mark_close"),
        first_non_null("binance_index_open", "__index_open"),
        pl.col("binance_index_high").max().alias("__index_high"),
        pl.col("binance_index_low").min().alias("__index_low"),
        last_non_null("binance_index_close", "__index_close"),
        pl.col("binance_mark_close").is_not_null().sum().alias("__mark_rows"),
        pl.col("binance_index_close").is_not_null().sum().alias("__index_rows"),
        pl.col("binance_volume_quote").sum().alias("__quote_volume"),
        pl.col("binance_trade_count").sum().alias("__trade_count"),
        pl.col("binance_taker_buy_quote_volume").sum().alias("__taker_buy_quote"),
        last_non_null("binance_funding_rate", "__funding_last_rate"),
        last_non_null("binance_funding_age_hours", "crypto_binance_funding_age_hours"),
        pl.col("binance_funding_rate")
        .filter(pl.col("__funding_event"))
        .sum()
        .alias("crypto_binance_funding_realized_rate"),
        pl.col("binance_funding_rate").is_not_null().sum().alias("__funding_rows"),
        pl.col("__available_utc").min().alias("__first_available_utc"),
        pl.col("__available_utc").max().alias("crypto_binance_source_available_at_utc"),
        pl.len().alias("__source_rows"),
        pl.col("__available_utc").n_unique().alias("__unique_available_rows"),
        (
            ((pl.col("__available_utc").dt.minute() % interval_minutes) == 0)
            & (pl.col("__available_utc").dt.second() == 0)
            & (pl.col("__available_utc").dt.microsecond() == 0)
        )
        .all()
        .alias("__availability_grid_ok"),
    ]
    last_columns = {
        "binance_contract_mark_basis_log": "crypto_binance_contract_mark_basis_log",
        "binance_mark_index_basis_log": "crypto_binance_mark_index_basis_log",
        "binance_global_long_short_account_ratio_log": "crypto_binance_global_long_short_account_ratio_log",
        "binance_top_long_short_account_ratio_log": "crypto_binance_top_trader_account_ratio_log",
        "binance_top_long_short_position_ratio_log": "crypto_binance_top_trader_position_ratio_log",
    }
    for source, target in last_columns.items():
        aggregations.append(
            last_non_null(source, target)
            if source in normalized.columns
            else pl.lit(None, dtype=pl.Float64).alias(target)
        )
    positioning_columns = (
        "binance_open_interest_value_usd",
        "binance_global_long_short_account_ratio_log",
        "binance_top_long_short_account_ratio_log",
        "binance_top_long_short_position_ratio_log",
    )
    for column in positioning_columns:
        aggregations.append(
            pl.col(column).is_not_null().sum().alias(f"__count_{column}")
            if column in normalized.columns
            else pl.lit(0, dtype=pl.UInt32).alias(f"__count_{column}")
        )
    if "binance_open_interest_value_usd" in normalized.columns:
        aggregations.extend(
            [
                first_non_null("binance_open_interest_value_usd", "__oi_first"),
                last_non_null("binance_open_interest_value_usd", "__oi_last"),
            ]
        )
    else:
        aggregations.extend(
            [
                pl.lit(None, dtype=pl.Float64).alias("__oi_first"),
                pl.lit(None, dtype=pl.Float64).alias("__oi_last"),
            ]
        )
    expected_rows = 1440 // interval_minutes
    first_available_offset_minutes = (
        BOUNDARY_MINUTES_UTC // interval_minutes + 1
    ) * interval_minutes
    grouped = (
        normalized.group_by("__session_date", maintain_order=True)
        .agg(aggregations)
        .with_columns(
            (pl.col("__source_rows") / float(expected_rows)).alias(
                "crypto_binance_session_coverage"
            ),
            pl.col("__session_date")
            .cast(pl.Datetime("us"))
            .dt.replace_time_zone("UTC")
            .alias("__boundary"),
        )
        .filter(
            (pl.col("__source_rows") == expected_rows)
            & (pl.col("__unique_available_rows") == expected_rows)
            & pl.col("__availability_grid_ok")
            & (
                pl.col("__first_available_utc")
                == pl.col("__boundary")
                - pl.duration(days=1)
                + pl.duration(minutes=first_available_offset_minutes)
            )
            & (pl.col("crypto_binance_source_available_at_utc") == pl.col("__boundary"))
        )
        .with_columns(
            (
                (pl.col("__mark_rows") == expected_rows)
                & (pl.col("__index_rows") == expected_rows)
            ).alias("__core_complete"),
            pl.min_horizontal(
                *[pl.col(f"__count_{column}") for column in positioning_columns]
            ).alias("__positioning_rows"),
        )
    )
    core_features = {
        "crypto_binance_mark_return_1d": _positive_log_ratio(
            "__mark_close", "__mark_open", "__unused"
        ),
        "crypto_binance_index_return_1d": _positive_log_ratio(
            "__index_close", "__index_open", "__unused"
        ),
        "crypto_binance_mark_range_log_1d": _positive_log_ratio(
            "__mark_high", "__mark_low", "__unused"
        ),
        "crypto_binance_index_range_log_1d": _positive_log_ratio(
            "__index_high", "__index_low", "__unused"
        ),
    }
    grouped = grouped.with_columns(
        *[
            pl.when(pl.col("__core_complete"))
            .then(expression)
            .otherwise(None)
            .alias(name)
            for name, expression in core_features.items()
        ],
        (pl.col("crypto_binance_funding_realized_rate") * 365.0).alias(
            "crypto_binance_funding_realized_annualized"
        ),
        (pl.col("__funding_rows") == expected_rows)
        .cast(pl.Float64)
        .alias("crypto_binance_funding_available"),
        pl.when(pl.col("__positioning_rows") == expected_rows)
        .then(_positive_log_ratio("__oi_last", "__oi_first", "__unused"))
        .otherwise(None)
        .alias("crypto_binance_open_interest_usd_log_change_1d"),
        pl.when(
            (pl.col("__positioning_rows") == expected_rows)
            & (pl.col("__oi_last") > 0)
            & (pl.col("__quote_volume") > 0)
        )
        .then((pl.col("__oi_last") / pl.col("__quote_volume")).log())
        .otherwise(None)
        .alias("crypto_binance_open_interest_usd_to_volume_log"),
        pl.when(pl.col("__quote_volume") > 0)
        .then(2.0 * pl.col("__taker_buy_quote") / pl.col("__quote_volume") - 1.0)
        .otherwise(None)
        .alias("crypto_binance_taker_imbalance_1d"),
        pl.col("__quote_volume")
        .clip(lower_bound=0.0)
        .log1p()
        .alias("crypto_binance_quote_volume_log1p"),
        pl.col("__trade_count")
        .cast(pl.Float64)
        .clip(lower_bound=0.0)
        .log1p()
        .alias("crypto_binance_trade_count_log1p"),
        pl.col("__core_complete")
        .cast(pl.Float64)
        .alias("crypto_binance_core_available"),
        (pl.col("__positioning_rows") == expected_rows)
        .cast(pl.Float64)
        .alias("crypto_binance_positioning_available"),
        pl.lit(symbol).alias("symbol"),
    )
    positioning_outputs = {
        "crypto_binance_global_long_short_account_ratio_log",
        "crypto_binance_top_trader_account_ratio_log",
        "crypto_binance_top_trader_position_ratio_log",
    }
    grouped = grouped.with_columns(
        *[
            pl.when(pl.col("__positioning_rows") == expected_rows)
            .then(pl.col(name))
            .otherwise(None)
            .alias(name)
            for name in positioning_outputs
        ]
    )
    return grouped.select(
        pl.col("__session_date").cast(pl.String).alias("date"),
        "symbol",
        *BINANCE_FEATURES,
        "crypto_binance_source_available_at_utc",
    )


def _log1p(value: float | None, enabled: bool) -> float | None:
    if value is None or not np.isfinite(value):
        return None
    if not enabled:
        return float(value)
    return float(np.log1p(max(0.0, value)))


def _free_public_rows(
    path: Path,
    dates: list[str],
    symbol_bases: dict[str, str],
) -> tuple[pl.DataFrame, dict[str, object]]:
    feature_names = (
        [item[0] for item in FREE_MARKET_SERIES]
        + [item[0] for item in FREE_SNAPSHOT_SERIES]
        + [item[0] for item in FREE_SUM_SNAPSHOT_SERIES]
        + [item[0] for item in FREE_LAST_SNAPSHOT_SERIES]
    )
    market_rows = [
        {
            "date": date,
            "symbol": MARKET_SYMBOL,
            **{name: None for name in feature_names},
        }
        for date in dates
    ]
    symbol_rows: list[dict[str, object]] = []
    if not path.is_file():
        return pl.DataFrame(market_rows), {"status": "missing", "path": str(path)}
    raw = pl.read_parquet(path).filter(pl.col("value_float").is_not_null())
    required = {
        "dataset",
        "entity",
        "metric",
        "event_ts_utc",
        "available_at_utc",
        "value_float",
    }
    if missing := required - set(raw.columns):
        raise ValueError(f"free-public observations missing columns: {sorted(missing)}")
    selected_datasets = (
        {item[1] for item in FREE_MARKET_SERIES}
        | {item[1] for item in FREE_SNAPSHOT_SERIES}
        | {item[1] for item in FREE_SUM_SNAPSHOT_SERIES}
        | {item[1] for item in FREE_LAST_SNAPSHOT_SERIES}
        | {"hyperliquid_perp_context"}
    )
    raw = (
        raw.filter(pl.col("dataset").is_in(selected_datasets))
        .with_columns(
            _parse_utc_expr("available_at_utc", raw.schema).alias("__available"),
            _parse_utc_expr("event_ts_utc", raw.schema).alias("__event"),
        )
        .drop_nulls("__available")
    )
    earliest_available = raw["__available"].min()
    if earliest_available is None:
        return pl.DataFrame(market_rows), {"status": "empty", "path": str(path)}

    for row in market_rows:
        cutoff = datetime.fromisoformat(str(row["date"])).replace(
            hour=0, minute=BOUNDARY_MINUTES_UTC, tzinfo=timezone.utc
        )
        if cutoff < earliest_available:
            continue
        eligible = raw.filter(
            (pl.col("__available") <= cutoff)
            & (pl.col("__event").is_null() | (pl.col("__event") <= cutoff))
        )
        for name, dataset, metric, entity, log_value in FREE_MARKET_SERIES:
            values = eligible.filter(
                (pl.col("dataset") == dataset)
                & (pl.col("metric") == metric)
                & (pl.col("entity") == entity)
            ).sort(["__event", "__available"], nulls_last=False)
            if values.height:
                row[name] = _log1p(values["value_float"][-1], log_value)
        for name, dataset, metric, log_value in FREE_SNAPSHOT_SERIES:
            values = eligible.filter(
                (pl.col("dataset") == dataset) & (pl.col("metric") == metric)
            ).sort("__available")
            if values.height:
                row[name] = _log1p(values["value_float"][-1], log_value)
        for name, dataset, metric, log_value in FREE_SUM_SNAPSHOT_SERIES:
            values = (
                eligible.filter(
                    (pl.col("dataset") == dataset) & (pl.col("metric") == metric)
                )
                .sort(["entity", "__event", "__available"])
                .group_by("entity", maintain_order=True)
                .agg(pl.col("value_float").last())
            )
            value = float(values["value_float"].sum()) if values.height else None
            row[name] = _log1p(value, log_value)
        for name, dataset, metric, log_value, scale in FREE_LAST_SNAPSHOT_SERIES:
            values = eligible.filter(
                (pl.col("dataset") == dataset) & (pl.col("metric") == metric)
            ).sort(["__event", "__available"])
            value = float(values["value_float"][-1]) * scale if values.height else None
            row[name] = _log1p(value, log_value)
        row["crypto_public_market_available"] = 1.0

        hyper = eligible.filter(pl.col("dataset") == "hyperliquid_perp_context")
        for symbol, base in symbol_bases.items():
            output: dict[str, object] = {"date": row["date"], "symbol": symbol}
            matched = hyper.filter(pl.col("entity").str.to_uppercase() == base.upper())
            any_value = False
            for metric, name in HYPERLIQUID_METRICS.items():
                values = matched.filter(pl.col("metric") == metric).sort("__available")
                value = values["value_float"][-1] if values.height else None
                log_value = metric in {"open_interest", "day_notional_volume"}
                output[name] = _log1p(value, log_value)
                any_value = any_value or output[name] is not None
            output["crypto_public_hyperliquid_available"] = float(any_value)
            if any_value:
                symbol_rows.append(output)
    market = pl.from_dicts(market_rows, infer_schema_length=None).with_columns(
        pl.col("crypto_public_market_available").fill_null(0.0)
        if "crypto_public_market_available"
        in pl.from_dicts(market_rows, infer_schema_length=None).columns
        else pl.lit(0.0).alias("crypto_public_market_available")
    )
    symbol_frame = (
        pl.from_dicts(symbol_rows, infer_schema_length=None)
        if symbol_rows
        else pl.DataFrame()
    )
    output = pl.concat([market, symbol_frame], how="diagonal_relaxed")
    return output, {
        "status": "prospective_only",
        "path": str(path),
        "sha256": _sha256(path),
        "earliest_available_at_utc": earliest_available.isoformat(),
        "historical_event_backprojection": False,
        "rows": raw.height,
    }


def _source_decisions() -> dict[str, dict[str, object]]:
    return {
        "bybit_funding": {
            "decision": "included",
            "reason": "official event history shifted to the prior completed decision session",
        },
        "binance_futures": {
            "decision": "included",
            "reason": "documented historical 15-minute event series; every bar is delayed until bar close",
        },
        "okx_futures": {
            "decision": "included",
            "reason": "historical interval series delayed until the inferred completed-bar boundary",
        },
        "free_public_observations": {
            "decision": "included_prospective_only",
            "reason": "requires event_ts and first local available_at no later than the decision cutoff",
        },
        "coinmetrics_community": {
            "decision": "included_prospective_vintages_only",
            "reason": "only canonical retrieval vintages available before each decision are used; latest-view history is never backprojected",
        },
        "crypto_etf": {
            "decision": "included_with_two_clocks",
            "reason": "SEC filings use acceptance timestamps; issuer holdings and reserves start only at first local available_at",
        },
        "dune_crypto": {
            "decision": "excluded",
            "reason": "retained query results were first available in August 2026 and cannot be backprojected to their historical event dates; latest refresh is credit-blocked",
        },
        "crypto_reference": {
            "decision": "included_prospective_snapshots_only",
            "reason": "CoinGecko snapshots are usable only after their observed available_at and ambiguous ticker matches fail closed",
        },
        "fred_macro": {
            "decision": "included_initial_release_only",
            "reason": "FRED output_type=4 initial releases are delayed to the next UTC midnight; later revisions are excluded",
        },
    }


def _feature_source_contract(feature: str) -> tuple[str, str]:
    if feature.startswith("crypto_bybit_"):
        return "bybit_funding", "historical_event_time_aligned"
    if feature.startswith("crypto_binance_"):
        return "binance_futures", "historical_completed_bar"
    if feature.startswith("crypto_okx_"):
        return "okx_futures", "historical_completed_bar"
    if feature.startswith("crypto_public_macro_"):
        return "fred_macro", "historical_initial_release_next_utc_day"
    if feature.startswith("crypto_public_sec_"):
        return "sec_edgar", "historical_acceptance_time_with_registry_selection_risk"
    if feature.startswith("crypto_public_onchain_"):
        return "coinmetrics_community", "prospective_retrieval_vintage"
    if feature.startswith("crypto_public_coingecko_"):
        return "coingecko", "prospective_snapshot"
    if feature.startswith("crypto_public_etf_"):
        return "crypto_etf_issuers", "prospective_snapshot_issuer_asserted"
    return "free_public_web", "prospective_first_observed"


def _feature_quality_rows(frame: pl.DataFrame) -> pl.DataFrame:
    features = sorted(
        column
        for column in frame.columns
        if column.startswith("crypto_")
        and not column.endswith("source_available_at_utc")
    )
    rows: list[dict[str, object]] = []
    for feature in features:
        finite = (
            pl.col(feature).cast(pl.Float64, strict=False).is_finite().fill_null(False)
        )
        stats = frame.select(
            finite.sum().alias("finite_rows"),
            pl.when(finite)
            .then(pl.col("date"))
            .otherwise(None)
            .min()
            .alias("first_date"),
            pl.when(finite)
            .then(pl.col("date"))
            .otherwise(None)
            .max()
            .alias("last_date"),
            pl.when(finite)
            .then(pl.col("symbol"))
            .otherwise(None)
            .drop_nulls()
            .n_unique()
            .alias("symbols"),
        ).row(0, named=True)
        source, contract = _feature_source_contract(feature)
        finite_rows = int(stats["finite_rows"] or 0)
        rows.append(
            {
                "feature": feature,
                "source_family": source,
                "point_in_time_contract": contract,
                "finite_rows": finite_rows,
                "finite_fraction": finite_rows / max(1, frame.height),
                "symbols": int(stats["symbols"] or 0),
                "first_date": stats["first_date"],
                "last_date": stats["last_date"],
                "status": "covered"
                if finite_rows
                else "waiting_no_eligible_observation",
            }
        )
    return pl.from_dicts(rows, infer_schema_length=None)


def _input_receipts(
    bybit_dir: Path, binance_dir: Path, okx_dir: Path
) -> dict[str, dict[str, object]]:
    paths = {
        "bybit_daily_symbols": bybit_dir / "symbols.csv",
        "bybit_materialize_summary": bybit_dir / "materialize_summary.json",
        "binance_symbols": binance_dir / "symbols.csv",
        "binance_historical_feature_catalog": (
            binance_dir / "binance_historical_feature_catalog.json"
        ),
        "okx_symbols": okx_dir / "symbols.csv",
        "coinmetrics_summary": Path("data_coinmetrics_community/download_summary.json"),
        "crypto_etf_summary": Path("data_crypto_etf/download_summary.json"),
        "dune_crypto_summary": Path("data_dune_crypto/download_summary.json"),
        "crypto_reference_summary": Path("data_crypto_reference/download_summary.json"),
        "fred_crypto_macro_summary": Path(
            "data_fred_crypto_macro/download_summary.json"
        ),
    }
    return {
        name: {
            "path": str(path),
            "exists": path.is_file(),
            "bytes": path.stat().st_size if path.is_file() else None,
            "sha256": _sha256(path) if path.is_file() else None,
        }
        for name, path in paths.items()
    }


def main() -> None:
    args = parse_args()
    started = datetime.now(timezone.utc)
    bybit_dir = Path(args.bybit_daily_dir)
    okx_dir = Path(args.okx_dir)
    binance_dir = Path(args.binance_dir)
    output_path = Path(args.output_path)
    requested = {
        str(item).strip().upper() for item in (args.symbols or []) if str(item).strip()
    }
    instruments = _instrument_rows(bybit_dir, requested)
    okx_map = _okx_base_map(okx_dir)
    binance_map = _binance_base_map(binance_dir)
    frames: list[pl.DataFrame] = []
    coverage: list[SymbolCoverage] = []
    all_dates: set[str] = set()
    symbol_bases: dict[str, str] = {}
    for record in instruments:
        symbol = str(record["code"])
        base = str(record["base_coin"] or "").upper()
        symbol_bases[symbol] = base
        bybit_path = bybit_dir / f"{symbol}_features.parquet"
        messages: list[str] = []
        source_frames: list[pl.DataFrame] = []
        bybit_failed = False
        if bybit_path.is_file():
            all_dates.update(
                pl.read_parquet(bybit_path, columns=["date"])["date"].cast(pl.String)
            )
            try:
                frames.append(_bybit_funding_features(bybit_path, symbol))
            except Exception as exc:
                messages.append(f"Bybit funding features: {type(exc).__name__}: {exc}")
                bybit_failed = True
        else:
            messages.append(f"missing Bybit daily feature file: {bybit_path}")
            bybit_failed = True
        okx_code, okx_mapping_basis = _resolve_okx_mapping(base, okx_map)
        okx_status = "unmapped"
        okx_rows = 0
        if okx_code is not None:
            try:
                frame = _okx_daily(okx_dir / f"{okx_code}_features.parquet", symbol)
                frames.append(frame)
                source_frames.append(frame)
                okx_status = "mapped"
                okx_rows = frame.height
            except Exception as exc:
                okx_status = "failed"
                messages.append(f"OKX: {type(exc).__name__}: {exc}")
        binance_code, binance_mapping_basis = _resolve_base_mapping(base, binance_map)
        binance_status = "unmapped"
        binance_rows = 0
        if binance_code is not None:
            try:
                frame = _binance_daily(
                    binance_dir / f"{binance_code}_features.parquet", symbol
                )
                frames.append(frame)
                source_frames.append(frame)
                binance_status = "mapped"
                binance_rows = frame.height
            except Exception as exc:
                binance_status = "failed"
                messages.append(f"Binance: {type(exc).__name__}: {exc}")
        date_values = [
            str(value)
            for frame in source_frames
            for value in (frame["date"].min(), frame["date"].max())
            if value is not None
        ]
        failed = bybit_failed or "failed" in {okx_status, binance_status}
        mapped = "mapped" in {okx_status, binance_status}
        coverage.append(
            SymbolCoverage(
                symbol=symbol,
                base_coin=base,
                okx_code=okx_code,
                status="failed" if failed else ("mapped" if mapped else "unmapped"),
                rows=okx_rows + binance_rows,
                first_date=min(date_values) if date_values else None,
                last_date=max(date_values) if date_values else None,
                message=" | ".join(messages) if messages else None,
                okx_mapping_basis=okx_mapping_basis,
                okx_status=okx_status,
                okx_rows=okx_rows,
                binance_code=binance_code,
                binance_mapping_basis=binance_mapping_basis,
                binance_status=binance_status,
                binance_rows=binance_rows,
            )
        )
    if not all_dates:
        raise RuntimeError("no Bybit daily dates found")
    free_frame, free_receipt = _free_public_rows(
        Path(args.free_public_path), sorted(all_dates), symbol_bases
    )
    if free_frame.height:
        frames.append(free_frame)
    source_receipts: dict[str, dict[str, object]] = {"free_public": free_receipt}
    web_builders = (
        (
            "fred_macro",
            fred_macro_rows,
            (Path(args.fred_macro_path), sorted(all_dates)),
        ),
        (
            "sec_etf_filings",
            sec_etf_filing_rows,
            (
                Path(args.crypto_etf_dir) / "normalized" / "sec",
                sorted(all_dates),
                symbol_bases,
            ),
        ),
        (
            "coinmetrics_vintages",
            coinmetrics_vintage_rows,
            (Path(args.coinmetrics_vintage_dir), sorted(all_dates), symbol_bases),
        ),
        (
            "coingecko_snapshots",
            coingecko_snapshot_rows,
            (Path(args.crypto_reference_dir), sorted(all_dates), symbol_bases),
        ),
        (
            "etf_issuer_snapshots",
            etf_issuer_snapshot_rows,
            (Path(args.crypto_etf_dir), sorted(all_dates), symbol_bases),
        ),
    )
    for source_name, builder, builder_args in web_builders:
        frame, receipt = builder(*builder_args)
        source_receipts[source_name] = receipt
        if frame.height:
            frames.append(frame)
    if not frames:
        raise RuntimeError("no causal public feature rows materialized")
    output = (
        pl.concat(frames, how="diagonal_relaxed")
        .group_by(["date", "symbol"], maintain_order=True)
        .agg(pl.exclude("date", "symbol").drop_nulls().last())
        .sort(["date", "symbol"])
    )
    output = _ensure_output_feature_schema(output)
    numeric_columns = [
        name for name, dtype in output.schema.items() if dtype.is_numeric()
    ]
    output = output.with_columns(
        *[
            pl.when(pl.col(name).is_finite())
            .then(pl.col(name))
            .otherwise(None)
            .alias(name)
            for name in numeric_columns
        ]
    )
    _write_parquet_atomic(output, output_path)
    quality_path = output_path.with_name(f"{output_path.stem}_quality.csv")
    quality = _feature_quality_rows(output)
    _write_text_atomic(quality.write_csv(), quality_path)
    coverage_path = output_path.with_name(f"{output_path.stem}_coverage.csv")
    _write_text_atomic(
        pl.DataFrame(
            [asdict(item) for item in coverage], infer_schema_length=None
        ).write_csv(),
        coverage_path,
    )
    failed = [item for item in coverage if item.status == "failed"]
    summary = {
        "contract_version": 5,
        "decision_boundary_utc": "00:00",
        "execution_boundary_utc": "00:05",
        "decision_to_execution_lag_minutes": 5,
        "okx_bar_availability": "bar_open_plus_inferred_interval_le_decision_cutoff",
        "free_public_availability": "available_at_utc_le_decision_cutoff",
        "historical_event_backprojection": False,
        "non_finite_output_policy": "normalize_nan_and_inf_to_null_before_write",
        "requested_symbols": len(instruments),
        "okx_mapped_symbols": sum(item.okx_status == "mapped" for item in coverage),
        "binance_bar_availability": "bar_open_plus_inferred_interval_le_decision_cutoff",
        "binance_mapped_symbols": sum(
            item.binance_status == "mapped" for item in coverage
        ),
        "binance_denomination_normalized_symbols": sum(
            str(item.binance_mapping_basis or "").startswith("denomination_")
            for item in coverage
        ),
        "binance_unmapped_symbols": sum(
            item.binance_status == "unmapped" for item in coverage
        ),
        "okx_denomination_normalized_symbols": sum(
            str(item.okx_mapping_basis or "").startswith("denomination_")
            for item in coverage
        ),
        "okx_unmapped_symbols": sum(item.okx_status == "unmapped" for item in coverage),
        "failed_symbols": len(failed),
        "output_rows": output.height,
        "output_columns": output.columns,
        "output_path": str(output_path),
        "output_sha256": _sha256(output_path),
        "quality_path": str(quality_path),
        "quality_sha256": _sha256(quality_path),
        "quality_feature_status_counts": quality["status"].value_counts().to_dicts(),
        "public_web_sources": source_receipts,
        "public_web_feature_families": {
            "fred_macro": list(FRED_FEATURES),
            "sec_market": list(SEC_MARKET_FEATURES),
            "sec_asset": list(SEC_ASSET_FEATURES),
            "coinmetrics_onchain": list(COINMETRICS_FEATURES),
            "coingecko_market": list(COINGECKO_MARKET_FEATURES),
            "coingecko_asset": list(COINGECKO_ASSET_FEATURES),
            "etf_issuer": list(ETF_ISSUER_FEATURES),
        },
        "source_decisions": _source_decisions(),
        "input_receipts": _input_receipts(bybit_dir, binance_dir, okx_dir),
        "started_at_utc": started.isoformat(),
        "ended_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    _write_text_atomic(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n",
        output_path.with_name(f"{output_path.stem}_summary.json"),
    )
    print(json.dumps(summary, ensure_ascii=False))
    if failed:
        raise RuntimeError(
            f"public feature materialization failed for {len(failed)} symbols"
        )


if __name__ == "__main__":
    main()
