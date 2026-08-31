"""Causal all-TAIFEX action sidecar for a full cash-stock input panel.

The stock panel remains the model's feature universe.  This module attaches a
separate fixed 1,936-slot futures axis containing prior-session model context
and current/future execution labels.  Keeping the two axes separate prevents
physical-contract outcomes from being mistaken for stock features.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import json
from pathlib import Path
from typing import Final

import numpy as np

from stockagent.data.panel import PanelData
from stockagent.data.tw_futures_portfolio_daily import (
    FUTURES_MODEL_FEATURE_COLUMNS,
    TAIFEX_FUTURES_PORTFOLIO_BACKTEST_CONTRACT_VERSION,
    TAIFEX_FUTURES_PORTFOLIO_DATA_CONTRACT_VERSION,
    TAIFEX_FUTURES_PORTFOLIO_FEATURE_CONTRACT_VERSION,
    TAIFEX_FUTURES_PORTFOLIO_FIXED_SLOT_COUNT,
    _sha256_file,
)
from stockagent.research.taifex_transaction_tax import (
    stock_index_futures_tax_rate,
)

try:
    import polars as pl
except Exception:  # pragma: no cover - checked at the public entry point
    pl = None

try:
    import pyarrow.parquet as pq
except Exception:  # pragma: no cover - checked at the public entry point
    pq = None


TW_STOCK_CONTEXT_FUTURES_PORTFOLIO_CONTRACT_VERSION: Final[int] = 2
TW_STOCK_CONTEXT_FUTURES_MODEL_FEATURE_COLUMNS: Final[tuple[str, ...]] = (
    *FUTURES_MODEL_FEATURE_COLUMNS,
    "cash_stock_underlying_panel_index",
)
TW_STOCK_CONTEXT_FUTURES_EXECUTION_CHANNELS: Final[tuple[str, ...]] = (
    "holding_log_return",
    "executable",
    "must_liquidate",
    "fee_rate_per_open_notional",
)
TW_STOCK_CONTEXT_FUTURES_INTEGER_EXECUTION_CHANNELS: Final[tuple[str, ...]] = (
    "holding_log_return",
    "executable",
    "must_liquidate",
    "open_notional_twd",
    "ending_notional_twd",
    "fixed_fee_per_contract_per_side_twd",
    "opening_transaction_tax_per_contract_twd",
    "ending_transaction_tax_per_contract_twd",
    "maximum_trade_contracts",
    "integer_exposure_group_index",
    "integer_candidate_tier",
)


def fixed_futures_slot_symbols() -> tuple[str, ...]:
    """Return the immutable ordered all-futures action ABI."""

    return tuple(
        f"TAIFEX_SLOT_{slot:04d}"
        for slot in range(1, TAIFEX_FUTURES_PORTFOLIO_FIXED_SLOT_COUNT + 1)
    )


@dataclass(frozen=True, slots=True)
class TaiwanStockContextFuturesPortfolioDaily:
    """Full-stock-context model sidecar and all-futures executor facts."""

    dates: np.ndarray
    symbols: tuple[str, ...]
    candidate_features: np.ndarray
    candidate_mask: np.ndarray
    holding_log_returns: np.ndarray
    executable_mask: np.ndarray
    must_liquidate_mask: np.ndarray
    can_hold_overnight_mask: np.ndarray
    fee_rate_per_open_notional: np.ndarray
    open_prices: np.ndarray
    close_prices: np.ndarray
    volumes: np.ndarray
    benchmark_log_returns: np.ndarray
    source_path: str
    manifest_path: str
    integer_execution: np.ndarray | None = None
    contract_version: int = TW_STOCK_CONTEXT_FUTURES_PORTFOLIO_CONTRACT_VERSION
    futures_data_contract_version: int = (
        TAIFEX_FUTURES_PORTFOLIO_DATA_CONTRACT_VERSION
    )
    futures_backtest_contract_version: int = (
        TAIFEX_FUTURES_PORTFOLIO_BACKTEST_CONTRACT_VERSION
    )

    def execution_tensor(
        self,
        *,
        must_liquidate_mask: np.ndarray | None = None,
    ) -> np.ndarray:
        """Pack executor-only channels without exposing them as model input."""

        liquidation = (
            self.must_liquidate_mask
            if must_liquidate_mask is None
            else np.asarray(must_liquidate_mask, dtype=bool)
        )
        if liquidation.shape != self.executable_mask.shape:
            raise ValueError("must_liquidate_mask must match the futures [T,S] axis")
        if self.integer_execution is not None:
            execution = np.asarray(self.integer_execution, dtype=np.float32).copy()
            if execution.shape[:2] != liquidation.shape or execution.shape[-1] != len(
                TW_STOCK_CONTEXT_FUTURES_INTEGER_EXECUTION_CHANNELS
            ):
                raise ValueError(
                    "integer all-futures execution tensor has an invalid shape"
                )
            execution[..., 2] = liquidation.astype(np.float32, copy=False)
            return execution
        return np.stack(
            (
                self.holding_log_returns,
                self.executable_mask.astype(np.float32, copy=False),
                liquidation.astype(np.float32, copy=False),
                self.fee_rate_per_open_notional,
            ),
            axis=-1,
        ).astype(np.float32, copy=False)


def _require_dependencies() -> None:
    if pl is None or pq is None:
        raise RuntimeError(
            "tw_stock_context_futures_portfolio requires polars and pyarrow"
        )


def attach_stock_context_futures_portfolio_daily(
    panel: PanelData,
    data_path: str | Path,
    *,
    fee_per_side_twd_by_group: dict[str, float],
    integer_contracts: bool = False,
    integer_fee_per_contract_per_side_twd: float = 40.0,
    max_volume_participation: float = 0.0,
) -> PanelData:
    """Attach prior-session futures tokens and current execution facts.

    ``candidate_features[t]`` is copied exclusively from futures row ``t-1``.
    Its mask additionally requires that the fixed slot still owns the same
    physical contract on ``t``.  Current open/close/volume, realised returns,
    and fill availability are stored only in the executor sidecar.
    """

    _require_dependencies()
    source_path = Path(data_path)
    manifest_path = source_path.parent / "manifest.json"
    if not source_path.exists() or not manifest_path.exists():
        raise FileNotFoundError(
            "TAIFEX futures portfolio dataset/manifest missing: "
            f"{source_path}, {manifest_path}"
        )
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if int(manifest.get("contract_version", -1)) != int(
        TAIFEX_FUTURES_PORTFOLIO_DATA_CONTRACT_VERSION
    ):
        raise ValueError("TAIFEX futures portfolio contract version mismatch")
    if int(manifest.get("feature_contract_version", -1)) != int(
        TAIFEX_FUTURES_PORTFOLIO_FEATURE_CONTRACT_VERSION
    ):
        raise ValueError("TAIFEX futures portfolio feature contract mismatch")
    if int(manifest.get("fixed_model_output_slots", -1)) != int(
        TAIFEX_FUTURES_PORTFOLIO_FIXED_SLOT_COUNT
    ):
        raise ValueError("TAIFEX futures portfolio fixed-slot count mismatch")
    expected_sha = (
        manifest.get("outputs", {})
        .get("continuous_daily", {})
        .get("sha256")
    )
    if expected_sha != _sha256_file(source_path):
        raise ValueError("TAIFEX futures portfolio data SHA-256 differs from manifest")

    dates = np.asarray(panel.dates, dtype="datetime64[D]")
    if dates.ndim != 1 or dates.size == 0 or np.isnat(dates).any():
        raise ValueError("stock panel dates must be a non-empty finite 1-D axis")
    if np.any(dates[1:] <= dates[:-1]):
        raise ValueError("stock panel dates must be strictly increasing")
    source_columns = [
        "date",
        "product",
        "symbol",
        "tenor_rank",
        "open",
        "close",
        "volume",
        "holding_log_return",
        "executable",
        "must_liquidate",
        "can_hold_overnight",
        "same_contract_as_previous_session",
        "contract_multiplier",
        "sinopac_network_fee_group",
        "underlying_symbol",
        *FUTURES_MODEL_FEATURE_COLUMNS,
    ]
    if integer_contracts:
        source_columns.extend(
            [
                "contract",
                "physical_contract",
                "asset_class",
                "previous_volume",
            ]
        )
    table = pq.read_table(
        source_path,
        columns=source_columns,
        filters=[
            ("date", ">=", date.fromisoformat(str(dates.min()))),
            ("date", "<=", date.fromisoformat(str(dates.max()))),
        ],
        memory_map=True,
    )
    frame = pl.from_arrow(table).with_columns(
        pl.col("date").cast(pl.Date),
        pl.col("symbol").cast(pl.String),
    )
    if frame.height == 0:
        raise ValueError("TAIFEX futures portfolio has no rows aligned to stock dates")

    source_dates = frame["date"].to_numpy().astype("datetime64[D]")
    date_indices = np.searchsorted(dates, source_dates)
    aligned = date_indices < dates.size
    aligned &= dates[np.minimum(date_indices, dates.size - 1)] == source_dates
    slot_numbers = (
        frame.select(
            pl.col("symbol")
            .str.extract(r"TAIFEX_SLOT_(\d+)$", 1)
            .cast(pl.Int32)
            .alias("slot")
        )["slot"]
        .to_numpy()
    )
    if np.any((slot_numbers < 1) | (slot_numbers > TAIFEX_FUTURES_PORTFOLIO_FIXED_SLOT_COUNT)):
        raise ValueError("TAIFEX source contains a symbol outside the fixed-slot ABI")
    symbol_indices = slot_numbers.astype(np.int64, copy=False) - 1
    date_indices = date_indices[aligned].astype(np.int64, copy=False)
    symbol_indices = symbol_indices[aligned]
    frame = frame.filter(pl.Series("aligned", aligned))
    if frame.height == 0:
        raise ValueError("TAIFEX futures rows do not intersect the stock date axis")

    flat_keys = date_indices * TAIFEX_FUTURES_PORTFOLIO_FIXED_SLOT_COUNT + symbol_indices
    if np.unique(flat_keys).size != flat_keys.size:
        raise ValueError("TAIFEX source contains duplicate date/fixed-slot rows")

    shape = (dates.size, TAIFEX_FUTURES_PORTFOLIO_FIXED_SLOT_COUNT)
    feature_shape = (
        *shape,
        len(TW_STOCK_CONTEXT_FUTURES_MODEL_FEATURE_COLUMNS),
    )
    candidate_features = np.zeros(feature_shape, dtype=np.float32)
    candidate_features[..., -1] = -1.0
    candidate_source = np.zeros(shape, dtype=bool)
    same_as_previous = np.zeros(shape, dtype=bool)
    returns = np.full(shape, np.nan, dtype=np.float32)
    executable = np.zeros(shape, dtype=bool)
    must_liquidate = np.zeros(shape, dtype=bool)
    can_hold = np.zeros(shape, dtype=bool)
    fee_rates = np.full(shape, np.nan, dtype=np.float32)
    opens = np.full(shape, np.nan, dtype=np.float32)
    closes = np.full(shape, np.nan, dtype=np.float32)
    volumes = np.full(shape, np.nan, dtype=np.float32)
    integer_execution: np.ndarray | None = None

    raw_features = frame.select(FUTURES_MODEL_FEATURE_COLUMNS).to_numpy().astype(
        np.float32, copy=False
    )
    raw_features = np.nan_to_num(
        raw_features,
        nan=0.0,
        posinf=0.0,
        neginf=0.0,
        copy=False,
    )
    next_date_indices = date_indices + 1
    has_next_stock_session = next_date_indices < dates.size
    candidate_features[
        next_date_indices[has_next_stock_session],
        symbol_indices[has_next_stock_session],
        : len(FUTURES_MODEL_FEATURE_COLUMNS),
    ] = raw_features[has_next_stock_session]
    panel_symbol_index = {
        str(symbol).strip(): index for index, symbol in enumerate(panel.symbols)
    }
    underlying_values = frame["underlying_symbol"].to_list()
    underlying_panel_indices = np.asarray(
        [
            panel_symbol_index.get(str(value).strip(), -1)
            if value is not None
            else -1
            for value in underlying_values
        ],
        dtype=np.int64,
    )
    candidate_features[
        next_date_indices[has_next_stock_session],
        symbol_indices[has_next_stock_session],
        -1,
    ] = underlying_panel_indices[has_next_stock_session].astype(
        np.float32,
        copy=False,
    )
    candidate_source[
        next_date_indices[has_next_stock_session],
        symbol_indices[has_next_stock_session],
    ] = True

    same_values = frame["same_contract_as_previous_session"].to_numpy().astype(bool)
    same_as_previous[date_indices, symbol_indices] = same_values
    executable[date_indices, symbol_indices] = frame["executable"].to_numpy().astype(bool)
    must_liquidate[date_indices, symbol_indices] = frame[
        "must_liquidate"
    ].to_numpy().astype(bool)
    can_hold[date_indices, symbol_indices] = frame[
        "can_hold_overnight"
    ].to_numpy().astype(bool)
    returns[date_indices, symbol_indices] = frame[
        "holding_log_return"
    ].to_numpy().astype(np.float32)
    opens[date_indices, symbol_indices] = frame["open"].to_numpy().astype(np.float32)
    closes[date_indices, symbol_indices] = frame["close"].to_numpy().astype(np.float32)
    volumes[date_indices, symbol_indices] = frame["volume"].to_numpy().astype(np.float32)

    multipliers = frame["contract_multiplier"].to_numpy().astype(np.float64)
    opening_values = frame["open"].to_numpy().astype(np.float64)
    fee_groups = frame["sinopac_network_fee_group"].to_numpy()
    fees = np.full(frame.height, np.nan, dtype=np.float64)
    for group, raw_fee in fee_per_side_twd_by_group.items():
        fee = float(raw_fee)
        if not np.isfinite(fee) or fee < 0.0:
            raise ValueError(f"fee for group={group!r} must be finite and non-negative")
        fees[fee_groups == str(group)] = fee
    if np.any(~np.isfinite(fees)):
        missing_groups = sorted({str(value) for value in fee_groups[~np.isfinite(fees)]})
        raise ValueError("missing fixed fee groups: " + ", ".join(missing_groups))
    valid_notional = (
        np.isfinite(multipliers)
        & (multipliers > 0.0)
        & np.isfinite(opening_values)
        & (opening_values > 0.0)
    )
    if not np.all(valid_notional):
        raise ValueError("active TAIFEX rows require finite positive open and multiplier")
    fee_rates[date_indices, symbol_indices] = (
        fees / (opening_values * multipliers)
    ).astype(np.float32)

    if integer_contracts:
        integer_fee = float(integer_fee_per_contract_per_side_twd)
        participation = float(max_volume_participation)
        if not np.isfinite(integer_fee) or integer_fee < 0.0:
            raise ValueError(
                "integer_fee_per_contract_per_side_twd must be finite and "
                "non-negative"
            )
        if not np.isfinite(participation) or not (0.0 < participation <= 1.0):
            raise ValueError(
                "integer all-futures max_volume_participation must be in (0,1]"
            )

        # Standard and mini stock/ETF futures for the same underlying and
        # delivery month form one sizing group.  Duplicate product-code roots
        # with the same multiplier remain independent so a transition cannot
        # manufacture a second denomination.  Other futures remain singleton
        # groups while still participating in the common integer ledger.
        group_columns = ["date", "asset_class", "underlying_symbol", "contract"]
        pair_stats = (
            frame.filter(
                pl.col("asset_class").is_in(["stock_future", "etf_future"])
                & pl.col("underlying_symbol").is_not_null()
            )
            .group_by(group_columns)
            .agg(
                pl.len().alias("_group_rows"),
                pl.col("contract_multiplier").n_unique().alias("_group_multipliers"),
            )
        )
        integer_frame = frame.join(
            pair_stats,
            on=group_columns,
            how="left",
            validate="m:1",
        ).with_columns(
            pl.when(
                (pl.col("_group_rows") == 2)
                & (pl.col("_group_multipliers") == 2)
            )
            .then(
                pl.concat_str(
                    "asset_class",
                    "underlying_symbol",
                    "contract",
                    separator=":",
                )
            )
            .otherwise(pl.col("symbol"))
            .alias("_integer_group_key")
        ).with_columns(
            (
                pl.col("_integer_group_key")
                .rank(method="dense")
                .over("date")
                - 1
            )
            .cast(pl.Int32)
            .alias("_integer_group_index"),
            (
                pl.col("contract_multiplier")
                .rank(method="ordinal", descending=True)
                .over("date", "_integer_group_key")
                - 1
            )
            .cast(pl.Int8)
            .alias("_integer_candidate_tier"),
        )
        group_indices = integer_frame["_integer_group_index"].to_numpy().astype(
            np.int64, copy=False
        )
        candidate_tiers = integer_frame["_integer_candidate_tier"].to_numpy().astype(
            np.int64, copy=False
        )
        if np.any((group_indices < 0) | (group_indices >= shape[1])):
            raise ValueError("integer exposure group index exceeds fixed-slot ABI")
        if np.any((candidate_tiers < 0) | (candidate_tiers > 1)):
            raise ValueError(
                "integer exposure groups support at most standard+mini candidates"
            )

        holding_values = integer_frame["holding_log_return"].to_numpy().astype(
            np.float64
        )
        open_notionals = opening_values * multipliers
        ending_notionals = open_notionals * np.exp(holding_values)
        unique_dates = integer_frame.select("date").unique().sort("date")
        tax_schedule = {
            value: stock_index_futures_tax_rate(value)
            for value in unique_dates["date"].to_list()
        }
        tax_rates = np.asarray(
            [tax_schedule[value] for value in integer_frame["date"].to_list()],
            dtype=np.float64,
        )
        opening_tax = np.floor(open_notionals * tax_rates + 0.5)
        ending_tax = np.floor(ending_notionals * tax_rates + 0.5)
        prior_volume = np.nan_to_num(
            integer_frame["previous_volume"].to_numpy().astype(np.float64),
            nan=0.0,
            posinf=0.0,
            neginf=0.0,
        )
        maximum_trade_contracts = np.floor(
            np.clip(prior_volume, 0.0, None) * participation
        )
        integer_execution = np.full(
            (*shape, len(TW_STOCK_CONTEXT_FUTURES_INTEGER_EXECUTION_CHANNELS)),
            np.nan,
            dtype=np.float32,
        )
        integer_execution[..., 1] = 0.0
        integer_execution[..., 2] = 0.0
        # Inactive fixed slots receive a unique singleton group so they cannot
        # collide with any active group if a corrupted non-zero action appears.
        integer_execution[..., 9] = np.arange(shape[1], dtype=np.float32)[None, :]
        integer_execution[..., 10] = 0.0
        packed = np.column_stack(
            (
                holding_values,
                integer_frame["executable"].to_numpy().astype(np.float32),
                integer_frame["must_liquidate"].to_numpy().astype(np.float32),
                open_notionals,
                ending_notionals,
                np.full(integer_frame.height, integer_fee, dtype=np.float64),
                opening_tax,
                ending_tax,
                maximum_trade_contracts,
                group_indices,
                candidate_tiers,
            )
        ).astype(np.float32, copy=False)
        integer_execution[date_indices, symbol_indices] = packed

        active_integer = (
            integer_frame["executable"].to_numpy().astype(bool)
            | integer_frame["must_liquidate"].to_numpy().astype(bool)
            | integer_frame["can_hold_overnight"].to_numpy().astype(bool)
        )
        if np.any(
            active_integer
            & (
                ~np.isfinite(open_notionals)
                | (open_notionals <= 0.0)
                | ~np.isfinite(ending_notionals)
                | (ending_notionals <= 0.0)
                | ~np.isfinite(opening_tax)
                | ~np.isfinite(ending_tax)
            )
        ):
            raise ValueError(
                "active integer futures rows require finite positive notionals "
                "and finite tax"
            )

    candidate_mask = candidate_source & same_as_previous
    if bool((candidate_mask[0]).any()):
        raise RuntimeError("first stock date cannot have prior-session futures context")
    if bool((can_hold & must_liquidate).any()):
        raise ValueError("can_hold_overnight and must_liquidate overlap")
    active = executable | must_liquidate | can_hold
    if bool((active & ~np.isfinite(returns)).any()):
        raise ValueError("active TAIFEX rows require finite holding returns")
    if bool((active & (~np.isfinite(fee_rates) | (fee_rates < 0.0))).any()):
        raise ValueError("active TAIFEX rows require finite non-negative fee rates")

    benchmark = np.zeros(dates.size, dtype=np.float32)
    benchmark_assigned = np.zeros(dates.size, dtype=bool)
    products = frame["product"].to_numpy()
    tenors = frame["tenor_rank"].to_numpy().astype(np.int64)
    benchmark_rows = (products == "TX") & (tenors == 1)
    benchmark_dates = date_indices[benchmark_rows]
    if np.unique(benchmark_dates).size != benchmark_dates.size:
        raise ValueError("TAIFEX TX front-month benchmark is not unique by date")
    benchmark_values = frame["holding_log_return"].to_numpy()[benchmark_rows]
    benchmark[benchmark_dates] = np.nan_to_num(
        benchmark_values.astype(np.float32), nan=0.0, posinf=0.0, neginf=0.0
    )
    benchmark_assigned[benchmark_dates] = True
    if not bool(benchmark_assigned.any()):
        raise ValueError("TAIFEX TX front-month benchmark rows are missing")

    panel.stock_context_futures_portfolio_daily = (
        TaiwanStockContextFuturesPortfolioDaily(
            dates=dates,
            symbols=fixed_futures_slot_symbols(),
            candidate_features=candidate_features,
            candidate_mask=candidate_mask,
            holding_log_returns=returns,
            executable_mask=executable,
            must_liquidate_mask=must_liquidate,
            can_hold_overnight_mask=can_hold,
            fee_rate_per_open_notional=fee_rates,
            open_prices=opens,
            close_prices=closes,
            volumes=volumes,
            benchmark_log_returns=benchmark,
            integer_execution=integer_execution,
            source_path=str(source_path),
            manifest_path=str(manifest_path),
        )
    )
    return panel


__all__ = [
    "TW_STOCK_CONTEXT_FUTURES_EXECUTION_CHANNELS",
    "TW_STOCK_CONTEXT_FUTURES_INTEGER_EXECUTION_CHANNELS",
    "TW_STOCK_CONTEXT_FUTURES_MODEL_FEATURE_COLUMNS",
    "TW_STOCK_CONTEXT_FUTURES_PORTFOLIO_CONTRACT_VERSION",
    "TaiwanStockContextFuturesPortfolioDaily",
    "attach_stock_context_futures_portfolio_daily",
    "fixed_futures_slot_symbols",
]
