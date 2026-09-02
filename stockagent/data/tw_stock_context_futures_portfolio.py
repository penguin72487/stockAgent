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


TW_STOCK_CONTEXT_FUTURES_PORTFOLIO_LEGACY_CONTRACT_VERSION: Final[int] = 2
TW_STOCK_CONTEXT_FUTURES_PORTFOLIO_CONTRACT_VERSION: Final[int] = 3
TW_STOCK_CONTEXT_FUTURES_PORTFOLIO_CURRENT_OPEN_CONTRACT_VERSION: Final[int] = 4
TW_STOCK_CONTEXT_FUTURES_PORTFOLIO_GUARDED_CURRENT_OPEN_CONTRACT_VERSION: Final[
    int
] = 5
TW_STOCK_CONTEXT_FUTURES_PORTFOLIO_EXPIRY_SETTLEMENT_CONTRACT_VERSION: Final[
    int
] = 6
TAIFEX_FUTURES_FINAL_SETTLEMENT_SCHEMA_VERSION: Final[int] = 1
TW_STOCK_CONTEXT_FUTURES_PRIOR_MARKET_FEATURE_COLUMNS: Final[tuple[str, ...]] = (
    *FUTURES_MODEL_FEATURE_COLUMNS,
    "cash_stock_underlying_panel_index",
)
TW_STOCK_CONTEXT_FUTURES_DENOMINATION_FEATURE_COLUMNS: Final[tuple[str, ...]] = (
    "current_integer_exposure_group_index",
    "current_integer_candidate_tier",
    "current_one_contract_cash_requirement_twd",
)
TW_STOCK_CONTEXT_FUTURES_MODEL_FEATURE_COLUMNS: Final[tuple[str, ...]] = (
    *TW_STOCK_CONTEXT_FUTURES_PRIOR_MARKET_FEATURE_COLUMNS,
    *TW_STOCK_CONTEXT_FUTURES_DENOMINATION_FEATURE_COLUMNS,
)
TW_STOCK_CONTEXT_FUTURES_CURRENT_OPEN_FEATURE_COLUMNS: Final[tuple[str, ...]] = (
    "current_futures_open_gap_logret",
)
TW_STOCK_CONTEXT_FUTURES_CURRENT_OPEN_MODEL_FEATURE_COLUMNS: Final[
    tuple[str, ...]
] = (
    *TW_STOCK_CONTEXT_FUTURES_MODEL_FEATURE_COLUMNS,
    *TW_STOCK_CONTEXT_FUTURES_CURRENT_OPEN_FEATURE_COLUMNS,
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
    carry_valuation_quarantine_mask: np.ndarray | None = None
    expiry_settlement_quarantine_mask: np.ndarray | None = None
    expiry_settlement_quarantined_physical_contracts: int = 0
    expiry_settlement_valuation: bool = False
    expiry_final_settlement_path: str | None = None
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
    current_open_feature: bool = False,
    carry_valuation_max_abs_simple_return: float = 0.0,
    expiry_settlement_valuation: bool = False,
    final_settlement_path: str | Path | None = None,
    integer_fee_per_contract_per_side_twd: float = 40.0,
    max_volume_participation: float = 0.0,
) -> PanelData:
    """Attach prior-session futures tokens and current execution facts.

    Market/policy features in ``candidate_features[t]`` are copied exclusively
    from futures row ``t-1`` unless ``current_open_feature`` explicitly opts
    into the session-t TAIFEX OPEN gap.  That research-only channel treats the
    observed 08:45 daily OPEN as both information and entry-price proxy, so it
    must never be described as a causal live fill.  The mask additionally
    requires that the fixed slot still owns the same physical contract on
    ``t``.  In integer mode the three denomination channels contain group
    identity, standard/mini tier, and one fully collateralized contract's cash
    need. Current close/return/volume remain executor-only.  With
    ``expiry_settlement_valuation``, only a contract's own ``last_trade_date``
    row replaces the ordinary close-valued terminal return with a separately
    receipted official TAIFEX *final* settlement price; the daily settlement
    column is deliberately not accepted as a substitute. Non-expiry exits are
    unchanged.
    """

    _require_dependencies()
    if current_open_feature and not integer_contracts:
        raise ValueError(
            "current futures OPEN context requires integer contract metadata"
        )
    carry_guard = float(carry_valuation_max_abs_simple_return)
    if not np.isfinite(carry_guard) or not (0.0 <= carry_guard < 1.0):
        raise ValueError(
            "carry_valuation_max_abs_simple_return must be finite in [0,1)"
        )
    if carry_guard > 0.0 and (not integer_contracts or not current_open_feature):
        raise ValueError(
            "carry valuation quarantine currently requires the 08:45 current-OPEN "
            "integer contract"
        )
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
    if current_open_feature:
        source_columns.append("previous_settlement")
    if expiry_settlement_valuation:
        source_columns.append("liquidation_reason")
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
    frame = frame.with_row_index("_aligned_source_row")

    resolved_final_settlement_path: Path | None = None
    if expiry_settlement_valuation:
        if final_settlement_path is None or not str(final_settlement_path).strip():
            raise ValueError(
                "expiry settlement valuation requires a receipt-backed official "
                "TAIFEX final settlement path"
            )
        resolved_final_settlement_path = Path(final_settlement_path)
        settlement_manifest_path = resolved_final_settlement_path.parent / "manifest.json"
        if not resolved_final_settlement_path.is_file() or not settlement_manifest_path.is_file():
            raise FileNotFoundError(
                "official TAIFEX final settlement dataset/manifest missing: "
                f"{resolved_final_settlement_path}, {settlement_manifest_path}"
            )
        settlement_manifest = json.loads(
            settlement_manifest_path.read_text(encoding="utf-8")
        )
        if int(settlement_manifest.get("schema_version", -1)) != int(
            TAIFEX_FUTURES_FINAL_SETTLEMENT_SCHEMA_VERSION
        ):
            raise ValueError("official TAIFEX final settlement schema mismatch")
        settlement_expected_sha = (
            settlement_manifest.get("outputs", {})
            .get("futures_final_settlement_history", {})
            .get("sha256")
        )
        if settlement_expected_sha != _sha256_file(resolved_final_settlement_path):
            raise ValueError(
                "official TAIFEX final settlement SHA-256 differs from manifest"
            )
        settlement_table = pq.read_table(
            resolved_final_settlement_path,
            columns=[
                "settlement_date",
                "product",
                "contract",
                "final_settlement_price",
            ],
            filters=[
                ("settlement_date", ">=", date.fromisoformat(str(dates.min()))),
                ("settlement_date", "<=", date.fromisoformat(str(dates.max()))),
            ],
            memory_map=True,
        )
        settlements = pl.from_arrow(settlement_table).with_columns(
            pl.col("settlement_date").cast(pl.Date),
            pl.col("product").cast(pl.String).str.strip_chars().str.to_uppercase(),
            pl.col("contract").cast(pl.String).str.strip_chars().str.to_uppercase(),
            pl.col("final_settlement_price").cast(pl.Float64),
        )
        duplicate_settlements = (
            settlements.group_by("settlement_date", "product", "contract")
            .agg(
                pl.len().alias("rows"),
                pl.col("final_settlement_price").n_unique().alias("prices"),
            )
            .filter((pl.col("rows") != 1) | (pl.col("prices") != 1))
        )
        if duplicate_settlements.height:
            raise ValueError(
                "official TAIFEX final settlement keys must be unique"
            )
        frame = frame.join(
            settlements.rename(
                {
                    "settlement_date": "date",
                    "final_settlement_price": "_official_final_settlement_price",
                }
            ),
            on=["date", "product", "contract"],
            how="left",
            validate="m:1",
        ).sort("_aligned_source_row")

    # A stock/ETF future physical contract whose adjacent-OPEN valuation jumps
    # outside the configured integrity envelope cannot be reconstructed from
    # this archive with trustworthy contract-unit economics. Quarantine the
    # complete physical contract, not only the discontinuity row: clipping the
    # label would invent P&L and dropping only the bad day would allow a model
    # to retain an unpriceable position. This is a data-integrity rule, not a
    # return-selection or portfolio-ranking heuristic.
    source_expiry_quarantine = np.zeros(frame.height, dtype=bool)
    incomplete_contracts: set[str] = set()
    if expiry_settlement_valuation:
        source_liquidation_reasons = np.asarray(
            [str(value or "").strip() for value in frame["liquidation_reason"].to_list()],
            dtype=object,
        )
        source_final_settlements = frame[
            "_official_final_settlement_price"
        ].to_numpy().astype(np.float64)
        source_expiry_rows = source_liquidation_reasons == "last_trade_date"
        missing_final = source_expiry_rows & (
            ~np.isfinite(source_final_settlements)
            | (source_final_settlements <= 0.0)
        )
        source_physical_contracts = np.asarray(
            [str(value or "").strip() for value in frame["physical_contract"].to_list()],
            dtype=object,
        )
        if np.any(missing_final & (source_physical_contracts == "")):
            raise ValueError(
                "missing official final settlement rows require physical_contract"
            )
        incomplete_contracts = set(
            source_physical_contracts[missing_final].tolist()
        )
        if incomplete_contracts:
            # An early archive termination is not an expiry fill.  Prevent the
            # policy from ever opening that physical contract instead of
            # inventing a terminal price or reallocating its desired cash.
            # ``np.isin`` on a multi-million-row object array and thousands
            # of strings degenerates into an expensive comparison workload.
            # Polars builds a hash membership set and preserves the same
            # row-aligned boolean result.
            source_expiry_quarantine = (
                frame["physical_contract"]
                .fill_null("")
                .is_in(sorted(incomplete_contracts))
                .to_numpy()
            )

    source_carry_quarantine = source_expiry_quarantine.copy()
    if carry_guard > 0.0:
        asset_values = np.asarray(frame["asset_class"].to_list(), dtype=object)
        physical_values = np.asarray(
            [str(value or "").strip() for value in frame["physical_contract"].to_list()],
            dtype=object,
        )
        source_holding = frame["holding_log_return"].to_numpy().astype(np.float64)
        source_simple_return = np.expm1(source_holding)
        relevant = np.isin(asset_values, ["stock_future", "etf_future"])
        suspicious = relevant & (
            ~np.isfinite(source_simple_return)
            | (np.abs(source_simple_return) > carry_guard)
        )
        if np.any(suspicious & (physical_values == "")):
            raise ValueError(
                "suspicious stock/ETF futures carry rows require physical_contract"
            )
        bad_physical_contracts = set(physical_values[suspicious].tolist())
        if bad_physical_contracts:
            source_carry_quarantine |= relevant & (
                frame["physical_contract"]
                .fill_null("")
                .is_in(sorted(bad_physical_contracts))
                .to_numpy()
            )

    flat_keys = date_indices * TAIFEX_FUTURES_PORTFOLIO_FIXED_SLOT_COUNT + symbol_indices
    if np.unique(flat_keys).size != flat_keys.size:
        raise ValueError("TAIFEX source contains duplicate date/fixed-slot rows")

    shape = (dates.size, TAIFEX_FUTURES_PORTFOLIO_FIXED_SLOT_COUNT)
    model_feature_columns = (
        TW_STOCK_CONTEXT_FUTURES_CURRENT_OPEN_MODEL_FEATURE_COLUMNS
        if current_open_feature
        else TW_STOCK_CONTEXT_FUTURES_MODEL_FEATURE_COLUMNS
        if integer_contracts
        else TW_STOCK_CONTEXT_FUTURES_PRIOR_MARKET_FEATURE_COLUMNS
    )
    feature_shape = (*shape, len(model_feature_columns))
    candidate_features = np.zeros(feature_shape, dtype=np.float32)
    prior_feature_count = len(TW_STOCK_CONTEXT_FUTURES_PRIOR_MARKET_FEATURE_COLUMNS)
    underlying_feature_index = len(FUTURES_MODEL_FEATURE_COLUMNS)
    denomination_feature_start = prior_feature_count
    current_open_feature_index = len(TW_STOCK_CONTEXT_FUTURES_MODEL_FEATURE_COLUMNS)
    candidate_features[..., underlying_feature_index] = -1.0
    if integer_contracts:
        candidate_features[..., denomination_feature_start] = np.arange(
            shape[1], dtype=np.float32
        )[None, :]
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
    current_open_available = np.ones(shape, dtype=bool)
    carry_valuation_quarantine = np.zeros(shape, dtype=bool)
    expiry_settlement_quarantine = np.zeros(shape, dtype=bool)

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
        underlying_feature_index,
    ] = underlying_panel_indices[has_next_stock_session].astype(
        np.float32,
        copy=False,
    )
    candidate_source[
        next_date_indices[has_next_stock_session],
        symbol_indices[has_next_stock_session],
    ] = True

    holding_values = frame["holding_log_return"].to_numpy().astype(np.float64)
    if expiry_settlement_valuation:
        liquidation_reasons = np.asarray(
            [str(value or "").strip() for value in frame["liquidation_reason"].to_list()],
            dtype=object,
        )
        expiry_rows = liquidation_reasons == "last_trade_date"
        settlement_values = frame[
            "_official_final_settlement_price"
        ].to_numpy().astype(np.float64)
        expiry_opens = frame["open"].to_numpy().astype(np.float64)
        usable_expiry = expiry_rows & ~source_expiry_quarantine
        invalid_expiry_open = usable_expiry & (
            ~np.isfinite(expiry_opens) | (expiry_opens <= 0.0)
        )
        if np.any(invalid_expiry_open):
            raise ValueError(
                "expiry settlement valuation requires a finite positive OPEN "
                "on every last_trade_date row"
            )
        holding_values[usable_expiry] = np.log(
            settlement_values[usable_expiry] / expiry_opens[usable_expiry]
        )
    # Keep the effective return attached to each source row.  The denomination
    # join below is not allowed to make integer P&L depend on Polars' join row
    # ordering, especially when standard and mini contracts share a group.
    frame = frame.with_columns(
        pl.Series(
            "_effective_holding_log_return",
            holding_values,
            dtype=pl.Float64,
        )
    )

    same_values = frame["same_contract_as_previous_session"].to_numpy().astype(bool)
    same_as_previous[date_indices, symbol_indices] = same_values
    executable[date_indices, symbol_indices] = frame["executable"].to_numpy().astype(bool)
    must_liquidate[date_indices, symbol_indices] = frame[
        "must_liquidate"
    ].to_numpy().astype(bool)
    can_hold[date_indices, symbol_indices] = frame[
        "can_hold_overnight"
    ].to_numpy().astype(bool)
    returns[date_indices, symbol_indices] = holding_values.astype(np.float32)
    opens[date_indices, symbol_indices] = frame["open"].to_numpy().astype(np.float32)
    closes[date_indices, symbol_indices] = frame["close"].to_numpy().astype(np.float32)
    volumes[date_indices, symbol_indices] = frame["volume"].to_numpy().astype(np.float32)

    multipliers = frame["contract_multiplier"].to_numpy().astype(np.float64)
    opening_values = frame["open"].to_numpy().astype(np.float64)
    if current_open_feature:
        previous_settlement = frame["previous_settlement"].to_numpy().astype(
            np.float64
        )
        valid_current_open = (
            np.isfinite(opening_values)
            & (opening_values > 0.0)
            & np.isfinite(previous_settlement)
            & (previous_settlement > 0.0)
        )
        current_open_gap = np.zeros(frame.height, dtype=np.float64)
        current_open_gap[valid_current_open] = np.log(
            opening_values[valid_current_open]
            / previous_settlement[valid_current_open]
        )
        valid_current_open &= np.isfinite(current_open_gap)
        candidate_features[
            date_indices,
            symbol_indices,
            current_open_feature_index,
        ] = np.nan_to_num(
            current_open_gap,
            nan=0.0,
            posinf=0.0,
            neginf=0.0,
        ).astype(np.float32, copy=False)
        current_open_available = np.zeros(shape, dtype=bool)
        current_open_available[date_indices, symbol_indices] = valid_current_open
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

        integer_holding_values = integer_frame[
            "_effective_holding_log_return"
        ].to_numpy().astype(np.float64, copy=False)
        open_notionals = opening_values * multipliers
        ending_notionals = open_notionals * np.exp(integer_holding_values)
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
        # Full notional plus both entry and eventual exit fee/tax cash is
        # reserved identically for long and short contracts.  In the optional
        # 08:45 research contract, current OPEN-derived sizing is the same-print
        # information/fill proxy and is intentionally checkpoint-distinct.
        one_contract_cash = (
            open_notionals + (2.0 * integer_fee) + (2.0 * opening_tax)
        )
        candidate_features[
            date_indices,
            symbol_indices,
            denomination_feature_start,
        ] = group_indices.astype(np.float32, copy=False)
        candidate_features[
            date_indices,
            symbol_indices,
            denomination_feature_start + 1,
        ] = candidate_tiers.astype(np.float32, copy=False)
        candidate_features[
            date_indices,
            symbol_indices,
            denomination_feature_start + 2,
        ] = one_contract_cash.astype(np.float32, copy=False)
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
                integer_holding_values,
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

    if bool(source_carry_quarantine.any()):
        execution_quarantine = np.zeros(shape, dtype=bool)
        execution_quarantine[date_indices, symbol_indices] = source_carry_quarantine
        prior_context_quarantine = np.zeros(shape, dtype=bool)
        quarantined_has_next = has_next_stock_session & source_carry_quarantine
        prior_context_quarantine[
            next_date_indices[quarantined_has_next],
            symbol_indices[quarantined_has_next],
        ] = True
        carry_valuation_quarantine = (
            execution_quarantine | prior_context_quarantine
        )
        source_expiry_execution_quarantine = np.zeros(shape, dtype=bool)
        source_expiry_execution_quarantine[
            date_indices, symbol_indices
        ] = source_expiry_quarantine
        source_expiry_prior_quarantine = np.zeros(shape, dtype=bool)
        expiry_has_next = has_next_stock_session & source_expiry_quarantine
        source_expiry_prior_quarantine[
            next_date_indices[expiry_has_next],
            symbol_indices[expiry_has_next],
        ] = True
        expiry_settlement_quarantine = (
            source_expiry_execution_quarantine
            | source_expiry_prior_quarantine
        )
        candidate_source[prior_context_quarantine] = False
        same_as_previous[execution_quarantine] = False
        current_open_available[execution_quarantine] = False
        executable[execution_quarantine] = False
        must_liquidate[execution_quarantine] = False
        can_hold[execution_quarantine] = False
        returns[execution_quarantine] = np.nan
        fee_rates[execution_quarantine] = np.nan
        if integer_execution is not None:
            integer_execution[execution_quarantine, :9] = np.nan
            integer_execution[execution_quarantine, 1] = 0.0
            integer_execution[execution_quarantine, 2] = 0.0
            integer_execution[execution_quarantine, 8] = 0.0

    candidate_mask = candidate_source & same_as_previous & current_open_available
    candidate_mask &= ~carry_valuation_quarantine
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
    benchmark_values = holding_values[benchmark_rows]
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
            carry_valuation_quarantine_mask=carry_valuation_quarantine,
            expiry_settlement_quarantine_mask=expiry_settlement_quarantine,
            expiry_settlement_quarantined_physical_contracts=len(
                incomplete_contracts
            ),
            expiry_settlement_valuation=bool(expiry_settlement_valuation),
            expiry_final_settlement_path=(
                str(resolved_final_settlement_path)
                if resolved_final_settlement_path is not None
                else None
            ),
            contract_version=(
                TW_STOCK_CONTEXT_FUTURES_PORTFOLIO_EXPIRY_SETTLEMENT_CONTRACT_VERSION
                if expiry_settlement_valuation
                else TW_STOCK_CONTEXT_FUTURES_PORTFOLIO_GUARDED_CURRENT_OPEN_CONTRACT_VERSION
                if carry_guard > 0.0
                else TW_STOCK_CONTEXT_FUTURES_PORTFOLIO_CURRENT_OPEN_CONTRACT_VERSION
                if current_open_feature
                else TW_STOCK_CONTEXT_FUTURES_PORTFOLIO_CONTRACT_VERSION
                if integer_contracts
                else TW_STOCK_CONTEXT_FUTURES_PORTFOLIO_LEGACY_CONTRACT_VERSION
            ),
            source_path=str(source_path),
            manifest_path=str(manifest_path),
        )
    )
    return panel


__all__ = [
    "TW_STOCK_CONTEXT_FUTURES_CURRENT_OPEN_FEATURE_COLUMNS",
    "TW_STOCK_CONTEXT_FUTURES_CURRENT_OPEN_MODEL_FEATURE_COLUMNS",
    "TW_STOCK_CONTEXT_FUTURES_DENOMINATION_FEATURE_COLUMNS",
    "TW_STOCK_CONTEXT_FUTURES_EXECUTION_CHANNELS",
    "TW_STOCK_CONTEXT_FUTURES_INTEGER_EXECUTION_CHANNELS",
    "TW_STOCK_CONTEXT_FUTURES_MODEL_FEATURE_COLUMNS",
    "TW_STOCK_CONTEXT_FUTURES_PRIOR_MARKET_FEATURE_COLUMNS",
    "TW_STOCK_CONTEXT_FUTURES_PORTFOLIO_LEGACY_CONTRACT_VERSION",
    "TW_STOCK_CONTEXT_FUTURES_PORTFOLIO_CONTRACT_VERSION",
    "TW_STOCK_CONTEXT_FUTURES_PORTFOLIO_CURRENT_OPEN_CONTRACT_VERSION",
    "TW_STOCK_CONTEXT_FUTURES_PORTFOLIO_GUARDED_CURRENT_OPEN_CONTRACT_VERSION",
    "TW_STOCK_CONTEXT_FUTURES_PORTFOLIO_EXPIRY_SETTLEMENT_CONTRACT_VERSION",
    "TAIFEX_FUTURES_FINAL_SETTLEMENT_SCHEMA_VERSION",
    "TaiwanStockContextFuturesPortfolioDaily",
    "attach_stock_context_futures_portfolio_daily",
    "fixed_futures_slot_symbols",
]
