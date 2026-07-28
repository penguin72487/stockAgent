"""Taiwan execution-mode primitives shared by backtest and training callers.

This module deliberately contains only static schedule construction.  It does
not maintain cash, positions, or settlement queues; those belong to the
canonical backtest executor.  Settlement offsets are exchange-session offsets,
not calendar-day offsets.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
import math
from numbers import Integral, Real
from typing import Final

import numpy as np

from stockagent.backtest.tw_commission_rebate import (
    normalize_commission_rebate_timing,
)
from stockagent.data.tw_security import classify_tw_stock_or_etf


EXECUTION_MODES: Final[tuple[str, ...]] = (
    "naive",
    "tw_cash",
    "tw_day_trade",
    "tw_overnight",
)
TW_CARRYING_EXECUTION_MODES: Final[tuple[str, ...]] = (
    "tw_cash",
    "tw_overnight",
)
FEE_ROUNDING_MODES: Final[tuple[str, ...]] = ("none", "floor", "half_up")

# These are market-wide statutory floors, not per-security margin schedules.
# End dates are exclusive so each interval explicitly records the first 90%
# restoration date as well as the first raised-rate date.
_TW_SHORT_INITIAL_MARGIN_RATE_WINDOWS: Final[
    tuple[tuple[np.datetime64, np.datetime64, float], ...]
] = (
    (np.datetime64("2015-08-13", "D"), np.datetime64("2015-10-16", "D"), 1.20),
    (np.datetime64("2016-01-08", "D"), np.datetime64("2016-03-01", "D"), 1.20),
    (np.datetime64("2022-10-12", "D"), np.datetime64("2023-02-24", "D"), 1.20),
    (np.datetime64("2025-04-07", "D"), np.datetime64("2025-05-26", "D"), 1.30),
)

_EXECUTION_MODE_ALIASES: Final[dict[str, str]] = {
    # Existing scalar-fee / continuous-weight behavior.
    "naive": "naive",
    "legacy": "naive",
    "legacy_naive": "naive",
    "default": "naive",
    # Taiwan cash-market execution.
    "tw_cash": "tw_cash",
    "taiwan_cash": "tw_cash",
    "cash": "tw_cash",
    "cash_stock": "tw_cash",
    "cash_market": "tw_cash",
    "spot": "tw_cash",
    "tw_spot": "tw_cash",
    "現股": "tw_cash",
    "台股現股": "tw_cash",
    # Taiwan next-session round-trip execution.  Every cohort opened on
    # session t must be closed no later than session t+1 close.
    "tw_overnight": "tw_overnight",
    "tw_overnight_trade": "tw_overnight",
    "taiwan_overnight": "tw_overnight",
    "overnight": "tw_overnight",
    "overnight_trade": "tw_overnight",
    "next_day_trade": "tw_overnight",
    "next_session_trade": "tw_overnight",
    "隔日沖": "tw_overnight",
    "台股隔日沖": "tw_overnight",
    # Taiwan same-day round-trip execution.
    "tw_day_trade": "tw_day_trade",
    "tw_daytrade": "tw_day_trade",
    "taiwan_day_trade": "tw_day_trade",
    "day_trade": "tw_day_trade",
    "daytrade": "tw_day_trade",
    "intraday": "tw_day_trade",
    "tw_intraday": "tw_day_trade",
    "當沖": "tw_day_trade",
    "台股當沖": "tw_day_trade",
}


def normalize_execution_mode(mode: object) -> str:
    """Return a canonical execution mode, rejecting unknown values.

    Only explicit aliases are accepted.  In particular, ``None`` and an empty
    string do not silently select the legacy mode.
    """

    if not isinstance(mode, str):
        raise ValueError(
            "execution_mode must be one of "
            "'naive', 'tw_cash', 'tw_day_trade', or 'tw_overnight'"
        )
    normalized = "_".join(mode.strip().casefold().replace("-", "_").split())
    canonical = _EXECUTION_MODE_ALIASES.get(normalized)
    if canonical is None:
        raise ValueError(
            "execution_mode must be one of "
            "'naive', 'tw_cash', 'tw_day_trade', or 'tw_overnight'"
        )
    return canonical


def require_naive_execution_for_tool(
    execution_mode: object,
    *,
    tool_name: str,
) -> str:
    """Fail closed when a legacy tool is not wired to Taiwan execution state.

    Config-driven diagnostics and microbenchmarks must not silently interpret a
    Taiwan cash or day-trade experiment with the legacy dataset, loss, or
    backtest semantics. Callers should invoke this immediately after loading
    their config and before doing any expensive setup.
    """

    mode = normalize_execution_mode(execution_mode)
    if mode != "naive":
        raise RuntimeError(
            f"{tool_name} supports execution_mode='naive' only; refusing to "
            f"silently run execution_mode={mode!r} with naive execution "
            "semantics. Use the canonical trainer/evaluation path or "
            "scripts/benchmark_tw_execution.py for Taiwan execution modes."
        )
    return mode


def _finite_nonnegative_rate(name: str, value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, Real):
        raise ValueError(f"{name} must be a finite non-negative real number")
    result = float(value)
    if not math.isfinite(result) or result < 0.0:
        raise ValueError(f"{name} must be a finite non-negative real number")
    return result


def _finite_positive_ratio(name: str, value: object, *, minimum: float = 0.0) -> float:
    if isinstance(value, bool) or not isinstance(value, Real):
        raise ValueError(f"{name} must be a finite real number greater than {minimum}")
    result = float(value)
    if not math.isfinite(result) or result <= minimum:
        raise ValueError(f"{name} must be a finite real number greater than {minimum}")
    return result


def _finite_unit_interval_rate(name: str, value: object) -> float:
    result = _finite_nonnegative_rate(name, value)
    if result > 1.0:
        raise ValueError(f"{name} must be between 0 and 1 inclusive")
    return result


def _positive_integer(name: str, value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, Integral):
        raise ValueError(f"{name} must be a positive integer")
    result = int(value)
    if result <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return result


def normalize_fee_rounding(value: object, *, name: str = "fee_rounding") -> str:
    """Normalize an exact-ledger currency rounding rule.

    ``none`` intentionally preserves the proportional floating-point schedule
    used by the differentiable training surrogate.  ``floor`` and ``half_up``
    round each nonzero symbol-side order's fee to a whole New Taiwan dollar in
    the exact integer executor.
    """

    if not isinstance(value, str):
        raise ValueError(f"{name} must be one of {FEE_ROUNDING_MODES}")
    normalized = "_".join(value.strip().casefold().replace("-", "_").split())
    aliases = {
        "none": "none",
        "exact": "none",
        "proportional": "none",
        "floor": "floor",
        "truncate": "floor",
        "truncate_to_twd": "floor",
        "half_up": "half_up",
        "round_half_up": "half_up",
    }
    result = aliases.get(normalized)
    if result is None:
        raise ValueError(f"{name} must be one of {FEE_ROUNDING_MODES}")
    return result


def official_tw_short_initial_margin_rates(dates: object) -> np.ndarray:
    """Return the known market-wide statutory short-margin floor by date.

    The result always is a float64 ``ndarray`` and preserves the input shape,
    including a zero-dimensional result for a scalar ``date``/``datetime`` or
    ``numpy.datetime64``.  The base floor is 90%; the encoded intervals are
    market-wide temporary increases whose end points are the first restoration
    dates.

    This is deliberately *not* the complete security-level rule.  A caller
    must take the elementwise maximum with its point-in-time ``[T, S]`` margin
    rate tensor, because an exchange or broker may require a higher rate for an
    individual security.  Missing security-level data must not be represented
    as if this market-wide scalar proved the security was shortable.
    """

    raw_dates = np.asarray(dates)
    if raw_dates.size == 0:
        return np.empty(raw_dates.shape, dtype=np.float64)
    if raw_dates.dtype.kind in {"b", "i", "u", "f", "c"}:
        raise ValueError("dates must contain date-like values, not numeric offsets")
    if raw_dates.dtype.kind == "O":
        for value in raw_dates.flat:
            if isinstance(value, (bool, Real)):
                raise ValueError("dates must contain date-like values, not numeric offsets")
    try:
        normalized_dates = np.asarray(dates, dtype="datetime64[D]")
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("dates must contain valid date-like values") from exc
    if np.isnat(normalized_dates).any():
        raise ValueError("dates must not contain NaT or missing values")

    rates = np.full(normalized_dates.shape, 0.90, dtype=np.float64)
    for start, end, rate in _TW_SHORT_INITIAL_MARGIN_RATE_WINDOWS:
        raised = (normalized_dates >= start) & (normalized_dates < end)
        rates = np.where(raised, rate, rates)
    return np.asarray(rates, dtype=np.float64)


@dataclass(frozen=True, slots=True)
class TaiwanMarginShortSchedule:
    """Checkpointable broker/account profile for ``tw_cash`` margin shorts.

    Ratios may exceed one: Taiwan has temporarily required 120% or 130%
    initial margin.  The maintenance ratio controls the deterministic Article
    56 collateral-retention rule on partial covers.  Margin-call cure policy
    and time-dependent broker carry are deliberately not exposed here until
    their required recurrent state and point-in-time inputs are modeled.
    """

    initial_margin_rate: float = 0.90
    maintenance_ratio: float = 1.30
    lot_size: int = 1000
    handling_fee_rate: float = 0.0

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "initial_margin_rate",
            _finite_positive_ratio("initial_margin_rate", self.initial_margin_rate),
        )
        object.__setattr__(
            self,
            "maintenance_ratio",
            _finite_positive_ratio(
                "maintenance_ratio",
                self.maintenance_ratio,
                minimum=1.0,
            ),
        )
        object.__setattr__(self, "lot_size", _positive_integer("lot_size", self.lot_size))
        object.__setattr__(
            self,
            "handling_fee_rate",
            _finite_unit_interval_rate("handling_fee_rate", self.handling_fee_rate),
        )

    def marketwide_initial_margin_rates(self, dates: object) -> np.ndarray:
        """Combine the configured floor with known market-wide legal floors.

        Security-level point-in-time rates still must override this result via
        an elementwise maximum in the execution-data layer.
        """

        official = official_tw_short_initial_margin_rates(dates)
        return np.maximum(official, self.initial_margin_rate)


DEFAULT_TAIWAN_MARGIN_SHORT_SCHEDULE: Final[TaiwanMarginShortSchedule] = (
    TaiwanMarginShortSchedule()
)


@dataclass(frozen=True, slots=True)
class TaiwanFeeSchedule:
    """Taiwan fee and lot-size profile used to build per-security schedules.

    ``commission_discount=0.2`` is the ultimate broker-price profile (二折),
    not the commission charged at execution and not a statutory exchange rate.
    The gross commission is charged first; the difference is an earned rebate
    paid according to ``commission_rebate_timing``.  Statutory/product tax
    rates remain separate fields so callers cannot accidentally rebate tax
    along with commission.
    """

    commission_rate: float = 0.001425
    commission_discount: float = 0.2
    commission_rebate_timing: str = "monthly_15th"
    stock_sell_tax: float = 0.003
    etf_sell_tax: float = 0.001
    day_trade_stock_sell_tax: float = 0.0015
    day_trade_etf_sell_tax: float = 0.001
    # Broker/account profile for the exact integer ledger only.  A 20 TWD
    # minimum is common for board-lot orders but is not universal; zero and no
    # rounding therefore remain the neutral defaults requested by the user.
    minimum_commission: float = 0.0
    commission_rounding: str = "none"
    tax_rounding: str = "none"
    settlement_lag_sessions: int = 2
    cash_lot_size: int = 1
    day_trade_default_lot_size: int = 1000

    def __post_init__(self) -> None:
        rate_names = (
            "commission_rate",
            "commission_discount",
            "stock_sell_tax",
            "etf_sell_tax",
            "day_trade_stock_sell_tax",
            "day_trade_etf_sell_tax",
            "minimum_commission",
        )
        for name in rate_names:
            object.__setattr__(self, name, _finite_nonnegative_rate(name, getattr(self, name)))

        if self.commission_discount > 1.0:
            raise ValueError("commission_discount must be between 0 and 1 inclusive")

        object.__setattr__(
            self,
            "commission_rebate_timing",
            normalize_commission_rebate_timing(
                self.commission_rebate_timing,
            ),
        )
        object.__setattr__(
            self,
            "commission_rounding",
            normalize_fee_rounding(
                self.commission_rounding,
                name="commission_rounding",
            ),
        )
        object.__setattr__(
            self,
            "tax_rounding",
            normalize_fee_rounding(self.tax_rounding, name="tax_rounding"),
        )

        for name in (
            "settlement_lag_sessions",
            "cash_lot_size",
            "day_trade_default_lot_size",
        ):
            object.__setattr__(self, name, _positive_integer(name, getattr(self, name)))

    @property
    def effective_commission_rate(self) -> float:
        """Ultimate economic commission rate after the broker rebate."""

        return self.commission_rate * self.commission_discount

    @property
    def commission_rebate_rate(self) -> float:
        """Commission-only rate earned back after the gross charge."""

        return self.commission_rate * (1.0 - self.commission_discount)


DEFAULT_TAIWAN_FEE_SCHEDULE: Final[TaiwanFeeSchedule] = TaiwanFeeSchedule()


def _coerce_fee_schedule(value: TaiwanFeeSchedule | None) -> TaiwanFeeSchedule:
    if value is None:
        return DEFAULT_TAIWAN_FEE_SCHEDULE
    if not isinstance(value, TaiwanFeeSchedule):
        raise ValueError("fee_schedule must be a TaiwanFeeSchedule")
    return value


def _normalized_symbols(symbols: Sequence[object]) -> tuple[str, ...]:
    if isinstance(symbols, (str, bytes)):
        raise ValueError("symbols must be a sequence, not a scalar string")
    try:
        raw_symbols = tuple(symbols)
    except TypeError as exc:
        raise ValueError("symbols must be a finite sequence") from exc
    return tuple(str(symbol or "").strip().upper() for symbol in raw_symbols)


def _normalize_security_type(value: object, *, context: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{context} must be 'stock' or 'etf'")
    normalized = value.strip().casefold()
    if normalized not in {"stock", "etf"}:
        raise ValueError(f"{context} must be 'stock' or 'etf'")
    return normalized


def _security_type_overrides(
    symbols: tuple[str, ...],
    security_types: Mapping[object, object] | Sequence[object] | None,
) -> tuple[str | None, ...]:
    if security_types is None:
        return (None,) * len(symbols)

    if isinstance(security_types, Mapping):
        by_symbol: dict[str, str] = {}
        for raw_symbol, raw_type in security_types.items():
            symbol = str(raw_symbol or "").strip().upper()
            if not symbol:
                raise ValueError("security_types contains an empty symbol")
            security_type = _normalize_security_type(
                raw_type,
                context=f"security_types[{raw_symbol!r}]",
            )
            previous = by_symbol.get(symbol)
            if previous is not None and previous != security_type:
                raise ValueError(f"conflicting security type overrides for symbol {symbol!r}")
            by_symbol[symbol] = security_type
        return tuple(by_symbol.get(symbol) for symbol in symbols)

    if isinstance(security_types, (str, bytes)):
        raise ValueError("security_types must be a mapping or a sequence aligned with symbols")
    try:
        raw_types = tuple(security_types)
    except TypeError as exc:
        raise ValueError(
            "security_types must be a mapping or a sequence aligned with symbols"
        ) from exc
    if len(raw_types) != len(symbols):
        raise ValueError("security_types sequence length must match symbols")
    return tuple(
        _normalize_security_type(value, context=f"security_types[{index}]")
        for index, value in enumerate(raw_types)
    )


def _resolve_security_types(
    symbols: tuple[str, ...],
    security_types: Mapping[object, object] | Sequence[object] | None,
) -> tuple[str, ...]:
    overrides = _security_type_overrides(symbols, security_types)
    resolved: list[str] = []
    for index, (symbol, override) in enumerate(zip(symbols, overrides, strict=True)):
        security_type = override or classify_tw_stock_or_etf(symbol)
        if security_type not in {"stock", "etf"}:
            raise ValueError(
                f"unsupported Taiwan security at symbols[{index}]={symbol!r}; "
                "only stocks and ETFs are supported"
            )
        resolved.append(security_type)
    return tuple(resolved)


def effective_fee_rate_vectors(
    symbols: Sequence[object],
    execution_mode: object,
    *,
    fee_schedule: TaiwanFeeSchedule | None = None,
    security_types: Mapping[object, object] | Sequence[object] | None = None,
    naive_buy_fee_rate: float = 0.0,
    naive_sell_fee_rate: float = 0.0,
) -> tuple[np.ndarray, np.ndarray]:
    """Build effective buy/sell fee-rate vectors in symbol order.

    ``naive`` broadcasts the caller's legacy scalar rates and performs no
    Taiwan product classification.  Taiwan modes charge discounted commission
    on both sides and add the applicable product tax only on sells.
    """

    mode = normalize_execution_mode(execution_mode)
    schedule = _coerce_fee_schedule(fee_schedule)
    normalized_symbols = _normalized_symbols(symbols)
    num_symbols = len(normalized_symbols)

    if mode == "naive":
        if security_types is not None:
            # The override is not used for tax in legacy mode, but an explicitly
            # supplied malformed type must not be silently ignored.
            _security_type_overrides(normalized_symbols, security_types)
        buy_rate = _finite_nonnegative_rate("naive_buy_fee_rate", naive_buy_fee_rate)
        sell_rate = _finite_nonnegative_rate("naive_sell_fee_rate", naive_sell_fee_rate)
        return (
            np.full(num_symbols, buy_rate, dtype=np.float64),
            np.full(num_symbols, sell_rate, dtype=np.float64),
        )

    resolved_types = _resolve_security_types(normalized_symbols, security_types)
    commission = schedule.effective_commission_rate
    buy_fees = np.full(num_symbols, commission, dtype=np.float64)
    sell_fees = np.empty(num_symbols, dtype=np.float64)

    if mode in TW_CARRYING_EXECUTION_MODES:
        stock_tax = schedule.stock_sell_tax
        etf_tax = schedule.etf_sell_tax
    elif mode == "tw_day_trade":
        stock_tax = schedule.day_trade_stock_sell_tax
        etf_tax = schedule.day_trade_etf_sell_tax
    else:  # normalize_execution_mode above makes this an internal contract check.
        raise AssertionError(f"unhandled Taiwan execution mode: {mode}")

    for index, security_type in enumerate(resolved_types):
        sell_fees[index] = commission + (stock_tax if security_type == "stock" else etf_tax)
    return buy_fees, sell_fees


def gross_fee_rate_vectors(
    symbols: Sequence[object],
    execution_mode: object,
    *,
    fee_schedule: TaiwanFeeSchedule | None = None,
    security_types: Mapping[object, object] | Sequence[object] | None = None,
    naive_buy_fee_rate: float = 0.0,
    naive_sell_fee_rate: float = 0.0,
) -> tuple[np.ndarray, np.ndarray]:
    """Build fee vectors charged at execution before any broker rebate.

    Taiwan modes charge the undiscounted commission on both sides and add the
    applicable statutory/product tax only on sells.  The rebate is intentionally
    absent from these vectors so settlement and cash availability cannot use it
    before the configured payment event.  ``naive`` has no deferred-rebate
    contract and therefore preserves its caller-supplied scalar fees.
    """

    mode = normalize_execution_mode(execution_mode)
    schedule = _coerce_fee_schedule(fee_schedule)
    normalized_symbols = _normalized_symbols(symbols)
    num_symbols = len(normalized_symbols)

    if mode == "naive":
        if security_types is not None:
            _security_type_overrides(normalized_symbols, security_types)
        buy_rate = _finite_nonnegative_rate(
            "naive_buy_fee_rate",
            naive_buy_fee_rate,
        )
        sell_rate = _finite_nonnegative_rate(
            "naive_sell_fee_rate",
            naive_sell_fee_rate,
        )
        return (
            np.full(num_symbols, buy_rate, dtype=np.float64),
            np.full(num_symbols, sell_rate, dtype=np.float64),
        )

    resolved_types = _resolve_security_types(normalized_symbols, security_types)
    commission = schedule.commission_rate
    buy_fees = np.full(num_symbols, commission, dtype=np.float64)
    sell_fees = np.empty(num_symbols, dtype=np.float64)

    if mode in TW_CARRYING_EXECUTION_MODES:
        stock_tax = schedule.stock_sell_tax
        etf_tax = schedule.etf_sell_tax
    elif mode == "tw_day_trade":
        stock_tax = schedule.day_trade_stock_sell_tax
        etf_tax = schedule.day_trade_etf_sell_tax
    else:  # normalize_execution_mode above makes this an internal contract check.
        raise AssertionError(f"unhandled Taiwan execution mode: {mode}")

    for index, security_type in enumerate(resolved_types):
        sell_fees[index] = commission + (
            stock_tax if security_type == "stock" else etf_tax
        )
    return buy_fees, sell_fees


def commission_rebate_rate_vector(
    symbols: Sequence[object],
    execution_mode: object,
    *,
    fee_schedule: TaiwanFeeSchedule | None = None,
    security_types: Mapping[object, object] | Sequence[object] | None = None,
) -> np.ndarray:
    """Build the commission-only earned-rebate rate in symbol order.

    The commission rate is identical on buy and sell legs, so one vector is the
    canonical representation.  Taiwan security classification is still
    validated to fail closed on unsupported products.  Legacy ``naive`` mode
    has no deferred-rebate contract and returns zeros.
    """

    mode = normalize_execution_mode(execution_mode)
    schedule = _coerce_fee_schedule(fee_schedule)
    normalized_symbols = _normalized_symbols(symbols)
    if mode == "naive":
        if security_types is not None:
            _security_type_overrides(normalized_symbols, security_types)
        return np.zeros(len(normalized_symbols), dtype=np.float64)

    _resolve_security_types(normalized_symbols, security_types)
    return np.full(
        len(normalized_symbols),
        schedule.commission_rebate_rate,
        dtype=np.float64,
    )


def lot_size_vector(
    symbols: Sequence[object],
    execution_mode: object,
    *,
    fee_schedule: TaiwanFeeSchedule | None = None,
    security_types: Mapping[object, object] | Sequence[object] | None = None,
    per_symbol_lot_sizes: Mapping[object, object] | None = None,
) -> np.ndarray:
    """Build the minimum executable share quantity for every symbol.

    Cash execution uses the cash profile's share unit.  Day trading uses the
    default board-lot profile, with explicit positive-integer per-symbol
    overrides for exceptional instruments.  Legacy ``naive`` returns neutral
    one-share units; its caller may elect not to apply lot rounding at all.
    """

    mode = normalize_execution_mode(execution_mode)
    schedule = _coerce_fee_schedule(fee_schedule)
    normalized_symbols = _normalized_symbols(symbols)

    if mode == "naive":
        if security_types is not None:
            _security_type_overrides(normalized_symbols, security_types)
        if per_symbol_lot_sizes is not None:
            raise ValueError("per_symbol_lot_sizes is supported only for tw_day_trade")
        return np.ones(len(normalized_symbols), dtype=np.int64)

    # Fail closed for warrants, ETNs, rights, and any other unsupported product.
    _resolve_security_types(normalized_symbols, security_types)

    if mode in TW_CARRYING_EXECUTION_MODES:
        if per_symbol_lot_sizes is not None:
            raise ValueError(
                "per_symbol_lot_sizes is supported only for tw_day_trade"
            )
        return np.full(len(normalized_symbols), schedule.cash_lot_size, dtype=np.int64)

    if mode != "tw_day_trade":
        raise AssertionError(f"unhandled Taiwan execution mode: {mode}")

    lot_sizes = np.full(
        len(normalized_symbols),
        schedule.day_trade_default_lot_size,
        dtype=np.int64,
    )
    if per_symbol_lot_sizes is None:
        return lot_sizes
    if not isinstance(per_symbol_lot_sizes, Mapping):
        raise ValueError("per_symbol_lot_sizes must be a mapping")

    overrides: dict[str, int] = {}
    for raw_symbol, raw_lot_size in per_symbol_lot_sizes.items():
        symbol = str(raw_symbol or "").strip().upper()
        if not symbol:
            raise ValueError("per_symbol_lot_sizes contains an empty symbol")
        lot_size = _positive_integer(
            f"per_symbol_lot_sizes[{raw_symbol!r}]",
            raw_lot_size,
        )
        previous = overrides.get(symbol)
        if previous is not None and previous != lot_size:
            raise ValueError(f"conflicting lot-size overrides for symbol {symbol!r}")
        overrides[symbol] = lot_size

    for index, symbol in enumerate(normalized_symbols):
        if symbol in overrides:
            lot_sizes[index] = overrides[symbol]
    return lot_sizes


def settlement_session_indices(num_sessions: int, lag: int = 2) -> np.ndarray:
    """Map each trade-session index to its settlement-session index.

    The returned value for trade session ``i`` is exactly ``i + lag``.  Values
    may therefore extend beyond ``num_sessions - 1``; this preserves pending
    settlements at a finite backtest boundary instead of silently discarding or
    accelerating them.  ``lag`` counts observed exchange sessions, not calendar
    days.
    """

    if isinstance(num_sessions, bool) or not isinstance(num_sessions, Integral):
        raise ValueError("num_sessions must be a non-negative integer")
    normalized_num_sessions = int(num_sessions)
    if normalized_num_sessions < 0:
        raise ValueError("num_sessions must be a non-negative integer")
    normalized_lag = _positive_integer("lag", lag)
    return np.arange(normalized_num_sessions, dtype=np.int64) + normalized_lag


# Readable integration aliases.  Keep one implementation for each primitive.
build_effective_fee_vectors = effective_fee_rate_vectors
build_gross_fee_vectors = gross_fee_rate_vectors
build_commission_rebate_rate_vector = commission_rebate_rate_vector
build_lot_size_vector = lot_size_vector


__all__ = [
    "DEFAULT_TAIWAN_FEE_SCHEDULE",
    "DEFAULT_TAIWAN_MARGIN_SHORT_SCHEDULE",
    "EXECUTION_MODES",
    "TW_CARRYING_EXECUTION_MODES",
    "FEE_ROUNDING_MODES",
    "TaiwanFeeSchedule",
    "TaiwanMarginShortSchedule",
    "build_commission_rebate_rate_vector",
    "build_effective_fee_vectors",
    "build_gross_fee_vectors",
    "build_lot_size_vector",
    "commission_rebate_rate_vector",
    "effective_fee_rate_vectors",
    "gross_fee_rate_vectors",
    "lot_size_vector",
    "normalize_execution_mode",
    "normalize_fee_rounding",
    "official_tw_short_initial_margin_rates",
    "require_naive_execution_for_tool",
    "settlement_session_indices",
]
