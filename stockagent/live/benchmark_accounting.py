"""Shared accounting primitives for public market benchmarks.

Benchmarks are passive cross-session comparators, not day-trade executions.
Their daily return clock is the last observed/official value divided by the
preceding session close for the same economic holding.  Futures are fully
collateralized at one times notional and chain-link across contract rolls so a
calendar-spread level difference is an external funding flow, never return.
"""

from __future__ import annotations

import math
from typing import Final


MARKET_BENCHMARK_ACCOUNTING_CONTRACT_VERSION: Final[int] = 2
DAILY_RETURN_BASIS_PREVIOUS_CLOSE: Final[str] = (
    "current_mark_vs_previous_session_close"
)
TX_FULLY_COLLATERALIZED_CAPITAL_BASIS: Final[str] = (
    "one_contract_full_notional_1x_no_leverage"
)


def positive_finite(value: object) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) and number > 0.0 else None


def previous_close_return(
    current_value: object,
    previous_close: object,
) -> tuple[float | None, float | None]:
    """Return simple close-to-current return as fraction and percentage."""

    current = positive_finite(current_value)
    previous = positive_finite(previous_close)
    if current is None or previous is None:
        return None, None
    fraction = current / previous - 1.0
    return fraction, fraction * 100.0


def fully_collateralized_futures_notional(
    price: object,
    multiplier_twd_per_point: object,
) -> float | None:
    price_value = positive_finite(price)
    multiplier = positive_finite(multiplier_twd_per_point)
    if price_value is None or multiplier is None:
        return None
    return price_value * multiplier


def current_roll_linked_wealth(
    settled_wealth_index: object,
    current_price: object,
    current_contract_entry_price: object,
) -> float | None:
    settled = positive_finite(settled_wealth_index)
    current = positive_finite(current_price)
    entry = positive_finite(current_contract_entry_price)
    if settled is None or current is None or entry is None:
        return None
    return settled * current / entry


def settle_roll_wealth(
    settled_wealth_index: object,
    old_exit_price: object,
    old_contract_entry_price: object,
) -> float | None:
    """Lock the held-contract return before switching contract identities."""

    return current_roll_linked_wealth(
        settled_wealth_index,
        old_exit_price,
        old_contract_entry_price,
    )


__all__ = [
    "DAILY_RETURN_BASIS_PREVIOUS_CLOSE",
    "MARKET_BENCHMARK_ACCOUNTING_CONTRACT_VERSION",
    "TX_FULLY_COLLATERALIZED_CAPITAL_BASIS",
    "current_roll_linked_wealth",
    "fully_collateralized_futures_notional",
    "positive_finite",
    "previous_close_return",
    "settle_roll_wealth",
]
