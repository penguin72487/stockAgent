"""Shared facts and accounting primitives for TW daily-policy simulations.

These are execution/accounting rules, not model features. Historical bar
proxies and causal live quotes are deliberately different evidence types.
Unlimited margin availability and board-price odd lots are explicit research
assumptions, not exchange or broker guarantees.
"""

from __future__ import annotations

from datetime import time
from typing import Final


# These names describe the accepted PAPER assumptions. Importing this module
# does not certify the legacy tensor executor's inventory/state parity.
MARGIN_CARRY_CONTRACT: Final[str] = "assumed_all_marginable_residual_next_signal_delta_v1"
ODD_LOT_BOARD_PRICE: Final[str] = "assumed_odd_lot_at_regular_board_price_v1"
ENTRY_MINUTE: Final[int] = 1
LIMIT_SUBMIT_MINUTE: Final[int] = 260
MARKET_SUBMIT_MINUTE: Final[int] = 264
FIRST_MARKET_FILL_MINUTE: Final[int] = 265
AUCTION_SUBMIT_MINUTE: Final[int] = 265
CLOSE_AUCTION_MINUTE: Final[int] = 270
BOARD_LOT_SHARES: Final[int] = 1000
MINUTE_VOLUME_PARTICIPATION: Final[float] = 0.5
MARGIN_FINANCING_PRINCIPAL_RATIO: Final[float] = 0.60
MARGIN_FINANCING_ANNUAL_RATE: Final[float] = 0.16
MARGIN_SHORT_HANDLING_FEE_RATE: Final[float] = 0.0010
MARGIN_SHORT_ANNUAL_BORROW_RATE: Final[float] = 0.20


def session_time(minute: int) -> time:
    """Convert a minute offset from 09:00 into a local session wall time."""
    if isinstance(minute, bool) or not isinstance(minute, int) or not 0 <= minute <= 270:
        raise ValueError("session minute must be an integer in [0,270]")
    return time(9 + minute // 60, minute % 60)


def historical_order_can_fill(*, submitted_minute: int, bar_end_minute: int) -> bool:
    """A right-labelled bar cannot execute an order created at its end."""
    session_time(submitted_minute)
    session_time(bar_end_minute)
    return bar_end_minute > submitted_minute


def net_liquidation_pnl(signed_shares, basis_price, mark_price, entry_cost, exit_rate):
    """One equation for scalar paper marks and differentiable tensor marks.

    Callers validate prices and select day-trade versus carried-position tax.
    Entry cost is the remaining unallocated NET cost (rebates applied once).
    This is a valuation, not an instruction to create a closing fill.
    """
    return (
        signed_shares * (mark_price - basis_price)
        - entry_cost
        - abs(signed_shares) * mark_price * exit_rate
    )


def inventory_carry_interest(signed_shares, basis_price, annual_rate, calendar_days):
    """Interest on still-outstanding principal, including weekends/holidays."""
    return abs(signed_shares) * basis_price * annual_rate * calendar_days / 365.0
