"""Lightweight Taiwan commission-rebate timing and calendar rules.

This module intentionally has no Torch dependency. Execution schedules and
live paper ledgers need the policy, not the differentiable rebate machinery.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Final

import numpy as np


COMMISSION_REBATE_TIMINGS: Final[tuple[str, ...]] = (
    "monthly_15th",
    "daily_close",
)

_TIMING_ALIASES: Final[dict[str, str]] = {
    "monthly_15th": "monthly_15th",
    "monthly-15th": "monthly_15th",
    "monthly": "monthly_15th",
    "month": "monthly_15th",
    "月退": "monthly_15th",
    "daily_close": "daily_close",
    "daily-close": "daily_close",
    "daily": "daily_close",
    "day": "daily_close",
    "日退": "daily_close",
}


def normalize_commission_rebate_timing(value: object) -> str:
    """Return the canonical commission-rebate timing name."""

    if not isinstance(value, str):
        raise ValueError(
            "commission_rebate_timing must be 'monthly_15th' or 'daily_close'"
        )
    normalized = _TIMING_ALIASES.get(value.strip().casefold())
    if normalized is None:
        raise ValueError(
            "commission_rebate_timing must be 'monthly_15th' or 'daily_close'"
        )
    return normalized


def commission_rebate_calendar(
    dates: Sequence[object] | np.ndarray,
) -> tuple[np.ndarray, np.ndarray]:
    """Build executor-only month ids and monthly-payment eligibility.

    A payment on a weekend or exchange holiday is deferred to the first
    observed session on or after the 15th.
    """

    raw = np.asarray(dates)
    if raw.ndim != 1:
        raise ValueError("dates must be one-dimensional")
    try:
        day_dates = raw.astype("datetime64[D]")
    except (TypeError, ValueError) as exc:
        raise ValueError("dates must be convertible to calendar dates") from exc
    if bool(np.isnat(day_dates).any()):
        raise ValueError("dates must not contain NaT")
    if day_dates.size > 1 and bool(np.any(day_dates[1:] <= day_dates[:-1])):
        raise ValueError("dates must be strictly increasing")

    month_dates = day_dates.astype("datetime64[M]")
    years = month_dates.astype("datetime64[Y]").astype(np.int64) + 1970
    months = (month_dates.astype(np.int64) % 12) + 1
    month_ids = years * 12 + months
    days = (day_dates - month_dates.astype("datetime64[D]")).astype(np.int64) + 1
    return (
        month_ids.astype(np.int64, copy=False),
        (days >= 15).astype(bool, copy=False),
    )
