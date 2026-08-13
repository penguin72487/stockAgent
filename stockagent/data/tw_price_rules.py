"""Versioned Taiwan regular-equity tick and daily price-limit rules.

The model archive starts in 2000, so two historical rule boundaries matter:

* 2005-03-01: the regular stock tick buckets changed.
* 2015-06-01: the daily price fluctuation limit widened from 7% to 10%.

Dates are execution-session dates.  Missing dates deliberately select the
current rule so callers that only price a live order retain their historical
behaviour; historical panel builders must always pass their row dates.
"""

from __future__ import annotations

from typing import Any

import numpy as np


TW_PRICE_RULE_CONTRACT_VERSION = 3
TW_TICK_RULE_2005_EFFECTIVE_DATE = np.datetime64("2005-03-01", "D")
TW_LIMIT_10_PERCENT_EFFECTIVE_DATE = np.datetime64("2015-06-01", "D")
TW_TICK_RULE_2005_EFFECTIVE_ORDINAL = int(
    TW_TICK_RULE_2005_EFFECTIVE_DATE.astype(np.int64)
)
TW_LIMIT_10_PERCENT_EFFECTIVE_ORDINAL = int(
    TW_LIMIT_10_PERCENT_EFFECTIVE_DATE.astype(np.int64)
)
TW_CURRENT_RULE_ORDINAL = int(np.iinfo(np.int64).max)


def trade_date_ordinals(values: Any | None, shape: tuple[int, ...]) -> np.ndarray:
    """Return broadcast execution-date ordinals, using current rules if absent."""

    if values is None:
        return np.full(shape, TW_CURRENT_RULE_ORDINAL, dtype=np.int64)
    dates = np.asarray(values)
    try:
        dates = dates.astype("datetime64[D]", copy=False)
    except (TypeError, ValueError) as exc:
        raise ValueError("TW price-rule dates must be datetime-like") from exc
    try:
        dates = np.broadcast_to(dates, shape)
    except ValueError as exc:
        raise ValueError(
            f"TW price-rule dates shape {dates.shape} cannot broadcast to {shape}"
        ) from exc
    ordinals = np.asarray(dates, dtype="datetime64[D]").astype(np.int64)
    # NumPy represents NaT with int64.min.  A missing historical date must not
    # accidentally select the pre-2005 rule; it follows the documented live
    # fallback instead.
    return np.where(np.isnat(dates), TW_CURRENT_RULE_ORDINAL, ordinals).astype(
        np.int64,
        copy=False,
    )


def tick_size_numpy(price: np.ndarray, dates: Any | None = None) -> np.ndarray:
    """Vectorized regular-stock tick size under the rule active on each date."""

    values = np.asarray(price, dtype=np.float64)
    ordinals = trade_date_ordinals(dates, values.shape)
    out = np.full(values.shape, np.nan, dtype=np.float64)
    valid = np.isfinite(values) & (values > 0.0)
    old = valid & (ordinals < TW_TICK_RULE_2005_EFFECTIVE_ORDINAL)
    current = valid & ~old

    out[old] = 5.0
    out[old & (values < 1000.0)] = 1.0
    out[old & (values < 150.0)] = 0.5
    out[old & (values < 50.0)] = 0.1
    out[old & (values < 15.0)] = 0.05
    out[old & (values < 5.0)] = 0.01

    out[current] = 5.0
    out[current & (values < 1000.0)] = 1.0
    out[current & (values < 500.0)] = 0.5
    out[current & (values < 100.0)] = 0.1
    out[current & (values < 50.0)] = 0.05
    out[current & (values < 10.0)] = 0.01
    return out


def dated_limit_ratio(ratio: float, dates: Any | None, shape: tuple[int, ...]) -> np.ndarray:
    """Map an up/down direction to the 7% or 10% rule active on each date."""

    requested = float(ratio)
    if requested == 1.0:
        return np.ones(shape, dtype=np.float64)
    ordinals = trade_date_ordinals(dates, shape)
    historical = ordinals < TW_LIMIT_10_PERCENT_EFFECTIVE_ORDINAL
    current_ratio = 1.10 if requested > 1.0 else 0.90
    historical_ratio = 1.07 if requested > 1.0 else 0.93
    return np.where(historical, historical_ratio, current_ratio)


def limit_price_numpy(
    reference_price: np.ndarray,
    ratio: float,
    dates: Any | None = None,
) -> np.ndarray:
    """Compute legal regular-stock limit prices using dated ratio and tick rules."""

    reference = np.asarray(reference_price, dtype=np.float64)
    ratios = dated_limit_ratio(ratio, dates, reference.shape)
    theoretical = reference * ratios
    tick = tick_size_numpy(theoretical, dates)
    out = np.full(theoretical.shape, np.nan, dtype=np.float64)
    valid = np.isfinite(theoretical) & np.isfinite(tick) & (tick > 0.0)
    scaled = theoretical[valid] / tick[valid]
    if float(ratio) < 1.0:
        out[valid] = np.ceil(scaled - 1e-12) * tick[valid]
    else:
        out[valid] = np.floor(scaled + 1e-12) * tick[valid]
    # Legal order prices are represented to cents for the regular-stock rules
    # covered by this project horizon.
    return np.floor(out * 100.0 + 0.5) / 100.0
