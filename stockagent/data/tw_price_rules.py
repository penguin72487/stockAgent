"""Versioned Taiwan cash-equity, ETF and single-stock-futures price grids.

The model archive starts in 2000, so these historical boundaries matter:

* 2005-03-01: the regular stock tick buckets changed.
* 2015-06-01: the daily price fluctuation limit widened from 7% to 10%.
* 2026-07-06: stock-futures 1-dollar ticks extend from below 1,000 to below
  2,500. This change does not apply to the underlying cash stock.
* 2020-03-23: emerging-stock quotes move from a flat 0.01 to the six
  regular-stock price bands. An emerging listing is not a TPEx mainboard stock.

Dates are execution-session dates.  Missing dates deliberately select the
current rule so callers that only price a live order retain their historical
behaviour; historical panel builders must always pass their row dates.
"""

from __future__ import annotations

from typing import Any

import numpy as np


TW_PRICE_RULE_CONTRACT_VERSION = 3
# Optional product-aware order pricing is independently versioned: the
# unchanged regular-stock panel limit contract above must remain reproducible.
TW_ORDER_PRICE_CONTRACT_VERSION = 2
TW_DERIVATIVE_PRICE_CONTRACT_VERSION = 2
TW_TICK_RULE_2005_EFFECTIVE_DATE = np.datetime64("2005-03-01", "D")
TW_REIT_TICK_2006_EFFECTIVE_DATE = np.datetime64("2006-03-06", "D")
TW_STOCK_FUTURE_TICK_2026_EFFECTIVE_DATE = np.datetime64("2026-07-06", "D")
TW_TEO_TICK_2025_EFFECTIVE_DATE = np.datetime64("2025-12-08", "D")
TW_EMERGING_STOCK_TICK_2020_EFFECTIVE_DATE = np.datetime64("2020-03-23", "D")
TW_STOCK_FUTURE_FIRST_DATE = np.datetime64("2010-01-25", "D")
TW_ETF_FUTURE_FIRST_DATE = np.datetime64("2014-10-06", "D")
TW_LIMIT_10_PERCENT_EFFECTIVE_DATE = np.datetime64("2015-06-01", "D")
TW_TICK_RULE_2005_EFFECTIVE_ORDINAL = int(
    TW_TICK_RULE_2005_EFFECTIVE_DATE.astype(np.int64)
)
TW_REIT_TICK_2006_EFFECTIVE_ORDINAL = int(
    TW_REIT_TICK_2006_EFFECTIVE_DATE.astype(np.int64)
)
TW_STOCK_FUTURE_TICK_2026_EFFECTIVE_ORDINAL = int(
    TW_STOCK_FUTURE_TICK_2026_EFFECTIVE_DATE.astype(np.int64)
)
TW_TEO_TICK_2025_EFFECTIVE_ORDINAL = int(
    TW_TEO_TICK_2025_EFFECTIVE_DATE.astype(np.int64)
)
TW_EMERGING_STOCK_TICK_2020_EFFECTIVE_ORDINAL = int(
    TW_EMERGING_STOCK_TICK_2020_EFFECTIVE_DATE.astype(np.int64)
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


def _security_types(values: Any, shape: tuple[int, ...]) -> np.ndarray:
    kinds = np.broadcast_to(np.asarray(values), shape)
    if not np.all(
        np.isin(
            kinds,
            [
                "stock",
                "emerging_stock",
                "etf",
                "reit",
                "etn",
                "warrant",
                "convertible_bond",
                "stock_future",
                "etf_future",
            ],
        )
    ):
        raise ValueError(
            "unsupported TW outright quote-grid security type"
        )
    return kinds


def tick_size_numpy(
    price: np.ndarray, dates: Any | None = None, *, security_types: Any = "stock"
) -> np.ndarray:
    """Dated outright quote/order tick, in quote-currency units, for named products.

    This is not a rule for VWAP, settlement, adjusted prices, dividends, spread
    orders, block trades, other foreign-currency products or other products.
    TWSE odd-lot quoted prices share the ordinary numerical grid, while their
    auction/execution mechanics remain a separate contract.
    ETFs use 0.01 below 50 and 0.05 at/above 50; their daily price limit must
    still come from the exchange. This numerical grid also covers TWSE foreign-
    currency ETF counters, but callers must resolve their actual quote currency
    separately. Historical callers must supply execution dates.
    Sources: https://accessibility.twse.com.tw/zh/products/system/trading.html
    and TAIFEX notice 1150001529 (effective 2026-07-06).
    """

    values = np.asarray(price, dtype=np.float64)
    ordinals = trade_date_ordinals(dates, values.shape)
    out = np.full(values.shape, np.nan, dtype=np.float64)
    valid = np.isfinite(values) & (values > 0.0)
    old = valid & (ordinals < TW_TICK_RULE_2005_EFFECTIVE_ORDINAL)
    current = valid & ~old
    # Most historical panel callers are stock-only. Preserve that hot path
    # without allocating a universe-sized string mask on every invocation.
    kinds = (
        None if isinstance(security_types, str) and security_types == "stock"
        else _security_types(security_types, values.shape)
    )

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
    if kinds is not None:
        stock_future = valid & (kinds == "stock_future")
        etf_future = valid & (kinds == "etf_future")
        if dates is not None:
            if np.any(stock_future & (ordinals < int(TW_STOCK_FUTURE_FIRST_DATE.astype(np.int64)))):
                raise ValueError("stock futures were not listed before 2010-01-25")
            if np.any(etf_future & (ordinals < int(TW_ETF_FUTURE_FIRST_DATE.astype(np.int64)))):
                raise ValueError("ETF futures were not listed before 2014-10-06")
            if np.any((stock_future | etf_future) & (ordinals == TW_CURRENT_RULE_ORDINAL)):
                raise ValueError("historical futures price requires a trading date")
        etf = valid & np.isin(kinds, ["etf", "etf_future", "etn"])
        out[etf] = np.where(values[etf] < 50.0, 0.01, 0.05)
        # REITs initially used the regular post-2005 equity grid.  TWSE changed
        # them to the ETF grid on 2006-03-06.
        reit = valid & (kinds == "reit") & (
            ordinals >= TW_REIT_TICK_2006_EFFECTIVE_ORDINAL
        )
        out[reit] = np.where(values[reit] < 50.0, 0.01, 0.05)
        emerging = valid & (kinds == "emerging_stock")
        if np.any(emerging & (ordinals == TW_CURRENT_RULE_ORDINAL)):
            raise ValueError("historical emerging-stock price requires a trading date")
        out[emerging & (ordinals < TW_EMERGING_STOCK_TICK_2020_EFFECTIVE_ORDINAL)] = 0.01
        warrant = valid & (kinds == "warrant")
        old_warrant = warrant & (ordinals < TW_TICK_RULE_2005_EFFECTIVE_ORDINAL)
        current_warrant = warrant & ~old_warrant
        out[old_warrant] = 5.0
        out[old_warrant & (values < 1000.0)] = 1.0
        out[old_warrant & (values < 150.0)] = 0.5
        out[old_warrant & (values < 50.0)] = 0.1
        out[old_warrant & (values < 15.0)] = 0.05
        out[current_warrant] = 5.0
        out[current_warrant & (values < 500.0)] = 1.0
        out[current_warrant & (values < 100.0)] = 0.5
        out[current_warrant & (values < 50.0)] = 0.1
        out[current_warrant & (values < 10.0)] = 0.05
        out[current_warrant & (values < 5.0)] = 0.01
        convertible = valid & (kinds == "convertible_bond")
        out[convertible] = 5.0
        out[convertible & (values < 1000.0)] = 1.0
        out[convertible & (values < 150.0)] = 0.05
        revised = stock_future & (ordinals >= TW_STOCK_FUTURE_TICK_2026_EFFECTIVE_ORDINAL)
        out[revised & (values < 2500.0) & (values >= 1000.0)] = 1.0
    return out


def taifex_index_future_tick_size_numpy(
    price: np.ndarray, *, product_codes: Any
) -> np.ndarray:
    """Return ordinary outright tick sizes for locally trained index futures.

    Settlement/VWAP and calendar-spread prices are outside this contract.
    TX, MTX and TMF all quote in one index-point increments.  Requiring the
    product code prevents this small verified registry from silently blessing
    a different TAIFEX future.
    """

    values = np.asarray(price, dtype=np.float64)
    products = np.char.upper(
        np.broadcast_to(np.asarray(product_codes).astype(str), values.shape)
    )
    supported = np.isin(products, ["TX", "MTX", "TMF"])
    if np.any(np.isfinite(values) & (values > 0.0) & ~supported):
        unknown = sorted(set(products[np.isfinite(values) & (values > 0.0) & ~supported]))
        raise ValueError(f"unsupported TAIFEX index future products: {unknown[:8]}")
    return np.where(np.isfinite(values) & (values > 0.0) & supported, 1.0, np.nan)


def taifex_option_tick_size_numpy(
    price: np.ndarray,
    dates: Any | None = None,
    *,
    product_families: Any,
    trading_method: str = "ordinary",
) -> np.ndarray:
    """Return TAIFEX option premium ticks, excluding strikes and settlement.

    Product families are ``txo``, ``teo``, ``tfo`` and ``stock_option``.
    The 2019 TXO block-trade rule is intentionally a distinct trading method;
    it must never be applied to ordinary-market daily or tick files.
    """

    values = np.asarray(price, dtype=np.float64)
    ordinals = trade_date_ordinals(dates, values.shape)
    families = np.char.lower(
        np.broadcast_to(np.asarray(product_families).astype(str), values.shape)
    )
    supported = np.isin(families, ["txo", "teo", "tfo", "stock_option"])
    valid = np.isfinite(values) & (values > 0.0)
    if np.any(valid & ~supported):
        unknown = sorted(set(families[valid & ~supported]))
        raise ValueError(f"unsupported TAIFEX option families: {unknown[:8]}")
    if trading_method not in {"ordinary", "block"}:
        raise ValueError("TAIFEX option trading_method must be ordinary or block")
    if trading_method == "block":
        if np.any(valid & (families != "txo")):
            raise ValueError("only the verified TXO block-price rule is supported")
        # The separate block-trade quotation unit has been 0.1 point since
        # 2019-05-27.  Earlier block history is deliberately unsupported.
        if dates is None or np.any(
            valid & (ordinals < int(np.datetime64("2019-05-27", "D").astype(np.int64)))
        ):
            raise ValueError("verified TXO block tick starts on 2019-05-27")
        return np.where(valid, 0.1, np.nan)

    out = np.full(values.shape, np.nan, dtype=np.float64)
    txo = valid & (families == "txo")
    out[txo] = 10.0
    out[txo & (values < 1000.0)] = 5.0
    out[txo & (values < 500.0)] = 1.0
    out[txo & (values < 50.0)] = 0.5
    out[txo & (values < 10.0)] = 0.1

    stock_option = valid & (families == "stock_option")
    out[stock_option] = 5.0
    out[stock_option & (values < 1000.0)] = 1.0
    out[stock_option & (values < 150.0)] = 0.5
    out[stock_option & (values < 50.0)] = 0.1
    out[stock_option & (values < 15.0)] = 0.05
    out[stock_option & (values < 5.0)] = 0.01

    # TFO and post-change TEO share the current five bands.
    modern_sector = valid & (
        (families == "tfo")
        | ((families == "teo") & (ordinals >= TW_TEO_TICK_2025_EFFECTIVE_ORDINAL))
    )
    out[modern_sector] = 2.0
    out[modern_sector & (values < 200.0)] = 1.0
    out[modern_sector & (values < 100.0)] = 0.2
    out[modern_sector & (values < 10.0)] = 0.1
    out[modern_sector & (values < 2.0)] = 0.02

    old_teo = valid & (families == "teo") & (
        ordinals < TW_TEO_TICK_2025_EFFECTIVE_ORDINAL
    )
    out[old_teo] = 0.5
    out[old_teo & (values < 50.0)] = 0.25
    out[old_teo & (values < 25.0)] = 0.05
    out[old_teo & (values < 2.5)] = 0.025
    out[old_teo & (values < 0.5)] = 0.005
    return out


def price_on_explicit_tick_grid_numpy(
    price: np.ndarray, tick: np.ndarray
) -> np.ndarray:
    """Validate source values against a caller-resolved tick array."""

    raw = np.asarray(price)
    values = np.asarray(raw, dtype=np.float64)
    ticks = np.asarray(tick, dtype=np.float64)
    with np.errstate(invalid="ignore", divide="ignore"):
        nearest = np.rint(values / ticks) * ticks
        tolerance = np.maximum(1e-9, np.abs(np.spacing(values)) * 4)
        if raw.dtype.kind == "f" and raw.dtype.itemsize <= 4:
            tolerance = np.maximum(
                tolerance, np.abs(np.spacing(raw)).astype(np.float64) * 2
            )
        tolerance = np.minimum(tolerance, ticks * 0.001)
        return (
            np.isfinite(values)
            & (values > 0.0)
            & np.isfinite(ticks)
            & (ticks > 0.0)
            & (np.abs(values - nearest) <= tolerance)
        )


def price_on_tick_grid_numpy(
    price: np.ndarray, dates: Any | None = None, *, security_types: Any = "stock"
) -> np.ndarray:
    """Validate an individual quote/order; never use this to round a VWAP.

    A tiny absolute/ULP tolerance absorbs representation error, not a fraction
    of a legal tick. The source dtype matters for float32 quotation arrays.
    """
    raw = np.asarray(price)
    values = np.asarray(raw, dtype=np.float64)
    tick = tick_size_numpy(values, dates, security_types=security_types)
    with np.errstate(invalid="ignore", divide="ignore"):
        nearest = np.rint(values / tick) * tick
        tolerance = np.maximum(1e-9, np.abs(np.spacing(values)) * 4)
        if raw.dtype.kind == "f" and raw.dtype.itemsize <= 4:
            tolerance = np.maximum(tolerance, np.abs(np.spacing(raw)).astype(np.float64) * 2)
        # Never let low precision make an economically different price legal.
        tolerance = np.minimum(tolerance, tick * 0.001)
        return np.isfinite(values) & (values > 0) & (np.abs(values - nearest) <= tolerance)


def quantize_order_price_numpy(
    price: np.ndarray, rounding: str, dates: Any | None = None, *,
    security_types: Any = "stock",
) -> np.ndarray:
    """Round a CALCULATED order bound directionally to a legal cent grid.

    Passive sells round up, passive buys round down. This must not rewrite a
    source quote, a VWAP, an inventory cost basis, a fee or an account NAV.
    """
    if rounding not in {"up", "down"}:
        raise ValueError("TW order-price rounding must be up or down")
    values = np.asarray(price, dtype=np.float64)
    tick = tick_size_numpy(values, dates, security_types=security_types)
    with np.errstate(invalid="ignore", divide="ignore"):
        scaled = values * 100.0 / np.rint(tick * 100.0)
        tolerance = np.abs(np.spacing(scaled)) * 4
        units = np.ceil(scaled - tolerance) if rounding == "up" else np.floor(scaled + tolerance)
        result = units * np.rint(tick * 100.0) / 100.0
    return np.where(np.isfinite(values) & (values > 0) & (result > 0), result, np.nan)


def move_price_ticks_numpy(
    price: np.ndarray,
    ticks: int,
    dates: Any | None = None,
    *,
    security_types: Any = "stock",
) -> np.ndarray:
    """Move legal stock/ETF prices by an integer number of dated ticks.

    Downward moves probe immediately below the current price before resolving
    the tick size.  This matters at bucket boundaries: the legal price one
    tick below 1,000 is 999, not 995.  Repeated movement is deliberate so a
    caller crossing a historical or current tick bucket remains legal.
    """

    if isinstance(ticks, bool) or int(ticks) != ticks:
        raise ValueError("TW price tick movement must be an integer")
    steps = int(ticks)
    out = np.asarray(price, dtype=np.float64).copy()
    if steps == 0:
        return out

    direction = 1.0 if steps > 0 else -1.0
    for _ in range(abs(steps)):
        probe = out if direction > 0.0 else np.nextafter(out, -np.inf)
        tick = tick_size_numpy(probe, dates, security_types=security_types)
        valid = np.isfinite(out) & (out > 0.0) & np.isfinite(tick) & (tick > 0.0)
        shifted = out + direction * tick
        shifted = np.floor(shifted * 100.0 + 0.5) / 100.0
        out = np.where(valid & (shifted > 0.0), shifted, np.nan)
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
