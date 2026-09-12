"""Executor-only minute opportunities for the paper residual-carry contract.

This source-side schedule contains FUTURE session facts. Never append it to
model inputs or derive opening eligibility from whether an exit succeeds.
It does not authorize source completeness or integration into train.py.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from stockagent.backtest.tw_day_trade_contract import (
    BOARD_LOT_SHARES,
    MINUTE_VOLUME_PARTICIPATION,
)
from stockagent.data.tw_price_rules import (
    move_price_ticks_numpy,
    price_on_tick_grid_numpy,
)


PAPER_MINUTE_SCHEDULE_ABI = "paper_margin_carry_right_labelled_270_v2"


@dataclass(frozen=True)
class PaperMinuteOpportunities:
    prices: np.ndarray  # [symbol, 270, (long exit, short exit)]; NaN = no order
    capacity_shares: np.ndarray  # same shape; one shared budget, NOT per cohort
    marks: np.ndarray  # source Close, with explicitly indexed last-trade carry
    mark_source_index: np.ndarray  # [S,270], 0-based source minute; -1 = unavailable


def paper_minute_opportunities(
    bars: np.ndarray,
    *,
    trading_date: np.datetime64,
    lower_limit: np.ndarray,
    upper_limit: np.ndarray,
    security_types: np.ndarray,
    halted: np.ndarray | None = None,
) -> PaperMinuteOpportunities:
    """Match canonical paper replay on OHLC/VWAP/volume, not live bid/ask.

    Bars are [S,270,6] in Open,High,Low,Close,VWAP,volume_shares order,
    representing 09:01 through 13:30. A valid row may have zero volume.
    Missing rows have all five prices NaN and zero/NaN volume. No interpolation,
    daily-price fills, guaranteed liquidation or extra adverse tick is allowed.
    Single prices obey the dated product grid; VWAP is deliberately unrounded.
    """
    raw = np.asarray(bars)
    if raw.ndim != 3 or raw.shape[1:] != (270, 6):
        raise ValueError("paper minute schedule requires [S,270,6] OHLC/VWAP/volume")
    day = np.asarray(trading_date, dtype="datetime64[D]")
    if day.ndim or np.isnat(day):
        raise ValueError("paper minute schedule requires an exact trading date")
    s = raw.shape[0]
    kinds = np.asarray(security_types)
    lower, upper = (
        np.asarray(lower_limit, dtype=float),
        np.asarray(upper_limit, dtype=float),
    )
    if lower.shape != (s,) or upper.shape != (s,) or kinds.shape != (s,):
        raise ValueError("paper minute limits/types differ from the symbol universe")
    stopped = (
        np.zeros(s, dtype=bool) if halted is None else np.asarray(halted, dtype=bool)
    )
    if stopped.shape != (s,):
        raise ValueError("paper minute halt mask differs from the symbol universe")
    no_limit = np.isclose(lower, 0.01, rtol=0.0, atol=1e-12) & np.isclose(
        upper, 9999.95, rtol=0.0, atol=1e-12
    )
    if not np.all(
        stopped
        | (np.isfinite(lower) & np.isfinite(upper) & (lower > 0) & (lower < upper))
    ):
        raise ValueError("missing source-backed daily price limits")
    for value in (lower, upper):
        if not np.all(
            stopped | no_limit
            | price_on_tick_grid_numpy(value, day, security_types=kinds)
        ):
            raise ValueError("daily limit is off the dated product tick grid")
    values = raw.astype(np.float64, copy=False)
    opening, high, low, close, vwap, volume = np.moveaxis(values, -1, 0)
    absent = np.isnan(values[..., :5]).all(-1) & (np.isnan(volume) | (volume == 0))
    observed = (
        np.isfinite(values).all(-1)
        & (values[..., :5] > 0).all(-1)
        & (volume >= 0)
        & (volume == np.floor(volume))
    )
    tolerance = np.maximum(high * 1e-7, 1e-8)
    observed &= (
        (low <= opening)
        & (opening <= high)
        & (low <= close)
        & (close <= high)
        & (vwap >= low - tolerance)
        & (vwap <= high + tolerance)
    )
    if not np.all(absent | observed):
        raise ValueError("malformed historical OHLC/VWAP/volume row")
    for field in range(4):
        if not np.all(
            absent
            | price_on_tick_grid_numpy(
                raw[..., field], day, security_types=kinds[:, None]
            )
        ):
            raise ValueError(
                "observed single-trade price is off the dated product grid"
            )
    if np.any(observed & stopped[:, None] & (volume > 0)):
        raise ValueError("observed trading contradicts the suspension receipt")
    # The canonical paper loader drops zero-volume padding before *both* marks
    # and order triggering. A reference-price KBar cannot latch a stop that a
    # later real trade would then execute. Sub-lot positive volume is different:
    # it is a real observation even when the 50% whole-lot capacity is zero.
    observed &= (volume > 0) & ~stopped[:, None]
    capacity = (
        np.floor(
            np.where(observed & ~stopped[:, None], volume, 0)
            * MINUTE_VOLUME_PARTICIPATION
            / BOARD_LOT_SHARES
        )
        * BOARD_LOT_SHARES
    )
    out = np.full((s, 270, 2), np.nan, dtype=np.float64)
    inner_low = move_price_ticks_numpy(lower, 1, day, security_types=kinds)
    inner_high = move_price_ticks_numpy(upper, -1, day, security_types=kinds)
    for direction, long in enumerate((True, False)):
        tp, stop = (inner_high, inner_low) if long else (inner_low, inner_high)
        # Right labels 09:02..13:19: stops latch even without capacity. When
        # OHLC cannot establish event ordering, stop wins over take-profit.
        intraday = slice(1, 259)
        seen = observed[:, intraday]
        stop_hit = (
            (low[:, intraday] <= stop[:, None])
            if long
            else (high[:, intraday] >= stop[:, None])
        )
        latched = np.logical_or.accumulate(stop_hit & seen, axis=1)
        tp_hit = (
            (high[:, intraday] > tp[:, None])
            if long
            else (low[:, intraday] < tp[:, None])
        )
        limit_fill = (
            np.maximum(opening[:, intraday], tp[:, None])
            if long
            else np.minimum(opening[:, intraday], tp[:, None])
        )
        out[:, intraday, direction] = np.where(
            seen & latched,
            vwap[:, intraday],
            np.where(seen & tp_hit, limit_fill, np.nan),
        )
        # 13:20 submit only. Later un-crossed passive orders follow Close;
        # a quote first seen at this label cannot fill an order it just created.
        working = np.where(
            observed[:, 259],
            np.clip(move_price_ticks_numpy(
                close[:, 259], 1 if long else -1, day, security_types=kinds
            ), lower, upper),
            np.nan,
        )
        for minute_index in range(260, 264):  # 13:21..13:24
            seen = observed[:, minute_index]
            had_order = np.isfinite(working)
            crossed = (
                (high[:, minute_index] >= working)
                if long
                else (low[:, minute_index] <= working)
            )
            fill = (
                np.maximum(opening[:, minute_index], working)
                if long
                else np.minimum(opening[:, minute_index], working)
            )
            out[:, minute_index, direction] = np.where(
                seen & had_order & crossed, fill, np.nan
            )
            # No reprice after a crossing, including zero-volume crossings.
            working = np.where(
                seen & (~had_order | ~crossed), close[:, minute_index], working
            )
        # 13:24 market replacement first executes on right-labelled 13:25.
        out[:, 264, direction] = np.where(observed[:, 264], vwap[:, 264], np.nan)
        # 13:26..13:29 are collection/trial matching, NOT executable volume.
        out[:, 269, direction] = np.where(observed[:, 269], close[:, 269], np.nan)
    out[stopped] = np.nan
    capacities = np.where(np.isfinite(out), capacity[..., None], 0)
    # Reuse paper's last-observed-trade valuation standard, never its fill
    # prices. This carries only backwards-observed same-session closes, not a
    # future bar, official open, or fabricated 09:01. The source index exposes
    # stale marks to the adapter/report; leading absent marks remain NaN.
    source_index = np.maximum.accumulate(
        np.where(observed, np.arange(270)[None, :], -1), axis=1)
    marks = np.take_along_axis(close, np.maximum(source_index, 0), axis=1)
    marks = np.where(source_index >= 0, marks, np.nan)
    return PaperMinuteOpportunities(out, capacities, marks, source_index)
