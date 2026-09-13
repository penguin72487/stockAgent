"""Durable multi-mode Taiwan stock day-trade paper execution.

The signal producer and this executor deliberately remain separate.  A live
signal artifact is an immutable model decision; once it is published at or
after 09:00, this module waits for a strictly later executable best Ask/Bid,
converts target weights to board lots, and owns the only paper order/fill/
position ledger used by the dashboard. Historical or missed-opening recovery
is a distinct counterfactual contract: size/infer from the official 09:00 open,
then value execution from a source-backed right-labelled 09:01 minute price.

This module never calls a broker order API.  ``simulation_only`` and
``production_order_possible`` are persisted in every status snapshot so a
paper fill cannot be mistaken for a real exchange fill.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from contextlib import contextmanager, nullcontext
import hashlib
import json
import math
import os
from pathlib import Path
from typing import Any, Callable, Final, Iterable, Mapping, Sequence
import uuid
from zoneinfo import ZoneInfo

import numpy as np

from stockagent.backtest.tw_day_trade_contract import (
    AUCTION_SUBMIT_MINUTE,
    CLOSE_AUCTION_MINUTE,
    ENTRY_MINUTE,
    LIMIT_SUBMIT_MINUTE,
    MARGIN_CARRY_CONTRACT,
    MARGIN_FINANCING_ANNUAL_RATE,
    MARGIN_FINANCING_PRINCIPAL_RATIO as MARGIN_FINANCING_RATIO,
    MARKET_SUBMIT_MINUTE,
    MINUTE_VOLUME_PARTICIPATION,
    inventory_carry_interest,
    net_liquidation_pnl,
    session_time,
)
from stockagent.backtest.tw_execution import (
    TaiwanFeeSchedule,
    commission_rebate_rate_vector,
    effective_fee_rate_vectors,
    gross_fee_rate_vectors,
)
from stockagent.backtest.tw_integer_execution import (
    _commission_fees_by_symbol,
    _scale_lot_buys_to_budget_with_fixed_fees,
    _tax_fees_by_symbol,
)
from stockagent.data.tw_index_futures import (
    TAIFEX_INDEX_FUTURES_FEE_PER_SIDE_TWD,
    TAIFEX_INDEX_FUTURES_MULTIPLIERS,
)
from stockagent.data.tw_price_rules import (
    TW_ORDER_PRICE_CONTRACT_VERSION,
    limit_price_numpy,
    move_price_ticks_numpy,
    price_on_tick_grid_numpy,
)
from stockagent.data.tw_security import classify_tw_stock_or_etf
from stockagent.live.benchmark_accounting import (
    DAILY_RETURN_BASIS_PREVIOUS_CLOSE,
    MARKET_BENCHMARK_ACCOUNTING_CONTRACT_VERSION,
    TX_FULLY_COLLATERALIZED_CAPITAL_BASIS,
    current_roll_linked_wealth,
    fully_collateralized_futures_notional,
    positive_finite,
    previous_close_return,
    settle_roll_wealth,
)
from stockagent.live.quote_provider import PriceSnapshot
from stockagent.live.tw_day_trade_service_sync import (
    SERVICE_SYNC_FILENAME,
    SERVICE_SYNC_SCHEMA_VERSION,
)
from stockagent.research.taifex_capital_returns import taifex_initial_margin_twd
from stockagent.research.taifex_transaction_tax import (
    stock_index_futures_tax_rate,
    taifex_tax_per_contract_twd,
)


TAIPEI: Final[ZoneInfo] = ZoneInfo("Asia/Taipei")
SIMULATION_SCHEMA_VERSION: Final[int] = 4
TX_CONTINUOUS_ROLL_CONTRACT_VERSION: Final[int] = 3
# The live path starts at the exchange open and must consume only a quote that
# is strictly later than the immutable signal publication.  ENTRY_GATE remains
# the historical-replay boundary for compatibility with existing replay tools.
LIVE_ENTRY_GATE: Final[time] = time(9, 0)
ENTRY_GATE: Final[time] = session_time(ENTRY_MINUTE)
FIRST_MINUTE_EXECUTION_TIME: Final[time] = ENTRY_GATE
EXIT_LIMIT_TIME: Final[time] = session_time(LIMIT_SUBMIT_MINUTE)
FORCE_EXIT_TIME: Final[time] = session_time(MARKET_SUBMIT_MINUTE)
CLOSING_AUCTION_TIME: Final[time] = session_time(AUCTION_SUBMIT_MINUTE)
SESSION_CLOSE: Final[time] = session_time(CLOSE_AUCTION_MINUTE)
AUCTION_OBSERVATION_DEADLINE: Final[time] = time(13, 35)
STRICT_INTRADAY_CONTRACT: Final[str] = "flatten_same_day_adverse_limit_exception_only_v1"
ENTRY_FILL_POLICY_CAUSAL_BOOK: Final[str] = "causal_best_quote"
ENTRY_FILL_POLICY_SYNTHETIC_OPEN_TICK: Final[str] = "synthetic_open_tick"
ENTRY_FILL_POLICY_CAUSAL_BOOK_ELSE_OPEN_TICK: Final[str] = (
    "causal_best_quote_else_adverse_open_tick"
)
ENTRY_FILL_POLICY_MARKET_AT_BEST_ELSE_OPEN_TICK: Final[str] = (
    "market_at_best_quote_else_adverse_open_tick"
)
ENTRY_FILL_POLICY_OFFICIAL_OPEN_AT_0901: Final[str] = "official_open_at_09_01"
ENTRY_FILL_POLICY_0901_MINUTE_VWAP: Final[str] = (
    "official_open_signal_0900_execute_0901_vwap"
)
# Compatibility alias: the persisted policy value predates the accepted
# source-published 09:01 KBar-close fallback. New code should use the
# minute-price name while old ledgers retain their stable enum value.
ENTRY_FILL_POLICY_0901_MINUTE_PRICE: Final[str] = (
    ENTRY_FILL_POLICY_0901_MINUTE_VWAP
)
REPLAY_FILL_CONTRACT_0901_MINUTE_PRICE: Final[str] = (
    "retrospective_official_open_signal_at_09_00_observed_09_01_"
    "minute_price_volume_capped_nav_counterfactual_v3"
)
EXECUTION_REALISM_CONTRACT: Final[str] = "nav_budget_source_capacity_no_synthetic_terminal_v1"
HISTORICAL_MINUTE_MARK_CONTRACT: Final[str] = "right_labelled_historical_last_trade_mark_v1"
ENTRY_FILL_POLICIES: Final[frozenset[str]] = frozenset(
    {
        ENTRY_FILL_POLICY_CAUSAL_BOOK,
        ENTRY_FILL_POLICY_CAUSAL_BOOK_ELSE_OPEN_TICK,
        ENTRY_FILL_POLICY_MARKET_AT_BEST_ELSE_OPEN_TICK,
        ENTRY_FILL_POLICY_0901_MINUTE_VWAP,
        ENTRY_FILL_POLICY_OFFICIAL_OPEN_AT_0901,
        ENTRY_FILL_POLICY_SYNTHETIC_OPEN_TICK,
    }
)
# Conservative paper assumptions.  The 7% is the TWSE cap for an unpaid
# day-trade securities shortfall and the extra 10% is the cap on the borrower
# handling charge as a fraction of that borrowing fee.  Margin financing has
# no exchange-wide tariff, so 16% is deliberately a stress assumption rather
# than a claim about a broker's current customer rate.
DAY_TRADE_SHORTFALL_BORROW_FEE_RATE: Final[float] = 0.07
DAY_TRADE_SHORTFALL_HANDLING_FEE_FRACTION: Final[float] = 0.10
MARGIN_SHORT_INITIAL_MARGIN_RATE: Final[float] = 0.90
DEFAULT_LIVE_RULE_DATA_DIR: Final[Path] = Path("/srv/stockagent-live/data_tw_public")
STOCK_BENCHMARKS: Final[tuple[tuple[str, str, str, str], ...]] = (
    ("benchmark_0050", "0050", "0050 元大台灣50（含息）", "etf"),
    ("benchmark_2330", "2330", "2330 台積電（含息）", "stock"),
)
TX_CONTINUOUS_BENCHMARK_ID: Final[str] = "benchmark_tx_continuous"
TX_CONTINUOUS_LOGICAL_CODE: Final[str] = "TXFR1"
DEFAULT_TAIFEX_INDEX_FINAL_SETTLEMENT_PATH: Final[Path] = (
    Path(__file__).resolve().parents[2]
    / "data_tw_index_options_daily/txo_final_settlement_history.parquet"
)


def _now_taipei(now: datetime | None = None) -> datetime:
    observed = now or datetime.now(TAIPEI)
    if observed.tzinfo is None:
        raise ValueError("simulation timestamps must be timezone-aware")
    return observed.astimezone(TAIPEI)


def _iso(now: datetime | None = None) -> str:
    return _now_taipei(now).isoformat(timespec="seconds")


def _parse_timestamp(value: object) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        try:
            parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S").replace(tzinfo=TAIPEI)
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=TAIPEI)
    return parsed.astimezone(TAIPEI)


def _event_temporal_key(
    event: Mapping[str, Any], *, sequence: int = 0
) -> tuple[str, str, int]:
    """Order ledger events by market time, not append position.

    Historical repairs are intentionally appended to the durable ledger instead
    of rewriting already committed live bytes. Their physical append position
    can therefore be newer while their market timestamp is older. Consumers
    recovering the latest signal must use the causal event clock or a valid
    session date; file sequence is only a deterministic equal-time tie breaker.
    """

    recorded_at = _parse_timestamp(event.get("recorded_at"))
    if recorded_at is not None:
        return (
            recorded_at.date().isoformat(),
            recorded_at.isoformat(timespec="microseconds"),
            int(sequence),
        )
    session_date = str(event.get("session_date") or "")[:10]
    try:
        date.fromisoformat(session_date)
    except ValueError:
        session_date = "0000-00-00"
    return (session_date, f"{session_date}T00:00:00+08:00", int(sequence))


def _timestamp_is_for_session(value: object, session_date: str) -> bool:
    """Return whether a lifecycle marker belongs to the active session.

    Persisted modes span trading days.  A truthy timestamp from yesterday must
    never suppress today's closing-auction submission or terminal settlement.
    """

    parsed = _parse_timestamp(value)
    return parsed is not None and parsed.date().isoformat() == str(session_date)


def _finite(value: object) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) and number > 0.0 else None


def position_net_liquidation_pnl(
    position: Mapping[str, Any],
    liquidation_price: float,
    *,
    signed_shares: int | None = None,
    remaining_entry_fee_twd: float | None = None,
) -> float:
    """Value one open paper position with the canonical cash-cost contract.

    Historical minute-curve reconstruction calls the same arithmetic as the
    live marker.  The optional overrides are necessary for an archived, fully
    closed position: its current signed shares and remaining entry fee are both
    zero, while the historical intraday mark must value the original quantity
    and entry fee.
    """

    price = _finite(liquidation_price)
    if price is None:
        raise ValueError("liquidation_price must be positive and finite")
    signed = (
        int(position.get("signed_shares") or 0)
        if signed_shares is None
        else int(signed_shares)
    )
    if signed == 0:
        return 0.0
    side = str(position.get("side") or ("long" if signed > 0 else "short"))
    prefix = "cash_" if position.get("margin_carry_contract") else ""
    exit_rate = float(position[f"{prefix}sell_fee_rate"] if side == "long" else position[f"{prefix}buy_fee_rate"])
    rebate_rate = float(position.get("commission_rebate_rate") or 0.0)
    entry_fee = (
        float(
            position.get(
                "remaining_entry_fee_twd",
                position.get("entry_fee_twd", 0.0),
            )
            or 0.0
        )
        if remaining_entry_fee_twd is None
        else float(remaining_entry_fee_twd)
    )
    return net_liquidation_pnl(
        signed,
        float(position.get("inventory_basis_price", position["entry_price"])),
        price,
        entry_fee,
        exit_rate - rebate_rate,
    )


def _date_value(value: object) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _top_book_capacity_shares(
    quote: Mapping[str, Any],
    *,
    transaction_side: str,
    lot_size: int,
) -> int:
    """Return executable shares visible at the relevant best quote.

    TWSE MIS ``f``/``g`` best-five quantities are board-lot counts.  This
    simulation intentionally consumes only level one: deeper prices and queue
    position are unknown, so the remainder must stay unfilled instead of being
    fabricated at the top price.
    """

    if transaction_side not in {"buy", "sell"}:
        raise ValueError("transaction_side must be 'buy' or 'sell'")
    displayed_lots = _finite(
        quote.get("ask_volume" if transaction_side == "buy" else "bid_volume")
    )
    if displayed_lots is None:
        return 0
    return int(math.floor(displayed_lots)) * int(lot_size)


def _minute_kbar_capacity_shares(
    quote: Mapping[str, Any],
    *,
    lot_size: int,
    participation: float = MINUTE_VOLUME_PARTICIPATION,
) -> int:
    """Return whole-lot capacity from one completed regular-session minute.

    Shioaji regular-board snapshot volume is denominated in board lots.  A
    missing or non-isolated minute is not evidence of liquidity and therefore
    has zero capacity.  Flooring before conversion to shares prevents a half
    lot from leaking into this board-lot-only simulator.
    """

    minute_lots = _finite(quote.get("minute_volume_lots"))
    if minute_lots is None:
        return 0
    return int(math.floor(minute_lots * float(participation))) * int(lot_size)


def _executable_capacity_shares(
    quote: Mapping[str, Any],
    *,
    transaction_side: str,
    lot_size: int,
) -> int:
    """Require both displayed level-one depth and 50% minute-K liquidity."""

    return min(
        _top_book_capacity_shares(
            quote,
            transaction_side=transaction_side,
            lot_size=lot_size,
        ),
        _minute_kbar_capacity_shares(quote, lot_size=lot_size),
    )


def _force_exit_retry_capacity_shares(
    position: dict[str, Any],
    quote: Mapping[str, Any],
    *,
    transaction_side: str,
    lot_size: int,
    now: datetime,
) -> int:
    """Return remaining same-minute capacity for repeated market exits.

    A fresh best quote may be retried every service poll from 13:24 to 13:25,
    but the completed-minute participation budget may only be consumed once.
    This prevents a two-second retry loop from multiplying the same one-minute
    volume evidence into fabricated liquidity.
    """

    minute_key = now.replace(second=0, microsecond=0).isoformat(timespec="minutes")
    observed_capacity = _minute_kbar_capacity_shares(quote, lot_size=lot_size)
    if position.get("force_exit_liquidity_minute") != minute_key:
        position["force_exit_liquidity_minute"] = minute_key
        position["force_exit_minute_capacity_shares"] = observed_capacity
        position["force_exit_minute_consumed_shares"] = 0
    else:
        position["force_exit_minute_capacity_shares"] = max(
            int(position.get("force_exit_minute_capacity_shares") or 0),
            observed_capacity,
        )
    remaining_volume_capacity = max(
        0,
        int(position.get("force_exit_minute_capacity_shares") or 0)
        - int(position.get("force_exit_minute_consumed_shares") or 0),
    )
    return min(
        _top_book_capacity_shares(
            quote,
            transaction_side=transaction_side,
            lot_size=lot_size,
        ),
        remaining_volume_capacity,
    )


def _synthetic_open_tick_entry_price(
    opening_price: float | None,
    *,
    side: str,
    trading_date: date,
    offset_ticks: int,
    lower_limit: float | None,
    upper_limit: float | None,
) -> float | None:
    """Return the explicitly synthetic, adverse open-plus-tick paper price.

    A buy pays above the observed session open; a sell/short receives below it.
    This is a deterministic counterfactual fill convention, not evidence that
    an exchange order could consume arbitrary size at that price.
    """

    if opening_price is None or side not in {"long", "short"}:
        return None
    signed_ticks = int(offset_ticks) if side == "long" else -int(offset_ticks)
    moved = float(
        move_price_ticks_numpy(
            np.asarray([opening_price], dtype=np.float64),
            signed_ticks,
            np.asarray([trading_date]),
        )[0]
    )
    if not math.isfinite(moved) or moved <= 0.0:
        return None
    if lower_limit is not None:
        moved = max(moved, float(lower_limit))
    if upper_limit is not None:
        moved = min(moved, float(upper_limit))
    return moved


def _prepare_entry_plan(
    raw_row: Mapping[str, Any],
    *,
    quote: Mapping[str, Any],
    evidence: LiveEligibility | None,
    signal_at: datetime,
    observation_at: datetime,
    spec: ModeSpec,
    allow_quote_at_signal: bool = False,
    sizing_nav_twd: float | None = None,
    requested_shares_override: int | None = None,
    reduce_only: bool = False,
) -> dict[str, Any]:
    """Resolve one symbol's admissible entry independently."""

    row = dict(raw_row)
    symbol = str(row.get("symbol") or "")
    target_weight = float(row.get("target_weight") or 0.0)
    side = "long" if target_weight > 0.0 else "short" if target_weight < 0.0 else "flat"
    quote_values = dict(quote)
    status = "ready"
    reason: str | None = None
    sizing_price = _finite(quote_values.get("open")) or _finite(row.get("open_price"))
    synthetic_open_fill = (
        spec.entry_fill_policy == ENTRY_FILL_POLICY_SYNTHETIC_OPEN_TICK
    )
    market_at_best_else_tick = (
        spec.entry_fill_policy == ENTRY_FILL_POLICY_MARKET_AT_BEST_ELSE_OPEN_TICK
    )
    official_open_at_0901 = (
        spec.entry_fill_policy == ENTRY_FILL_POLICY_OFFICIAL_OPEN_AT_0901
    )
    minute_price_at_0901 = (
        spec.entry_fill_policy == ENTRY_FILL_POLICY_0901_MINUTE_PRICE
    )
    synthetic_fallback_fill = bool(
        spec.entry_fill_policy == ENTRY_FILL_POLICY_CAUSAL_BOOK_ELSE_OPEN_TICK
        and quote_values.get("entry_price_is_synthetic_fallback") is True
    )
    synthetic_entry_fill = synthetic_open_fill or synthetic_fallback_fill
    entry_price = (
        sizing_price
        if official_open_at_0901
        else _finite(quote_values.get("execution_price_0901"))
        if minute_price_at_0901
        else _finite(quote_values.get("ask" if side == "long" else "bid"))
    )
    quote_at = _parse_timestamp(quote_values.get("quote_at"))
    upper = _finite(quote_values.get("upper_limit"))
    lower = _finite(quote_values.get("lower_limit"))
    if synthetic_entry_fill:
        entry_price = _synthetic_open_tick_entry_price(
            sizing_price,
            side=side,
            trading_date=observation_at.date(),
            offset_ticks=int(spec.entry_price_offset_ticks),
            lower_limit=lower,
            upper_limit=upper,
        )
    if side == "flat":
        status, reason = "hold", "zero_target_weight"
    elif not reduce_only and not bool(row.get("tradable")):
        status, reason = "blocked", "model_tradable_mask_false"
    elif not reduce_only and side == "long" and not bool(row.get("can_buy")):
        status, reason = "blocked", "cannot_buy_open"
    elif not reduce_only and side == "short" and not bool(row.get("can_sell")):
        status, reason = "blocked", "cannot_sell_open"
    elif not reduce_only and (evidence is None or not evidence.covered):
        status, reason = "blocked", "exact_session_eligibility_missing"
    elif not reduce_only and not evidence.eligible:
        status, reason = "blocked", "not_day_trade_eligible"
    elif not reduce_only and side == "short" and not evidence.short_open:
        status, reason = "blocked", "sell_first_suspended"
    elif sizing_price is None:
        status, reason = (
            ("blocked", "official_session_no_trade_print")
            if bool(quote_values.get("official_session_no_trade_print"))
            else ("blocked", "official_open_price_unavailable")
        )

    requested_shares = 0
    filled_shares = 0
    top_book_capacity_shares = 0
    minute_kbar_capacity_shares = 0
    if status == "ready" and sizing_price is not None:
        requested_shares = int(
            math.floor(
                abs(target_weight)
                * float(spec.initial_capital_twd if sizing_nav_twd is None else sizing_nav_twd)
                / sizing_price
                / int(spec.lot_size)
            )
        ) * int(spec.lot_size)
        if requested_shares_override is not None:
            requested_shares = int(requested_shares_override)
            if requested_shares < 0 or (requested_shares % spec.lot_size and not (
                    reduce_only and spec.odd_lot_execution_policy == "assumed_odd_lot_at_regular_board_price_v1")):
                raise ValueError("override must be nonnegative whole-lot shares")
        if requested_shares <= 0:
            status, reason = "skipped", "below_one_board_lot"
    if status == "ready":
        quote_is_causal = bool(
            entry_price is not None
            and quote_at is not None
            and (
                quote_at >= signal_at if allow_quote_at_signal else quote_at > signal_at
            )
            and quote_at <= observation_at
        )
        if market_at_best_else_tick and not quote_is_causal:
            synthetic_fallback_fill = True
            synthetic_entry_fill = True
            entry_price = _synthetic_open_tick_entry_price(
                sizing_price,
                side=side,
                trading_date=observation_at.date(),
                offset_ticks=max(1, int(spec.entry_price_offset_ticks)),
                lower_limit=lower,
                upper_limit=upper,
            )
        if (spec.strict_intraday and spec.entry_fill_policy == ENTRY_FILL_POLICY_CAUSAL_BOOK
                and (quote_values.get("simtrade") is not False or quote_at is None
                     or not 0 <= (observation_at - quote_at).total_seconds() <= 10)):
            status, reason = "blocked", "waiting_non_trial_quote"
        elif entry_price is None:
            status, reason = (
                ("blocked", "observed_09_01_minute_price_unavailable")
                if minute_price_at_0901
                else
                ("blocked", "synthetic_open_tick_price_unavailable")
                if synthetic_entry_fill
                else ("blocked", "no_executable_best_quote")
            )
        elif upper is None or lower is None:
            status, reason = "blocked", "price_limit_unavailable"
        elif spec.uses_realistic_execution and not (lower - 1e-8 <= entry_price <= upper + 1e-8):
            status, reason = "blocked", "execution_price_outside_daily_limits"
        elif minute_price_at_0901:
            # Missed-opening recovery separates the two causal roles that the
            # legacy replay conflated. The official session open sizes the
            # order/model input, while this observed right-labelled first
            # source-backed minute price values the 09:01 counterfactual paper
            # execution. Prefer VWAP, but a source-published KBar Close is
            # admissible when no tick/VWAP exists. A missing minute bar remains
            # blocked and is never replaced by the open, a carried last price,
            # a best quote, or an adverse tick.
            minute_kbar_capacity_shares = _minute_kbar_capacity_shares(quote_values, lot_size=spec.lot_size)
            filled_shares = min(requested_shares, minute_kbar_capacity_shares)
            reason = "counterfactual_observed_09_01_minute_price_fill"
            if filled_shares <= 0:
                status, reason = "blocked", "observed_09_01_minute_liquidity_unavailable"
            elif filled_shares < requested_shares:
                status, reason = "partial_depth", "observed_09_01_minute_capacity_exhausted"
        elif official_open_at_0901:
            # User-selected paper convention: at 09:01 use the already observed
            # official session open for both directions.  This is deterministic
            # counterfactual valuation, not a claim that a 09:01 exchange order
            # could receive the earlier auction/opening price.
            filled_shares = requested_shares
            reason = "counterfactual_official_open_price_fill_at_09_01"
        elif synthetic_entry_fill:
            # Explicit paper fallback: every otherwise legal whole-lot request
            # is filled at the observed session open moved one adverse tick.
            # Market depth is intentionally not claimed or inferred.  The
            # hybrid policy is restricted to a separately labelled historical
            # counterfactual rebuild; the active runner never enables it.
            filled_shares = requested_shares
            status = "forced_synthetic_fill"
            reason = (
                "synthetic_adverse_open_tick_fallback_fill"
                if synthetic_fallback_fill
                else "synthetic_open_tick_fill"
            )
        elif market_at_best_else_tick:
            # The configured paper-market-order contract consumes the complete
            # requested whole-lot quantity at the causally observed best Ask
            # for buys/covers or best Bid for sells/shorts. Displayed L1 size
            # remains audit evidence, not a quantity ceiling. This never claims
            # an exchange fill or queue position.
            top_book_capacity_shares = _top_book_capacity_shares(
                quote_values,
                transaction_side="buy" if side == "long" else "sell",
                lot_size=spec.lot_size,
            )
            minute_kbar_capacity_shares = _minute_kbar_capacity_shares(
                quote_values,
                lot_size=spec.lot_size,
            )
            filled_shares = requested_shares
        elif quote_at is None or (
            quote_at < signal_at if allow_quote_at_signal else quote_at <= signal_at
        ):
            status, reason = "blocked", "quote_not_after_signal"
        elif quote_at > observation_at:
            status, reason = "blocked", "quote_after_local_observation"
        else:
            top_book_capacity_shares = _top_book_capacity_shares(
                quote_values,
                transaction_side="buy" if side == "long" else "sell",
                lot_size=spec.lot_size,
            )
            minute_kbar_capacity_shares = _minute_kbar_capacity_shares(
                quote_values,
                lot_size=spec.lot_size,
            )
            quote_wall_time = (
                quote_at.astimezone(TAIPEI).timetz().replace(tzinfo=None)
                if quote_at is not None
                else None
            )
            # At 09:00 no completed one-minute K bar exists yet. Requiring one
            # delayed every entry until 09:01 even when a causally newer,
            # executable best quote was already available. Opening orders use
            # only fresh displayed level-one depth; later orders retain the
            # conservative completed-minute participation cap.
            if quote_wall_time is not None and quote_wall_time < time(9, 1):
                filled_shares = min(requested_shares, top_book_capacity_shares)
            else:
                filled_shares = min(
                    requested_shares,
                    top_book_capacity_shares,
                    minute_kbar_capacity_shares,
                )
            if filled_shares <= 0:
                status, reason = "blocked", "marketable_depth_unavailable"
            elif filled_shares < requested_shares:
                status, reason = "partial_depth", "marketable_depth_exhausted"
    return {
        "row": row,
        "symbol": symbol,
        "target_weight": target_weight,
        "side": side,
        "quote": quote_values,
        "evidence": evidence,
        "status": status,
        "reason": reason,
        "entry_price": entry_price,
        "sizing_price": sizing_price,
        "upper": upper,
        "lower": lower,
        "requested_shares": requested_shares,
        "filled_shares": filled_shares,
        "top_book_capacity_shares": top_book_capacity_shares,
        "minute_kbar_capacity_shares": minute_kbar_capacity_shares,
        "entry_fill_policy": spec.entry_fill_policy,
        "entry_price_offset_ticks": int(spec.entry_price_offset_ticks),
        "entry_price_source": quote_values.get("entry_price_source")
        or (
            "observed_right_labelled_09_01_minute_price"
            if minute_price_at_0901
            else
            "official_session_open_observed_by_09_01"
            if official_open_at_0901
            else quote_values.get("source")
        ),
        "synthetic_fill": synthetic_entry_fill and filled_shares > 0,
        "synthetic_fallback_fill": synthetic_fallback_fill and filled_shares > 0,
        "paper_market_fill": market_at_best_else_tick and filled_shares > 0,
        "counterfactual_0901_price_fill": (
            minute_price_at_0901 and filled_shares > 0
        ),
        "entry_price_method": quote_values.get("execution_price_0901_method"),
        "counterfactual_open_price_fill": (official_open_at_0901 and filled_shares > 0),
    }


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".tmp.{uuid.uuid4().hex}")
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            handle.write(
                json.dumps(
                    payload,
                    ensure_ascii=False,
                    indent=2,
                    sort_keys=True,
                    default=str,
                )
                + "\n"
            )
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        # The rename is only crash-durable after the containing directory is
        # synced.  This state is the exactly-once boundary for the append-only
        # paper ledgers, so a merely atomic-but-not-durable replace is not
        # sufficient.
        directory_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
        directory_fd = os.open(path.parent, directory_flags)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        temporary.unlink(missing_ok=True)


def _append_jsonl(path: Path, payload: Mapping[str, Any]) -> None:
    _append_jsonl_many(path, (payload,))


@contextmanager
def minute_curve_write_lock(root: Path):
    """Serialize the minute ledger append with background atomic replacement."""
    root.mkdir(parents=True, exist_ok=True)
    with (root / ".minute_curve_write.lock").open("a+") as lock:
        if os.name == "nt":
            import msvcrt
            if lock.tell() == 0:
                lock.write("0")
                lock.flush()
            lock.seek(0)
            msvcrt.locking(lock.fileno(), msvcrt.LK_LOCK, 1)
            try:
                yield
            finally:
                lock.seek(0)
                msvcrt.locking(lock.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl
            fcntl.flock(lock, fcntl.LOCK_EX)
            yield


def _append_jsonl_many(
    path: Path,
    payloads: Sequence[Mapping[str, Any]],
) -> None:
    """Durably append one logical ledger batch with a single fsync.

    A full-universe signal contains thousands of audit rows. Syncing every
    row separately made ledger persistence several seconds slower without
    improving the durability boundary of the logical signal transaction.
    Encode the complete batch first, then append and fsync it once.
    """

    if not payloads:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = "".join(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str) + "\n"
        for payload in payloads
    ).encode("utf-8")
    with minute_curve_write_lock(path.parent) if path.name == "marks.jsonl" else nullcontext():
        descriptor = os.open(path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o600)
        try:
            view = memoryview(encoded)
            while view:
                written = os.write(descriptor, view)
                if written <= 0:
                    raise OSError(f"short append to {path}")
                view = view[written:]
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


@dataclass(frozen=True, slots=True)
class ModeSpec:
    market: str
    label: str
    initial_capital_twd: float
    config_path: str
    checkpoint_path: str | None
    parquet_root: Path
    live_output_dir: Path
    fee_schedule: TaiwanFeeSchedule
    lot_size: int = 1_000
    signal_market: str | None = None
    price_limit_offset_ticks: int = 0
    entry_fill_policy: str = ENTRY_FILL_POLICY_CAUSAL_BOOK
    entry_price_offset_ticks: int = 0
    realistic_execution: bool = True
    residual_margin_conversion: bool = False
    strict_intraday: bool = False
    odd_lot_execution_policy: str = "reject"
    margin_corporate_action_reference_path: Path | None = None
    margin_financing_ratio: float = 0.60
    margin_financing_annual_rate: float = 0.16
    margin_short_annual_borrow_rate: float = 0.20
    margin_short_handling_fee_rate: float = 0.001

    @property
    def uses_realistic_execution(self) -> bool:
        return self.realistic_execution and self.entry_fill_policy in {
            ENTRY_FILL_POLICY_CAUSAL_BOOK, ENTRY_FILL_POLICY_0901_MINUTE_PRICE,
        }

    def __post_init__(self) -> None:
        if self.strict_intraday and not self.uses_realistic_execution:
            raise ValueError("strict intraday requires source-backed realistic execution")
        if self.odd_lot_execution_policy not in {"reject", "assumed_odd_lot_at_regular_board_price_v1"}:
            raise ValueError("unknown odd-lot execution policy")
        if self.odd_lot_execution_policy != "reject" and not self.residual_margin_conversion:
            raise ValueError("odd-lot board-price assumption requires margin carry")
        if self.residual_margin_conversion and not self.uses_realistic_execution:
            raise ValueError("margin carry requires causal-book or source-backed 09:01 volume-capped execution")
        for field in ("margin_financing_ratio", "margin_financing_annual_rate",
                      "margin_short_annual_borrow_rate", "margin_short_handling_fee_rate"):
            value = float(getattr(self, field))
            if not math.isfinite(value) or value < 0 or (field == "margin_financing_ratio" and value > 1):
                raise ValueError(f"invalid {field}")
        if (
            isinstance(self.price_limit_offset_ticks, bool)
            or int(self.price_limit_offset_ticks) != self.price_limit_offset_ticks
            or int(self.price_limit_offset_ticks) < 0
        ):
            raise ValueError("price_limit_offset_ticks must be a non-negative integer")
        if str(self.entry_fill_policy) not in ENTRY_FILL_POLICIES:
            raise ValueError(
                f"entry_fill_policy must be one of {sorted(ENTRY_FILL_POLICIES)}"
            )
        if (
            isinstance(self.entry_price_offset_ticks, bool)
            or int(self.entry_price_offset_ticks) != self.entry_price_offset_ticks
            or int(self.entry_price_offset_ticks) < 0
        ):
            raise ValueError("entry_price_offset_ticks must be a non-negative integer")


@dataclass(frozen=True, slots=True)
class LiveEligibility:
    symbol: str
    venue: str | None
    security_type: str | None
    eligible: bool
    short_open: bool
    covered: bool
    source_date: str | None
    reason: str | None = None


def resolve_day_trade_rule_data_dir(
    configured: str | Path | None,
    *,
    parquet_root: Path,
    repo_root: Path,
) -> Path:
    """Resolve the mutable same-session rule source shared by live consumers."""

    raw = configured or os.getenv("STOCKAGENT_TW_DAY_TRADE_RULE_DATA_DIR")
    if raw:
        path = Path(raw).expanduser()
        return path if path.is_absolute() else Path(repo_root) / path
    if DEFAULT_LIVE_RULE_DATA_DIR.is_dir():
        return DEFAULT_LIVE_RULE_DATA_DIR
    return Path(parquet_root).parent


def load_symbol_metadata(parquet_root: Path) -> dict[str, dict[str, str]]:
    path = Path(parquet_root) / "symbols.csv"
    if not path.is_file():
        return {}
    with path.open("r", encoding="utf-8", newline="") as handle:
        return {
            str(row.get("code") or "").strip(): {
                "venue": str(row.get("market") or "").strip().casefold(),
                "security_type": str(row.get("security_type") or "").strip().casefold(),
                "name": str(row.get("name") or "").strip(),
            }
            for row in csv.DictReader(handle)
            if str(row.get("code") or "").strip()
        }


def load_live_eligibility(
    *,
    rule_data_dir: Path,
    parquet_root: Path,
    symbols: Sequence[str],
    trading_date: date,
    require_latest: bool = True,
) -> tuple[dict[str, LiveEligibility], dict[str, Any]]:
    """Resolve exact-session membership; a missing venue/date fails closed."""

    import polars as pl

    metadata = load_symbol_metadata(parquet_root)
    target_date = trading_date.isoformat()
    members: dict[str, dict[str, bool]] = {}
    coverage: dict[str, dict[str, Any]] = {}
    for venue, dataset in (
        ("twse", "twse_day_trade_eligibility"),
        ("tpex", "tpex_day_trade_eligibility"),
    ):
        path = Path(rule_data_dir) / f"{dataset}.parquet"
        venue_members: dict[str, bool] = {}
        latest_date: str | None = None
        error: str | None = None
        if path.is_file():
            try:
                lazy = pl.scan_parquet(path)
                latest_date = lazy.select(pl.col("date").max()).collect().item()
                rows = (
                    lazy.filter(pl.col("date") == target_date)
                    .select(
                        pl.col("證券代號")
                        .cast(pl.String)
                        .str.strip_chars()
                        .alias("symbol"),
                        pl.col("暫停現股賣出後現款買進當沖註記")
                        .cast(pl.String)
                        .fill_null("")
                        .str.strip_chars()
                        .alias("suspension"),
                    )
                    .collect()
                )
                venue_members = {
                    str(row["symbol"]): str(row["suspension"] or "") == ""
                    for row in rows.iter_rows(named=True)
                    if str(row["symbol"] or "")
                }
            except Exception as exc:  # fail closed and surface provenance
                error = f"{type(exc).__name__}: {exc}"
        else:
            error = f"missing {path}"
        members[venue] = venue_members
        coverage[venue] = {
            "dataset": dataset,
            "path": str(path),
            "target_date": target_date,
            "latest_date": latest_date,
            "covered": bool(venue_members) and (not require_latest or latest_date == target_date),
            "member_count": len(venue_members),
            "error": error,
        }

    resolved: dict[str, LiveEligibility] = {}
    for raw_symbol in symbols:
        symbol = str(raw_symbol)
        item = metadata.get(symbol, {})
        venue = str(item.get("venue") or "").casefold() or None
        security_type = str(item.get("security_type") or "").casefold() or None
        venue_coverage = coverage.get(str(venue), {})
        covered = bool(venue_coverage.get("covered"))
        eligible = covered and symbol in members.get(str(venue), {})
        short_open = eligible and bool(members[str(venue)][symbol])
        if venue not in {"twse", "tpex"}:
            reason = "unknown_venue"
        elif not covered:
            reason = "exact_session_eligibility_missing"
        elif not eligible:
            reason = "not_day_trade_eligible"
        elif not short_open:
            reason = "sell_first_suspended"
        else:
            reason = None
        resolved[symbol] = LiveEligibility(
            symbol=symbol,
            venue=venue,
            security_type=security_type,
            eligible=eligible,
            short_open=short_open,
            covered=covered,
            source_date=target_date if covered else venue_coverage.get("latest_date"),
            reason=reason,
        )
    return resolved, coverage


def require_exact_session_eligibility(
    *,
    rule_data_dir: Path,
    parquet_root: Path,
    trading_date: date,
) -> dict[str, Any]:
    """Return official same-session coverage or fail before READY/trading."""

    _resolved, coverage = load_live_eligibility(
        rule_data_dir=rule_data_dir,
        parquet_root=parquet_root,
        symbols=(),
        trading_date=trading_date,
    )
    missing = {
        venue: row for venue, row in coverage.items() if not bool(row.get("covered"))
    }
    if missing:
        details = "; ".join(
            f"{venue} target={row.get('target_date')} "
            f"latest={row.get('latest_date') or 'missing'}"
            + (f" error={row.get('error')}" if row.get("error") else "")
            for venue, row in sorted(missing.items())
        )
        raise RuntimeError(
            f"exact-session day-trade eligibility unavailable: {details}"
        )
    return coverage


def quote_map_from_snapshot(
    symbols: Sequence[str],
    snapshot: PriceSnapshot,
    *,
    trading_date: date,
) -> dict[str, dict[str, Any]]:
    count = len(symbols)

    def values(source: np.ndarray | None) -> np.ndarray:
        if source is None:
            return np.full((count,), np.nan, dtype=np.float64)
        array = np.array(source, dtype=np.float64, copy=True)
        if array.shape != (count,):
            raise ValueError(f"quote array shape {array.shape} != {(count,)}")
        return array

    last = values(snapshot.prices)
    open_prices = values(snapshot.open_prices)
    cumulative_volume_lots = values(snapshot.volumes)
    bid = values(snapshot.bid_prices)
    ask = values(snapshot.ask_prices)
    bid_volume = values(snapshot.bid_volumes)
    ask_volume = values(snapshot.ask_volumes)
    upper = values(snapshot.upper_limit_prices)
    lower = values(snapshot.lower_limit_prices)
    reference = values(snapshot.reference_prices)
    timestamps = (
        np.asarray(snapshot.timestamps_ms, dtype=np.int64)
        if snapshot.timestamps_ms is not None
        else np.zeros((count,), dtype=np.int64)
    )
    exchange_timestamps = (
        np.asarray(snapshot.exchange_timestamps_ms, dtype=np.int64)
        if snapshot.exchange_timestamps_ms is not None
        else np.zeros((count,), dtype=np.int64)
    )
    simtrade_flags = (
        np.asarray(snapshot.simtrade_flags, dtype=np.int8)
        if snapshot.simtrade_flags is not None
        else np.full((count,), -1, dtype=np.int8)
    )
    for name, array in (
        ("timestamps_ms", timestamps),
        ("exchange_timestamps_ms", exchange_timestamps),
        ("simtrade_flags", simtrade_flags),
    ):
        if array.shape != (count,):
            raise ValueError(f"{name} shape {array.shape} != {(count,)}")
    date_values = np.full((count,), np.datetime64(trading_date.isoformat(), "D"))
    classified = [classify_tw_stock_or_etf(s) for s in symbols]
    product_types = np.asarray([kind or "stock" for kind in classified])
    known_products = np.asarray([kind is not None for kind in classified])
    computed_upper = limit_price_numpy(reference, 1.10, date_values)
    computed_lower = limit_price_numpy(reference, 0.90, date_values)
    # ETF tick schedules do NOT imply a 10% price limit. Leveraged and foreign
    # products require their actual exchange limits; never fabricate them from
    # a stock's reference-price formula.
    computed_upper = np.where(known_products & (product_types == "stock"), computed_upper, np.nan)
    computed_lower = np.where(known_products & (product_types == "stock"), computed_lower, np.nan)
    upper = np.where(np.isfinite(upper), upper, computed_upper)
    lower = np.where(np.isfinite(lower), lower, computed_lower)
    invalid_price_fields: dict[str, np.ndarray] = {}
    for name, array, raw in (
        ("bid", bid, snapshot.bid_prices), ("ask", ask, snapshot.ask_prices),
        ("upper_limit", upper, upper), ("lower_limit", lower, lower),
    ):
        legal = known_products & price_on_tick_grid_numpy(
            raw if raw is not None else array, date_values, security_types=product_types,
        )
        invalid_price_fields[name] = np.isfinite(array) & (array > 0) & ~legal
        # Preserve the original evidence in the provider. Reject invalid
        # executable prices here instead of snapping an observed quote.
        array[~legal] = np.nan
        # Only already-validated representation noise is normalized to cents.
        # A float32 49.9000015 can denote 49.90; an actual 49.91 stock bid cannot.
        array[legal] = np.rint(array[legal] * 100.0) / 100.0

    output: dict[str, dict[str, Any]] = {}
    for idx, raw_symbol in enumerate(symbols):
        timestamp = None
        if int(timestamps[idx]) > 0:
            timestamp = (
                datetime.fromtimestamp(int(timestamps[idx]) / 1000.0, tz=timezone.utc)
                .astimezone(TAIPEI)
                .isoformat(timespec="milliseconds")
            )
        exchange_timestamp = None
        if int(exchange_timestamps[idx]) > 0:
            exchange_timestamp = (
                datetime.fromtimestamp(
                    int(exchange_timestamps[idx]) / 1000.0,
                    tz=timezone.utc,
                )
                # Shioaji encodes Taiwan exchange wall time in its numeric
                # Snapshot.ts field. Preserve that clock; converting it as a
                # real UTC epoch would incorrectly add eight hours.
                .replace(tzinfo=TAIPEI)
                .isoformat(timespec="milliseconds")
            )
        output[str(raw_symbol)] = {
            "symbol": str(raw_symbol),
            "last": _finite(last[idx]),
            "open": _finite(open_prices[idx]),
            "cumulative_volume_lots": _finite(cumulative_volume_lots[idx]),
            "bid": _finite(bid[idx]),
            "ask": _finite(ask[idx]),
            "bid_volume": _finite(bid_volume[idx]),
            "ask_volume": _finite(ask_volume[idx]),
            "upper_limit": _finite(upper[idx]),
            "lower_limit": _finite(lower[idx]),
            "reference_price": _finite(reference[idx]),
            "quote_at": timestamp,
            "exchange_quote_at": exchange_timestamp,
            "simtrade": (
                None
                if int(simtrade_flags[idx]) < 0
                else bool(simtrade_flags[idx])
            ),
            "source": snapshot.source,
            "invalid_order_price_fields": [
                name for name, invalid in invalid_price_fields.items() if invalid[idx]
            ],
        }
    return output


class TwDayTradeSimulationEngine:
    """One durable simulation ledger shared by all stock day-trade modes."""

    def __init__(
        self,
        state_dir: Path,
        *,
        final_settlement_path: str | Path = DEFAULT_TAIFEX_INDEX_FINAL_SETTLEMENT_PATH,
        publication_clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._publication_clock = publication_clock
        self.state_dir = Path(state_dir)
        self.state_path = self.state_dir / "state.json"
        self.status_path = self.state_dir / "status.json"
        self.service_sync_path = self.state_dir / SERVICE_SYNC_FILENAME
        self.positions_path = self.state_dir / "positions.json"
        self.position_history_dir = self.state_dir / "position_history"
        self.signals_path = self.state_dir / "signals.jsonl"
        self.orders_path = self.state_dir / "orders.jsonl"
        self.fills_path = self.state_dir / "fills.jsonl"
        self.marks_path = self.state_dir / "marks.jsonl"
        self.benchmark_marks_path = self.state_dir / "benchmark_marks.jsonl"
        self.events_path = self.state_dir / "events.jsonl"
        self.latency_path = self.state_dir / "latency.jsonl"
        self.final_settlement_path = Path(final_settlement_path)
        self._tx_final_settlement_error: str | None = None
        self._corporate_action_reference_path: Path | None = None
        self._corporate_action_cache_signature: tuple[int, ...] | None = None
        self._corporate_actions_by_symbol: dict[str, list[dict[str, Any]]] = {}
        self._corporate_action_load_error: str | None = None
        self._corporate_action_coverage_end: date | None = None
        self._margin_action_cache: dict[tuple[Any, ...], Any] = {}
        self._engine_run_id = uuid.uuid4().hex
        self._deferred_ledger_rows: dict[Path, list[Mapping[str, Any]]] | None = None
        self.state = self._load_state()
        stock_benchmarks_migrated = self._migrate_stock_benchmark_contract()
        tx_benchmark_migrated = self._migrate_tx_continuous_benchmark_contract()
        self._reconcile_daily_duplicate_signal_ids()
        self._audit_signal_commit_state()
        for mode in self.state.get("modes", {}).values():
            if mode.get("entry_retry_pending_commit"):
                mode["ledger_state_divergence"] = {"kind": "entry_retry_commit_interrupted",
                    "signal_id": mode.get("signal_id")}
                mode["engine_status"] = "critical_ledger_state_divergence"
        self._restore_position_artifact_paths()
        if stock_benchmarks_migrated or tx_benchmark_migrated:
            _atomic_json(self.state_path, self.state)

    def begin_deferred_ledger_writes(self) -> None:
        """Batch historical replay ledgers until the session commit boundary."""

        if self._deferred_ledger_rows is not None:
            raise RuntimeError("deferred ledger writes are already active")
        self._deferred_ledger_rows = {}

    def flush_deferred_ledger_writes(self) -> None:
        """Durably append every deferred ledger before state.json advances."""

        pending = self._deferred_ledger_rows
        if pending is None:
            return
        try:
            for path, rows in pending.items():
                _append_jsonl_many(path, rows)
        finally:
            self._deferred_ledger_rows = None

    def _append_ledger(self, path: Path, payload: Mapping[str, Any]) -> None:
        pending = self._deferred_ledger_rows
        if pending is None:
            _append_jsonl(path, payload)
            return
        pending.setdefault(path, []).append(dict(payload))

    def _audit_signal_commit_state(self) -> None:
        """Fail closed when an interrupted ledger commit and state disagree.

        Ledger batches are fsynced before state.json is advanced.  A process or
        host failure inside that small window must never make the same signal
        executable twice after restart.  The compact event ledger records the
        start and completion of every new commit; any unmatched start, or a
        completed event absent from state, is quarantined for explicit repair.
        """

        if not self.events_path.is_file():
            return
        latest_started: dict[str, tuple[tuple[str, str, int], str, str]] = {}
        latest_registered: dict[str, tuple[tuple[str, str, int], str, str]] = {}
        with self.events_path.open("r", encoding="utf-8") as handle:
            for sequence, line in enumerate(handle):
                try:
                    event = json.loads(line)
                except (TypeError, ValueError):
                    continue
                market = str(event.get("market") or "").strip()
                signal_id = str(event.get("signal_id") or "").strip()
                event_name = str(event.get("event") or "").strip()
                if not market or not signal_id:
                    continue
                session_date = str(event.get("session_date") or "").strip()
                if not session_date:
                    recorded_at = _parse_timestamp(event.get("recorded_at"))
                    session_date = (
                        recorded_at.date().isoformat()
                        if recorded_at is not None
                        else ""
                    )
                ordered = (
                    _event_temporal_key(event, sequence=sequence),
                    signal_id,
                    session_date,
                )
                if event_name == "signal_commit_started":
                    if ordered > latest_started.get(
                        market, (("0000-00-00", "", -1), "", "")
                    ):
                        latest_started[market] = ordered
                elif event_name == "signal_registered":
                    if ordered > latest_registered.get(
                        market, (("0000-00-00", "", -1), "", "")
                    ):
                        latest_registered[market] = ordered

        markets = set(latest_started) | set(latest_registered)
        for market in markets:
            raw_started = latest_started.get(market)
            raw_registered = latest_registered.get(market)
            started = raw_started[1:] if raw_started is not None else None
            registered = raw_registered[1:] if raw_registered is not None else None
            divergence_kind: str | None = None
            signal_id = ""
            session_date = ""
            if started is not None and started != registered:
                divergence_kind = "signal_commit_started_without_registration"
                signal_id, session_date = started
            elif registered is not None:
                signal_id, session_date = registered
                mode = (self.state.get("modes") or {}).get(market)
                processed = (
                    set(mode.get("processed_signal_ids") or ())
                    if isinstance(mode, Mapping)
                    else set()
                )
                state_accepts_signal = bool(
                    isinstance(mode, Mapping)
                    and (
                        signal_id in processed
                        or (
                            str(mode.get("signal_id") or "") == signal_id
                            and bool(mode.get("entry_completed_at"))
                        )
                    )
                )
                if not state_accepts_signal:
                    divergence_kind = "registered_ledger_missing_from_state"
            if divergence_kind is None:
                continue
            mode = self.state.setdefault("modes", {}).setdefault(market, {})
            mode["market"] = market
            mode["ledger_state_divergence"] = {
                "kind": divergence_kind,
                "signal_id": signal_id,
                "session_date": session_date,
                "detected_at": _iso(),
            }
            mode["engine_status"] = "critical_ledger_state_divergence"
            mode["readiness_error"] = (
                f"ledger_state_divergence:{divergence_kind}:{signal_id}"
            )

    def _migrate_stock_benchmark_contract(self) -> bool:
        benchmarks = self.state.get("benchmarks") or {}
        changed = False
        for benchmark_id, _symbol, label, _security_type in STOCK_BENCHMARKS:
            row = benchmarks.get(benchmark_id)
            if not isinstance(row, dict):
                continue
            legacy_entry_cost = float(row.get("fixed_fees_twd") or 0.0)
            legacy_liquidation_cost = float(row.get("liquidation_cost_twd") or 0.0)
            updates = {
                "label": label,
                "return_type": "total_return",
                "total_return_contract": ("official_ex_date_reference_reinvestment_v1"),
                "holding_period": "cross_session_buy_and_hold",
                "daily_return_basis": DAILY_RETURN_BASIS_PREVIOUS_CLOSE,
                "benchmark_accounting_contract_version": (
                    MARKET_BENCHMARK_ACCOUNTING_CONTRACT_VERSION
                ),
                "tracking_costs_excluded_from_return": True,
                "fixed_fees_twd": 0.0,
                "transaction_tax_twd": 0.0,
                "liquidation_cost_twd": 0.0,
                "estimated_entry_cost_twd": max(
                    float(row.get("estimated_entry_cost_twd") or 0.0),
                    legacy_entry_cost,
                ),
                "estimated_liquidation_cost_twd": max(
                    float(row.get("estimated_liquidation_cost_twd") or 0.0),
                    legacy_liquidation_cost,
                ),
            }
            for key, value in updates.items():
                if row.get(key) != value:
                    row[key] = value
                    changed = True
            estimated_tracking_cost = float(
                row.get("estimated_entry_cost_twd") or 0.0
            ) + float(row.get("estimated_liquidation_cost_twd") or 0.0)
            if row.get("estimated_tracking_cost_twd") != estimated_tracking_cost:
                row["estimated_tracking_cost_twd"] = estimated_tracking_cost
                changed = True
        return changed

    def _migrate_tx_continuous_benchmark_contract(self) -> bool:
        row = (self.state.get("benchmarks") or {}).get(TX_CONTINUOUS_BENCHMARK_ID)
        if not isinstance(row, dict):
            return False
        changed = False
        previous_contract_version = int(row.get("roll_contract_version") or 0)
        updates = {
            "roll_contract_version": TX_CONTINUOUS_ROLL_CONTRACT_VERSION,
            "official_final_settlement_path": str(self.final_settlement_path),
            "return_type": "gross_buy_and_hold_time_weighted",
            "holding_period": "cross_session_buy_and_hold_with_continuous_roll",
            "daily_return_basis": DAILY_RETURN_BASIS_PREVIOUS_CLOSE,
            "capital_basis": TX_FULLY_COLLATERALIZED_CAPITAL_BASIS,
            "benchmark_accounting_contract_version": (
                MARKET_BENCHMARK_ACCOUNTING_CONTRACT_VERSION
            ),
            "tracking_costs_excluded_from_return": True,
            "maximum_leverage": 1.0,
        }
        for key, value in updates.items():
            if row.get(key) != value:
                row[key] = value
                changed = True
        if int(row.get("roll_count") or 0) == 0:
            row.setdefault("origin_entry_price", row.get("entry_price"))
            row.setdefault("origin_entry_at", row.get("entry_at"))
            row.setdefault("current_contract_entry_price", row.get("entry_price"))
            row.setdefault("current_contract_entry_at", row.get("entry_at"))
        elif row.get("origin_entry_price") is None:
            # A legacy already-rolled row cannot prove its immutable origin
            # from the current-contract basis alone.  Keep it visible but do
            # not manufacture a rebase identity.
            row["roll_contract_migration_error"] = (
                "legacy_rolled_benchmark_origin_unavailable"
            )
            return True

        if previous_contract_version >= TX_CONTINUOUS_ROLL_CONTRACT_VERSION:
            return changed

        origin_entry = positive_finite(row.get("origin_entry_price"))
        current_entry = origin_entry
        settled_wealth = 1.0
        funding_flow = 0.0
        margin_top_up = 0.0
        margin_withdrawal = 0.0
        for roll in row.get("roll_history") or ():
            if not isinstance(roll, Mapping):
                current_entry = None
                break
            old_exit = positive_finite(
                roll.get("old_exit_price")
                if roll.get("old_exit_price") is not None
                else roll.get("old_bid")
            )
            new_entry = positive_finite(roll.get("new_ask"))
            next_wealth = settle_roll_wealth(
                settled_wealth,
                old_exit,
                current_entry,
            )
            if next_wealth is None or old_exit is None or new_entry is None:
                current_entry = None
                break
            settled_wealth = next_wealth
            roll_funding = (new_entry - old_exit) * TAIFEX_INDEX_FUTURES_MULTIPLIERS[
                "TX"
            ]
            funding_flow += roll_funding
            margin_top_up += max(0.0, roll_funding)
            margin_withdrawal += max(0.0, -roll_funding)
            current_entry = new_entry

        persisted_current_entry = positive_finite(
            row.get("current_contract_entry_price") or row.get("entry_price")
        )
        if (
            origin_entry is None
            or current_entry is None
            or persisted_current_entry is None
            or not math.isclose(
                current_entry,
                persisted_current_entry,
                rel_tol=0.0,
                abs_tol=1e-9,
            )
        ):
            row["roll_contract_migration_error"] = (
                "legacy_roll_history_cannot_reconstruct_buy_hold_wealth"
            )
            row["valuation_stale"] = True
            return True

        old_capital = _finite(row.get("initial_capital_twd"))
        full_notional = fully_collateralized_futures_notional(
            origin_entry,
            TAIFEX_INDEX_FUTURES_MULTIPLIERS["TX"],
        )
        assert full_notional is not None
        if previous_contract_version < TX_CONTINUOUS_ROLL_CONTRACT_VERSION:
            row["legacy_initial_margin_twd"] = old_capital
        legacy_fixed_fees = float(row.get("fixed_fees_twd") or 0.0)
        legacy_transaction_tax = float(row.get("transaction_tax_twd") or 0.0)
        row["estimated_fixed_fees_twd"] = max(
            float(row.get("estimated_fixed_fees_twd") or 0.0),
            legacy_fixed_fees,
        )
        row["estimated_transaction_tax_twd"] = max(
            float(row.get("estimated_transaction_tax_twd") or 0.0),
            legacy_transaction_tax,
        )
        row["fixed_fees_twd"] = 0.0
        row["transaction_tax_twd"] = 0.0
        row["liquidation_cost_twd"] = 0.0
        row["estimated_tracking_cost_twd"] = (
            float(row.get("estimated_fixed_fees_twd") or 0.0)
            + float(row.get("estimated_transaction_tax_twd") or 0.0)
            + float(row.get("estimated_liquidation_cost_twd") or 0.0)
        )
        row["initial_capital_twd"] = full_notional
        row["settled_buy_hold_wealth_index"] = settled_wealth
        row["cumulative_external_funding_flow_twd"] = funding_flow
        row["cumulative_margin_top_up_twd"] = margin_top_up
        row["cumulative_margin_withdrawal_twd"] = margin_withdrawal
        mark = positive_finite(row.get("last_mark_price"))
        wealth = current_roll_linked_wealth(settled_wealth, mark, current_entry)
        if wealth is not None and mark is not None:
            performance_pnl = full_notional * (wealth - 1.0)
            current_notional = fully_collateralized_futures_notional(
                mark,
                TAIFEX_INDEX_FUTURES_MULTIPLIERS["TX"],
            )
            row.update(
                {
                    "buy_hold_wealth_index": wealth,
                    "performance_pnl_twd": performance_pnl,
                    "gross_pnl_twd": performance_pnl,
                    "net_pnl_twd": performance_pnl,
                    "total_equity_twd": full_notional * wealth,
                    "return_fraction": wealth - 1.0,
                    "return_pct": (wealth - 1.0) * 100.0,
                    "current_contract_notional_twd": current_notional,
                    "fully_collateralized_account_equity_twd": current_notional,
                    "collateral_coverage_ratio": 1.0,
                }
            )
        return True

    def _official_tx_final_settlement(
        self,
        *,
        delivery_date: date,
        delivery_month: str,
    ) -> dict[str, Any] | None:
        """Resolve one monthly TX terminal value from the official index FSP file.

        The retained file is produced from TAIFEX's index-option table, but a
        monthly TXO row and TX/MTX/TMF use the same underlying-index final
        settlement formula and value.  Requiring both date and six-digit month
        prevents a weekly option settlement from being mistaken for the
        expiring monthly future.
        """

        self._tx_final_settlement_error = None
        month = str(delivery_month or "").strip()
        if len(month) != 6 or not month.isdigit():
            self._tx_final_settlement_error = (
                f"invalid_contract_delivery_month:{delivery_month}"
            )
            return None
        if not self.final_settlement_path.is_file():
            self._tx_final_settlement_error = (
                f"missing_official_final_settlement_file:{self.final_settlement_path}"
            )
            return None
        try:
            import polars as pl

            frame = pl.read_parquet(self.final_settlement_path)
            required = {"settlement_date", "final_settlement_price"}
            if not required <= set(frame.columns):
                raise ValueError(
                    "official final-settlement file lacks settlement_date or price"
                )
            series_column = (
                "option_series"
                if "option_series" in frame.columns
                else "contract_delivery_month"
                if "contract_delivery_month" in frame.columns
                else None
            )
            if series_column is None:
                raise ValueError(
                    "official final-settlement file lacks a contract month column"
                )
            matched = frame.filter(
                (pl.col("settlement_date") == delivery_date)
                & (pl.col(series_column).cast(pl.String) == month)
            )
            if matched.height != 1:
                self._tx_final_settlement_error = (
                    "missing_official_tx_final_settlement:"
                    f"{delivery_date.isoformat()}:{month}:rows={matched.height}"
                )
                return None
            row = matched.row(0, named=True)
            price = _finite(row.get("final_settlement_price"))
            if price is None:
                raise ValueError("official TX final settlement is not positive finite")
        except Exception as exc:
            self._tx_final_settlement_error = (
                f"invalid_official_final_settlement:{type(exc).__name__}:{exc}"
            )
            return None
        return {
            "price": price,
            "settlement_date": delivery_date.isoformat(),
            "delivery_month": month,
            "source_file": str(row.get("source_file") or self.final_settlement_path),
            "source_sha256": str(row.get("source_sha256") or ""),
            "source_url": str(row.get("source_url") or ""),
            "price_source": "official_taifex_index_final_settlement",
        }

    def _reconcile_daily_duplicate_signal_ids(self) -> None:
        """Restore the accepted signal identity after a blocked duplicate.

        Older state writers recorded a later ``daily_signal_already_consumed``
        candidate as the mode's active signal even though the accepted signal,
        positions, and execution ledger were left untouched.  The append-only
        event ledger is authoritative for repairing that display-only mismatch.
        """

        if not self.events_path.is_file():
            return
        latest_registered: dict[str, tuple[tuple[str, str, int], str]] = {}
        latest_event: dict[
            str, tuple[tuple[str, str, int], str, str, str | None]
        ] = {}
        with self.events_path.open("r", encoding="utf-8") as handle:
            for sequence, line in enumerate(handle):
                try:
                    event = json.loads(line)
                except (TypeError, ValueError):
                    continue
                market = str(event.get("market") or "")
                if not market:
                    continue
                event_name = str(event.get("event") or "")
                signal_id = str(event.get("signal_id") or "")
                reason = (
                    str(event.get("reason"))
                    if event.get("reason") is not None
                    else None
                )
                event_key = _event_temporal_key(event, sequence=sequence)
                if event_name == "signal_registered" and signal_id:
                    registered = (event_key, signal_id)
                    if registered > latest_registered.get(
                        market, (("0000-00-00", "", -1), "")
                    ):
                        latest_registered[market] = registered
                if event_name in {"signal_registered", "signal_blocked"}:
                    latest = (event_key, event_name, signal_id, reason)
                    if event_key >= latest_event.get(
                        market, (("0000-00-00", "", -1), "", "", None)
                    )[0]:
                        latest_event[market] = latest

        for market, mode in (self.state.get("modes") or {}).items():
            if not isinstance(mode, dict) or not mode.get("entry_completed_at"):
                continue
            registered = latest_registered.get(str(market))
            registered_id = registered[1] if registered is not None else None
            _event_key, event_name, duplicate_id, reason = latest_event.get(
                str(market), (("0000-00-00", "", -1), "", "", None)
            )
            if (
                registered_id
                and event_name == "signal_blocked"
                and reason == "daily_signal_already_consumed"
                and duplicate_id
                and str(mode.get("signal_id") or "") == duplicate_id
            ):
                mode["signal_id"] = registered_id
                mode["last_duplicate_signal_id"] = duplicate_id
                mode["last_duplicate_signal_reason"] = reason

    def _restore_position_artifact_paths(self) -> None:
        """Backfill position artifact pointers for signals accepted before schema 1."""

        for mode in (self.state.get("modes") or {}).values():
            if not isinstance(mode, dict):
                continue
            mode["executed_positions_path"] = str(self.positions_path)
            if mode.get("target_weights_path") and mode.get("target_positions_path"):
                continue
            source_path = str(mode.get("signal_source_path") or "").strip()
            if not source_path:
                continue
            try:
                summary = json.loads(Path(source_path).read_text(encoding="utf-8"))
            except (OSError, TypeError, ValueError):
                continue
            if not isinstance(summary, Mapping) or str(
                summary.get("signal_id") or ""
            ) != str(mode.get("signal_id") or ""):
                continue
            mode["target_weights_path"] = summary.get("weights_path")
            mode["target_positions_path"] = summary.get(
                "positions_markdown_path"
            ) or summary.get("weights_path")
            mode["target_symbol_count"] = summary.get("symbol_count")
            mode["target_risk"] = summary.get("target_risk") or {}

    @staticmethod
    def _position_history_filename(market: object) -> str:
        normalized = "".join(
            character if character.isalnum() or character in {"-", "_"} else "_"
            for character in str(market or "unknown")
        )
        return f"{normalized or 'unknown'}.json"

    def _archive_mode_positions(
        self,
        mode: Mapping[str, Any],
        *,
        archived_at: datetime,
    ) -> None:
        """Persist the completed prior-session position lifecycle by date.

        ``state.json`` intentionally owns only the latest session.  Without a
        dated archive the dashboard date selector could retain marks and fills
        but lose the corresponding closed position rows as soon as the next
        signal arrived.
        """

        positions = [
            dict(position)
            for position in (mode.get("positions") or {}).values()
            if isinstance(position, Mapping)
        ]
        session_date = str(mode.get("session_date") or "").strip()
        if not session_date:
            return
        destination = (
            self.position_history_dir
            / session_date
            / self._position_history_filename(mode.get("market"))
        )
        if (mode.get("margin_carry_contract") == MARGIN_CARRY_CONTRACT
                and archived_at.date().isoformat() > session_date and destination.is_file()):
            return  # freeze prior-close inventory before next-day interest/marks
        _atomic_json(
            destination,
            {
                "schema_version": 1,
                "session_date": session_date,
                "market": mode.get("market"),
                "signal_id": mode.get("signal_id"),
                "archived_at": archived_at.isoformat(timespec="seconds"),
                "checkpoint_fingerprint": mode.get("checkpoint_fingerprint"),
                "config_fingerprint": mode.get("config_fingerprint"),
                "execution_realism_contract": mode.get("execution_realism_contract"),
                "margin_carry_contract": mode.get("margin_carry_contract"),
                "cumulative_carry_cost_twd": mode.get("cumulative_carry_cost_twd", 0),
                "cumulative_corporate_action_net_twd": mode.get("cumulative_corporate_action_net_twd", 0),
                "entry_fill_count": mode.get("entry_fill_count"),
                "simulation_only": True,
                "production_order_possible": False,
                "positions": positions,
            },
        )

    def _load_state(self) -> dict[str, Any]:
        if self.state_path.is_file():
            payload = json.loads(self.state_path.read_text(encoding="utf-8"))
            if isinstance(payload, dict) and int(payload.get("schema_version", 0)) in {
                2,
                3,
                SIMULATION_SCHEMA_VERSION,
            }:
                prior_schema = int(payload.get("schema_version", 0))
                payload["schema_version"] = SIMULATION_SCHEMA_VERSION
                payload.setdefault("modes", {})
                payload.setdefault("benchmarks", {})
                payload.setdefault("minute_liquidity", {})
                if prior_schema < SIMULATION_SCHEMA_VERSION:
                    payload["migrated_from_schema_version"] = prior_schema
                    for mode in payload["modes"].values():
                        mode.pop("execution_projection", None)
                        for position in (mode.get("positions") or {}).values():
                            for key in (
                                "pre_balance_filled_shares",
                                "pre_balance_filled_weight",
                                "directional_mix_adjusted",
                                "pre_balance_status",
                                "pre_balance_reason",
                            ):
                                position.pop(key, None)
                        open_legacy = any(
                            int(position.get("signed_shares") or 0) != 0
                            for position in (mode.get("positions") or {}).values()
                        )
                        if open_legacy:
                            mode["engine_status"] = (
                                "critical_legacy_position_requires_reconciliation"
                            )
                            mode["legacy_execution_contract"] = True
                return payload
        return {
            "schema_version": SIMULATION_SCHEMA_VERSION,
            "simulation_only": True,
            "production_order_possible": False,
            "created_at": _iso(),
            "modes": {},
            "benchmarks": {},
            "minute_liquidity": {},
        }

    def prepare_minute_quotes(
        self,
        quotes: Mapping[str, Mapping[str, Any]],
        *,
        now: datetime | None = None,
    ) -> dict[str, dict[str, Any]]:
        """Attach one-minute K-bar volume inferred from cumulative snapshots.

        A delta is accepted only between adjacent wall-clock minutes in the
        same session.  The 09:01 observation is the sole exception: its
        cumulative regular-board volume represents the completed opening
        auction plus first minute.  Service gaps fail closed instead of
        smearing several minutes of volume into one oversized fill budget.
        Repeated consumers in the same minute receive the cached same delta.
        """

        observed = _now_taipei(now)
        minute = observed.replace(second=0, microsecond=0)
        minute_key = minute.isoformat(timespec="minutes")
        session_date = observed.date().isoformat()
        ledger = self.state.setdefault("minute_liquidity", {})
        prepared: dict[str, dict[str, Any]] = {}
        for raw_symbol, raw_quote in quotes.items():
            symbol = str(raw_symbol)
            quote = dict(raw_quote)
            raw_cumulative = quote.get("cumulative_volume_lots")
            try:
                cumulative = float(raw_cumulative)
            except (TypeError, ValueError):
                cumulative = math.nan
            if not math.isfinite(cumulative) or cumulative < 0.0:
                cumulative = math.nan

            previous = ledger.get(symbol) or {}
            if (previous.get("session_date") == session_date and math.isfinite(cumulative)
                    and previous.get("cumulative_volume_lots") is not None
                    and cumulative < float(previous["cumulative_volume_lots"])):
                # A stale/out-of-order message must not lower the durable
                # baseline and inflate the capacity of a subsequent message.
                quote["minute_volume_lots"] = None
                quote["minute_volume_source"] = "rejected_cumulative_regression"
                prepared[symbol] = quote
                continue
            minute_lots: float | None = None
            baseline = None
            if (
                str(previous.get("session_date") or "") == session_date
                and str(previous.get("minute") or "") == minute_key
            ):
                cached = previous.get("minute_volume_lots")
                if cached is not None:
                    baseline = previous.get("baseline_cumulative_volume_lots")
                    if baseline is None and previous.get("cumulative_volume_lots") is not None:
                        baseline = float(previous["cumulative_volume_lots"]) - float(cached)
                    if baseline is not None and math.isfinite(cumulative) and cumulative >= float(previous.get("cumulative_volume_lots") or 0):
                        minute_lots = cumulative - float(baseline)
            elif math.isfinite(cumulative):
                if minute.timetz().replace(tzinfo=None) == FIRST_MINUTE_EXECUTION_TIME:
                    baseline = 0.0
                    minute_lots = cumulative
                elif str(previous.get("session_date") or "") == session_date:
                    previous_at = _parse_timestamp(previous.get("minute"))
                    previous_cumulative = previous.get("cumulative_volume_lots")
                    if (
                        previous_at is not None
                        and (minute - previous_at).total_seconds() == 60.0
                        and previous_cumulative is not None
                    ):
                        delta = cumulative - float(previous_cumulative)
                        if math.isfinite(delta) and delta >= 0.0:
                            baseline = float(previous_cumulative)
                            minute_lots = delta
            if math.isfinite(cumulative):
                auction_baseline = previous.get("auction_baseline_cumulative_volume_lots") if previous.get("session_date") == session_date else None
                if CLOSING_AUCTION_TIME <= observed.time() < SESSION_CLOSE:
                    auction_baseline = cumulative
                ledger[symbol] = {
                    "session_date": session_date,
                    "minute": minute_key,
                    "cumulative_volume_lots": cumulative,
                    "minute_volume_lots": minute_lots,
                    "baseline_cumulative_volume_lots": baseline,
                    "auction_baseline_cumulative_volume_lots": auction_baseline,
                }
                if "auction_volume_lots" not in quote and auction_baseline is not None and cumulative >= float(auction_baseline):
                    quote["auction_volume_lots"] = cumulative - float(auction_baseline)
            quote["minute_volume_lots"] = minute_lots
            quote["minute_volume_source"] = (
                "adjacent_cumulative_snapshot_delta"
                if minute_lots is not None
                else "unavailable_non_adjacent_or_missing_snapshot"
            )
            prepared[symbol] = quote
        return prepared

    def benchmark_fallback_prices(self) -> dict[str, float]:
        """Return last executable stock marks for the next snapshot fallback."""

        output: dict[str, float] = {}
        benchmarks = self.state.get("benchmarks") or {}
        for benchmark_id, symbol, _label, _security_type in STOCK_BENCHMARKS:
            row = benchmarks.get(benchmark_id) or {}
            price = _finite(row.get("last_mark_price") or row.get("entry_price"))
            output[symbol] = price or 1.0
        return output

    def benchmark_tx_contract(self) -> str | None:
        row = (self.state.get("benchmarks") or {}).get(TX_CONTINUOUS_BENCHMARK_ID) or {}
        code = str(row.get("contract_code") or "").strip().upper()
        return code or None

    @staticmethod
    def _stock_benchmark_fee_rates(
        *,
        symbol: str,
        security_type: str,
        fee_schedule: TaiwanFeeSchedule,
    ) -> tuple[float, float]:
        buy, sell = effective_fee_rate_vectors(
            [symbol],
            "tw_cash",
            fee_schedule=fee_schedule,
            security_types=[security_type],
        )
        return float(buy[0]), float(sell[0])

    @staticmethod
    def _stock_benchmark_order_cost(
        *,
        notional: float,
        commission_rate: float,
        tax_rate: float,
        fee_schedule: TaiwanFeeSchedule,
    ) -> tuple[float, float]:
        commission = float(
            _commission_fees_by_symbol(
                np.asarray([notional], dtype=np.float64),
                np.asarray([commission_rate], dtype=np.float64),
                minimum_commission=float(fee_schedule.minimum_commission),
                rounding=str(fee_schedule.commission_rounding),
            )[0]
        )
        tax = float(
            _tax_fees_by_symbol(
                np.asarray([notional], dtype=np.float64),
                np.asarray([tax_rate], dtype=np.float64),
                rounding=str(fee_schedule.tax_rounding),
            )[0]
        )
        return commission, tax

    def _append_benchmark_mark(
        self,
        benchmark: Mapping[str, Any],
        *,
        now: datetime,
    ) -> None:
        self._append_ledger(
            self.benchmark_marks_path,
            {
                "recorded_at": now.isoformat(timespec="seconds"),
                "minute": now.replace(second=0, microsecond=0).isoformat(
                    timespec="minutes"
                ),
                "session_date": now.date().isoformat(),
                **{
                    key: benchmark.get(key)
                    for key in (
                        "benchmark_id",
                        "label",
                        "instrument_type",
                        "symbol",
                        "logical_code",
                        "contract_code",
                        "initial_capital_twd",
                        "total_equity_twd",
                        "net_pnl_twd",
                        "return_fraction",
                        "return_pct",
                        "daily_return_fraction",
                        "daily_return_pct",
                        "daily_return_basis",
                        "daily_return_previous_close",
                        "daily_return_previous_close_date",
                        "daily_return_previous_close_source",
                        "daily_return_reference_price",
                        "buy_hold_wealth_index",
                        "settled_buy_hold_wealth_index",
                        "performance_pnl_twd",
                        "gross_pnl_twd",
                        "last_mark_price",
                        "last_quote_at",
                        "valuation_stale",
                        "valuation_source",
                        "entry_at",
                        "entry_price",
                        "roll_count",
                        "previous_contract_code",
                        "last_roll_at",
                        "last_roll_old_bid",
                        "last_roll_new_ask",
                        "fixed_fees_twd",
                        "transaction_tax_twd",
                        "total_return_contract",
                        "corporate_action_factor",
                        "adjusted_quantity",
                        "corporate_action_count",
                        "last_corporate_action_date",
                        "corporate_action_coverage",
                        "corporate_action_status",
                        "corporate_action_coverage_end",
                        "current_session_reference_price",
                        "current_session_reference_source",
                        "previous_official_close",
                        "previous_official_close_date",
                        "previous_official_close_source",
                        "return_type",
                        "holding_period",
                        "capital_basis",
                        "benchmark_accounting_contract_version",
                        "tracking_costs_excluded_from_return",
                        "estimated_tracking_cost_twd",
                        "estimated_entry_cost_twd",
                        "estimated_liquidation_cost_twd",
                        "current_contract_notional_twd",
                        "fully_collateralized_account_equity_twd",
                        "collateral_coverage_ratio",
                        "maximum_leverage",
                        "last_roll_funding_flow_twd",
                        "last_margin_top_up_twd",
                        "last_margin_withdrawal_twd",
                        "cumulative_external_funding_flow_twd",
                        "cumulative_margin_top_up_twd",
                        "cumulative_margin_withdrawal_twd",
                    )
                },
            },
        )

    def _load_corporate_actions(self, reference_path: str | Path | None) -> None:
        """Cache the official ex-right/ex-dividend reference by file identity."""

        if reference_path is None:
            return
        path = Path(reference_path).expanduser().resolve()
        self._corporate_action_reference_path = path
        try:
            stat = path.stat()
            summary_path = path.with_suffix(".summary.json")
            summary_stat = summary_path.stat()
            signature = (
                stat.st_dev,
                stat.st_ino,
                stat.st_size,
                stat.st_mtime_ns,
                summary_stat.st_dev,
                summary_stat.st_ino,
                summary_stat.st_size,
                summary_stat.st_mtime_ns,
            )
            if signature == self._corporate_action_cache_signature:
                return
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
            if (
                not isinstance(summary, Mapping)
                or not bool(summary.get("coverage_complete"))
                or int(summary.get("failure_count") or 0) != 0
            ):
                raise ValueError("corporate-action completeness receipt failed")
            coverage_end = date.fromisoformat(str(summary.get("end_date") or ""))
            import polars as pl

            frame = (
                pl.scan_parquet(path)
                .filter(pl.col("symbol").is_in([row[1] for row in STOCK_BENCHMARKS]))
                .select(
                    pl.col("date").cast(pl.Date),
                    pl.col("symbol").cast(pl.String),
                    pl.col("previous_close").cast(pl.Float64),
                    pl.col("reference_price").cast(pl.Float64),
                    pl.col("event_type").cast(pl.String),
                )
                .sort(["symbol", "date"])
                .collect()
            )
            by_symbol: dict[str, list[dict[str, Any]]] = {}
            for item in frame.iter_rows(named=True):
                by_symbol.setdefault(str(item["symbol"]), []).append(dict(item))
            self._corporate_actions_by_symbol = by_symbol
            self._corporate_action_cache_signature = signature
            self._corporate_action_load_error = None
            self._corporate_action_coverage_end = coverage_end
        except Exception as exc:
            self._corporate_actions_by_symbol = {}
            self._corporate_action_cache_signature = None
            self._corporate_action_load_error = f"{type(exc).__name__}: {exc}"
            self._corporate_action_coverage_end = None

    def _stock_total_return_adjustment(
        self,
        *,
        symbol: str,
        entry_at: object,
        mark_date: date,
        current_reference_price: object = None,
        current_reference_source: object = None,
        previous_close: object = None,
        previous_close_date: object = None,
        previous_close_source: object = None,
    ) -> tuple[float | None, list[dict[str, Any]], str]:
        """Return the reinvested-unit factor implied by official reference prices.

        A holder crossing an ex-date receives equivalent economic value when
        units are multiplied by ``previous_close / reference_price``.  The
        official factor handles cash distributions, stock dividends, splits,
        reverse splits, and mixed capital actions without guessing their type.
        """

        parsed_entry = _parse_timestamp(entry_at)
        if parsed_entry is None:
            return None, [], "entry_timestamp_invalid"
        # Both prices in a same-session benchmark are already on the same side
        # of that session's ex-right/ex-dividend boundary.  No distribution or
        # split can be crossed between the opening ask and a later bid, so the
        # exact total-return factor is 1 even while the official daily archive
        # still ends at the preceding completed session.  Cross-session marks
        # continue to fail closed until the reference covers the mark date.
        if parsed_entry.date() == mark_date:
            return 1.0, [], "same_session_no_action_boundary"
        if self._corporate_action_reference_path is None:
            return None, [], "reference_not_configured"
        if self._corporate_action_load_error is not None:
            return None, [], "reference_unavailable"
        if self._corporate_action_coverage_end is None:
            return None, [], "reference_coverage_incomplete"
        factor = 1.0
        applied: list[dict[str, Any]] = []
        for item in self._corporate_actions_by_symbol.get(str(symbol), ()):
            action_date = item.get("date")
            if not isinstance(action_date, date):
                return None, [], "reference_date_invalid"
            # The opening ask on the entry date is already ex-action.  Only
            # actions crossed while holding the benchmark belong in return.
            if not (parsed_entry.date() < action_date <= mark_date):
                continue
            action_previous_close = _finite(item.get("previous_close"))
            action_reference_price = _finite(item.get("reference_price"))
            if (
                action_previous_close is None
                or action_reference_price is None
                or action_previous_close <= 0.0
                or action_reference_price <= 0.0
            ):
                return None, [], "reference_factor_invalid"
            action_factor = action_previous_close / action_reference_price
            factor *= action_factor
            applied.append(
                {
                    "date": action_date.isoformat(),
                    "event_type": item.get("event_type"),
                    "previous_close": action_previous_close,
                    "reference_price": action_reference_price,
                    "factor": action_factor,
                }
            )

        status = "official_reference_complete"
        if self._corporate_action_coverage_end < mark_date:
            try:
                parsed_previous_close_date = (
                    previous_close_date
                    if isinstance(previous_close_date, date)
                    else date.fromisoformat(str(previous_close_date or ""))
                )
            except (TypeError, ValueError):
                return None, [], "current_session_previous_close_date_invalid"
            official_previous_close = _finite(previous_close)
            official_current_reference = _finite(current_reference_price)
            if self._corporate_action_coverage_end != parsed_previous_close_date:
                return None, [], "current_session_previous_close_not_contiguous"
            if parsed_previous_close_date >= mark_date:
                return None, [], "current_session_previous_close_date_invalid"
            if (
                official_previous_close is None
                or official_previous_close <= 0.0
                or official_current_reference is None
                or official_current_reference <= 0.0
            ):
                return None, [], "current_session_reference_invalid"
            current_factor = official_previous_close / official_current_reference
            factor *= current_factor
            if not math.isclose(current_factor, 1.0, rel_tol=0.0, abs_tol=1e-12):
                applied.append(
                    {
                        "date": mark_date.isoformat(),
                        "event_type": "current_session_reference_transition",
                        "previous_close": official_previous_close,
                        "reference_price": official_current_reference,
                        "factor": current_factor,
                        "reference_source": str(current_reference_source or ""),
                        "previous_close_source": str(previous_close_source or ""),
                    }
                )
            status = "official_reference_complete_with_current_session_reference"
        if not math.isfinite(factor) or factor <= 0.0:
            return None, [], "cumulative_factor_invalid"
        return factor, applied, status

    def _mark_stock_benchmark(
        self,
        *,
        benchmark_id: str,
        symbol: str,
        label: str,
        security_type: str,
        quote: Mapping[str, Any],
        fee_schedule: TaiwanFeeSchedule,
        now: datetime,
    ) -> None:
        benchmarks = self.state.setdefault("benchmarks", {})
        row = benchmarks.setdefault(
            benchmark_id,
            {
                "benchmark_id": benchmark_id,
                "label": label,
                "instrument_type": "stock_buy_and_hold",
                "symbol": symbol,
                "quantity": 1_000,
                "fixed_fees_twd": 0.0,
                "transaction_tax_twd": 0.0,
                "valuation_stale": True,
            },
        )
        row["label"] = label
        row["return_type"] = "total_return"
        row["holding_period"] = "cross_session_buy_and_hold"
        row["daily_return_basis"] = DAILY_RETURN_BASIS_PREVIOUS_CLOSE
        row["benchmark_accounting_contract_version"] = (
            MARKET_BENCHMARK_ACCOUNTING_CONTRACT_VERSION
        )
        row["tracking_costs_excluded_from_return"] = True
        buy_rate, sell_rate = self._stock_benchmark_fee_rates(
            symbol=symbol,
            security_type=security_type,
            fee_schedule=fee_schedule,
        )
        ask = _finite(quote.get("ask"))
        bid = _finite(quote.get("bid"))
        quantity = int(row.get("quantity") or 1_000)
        if row.get("entry_price") is None:
            entry_reference = positive_finite(quote.get("reference_price"))
            if entry_reference is None:
                entry_reference = positive_finite(quote.get("previous_close"))
            if entry_reference is None:
                entry_reference = positive_finite(ask)
            previous_close_date = _date_value(quote.get("previous_close_date"))
        else:
            entry_reference = None
            previous_close_date = None
        if row.get("entry_price") is None and entry_reference is not None:
            entry_notional = quantity * entry_reference
            entry_commission, _entry_tax = self._stock_benchmark_order_cost(
                notional=entry_notional,
                commission_rate=buy_rate,
                tax_rate=0.0,
                fee_schedule=fee_schedule,
            )
            row.update(
                {
                    "entry_price": entry_reference,
                    "entry_at": (
                        datetime.combine(
                            previous_close_date,
                            SESSION_CLOSE,
                            tzinfo=TAIPEI,
                        ).isoformat(timespec="seconds")
                        if previous_close_date is not None
                        else now.isoformat(timespec="seconds")
                    ),
                    "initial_capital_twd": entry_notional,
                    "fixed_fees_twd": 0.0,
                    "estimated_entry_cost_twd": entry_commission,
                    "capital_basis": "one_board_lot_entry_notional",
                }
            )
        entry = _finite(row.get("entry_price"))
        adjustment_factor, corporate_actions, action_status = (
            self._stock_total_return_adjustment(
                symbol=symbol,
                entry_at=row.get("entry_at"),
                mark_date=now.date(),
                current_reference_price=quote.get("reference_price"),
                current_reference_source=quote.get("reference_price_source")
                or quote.get("source"),
                previous_close=quote.get("previous_close"),
                previous_close_date=quote.get("previous_close_date"),
                previous_close_source=quote.get("previous_close_source"),
            )
        )
        action_coverage_complete = action_status in {
            "official_reference_complete",
            "official_reference_complete_with_current_session_reference",
            "same_session_no_action_boundary",
        }
        effective_coverage_end = (
            now.date()
            if action_status
            == "official_reference_complete_with_current_session_reference"
            else self._corporate_action_coverage_end
        )
        row.update(
            {
                "total_return_contract": ("official_ex_date_reference_reinvestment_v1"),
                "corporate_action_coverage": action_coverage_complete,
                "corporate_action_status": action_status,
                "corporate_action_coverage_end": (
                    effective_coverage_end.isoformat()
                    if effective_coverage_end is not None
                    else None
                ),
                "current_session_reference_price": _finite(
                    quote.get("reference_price")
                ),
                "current_session_reference_source": quote.get("reference_price_source")
                or quote.get("source"),
                "previous_official_close": _finite(quote.get("previous_close")),
                "previous_official_close_date": quote.get("previous_close_date"),
                "previous_official_close_source": quote.get("previous_close_source"),
            }
        )
        if entry is not None and bid is not None and adjustment_factor is not None:
            adjusted_quantity = quantity * adjustment_factor
            gross_value = adjusted_quantity * bid
            entry_notional = quantity * entry
            gross_pnl = gross_value - entry_notional
            liquidation_notional = gross_value
            liquidation_commission, liquidation_tax = self._stock_benchmark_order_cost(
                notional=liquidation_notional,
                commission_rate=buy_rate,
                tax_rate=max(0.0, sell_rate - buy_rate),
                fee_schedule=fee_schedule,
            )
            liquidation_cost = liquidation_commission + liquidation_tax
            estimated_entry_cost = float(
                row.get("estimated_entry_cost_twd")
                or row.get("fixed_fees_twd")
                or 0.0
            )
            estimated_tracking_cost = estimated_entry_cost + liquidation_cost
            initial_capital = float(row.get("initial_capital_twd") or 0.0)
            return_fraction = (
                gross_pnl / initial_capital if initial_capital > 0.0 else None
            )
            daily_reference = positive_finite(quote.get("reference_price"))
            if daily_reference is None:
                daily_reference = positive_finite(quote.get("previous_close"))
            daily_return_fraction, daily_return_pct = previous_close_return(
                bid,
                daily_reference,
            )
            row.update(
                {
                    "last_mark_price": bid,
                    "last_quote_at": quote.get("quote_at"),
                    "last_mark_at": now.isoformat(timespec="seconds"),
                    "fixed_fees_twd": 0.0,
                    "transaction_tax_twd": 0.0,
                    "liquidation_cost_twd": 0.0,
                    "estimated_entry_cost_twd": estimated_entry_cost,
                    "estimated_liquidation_cost_twd": liquidation_cost,
                    "estimated_tracking_cost_twd": estimated_tracking_cost,
                    "performance_pnl_twd": gross_pnl,
                    "gross_pnl_twd": gross_pnl,
                    "net_pnl_twd": gross_pnl,
                    "total_equity_twd": initial_capital + gross_pnl,
                    "return_fraction": return_fraction,
                    "return_pct": None
                    if return_fraction is None
                    else return_fraction * 100.0,
                    "buy_hold_wealth_index": (
                        None if return_fraction is None else 1.0 + return_fraction
                    ),
                    "daily_return_fraction": daily_return_fraction,
                    "daily_return_pct": daily_return_pct,
                    "daily_return_previous_close": _finite(
                        quote.get("previous_close")
                    ),
                    "daily_return_previous_close_date": quote.get(
                        "previous_close_date"
                    ),
                    "daily_return_previous_close_source": quote.get(
                        "previous_close_source"
                    ),
                    "daily_return_reference_price": daily_reference,
                    "corporate_action_factor": adjustment_factor,
                    "adjusted_quantity": adjusted_quantity,
                    "corporate_action_count": len(corporate_actions),
                    "last_corporate_action_date": (
                        corporate_actions[-1]["date"] if corporate_actions else None
                    ),
                    "applied_corporate_actions": corporate_actions,
                    "valuation_stale": False,
                    "valuation_source": (
                        "gross_total_return_buy_and_hold_units_at_best_bid"
                    ),
                    "source": quote.get("source"),
                }
            )
        elif adjustment_factor is None:
            row["valuation_stale"] = True
            row["valuation_source"] = (
                "corporate_action_reference_unavailable_fail_closed"
            )
        elif row.get("total_equity_twd") is not None:
            row["valuation_stale"] = True
            row["valuation_source"] = "carried_forward_last_complete_mark"
        self._append_benchmark_mark(row, now=now)

    @staticmethod
    def _tx_trade_tax(price: float, *, trading_date: date) -> float:
        return taifex_tax_per_contract_twd(
            price,
            multiplier_twd_per_point=TAIFEX_INDEX_FUTURES_MULTIPLIERS["TX"],
            tax_rate=stock_index_futures_tax_rate(trading_date),
        )

    def _mark_tx_continuous_benchmark(
        self,
        *,
        current_contract_code: str | None,
        current_quote: Mapping[str, Any],
        previous_contract_quote: Mapping[str, Any],
        now: datetime,
    ) -> None:
        benchmarks = self.state.setdefault("benchmarks", {})
        row = benchmarks.setdefault(
            TX_CONTINUOUS_BENCHMARK_ID,
            {
                "benchmark_id": TX_CONTINUOUS_BENCHMARK_ID,
                "label": "台指期無限轉倉（大台一口）",
                "instrument_type": "continuous_long_future",
                "logical_code": TX_CONTINUOUS_LOGICAL_CODE,
                "multiplier_twd_per_point": TAIFEX_INDEX_FUTURES_MULTIPLIERS["TX"],
                "realized_gross_pnl_twd": 0.0,
                "fixed_fees_twd": 0.0,
                "transaction_tax_twd": 0.0,
                "roll_count": 0,
                "roll_contract_version": TX_CONTINUOUS_ROLL_CONTRACT_VERSION,
                "settled_buy_hold_wealth_index": 1.0,
                "cumulative_external_funding_flow_twd": 0.0,
                "cumulative_margin_top_up_twd": 0.0,
                "cumulative_margin_withdrawal_twd": 0.0,
                "valuation_stale": True,
            },
        )
        row["roll_contract_version"] = TX_CONTINUOUS_ROLL_CONTRACT_VERSION
        row["official_final_settlement_path"] = str(self.final_settlement_path)
        row["return_type"] = "gross_buy_and_hold_time_weighted"
        row["holding_period"] = "cross_session_buy_and_hold_with_continuous_roll"
        row["daily_return_basis"] = DAILY_RETURN_BASIS_PREVIOUS_CLOSE
        row["capital_basis"] = TX_FULLY_COLLATERALIZED_CAPITAL_BASIS
        row["benchmark_accounting_contract_version"] = (
            MARKET_BENCHMARK_ACCOUNTING_CONTRACT_VERSION
        )
        row["tracking_costs_excluded_from_return"] = True
        row["maximum_leverage"] = 1.0
        current_code = str(current_contract_code or "").strip().upper()
        held_code = str(row.get("contract_code") or "").strip().upper()
        current_ask = _finite(current_quote.get("ask"))
        current_bid = _finite(current_quote.get("bid"))
        fee_per_side = TAIFEX_INDEX_FUTURES_FEE_PER_SIDE_TWD["TX"]
        multiplier = TAIFEX_INDEX_FUTURES_MULTIPLIERS["TX"]

        def update_contract_identity(
            target: dict[str, Any], quote: Mapping[str, Any]
        ) -> None:
            delivery_month = str(quote.get("delivery_month") or "").strip()
            delivery_date = _date_value(
                quote.get("last_trading_date") or quote.get("delivery_date")
            )
            if delivery_month:
                target["contract_delivery_month"] = delivery_month
            if delivery_date is not None:
                target["contract_last_trading_date"] = delivery_date.isoformat()

        if row.get("entry_price") is None and current_code and current_ask is not None:
            entry_reference = positive_finite(current_quote.get("previous_close"))
            previous_close_date = _date_value(
                current_quote.get("previous_close_date")
            )
            if entry_reference is None:
                entry_reference = current_ask
            full_notional = fully_collateralized_futures_notional(
                entry_reference,
                multiplier,
            )
            assert full_notional is not None
            entry_tax = self._tx_trade_tax(entry_reference, trading_date=now.date())
            entry_at = (
                datetime.combine(
                    previous_close_date,
                    time(13, 45),
                    tzinfo=TAIPEI,
                )
                if previous_close_date is not None
                else now
            )
            row.update(
                {
                    "contract_code": current_code,
                    "entry_price": entry_reference,
                    "entry_at": entry_at.isoformat(timespec="seconds"),
                    "origin_entry_price": entry_reference,
                    "origin_entry_at": entry_at.isoformat(timespec="seconds"),
                    "current_contract_entry_price": entry_reference,
                    "current_contract_entry_at": entry_at.isoformat(timespec="seconds"),
                    "initial_capital_twd": full_notional,
                    "official_initial_margin_at_origin_twd": (
                        taifex_initial_margin_twd("TX", now.date())
                    ),
                    "fixed_fees_twd": 0.0,
                    "transaction_tax_twd": 0.0,
                    "estimated_fixed_fees_twd": fee_per_side,
                    "estimated_transaction_tax_twd": entry_tax,
                    "estimated_tracking_cost_twd": fee_per_side + entry_tax,
                    "settled_buy_hold_wealth_index": 1.0,
                    "buy_hold_wealth_index": 1.0,
                    "cumulative_external_funding_flow_twd": 0.0,
                    "cumulative_margin_top_up_twd": 0.0,
                    "cumulative_margin_withdrawal_twd": 0.0,
                    "current_contract_notional_twd": full_notional,
                    "fully_collateralized_account_equity_twd": full_notional,
                    "collateral_coverage_ratio": 1.0,
                    "capital_basis": TX_FULLY_COLLATERALIZED_CAPITAL_BASIS,
                }
            )
            update_contract_identity(row, current_quote)
            held_code = current_code

        if row.get("entry_price") is not None:
            row.setdefault("origin_entry_price", row.get("entry_price"))
            row.setdefault("origin_entry_at", row.get("entry_at"))
            row["current_contract_entry_price"] = row.get("entry_price")
            row.setdefault("current_contract_entry_at", row.get("entry_at"))

        if held_code == current_code:
            update_contract_identity(row, current_quote)
        elif held_code:
            update_contract_identity(row, previous_contract_quote)

        if (
            row.get("entry_price") is not None
            and current_code
            and held_code != current_code
        ):
            old_bid = _finite(previous_contract_quote.get("bid"))
            held_last_trading_date = _date_value(row.get("contract_last_trading_date"))
            held_delivery_month = str(row.get("contract_delivery_month") or "").strip()
            expired = held_last_trading_date is not None and (
                now.date() > held_last_trading_date
                or (
                    now.date() == held_last_trading_date and now.time() >= SESSION_CLOSE
                )
            )
            official_settlement = (
                self._official_tx_final_settlement(
                    delivery_date=held_last_trading_date,
                    delivery_month=held_delivery_month,
                )
                if expired and held_last_trading_date is not None
                else None
            )
            old_exit_price = (
                float(official_settlement["price"])
                if official_settlement is not None
                else old_bid
                if not expired
                else None
            )
            new_previous_close = positive_finite(current_quote.get("previous_close"))
            if (
                old_exit_price is not None
                and current_ask is not None
                and new_previous_close is not None
            ):
                entry = float(row["entry_price"])
                settled_wealth = positive_finite(row.get("buy_hold_wealth_index"))
                if settled_wealth is None:
                    row["valuation_stale"] = True
                    row["roll_blocked_reason"] = "invalid_roll_wealth_inputs"
                    row["valuation_source"] = "roll_accounting_fail_closed"
                    self._append_benchmark_mark(row, now=now)
                    return
                old_funding_basis = positive_finite(row.get("last_mark_price"))
                if old_funding_basis is None:
                    old_funding_basis = old_exit_price
                roll_funding_flow = (
                    new_previous_close - old_funding_basis
                ) * multiplier
                margin_top_up = max(0.0, roll_funding_flow)
                margin_withdrawal = max(0.0, -roll_funding_flow)
                row["realized_gross_pnl_twd"] = (
                    float(row.get("realized_gross_pnl_twd") or 0.0)
                    + (old_exit_price - entry) * multiplier
                )
                old_exit_fee = 0.0 if official_settlement is not None else fee_per_side
                row["estimated_fixed_fees_twd"] = (
                    float(row.get("estimated_fixed_fees_twd") or 0.0)
                    + old_exit_fee
                    + fee_per_side
                )
                row["estimated_transaction_tax_twd"] = (
                    float(row.get("estimated_transaction_tax_twd") or 0.0)
                    + self._tx_trade_tax(
                        old_exit_price,
                        trading_date=held_last_trading_date or now.date(),
                    )
                    + self._tx_trade_tax(current_ask, trading_date=now.date())
                )
                row.update(
                    {
                        "previous_contract_code": held_code,
                        "contract_code": current_code,
                        "entry_price": new_previous_close,
                        "current_contract_entry_price": new_previous_close,
                        "current_contract_entry_at": (
                            datetime.combine(
                                _date_value(current_quote.get("previous_close_date"))
                                or now.date(),
                                time(13, 45),
                                tzinfo=TAIPEI,
                            ).isoformat(timespec="seconds")
                        ),
                        "last_roll_at": now.isoformat(timespec="seconds"),
                        "last_roll_old_bid": (
                            old_bid if official_settlement is None else None
                        ),
                        "last_roll_old_price": old_exit_price,
                        "last_roll_old_price_source": (
                            "official_taifex_index_final_settlement"
                            if official_settlement is not None
                            else "executable_old_contract_bid"
                        ),
                        "last_roll_official_final_settlement": (
                            official_settlement["price"]
                            if official_settlement is not None
                            else None
                        ),
                        "last_roll_official_settlement_source_file": (
                            official_settlement["source_file"]
                            if official_settlement is not None
                            else None
                        ),
                        "last_roll_official_settlement_source_sha256": (
                            official_settlement["source_sha256"]
                            if official_settlement is not None
                            else None
                        ),
                        "last_roll_new_ask": current_ask,
                        "last_roll_new_previous_close": new_previous_close,
                        "settled_buy_hold_wealth_index": settled_wealth,
                        "last_roll_funding_flow_twd": roll_funding_flow,
                        "last_margin_top_up_twd": margin_top_up,
                        "last_margin_withdrawal_twd": margin_withdrawal,
                        "cumulative_external_funding_flow_twd": (
                            float(
                                row.get("cumulative_external_funding_flow_twd")
                                or 0.0
                            )
                            + roll_funding_flow
                        ),
                        "cumulative_margin_top_up_twd": (
                            float(row.get("cumulative_margin_top_up_twd") or 0.0)
                            + margin_top_up
                        ),
                        "cumulative_margin_withdrawal_twd": (
                            float(
                                row.get("cumulative_margin_withdrawal_twd") or 0.0
                            )
                            + margin_withdrawal
                        ),
                        "roll_count": int(row.get("roll_count") or 0) + 1,
                    }
                )
                row["estimated_tracking_cost_twd"] = float(
                    row.get("estimated_fixed_fees_twd") or 0.0
                ) + float(row.get("estimated_transaction_tax_twd") or 0.0)
                row["fixed_fees_twd"] = 0.0
                row["transaction_tax_twd"] = 0.0
                update_contract_identity(row, current_quote)
                roll_history = list(row.get("roll_history") or ())
                roll_history.append(
                    {
                        "rolled_at": now.isoformat(timespec="seconds"),
                        "from_contract": held_code,
                        "to_contract": current_code,
                        "old_bid": old_bid if official_settlement is None else None,
                        "old_exit_price": old_exit_price,
                        "old_exit_price_source": (
                            "official_taifex_index_final_settlement"
                            if official_settlement is not None
                            else "executable_old_contract_bid"
                        ),
                        "official_final_settlement": official_settlement,
                        "new_ask": current_ask,
                        "new_previous_close": new_previous_close,
                        "old_exit_fee_twd": old_exit_fee,
                        "new_entry_fee_twd": fee_per_side,
                        "funding_flow_twd": roll_funding_flow,
                        "margin_top_up_twd": margin_top_up,
                        "margin_withdrawal_twd": margin_withdrawal,
                    }
                )
                row["roll_history"] = roll_history[-100:]
                row["roll_blocked_reason"] = None
                self._append_ledger(
                    self.events_path,
                    {
                        "event": "benchmark_tx_continuous_rolled",
                        "recorded_at": now.isoformat(timespec="seconds"),
                        "from_contract": held_code,
                        "to_contract": current_code,
                        "old_exit_price": old_exit_price,
                        "old_exit_price_source": row["last_roll_old_price_source"],
                        "new_entry_ask": current_ask,
                        "official_final_settlement": official_settlement,
                        "realized_gross_pnl_twd": row["realized_gross_pnl_twd"],
                        "settled_buy_hold_wealth_index": settled_wealth,
                        "funding_flow_twd": roll_funding_flow,
                        "margin_top_up_twd": margin_top_up,
                        "margin_withdrawal_twd": margin_withdrawal,
                        "roll_contract_version": (TX_CONTINUOUS_ROLL_CONTRACT_VERSION),
                    },
                )
                held_code = current_code
            else:
                row["valuation_stale"] = True
                if expired and official_settlement is None:
                    row["roll_blocked_reason"] = self._tx_final_settlement_error
                    row["valuation_source"] = (
                        "roll_waiting_for_official_final_settlement_and_new_ask"
                    )
                else:
                    if old_bid is None:
                        row["roll_blocked_reason"] = "missing_old_bid"
                    elif current_ask is None:
                        row["roll_blocked_reason"] = "missing_new_ask"
                    else:
                        row["roll_blocked_reason"] = "missing_new_contract_previous_close"
                    row["valuation_source"] = "roll_waiting_for_old_bid_and_new_ask"
                self._append_benchmark_mark(row, now=now)
                return

        if (
            row.get("entry_price") is not None
            and held_code == current_code
            and current_bid is not None
        ):
            settled_wealth = positive_finite(
                row.get("settled_buy_hold_wealth_index") or 1.0
            )
            wealth_index = current_roll_linked_wealth(
                settled_wealth,
                current_bid,
                row.get("entry_price"),
            )
            if wealth_index is None:
                row["valuation_stale"] = True
                row["valuation_source"] = "buy_hold_wealth_accounting_fail_closed"
                self._append_benchmark_mark(row, now=now)
                return
            liquidation_fee = fee_per_side
            liquidation_tax = self._tx_trade_tax(current_bid, trading_date=now.date())
            initial_capital = fully_collateralized_futures_notional(
                row.get("origin_entry_price") or row.get("entry_price"),
                multiplier,
            )
            assert initial_capital is not None
            return_fraction = wealth_index - 1.0
            performance_pnl = initial_capital * return_fraction
            current_notional = fully_collateralized_futures_notional(
                current_bid,
                multiplier,
            )
            assert current_notional is not None
            daily_return_fraction, daily_return_pct = previous_close_return(
                current_bid,
                current_quote.get("previous_close"),
            )
            estimated_incurred_cost = float(
                row.get("estimated_fixed_fees_twd") or 0.0
            ) + float(row.get("estimated_transaction_tax_twd") or 0.0)
            row.update(
                {
                    "last_mark_price": current_bid,
                    "last_quote_at": current_quote.get("quote_at"),
                    "last_mark_at": now.isoformat(timespec="seconds"),
                    "initial_capital_twd": initial_capital,
                    "fixed_fees_twd": 0.0,
                    "transaction_tax_twd": 0.0,
                    "liquidation_cost_twd": 0.0,
                    "estimated_liquidation_cost_twd": (
                        liquidation_fee + liquidation_tax
                    ),
                    "estimated_tracking_cost_twd": (
                        estimated_incurred_cost + liquidation_fee + liquidation_tax
                    ),
                    "buy_hold_wealth_index": wealth_index,
                    "performance_pnl_twd": performance_pnl,
                    "gross_pnl_twd": performance_pnl,
                    "net_pnl_twd": performance_pnl,
                    "total_equity_twd": initial_capital * wealth_index,
                    "return_fraction": return_fraction,
                    "return_pct": return_fraction * 100.0,
                    "daily_return_fraction": daily_return_fraction,
                    "daily_return_pct": daily_return_pct,
                    "daily_return_previous_close": positive_finite(
                        current_quote.get("previous_close")
                    ),
                    "daily_return_reference_price": positive_finite(
                        current_quote.get("previous_close")
                    ),
                    "daily_return_previous_close_date": current_quote.get(
                        "previous_close_date"
                    ),
                    "daily_return_previous_close_source": current_quote.get(
                        "previous_close_source"
                    ),
                    "current_contract_notional_twd": current_notional,
                    "fully_collateralized_account_equity_twd": current_notional,
                    "collateral_coverage_ratio": 1.0,
                    "maximum_leverage": 1.0,
                    "valuation_stale": False,
                    "valuation_source": (
                        "gross_fully_collateralized_1x_tx_buy_and_hold_at_bid_"
                        "with_close_to_close_roll_linking"
                    ),
                    "source": current_quote.get("source"),
                }
            )
        elif row.get("total_equity_twd") is not None:
            row["valuation_stale"] = True
            row["valuation_source"] = "carried_forward_last_complete_mark"
        self._append_benchmark_mark(row, now=now)

    def process_benchmarks(
        self,
        *,
        stock_quotes: Mapping[str, Mapping[str, Any]],
        stock_fee_schedule: TaiwanFeeSchedule,
        current_future_contract_code: str | None,
        current_future_quote: Mapping[str, Any] | None,
        previous_future_quote: Mapping[str, Any] | None = None,
        corporate_action_reference_path: str | Path | None = None,
        now: datetime | None = None,
    ) -> None:
        """Advance three independent, executable-price comparison ledgers."""

        observed = _now_taipei(now)
        self._load_corporate_actions(corporate_action_reference_path)
        for benchmark_id, symbol, label, security_type in STOCK_BENCHMARKS:
            self._mark_stock_benchmark(
                benchmark_id=benchmark_id,
                symbol=symbol,
                label=label,
                security_type=security_type,
                quote=stock_quotes.get(symbol) or {},
                fee_schedule=stock_fee_schedule,
                now=observed,
            )
        self._mark_tx_continuous_benchmark(
            current_contract_code=current_future_contract_code,
            current_quote=current_future_quote or {},
            previous_contract_quote=previous_future_quote or {},
            now=observed,
        )
        self._persist(observed)

    def _mode(self, spec: ModeSpec) -> dict[str, Any]:
        mode = self.state.setdefault("modes", {}).setdefault(spec.market, {})
        mode["market"] = spec.market
        mode["label"] = spec.label
        mode.setdefault("initial_capital_twd", float(spec.initial_capital_twd))
        mode.setdefault("cumulative_realized_net_pnl_twd", 0.0)
        mode.setdefault("cumulative_commission_rebate_accrued_twd", 0.0)
        mode.setdefault("open_net_liquidation_pnl_twd", 0.0)
        mode.setdefault("total_equity_twd", float(spec.initial_capital_twd))
        mode.setdefault("open_position_count", 0)
        mode.setdefault("stale_position_count", 0)
        mode.setdefault("force_exit_failures", 0)
        mode.setdefault("positions", {})
        mode.setdefault("processed_signal_ids", [])
        mode["config_path"] = spec.config_path
        mode["checkpoint_path"] = spec.checkpoint_path
        mode["live_output_dir"] = str(spec.live_output_dir)
        mode["signal_market"] = spec.signal_market or spec.market
        mode["executed_positions_path"] = str(self.positions_path)
        mode["price_limit_offset_ticks"] = int(spec.price_limit_offset_ticks)
        mode["bracket_price_policy"] = (
            "inside_daily_limits_by_ticks"
            if int(spec.price_limit_offset_ticks) > 0
            else "full_daily_limits"
        )
        # Keep the policy that actually produced an already committed session.
        # The runtime spec describes the next live registration attempt; using
        # it to rewrite a completed counterfactual replay makes state and the
        # append-only fill ledger disagree immediately after a service restart.
        mode["configured_entry_fill_policy"] = spec.entry_fill_policy
        mode["configured_intraday_contract"] = STRICT_INTRADAY_CONTRACT if spec.strict_intraday else None
        if mode.get("entry_completed_at"):
            if not mode.get("pending_signal_id"):
                mode.pop("missing_carried_open_symbols", None)
            committed_policies = {
                str(position.get("entry_fill_policy") or "")
                for position in (mode.get("positions") or {}).values()
                if isinstance(position, Mapping)
                and str(position.get("entry_fill_policy") or "")
            }
            if len(committed_policies) == 1:
                mode["entry_fill_policy"] = committed_policies.pop()
            elif mode.get("counterfactual_0901_price_fill") is True:
                mode["entry_fill_policy"] = ENTRY_FILL_POLICY_0901_MINUTE_PRICE
            elif mode.get("counterfactual_open_price_fill") is True:
                mode["entry_fill_policy"] = ENTRY_FILL_POLICY_OFFICIAL_OPEN_AT_0901
        else:
            mode["entry_fill_policy"] = spec.entry_fill_policy
            mode["entry_price_offset_ticks"] = int(spec.entry_price_offset_ticks)
            mode["entry_fill_is_synthetic"] = (
                spec.entry_fill_policy == ENTRY_FILL_POLICY_SYNTHETIC_OPEN_TICK
            )
            mode["paper_fill_deterministic"] = spec.entry_fill_policy in {
                ENTRY_FILL_POLICY_MARKET_AT_BEST_ELSE_OPEN_TICK,
                ENTRY_FILL_POLICY_OFFICIAL_OPEN_AT_0901,
            }
            mode["fill_guaranteed"] = bool(
                mode["entry_fill_is_synthetic"]
                or mode["paper_fill_deterministic"]
            )
        mode["exchange_fill_guaranteed"] = False
        return mode

    def update_readiness(
        self,
        specs: Sequence[ModeSpec],
        *,
        now: datetime | None = None,
        errors: Mapping[str, str] | None = None,
        current_eligibility_coverage: Mapping[str, Mapping[str, Any]] | None = None,
    ) -> None:
        observed = _now_taipei(now)
        enabled_markets = [str(spec.market) for spec in specs]
        self.state["enabled_markets"] = enabled_markets
        enabled = set(enabled_markets)
        for market, existing in (self.state.get("modes") or {}).items():
            if isinstance(existing, dict):
                existing["configured_enabled"] = str(market) in enabled
        for spec in specs:
            mode = self._mode(spec)
            mode["configured_enabled"] = True
            checkpoint = Path(spec.checkpoint_path) if spec.checkpoint_path else None
            checkpoint_ready = bool(checkpoint and checkpoint.is_file())
            mode["checkpoint_ready"] = checkpoint_ready
            divergence = mode.get("ledger_state_divergence")
            mode["readiness_error"] = (
                "ledger_state_divergence:"
                f"{divergence.get('kind')}:{divergence.get('signal_id')}"
                if isinstance(divergence, Mapping)
                else (errors or {}).get(spec.market)
            )
            if current_eligibility_coverage is not None:
                mode["current_eligibility_coverage"] = dict(
                    current_eligibility_coverage.get(spec.market) or {}
                )
            has_open_position = bool(mode.get("positions")) and any(
                int(item.get("signed_shares") or 0) != 0
                for item in mode["positions"].values()
            )
            if isinstance(divergence, Mapping):
                mode["engine_status"] = "critical_ledger_state_divergence"
            elif (mode.get("margin_corporate_action_receipt") or {}).get("status") == "blocked":
                mode["engine_status"] = "waiting_margin_corporate_action"
            elif bool(mode.get("legacy_execution_contract")) and has_open_position:
                mode["engine_status"] = (
                    "critical_legacy_position_requires_reconciliation"
                )
            elif has_open_position:
                mode["engine_status"] = (
                    "margin_carried_waiting_next_signal"
                    if mode.get("margin_carry_contract") == MARGIN_CARRY_CONTRACT and (
                        str(mode.get("session_date") or "") != observed.date().isoformat()
                        or observed.time() >= SESSION_CLOSE)
                    else
                    "critical_unflattened_after_13_24"
                    if observed.timetz().replace(tzinfo=None) >= FORCE_EXIT_TIME
                    else "active"
                )
            elif (
                mode.get("session_valid") is False
                and str(mode.get("session_date") or "") == observed.date().isoformat()
                and mode.get("non_session_invalidated_at")
            ):
                mode["engine_status"] = "invalid_non_trading_session"
            elif not checkpoint_ready:
                mode["engine_status"] = "blocked_missing_checkpoint"
            elif str(
                mode.get("session_date") or ""
            ) == observed.date().isoformat() and mode.get("entry_completed_at"):
                if mode.get("positions"):
                    mode["engine_status"] = "session_flat_after_exit"
                else:
                    mode["engine_status"] = "flat_no_executable_signal"
            elif observed.weekday() >= 5:
                mode["engine_status"] = "waiting_trading_day"
            elif observed.timetz().replace(tzinfo=None) < LIVE_ENTRY_GATE:
                mode["engine_status"] = "waiting_09_00_signal"
            elif observed.timetz().replace(tzinfo=None) >= SESSION_CLOSE:
                mode["engine_status"] = "session_complete"
            else:
                mode["engine_status"] = "waiting_signal"
            if not divergence and not mode.get("readiness_error") and (mode.get("margin_corporate_action_receipt") or {}).get("status") != "blocked":
                strict_status = self._intraday_status(mode, observed)
                if strict_status:
                    mode["engine_status"] = strict_status
        self._persist(observed)

    def rearm_flat_session(
        self,
        market: str,
        *,
        now: datetime | None = None,
        reason: str,
    ) -> str:
        """Allow one replacement signal only when the session never filled.

        This is an operational recovery path for a signal that was consumed
        while an exact-session prerequisite was unavailable.  It deliberately
        preserves processed signal IDs and the append-only signal ledger, and
        refuses to rearm after any position or fill so it cannot become a
        same-day double-entry bypass.
        """

        observed = _now_taipei(now)
        wall_time = observed.timetz().replace(tzinfo=None)
        if wall_time < LIVE_ENTRY_GATE or wall_time >= EXIT_LIMIT_TIME:
            raise RuntimeError("flat-session rearm is outside the entry window")
        normalized_reason = str(reason or "").strip()
        if not normalized_reason:
            raise ValueError("flat-session rearm requires an audit reason")
        mode = (self.state.get("modes") or {}).get(str(market))
        if not isinstance(mode, dict):
            raise KeyError(f"unknown simulation market: {market}")
        session_date = observed.date().isoformat()
        if str(mode.get("session_date") or "") != session_date:
            raise RuntimeError(
                f"{market} has no consumed signal for current session {session_date}"
            )
        if not mode.get("entry_completed_at"):
            return "already_armed"
        if mode.get("positions"):
            raise RuntimeError(f"{market} has position history and cannot be rearmed")
        if self._session_has_fill(str(market), session_date):
            raise RuntimeError(f"{market} has fills and cannot be rearmed")

        previous_signal_id = mode.get("signal_id")
        previous_entry_completed_at = mode.get("entry_completed_at")
        previous_signal_counts = dict(mode.get("signal_counts") or {})
        mode["entry_completed_at"] = None
        mode["pending_signal_id"] = None
        mode["pending_signal_at"] = None
        mode["exit_limit_submitted_at"] = None
        mode["force_exit_started_at"] = None
        mode["closing_auction_submitted_at"] = None
        mode["closing_auction_settled_at"] = None
        mode["residual_conversion_completed_at"] = None
        mode["engine_status"] = "waiting_signal"
        mode["blocked_reason"] = None
        mode["signal_counts"] = {}
        mode["signal_reason_counts"] = {}
        mode["entry_fill_count"] = 0
        mode["entry_requested_shares"] = 0
        mode["entry_filled_shares"] = 0
        mode["entry_unfilled_shares"] = 0
        mode["entry_fill_outcome"] = "pending"
        mode["rearmed_at"] = observed.isoformat(timespec="seconds")
        mode["rearm_reason"] = normalized_reason
        mode["rearm_count"] = int(mode.get("rearm_count") or 0) + 1
        self._event(
            "flat_session_rearmed",
            recorded_at=observed,
            market=market,
            session_date=session_date,
            reason=normalized_reason,
            previous_signal_id=previous_signal_id,
            previous_entry_completed_at=previous_entry_completed_at,
            previous_signal_counts=previous_signal_counts,
        )
        self._persist(observed)
        return "rearmed"

    def invalidate_non_session_flat_signal(
        self,
        market: str,
        *,
        now: datetime | None = None,
        reason: str,
    ) -> str:
        """Void an impossible non-session signal without deleting its audit trail."""

        observed = _now_taipei(now)
        normalized_reason = str(reason or "").strip()
        if not normalized_reason:
            raise ValueError("non-session invalidation requires an audit reason")
        mode = (self.state.get("modes") or {}).get(str(market))
        if not isinstance(mode, dict):
            raise KeyError(f"unknown simulation market: {market}")
        session_date = str(mode.get("session_date") or "")
        if session_date != observed.date().isoformat():
            return "no_current_session_signal"
        if mode.get("non_session_invalidated_at"):
            return "already_invalidated"
        if mode.get("positions"):
            raise RuntimeError(
                f"{market} has positions; non-session signal cannot be auto-invalidated"
            )
        if self._session_has_fill(str(market), session_date):
            raise RuntimeError(
                f"{market} has fills; non-session signal cannot be auto-invalidated"
            )
        previous_signal_id = mode.get("signal_id")
        previous_entry_completed_at = mode.get("entry_completed_at")
        mode["entry_completed_at"] = None
        mode["pending_signal_id"] = None
        mode["pending_signal_at"] = None
        mode["engine_status"] = "invalid_non_trading_session"
        mode["blocked_reason"] = "non_trading_session"
        mode["session_valid"] = False
        mode["non_session_invalidated_at"] = observed.isoformat(timespec="seconds")
        mode["non_session_invalidation_reason"] = normalized_reason
        self._event(
            "non_session_signal_invalidated",
            recorded_at=observed,
            market=market,
            session_date=session_date,
            signal_id=previous_signal_id,
            reason=normalized_reason,
            previous_entry_completed_at=previous_entry_completed_at,
            positions=0,
            fills=0,
        )
        self._persist(observed)
        return "invalidated"

    def retire_flat_mode(
        self,
        market: str,
        *,
        now: datetime | None = None,
        reason: str,
    ) -> str:
        """Remove a mistakenly configured flat mode while preserving its logs."""

        observed = _now_taipei(now)
        normalized_market = str(market or "").strip()
        normalized_reason = str(reason or "").strip()
        if not normalized_market or not normalized_reason:
            raise ValueError("retiring a simulation mode requires market and reason")
        modes = self.state.get("modes") or {}
        mode = modes.get(normalized_market)
        if not isinstance(mode, dict):
            return "absent"
        if any(
            int(position.get("signed_shares") or 0) != 0
            for position in (mode.get("positions") or {}).values()
        ):
            raise RuntimeError(f"{normalized_market} has an open position")
        session_date = str(mode.get("session_date") or observed.date().isoformat())
        if self._session_has_fill(normalized_market, session_date):
            raise RuntimeError(f"{normalized_market} has fills and cannot be retired")
        modes.pop(normalized_market)
        self._event(
            "simulation_mode_retired",
            recorded_at=observed,
            market=normalized_market,
            reason=normalized_reason,
        )
        self._persist(observed)
        return "retired"

    def _session_has_fill(self, market: str, session_date: str) -> bool:
        if not self.fills_path.is_file():
            return False
        with self.fills_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                try:
                    row = json.loads(line)
                except (TypeError, ValueError):
                    continue
                if (
                    str(row.get("market") or "") == market
                    and str(row.get("session_date") or "") == session_date
                    and int(row.get("quantity") or 0) > 0
                ):
                    return True
        return False

    def _event(
        self,
        event: str,
        *,
        recorded_at: datetime | None = None,
        **payload: Any,
    ) -> None:
        self._append_ledger(
            self.events_path,
            {"recorded_at": _iso(recorded_at), "event": event, **payload},
        )

    def _order(self, payload: Mapping[str, Any]) -> None:
        self._append_ledger(self.orders_path, payload)

    def _fill(self, payload: Mapping[str, Any]) -> None:
        self._append_ledger(self.fills_path, payload)

    def record_latency_sample(
        self,
        *,
        market: str,
        signal_id: str,
        result: str,
        summary: Mapping[str, Any],
        consumer_detected_at: datetime,
        ledger_persisted_at: datetime,
        executor_quote_fetch_ms: float,
        eligibility_load_ms: float,
        ledger_compute_persist_ms: float,
        opening_signal_batch_wait_ms: float = 0.0,
        opening_signal_batch_mode_count: int = 1,
        opening_signal_batch_expected_mode_count: int = 1,
        opening_signal_batch_complete: bool = True,
    ) -> None:
        """Persist one measured input-to-ledger sample for the public panel."""

        started_at = _parse_timestamp(
            summary.get("signal_started_at") or summary.get("generated_at")
        )
        ready_at = _parse_timestamp(
            summary.get("signal_ready_at")
            or summary.get("artifact_published_at")
            or summary.get("generated_at")
        )
        published_at = _parse_timestamp(
            summary.get("artifact_published_at") or summary.get("signal_ready_at")
        )
        live_latency = dict(summary.get("live_latency") or {})
        signal_quote_ms = _finite(live_latency.get("quote_fetch_ms"))
        model_inference_ms = _finite(live_latency.get("model_inference_ms"))
        signal_total_ms = _finite(live_latency.get("compute_before_publish_ms"))
        detailed_signal_stages = {
            "signal_pre_quote_prepare_ms": _finite(
                live_latency.get("pre_quote_prepare_ms")
            ),
            "signal_pre_inference_prepare_ms": _finite(
                live_latency.get("pre_inference_prepare_ms")
            ),
            "signal_post_inference_format_ms": _finite(
                live_latency.get("post_inference_format_ms")
            ),
        }
        has_detailed_signal_stages = any(
            value is not None for value in detailed_signal_stages.values()
        )
        signal_other_ms = None
        if signal_total_ms is not None and not has_detailed_signal_stages:
            signal_other_ms = max(
                0.0,
                signal_total_ms
                - float(signal_quote_ms or 0.0)
                - float(model_inference_ms or 0.0),
            )

        def elapsed_ms(start: datetime | None, end: datetime | None) -> float | None:
            if start is None or end is None:
                return None
            return round(max(0.0, (end - start).total_seconds() * 1000.0), 3)

        opening_gate = ledger_persisted_at.astimezone(TAIPEI).replace(
            hour=9,
            minute=0,
            second=0,
            microsecond=0,
        )
        opening_delay_seconds = (ledger_persisted_at - opening_gate).total_seconds()
        opening_gate_to_ledger_ms = (
            round(opening_delay_seconds * 1000.0, 3)
            if opening_delay_seconds >= 0.0
            else None
        )
        opening_commit_slo_ms = 15_000.0

        stages = {
            **detailed_signal_stages,
            "signal_quote_fetch_ms": signal_quote_ms,
            "model_inference_ms": model_inference_ms,
            "signal_other_compute_ms": signal_other_ms,
            "artifact_publish_ms": _finite(live_latency.get("artifact_publish_ms")),
            "artifact_discovery_ms": elapsed_ms(published_at, consumer_detected_at),
            "opening_signal_batch_wait_ms": round(
                max(0.0, opening_signal_batch_wait_ms), 3
            ),
            "eligibility_load_ms": round(max(0.0, eligibility_load_ms), 3),
            "executor_quote_fetch_ms": round(max(0.0, executor_quote_fetch_ms), 3),
            "ledger_compute_persist_ms": round(max(0.0, ledger_compute_persist_ms), 3),
        }
        finite_stages = {
            key: float(value)
            for key, value in stages.items()
            if value is not None and math.isfinite(float(value))
        }
        bottleneck = (
            max(finite_stages, key=finite_stages.get) if finite_stages else None
        )
        self._append_ledger(
            self.latency_path,
            {
                "schema_version": 1,
                "recorded_at": ledger_persisted_at.isoformat(timespec="microseconds"),
                "session_date": ledger_persisted_at.astimezone(TAIPEI)
                .date()
                .isoformat(),
                "market": market,
                "signal_id": signal_id,
                "result": result,
                "simulation_only": True,
                "measurement_boundary": "signal_input_to_simulation_ledger_persisted",
                "signal_started_at": started_at.isoformat(timespec="microseconds")
                if started_at
                else None,
                "signal_ready_at": ready_at.isoformat(timespec="microseconds")
                if ready_at
                else None,
                "artifact_published_at": published_at.isoformat(timespec="microseconds")
                if published_at
                else None,
                "consumer_detected_at": consumer_detected_at.isoformat(
                    timespec="microseconds"
                ),
                "ledger_persisted_at": ledger_persisted_at.isoformat(
                    timespec="microseconds"
                ),
                "input_to_ledger_ms": elapsed_ms(started_at, ledger_persisted_at),
                "ready_to_ledger_ms": elapsed_ms(ready_at, ledger_persisted_at),
                "opening_gate_to_ledger_ms": opening_gate_to_ledger_ms,
                "opening_commit_slo_ms": opening_commit_slo_ms,
                "opening_commit_slo_met": (
                    opening_gate_to_ledger_ms is not None
                    and opening_gate_to_ledger_ms <= opening_commit_slo_ms
                ),
                "signal_compute_total_ms": signal_total_ms,
                # Keep the original local callback/response evidence even if
                # Discord's later rich-artifact delivery fails. Never infer
                # exchange receipt time from the execution loop's clock.
                "price_request_started_at": summary.get("price_request_started_at"),
                "price_response_received_at": summary.get("price_response_received_at"),
                "price_receipt_timing": dict(summary.get("price_receipt_timing") or {}),
                "quote_transport": dict(live_latency.get("quote_transport") or {}),
                "opening_signal_batch": {
                    "observed_mode_count": max(0, int(opening_signal_batch_mode_count)),
                    "expected_mode_count": max(
                        0, int(opening_signal_batch_expected_mode_count)
                    ),
                    "complete": bool(opening_signal_batch_complete),
                    "single_causal_quote_request": bool(
                        opening_signal_batch_complete
                        and int(opening_signal_batch_mode_count) > 1
                    ),
                },
                "stages": stages,
                "bottleneck_stage": bottleneck,
                "bottleneck_ms": finite_stages.get(bottleneck) if bottleneck else None,
            },
        )

    @staticmethod
    def _entry_and_exit_rates(
        *,
        symbols: Sequence[str],
        security_types: Sequence[str],
        fee_schedule: TaiwanFeeSchedule,
    ) -> dict[str, tuple[float, float, float, float, float]]:
        buy, sell = gross_fee_rate_vectors(
            symbols,
            "tw_day_trade",
            fee_schedule=fee_schedule,
            security_types=security_types,
        )
        rebate = commission_rebate_rate_vector(
            symbols,
            "tw_day_trade",
            fee_schedule=fee_schedule,
            security_types=security_types,
        )
        cash_buy, cash_sell = gross_fee_rate_vectors(
            symbols,
            "tw_cash",
            fee_schedule=fee_schedule,
            security_types=security_types,
        )
        return {
            str(symbol): (
                float(buy[idx]),
                float(sell[idx]),
                float(rebate[idx]),
                float(cash_buy[idx]),
                float(cash_sell[idx]),
            )
            for idx, symbol in enumerate(symbols)
        }

    def register_signal(
        self,
        *,
        spec: ModeSpec,
        summary: Mapping[str, Any],
        signal_rows: Sequence[Mapping[str, Any]],
        quotes: Mapping[str, Mapping[str, Any]],
        eligibility: Mapping[str, LiveEligibility],
        eligibility_coverage: Mapping[str, Any],
        now: datetime | None = None,
        counterfactual_open_replay: bool = False,
    ) -> str:
        observed = _now_taipei(now)
        mode = self._mode(spec)
        signal_id = str(summary.get("signal_id") or "").strip()
        if not signal_id:
            raise ValueError("signal summary has no signal_id")
        if isinstance(mode.get("ledger_state_divergence"), Mapping):
            mode["pending_signal_id"] = signal_id
            mode["engine_status"] = "critical_ledger_state_divergence"
            self._persist(observed)
            return "blocked_ledger_state_divergence"
        if signal_id in set(mode.get("processed_signal_ids") or ()):
            return "already_processed"
        if str(summary.get("execution_mode") or "") != "tw_day_trade":
            return self._block_signal(mode, signal_id, "not_tw_day_trade", observed)
        source_signal_at = _parse_timestamp(
            summary.get("signal_started_at")
            or summary.get("generated_at")
            or summary.get("asof_date")
        )
        if source_signal_at is None:
            return self._block_signal(
                mode, signal_id, "invalid_signal_timestamp", observed
            )
        if source_signal_at.date() != observed.date():
            return self._block_signal(
                mode, signal_id, "signal_not_current_session", observed
            )
        wall_time = observed.timetz().replace(tzinfo=None)
        if counterfactual_open_replay:
            if not bool(summary.get("simulation_replay")):
                raise ValueError(
                    "counterfactual open replay requires simulation_replay=true"
                )
            replay_fill_contract = str(summary.get("entry_fill_contract") or "")
            required_replay_fill_contract = (
                "retrospective_actual_session_open_price_counterfactual"
                if spec.entry_fill_policy == ENTRY_FILL_POLICY_SYNTHETIC_OPEN_TICK
                else (
                    REPLAY_FILL_CONTRACT_0901_MINUTE_PRICE
                    if spec.entry_fill_policy == ENTRY_FILL_POLICY_0901_MINUTE_PRICE
                    else
                    "retrospective_official_session_open_at_09_01_counterfactual"
                    if spec.entry_fill_policy == ENTRY_FILL_POLICY_OFFICIAL_OPEN_AT_0901
                    else "retrospective_historical_best_quote_else_adverse_open_tick_counterfactual"
                    if spec.entry_fill_policy
                    == ENTRY_FILL_POLICY_CAUSAL_BOOK_ELSE_OPEN_TICK
                    else (
                        "retrospective_historical_best_quote_market_else_adverse_open_tick_counterfactual"
                        if spec.entry_fill_policy
                        == ENTRY_FILL_POLICY_MARKET_AT_BEST_ELSE_OPEN_TICK
                        else "retrospective_observed_best_quote_counterfactual"
                    )
                )
            )
            if replay_fill_contract != required_replay_fill_contract:
                raise ValueError(
                    "counterfactual replay entry contract mismatch: "
                    f"policy={spec.entry_fill_policy!r} requires "
                    f"{required_replay_fill_contract!r}, got "
                    f"{replay_fill_contract!r}"
                )
            if wall_time != ENTRY_GATE:
                raise ValueError(
                    "counterfactual open replay must be recorded exactly at 09:01"
                )
            signal_at = observed
        else:
            signal_at = _parse_timestamp(
                summary.get("signal_ready_at")
                or summary.get("artifact_published_at")
                or summary.get("generated_at")
            )
            if signal_at is None:
                return self._block_signal(
                    mode, signal_id, "invalid_signal_ready_timestamp", observed
                )
        entry_gate = (
            ENTRY_GATE
            if counterfactual_open_replay
            or spec.entry_fill_policy == ENTRY_FILL_POLICY_0901_MINUTE_PRICE
            or spec.entry_fill_policy == ENTRY_FILL_POLICY_OFFICIAL_OPEN_AT_0901
            else LIVE_ENTRY_GATE
        )
        if wall_time < entry_gate or wall_time >= EXIT_LIMIT_TIME:
            return self._block_signal(mode, signal_id, "outside_entry_window", observed)
        if not bool(summary.get("live_session_open_feature_applied")):
            return self._block_signal(
                mode, signal_id, "open_feature_not_observed", observed
            )
        if str(
            mode.get("session_date") or ""
        ) == observed.date().isoformat() and mode.get("entry_completed_at"):
            return self._block_signal(
                mode, signal_id, "daily_signal_already_consumed", observed
            )
        carrying_positions = [p for p in (mode.get("positions") or {}).values() if int(p.get("signed_shares") or 0)]
        if spec.strict_intraday and carrying_positions:
            # A prior-day failure is a liquidation obligation, not today's
            # target inventory. Preserve its ledger and close it before any
            # new risk, even if today's model requests the same direction.
            if not self._margin_corporate_action_gate(mode, observed):
                return "waiting_quote"
            self._accrue_margin_carry_cost(mode, observed)
            self._liquidate_prior_inventory(mode, quotes, observed)
            if any(p.get("signed_shares") for p in carrying_positions):
                mode["engine_status"] = "critical_prior_inventory_liquidation"
                mode["pending_signal_id"] = signal_id
                self._persist(observed)
                return "waiting_prior_liquidation"
            self._archive_mode_positions(mode, archived_at=observed)
            carrying_positions = []
        margin_rebalance = bool(carrying_positions and spec.residual_margin_conversion
            and all(p.get("margin_carry_contract") == MARGIN_CARRY_CONTRACT for p in carrying_positions))
        if not margin_rebalance and any(
            int(position.get("signed_shares") or 0) != 0
            for position in (mode.get("positions") or {}).values()
        ):
            return self._block_signal(
                mode,
                signal_id,
                "prior_position_unflattened",
                observed,
                engine_status="critical_prior_position_unflattened",
            )

        row_symbols = [str(row.get("symbol") or "") for row in signal_rows if str(row.get("symbol") or "")]
        if len(row_symbols) != len(set(row_symbols)):
            return self._block_signal(mode, signal_id, "duplicate_signal_symbols", observed)

        if spec.residual_margin_conversion:
            mode["odd_lot_execution_policy"] = spec.odd_lot_execution_policy
            mode["margin_corporate_action_reference_path"] = (
                str(spec.margin_corporate_action_reference_path)
                if spec.margin_corporate_action_reference_path is not None else None
            )
            self._settle_corporate_action_claims(mode, observed)
        if margin_rebalance and not self._margin_corporate_action_gate(mode, observed):
            # Recoverable source/accounting wait: do not consume this signal,
            # overwrite the old inventory, or size from an ex-date NAV gap.
            mode["pending_signal_id"] = signal_id
            self._persist(observed)
            return "waiting_margin_corporate_action"

        # Freeze economic NAV once per session, including pending-quote retries
        # and restart. The initial capital remains the reporting denominator.
        sizing_date = observed.date().isoformat()
        if mode.get("sizing_session_date") != sizing_date:
            sizing_nav = (float(mode["initial_capital_twd"])
                          + float(mode.get("cumulative_realized_net_pnl_twd") or 0.0)
                          + float(mode.get("cumulative_corporate_action_net_twd") or 0.0))
            if margin_rebalance:
                # Only observed opening prices, not the completed 09:01 fill,
                # may value the pre-decision inventory used for target sizing.
                missing = [p["symbol"] for p in carrying_positions
                           if _finite(quotes.get(p["symbol"], {}).get("open")) is None
                           and not (quotes.get(p["symbol"], {}).get("official_session_no_trade_print")
                                    and _finite(p.get("last_mark_price")) is not None)]
                if missing:
                    mode["pending_signal_id"] = signal_id
                    mode["engine_status"] = "waiting_carried_position_open_price"
                    mode["missing_carried_open_symbols"] = sorted(set(missing))
                    self._persist(observed)
                    return "waiting_quote"
                self._accrue_margin_carry_cost(mode, observed)
                mode["sizing_nav_carried_price_symbols"] = sorted({p["symbol"] for p in carrying_positions if _finite(quotes[p["symbol"]].get("open")) is None})
                sizing_nav += sum(position_net_liquidation_pnl(p,
                    _finite(quotes[p["symbol"]].get("open")) or float(p["last_mark_price"])) for p in carrying_positions)
            sizing_nav -= float(mode.get("cumulative_carry_cost_twd") or 0.0)
            if not math.isfinite(sizing_nav) or sizing_nav <= 0.0:
                return self._block_signal(mode, signal_id, "nonpositive_account_nav", observed)
            mode["sizing_session_date"] = sizing_date
            mode["session_sizing_nav_twd"] = sizing_nav
        sizing_nav = float(mode["session_sizing_nav_twd"])
        if not math.isfinite(sizing_nav) or sizing_nav <= 0.0:
            return self._block_signal(mode, signal_id, "invalid_session_sizing_nav", observed)

        actionable_symbols: list[str] = []
        for row in signal_rows:
            symbol = str(row.get("symbol") or "")
            weight = float(row.get("target_weight") or 0.0)
            side_allowed = (
                bool(row.get("can_buy")) if weight > 0.0 else bool(row.get("can_sell"))
            )
            evidence = eligibility.get(symbol)
            sizing_price = _finite(row.get("open_price")) or _finite(
                quotes.get(symbol, {}).get("open")
            )
            if (
                not symbol
                or weight == 0.0
                or not bool(row.get("tradable"))
                or not side_allowed
                or evidence is None
                or not evidence.covered
                or not evidence.eligible
                or (weight < 0.0 and not evidence.short_open)
                or sizing_price is None
            ):
                continue
            requested_shares = int(
                math.floor(
                    abs(weight)
                    * sizing_nav
                    / sizing_price
                    / int(spec.lot_size)
                )
            ) * int(spec.lot_size)
            if requested_shares > 0:
                actionable_symbols.append(symbol)
        later_quote_found = any(
            (quote_at := _parse_timestamp(quotes.get(symbol, {}).get("quote_at")))
            is not None
            and quote_at <= observed
            and (
                quote_at >= signal_at
                if counterfactual_open_replay
                else quote_at > signal_at
            )
            for symbol in actionable_symbols
        )
        synthetic_open_fill = (
            spec.entry_fill_policy == ENTRY_FILL_POLICY_SYNTHETIC_OPEN_TICK
        )
        deterministic_paper_market_fill = (
            spec.entry_fill_policy == ENTRY_FILL_POLICY_MARKET_AT_BEST_ELSE_OPEN_TICK
        )
        deterministic_official_open_fill = (
            spec.entry_fill_policy == ENTRY_FILL_POLICY_OFFICIAL_OPEN_AT_0901
        )
        deterministic_0901_minute_price_fill = (
            spec.entry_fill_policy == ENTRY_FILL_POLICY_0901_MINUTE_PRICE
        )
        if (
            actionable_symbols
            and not later_quote_found
            and not synthetic_open_fill
            and not deterministic_paper_market_fill
            and not deterministic_0901_minute_price_fill
            and not deterministic_official_open_fill
        ):
            mode["pending_signal_id"] = signal_id
            mode["pending_signal_at"] = signal_at.isoformat(timespec="seconds")
            mode["engine_status"] = "waiting_causally_later_quote"
            self._persist(observed)
            return "waiting_quote"
        if (
            actionable_symbols
            and not synthetic_open_fill
            and not deterministic_paper_market_fill
            and not deterministic_0901_minute_price_fill
            and not deterministic_official_open_fill
            and wall_time >= FIRST_MINUTE_EXECUTION_TIME
            and all(
                _finite(quotes.get(symbol, {}).get("minute_volume_lots")) is None
                for symbol in actionable_symbols
            )
        ):
            # After 09:01, displayed depth alone is insufficient: entry also
            # needs an isolated completed-minute volume budget.  A process
            # restart has no adjacent cumulative-volume baseline on its first
            # observation, so keep the signal pending until the next minute
            # instead of consuming the one allowed daily entry as false-flat.
            mode["pending_signal_id"] = signal_id
            mode["pending_signal_at"] = signal_at.isoformat(timespec="seconds")
            mode["pending_wait_reason"] = "completed_minute_liquidity_unavailable"
            mode["engine_status"] = "waiting_completed_minute_liquidity"
            self._persist(observed)
            return "waiting_first_minute"

        security_types = []
        symbols = []
        for row in signal_rows:
            symbol = str(row.get("symbol") or "")
            if not symbol:
                continue
            evidence = eligibility.get(symbol)
            security_type = evidence.security_type if evidence else None
            symbols.append(symbol)
            security_types.append(
                security_type if security_type in {"stock", "etf"}
                else classify_tw_stock_or_etf(symbol) or "stock"
            )
        for position in carrying_positions:
            if position["symbol"] not in symbols:
                symbols.append(position["symbol"])
                security_types.append(position.get("security_type") or "stock")
        security_types_by_symbol = dict(zip(symbols, security_types))
        fee_rates = self._entry_and_exit_rates(
            symbols=symbols,
            security_types=security_types,
            fee_schedule=spec.fee_schedule,
        )

        prior_session_date = str(mode.get("session_date") or "")
        if prior_session_date and prior_session_date != observed.date().isoformat():
            self._archive_mode_positions(mode, archived_at=observed)

        mode["session_date"] = observed.date().isoformat()
        mode["session_valid"] = True
        mode.pop("non_session_invalidated_at", None)
        mode.pop("non_session_invalidation_reason", None)
        mode["blocked_reason"] = None
        mode["signal_id"] = signal_id
        mode["signal_at"] = signal_at.isoformat(timespec="seconds")
        mode["source_signal_at"] = source_signal_at.isoformat(timespec="seconds")
        mode["counterfactual_open_replay"] = bool(counterfactual_open_replay)
        # A replay has two different clocks: the immutable model decision time
        # and the paper ledger time at which the already-observed session open
        # is applied.  Keep the latter explicit instead of forcing dashboard
        # clients to infer it from the overloaded ``signal_at`` field.
        mode["open_reconstructed_at"] = (
            signal_at.isoformat(timespec="seconds")
            if counterfactual_open_replay
            else None
        )
        mode["signal_source_path"] = summary.get("summary_path")
        mode["target_weights_path"] = summary.get("weights_path")
        mode["target_positions_path"] = summary.get(
            "positions_markdown_path"
        ) or summary.get("weights_path")
        mode["target_symbol_count"] = summary.get("symbol_count") or len(signal_rows)
        mode["target_risk"] = dict(summary.get("target_risk") or {})
        mode["feature_cutoff_date"] = summary.get("feature_cutoff_date")
        mode["checkpoint_fingerprint"] = summary.get("checkpoint_fingerprint")
        mode["config_fingerprint"] = summary.get("config_fingerprint")
        mode["eligibility_coverage"] = dict(eligibility_coverage)
        mode["simulation_replay"] = bool(summary.get("simulation_replay", False))
        mode["historical_minute_valuation"] = bool(summary.get("historical_minute_valuation", False))
        mode["replay_basis"] = summary.get("replay_basis")
        mode["replay_source"] = summary.get("replay_source")
        mode["entry_fill_contract"] = summary.get("entry_fill_contract") or (
            "synthetic_observed_session_open_adverse_tick"
            if synthetic_open_fill
            else "counterfactual_official_open_signal_0900_observed_0901_minute_price"
            if deterministic_0901_minute_price_fill
            else "counterfactual_official_session_open_at_09_01"
            if deterministic_official_open_fill
            else "paper_market_order_at_best_quote_else_adverse_open_tick"
            if deterministic_paper_market_fill
            else "best_ask_for_buy_best_bid_for_sell"
        )
        mode["entry_liquidity_assumption"] = summary.get(
            "entry_liquidity_assumption"
        ) or (
            "counterfactual_unbounded_no_exchange_fill_claim"
            if synthetic_open_fill
            else "observed_09_01_minute_price_50pct_volume_capped_no_exchange_fill_claim"
            if deterministic_0901_minute_price_fill
            else "official_open_price_full_requested_paper_quantity_no_exchange_fill_claim"
            if deterministic_official_open_fill
            else "full_requested_quantity_at_observed_best_quote_else_adverse_open_tick_no_exchange_depth_claim"
            if deterministic_paper_market_fill
            else "09:00_fresh_level_one_depth_then_minimum_with_50pct_completed_minute_volume"
        )
        mode["entry_fill_policy"] = spec.entry_fill_policy
        mode["execution_realism_contract"] = EXECUTION_REALISM_CONTRACT if spec.uses_realistic_execution else None
        mode["intraday_contract"] = STRICT_INTRADAY_CONTRACT if spec.strict_intraday else None
        mode["capital_sizing_basis"] = "session_start_account_nav"
        mode["funding_assumption"] = "paper_nav_risk_budget_not_verified_broker_buying_power"
        mode["margin_carry_contract"] = MARGIN_CARRY_CONTRACT if spec.residual_margin_conversion else None
        mode["margin_cost_assumptions"] = {
            "financing_ratio": spec.margin_financing_ratio,
            "financing_annual_rate": spec.margin_financing_annual_rate,
            "short_annual_borrow_rate": spec.margin_short_annual_borrow_rate,
            "short_handling_fee_rate": spec.margin_short_handling_fee_rate,
            "day_count": "actual_calendar_days_365",
            "source": "configured_stress_assumptions_not_broker_quote",
        }
        if spec.residual_margin_conversion:
            mode["funding_assumption"] = "assumed_all_marginable_residual_no_inventory_or_credit_gate_gross_target_nav_budget"
        if spec.strict_intraday:
            mode["funding_assumption"] = "same_day_flatten_required_adverse_limit_exception_only_no_broker_credit_claim"
        mode["entry_price_offset_ticks"] = int(spec.entry_price_offset_ticks)
        mode["entry_fill_is_synthetic"] = bool(synthetic_open_fill)
        mode["counterfactual_0901_price_fill"] = bool(
            deterministic_0901_minute_price_fill
        )
        mode["counterfactual_open_price_fill"] = bool(deterministic_official_open_fill)
        mode["entry_fill_has_synthetic_fallback"] = False
        mode.pop("execution_projection", None)
        mode["positions"] = {p["position_id"]: p for p in carrying_positions} if margin_rebalance else {}
        mode["pending_entry_orders"] = {}
        mode["entry_completed_at"] = observed.isoformat(timespec="seconds")
        mode.pop("pending_wait_reason", None)
        mode["exit_limit_submitted_at"] = None
        mode["force_exit_started_at"] = None
        mode["closing_auction_submitted_at"] = None
        mode["closing_auction_settled_at"] = None
        mode["residual_conversion_completed_at"] = None
        mode["force_exit_failures"] = 0
        mode["pending_entry_shares"] = 0
        mode["margin_exception_count"] = 0
        mode["unresolved_exit_count"] = 0
        mode["closing_auction_pending_count"] = 0
        mode["terminal_flatten_count"] = 0
        mode["terminal_flatten_degraded_count"] = 0
        counts: dict[str, int] = {}
        signal_records: list[dict[str, Any]] = []
        order_records: list[dict[str, Any]] = []
        fill_records: list[dict[str, Any]] = []

        plans = [
            _prepare_entry_plan(
                row,
                quote=quotes.get(str(row.get("symbol") or "")) or {},
                evidence=eligibility.get(str(row.get("symbol") or "")),
                signal_at=signal_at,
                observation_at=observed,
                spec=spec,
                allow_quote_at_signal=counterfactual_open_replay,
                sizing_nav_twd=sizing_nav,
            )
            for row in signal_rows
            if str(row.get("symbol") or "")
        ]
        if margin_rebalance:
            plans = self._reconcile_margin_targets(
                mode, plans, quotes=quotes, spec=spec, signal_at=signal_at,
                observed=observed, allow_quote_at_signal=counterfactual_open_replay,
                order_records=order_records, fill_records=fill_records,
            )
        if spec.uses_realistic_execution and plans:
            # Reserve full notional for BOTH directions: short-sale proceeds
            # are never buying power. Reuse the canonical integer budget
            # scaler; do not renormalize unfilled targets into other names.
            prices = np.asarray([float(p["entry_price"] or 0.0) for p in plans])
            desired = np.asarray([p["filled_shares"] for p in plans], dtype=np.int64)
            rates = np.asarray([
                fee_rates[p["symbol"]][0 if p["side"] == "long" else 1]
                for p in plans
            ])
            funded = _scale_lot_buys_to_budget_with_fixed_fees(
                desired, prices, np.asarray([p.get("quantity_step_shares", spec.lot_size) for p in plans]), rates,
                budget=max(0.0, sizing_nav - sum(
                    abs(int(p.get("signed_shares") or 0)) * (_finite(quotes[p["symbol"]].get("open")) or float(p["last_mark_price"]))
                    for p in carrying_positions
                ) - sum(float(r.get("fee_and_tax_twd") or 0) for r in fill_records)
                    - float(mode.get("corporate_action_receivable_twd") or 0.0)),
                minimum_commission=0.0, commission_rounding="none",
            )
            for plan, quantity in zip(plans, funded):
                if int(quantity) < plan["filled_shares"]:
                    plan["filled_shares"] = int(quantity)
                    plan["status"] = "partial_depth" if quantity else "blocked"
                    plan["reason"] = "account_nav_budget_exhausted"
        for plan in plans:
            # Flags describe actual additions after inventory netting and
            # funding, not the now-superseded flat-account preview quantity.
            if not int(plan["filled_shares"]):
                for key in ("synthetic_fill", "synthetic_fallback_fill", "paper_market_fill",
                            "counterfactual_0901_price_fill", "counterfactual_open_price_fill"):
                    plan[key] = False
            row = plan["row"]
            symbol = plan["symbol"]
            target_weight = float(plan["target_weight"])
            side = plan["side"]
            quote = plan["quote"]
            evidence = plan["evidence"]
            status = plan["status"]
            reason = plan["reason"]
            entry_price = plan["entry_price"]
            sizing_price = plan["sizing_price"]
            upper = plan["upper"]
            lower = plan["lower"]
            requested_shares = int(plan["requested_shares"])
            filled_shares = int(plan["filled_shares"])
            top_book_capacity_shares = int(plan["top_book_capacity_shares"])
            minute_kbar_capacity_shares = int(plan["minute_kbar_capacity_shares"])
            entry_price_source = plan["entry_price_source"]
            offset_ticks = int(spec.price_limit_offset_ticks)
            filled_weight = (
                (1.0 if side == "long" else -1.0)
                * filled_shares
                * entry_price
                / sizing_nav
                if filled_shares > 0 and entry_price is not None
                else 0.0
            )
            signal_record = {
                "recorded_at": observed.isoformat(timespec="seconds"),
                "session_date": observed.date().isoformat(),
                "market": spec.market,
                "signal_source_path": summary.get("summary_path"),
                "signal_market": spec.signal_market or spec.market,
                "price_limit_offset_ticks": offset_ticks,
                "signal_id": signal_id,
                "signal_at": signal_at.isoformat(timespec="seconds"),
                "source_signal_at": source_signal_at.isoformat(timespec="seconds"),
                "open_reconstructed_at": (
                    signal_at.isoformat(timespec="seconds")
                    if counterfactual_open_replay
                    else None
                ),
                "symbol": symbol,
                "name": row.get("name"),
                "side": side,
                "action": row.get("action"),
                "score": row.get("score"),
                "raw_score": row.get("raw_score"),
                "target_weight": target_weight,
                "requested_shares": requested_shares,
                "previous_signed_shares": plan.get("previous_signed_shares", 0),
                "target_signed_shares": plan.get("target_signed_shares"),
                "reduction_requested_shares": plan.get("reduction_requested_shares", 0),
                "reduction_filled_shares": plan.get("reduction_filled_shares", 0),
                "margin_carry_contract": mode.get("margin_carry_contract"),
                "sizing_nav_twd": sizing_nav,
                "filled_weight_basis": "session_start_account_nav",
                "execution_realism_contract": mode.get("execution_realism_contract"),
                "executable_order_shares": filled_shares,
                "target_unsubmitted_shares": max(0, requested_shares - filled_shares),
                "sizing_open_price": sizing_price,
                "execution_price": entry_price,
                "filled_shares": filled_shares,
                "filled_weight": filled_weight,
                "top_book_capacity_shares": top_book_capacity_shares,
                "minute_kbar_volume_lots": quote.get("minute_volume_lots"),
                "minute_kbar_capacity_shares": minute_kbar_capacity_shares,
                "minute_volume_participation": MINUTE_VOLUME_PARTICIPATION,
                "status": status,
                "reason": reason,
                "quote_at": quote.get("quote_at"),
                "historical_source_quote_at": quote.get("historical_source_quote_at"),
                "bid": quote.get("bid"),
                "ask": quote.get("ask"),
                "bid_volume_lots": quote.get("bid_volume"),
                "ask_volume_lots": quote.get("ask_volume"),
                "upper_limit": upper,
                "lower_limit": lower,
                "eligibility_covered": bool(evidence and evidence.covered),
                "day_trade_eligible": bool(evidence and evidence.eligible),
                "sell_first_allowed": bool(evidence and evidence.short_open),
                "quote_source": quote.get("source"),
                "simulation_replay": bool(mode.get("simulation_replay")),
                "replay_basis": mode.get("replay_basis"),
                "counterfactual_open_replay": bool(counterfactual_open_replay),
                "entry_fill_policy": plan["entry_fill_policy"],
                "entry_price_offset_ticks": plan["entry_price_offset_ticks"],
                "entry_price_source": entry_price_source,
                "entry_price_method": plan["entry_price_method"],
                "synthetic_fill": bool(plan["synthetic_fill"]),
                "synthetic_fallback_fill": bool(plan["synthetic_fallback_fill"]),
                "paper_market_fill": bool(plan["paper_market_fill"]),
                "counterfactual_0901_price_fill": bool(
                    plan["counterfactual_0901_price_fill"]
                ),
                "counterfactual_open_price_fill": bool(
                    plan["counterfactual_open_price_fill"]
                ),
            }
            signal_records.append(signal_record)
            counts[status] = counts.get(status, 0) + 1
            retryable = (spec.strict_intraday and not counterfactual_open_replay
                         and spec.entry_fill_policy == ENTRY_FILL_POLICY_CAUSAL_BOOK
                         and requested_shares > filled_shares and upper is not None and lower is not None
                         and reason in {"marketable_depth_exhausted", "marketable_depth_unavailable",
                                        "no_executable_best_quote", "quote_not_after_signal",
                                        "quote_after_local_observation", "account_nav_budget_exhausted",
                                        "waiting_non_trial_quote"})
            if (
                status
                not in {
                    "ready",
                    "partial_depth",
                    "forced_synthetic_fill",
                }
                or entry_price is None
            ) and not retryable:
                continue
            # A zero-fill template is private pending-order metadata. It never
            # enters positions/fills until an actual causal quote is consumed.
            entry_price = entry_price if entry_price is not None else sizing_price

            upper_bracket = float(
                move_price_ticks_numpy(
                    np.asarray([upper], dtype=np.float64),
                    -offset_ticks,
                    np.asarray([observed.date()]),
                    security_types=security_types_by_symbol[symbol],
                )[0]
            )
            lower_bracket = float(
                move_price_ticks_numpy(
                    np.asarray([lower], dtype=np.float64),
                    offset_ticks,
                    np.asarray([observed.date()]),
                    security_types=security_types_by_symbol[symbol],
                )[0]
            )

            signed_shares = filled_shares if side == "long" else -filled_shares
            buy_rate, sell_rate, rebate_rate, cash_buy_rate, cash_sell_rate = fee_rates[
                symbol
            ]
            entry_rate = buy_rate if side == "long" else sell_rate
            entry_gross_fee = filled_shares * entry_price * entry_rate
            entry_rebate = filled_shares * entry_price * rebate_rate
            entry_fee = entry_gross_fee - entry_rebate
            position_id = f"{spec.market}:{observed.date().isoformat()}:{symbol}"
            entry_order_id = f"{position_id}:entry"
            position = {
                "position_id": position_id,
                "market": spec.market,
                "signal_market": spec.signal_market or spec.market,
                "session_date": observed.date().isoformat(),
                "signal_id": signal_id,
                "signal_at": signal_at.isoformat(timespec="seconds"),
                "source_signal_at": source_signal_at.isoformat(timespec="seconds"),
                "open_reconstructed_at": (
                    signal_at.isoformat(timespec="seconds")
                    if counterfactual_open_replay
                    else None
                ),
                "symbol": symbol,
                "name": row.get("name"),
                "side": side,
                "target_weight": target_weight,
                "requested_shares": requested_shares,
                "executable_order_shares": filled_shares,
                "target_unsubmitted_shares": max(0, requested_shares - filled_shares),
                "filled_shares": filled_shares,
                "signed_shares": signed_shares,
                "lot_size": int(spec.lot_size),
                "odd_lot_execution_policy": spec.odd_lot_execution_policy,
                "entry_order_id": entry_order_id,
                "entry_at": observed.isoformat(timespec="seconds"),
                "entry_quote_at": quote.get("quote_at"),
                "historical_entry_quote_at": quote.get("historical_source_quote_at"),
                "entry_price": entry_price,
                "sizing_open_price": sizing_price,
                "entry_fee_twd": entry_fee,
                "remaining_entry_fee_twd": entry_fee,
                "entry_gross_fee_and_tax_twd": entry_gross_fee,
                "entry_commission_rebate_accrued_twd": entry_rebate,
                "buy_fee_rate": buy_rate,
                "sell_fee_rate": sell_rate,
                "commission_rebate_rate": rebate_rate,
                "cash_buy_fee_rate": cash_buy_rate,
                "cash_sell_fee_rate": cash_sell_rate,
                "security_type": security_types_by_symbol[symbol],
                "upper_limit": upper,
                "lower_limit": lower,
                "take_profit_price": upper_bracket if side == "long" else lower_bracket,
                "stop_trigger_price": lower_bracket
                if side == "long"
                else upper_bracket,
                "price_limit_offset_ticks": offset_ticks,
                "bracket_price_policy": (
                    "inside_daily_limits_by_ticks"
                    if offset_ticks > 0
                    else "full_daily_limits"
                ),
                "fill_guaranteed": bool(
                    plan["synthetic_fill"] or plan["counterfactual_open_price_fill"]
                ),
                "take_profit_order_status": "working",
                "stop_order_status": "armed_local_trigger",
                "eod_limit_order_status": None,
                "status": "open",
                "last_mark_price": entry_price,
                "last_complete_net_pnl_twd": -entry_fee,
                "realized_gross_pnl_twd": 0.0,
                "realized_exit_fee_twd": 0.0,
                "realized_net_pnl_twd": 0.0,
                "valuation_stale": False,
                "simulation_replay": bool(mode.get("simulation_replay")),
                "replay_basis": mode.get("replay_basis"),
                "replay_source": mode.get("replay_source"),
                "counterfactual_open_replay": bool(counterfactual_open_replay),
                "entry_fill_policy": plan["entry_fill_policy"],
                "entry_price_offset_ticks": plan["entry_price_offset_ticks"],
                "entry_fill_is_synthetic": bool(plan["synthetic_fill"]),
                "entry_price_source": entry_price_source,
                "entry_price_method": plan["entry_price_method"],
                "synthetic_fallback_fill": bool(plan["synthetic_fallback_fill"]),
                "paper_market_fill": bool(plan["paper_market_fill"]),
                "counterfactual_0901_price_fill": bool(
                    plan["counterfactual_0901_price_fill"]
                ),
                "counterfactual_open_price_fill": bool(
                    plan["counterfactual_open_price_fill"]
                ),
            }
            if retryable:
                mode["pending_entry_orders"][symbol] = {
                    "position": dict(position), "remaining_shares": requested_shares - filled_shares,
                    "last_cumulative_volume_lots": quote.get("cumulative_volume_lots"),
                    "minute": observed.strftime("%Y-%m-%dT%H:%M"),
                    "minute_used_shares": filled_shares,
                    "status": "working", "sequence": 0,
                }
            if filled_shares <= 0:
                continue
            mode["positions"][position_id] = position
            mode["cumulative_commission_rebate_accrued_twd"] = (
                float(mode.get("cumulative_commission_rebate_accrued_twd") or 0.0)
                + entry_rebate
            )
            order_base = {
                "recorded_at": observed.isoformat(timespec="seconds"),
                "session_date": observed.date().isoformat(),
                "market": spec.market,
                "signal_market": spec.signal_market or spec.market,
                "entry_fill_policy": plan["entry_fill_policy"],
                "entry_price_offset_ticks": plan["entry_price_offset_ticks"],
                "price_limit_offset_ticks": offset_ticks,
                "bracket_price_offset_ticks": offset_ticks,
                "position_id": position_id,
                "symbol": symbol,
                "quantity": filled_shares,
                "simulation_only": True,
                "odd_lot_execution_policy": spec.odd_lot_execution_policy if filled_shares % spec.lot_size else None,
            }
            order_records.append(
                {
                    **order_base,
                    "order_id": entry_order_id,
                    "purpose": "entry",
                    "side": "buy" if side == "long" else "sell_short",
                    "order_type": (
                        "PAPER_0901_MINUTE_PRICE"
                        if bool(plan["counterfactual_0901_price_fill"])
                        else "PAPER_OPEN_PRICE_0901"
                        if bool(plan["counterfactual_open_price_fill"])
                        else "SYNTHETIC_OPEN_TICK"
                        if bool(plan["synthetic_fill"])
                        else "MKT"
                    ),
                    "status": "filled",
                    "filled_quantity": filled_shares,
                    "unfilled_quantity": 0,
                    "model_requested_quantity": requested_shares,
                    "target_unsubmitted_quantity": max(
                        0, requested_shares - filled_shares
                    ),
                    "synthetic_fill": bool(plan["synthetic_fill"]),
                    "synthetic_fallback_fill": bool(plan["synthetic_fallback_fill"]),
                    "paper_market_fill": bool(plan["paper_market_fill"]),
                    "counterfactual_0901_price_fill": bool(
                        plan["counterfactual_0901_price_fill"]
                    ),
                    "counterfactual_open_price_fill": bool(
                        plan["counterfactual_open_price_fill"]
                    ),
                    "entry_price_source": entry_price_source,
                    "entry_price_method": plan["entry_price_method"],
                    "historical_source_quote_at": quote.get(
                        "historical_source_quote_at"
                    ),
                }
            )
            fill_records.append(
                {
                    **order_base,
                    "order_id": entry_order_id,
                    "purpose": "entry",
                    "fill_at": observed.isoformat(timespec="seconds"),
                    "quote_at": quote.get("quote_at"),
                    "historical_source_quote_at": quote.get(
                        "historical_source_quote_at"
                    ),
                    "quantity": filled_shares,
                    "price": entry_price,
                    "fee_and_tax_twd": entry_fee,
                    "gross_fee_and_tax_twd": entry_gross_fee,
                    "commission_rebate_accrued_twd": entry_rebate,
                    "fill_contract": mode.get("entry_fill_contract"),
                    "depth_assumption": mode.get("entry_liquidity_assumption"),
                    "simulation_replay": bool(mode.get("simulation_replay")),
                    "replay_basis": mode.get("replay_basis"),
                    "synthetic_fill": bool(plan["synthetic_fill"]),
                    "synthetic_fallback_fill": bool(plan["synthetic_fallback_fill"]),
                    "paper_market_fill": bool(plan["paper_market_fill"]),
                    "counterfactual_0901_price_fill": bool(
                        plan["counterfactual_0901_price_fill"]
                    ),
                    "counterfactual_open_price_fill": bool(
                        plan["counterfactual_open_price_fill"]
                    ),
                    "entry_price_source": entry_price_source,
                    "entry_price_method": plan["entry_price_method"],
                }
            )
            for purpose, order_type, price, order_status in (
                ("take_profit", "LMT", position["take_profit_price"], "working"),
                (
                    "stop_loss",
                    "LOCAL_STOP_MKT",
                    position["stop_trigger_price"],
                    "armed",
                ),
            ):
                order_records.append(
                    {
                        **order_base,
                        "order_id": f"{position_id}:{purpose}",
                        "purpose": purpose,
                        "side": "sell" if side == "long" else "buy_to_cover",
                        "order_type": order_type,
                        "price": price,
                        "quantity": filled_shares,
                        "status": order_status,
                    }
                )

        if spec.residual_margin_conversion:
            inventory: dict[str, int] = {}
            inventory_marks: dict[str, float] = {}
            for p in mode["positions"].values():
                symbol = str(p["symbol"])
                inventory[symbol] = inventory.get(symbol, 0) + int(p["signed_shares"])
                quote = quotes.get(symbol) or {}
                inventory_marks[symbol] = (_finite(quote.get("execution_price_0901"))
                    or _finite(quote.get("bid" if int(p["signed_shares"]) > 0 else "ask"))
                    or float(p["last_mark_price"]))
            for record in signal_records:
                symbol = str(record["symbol"])
                record["inventory_signed_shares_after"] = inventory.get(symbol, 0)
                record["inventory_weight_after"] = inventory.get(symbol, 0) * inventory_marks.get(symbol, 0.0) / sizing_nav
                record["inventory_weight_basis"] = "session_nav_observed_execution_or_last_mark"
        reason_counts: dict[str, int] = {}
        for plan in plans:
            reason = str(plan.get("reason") or "none")
            reason_counts[reason] = reason_counts.get(reason, 0) + 1
        entry_requested_shares = sum(
            int(plan.get("requested_shares") or 0) for plan in plans
        )
        entry_filled_shares = sum(int(plan.get("filled_shares") or 0) for plan in plans)
        entry_unfilled_shares = max(0, entry_requested_shares - entry_filled_shares)
        entry_fill_count = sum(r.get("purpose") == "entry" for r in fill_records)
        mode["rebalance_reduction_fill_count"] = sum(r.get("purpose") == "next_signal_inventory_delta" for r in fill_records)
        entry_best_quote_fill_count = sum(
            int(plan.get("filled_shares") or 0) > 0
            and not bool(plan.get("synthetic_fill"))
            and not bool(plan.get("counterfactual_0901_price_fill"))
            and not bool(plan.get("counterfactual_open_price_fill"))
            for plan in plans
        )
        entry_synthetic_fallback_fill_count = sum(
            bool(plan.get("synthetic_fallback_fill")) for plan in plans
        )
        entry_synthetic_fill_count = sum(
            bool(plan.get("synthetic_fill")) for plan in plans
        )
        entry_paper_market_fill_count = sum(
            bool(plan.get("paper_market_fill")) for plan in plans
        )
        entry_official_open_fill_count = sum(
            bool(plan.get("counterfactual_open_price_fill")) for plan in plans
        )
        entry_0901_vwap_fill_count = sum(
            bool(plan.get("counterfactual_0901_price_fill"))
            and str(plan.get("entry_price_method") or "minute_vwap")
            == "minute_vwap"
            for plan in plans
        )
        entry_0901_close_fill_count = sum(
            bool(plan.get("counterfactual_0901_price_fill"))
            and plan.get("entry_price_method") == "minute_close"
            for plan in plans
        )
        entry_0901_minute_price_fill_count = sum(
            bool(plan.get("counterfactual_0901_price_fill")) for plan in plans
        )
        entry_fill_outcome = (
            "filled"
            if entry_fill_count and entry_unfilled_shares == 0
            else "partial"
            if entry_fill_count
            else "no_order"
            if entry_requested_shares == 0
            else "no_fill"
        )
        mode["signal_reason_counts"] = reason_counts
        mode["entry_fill_count"] = entry_fill_count
        mode["entry_requested_shares"] = entry_requested_shares
        mode["entry_filled_shares"] = entry_filled_shares
        mode["entry_unfilled_shares"] = entry_unfilled_shares
        mode["pending_entry_shares"] = sum(
            int(order["remaining_shares"])
            for order in (mode.get("pending_entry_orders") or {}).values()
            if order.get("status") == "working"
        )
        mode["entry_fill_outcome"] = entry_fill_outcome
        mode["entry_best_quote_fill_count"] = entry_best_quote_fill_count
        mode["entry_synthetic_fallback_fill_count"] = (
            entry_synthetic_fallback_fill_count
        )
        mode["entry_paper_market_fill_count"] = entry_paper_market_fill_count
        mode["entry_official_open_fill_count"] = entry_official_open_fill_count
        mode["entry_0901_vwap_fill_count"] = entry_0901_vwap_fill_count
        mode["entry_0901_close_fill_count"] = entry_0901_close_fill_count
        mode["entry_0901_minute_price_fill_count"] = (
            entry_0901_minute_price_fill_count
        )
        mode["entry_fill_has_synthetic_fallback"] = bool(
            entry_synthetic_fallback_fill_count
        )
        mode["entry_fill_is_synthetic"] = bool(entry_synthetic_fill_count)
        mode["paper_fill_deterministic"] = bool(deterministic_paper_market_fill)
        mode["counterfactual_0901_price_fill"] = bool(
            deterministic_0901_minute_price_fill
        )
        mode["counterfactual_open_price_fill"] = bool(deterministic_official_open_fill)
        mode["exchange_fill_guaranteed"] = False

        # One signal is one logical append transaction per ledger.  The compact
        # start marker is fsynced first so an interrupted transaction can never
        # be mistaken for a never-seen signal and executed twice after restart.
        self._event(
            "signal_commit_started",
            recorded_at=observed,
            market=spec.market,
            session_date=observed.date().isoformat(),
            signal_id=signal_id,
        )
        try:
            _append_jsonl_many(self.signals_path, signal_records)
            _append_jsonl_many(self.orders_path, order_records)
            _append_jsonl_many(self.fills_path, fill_records)
        except Exception as exc:
            mode["ledger_state_divergence"] = {
                "kind": "signal_ledger_append_failed",
                "signal_id": signal_id,
                "session_date": observed.date().isoformat(),
                "detected_at": observed.isoformat(timespec="seconds"),
                "error": f"{type(exc).__name__}: {exc}",
            }
            mode["engine_status"] = "critical_ledger_state_divergence"
            mode["readiness_error"] = (
                f"ledger_state_divergence:signal_ledger_append_failed:{signal_id}"
            )
            self._persist(observed)
            raise

        processed = list(mode.get("processed_signal_ids") or ())
        processed.append(signal_id)
        mode["processed_signal_ids"] = processed[-32:]
        mode["pending_signal_id"] = None
        mode["signal_counts"] = counts
        mode["engine_status"] = (
            "active"
            if any(
                int(item.get("signed_shares") or 0) != 0
                for item in mode["positions"].values()
            )
            else "entry_price_missing_no_fill"
            if entry_requested_shares > 0 and entry_fill_count == 0
            else "flat_no_executable_signal"
        )
        try:
            self._event(
                "signal_registered",
                recorded_at=observed,
                market=spec.market,
                session_date=observed.date().isoformat(),
                signal_id=signal_id,
                counts=counts,
                reason_counts=reason_counts,
                entry_fill_count=entry_fill_count,
                entry_requested_shares=entry_requested_shares,
                entry_filled_shares=entry_filled_shares,
                entry_unfilled_shares=entry_unfilled_shares,
                entry_fill_outcome=entry_fill_outcome,
                entry_fill_policy=spec.entry_fill_policy,
                entry_price_offset_ticks=int(spec.entry_price_offset_ticks),
                entry_fill_is_synthetic=bool(entry_synthetic_fallback_fill_count),
                entry_best_quote_fill_count=entry_best_quote_fill_count,
                entry_synthetic_fallback_fill_count=(
                    entry_synthetic_fallback_fill_count
                ),
                entry_0901_vwap_fill_count=entry_0901_vwap_fill_count,
                entry_0901_close_fill_count=entry_0901_close_fill_count,
                entry_0901_minute_price_fill_count=(
                    entry_0901_minute_price_fill_count
                ),
                simulation_replay=bool(mode.get("simulation_replay")),
                replay_basis=mode.get("replay_basis"),
                source_signal_at=source_signal_at.isoformat(timespec="seconds"),
                open_reconstructed_at=(
                    signal_at.isoformat(timespec="seconds")
                    if counterfactual_open_replay
                    else None
                ),
                counterfactual_open_replay=bool(counterfactual_open_replay),
            )
            self._mark_mode(spec.market, observed, quotes)
            self._persist(observed)
        except Exception as exc:
            mode["ledger_state_divergence"] = {
                "kind": "signal_state_commit_failed",
                "signal_id": signal_id,
                "session_date": observed.date().isoformat(),
                "detected_at": observed.isoformat(timespec="seconds"),
                "error": f"{type(exc).__name__}: {exc}",
            }
            mode["engine_status"] = "critical_ledger_state_divergence"
            mode["readiness_error"] = (
                f"ledger_state_divergence:signal_state_commit_failed:{signal_id}"
            )
            raise
        return "registered"

    def _liquidate_prior_inventory(self, mode, quotes, now):
        """Old inventory is reduction-only, independently of today's inference."""
        if not LIVE_ENTRY_GATE <= now.time() < AUCTION_OBSERVATION_DEADLINE:
            return
        for p in (mode.get("positions") or {}).values():
            if not p.get("signed_shares") or str(p.get("session_date") or "") >= now.date().isoformat():
                continue
            quote = quotes.get(p["symbol"]) or {}
            side = "sell" if int(p["signed_shares"]) > 0 else "buy"
            lower, upper = _finite(quote.get("lower_limit")), _finite(quote.get("upper_limit"))
            if lower is None or upper is None:
                continue
            p.update(lower_limit=lower, upper_limit=upper, bracket_prices_current=True, mandatory_exit_pending=True)
            if now.time() >= CLOSING_AUCTION_TIME:
                if p.get("mandatory_auction_session_date") != now.date().isoformat():
                    self._order({"recorded_at": now.isoformat(), "session_date": now.date().isoformat(),
                        "market": mode["market"], "position_id": p["position_id"], "symbol": p["symbol"],
                        "order_id": f"{p['position_id']}:mandatory_auction:{now.date()}",
                        "purpose": "prior_day_mandatory_liquidation", "side": side,
                        "order_type": "LMT_ROD", "price": lower if side == "sell" else upper,
                        "quantity": abs(int(p["signed_shares"])), "status": "working", "simulation_only": True})
                    p["mandatory_auction_session_date"] = now.date().isoformat()
                    p["mandatory_auction_submitted_at"] = now.isoformat()
                if now.time() < SESSION_CLOSE:
                    continue
                quote = self._auction_execution_quote(quote)
                exchange_at = _parse_timestamp(quote.get("exchange_quote_at"))
                submitted_at = _parse_timestamp(p.get("mandatory_auction_submitted_at"))
                if (exchange_at is None or exchange_at.date() != now.date() or exchange_at > now
                        or submitted_at is None or exchange_at <= submitted_at
                        or not SESSION_CLOSE <= exchange_at.time() < time(13, 34)):
                    continue
                quote["minute_volume_lots"] = quote.get("auction_volume_lots")
                quote["capacity_bucket"] = f"{now.date()}:closing_auction:{p['symbol']}"
                price = _finite(quote.get("last"))
                capacity = _minute_kbar_capacity_shares(quote, lot_size=int(p["lot_size"]))
                order_type = "LMT_ROD"
            else:
                price = _finite(quote.get("bid" if side == "sell" else "ask"))
                capacity_fn = _top_book_capacity_shares if now.time() < FIRST_MINUTE_EXECUTION_TIME else _executable_capacity_shares
                capacity = capacity_fn(quote, transaction_side=side, lot_size=int(p["lot_size"]))
                order_type = "MKT"
            if (not self._fresh_regular_quote(quote, now) or price is None or not lower <= price <= upper
                    or not bool(price_on_tick_grid_numpy(np.array([price]), np.array([now.date()]),
                        security_types=p.get("security_type") or "stock")[0])):
                continue
            self._close_position(p, mode, price=price, quote=quote, now=now,
                reason="prior_day_mandatory_liquidation", order_type=order_type, quantity=capacity,
                ledger_session_date=now.date().isoformat(),
                ledger_order_id=f"{p['position_id']}:mandatory_exit:{now.isoformat()}")

    def _retry_entry_orders(self, mode, quotes, now):
        """Continue a frozen daily target; never re-infer, re-size, or reopen exits.

        Replenishment requires new cumulative trade volume, not another poll of
        identical depth. Fees, acquisition cost and the NAV budget are retained.
        A durable intent marker quarantines an interrupted append on restart.
        """
        orders = mode.get("pending_entry_orders") or {}
        if not orders or mode.get("intraday_contract") != STRICT_INTRADAY_CONTRACT:
            return
        for symbol, order in orders.items():
            if order.get("status") != "working":
                continue
            template = order["position"]
            p = mode["positions"].get(template["position_id"])
            if (mode.get("session_date") != now.date().isoformat() or now.time() >= EXIT_LIMIT_TIME
                    or p and (p.get("last_exit_at") or p.get("stop_triggered_at"))):
                order["status"] = "cancelled_exit_priority"
                continue
            quote = quotes.get(symbol) or {}
            signal_at = _parse_timestamp(mode.get("signal_at"))
            quote_at = _parse_timestamp(quote.get("quote_at"))
            if not self._fresh_regular_quote(quote, now) or signal_at is None or quote_at <= signal_at:
                continue
            side = template["side"]
            price = _finite(quote.get("ask" if side == "long" else "bid"))
            if price is None or not template["lower_limit"] <= price <= template["upper_limit"]:
                continue
            if not bool(price_on_tick_grid_numpy(np.array([price]), np.array([now.date()]),
                    security_types=template.get("security_type") or "stock")[0]):
                continue
            lot = int(template["lot_size"])
            cumulative = quote.get("cumulative_volume_lots")
            previous = order.get("last_cumulative_volume_lots")
            if cumulative is None or previous is None:
                order["last_cumulative_volume_lots"] = cumulative
                if p is not None or cumulative is None:
                    continue
                # This zero-fill target has never consumed an executable book.
                capacity = _top_book_capacity_shares(quote, transaction_side="buy" if side == "long" else "sell", lot_size=lot)
            else:
                delta = float(cumulative) - float(previous)
                if not math.isfinite(delta) or delta <= 0:
                    continue
                capacity = min(_top_book_capacity_shares(quote, transaction_side="buy" if side == "long" else "sell", lot_size=lot),
                               int(math.floor(delta * MINUTE_VOLUME_PARTICIPATION)) * lot)
            minute = now.strftime("%Y-%m-%dT%H:%M")
            used = int(order.get("minute_used_shares") or 0) if minute == order.get("minute") else 0
            if now.time() >= FIRST_MINUTE_EXECUTION_TIME:
                capacity = min(capacity, max(0, _minute_kbar_capacity_shares(quote, lot_size=lot) - used))
            rate = float(template["buy_fee_rate"] if side == "long" else template["sell_fee_rate"])
            rebate = float(template["commission_rebate_rate"])
            reserved = sum(abs(int(x.get("filled_shares") or 0)) * float(x["entry_price"])
                           + float(x.get("entry_fee_twd") or 0) for x in mode["positions"].values())
            budget = max(0., float(mode["session_sizing_nav_twd"]) - reserved
                         - float(mode.get("corporate_action_receivable_twd") or 0))
            affordable = int(budget / (price * (1 + rate - rebate)) / lot) * lot
            quantity = min(int(order["remaining_shares"]), capacity, affordable)
            if quantity <= 0:
                continue
            sequence = int(order["sequence"]) + 1
            order_id = f"{template['position_id']}:entry_retry:{sequence}"
            mode["entry_retry_pending_commit"] = order_id
            self._persist(now)
            # State is committed only after both append-only ledgers succeed.
            gross_fee, rebate_amount = quantity * price * rate, quantity * price * rebate
            fee = gross_fee - rebate_amount
            if p is None:
                p = dict(template)
                p.update(entry_at=now.isoformat(), entry_quote_at=quote.get("quote_at"), entry_price=price,
                         filled_shares=0, signed_shares=0, entry_fee_twd=0., remaining_entry_fee_twd=0.,
                         entry_gross_fee_and_tax_twd=0., entry_commission_rebate_accrued_twd=0.)
                mode["positions"][p["position_id"]] = p
            old = int(p["filled_shares"])
            p["entry_price"] = (old * float(p["entry_price"]) + quantity * price) / (old + quantity)
            p["filled_shares"] = old + quantity
            p["signed_shares"] = (1 if side == "long" else -1) * (old + quantity)
            p["executable_order_shares"] = old + quantity
            p["target_unsubmitted_shares"] = int(order["remaining_shares"]) - quantity
            for key, amount in (("entry_fee_twd", fee), ("remaining_entry_fee_twd", fee),
                                ("entry_gross_fee_and_tax_twd", gross_fee),
                                ("entry_commission_rebate_accrued_twd", rebate_amount)):
                p[key] = float(p.get(key) or 0) + amount
            common = {"recorded_at": now.isoformat(), "session_date": now.date().isoformat(),
                      "market": mode["market"], "signal_id": mode["signal_id"], "symbol": symbol,
                      "position_id": p["position_id"], "order_id": order_id, "purpose": "entry",
                      "quantity": quantity, "simulation_only": True, "entry_fill_policy": ENTRY_FILL_POLICY_CAUSAL_BOOK}
            self._order(common | {"order_type": "MKT", "price": None, "status": "filled",
                                  "side": "buy" if side == "long" else "sell_short"})
            self._fill(common | {"fill_at": now.isoformat(), "quote_at": quote.get("quote_at"),
                                 "price": price, "fee_and_tax_twd": fee,
                                 "gross_fee_and_tax_twd": gross_fee, "commission_rebate_accrued_twd": rebate_amount,
                                 "fill_contract": "causal_best_quote_remaining_order",
                                 "depth_assumption": "new_volume_replenishment_and_minute_budget"})
            mode["cumulative_commission_rebate_accrued_twd"] += rebate_amount
            mode["entry_fill_count"] += 1
            mode["entry_best_quote_fill_count"] += 1
            mode["entry_filled_shares"] += quantity
            mode["entry_unfilled_shares"] -= quantity
            mode["entry_fill_outcome"] = "filled" if not mode["entry_unfilled_shares"] else "partial"
            order.update(remaining_shares=int(order["remaining_shares"]) - quantity, sequence=sequence,
                         last_cumulative_volume_lots=cumulative, minute=minute, minute_used_shares=used + quantity)
            if not order["remaining_shares"]:
                order["status"] = "filled"
            mode.pop("entry_retry_pending_commit", None)
            self._persist(now)
        mode["pending_entry_shares"] = sum(int(o["remaining_shares"]) for o in orders.values() if o["status"] == "working")

    def _block_signal(
        self,
        mode: dict[str, Any],
        signal_id: str,
        reason: str,
        now: datetime,
        *,
        engine_status: str = "blocked_signal",
    ) -> str:
        processed = list(mode.get("processed_signal_ids") or ())
        processed.append(signal_id)
        mode["processed_signal_ids"] = processed[-32:]
        if reason == "daily_signal_already_consumed":
            mode["last_duplicate_signal_id"] = signal_id
            mode["last_duplicate_signal_reason"] = reason
            mode["last_duplicate_signal_blocked_at"] = now.isoformat(timespec="seconds")
            self._event(
                "signal_blocked",
                recorded_at=now,
                market=mode.get("market"),
                signal_id=signal_id,
                reason=reason,
            )
            self._persist(now)
            return "blocked"
        mode["signal_id"] = signal_id
        mode["engine_status"] = engine_status
        mode["blocked_reason"] = reason
        self._event(
            "signal_blocked",
            recorded_at=now,
            market=mode.get("market"),
            signal_id=signal_id,
            reason=reason,
        )
        self._persist(now)
        return "blocked"

    def process_quotes(
        self,
        *,
        quotes: Mapping[str, Mapping[str, Any]],
        now: datetime | None = None,
        append_mark_history: bool = True,
        markets: Iterable[str] | None = None,
        persist: bool = True,
    ) -> None:
        observed = _now_taipei(now)
        wall_time = observed.timetz().replace(tzinfo=None)
        selected_markets = None if markets is None else {str(value) for value in markets}
        for market, mode in self.state.get("modes", {}).items():
            if selected_markets is not None and str(market) not in selected_markets:
                continue
            if mode.get("ledger_state_divergence") or mode.get("entry_retry_pending_commit"):
                mode["engine_status"] = "critical_ledger_state_divergence"
                continue
            positions = mode.get("positions") or {}
            if (mode.get("configured_intraday_contract") == STRICT_INTRADAY_CONTRACT
                    and str(mode.get("session_date") or "") < observed.date().isoformat()):
                if not self._margin_corporate_action_gate(mode, observed):
                    continue
                self._accrue_margin_carry_cost(mode, observed)
                self._liquidate_prior_inventory(mode, quotes, observed)
                self._mark_mode(market, observed, quotes, append_history=False)
                if any(p.get("signed_shares") for p in positions.values()):
                    mode["engine_status"] = "critical_prior_inventory_liquidation"
                continue
            if mode.get("margin_carry_contract") == MARGIN_CARRY_CONTRACT:
                self._settle_corporate_action_claims(mode, observed)
                if not self._margin_corporate_action_gate(mode, observed):
                    continue
                self._accrue_margin_carry_cost(mode, observed)
                if str(mode.get("session_date") or "") != observed.date().isoformat():
                    # No stale prior-session brackets before the next decision.
                    self._mark_mode(market, observed, quotes, append_history=False)
                    if any(int(p.get("signed_shares") or 0) for p in positions.values()):
                        mode["engine_status"] = "margin_carried_waiting_next_signal"
                    continue
            if bool(mode.get("legacy_execution_contract")) and any(
                int(position.get("signed_shares") or 0) != 0
                for position in positions.values()
            ):
                # Schema-migrated positions may only be reduced.  Reuse the
                # ordinary bracket close path once a real executable quote and
                # completed-minute capacity exist; never open or enlarge a
                # legacy position merely to unblock the next session.
                for position in positions.values():
                    if int(position.get("signed_shares") or 0) == 0:
                        continue
                    quote = quotes.get(str(position.get("symbol"))) or {}
                    self._apply_bracket(position, mode, quote, observed)
                self._mark_mode(
                    market,
                    observed,
                    quotes,
                    append_history=append_mark_history,
                )
                still_open = any(
                    int(position.get("signed_shares") or 0) != 0
                    for position in positions.values()
                )
                if still_open:
                    mode["engine_status"] = (
                        "critical_legacy_position_requires_reconciliation"
                    )
                else:
                    mode["legacy_execution_contract"] = False
                    mode["legacy_reconciled_at"] = observed.isoformat(
                        timespec="seconds"
                    )
                    mode["engine_status"] = "waiting_signal"
                    self._event(
                        "legacy_positions_reconciled",
                        recorded_at=observed,
                        market=market,
                        session_date=observed.date().isoformat(),
                    )
                continue
            if wall_time < EXIT_LIMIT_TIME:
                for position in positions.values():
                    if int(position.get("signed_shares") or 0) == 0:
                        continue
                    quote = quotes.get(str(position.get("symbol"))) or {}
                    self._apply_bracket(position, mode, quote, observed)
            elif not mode.get("exit_limit_submitted_at"):
                self._submit_exit_limits(market, mode, quotes, observed)
            if EXIT_LIMIT_TIME <= wall_time < FORCE_EXIT_TIME:
                if mode.get("intraday_contract") == STRICT_INTRADAY_CONTRACT:
                    for position in positions.values():
                        self._apply_bracket(position, mode, quotes.get(position.get("symbol")) or {}, observed)
                self._fill_crossed_exit_limits(mode, quotes, observed)
            if FORCE_EXIT_TIME <= wall_time < CLOSING_AUCTION_TIME:
                self._force_exit(market, mode, quotes, observed)
            if CLOSING_AUCTION_TIME <= wall_time < SESSION_CLOSE:
                self._submit_closing_auction_limits(market, mode, observed)
            if wall_time >= SESSION_CLOSE:
                self._submit_closing_auction_limits(market, mode, observed)
                self._settle_closing_auction(market, mode, quotes, observed)
                self._convert_residual_to_carry(market, mode, quotes, observed)
            self._retry_entry_orders(mode, quotes, observed)
            self._mark_mode(
                market,
                observed,
                quotes,
                append_history=(append_mark_history or (
                    mode.get("intraday_contract") == STRICT_INTRADAY_CONTRACT
                    and wall_time >= SESSION_CLOSE)),
            )
            if (mode.get("margin_carry_contract") == MARGIN_CARRY_CONTRACT
                    and wall_time >= SESSION_CLOSE):
                self._archive_mode_positions(mode, archived_at=observed)
        if persist:
            self._persist(observed)

    def _apply_bracket(
        self,
        position: dict[str, Any],
        mode: dict[str, Any],
        quote: Mapping[str, Any],
        now: datetime,
    ) -> None:
        signed = int(position.get("signed_shares") or 0)
        if position.get("bracket_prices_current") is False:
            return
        if signed == 0:
            return
        strict = mode.get("intraday_contract") == STRICT_INTRADAY_CONTRACT
        if strict and not self._fresh_regular_quote(quote, now):
            position["exit_quote_status"] = "waiting_fresh_non_trial_quote"
            return
        bid = _finite(quote.get("bid"))
        ask = _finite(quote.get("ask"))
        last = _finite(quote.get("last"))
        side = str(position.get("side"))
        take_profit = float(position["take_profit_price"])
        stop = float(position["stop_trigger_price"])
        if strict:
            # A last trade can lag the executable side; either adverse price
            # triggers the stop. Once triggered, recovery cannot disarm it.
            adverse_prices = [p for p in (last, bid if side == "long" else ask) if p is not None]
            if any(p <= stop if side == "long" else p >= stop for p in adverse_prices):
                position.setdefault("stop_triggered_at", now.isoformat(timespec="seconds"))
                position["stop_order_status"] = "triggered_waiting_liquidity"
        tp_hit = (side == "long" and bid is not None and bid >= take_profit) or (
            side == "short" and ask is not None and ask <= take_profit
        )
        if strict and (position.get("stop_triggered_at") or now.time() >= EXIT_LIMIT_TIME):
            tp_hit = False
        if tp_hit:
            capacity = _executable_capacity_shares(
                quote,
                transaction_side="sell" if side == "long" else "buy",
                lot_size=int(position.get("lot_size") or 1_000),
            )
            if capacity <= 0:
                position["take_profit_order_status"] = "working_no_displayed_volume"
                return
            self._close_position(
                position,
                mode,
                price=take_profit,
                quote=quote,
                now=now,
                reason=(
                    "take_profit_inside_daily_limit_"
                    f"{int(position.get('price_limit_offset_ticks') or 0)}_tick"
                    if int(position.get("price_limit_offset_ticks") or 0) > 0
                    else "take_profit_full_price_limit"
                ),
                order_type="LMT",
                quantity=capacity,
            )
            return
        stop_reference = last if last is not None else bid if side == "long" else ask
        stop_hit = stop_reference is not None and (
            (side == "long" and stop_reference <= stop)
            or (side == "short" and stop_reference >= stop)
        )
        if not stop_hit and not str(position.get("stop_order_status") or "").startswith(
            "triggered"
        ):
            return
        position["stop_order_status"] = "triggered_waiting_liquidity"
        position.setdefault("stop_triggered_at", now.isoformat(timespec="seconds"))
        executable = bid if side == "long" else ask
        capacity_fn = _top_book_capacity_shares if strict and now.time() < FIRST_MINUTE_EXECUTION_TIME else _executable_capacity_shares
        capacity = capacity_fn(
            quote,
            transaction_side="sell" if side == "long" else "buy",
            lot_size=int(position.get("lot_size") or 1_000),
        )
        if executable is None or capacity <= 0:
            position["status"] = "stop_triggered_waiting_liquidity"
            return
        self._close_position(
            position,
            mode,
            price=executable,
            quote=quote,
            now=now,
            reason=(
                "stop_loss_inside_daily_limit_"
                f"{int(position.get('price_limit_offset_ticks') or 0)}_tick_trigger"
                if int(position.get("price_limit_offset_ticks") or 0) > 0
                else "stop_loss_full_price_limit_trigger"
            ),
            order_type="MKT",
            quantity=capacity,
        )

    def _submit_exit_limits(
        self,
        market: str,
        mode: dict[str, Any],
        quotes: Mapping[str, Mapping[str, Any]],
        now: datetime,
    ) -> None:
        mode["exit_limit_submitted_at"] = now.isoformat(timespec="seconds")
        for position in (mode.get("positions") or {}).values():
            quote = quotes.get(str(position.get("symbol"))) or {}
            self._place_exit_limit(
                market=market,
                mode=mode,
                position=position,
                quote=quote,
                now=now,
                purpose="13_20_exit_limit",
            )
        self._event("exit_limits_submitted", recorded_at=now, market=market)

    def _place_exit_limit(
        self,
        *,
        market: str,
        mode: dict[str, Any],
        position: dict[str, Any],
        quote: Mapping[str, Any],
        now: datetime,
        purpose: str,
    ) -> bool:
        signed = int(position.get("signed_shares") or 0)
        if signed == 0:
            return False
        side = str(position.get("side"))
        passive_price = _finite(quote.get("ask" if side == "long" else "bid"))
        position["take_profit_order_status"] = "cancelled_replaced_at_13_20"
        if passive_price is None:
            position["eod_limit_order_status"] = "not_submitted_no_quote"
            return False
        position["eod_limit_price"] = passive_price
        position["eod_limit_submitted_at"] = now.isoformat(timespec="seconds")
        position["eod_limit_order_status"] = "working"
        self._order(
            {
                "recorded_at": now.isoformat(timespec="seconds"),
                "session_date": mode.get("session_date"),
                "market": market,
                "position_id": position.get("position_id"),
                "symbol": position.get("symbol"),
                "order_id": f"{position.get('position_id')}:eod_limit",
                "purpose": purpose,
                "side": "sell" if side == "long" else "buy_to_cover",
                "order_type": "LMT",
                "price": passive_price,
                "quantity": abs(signed),
                "status": "working",
                "simulation_only": True,
                "pricing_rule": "passive_best_ask_for_sell_best_bid_for_buy",
            }
        )
        return True

    def _fill_crossed_exit_limits(
        self,
        mode: dict[str, Any],
        quotes: Mapping[str, Mapping[str, Any]],
        now: datetime,
    ) -> None:
        for position in (mode.get("positions") or {}).values():
            if int(position.get("signed_shares") or 0) == 0:
                continue
            quote = quotes.get(str(position.get("symbol"))) or {}
            if mode.get("intraday_contract") == STRICT_INTRADAY_CONTRACT and position.get("stop_triggered_at"):
                # Stop orders remain aggressive; do not replace them with a
                # passive 13:20 order after only a partial stop execution.
                continue
            if position.get("eod_limit_order_status") == "not_submitted_no_quote":
                self._place_exit_limit(
                    market=str(mode.get("market")),
                    mode=mode,
                    position=position,
                    quote=quote,
                    now=now,
                    purpose="13_20_exit_limit_late_quote",
                )
            if position.get("eod_limit_order_status") not in {"working", "part_filled"}:
                continue
            side = str(position.get("side"))
            limit_price = float(position["eod_limit_price"])
            bid = _finite(quote.get("bid"))
            ask = _finite(quote.get("ask"))
            crossed = (side == "long" and bid is not None and bid >= limit_price) or (
                side == "short" and ask is not None and ask <= limit_price
            )
            if crossed:
                execution_price = limit_price
                if str(quote.get("fill_contract") or "").startswith("historical_1m_ohlcv_"):
                    submitted = _parse_timestamp(position.get("eod_limit_submitted_at"))
                    # Right-labelled bars contain trades BEFORE their label.
                    # An order submitted at that label cannot match them.
                    if submitted is None or (now - submitted).total_seconds() < 60:
                        continue
                    opening = _finite(quote.get("historical_bar_open"))
                    high = _finite(quote.get("historical_bar_high"))
                    low = _finite(quote.get("historical_bar_low"))
                    if opening is None or high is None or low is None or not low <= opening <= high:
                        position["eod_limit_liquidity_status"] = "invalid_historical_price_range"
                        continue
                    execution_price = max(limit_price, opening) if side == "long" else min(limit_price, opening)
                    if not low <= execution_price <= high:
                        position["eod_limit_liquidity_status"] = "limit_not_reached_in_historical_range"
                        continue
                capacity = _executable_capacity_shares(
                    quote,
                    transaction_side="sell" if side == "long" else "buy",
                    lot_size=int(position.get("lot_size") or 1_000),
                )
                if capacity <= 0:
                    position["eod_limit_liquidity_status"] = "no_displayed_volume"
                    continue
                self._close_position(
                    position,
                    mode,
                    price=execution_price,
                    quote=quote,
                    now=now,
                    reason="13_20_limit_filled",
                    order_type="LMT",
                    quantity=capacity,
                )
                continue
            # The user's 13:20 contract is to remain at the passive best quote,
            # not merely to preserve the price observed exactly at 13:20. Once
            # per new minute, an unfilled order is cancel-replaced to the
            # current ask for a sell or bid for a buy-to-cover.
            passive_price = _finite(quote.get("ask" if side == "long" else "bid"))
            if passive_price is None or math.isclose(
                passive_price,
                limit_price,
                rel_tol=0.0,
                abs_tol=1e-12,
            ):
                continue
            previous_price = limit_price
            position["eod_limit_price"] = passive_price
            position["eod_limit_submitted_at"] = now.isoformat(timespec="seconds")
            position["eod_limit_order_status"] = "working"
            position["eod_limit_reprice_count"] = (
                int(position.get("eod_limit_reprice_count") or 0) + 1
            )
            self._order(
                {
                    "recorded_at": now.isoformat(timespec="seconds"),
                    "session_date": mode.get("session_date"),
                    "market": mode.get("market"),
                    "position_id": position.get("position_id"),
                    "symbol": position.get("symbol"),
                    "order_id": (
                        f"{position.get('position_id')}:eod_limit:"
                        f"reprice:{int(position['eod_limit_reprice_count'])}"
                    ),
                    "replaces_order_id": f"{position.get('position_id')}:eod_limit",
                    "purpose": "13_20_exit_limit_reprice",
                    "side": "sell" if side == "long" else "buy_to_cover",
                    "order_type": "LMT",
                    "previous_price": previous_price,
                    "price": passive_price,
                    "quantity": abs(int(position.get("signed_shares") or 0)),
                    "status": "working",
                    "simulation_only": True,
                    "pricing_rule": "follow_passive_best_until_13_24",
                }
            )

    def _force_exit(
        self,
        market: str,
        mode: dict[str, Any],
        quotes: Mapping[str, Mapping[str, Any]],
        now: datetime,
    ) -> None:
        if not mode.get("force_exit_started_at"):
            mode["force_exit_started_at"] = now.isoformat(timespec="seconds")
            self._event("force_exit_started", recorded_at=now, market=market)
        failures = 0
        for position in (mode.get("positions") or {}).values():
            if int(position.get("signed_shares") or 0) == 0:
                continue
            quote = quotes.get(str(position.get("symbol"))) or {}
            side = str(position.get("side"))
            executable = _finite(quote.get("bid" if side == "long" else "ask"))
            capacity = _force_exit_retry_capacity_shares(
                position,
                quote,
                transaction_side="sell" if side == "long" else "buy",
                lot_size=int(position.get("lot_size") or 1_000),
                now=now,
            )
            if executable is None or capacity <= 0:
                failures += 1
                position["status"] = "force_exit_unfilled_no_executable_depth"
                position["eod_limit_order_status"] = "cancelled_at_13_24"
                continue
            before = abs(int(position.get("signed_shares") or 0))
            self._close_position(
                position,
                mode,
                price=executable,
                quote=quote,
                now=now,
                reason="13_24_market_force_exit",
                order_type="MKT",
                quantity=capacity,
            )
            filled = before - abs(int(position.get("signed_shares") or 0))
            position["force_exit_minute_consumed_shares"] = (
                int(position.get("force_exit_minute_consumed_shares") or 0) + filled
            )
            if int(position.get("signed_shares") or 0) != 0:
                failures += 1
        mode["force_exit_failures"] = failures
        if failures:
            mode["engine_status"] = "critical_unflattened_after_13_24"

    def _submit_closing_auction_limits(
        self,
        market: str,
        mode: dict[str, Any],
        now: datetime,
    ) -> None:
        """Replace residuals with maximally marketable Limit ROD orders.

        TWSE/TPEx closing call auction does not accept market orders.  A long
        liquidation is therefore priced at the legal lower limit and a short
        cover at the legal upper limit.  The simulation still waits for the
        13:30 auction result and never treats the indicative book as a fill.
        """

        session_date = str(mode.get("session_date") or now.date().isoformat())
        already_submitted = _timestamp_is_for_session(
            mode.get("closing_auction_submitted_at"), session_date
        )
        if already_submitted and mode.get("intraday_contract") != STRICT_INTRADAY_CONTRACT:
            return
        if not already_submitted:
            mode["closing_auction_submitted_at"] = now.isoformat(timespec="seconds")
        submitted = 0
        for position in (mode.get("positions") or {}).values():
            signed = int(position.get("signed_shares") or 0)
            if signed == 0:
                continue
            if already_submitted and position.get("closing_auction_order_status") != "not_submitted_price_limit_unavailable":
                continue
            side = str(position.get("side"))
            limit_price = _finite(
                position.get("lower_limit" if side == "long" else "upper_limit")
            )
            position["eod_limit_order_status"] = "cancelled_replaced_at_13_25"
            if limit_price is None:
                position["closing_auction_order_status"] = (
                    "not_submitted_price_limit_unavailable"
                )
                continue
            position["closing_auction_limit_price"] = limit_price
            position["closing_auction_order_submitted_at"] = now.isoformat()
            position["closing_auction_order_status"] = "working"
            submitted += 1
            self._order(
                {
                    "recorded_at": now.isoformat(timespec="seconds"),
                    "session_date": mode.get("session_date"),
                    "market": market,
                    "position_id": position.get("position_id"),
                    "symbol": position.get("symbol"),
                    "order_id": f"{position.get('position_id')}:closing_auction",
                    "purpose": "13_25_closing_auction_force_exit",
                    "side": "sell" if side == "long" else "buy_to_cover",
                    "order_type": "LMT_ROD",
                    "price": limit_price,
                    "quantity": abs(signed),
                    "status": "working",
                    "simulation_only": True,
                    "pricing_rule": (
                        "lower_limit_for_sell_upper_limit_for_buy_during_call_auction"
                    ),
                }
            )
        if submitted or not already_submitted:
            self._event(
                "closing_auction_limits_submitted",
                recorded_at=now,
                market=market,
                submitted=submitted,
            )

    def _settle_closing_auction(
        self,
        market: str,
        mode: dict[str, Any],
        quotes: Mapping[str, Mapping[str, Any]],
        now: datetime,
    ) -> None:
        if mode.get("intraday_contract") == STRICT_INTRADAY_CONTRACT:
            self._settle_intraday_auction(market, mode, quotes, now)
            return
        session_date = str(mode.get("session_date") or now.date().isoformat())
        if _timestamp_is_for_session(
            mode.get("closing_auction_settled_at"), session_date
        ):
            return
        mode["closing_auction_settled_at"] = now.isoformat(timespec="seconds")
        for position in (mode.get("positions") or {}).values():
            if int(position.get("signed_shares") or 0) == 0:
                continue
            if position.get("closing_auction_order_status") != "working":
                continue
            quote = quotes.get(str(position.get("symbol"))) or {}
            if mode.get("execution_realism_contract") == EXECUTION_REALISM_CONTRACT:
                quote_time = _parse_timestamp(quote.get("quote_at"))
                if (quote_time is None or quote_time.date() != now.date()
                        or quote_time.timetz().replace(tzinfo=None) < SESSION_CLOSE
                        or quote_time > now
                        or "no_historical_auction_depth_claim" in str(quote.get("depth_assumption") or "")):
                    position["closing_auction_order_status"] = "unfilled_no_causal_auction_evidence"
                    continue
            close_price = _finite(quote.get("last"))
            capacity = _minute_kbar_capacity_shares(
                quote,
                lot_size=int(position.get("lot_size") or 1_000),
            )
            if close_price is None or capacity <= 0:
                position["closing_auction_order_status"] = (
                    "unfilled_no_close_or_minute_volume"
                )
                continue
            self._close_position(
                position,
                mode,
                price=close_price,
                quote=quote,
                now=now,
                reason="13_30_closing_auction_fill",
                order_type="LMT_ROD",
                quantity=capacity,
            )
            if int(position.get("signed_shares") or 0) == 0:
                position["closing_auction_order_status"] = "filled"
            else:
                position["closing_auction_order_status"] = "part_filled"
        self._event("closing_auction_settled", recorded_at=now, market=market)

    @staticmethod
    def _fresh_regular_quote(quote: Mapping[str, Any], now: datetime) -> bool:
        """Receipt freshness is not exchange event time, nor trial matching."""
        received = _parse_timestamp(quote.get("quote_at"))
        return bool(received is not None and received.date() == now.date()
                    and 0 <= (now - received).total_seconds() <= 10
                    and quote.get("simtrade") is False)

    @staticmethod
    def _auction_execution_quote(quote: Mapping[str, Any]) -> dict[str, Any]:
        result = dict(quote)
        if result.get("auction_volume_source") == "exchange_non_trial_tick":
            result["quote_at"] = result.get("trade_quote_at")
            result["simtrade"] = result.get("trade_simtrade")
        return result

    @staticmethod
    def _intraday_status(mode: Mapping[str, Any], now: datetime) -> str | None:
        if mode.get("configured_intraday_contract") != STRICT_INTRADAY_CONTRACT and mode.get("intraday_contract") != STRICT_INTRADAY_CONTRACT:
            return None
        opened = [p for p in (mode.get("positions") or {}).values() if p.get("signed_shares")]
        if opened and str(mode.get("session_date") or "") < now.date().isoformat():
            return "critical_prior_inventory_liquidation"
        if mode.get("intraday_contract") != STRICT_INTRADAY_CONTRACT:
            return "critical_legacy_carry_requires_review" if opened and now.time() >= SESSION_CLOSE else None
        if opened and now.time() >= SESSION_CLOSE:
            if not mode.get("closing_auction_settled_at"):
                return "waiting_closing_auction_evidence"
            if any(not p.get("margin_exception_evidence") for p in opened):
                return "critical_day_trade_exit_unresolved"
            return "critical_adverse_limit_exception"
        if int(mode.get("pending_entry_shares") or 0):
            return "entry_partial_retrying"
        return None

    def _settle_intraday_auction(self, market, mode, quotes, now) -> None:
        """Observe each auction until it is evidenced, including 13:33 delays.

        Never seal the session on the first empty response. Each symbol owns
        one auction capacity budget, shared across all acquisition cohorts.
        The observation deadline expires orders; it never manufactures fills.
        """
        if str(mode.get("session_date")) != now.date().isoformat():
            return
        expired = now.time() >= AUCTION_OBSERVATION_DEADLINE
        for p in (mode.get("positions") or {}).values():
            if not int(p.get("signed_shares") or 0):
                continue
            if expired:
                p["closing_auction_order_status"] = "expired_unresolved"
                continue
            quote = self._auction_execution_quote(quotes.get(p["symbol"]) or {})
            exchange_at = _parse_timestamp(quote.get("exchange_quote_at"))
            submitted_at = _parse_timestamp(p.get("closing_auction_order_submitted_at") or mode.get("closing_auction_submitted_at"))
            valid = (self._fresh_regular_quote(quote, now) and exchange_at is not None
                     and exchange_at.date() == now.date() and exchange_at <= now
                     and submitted_at is not None and exchange_at > submitted_at
                     and SESSION_CLOSE <= exchange_at.time() < time(13, 34))
            price = _finite(quote.get("last"))
            limit = _finite(p.get("closing_auction_limit_price"))
            volume = quote.get("auction_volume_lots")
            if not valid or price is None or limit is None or volume is None:
                if limit is not None:
                    p["closing_auction_order_status"] = "waiting_auction_evidence"
                continue
            if not (float(p["lower_limit"]) <= price <= float(p["upper_limit"])) or (
                int(p["signed_shares"]) > 0 and price < limit
                or int(p["signed_shares"]) < 0 and price > limit
            ):
                p["closing_auction_order_status"] = "waiting_legal_auction_price"
                continue
            if not bool(price_on_tick_grid_numpy(np.array([price]), np.array([now.date()]),
                    security_types=p.get("security_type") or "stock")[0]):
                p["closing_auction_order_status"] = "waiting_legal_auction_price"
                continue
            quote["minute_volume_lots"] = volume
            quote["exchange_match_at"] = exchange_at.isoformat()
            # One auction bucket also covers late delivery of the SAME print.
            quote["capacity_bucket"] = f"{now.date()}:closing_auction:{p['symbol']}"
            capacity = _minute_kbar_capacity_shares(quote, lot_size=int(p["lot_size"]))
            if capacity <= 0:
                p["closing_auction_order_status"] = "waiting_auction_volume"
                continue
            self._close_position(p, mode, price=price, quote=quote, now=now,
                reason="13_30_closing_auction_fill", order_type="LMT_ROD", quantity=capacity)
            p["closing_auction_order_status"] = "filled" if not p["signed_shares"] else "part_filled"
        residuals = [p for p in (mode.get("positions") or {}).values() if p.get("signed_shares")]
        mode["closing_auction_pending_count"] = len(residuals)
        if not residuals or expired:
            if not mode.get("closing_auction_settled_at"):
                mode["closing_auction_settled_at"] = now.isoformat(timespec="seconds")
                self._event("closing_auction_observation_completed", recorded_at=now,
                            market=market, residual_count=len(residuals), expired=expired)

    def _adverse_limit_carry_evidence(self, position, quote, now):
        """Missing quotes/capacity or a missed stop alone never authorize carry."""
        received = _parse_timestamp(quote.get("quote_at"))
        closed_book = (received is not None and received.date() == now.date()
                       and SESSION_CLOSE <= received.time() <= AUCTION_OBSERVATION_DEADLINE
                       and 0 <= (now - received).total_seconds() <= 300
                       and quote.get("simtrade") is False)
        if (not (self._fresh_regular_quote(quote, now) or closed_book)
                or position.get("bracket_prices_current") is False
                or quote.get("trade_simtrade", False) is not False
                or position.get("closing_auction_limit_price") is None):
            return None
        signed = int(position["signed_shares"])
        boundary = _finite(position.get("lower_limit" if signed > 0 else "upper_limit"))
        last = _finite(quote.get("last"))
        raw_depth = quote.get("bid_volume" if signed > 0 else "ask_volume")
        try:
            zero_depth = math.isfinite(float(raw_depth)) and float(raw_depth) == 0
        except (TypeError, ValueError):
            zero_depth = False
        if boundary is None or last is None or not math.isclose(last, boundary, abs_tol=1e-8, rel_tol=0) or not zero_depth:
            return None
        return {"reason": "adverse_limit_locked_no_counterparty", "observed_at": now.isoformat(),
                "quote_at": quote.get("quote_at"), "exchange_quote_at": quote.get("exchange_quote_at"),
                "limit_price": boundary, "last": last, "counterparty_volume_lots": 0,
                "stop_triggered_at": position.get("stop_triggered_at"),
                "stop_missed_before_limit": not bool(position.get("stop_triggered_at")),
                "simulation_only": True, "broker_conversion_confirmed": False}

    @staticmethod
    def _charge_margin_cost(mode: dict[str, Any], position: dict[str, Any], *,
                            amount: float, kind: str, charged_date: date) -> None:
        if not math.isfinite(amount) or amount < 0:
            raise ValueError("invalid margin carry charge")
        if not amount:
            return
        # Cost receipt and its accumulator are one atomic state.json commit.
        # Conversion is not a transaction and must never manufacture a fill.
        row = {"cost_id": f"{position['position_id']}:{kind}:{charged_date}",
               "position_id": position["position_id"], "symbol": position["symbol"],
               "date": charged_date.isoformat(), "kind": kind, "amount_twd": amount,
               "simulation_only": True, "assumption_contract": MARGIN_CARRY_CONTRACT}
        mode.setdefault("carry_cost_ledger", []).append(row)
        mode["cumulative_carry_cost_twd"] = float(mode.get("cumulative_carry_cost_twd") or 0) + amount
        position["margin_cost_twd"] = float(position.get("margin_cost_twd") or 0) + amount

    def _accrue_margin_carry_cost(self, mode: dict[str, Any], now: datetime) -> None:
        rates = mode.get("margin_cost_assumptions") or {}
        for position in (mode.get("positions") or {}).values():
            signed = int(position.get("signed_shares") or 0)
            if not signed or position.get("margin_carry_contract") != MARGIN_CARRY_CONTRACT:
                continue
            last = date.fromisoformat(position["margin_cost_accrued_through"])
            days = (now.date() - last).days
            if days <= 0:
                continue
            rate = (float(rates["financing_ratio"]) * float(rates["financing_annual_rate"])) if signed > 0 else float(rates["short_annual_borrow_rate"])
            amount = inventory_carry_interest(
                signed,
                float(position.get("inventory_basis_price", position["entry_price"])),
                rate,
                days,
            )
            self._charge_margin_cost(mode, position, amount=amount,
                kind="financing_interest" if signed > 0 else "margin_short_borrow_interest", charged_date=now.date())
            position["margin_cost_accrued_through"] = now.date().isoformat()

    def _margin_corporate_action_gate(self, mode: dict[str, Any], now: datetime) -> bool:
        """Never silently value carried physical shares through an ex-date.

        Recognize signed exact cash claims before the ex-date opening rebalance.
        Benchmark factors are not physical-share entitlements. Unknown/noncash
        actions remain a recoverable accounting wait, never an invented amount.
        """
        carried = [p for p in (mode.get("positions") or {}).values()
                   if int(p.get("signed_shares") or 0)
                   and p.get("margin_carry_contract") == MARGIN_CARRY_CONTRACT
                   and str(p.get("session_date") or "") < now.date().isoformat()]
        if not carried:
            return True
        receipt: dict[str, Any] = {"checked_session": now.date().isoformat(),
                                   "status": "blocked", "events": []}
        try:
            raw_path = mode.get("margin_corporate_action_reference_path")
            if not raw_path:
                raise ValueError("margin corporate-action reference not configured")
            path = Path(raw_path).resolve()
            from stockagent.live.tw_share_replacement import (
                ODD_LOT_BOARD_PRICE, SHARE_REPLACEMENT_CONTRACT, load_share_replacements,
            )
            # Stage physical changes until EVERY encountered action validates.
            # A later missing term must not leave a half-converted account.
            staged = [dict(p) for p in carried]
            share_actions, share_claims = [], []
            known_actions = {r["action_id"]: r for r in mode.get("share_replacement_ledger", [])}
            strict_physical = mode.get("odd_lot_execution_policy") == ODD_LOT_BOARD_PRICE
            replacements = load_share_replacements(
                path.parent,
                required_start=min(date.fromisoformat(p["session_date"]) for p in carried) if strict_physical else None,
                required_end=now.date() if strict_physical else None,
            )
            for p in staged:
                for event in replacements:
                    if (event["symbol"] != p["symbol"] or not
                            str(p["session_date"]) < str(event["suspension_date"]) <= now.date().isoformat()):
                        continue
                    receipt["blocking_domain"] = "corporate_action_accounting_not_quote_download"
                    if mode.get("odd_lot_execution_policy") != ODD_LOT_BOARD_PRICE:
                        raise ValueError("physical_share_replacement_requires_share_cash_and_odd_lot_accounting")
                    p["share_replacement_halted_until"] = str(event["resume_date"])
                    if now.date() < event["resume_date"]:
                        continue
                    if event.get("reference_only"):
                        raise ValueError(f"issuer replacement reference lacks complete physical accounting terms: {p['symbol']}")
                    action_id = f"{p['position_id']}:{event['resume_date']}:share_replacement"
                    ratio = float(event["new_shares_per_1000_old"]) / 1000.
                    cash = float(event["cash_return_per_old_share"])
                    if action_id in known_actions:
                        old = known_actions[action_id]
                        if old["ratio"] != ratio or old["cash_per_old_share"] != cash:
                            raise ValueError("previously applied share replacement terms revised")
                        if cash:
                            existing_cash = [c for c in mode.get("corporate_action_ledger", [])
                                             if c.get("share_action_id") == action_id]
                            if len(existing_cash) != 1 or existing_cash[0]["payment_date"] != str(event.get("cash_payment_date")):
                                raise ValueError("previously applied capital-return payment revised")
                        continue
                    payment = event.get("cash_payment_date")
                    if (not math.isfinite(ratio) or ratio <= 0 or not math.isfinite(cash) or cash < 0
                            or event.get("cash_dividend_per_old_share", 0) or event.get("subscription_shares_per_1000", 0)
                            or event.get("subscription_terms_present")
                            or (cash and (payment is None or payment < event["resume_date"]))):
                        raise ValueError(f"incomplete share replacement cash/subscription terms: {p['symbol']}")
                    old_quantity = int(p["signed_shares"])
                    new_quantity = old_quantity * ratio
                    if not math.isclose(new_quantity, round(new_quantity), abs_tol=1e-8, rel_tol=0):
                        raise ValueError("fraction below one share requires issuer cash-in-lieu terms")
                    new_quantity = int(round(new_quantity))
                    old_basis = float(p.get("inventory_basis_price", p["entry_price"]))
                    # Keep original invested principal; the cash return is a
                    # separate signed claim. Subtracting it from basis as well
                    # would count the same return of capital twice in NAV.
                    p["signed_shares"], p["inventory_basis_price"] = new_quantity, old_basis / ratio
                    p["odd_lot_execution_policy"] = ODD_LOT_BOARD_PRICE
                    p["share_replacement_contract"] = SHARE_REPLACEMENT_CONTRACT
                    last = _finite(p.get("last_mark_price"))
                    if last is not None:
                        p["last_mark_price"] = (last - cash) / ratio
                        if p["last_mark_price"] <= 0:
                            raise ValueError("share replacement creates invalid carried valuation")
                        p["last_complete_net_pnl_twd"] = position_net_liquidation_pnl(p, p["last_mark_price"])
                    p["valuation_stale"] = True
                    share_actions.append({"action_id": action_id, "position_id": p["position_id"],
                        "symbol": p["symbol"], "effective_date": str(event["resume_date"]),
                        "recorded_at": now.isoformat(), "old_signed_shares": old_quantity,
                        "new_signed_shares": new_quantity, "old_entry_price": old_basis,
                        "new_entry_price": p["inventory_basis_price"], "ratio": ratio,
                        "cash_per_old_share": cash, "contract": SHARE_REPLACEMENT_CONTRACT,
                        "source": str(path.parent / "tw_share_replacement_reference.parquet"),
                        "simulation_only": True, "is_fill": False})
                    if cash:
                        share_claims.append({"claim_id": action_id + ":cash_return",
                            "position_id": p["position_id"], "symbol": p["symbol"],
                            "ex_date": str(event["resume_date"]), "payment_date": str(payment),
                            "entitled_signed_shares": old_quantity, "cash_per_share": cash,
                            "amount_twd": old_quantity * cash, "kind": "signed_return_of_capital",
                            "recognized_at": now.isoformat(), "paid_at": None,
                            "contract": SHARE_REPLACEMENT_CONTRACT, "share_action_id": action_id})
            summary_path = path.with_suffix(".summary.json")
            entitlement_path = path.parent / "tw_corporate_action_entitlements.parquet"
            entitlement_summary = entitlement_path.with_suffix(".summary.json")
            has_entitlements = entitlement_path.is_file() or entitlement_summary.is_file()
            sources = [path, summary_path]
            if has_entitlements:
                sources += [entitlement_path, entitlement_summary]
            signature = (str(path), *[(p.stat().st_size, p.stat().st_mtime_ns,
                                      p.stat().st_ctime_ns) for p in sources])
            reference = self._margin_action_cache.get(signature)
            if reference is None:
                from stockagent.data.panel import (
                    _CorporateActionReferencePaths, _load_corporate_action_reference,
                )
                reference = _load_corporate_action_reference(
                    _CorporateActionReferencePaths(path, summary_path,
                        entitlement_path if has_entitlements else None,
                        entitlement_summary if has_entitlements else None))
                self._margin_action_cache = {signature: reference}
            today = np.datetime64(now.date(), "D")
            earliest = min(np.datetime64(str(p["session_date"]), "D") for p in carried)
            if reference.coverage_start > earliest or reference.coverage_end < today:
                raise ValueError("margin corporate-action coverage does not cover carried interval")
            events = set()
            claims = {str(row["claim_id"]): row for row in mode.get("corporate_action_ledger", ())}
            new_claims = []
            unresolved = []
            for p in staged:
                entry_day = np.datetime64(str(p["session_date"]), "D")
                for exdate in reference.event_dates_by_symbol.get(p["symbol"], ()):
                    if entry_day < exdate <= today:
                        day = str(exdate.astype("datetime64[D]"))
                        events.add((p["symbol"], day))
                        terms = (reference.exact_cash_terms_by_symbol or {}).get(p["symbol"])
                        matches = np.flatnonzero(terms[0] == exdate) if terms is not None else []
                        if (len(matches) != 1 or reference.exact_coverage_start is None
                                or not reference.exact_coverage_start <= exdate <= reference.exact_coverage_end):
                            unresolved.append({"symbol": p["symbol"], "ex_date": day})
                            continue
                        index = int(matches[0])
                        amount_per_share, payment = float(terms[1][index]), str(terms[2][index])
                        if payment < day:
                            raise ValueError("cash entitlement payment precedes ex-date")
                        claim_id = f"{p['position_id']}:{day}:cash_distribution"
                        existing = claims.get(claim_id)
                        if existing:
                            if (existing["cash_per_share"] != amount_per_share or existing["payment_date"] != payment):
                                raise ValueError(f"earned entitlement revised; reconciliation required: {claim_id}")
                            continue
                        new_claims.append({"claim_id": claim_id, "position_id": p["position_id"],
                            "symbol": p["symbol"], "ex_date": day, "payment_date": payment,
                            "entitled_signed_shares": int(p["signed_shares"]), "cash_per_share": amount_per_share,
                            "amount_twd": int(p["signed_shares"]) * amount_per_share,
                            "kind": "cash_distribution_receivable" if int(p["signed_shares"]) > 0 else "assumed_short_distribution_compensation",
                            "recognized_at": now.isoformat(), "paid_at": None,
                            "source": str(entitlement_path), "contract": "signed_cash_entitlement_exdate_paydate_v1",
                            "investor_personal_tax_included": False})
            receipt.update(reference_path=str(path), coverage_end=str(reference.coverage_end),
                           events=[{"symbol": symbol, "ex_date": day} for symbol, day in sorted(events)])
            if unresolved:
                receipt["unresolved_events"] = unresolved
                raise ValueError("physical_inventory_corporate_action_terms_unavailable_or_noncash")
            for original, updated in zip(carried, staged, strict=True):
                original.update(updated)
            mode.setdefault("share_replacement_ledger", []).extend(share_actions)
            mode.setdefault("corporate_action_ledger", []).extend([*share_claims, *new_claims])
            self._settle_corporate_action_claims(mode, now)
            receipt["new_cash_claims"] = len(new_claims)
            receipt["new_share_replacements"] = len(share_actions)
            receipt["status"] = "ready"
            mode["margin_corporate_action_receipt"] = receipt
            return True
        except (OSError, ValueError, RuntimeError) as exc:
            receipt["error"] = f"{type(exc).__name__}: {exc}"
            mode["margin_corporate_action_receipt"] = receipt
            mode["engine_status"] = "waiting_margin_corporate_action"
            mode["valuation_stale"] = True
            mode["valuation_complete"] = False
            return False

    @staticmethod
    def _settle_corporate_action_claims(mode: dict[str, Any], now: datetime) -> None:
        """Claims affect NAV once at ex-date; paying one only changes liquidity."""
        claims = mode.get("corporate_action_ledger") or []
        for claim in claims:
            if not claim.get("paid_at") and claim["payment_date"] <= now.date().isoformat():
                claim["paid_at"] = now.isoformat()
        mode["cumulative_corporate_action_net_twd"] = sum(float(c["amount_twd"]) for c in claims)
        mode["corporate_action_cash_net_twd"] = sum(float(c["amount_twd"]) for c in claims if c.get("paid_at"))
        mode["corporate_action_receivable_twd"] = sum(max(0.0, float(c["amount_twd"])) for c in claims if not c.get("paid_at"))
        mode["corporate_action_payable_twd"] = sum(max(0.0, -float(c["amount_twd"])) for c in claims if not c.get("paid_at"))

    def _reconcile_margin_targets(
        self, mode: dict[str, Any], plans: list[dict[str, Any]], *,
        quotes: Mapping[str, Mapping[str, Any]], spec: ModeSpec,
        signal_at: datetime, observed: datetime, allow_quote_at_signal: bool,
        order_records: list[dict[str, Any]], fill_records: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """FIFO reductions before additions, one capacity budget per symbol.

        Existing financed lots retain basis, fees and entry identity. Reducing
        owned exposure needs an executable price, not new-entry eligibility.
        """
        by_symbol = {p["symbol"]: p for p in plans}
        holdings: dict[str, list[dict[str, Any]]] = {}
        for p in mode["positions"].values():
            if int(p.get("signed_shares") or 0):
                holdings.setdefault(p["symbol"], []).append(p)
        nav = float(mode["session_sizing_nav_twd"])
        for symbol, lots in holdings.items():
            quote = quotes.get(symbol) or {}
            plan = by_symbol.get(symbol)
            if plan is None:
                plan = _prepare_entry_plan({"symbol": symbol, "target_weight": 0.0},
                    quote=quote, evidence=None, signal_at=signal_at, observation_at=observed,
                    spec=spec, sizing_nav_twd=nav, allow_quote_at_signal=allow_quote_at_signal)
                plans.append(plan)
            held = sum(int(p["signed_shares"]) for p in lots)
            if held % spec.lot_size and spec.odd_lot_execution_policy == "assumed_odd_lot_at_regular_board_price_v1":
                plan["quantity_step_shares"] = 1
            if any(int(p["signed_shares"]) * held <= 0 for p in lots):
                raise ValueError(f"opposed inventory cohorts: {symbol}")
            target = int(plan["requested_shares"]) * (1 if plan["side"] == "long" else -1)
            plan.update(previous_signed_shares=held, target_signed_shares=target)
            reduce = max(0, abs(held) - abs(target)) if held * target > 0 else abs(held)
            reduction_filled = 0
            if reduce:
                reducing = _prepare_entry_plan({"symbol": symbol, "target_weight": -1.0 if held > 0 else 1.0},
                    quote=quote, evidence=None, signal_at=signal_at, observation_at=observed,
                    spec=spec, sizing_nav_twd=nav, allow_quote_at_signal=allow_quote_at_signal,
                    requested_shares_override=reduce, reduce_only=True)
                left = int(reducing["filled_shares"])
                for p in sorted(lots, key=lambda p: str(p["entry_at"])):
                    take = min(left, abs(int(p["signed_shares"])))
                    if take:
                        before = abs(int(p["signed_shares"]))
                        self._close_position(p, mode, price=float(reducing["entry_price"]),
                            quote=({**quote, "fill_contract": REPLAY_FILL_CONTRACT_0901_MINUTE_PRICE}
                                   if allow_quote_at_signal else quote),
                            now=observed, reason="next_signal_inventory_delta",
                            order_type="PAPER_0901_MINUTE_PRICE" if allow_quote_at_signal else "MKT",
                            quantity=take, ledger_session_date=observed.date().isoformat(),
                            ledger_order_id=f"{p['position_id']}:{observed.date()}:target_delta",
                            ledger_rows=(order_records, fill_records))
                        actual = before - abs(int(p["signed_shares"]))
                        reduction_filled += actual
                        left -= actual
                plan["reduction_block_reason"] = reducing["reason"] if reduction_filled < reduce else None
            remaining = held - (reduction_filled if held > 0 else -reduction_filled)
            addition = max(0, abs(target) - abs(remaining)) if target * remaining >= 0 else 0
            # A failed reduction may not be bypassed by opening the other side.
            if remaining and target * remaining < 0:
                addition = 0
            plan.update(reduction_requested_shares=reduce, reduction_filled_shares=reduction_filled)
            plan["requested_shares"] = addition
            capacity = int(plan["minute_kbar_capacity_shares"])
            if spec.entry_fill_policy == ENTRY_FILL_POLICY_CAUSAL_BOOK:
                capacity = int(plan["top_book_capacity_shares"]) if observed.time() < time(9, 1) else min(capacity, int(plan["top_book_capacity_shares"]))
            if plan["status"] not in {"ready", "partial_depth"}:
                capacity = 0
            capacity = max(0, capacity - reduction_filled)
            plan["filled_shares"] = min(addition, capacity)
            if not plan["filled_shares"]:
                plan["status"] = "hold" if reduction_filled == reduce else "blocked"
                plan["reason"] = "inventory_target_retained" if reduction_filled == reduce else "inventory_reduction_incomplete"
            elif plan["filled_shares"] < addition:
                plan["status"] = "partial_depth"
            for p in lots:
                if not int(p.get("signed_shares") or 0):
                    continue
                p["current_target_signal_id"] = mode["signal_id"]
                p["target_weight"] = plan["target_weight"]
                p["status"] = "margin_inventory_active"
                p["take_profit_order_status"] = "working"
                p["stop_order_status"] = "armed_local_trigger"
                p["eod_limit_order_status"] = None
                p["closing_auction_order_status"] = None
                # Today's limits, never yesterday's bracket across a gap.
                upper, lower = _finite(quote.get("upper_limit")), _finite(quote.get("lower_limit"))
                if upper is None or lower is None:
                    p["bracket_prices_current"] = False
                    continue
                p["bracket_prices_current"] = True
                p["upper_limit"], p["lower_limit"] = upper, lower
                security_type = p.get("security_type") or classify_tw_stock_or_etf(p["symbol"]) or "stock"
                upper_inner = float(move_price_ticks_numpy(np.array([upper]), -spec.price_limit_offset_ticks, np.array([observed.date()]), security_types=security_type)[0])
                lower_inner = float(move_price_ticks_numpy(np.array([lower]), spec.price_limit_offset_ticks, np.array([observed.date()]), security_types=security_type)[0])
                p["take_profit_price"] = upper_inner if int(p["signed_shares"]) > 0 else lower_inner
                p["stop_trigger_price"] = lower_inner if int(p["signed_shares"]) > 0 else upper_inner
        return plans

    def _convert_residual_to_carry(
        self,
        market: str,
        mode: dict[str, Any],
        quotes: Mapping[str, Mapping[str, Any]],
        now: datetime,
    ) -> None:
        """Preserve residuals, optionally under the explicit margin assumption.

        Only obsolete compatibility contracts use synthetic terminal valuation.
        Conversion retains quantity, acquisition basis and mark-to-market risk.
        """

        session_date = str(mode.get("session_date") or now.date().isoformat())
        if _timestamp_is_for_session(
            mode.get("residual_conversion_completed_at"), session_date
        ):
            return
        strict = mode.get("intraday_contract") == STRICT_INTRADAY_CONTRACT
        if strict and not _timestamp_is_for_session(mode.get("closing_auction_settled_at"), session_date):
            return
        if mode.get("execution_realism_contract") == EXECUTION_REALISM_CONTRACT:
            residuals = [p for p in (mode.get("positions") or {}).values() if int(p.get("signed_shares") or 0)]
            for position in residuals:
                exception = self._adverse_limit_carry_evidence(position, quotes.get(position["symbol"]) or {}, now) if strict else None
                if strict:
                    position["margin_exception_evidence"] = exception
                    position["mandatory_exit_pending"] = True
                if mode.get("margin_carry_contract") == MARGIN_CARRY_CONTRACT and (not strict or exception):
                    if position.get("margin_carry_contract") != MARGIN_CARRY_CONTRACT:
                        signed = int(position["signed_shares"])
                        notional = abs(signed) * float(position["entry_price"])
                        if signed < 0:
                            tax_delta = max(0.0, float(position["cash_sell_fee_rate"]) - float(position["sell_fee_rate"]))
                            self._charge_margin_cost(mode, position,
                                amount=notional * (tax_delta + float(mode["margin_cost_assumptions"]["short_handling_fee_rate"])),
                                kind="short_conversion_tax_and_handling", charged_date=now.date())
                        position["margin_carry_contract"] = MARGIN_CARRY_CONTRACT
                        position["margin_converted_at"] = now.isoformat(timespec="seconds")
                        position["margin_cost_accrued_through"] = now.date().isoformat()
                    position["carry_type"] = "assumed_margin_financing" if int(position["signed_shares"]) > 0 else "assumed_margin_short"
                    position["status"] = "exception_adverse_limit_carry" if strict else "margin_carried_waiting_next_signal"
                else:
                    position["carry_type"] = "unresolved_day_trade_delivery_obligation"
                    position["status"] = "unfilled_at_session_close"
                position["fill_guaranteed"] = False
            mode["residual_conversion_completed_at"] = now.isoformat(timespec="seconds")
            mode["force_exit_failures"] = len(residuals)
            mode["execution_evidence_complete"] = not residuals
            mode["margin_carry_position_count"] = sum(p.get("margin_carry_contract") == MARGIN_CARRY_CONTRACT for p in residuals)
            mode["margin_exception_count"] = sum(bool(p.get("margin_exception_evidence")) for p in residuals)
            mode["unresolved_exit_count"] = sum(not p.get("margin_exception_evidence") for p in residuals) if strict else 0
            self._event("residual_delivery_obligation_unresolved" if residuals else "session_closed_with_observed_fills",
                        recorded_at=now, market=market, residual_count=len(residuals), simulation_only=True)
            return
        flattened = 0
        degraded = 0
        for position in (mode.get("positions") or {}).values():
            signed = int(position.get("signed_shares") or 0)
            if signed == 0:
                continue
            quote = quotes.get(str(position.get("symbol"))) or {}
            close_price = _finite(quote.get("last"))
            side = str(position.get("side"))
            price_source = "observed_session_close"
            if close_price is None:
                close_price = _finite(
                    position.get("lower_limit" if side == "long" else "upper_limit")
                )
                price_source = "adverse_daily_limit_fallback"
                degraded += 1
            if close_price is None:
                close_price = _finite(position.get("last_mark_price"))
                price_source = "last_complete_mark_fallback"
            if close_price is None:
                close_price = _finite(position.get("entry_price"))
                price_source = "entry_price_last_resort"
            if close_price is None:
                raise RuntimeError(
                    "terminal day-trade flatten has no positive ledger price "
                    f"for {market}:{position.get('symbol')}"
                )
            position["terminal_flatten_price_source"] = price_source
            position["terminal_flatten_simulation_only"] = True
            self._close_position(
                position,
                mode,
                price=close_price,
                quote=quote,
                now=now,
                reason="13_30_terminal_ledger_flatten",
                order_type="SIM_TERMINAL",
                quantity=abs(signed),
            )
            position["closing_auction_order_status"] = "terminal_ledger_flattened"
            flattened += 1
        mode["residual_conversion_completed_at"] = now.isoformat(timespec="seconds")
        mode["terminal_flatten_count"] = flattened
        mode["terminal_flatten_degraded_count"] = degraded
        mode["force_exit_failures"] = 0
        self._event(
            "residual_positions_terminal_flattened",
            recorded_at=now,
            market=market,
            flattened=flattened,
            degraded=degraded,
            simulation_only=True,
        )

    def _close_position(
        self,
        position: dict[str, Any],
        mode: dict[str, Any],
        *,
        price: float,
        quote: Mapping[str, Any],
        now: datetime,
        reason: str,
        order_type: str,
        quantity: int,
        ledger_order_id: str | None = None,
        ledger_session_date: str | None = None,
        ledger_rows: tuple[list[dict[str, Any]], list[dict[str, Any]]] | None = None,
    ) -> None:
        signed = int(position.get("signed_shares") or 0)
        if signed == 0:
            return
        if mode.get("intraday_contract") == STRICT_INTRADAY_CONTRACT:
            if not self._fresh_regular_quote(quote, now):
                position["exit_quote_status"] = "waiting_fresh_non_trial_quote"
                return
            lower, upper = _finite(position.get("lower_limit")), _finite(position.get("upper_limit"))
            if lower is None or upper is None or not lower <= price <= upper:
                position["exit_quote_status"] = "invalid_execution_price_limits"
                return
        if str(position.get("share_replacement_halted_until") or "") > now.date().isoformat():
            return
        if (position.get("bracket_prices_current") is False
                and reason.startswith(("take_profit", "stop_loss"))):
            return
        remaining_before = abs(signed)
        fill_quantity = min(max(int(quantity), 0), remaining_before)
        if mode.get("margin_carry_contract") == MARGIN_CARRY_CONTRACT or mode.get("intraday_contract") == STRICT_INTRADAY_CONTRACT:
            # Multiple carried acquisition cohorts share ONE source budget.
            quote_at = _parse_timestamp(quote.get("quote_at"))
            if quote_at is None or quote_at.date() != now.date() or quote_at > now:
                return
            key = str(quote.get("capacity_bucket") or f"{now.date()}:{quote_at.strftime('%H:%M')}:{position['symbol']}")
            budget = _minute_kbar_capacity_shares(quote, lot_size=int(position["lot_size"]))
            if now.time() < time(9, 1):
                key += quote_at.isoformat()
                budget = _top_book_capacity_shares(quote, transaction_side="sell" if signed > 0 else "buy", lot_size=int(position["lot_size"]))
            usage = mode.setdefault("margin_exit_capacity_used", {})
            fill_quantity = min(fill_quantity, max(0, budget - int(usage.get(key) or 0)))
            usage[key] = int(usage.get(key) or 0) + fill_quantity
        if fill_quantity <= 0:
            return
        self._book_position_close(
            position, mode, price=price, quote=quote, now=now, reason=reason,
            order_type=order_type, quantity=fill_quantity, ledger_order_id=ledger_order_id,
            ledger_session_date=ledger_session_date, ledger_rows=ledger_rows,
        )

    def _book_position_close(
        self, position: dict[str, Any], mode: dict[str, Any], *, price: float,
        quote: Mapping[str, Any], now: datetime, reason: str, order_type: str,
        quantity: int, ledger_order_id: str | None = None,
        ledger_session_date: str | None = None,
        ledger_rows: tuple[list[dict[str, Any]], list[dict[str, Any]]] | None = None,
    ) -> None:
        """Accounting only, after execution or an explicit offline settlement gate.

        Live execution must use _close_position for causal/liquidity checks.
        The offline official-close reconciliation records its distinct contract,
        never a quote, broker order, or historically observable exchange fill.
        """
        signed = int(position.get("signed_shares") or 0)
        remaining_before = abs(signed)
        fill_quantity = int(quantity)
        if not math.isfinite(price) or price <= 0 or not 0 < fill_quantity <= remaining_before:
            raise ValueError("invalid close accounting quantity or price")
        side = str(position.get("side"))
        direction = 1 if signed > 0 else -1
        prefix = "cash_" if position.get("margin_carry_contract") else ""
        exit_rate = float(position[f"{prefix}sell_fee_rate"] if side == "long" else position[f"{prefix}buy_fee_rate"])
        rebate_rate = float(position.get("commission_rebate_rate") or 0.0)
        exit_gross_fee = fill_quantity * float(price) * exit_rate
        exit_rebate = fill_quantity * float(price) * rebate_rate
        exit_fee = exit_gross_fee - exit_rebate
        gross_pnl = (
            direction * fill_quantity * (float(price) - float(position.get("inventory_basis_price", position["entry_price"])))
        )
        remaining_entry_fee = float(
            position.get("remaining_entry_fee_twd", position.get("entry_fee_twd", 0.0))
            or 0.0
        )
        entry_fee_allocated = (
            remaining_entry_fee
            if fill_quantity == remaining_before
            else remaining_entry_fee * fill_quantity / remaining_before
        )
        net_pnl = gross_pnl - entry_fee_allocated - exit_fee
        remaining_after = remaining_before - fill_quantity
        realized_gross = (
            float(position.get("realized_gross_pnl_twd") or 0.0) + gross_pnl
        )
        realized_exit_fee = (
            float(position.get("realized_exit_fee_twd") or 0.0) + exit_fee
        )
        realized_net = float(position.get("realized_net_pnl_twd") or 0.0) + net_pnl
        fully_closed = remaining_after == 0
        position.update(
            {
                "signed_shares": direction * remaining_after,
                "status": "closed" if fully_closed else "partially_closed",
                "last_exit_at": now.isoformat(timespec="seconds"),
                "last_exit_quote_at": quote.get("quote_at"),
                "last_exit_price": float(price),
                "last_exit_quantity": fill_quantity,
                "remaining_entry_fee_twd": remaining_entry_fee - entry_fee_allocated,
                "exit_fee_twd": realized_exit_fee,
                "gross_pnl_twd": realized_gross,
                "net_pnl_twd": realized_net,
                "realized_gross_pnl_twd": realized_gross,
                "realized_exit_fee_twd": realized_exit_fee,
                "realized_net_pnl_twd": realized_net,
                "exit_reason": reason,
                "last_complete_net_pnl_twd": 0.0
                if fully_closed
                else position.get("last_complete_net_pnl_twd", 0.0),
                "valuation_stale": False,
            }
        )
        if fully_closed:
            position.update(
                {
                    "exit_at": now.isoformat(timespec="seconds"),
                    "exit_quote_at": quote.get("quote_at"),
                    "exit_price": float(price),
                    "take_profit_order_status": "filled"
                    if reason.startswith("take_profit")
                    else "cancelled_oco",
                    "stop_order_status": "filled"
                    if reason.startswith("stop_loss")
                    else "cancelled_oco",
                    "eod_limit_order_status": "filled"
                    if reason == "13_20_limit_filled"
                    else "cancelled_oco",
                    "closing_auction_order_status": "filled"
                    if reason == "13_30_closing_auction_fill"
                    else position.get("closing_auction_order_status"),
                    # A flat position has no remaining liquidation component.
                    # Keep the cached total aligned with the final realized
                    # result instead of leaving the preceding minute mark.
                    "total_net_pnl_twd": realized_net,
                }
            )
        elif reason.startswith("take_profit"):
            position["take_profit_order_status"] = "part_filled"
        elif reason.startswith("stop_loss"):
            position["stop_order_status"] = "triggered_part_filled"
            position["status"] = "stop_triggered_partially_filled"
        elif reason == "13_20_limit_filled":
            position["eod_limit_order_status"] = "part_filled"
        elif reason == "13_24_market_force_exit":
            position["eod_limit_order_status"] = "cancelled_at_13_24"
            position["status"] = "force_exit_partially_filled"
        elif reason == "13_30_closing_auction_fill":
            position["closing_auction_order_status"] = "part_filled"
            position["status"] = "closing_auction_partially_filled"
        mode["cumulative_realized_net_pnl_twd"] = (
            float(mode.get("cumulative_realized_net_pnl_twd") or 0.0) + net_pnl
        )
        mode["cumulative_commission_rebate_accrued_twd"] = (
            float(mode.get("cumulative_commission_rebate_accrued_twd") or 0.0)
            + exit_rebate
        )
        order_id = ledger_order_id or (
            f"{position.get('position_id')}:{reason}:{remaining_before}"
        )
        common = {
            "recorded_at": now.isoformat(timespec="seconds"),
            "session_date": ledger_session_date or mode.get("session_date"),
            "market": position.get("market"),
            "position_id": position.get("position_id"),
            "symbol": position.get("symbol"),
            "order_id": order_id,
            "purpose": reason,
            "quantity": fill_quantity,
            "requested_quantity": remaining_before,
            "remaining_quantity": remaining_after,
            "simulation_only": True,
            "synthetic_terminal_ledger": (reason == "13_30_terminal_ledger_flatten"),
            "odd_lot_execution_policy": (position.get("odd_lot_execution_policy")
                                         if fill_quantity % int(position.get("lot_size") or 1000) else None),
        }
        append_order = self._order if ledger_rows is None else ledger_rows[0].append
        append_fill = self._fill if ledger_rows is None else ledger_rows[1].append
        append_order(
            {
                **common,
                "side": "sell" if side == "long" else "buy_to_cover",
                "order_type": order_type,
                "price": None if order_type == "MKT" else float(price),
                "status": "filled" if fully_closed else "part_filled",
            }
        )
        append_fill(
            {
                **common,
                "fill_at": now.isoformat(timespec="seconds"),
                "quote_at": quote.get("quote_at"),
                "exchange_match_at": quote.get("exchange_match_at"),
                "price": float(price),
                "fee_and_tax_twd": exit_fee,
                "gross_fee_and_tax_twd": exit_gross_fee,
                "commission_rebate_accrued_twd": exit_rebate,
                "entry_fee_allocated_twd": entry_fee_allocated,
                "gross_pnl_twd": gross_pnl,
                "net_pnl_twd": net_pnl,
                "fill_contract": (
                    str(quote.get("fill_contract"))
                    if quote.get("fill_contract")
                    else "simulation_terminal_ledger_not_exchange_fill"
                    if reason == "13_30_terminal_ledger_flatten"
                    else "best_bid_for_sell_best_ask_for_buy"
                ),
                "depth_assumption": (
                    str(quote.get("depth_assumption"))
                    if quote.get("depth_assumption")
                    else "full_residual_ledger_close_ignores_displayed_depth"
                    if reason == "13_30_terminal_ledger_flatten"
                    else "minimum_of_level_one_and_50pct_minute_volume_except_closing_auction_which_uses_50pct_auction_minute_volume"
                ),
            }
        )

    def _mark_mode(
        self,
        market: str,
        now: datetime,
        quotes: Mapping[str, Mapping[str, Any]],
        *,
        append_history: bool = True,
    ) -> None:
        mode = self.state.get("modes", {}).get(market)
        if not isinstance(mode, dict):
            return
        open_net = 0.0
        stale_count = 0
        open_count = 0
        total_notional = 0.0
        fresh_notional = 0.0
        missing_count = 0
        for position in (mode.get("positions") or {}).values():
            signed = int(position.get("signed_shares") or 0)
            if signed == 0:
                continue
            open_count += 1
            quote = quotes.get(str(position.get("symbol"))) or {}
            side = str(position.get("side"))
            if str(position.get("share_replacement_halted_until") or "") > now.date().isoformat():
                quote = {}
            liquidation = _finite(quote.get("bid" if side == "long" else "ask"))
            if mode.get("historical_minute_valuation") and quote.get("historical_minute_valuation"):
                liquidation = _finite(quote.get("valuation_price_0901"))
            mark_price = liquidation or _finite(position.get("last_mark_price"))
            if mark_price is not None:
                total_notional += abs(signed) * mark_price
            else:
                missing_count += 1
            if liquidation is None:
                stale_count += 1
                position["valuation_stale"] = True
                # A stale PRICE can be carried, but its cached PnL cannot:
                # quantity, allocated entry fees or the cash fee regime may
                # have changed since the last quote.
                stale_net = (position_net_liquidation_pnl(position, mark_price)
                             if mark_price is not None else float(position.get("last_complete_net_pnl_twd") or 0.0))
                position["last_complete_net_pnl_twd"] = stale_net
                position["total_net_pnl_twd"] = float(position.get("realized_net_pnl_twd") or 0.0) + stale_net
                open_net += stale_net
                continue
            fresh_notional += abs(signed) * liquidation
            net_pnl = position_net_liquidation_pnl(position, liquidation)
            position["last_mark_at"] = now.isoformat(timespec="seconds")
            position["last_quote_at"] = quote.get("quote_at")
            position["last_mark_price"] = liquidation
            position["last_complete_net_pnl_twd"] = net_pnl
            position["total_net_pnl_twd"] = (
                float(position.get("realized_net_pnl_twd") or 0.0) + net_pnl
            )
            position["valuation_stale"] = False
            open_net += net_pnl
        cumulative = float(mode.get("cumulative_realized_net_pnl_twd") or 0.0)
        total_equity = (
            float(mode.get("initial_capital_twd") or 0.0) + cumulative + open_net
            + float(mode.get("cumulative_corporate_action_net_twd") or 0.0)
            - float(mode.get("cumulative_carry_cost_twd") or 0.0)
        )
        flat_status = "flat"
        if str(mode.get("session_date") or "") == now.date().isoformat() and mode.get(
            "entry_completed_at"
        ):
            if mode.get("positions"):
                flat_status = "session_flat_after_exit"
            elif (
                int(mode.get("entry_requested_shares") or 0) > 0
                and int(mode.get("entry_fill_count") or 0) == 0
            ):
                flat_status = "entry_price_missing_no_fill"
            else:
                flat_status = "flat_no_executable_signal"
        mode.update(
            {
                "open_position_count": open_count,
                "stale_position_count": stale_count,
                "open_net_liquidation_pnl_twd": open_net,
                "total_equity_twd": total_equity,
                "last_mark_at": now.isoformat(timespec="seconds"),
                "valuation_stale": stale_count > 0,
                "valuation_complete": missing_count == 0,
                "engine_status": (
                    "margin_carried_waiting_next_signal"
                    if open_count and mode.get("margin_carry_contract") == MARGIN_CARRY_CONTRACT
                    and now.timetz().replace(tzinfo=None) >= SESSION_CLOSE
                    else
                    "critical_residual_carried_after_13_30"
                    if open_count
                    and any(
                        position.get("carry_type") == "unresolved_day_trade_delivery_obligation"
                        for position in (mode.get("positions") or {}).values()
                        if int(position.get("signed_shares") or 0) != 0
                    )
                    else "critical_unflattened_after_13_24"
                    if now.timetz().replace(tzinfo=None) >= FORCE_EXIT_TIME
                    and open_count
                    else "active"
                    if open_count
                    else flat_status
                ),
            }
        )
        strict_status = self._intraday_status(mode, now)
        if strict_status:
            mode["engine_status"] = strict_status
        if not append_history:
            return
        mark_at = now.replace(second=0, microsecond=0)
        curve_clock = mark_at.timetz().replace(tzinfo=None)
        same_session = str(mode.get("session_date") or now.date().isoformat()) == now.date().isoformat()
        settled_at = str(mode.get("closing_auction_settled_at") or "")
        try:
            settled_time = datetime.fromisoformat(settled_at)
            settled_valid = settled_time.tzinfo is not None and settled_time <= now and settled_time.astimezone(now.tzinfo).date() == now.date()
        except ValueError:
            settled_valid = False
        settled_flat = same_session and not open_count and settled_valid
        terminal = same_session and curve_clock >= SESSION_CLOSE
        if terminal and settled_flat:
            # A delayed loop/restart may publish a proven flat settlement NAV
            # at the session endpoint. No price, fill, or interior bar is made up.
            mark_at = now.replace(hour=13, minute=30, second=0, microsecond=0)
            curve_clock = SESSION_CLOSE
        if not same_session or not ENTRY_GATE <= curve_clock <= SESSION_CLOSE:
            # The operational state may be marked immediately after a 09:00
            # signal or during post-close reconciliation, but the canonical
            # strategy curve is the 270 right-labelled minutes 09:01..13:30.
            # Persisting the operational mark would give only the current day
            # a different grain and distort historical comparisons.
            return
        terminal_signature = [now.date().isoformat(), total_equity, cumulative, open_net, open_count, stale_count, settled_at]
        if terminal and mode.get("terminal_curve_mark") == terminal_signature:
            return
        self._append_ledger(
            self.marks_path,
            {
                "recorded_at": now.isoformat(timespec="seconds"),
                "minute": mark_at.isoformat(
                    timespec="minutes"
                ),
                "session_date": mode.get("session_date") or now.date().isoformat(),
                "market": market,
                "initial_capital_twd": mode.get("initial_capital_twd"),
                "cumulative_realized_net_pnl_twd": cumulative,
                "cumulative_carry_cost_twd": float(mode.get("cumulative_carry_cost_twd") or 0),
                "cumulative_corporate_action_net_twd": float(mode.get("cumulative_corporate_action_net_twd") or 0),
                "corporate_action_cash_net_twd": float(mode.get("corporate_action_cash_net_twd") or 0),
                "corporate_action_receivable_twd": float(mode.get("corporate_action_receivable_twd") or 0),
                "corporate_action_payable_twd": float(mode.get("corporate_action_payable_twd") or 0),
                "margin_carry_contract": mode.get("margin_carry_contract"),
                "odd_lot_execution_policy": mode.get("odd_lot_execution_policy"),
                "open_net_liquidation_pnl_twd": open_net,
                "total_equity_twd": total_equity,
                "open_position_count": open_count,
                "stale_position_count": stale_count,
                "valuation_stale": stale_count > 0,
                **({
                    "historical_minute_replay": True,
                    "minute_valuation_contract": HISTORICAL_MINUTE_MARK_CONTRACT,
                    "valuation_source": "retained_1m_close_with_explicit_last_trade_carry",
                    "valuation_executable": False,
                    "fresh_trade_position_count": open_count - stale_count,
                    "last_trade_carried_position_count": stale_count,
                    "missing_price_position_count": missing_count,
                    "fresh_trade_notional_coverage_ratio": fresh_notional / total_notional if total_notional else 1.0,
                } if mode.get("historical_minute_valuation") else {}),
            },
        )
        if terminal:
            mode["terminal_curve_mark"] = terminal_signature

    def _persist(self, now: datetime | None = None) -> None:
        # A live process can replay a 09:01 financial event at 11:00. Its
        # liveness/commit publication time is not that historical event time.
        # Offline replay keeps its explicit deterministic clock by default.
        observed = _now_taipei(self._publication_clock() if self._publication_clock else now)
        revision = int(self.state.get("state_revision") or 0) + 1
        configured_markets = self.state.get("enabled_markets")
        active_markets = [
            str(market)
            for market in (
                configured_markets
                if isinstance(configured_markets, list)
                else sorted((self.state.get("modes") or {}).keys())
            )
        ]
        material_projection = {
            "enabled_markets": active_markets,
            "modes": {
                market: {
                    key: value
                    for key, value in (
                        self.state.get("modes", {}).get(market) or {}
                    ).items()
                    if key not in {"positions", "processed_signal_ids"}
                }
                for market in active_markets
            },
            "benchmarks": self.state.get("benchmarks") or {},
        }
        material_fingerprint = hashlib.sha256(
            json.dumps(
                material_projection,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                default=str,
            ).encode("utf-8")
        ).hexdigest()
        content_revision = int(self.state.get("dashboard_content_revision") or 0)
        if material_fingerprint != self.state.get("dashboard_content_fingerprint"):
            content_revision += 1
        self.state["dashboard_content_revision"] = content_revision
        self.state["dashboard_content_fingerprint"] = material_fingerprint
        self.state["state_revision"] = revision
        self.state["engine_run_id"] = self._engine_run_id
        self.state["updated_at"] = observed.isoformat(timespec="seconds")
        _atomic_json(self.state_path, self.state)
        enabled_markets = active_markets
        all_modes = self.state.get("modes") or {}
        mode_rows = [
            all_modes[market]
            for market in enabled_markets
            if isinstance(all_modes.get(market), Mapping)
        ]
        _atomic_json(
            self.positions_path,
            {
                "schema_version": 1,
                "state_revision": revision,
                "content_revision": content_revision,
                "engine_run_id": self._engine_run_id,
                "generated_at": observed.isoformat(timespec="seconds"),
                "simulation_only": True,
                "production_order_possible": False,
                "position_contract": {
                    "target": (
                        "complete model targets are stored at target_weights_path "
                        "and target_positions_path"
                    ),
                    "executed": (
                        "positions contains paper-simulation fills; an empty list "
                        "is an explicit flat position"
                    ),
                },
                "modes": {
                    str(item.get("market")): {
                        "market": item.get("market"),
                        "label": item.get("label"),
                        "session_date": item.get("session_date"),
                        "signal_id": item.get("signal_id"),
                        "signal_at": item.get("signal_at"),
                        "engine_status": item.get("engine_status"),
                        "target_weights_path": item.get("target_weights_path"),
                        "target_positions_path": item.get("target_positions_path"),
                        "target_symbol_count": item.get("target_symbol_count"),
                        "target_risk": item.get("target_risk") or {},
                        "open_position_count": int(
                            item.get("open_position_count") or 0
                        ),
                        "positions": [
                            dict(position)
                            for position in (item.get("positions") or {}).values()
                            if isinstance(position, Mapping)
                            and int(position.get("signed_shares") or 0) != 0
                        ],
                    }
                    for item in mode_rows
                },
            },
        )
        critical = any(
            str(item.get("engine_status") or "").startswith("critical")
            for item in mode_rows
        )
        divergence_rows = {
            str(item.get("market")): dict(item.get("ledger_state_divergence") or {})
            for item in mode_rows
            if isinstance(item.get("ledger_state_divergence"), Mapping)
        }
        blocked = any(
            str(item.get("engine_status") or "").startswith("blocked")
            for item in mode_rows
        )
        active = any(item.get("engine_status") == "active" for item in mode_rows)
        synthetic_open_tick = bool(mode_rows) and all(
            str(item.get("entry_fill_policy") or "")
            == ENTRY_FILL_POLICY_SYNTHETIC_OPEN_TICK
            for item in mode_rows
        )
        paper_market_at_best = bool(mode_rows) and all(
            str(item.get("entry_fill_policy") or "")
            == ENTRY_FILL_POLICY_MARKET_AT_BEST_ELSE_OPEN_TICK
            for item in mode_rows
        )
        health = (
            "critical"
            if critical
            else "active"
            if active
            else "blocked"
            if blocked
            else "waiting"
        )
        _atomic_json(
            self.status_path,
            {
                "schema_version": SIMULATION_SCHEMA_VERSION,
                "order_price_contract_version": TW_ORDER_PRICE_CONTRACT_VERSION,
                "state_revision": revision,
                "content_revision": content_revision,
                "engine_run_id": self._engine_run_id,
                "updated_at": observed.isoformat(timespec="seconds"),
                "health": health,
                "simulation_only": True,
                "production_order_possible": False,
                "ledger_integrity": {
                    "ready": not divergence_rows,
                    "divergence_count": len(divergence_rows),
                    "modes": divergence_rows,
                    "commit_protocol": "fsynced_start_then_ledgers_then_registered_then_state",
                },
                "schedule": {
                    "signal_gate": "09:00",
                    "entry": (
                        "synthetic_fill_at_observed_session_open_plus_one_adverse_tick"
                        if synthetic_open_tick
                        else "paper_market_order_at_causal_best_ask_bid_else_open_plus_one_adverse_tick"
                        if paper_market_at_best
                        else "size_at_official_open_then_execute_at_a_causally_later_best_ask_bid"
                    ),
                    "liquidity": (
                        "counterfactual_unbounded_paper_fill_no_exchange_claim"
                        if synthetic_open_tick
                        else "full_requested_quantity_at_observed_best_quote_no_exchange_depth_claim"
                        if paper_market_at_best
                        else "min(level_one_depth, 50pct_completed_minute_kbar_volume)"
                    ),
                    "take_profit": "mode-specific daily-limit LMT: full limit or configured ticks inside",
                    "stop_loss": "mode-specific local trigger: full limit or configured ticks inside, then market",
                    "fill_guarantee": bool(synthetic_open_tick or paper_market_at_best),
                    "fill_caveat": (
                        "guaranteed only inside the synthetic paper ledger; not a broker or exchange fill"
                        if synthetic_open_tick
                        else "deterministic only inside the paper ledger; displayed depth and queue position do not guarantee an exchange fill"
                        if paper_market_at_best
                        else "inside-limit prices improve fill probability but cannot guarantee a fill without executable counterparty volume"
                    ),
                    "exit_limit": "13:20 passive top-of-book limit",
                    "continuous_force_exit": "13:24<=t<13:25 market retry every service poll while residual exists",
                    "closing_auction": "13:25 long sell at lower limit and short cover at upper limit using LMT_ROD; settle at 13:30 call auction",
                    "residual": "strict_intraday: same-day flatten required; only evidenced adverse limit-lock permits exceptional paper margin carry; every prior residual is liquidation-only; legacy contracts retain their recorded semantics",
                    "stress_rates": {
                        "financing_annual_rate": MARGIN_FINANCING_ANNUAL_RATE,
                        "shortfall_borrow_fee_rate_per_day": DAY_TRADE_SHORTFALL_BORROW_FEE_RATE,
                        "shortfall_handling_fee_fraction": DAY_TRADE_SHORTFALL_HANDLING_FEE_FRACTION,
                    },
                    "decision_and_mark_interval_seconds": 60,
                },
                "mode_count": len(mode_rows),
                "modes": {
                    str(item.get("market")): {
                        key: item.get(key)
                        for key in (
                            "market",
                            "label",
                            "signal_market",
                            "price_limit_offset_ticks",
                            "bracket_price_policy",
                            "fill_guaranteed",
                            "paper_fill_deterministic",
                            "exchange_fill_guaranteed",
                            "entry_fill_policy",
                            "entry_price_offset_ticks",
                            "entry_fill_is_synthetic",
                            "entry_fill_has_synthetic_fallback",
                            "entry_best_quote_fill_count",
                            "entry_synthetic_fallback_fill_count",
                            "entry_paper_market_fill_count",
                            "entry_fill_contract",
                            "execution_realism_contract",
                            "capital_sizing_basis",
                            "session_sizing_nav_twd",
                            "funding_assumption",
                            "execution_evidence_complete",
                            "entry_liquidity_assumption",
                            "engine_status",
                            "checkpoint_ready",
                            "readiness_error",
                            "session_date",
                            "signal_id",
                            "signal_at",
                            "target_weights_path",
                            "target_positions_path",
                            "executed_positions_path",
                            "target_symbol_count",
                            "target_risk",
                            "signal_counts",
                            "signal_reason_counts",
                            "entry_fill_count",
                            "entry_requested_shares",
                            "entry_filled_shares",
                            "entry_unfilled_shares",
                            "intraday_contract",
                            "configured_intraday_contract",
                            "pending_entry_shares",
                            "manual_close_settlement",
                            "closing_auction_pending_count",
                            "margin_exception_count",
                            "unresolved_exit_count",
                            "entry_fill_outcome",
                            "initial_capital_twd",
                            "total_equity_twd",
                            "cumulative_realized_net_pnl_twd",
                            "open_net_liquidation_pnl_twd",
                            "open_position_count",
                            "stale_position_count",
                            "force_exit_failures",
                            "terminal_flatten_count",
                            "terminal_flatten_degraded_count",
                            "cumulative_carry_cost_twd",
                            "cumulative_corporate_action_net_twd",
                            "corporate_action_cash_net_twd",
                            "corporate_action_receivable_twd",
                            "corporate_action_payable_twd",
                            "margin_carry_contract",
                            "odd_lot_execution_policy",
                            "margin_cost_assumptions",
                            "margin_carry_position_count",
                            "margin_corporate_action_receipt",
                            "rebalance_reduction_fill_count",
                            "ledger_state_divergence",
                        )
                    }
                    for item in mode_rows
                },
            },
        )
        _atomic_json(
            self.service_sync_path,
            {
                "schema_version": SERVICE_SYNC_SCHEMA_VERSION,
                "state_revision": revision,
                "content_revision": content_revision,
                "engine_run_id": self._engine_run_id,
                "published_at": observed.isoformat(timespec="milliseconds"),
                "simulation_only": True,
                "production_order_possible": False,
                "ledger_integrity_ready": not divergence_rows,
                "enabled_markets": enabled_markets,
                "mode_count": len(mode_rows),
                "modes": {
                    str(item.get("market")): {
                        key: item.get(key)
                        for key in (
                            "market",
                            "session_date",
                            "signal_id",
                            "signal_at",
                            "entry_completed_at",
                            "entry_fill_policy",
                            "entry_price_offset_ticks",
                            "engine_status",
                            "checkpoint_ready",
                            "open_position_count",
                            "margin_carry_contract",
                            "odd_lot_execution_policy",
                            "margin_carry_position_count",
                            "margin_corporate_action_receipt",
                            "valuation_complete",
                            "closing_auction_settled_at",
                            "residual_conversion_completed_at",
                        )
                    }
                    for item in mode_rows
                },
            },
        )


__all__ = [
    "CLOSING_AUCTION_TIME",
    "ENTRY_FILL_POLICY_0901_MINUTE_PRICE",
    "ENTRY_FILL_POLICY_0901_MINUTE_VWAP",
    "ENTRY_FILL_POLICY_CAUSAL_BOOK",
    "ENTRY_FILL_POLICY_CAUSAL_BOOK_ELSE_OPEN_TICK",
    "ENTRY_FILL_POLICY_MARKET_AT_BEST_ELSE_OPEN_TICK",
    "ENTRY_FILL_POLICY_OFFICIAL_OPEN_AT_0901",
    "ENTRY_FILL_POLICY_SYNTHETIC_OPEN_TICK",
    "ENTRY_GATE",
    "LIVE_ENTRY_GATE",
    "EXIT_LIMIT_TIME",
    "FIRST_MINUTE_EXECUTION_TIME",
    "FORCE_EXIT_TIME",
    "REPLAY_FILL_CONTRACT_0901_MINUTE_PRICE",
    "LiveEligibility",
    "ModeSpec",
    "TwDayTradeSimulationEngine",
    "load_live_eligibility",
    "load_symbol_metadata",
    "quote_map_from_snapshot",
]
