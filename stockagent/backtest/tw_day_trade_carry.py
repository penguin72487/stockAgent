"""Session composition of the physical FIFO ledger, not a second trainer.

The caller must resolve receipt-verified execution facts outside model inputs.
This module owns chronological session state and daily/minute return accounting;
the existing inventory kernels own fills, costs, corporate claims and FIFO.
"""
from __future__ import annotations

from dataclasses import dataclass, fields, replace
from datetime import date
import math

import torch
from torch import Tensor

from stockagent.backtest.tw_day_trade_inventory import (
    CohortField, DayTradeInventoryState, _require, accrue_inventory_interest,
    apply_inventory_action, convert_inventory_to_margin, inventory_nav,
    inventory_path_nav, rebalance_inventory_at_open, reduce_inventory_fifo_path,
    settle_inventory_claims, validate_inventory_state,
)

CARRY_SESSION_ABI = "tw_day_trade_physical_fifo_sessions_v4_daily_proxy"


@dataclass(frozen=True)
class DayTradeCarrySession:
    """Executor-only facts: prices are not features or historical broker fills.

    Exit fields have shape [S,270,2] (long, short); marks [S,270]. All remaining
    vectors have shape [S]. ``opening_marks`` may contain an explicitly sourced
    carried valuation during a proven halt, never an invented executable open.
    Source adapters own completeness, dated legal prices and halt provenance.
    ``source_gap_mask`` is an explicit retrospective research exclusion, NOT
    market eligibility or a model feature. It suppresses only that symbol's
    new request, without renormalizing peers. It describes MISSING MINUTE
    SOURCES, not missing action terms. An affected physical position is a hard
    source failure. Already recognized, exactly dated cash claims need no stock
    quote and continue to settlement unchanged. Unknown corporate-action terms
    are checked only for prior physical inventory entitled to that event.
    """
    day: int
    official_open: Tensor
    opening_marks: Tensor
    entry_price: Tensor
    entry_volume: Tensor
    lower_limit: Tensor
    upper_limit: Tensor
    halted: Tensor
    exit_prices: Tensor
    exit_capacity: Tensor
    marks: Tensor
    action_mask: Tensor | None = None
    share_ratio: Tensor | None = None
    cash_per_old_share: Tensor | None = None
    payment_day: Tensor | None = None
    source_gap_mask: Tensor | None = None
    daily_proxy_mask: Tensor | None = None

    def validate_shape(self, symbols: int, device: torch.device) -> None:
        if not isinstance(self.day, int) or isinstance(self.day, bool) or not 0 < self.day <= 3652059:
            raise ValueError("carry session requires an exact Gregorian day ordinal")
        actions = (self.action_mask, self.share_ratio, self.cash_per_old_share, self.payment_day)
        if any(x is None for x in actions) and not all(x is None for x in actions):
            raise ValueError("corporate action requires all exact physical/cash fields")
        for field in fields(self):
            value = getattr(self, field.name)
            if field.name == "day" or value is None:
                continue
            shape = ((symbols, 270, 2) if field.name in {"exit_prices", "exit_capacity"}
                     else (symbols, 270) if field.name == "marks" else (symbols,))
            if not isinstance(value, Tensor) or value.shape != shape or value.device != device:
                raise ValueError(f"carry session {field.name} differs from pinned universe/device")
            if value.dtype != torch.float64:
                raise ValueError(f"carry session {field.name} requires float64 source precision")


@dataclass(frozen=True)
class DayTradeCarryState:
    inventory: DayTradeInventoryState
    last_nav: Tensor  # last committed close, not today's opening mark
    alive: Tensor  # financial default is independent of source failure
    initial_capital: float
    last_session_day: int = 0

    @classmethod
    def empty(cls, symbols: int, initial_capital: float, device: torch.device) -> DayTradeCarryState:
        if not math.isfinite(initial_capital) or initial_capital <= 0:
            raise ValueError("carry account initial capital must be positive finite")
        return cls(DayTradeInventoryState.empty(symbols, device=device),
                   torch.tensor(initial_capital, device=device, dtype=torch.float64),
                   torch.ones((), device=device, dtype=torch.bool), float(initial_capital))

    def detached(self, *, device: torch.device | str | None = None) -> DayTradeCarryState:
        target = self.last_nav.device if device is None else device
        inventory = DayTradeInventoryState(**{f.name: getattr(self.inventory, f.name).detach().to(target).clone()
                                             for f in fields(self.inventory)})
        return DayTradeCarryState(inventory, self.last_nav.detach().to(target).clone(),
            self.alive.detach().to(target).clone(), self.initial_capital, self.last_session_day)

    def validate(self, *, symbols: int, device: torch.device, initial_capital: float) -> Tensor:
        if (not math.isfinite(initial_capital) or initial_capital <= 0
                or self.initial_capital != initial_capital):
            raise ValueError("carry account initial capital differs from the continuing account")
        if (not isinstance(self.last_session_day, int) or isinstance(self.last_session_day, bool)
                or not 0 <= self.last_session_day <= 3652059):
            raise ValueError("carry account requires its last committed session ordinal")
        if (self.last_nav.shape != () or self.last_nav.dtype != torch.float64
                or self.last_nav.device != device or self.alive.shape != ()
                or self.alive.dtype != torch.bool or self.alive.device != device
                or self.inventory.cohorts.shape[1:] != (symbols, len(CohortField))):
            raise ValueError("carry account differs from the pinned universe/device/precision")
        for field in fields(self.inventory):
            value = getattr(self.inventory, field.name)
            if value.device != device or value.dtype != torch.float64:
                raise ValueError("carry inventory differs from the execution device/precision")
            expected = (self.inventory.cohorts.shape if field.name == "cohorts"
                else (self.inventory.claims.shape[0], symbols, 3) if field.name == "claims"
                else (symbols,) if field.name == "action_cursor" else ())
            if value.shape != expected:
                raise ValueError("carry inventory has invalid scalar/cursor/claim shape")
        return _require(torch.isfinite(self.last_nav)
            & torch.where(self.alive, self.last_nav > 0, self.last_nav == 0)
            & (self.inventory.failed == 0) & (self.inventory.observed_day <= self.last_session_day),
            "invalid continuing carry account valuation/state")

    def checkpoint_state(self, *, universe: tuple[str, ...], release_id: str) -> dict:
        valid = self.validate(symbols=len(universe), device=self.last_nav.device,
                              initial_capital=self.initial_capital)
        # Acceptance is outside the hot path and must observe CUDA predicates.
        if not bool(valid.detach().cpu()):
            raise ValueError("cannot checkpoint an invalid carry account")
        return {"abi": CARRY_SESSION_ABI, "initial_capital": self.initial_capital,
            "last_session_day": self.last_session_day,
            "last_nav": self.last_nav.detach().clone(), "alive": self.alive.detach().clone(),
            "inventory": self.inventory.checkpoint_state(universe=universe, release_id=release_id)}

    @classmethod
    def from_checkpoint_state(cls, payload: dict, *, universe: tuple[str, ...],
                              release_id: str, initial_capital: float,
                              device: torch.device | str = "cpu") -> DayTradeCarryState:
        if (set(payload) != {"abi", "initial_capital", "last_session_day", "last_nav", "alive", "inventory"}
                or payload["abi"] != CARRY_SESSION_ABI
                or payload["initial_capital"] != initial_capital):
            raise ValueError("incompatible physical carry checkpoint contract/capital")
        if (not isinstance(payload["last_nav"], Tensor) or not isinstance(payload["alive"], Tensor)
                or payload["last_nav"].dtype != torch.float64 or payload["alive"].dtype != torch.bool):
            raise ValueError("carry checkpoint lost valuation precision or boolean state")
        state = cls(DayTradeInventoryState.from_checkpoint_state(payload["inventory"],
            universe=universe, release_id=release_id, device="cpu"),
            payload["last_nav"].detach().cpu().clone(), payload["alive"].detach().cpu().clone(),
            initial_capital, payload["last_session_day"])
        state.validate(symbols=len(universe), device=torch.device("cpu"), initial_capital=initial_capital)
        validate_inventory_state(state.inventory, symbols=len(universe))
        return cls(DayTradeInventoryState(**{f.name: getattr(state.inventory, f.name).to(device)
            for f in fields(state.inventory)}), state.last_nav.to(device), state.alive.to(device),
            initial_capital, state.last_session_day)


@dataclass(frozen=True)
class DayTradeCarryResult:
    strategy_returns: Tensor
    turnovers: Tensor
    weights_history: Tensor
    shares_history: Tensor
    minute_nav: Tensor
    settlement_default: Tensor
    final_state: DayTradeCarryState


def _choose_inventory(condition: Tensor, new: DayTradeInventoryState,
                      old: DayTradeInventoryState) -> DayTradeInventoryState:
    """Freeze physical evidence on economic default; do not call it bad data."""
    values = {}
    for field in fields(old):
        before, after = getattr(old, field.name), getattr(new, field.name)
        if field.name in {"cohorts", "claims"} and before.shape[0] < after.shape[0]:
            padding = before.new_zeros((after.shape[0] - before.shape[0], *before.shape[1:]))
            if field.name == "cohorts" and before.shape[0]:
                # Inactive shape-padding still has to preserve FIFO chronology
                # when a financially defaulted (but valid) account is restored.
                padding[..., CohortField.ACQUIRED_DAY] = before[-1, :, CohortField.ACQUIRED_DAY]
            before = torch.cat((before, padding))
        values[field.name] = torch.where(condition, after, before)
    return DayTradeInventoryState(**values)


def execute_carry_session(
    state: DayTradeCarryState, session: DayTradeCarrySession, *, weights: Tensor,
    can_enter: Tensor, buy_fee_rate: Tensor, day_sell_fee_rate: Tensor,
    normal_sell_fee_rate: Tensor, rebate_rate: Tensor, initial_capital: float,
) -> tuple[DayTradeCarryState, Tensor, Tensor]:
    """09:00 target, 09:01 delta, scheduled exits, then physical residual carry.

    Returns committed state, all 270 net-liquidation marks, and gross traded
    notional. A default freezes physical evidence and prevents later trading;
    missing source evidence remains NaN/failed, not an economic default.
    """
    inventory = state.inventory
    source_valid = torch.ones((), dtype=torch.bool, device=weights.device)
    gap = torch.zeros_like(weights, dtype=torch.bool)
    if session.source_gap_mask is not None:
        raw_gap = session.source_gap_mask
        source_valid = _require(torch.isfinite(raw_gap) & ((raw_gap == 0) | (raw_gap == 1)),
                                "source gap mask must contain exact binary values")
        gap = raw_gap != 0
        # Check gross cohort quantities, not net shares (opposite cohorts must
        # not cancel evidence). A recognized cash claim has its own immutable
        # amount/payment date: missing stock quotes cannot invalidate it or
        # prevent payment. Missing/revised claim receipts remain adapter errors.
        exposed = (inventory.cohorts[..., CohortField.SHARES] != 0).any(dim=0)
        source_valid = source_valid & _require(~gap | ~exposed,
            "minute source gap intersects carried inventory; cannot mask away ownership")
        can_enter = torch.where(gap, 0, can_enter)
    # Defaulted accounts use an inert calculation branch; actual evidence is
    # restored below. This is tensor predication, not a refill of account cash.
    working = DayTradeInventoryState(**{
        f.name: torch.where(state.alive & source_valid, getattr(inventory, f.name), 0)
        for f in fields(inventory)
    })
    if session.action_mask is not None:
        # Match paper's held-interval gate. An ex-date catalogue row is not an
        # account entitlement: flat symbols and same-day purchases do not own
        # it. Never require every issuer's exact terms just to run healthy peers.
        source_valid = source_valid & _require(
            torch.isfinite(session.action_mask)
            & ((session.action_mask == 0) | (session.action_mask == 1)),
            "corporate action mask must contain exact binary values")
        entitled = ((working.cohorts[..., CohortField.SHARES] != 0)
                    & (working.cohorts[..., CohortField.ACQUIRED_DAY] < session.day)).any(dim=0)
        working = apply_inventory_action(working, event_day=session.day, as_of_day=session.day,
            event_mask=torch.where(entitled & source_valid, session.action_mask, 0), share_ratio=session.share_ratio,
            cash_per_old_share=session.cash_per_old_share, payment_day=session.payment_day)
    working = settle_inventory_claims(accrue_inventory_interest(working, day=session.day), day=session.day)
    opening_nav = inventory_nav(working, initial_capital=initial_capital,
                               marks=session.opening_marks)
    valid = (_require(torch.isfinite(opening_nav), "carry account opening valuation is invalid")
             & (inventory.failed == 0) & source_valid)
    trade_alive = state.alive & (opening_nav > 0) & valid & (working.failed == 0)
    funded = DayTradeInventoryState(**{
        f.name: torch.where(trade_alive, getattr(working, f.name), 0)
        for f in fields(working)
    })
    opening = rebalance_inventory_at_open(funded, weights=torch.where(trade_alive, weights, 0),
        official_open=session.official_open, opening_marks=session.opening_marks,
        entry_price=session.entry_price, entry_volume_shares=session.entry_volume,
        lower_limit=session.lower_limit, upper_limit=session.upper_limit,
        can_enter=can_enter, buy_fee_rate=buy_fee_rate, day_sell_fee_rate=day_sell_fee_rate,
        normal_sell_fee_rate=normal_sell_fee_rate, rebate_rate=rebate_rate,
        initial_capital=initial_capital, day=session.day, halted=session.halted,
        daily_proxy_mask=session.daily_proxy_mask)
    # There are exactly two sides, so use a predicate, not arbitrary indices.
    # Preserve both NaN/no-order semantics and the selected side's gradient.
    short = (opening.state.shares < 0)[:, None]
    prices = torch.where(short, session.exit_prices[..., 1], session.exit_prices[..., 0])
    capacity = torch.where(short, session.exit_capacity[..., 1], session.exit_capacity[..., 0])
    path = reduce_inventory_fifo_path(opening.state, prices=prices, capacity_shares=capacity)
    marks = inventory_path_nav(opening.state, prices=prices,
        minute_filled_shares=path.minute_filled_shares, marks=session.marks,
        initial_capital=initial_capital)
    valid = valid & _require(torch.isfinite(marks), "carry account minute valuation is invalid")
    # Fills at the first insolvent mark happened; no later fill is permitted.
    before_alive = torch.cat((torch.ones_like(marks[:1], dtype=torch.bool),
                              (marks[:-1] > 0).cumprod(0).bool()))
    path = reduce_inventory_fifo_path(opening.state, prices=prices,
                                      capacity_shares=capacity * before_alive)
    marks = inventory_path_nav(opening.state, prices=prices,
        minute_filled_shares=path.minute_filled_shares, marks=session.marks,
        initial_capital=initial_capital)
    intraday_alive = trade_alive & (marks > 0).all()
    converted = convert_inventory_to_margin(path.reduction.state, day=session.day)
    final_inventory = _choose_inventory(intraday_alive, converted, path.reduction.state)
    closing_nav = inventory_nav(final_inventory, initial_capital=initial_capital,
                                marks=session.marks[:, -1])
    valid = valid & _require(torch.isfinite(closing_nav), "carry account close valuation is invalid")
    final_alive = intraday_alive & (closing_nav > 0)
    raw_marks = torch.cat((marks[:-1], closing_nav.reshape(1)))
    mark_alive = trade_alive & (raw_marks > 0).cumprod(0).bool()
    committed_marks = torch.where(mark_alive, raw_marks, 0)
    committed_marks = torch.where(valid, committed_marks, float("nan"))
    final_inventory = _choose_inventory(trade_alive, final_inventory, working)
    final_inventory = _choose_inventory(state.alive, final_inventory, inventory)
    # Source failure is atomic across the account, including malformed action
    # terms on CUDA. Do not leave successful peers half-advanced on retry.
    final_inventory = _choose_inventory(valid, final_inventory, inventory)
    final_inventory = replace(final_inventory,
        failed=torch.maximum(final_inventory.failed, (~valid).to(torch.float64)))
    notional = ((opening.reduction.filled_shares + opening.addition_shares.abs())
                * torch.nan_to_num(session.entry_price, nan=0)).sum()
    notional = notional + (path.minute_filled_shares * torch.nan_to_num(prices, nan=0)).sum()
    return (DayTradeCarryState(final_inventory, committed_marks[-1],
            torch.where(valid, final_alive, state.alive), initial_capital, session.day),
            committed_marks, torch.where(trade_alive & valid, notional, 0))


def run_day_trade_carry_sessions(
    weights: Tensor, sessions: tuple[DayTradeCarrySession, ...], *, can_enter: Tensor,
    buy_fee_rate: Tensor, day_sell_fee_rate: Tensor, normal_sell_fee_rate: Tensor,
    rebate_rate: Tensor, initial_capital: float, initial_state: DayTradeCarryState | None = None,
) -> DayTradeCarryResult:
    """Chronological executor used by the canonical simulator/loss boundary."""
    if weights.ndim != 2 or weights.shape[0] != len(sessions) or not sessions:
        raise ValueError("carry sessions must match a nonempty [T,S] action trajectory")
    if can_enter.shape != weights.shape:
        raise ValueError("carry entry permissions must match the action trajectory")
    if any(a.day >= b.day for a, b in zip(sessions, sessions[1:])):
        raise ValueError("carry sessions must be strictly chronological")
    state = initial_state or DayTradeCarryState.empty(weights.shape[1], initial_capital, weights.device)
    if sessions[0].day <= state.last_session_day:
        raise ValueError("carry session precedes or repeats the last committed session")
    valid_state = state.validate(symbols=weights.shape[1], device=weights.device,
                                 initial_capital=initial_capital)
    state = replace(state, inventory=replace(state.inventory,
        failed=torch.maximum(state.inventory.failed, (~valid_state).to(torch.float64))))
    returns, turns, curves, holdings, exposure, defaults = [], [], [], [], [], []
    for i, session in enumerate(sessions):
        session.validate_shape(weights.shape[1], weights.device)
        prior_nav, prior_alive, prior_inventory = (
            state.last_nav,
            state.alive,
            state.inventory,
        )
        state, curve, notional = execute_carry_session(state, session,
            weights=weights[i], can_enter=can_enter[i], buy_fee_rate=buy_fee_rate,
            day_sell_fee_rate=day_sell_fee_rate, normal_sell_fee_rate=normal_sell_fee_rate,
            rebate_rate=rebate_rate, initial_capital=initial_capital)
        if (
            not torch.compiler.is_compiling()
            and not torch.is_grad_enabled()
            and bool(state.inventory.failed.detach().cpu())
        ):
            # CUDA kernels return a device validity predicate instead of using
            # a device assert, which would poison every DDP rank. At the eager
            # evaluation boundary, stop on the first bad contract-day and emit
            # enough evidence to repair that exact symbol/day without masking
            # the rest of the session.
            exposed = (
                prior_inventory.cohorts[..., CohortField.SHARES] != 0
            ).any(dim=0)
            gap = (
                torch.zeros_like(exposed)
                if session.source_gap_mask is None
                else session.source_gap_mask != 0
            )
            gap_held = torch.nonzero(gap & exposed).flatten().detach().cpu().tolist()
            opening_missing = torch.nonzero(
                exposed
                & (~torch.isfinite(session.opening_marks) | (session.opening_marks <= 0))
            ).flatten().detach().cpu().tolist()
            path_missing = torch.nonzero(
                exposed
                & (
                    ~torch.isfinite(session.marks)
                    | (session.marks <= 0)
                ).any(dim=1)
            ).flatten().detach().cpu().tolist()
            close_missing = torch.nonzero(
                exposed
                & (
                    ~torch.isfinite(session.marks[:, -1])
                    | (session.marks[:, -1] <= 0)
                )
            ).flatten().detach().cpu().tolist()
            requested_missing_path = torch.nonzero(
                (weights[i] != 0)
                & can_enter[i].bool()
                & (
                    ~torch.isfinite(session.marks)
                    | (session.marks <= 0)
                ).any(dim=1)
            ).flatten().detach().cpu().tolist()
            action_indices = (
                []
                if session.action_mask is None
                else torch.nonzero(session.action_mask != 0)
                .flatten().detach().cpu().tolist()
            )
            raise FloatingPointError(
                "physical source integrity failed at first bad contract-day: "
                f"date={date.fromordinal(session.day).isoformat()} "
                f"row={i} gap_held_symbol_indices={gap_held[:32]} "
                f"gap_held_count={len(gap_held)} "
                f"opening_mark_missing_held_indices={opening_missing[:32]} "
                f"opening_mark_missing_held_count={len(opening_missing)} "
                f"minute_mark_missing_held_indices={path_missing[:32]} "
                f"minute_mark_missing_held_count={len(path_missing)} "
                f"close_mark_missing_held_indices={close_missing[:32]} "
                f"close_mark_missing_held_count={len(close_missing)} "
                f"requested_missing_path_indices={requested_missing_path[:32]} "
                f"requested_missing_path_count={len(requested_missing_path)} "
                f"action_indices={action_indices[:32]} "
                f"action_count={len(action_indices)}"
            )
        # Reuse the canonical return/ruin-floor math, while retaining invalid
        # source NaNs (the generic helper's legacy NaN sanitization is unsafe
        # for physical inventory acceptance).
        from stockagent.backtest.simulator import _portfolio_simple_returns_to_log_torch
        ratio = state.last_nav / prior_nav.clamp_min(1e-30)
        log_return = _portfolio_simple_returns_to_log_torch(ratio - 1)
        returns.append(torch.where(torch.isfinite(ratio),
            torch.where(prior_alive, log_return, 0), float("nan")))
        turns.append(notional / prior_nav.clamp_min(1e-30))
        curves.append(curve)
        holdings.append(state.inventory.shares)
        exposure.append(torch.where(state.alive,
            state.inventory.shares * torch.nan_to_num(session.marks[:, -1], nan=0)
            / state.last_nav.clamp_min(1e-30), 0))
        defaults.append(~state.alive)
    return DayTradeCarryResult(torch.stack(returns), torch.stack(turns), torch.stack(exposure),
        torch.stack(holdings), torch.stack(curves), torch.stack(defaults), state)
