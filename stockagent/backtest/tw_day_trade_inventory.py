"""Recurrent FIFO inventory algebra for the TW day-trade minute executor.

This is an execution-state component, NOT a second trainer or a new live
account. The legacy minute executor still rejects margin-carry configurations
until its source tape, scheduling and canonical trainer state plumbing use
this component. Passing these kernels' tests is not full paper/training parity.

All money is in TWD; shares are signed physical shares, never normalized
weights. Acquisition cohorts retain their own basis, remaining entry cost,
fee contract and calendar-day accrual cursor. Operations are functional so a
failed validation cannot partially mutate the input, and autograd can follow
the same ledger across successive days. Discrete fills use the caller's
straight-through integer sizing; no synthetic closing transaction is made.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from enum import IntEnum
import math

import torch
from torch import Tensor
from stockagent.backtest.tw_inventory_scan import fifo_cumsum

from stockagent.backtest.tw_day_trade_contract import (
    MARGIN_CARRY_CONTRACT,
    MARGIN_FINANCING_ANNUAL_RATE,
    MARGIN_FINANCING_PRINCIPAL_RATIO,
    MARGIN_SHORT_ANNUAL_BORROW_RATE,
    MARGIN_SHORT_HANDLING_FEE_RATE,
    ODD_LOT_BOARD_PRICE,
    inventory_carry_interest,
    net_liquidation_pnl,
)


INVENTORY_STATE_ABI = "tw_day_trade_fifo_physical_inventory_v2"


class CohortField(IntEnum):
    SHARES = 0
    BASIS = 1
    ENTRY_COST = 2
    ENTRY_PRICE = 3  # immutable historical price, unlike a replacement basis
    DAY_EXIT_RATE = 4  # NET of the same entry contract's rebate
    CARRY_EXIT_RATE = 5
    SHORT_CONVERSION_RATE = 6
    ACQUIRED_DAY = 7  # Gregorian ordinal, exactly representable in float64
    CONVERTED = 8
    ACCRUED_THROUGH = 9


F = CohortField
_COHORT_FIELD_INDICES = tuple(range(len(F)))


def _require(condition: Tensor, message: str) -> Tensor:
    """Host diagnostics or a device-resident validity predicate, never an assert.

    A CUDA device assertion poisons the context (and often other DDP ranks).
    CPU reference calls raise synchronously. CUDA/compiled calls instead return
    a predicate which MUST reach the atomic commit/failure flag or NAV below.
    Acceptance checks belong at an eager host boundary, not every GPU event.
    """
    valid = condition.all()
    if not torch.compiler.is_compiling() and condition.device.type == "cpu":
        if not bool(valid):
            raise RuntimeError(message)
    return valid


def _finite_nonnegative(value: Tensor, name: str) -> Tensor:
    return _require(torch.isfinite(value) & (value >= 0), f"invalid {name}")


def _physical_shares(value: Tensor) -> Tensor:
    return _require(
        torch.isfinite(value) & ((value - value.round()).abs() <= 1e-8),
        "physical shares must be finite integers; unresolved fractional shares",
    )


def _calendar_day(state: DayTradeInventoryState, day: int) -> Tensor:
    if not isinstance(day, int) or isinstance(day, bool) or not 0 < day <= 3652059:
        raise ValueError("event requires a Gregorian day ordinal")
    return _require(state.observed_day <= day, "inventory event clock moved backwards")


@dataclass(frozen=True)
class DayTradeInventoryState:
    cohorts: Tensor  # [acquisition cohort, symbol, CohortField]
    realized_net_pnl: Tensor  # scalar; allocated entry fees already included
    carry_cost: Tensor  # scalar cumulative cost, never an artificial fill
    claims: Tensor  # [claim, symbol, (signed amount, payment ordinal, paid)]
    action_cursor: Tensor  # [symbol]; last accepted corporate-action ordinal
    decision_day: Tensor  # scalar; guards duplicate daily decision/volume use
    observed_day: Tensor  # scalar; prevents future claims from funding old orders
    failed: Tensor  # scalar 0/1; absorbing invalid trajectory, never a CUDA assert

    @classmethod
    def empty(
        cls, symbols: int, *, device: torch.device | str = "cpu"
    ) -> DayTradeInventoryState:
        if symbols <= 0:
            raise ValueError("inventory requires a nonempty stable symbol universe")
        zero = torch.zeros((), dtype=torch.float64, device=device)
        return cls(
            zero.new_empty((0, symbols, len(F))),
            zero.clone(),
            zero.clone(),
            zero.new_empty((0, symbols, 3)),
            zero.new_zeros(symbols),
            zero.clone(),
            zero.clone(),
            zero.clone(),
        )

    @property
    def shares(self) -> Tensor:
        return self.cohorts[..., F.SHARES].sum(dim=0)

    @property
    def corporate_action_net(self) -> Tensor:
        return self.claims[..., 0].sum()

    @property
    def corporate_action_cash(self) -> Tensor:
        return (self.claims[..., 0] * self.claims[..., 2]).sum()

    @property
    def corporate_action_receivable(self) -> Tensor:
        return (self.claims[..., 0].clamp_min(0) * (1 - self.claims[..., 2])).sum()

    @property
    def corporate_action_payable(self) -> Tensor:
        return ((-self.claims[..., 0]).clamp_min(0) * (1 - self.claims[..., 2])).sum()

    def detached(self) -> DayTradeInventoryState:
        """Private storage at a canonical truncated-BPTT/checkpoint boundary."""
        return DayTradeInventoryState(
            **{
                name: getattr(self, name).detach().clone()
                for name in self.__dataclass_fields__
            }
        )

    def checkpoint_state(self, *, universe: tuple[str, ...], release_id: str) -> dict:
        """Namespaced payload for the canonical safe Torch checkpoint writer."""
        _validate_checkpoint_identity(universe, release_id, self.cohorts.shape[1])
        validate_inventory_state(self, symbols=len(universe))
        return {
            "abi": INVENTORY_STATE_ABI,
            "carry_contract": MARGIN_CARRY_CONTRACT,
            "odd_lot_contract": ODD_LOT_BOARD_PRICE,
            "universe": list(universe),
            "release_id": release_id,
            **{
                name: getattr(self, name).detach().clone()
                for name in self.__dataclass_fields__
            },
        }

    @classmethod
    def from_checkpoint_state(
        cls,
        payload: dict,
        *,
        universe: tuple[str, ...],
        release_id: str,
        device: torch.device | str = "cpu",
    ) -> DayTradeInventoryState:
        _validate_checkpoint_identity(universe, release_id, len(universe))
        if (
            payload.get("abi") != INVENTORY_STATE_ABI
            or payload.get("carry_contract") != MARGIN_CARRY_CONTRACT
            or payload.get("odd_lot_contract") != ODD_LOT_BOARD_PRICE
        ):
            raise ValueError("incompatible physical inventory checkpoint contract")
        if (
            payload.get("universe") != list(universe)
            or payload.get("release_id") != release_id
        ):
            raise ValueError(
                "inventory checkpoint differs from pinned universe/release"
            )
        names = cls.__dataclass_fields__
        if set(payload) != set(names) | {
            "abi",
            "carry_contract",
            "odd_lot_contract",
            "universe",
            "release_id",
        }:
            raise ValueError("incomplete or unknown inventory checkpoint fields")
        if any(not isinstance(payload[n], Tensor) for n in names):
            raise ValueError("inventory checkpoint fields must be tensors")
        if any(payload[n].dtype != torch.float64 for n in names):
            raise ValueError("inventory checkpoint lost float64 accounting precision")
        state = cls(
            **{
                n: payload[n].detach().to(device=device, dtype=torch.float64).clone()
                for n in names
            }
        )
        validate_inventory_state(state, symbols=len(universe))
        return state


def _validate_checkpoint_identity(
    universe: tuple[str, ...], release_id: str, symbols: int
) -> None:
    if (
        not universe
        or len(universe) != symbols
        or len(set(universe)) != symbols
        or any(not isinstance(s, str) or not s.strip() for s in universe)
    ):
        raise ValueError(
            "inventory checkpoint requires a unique ordered pinned universe"
        )
    if (
        not isinstance(release_id, str)
        or not release_id.strip()
        or release_id.strip().lower() == "latest"
    ):
        raise ValueError(
            "inventory checkpoint requires an exact data release, not latest"
        )


def _valid_claim_terms(claims: Tensor) -> Tensor:
    """Dated cash needs its own exact contract, not an underlying stock quote."""
    return (_require(torch.isfinite(claims), "nonfinite inventory claim")
        & _require((claims[..., 2] == 0) | (claims[..., 2] == 1),
                   "invalid inventory claim paid flag")
        & _require((claims[..., 0] == 0) | ((claims[..., 1] > 0)
                   & (claims[..., 1] <= 3652059)
                   & (claims[..., 1] == claims[..., 1].round())),
                   "claim has no exact payment date"))


def validate_inventory_state(state: DayTradeInventoryState, *, symbols: int) -> None:
    """Explicit eager acceptance/checkpoint boundary, allowed to synchronize once.

    Hot kernels record failure on-device. This boundary may copy a detached
    audit view; it never replaces the recurrent device state with CPU tensors.
    """
    if torch.compiler.is_compiling():
        raise RuntimeError(
            "inventory acceptance must run outside the compiled hot path"
        )
    if state.cohorts.device.type != "cpu":
        for name in state.__dataclass_fields__:
            value = getattr(state, name)
            if value.device != state.cohorts.device or value.dtype != torch.float64:
                raise ValueError(
                    "inventory state must share one float64 execution device"
                )
        state = DayTradeInventoryState(
            **{
                name: getattr(state, name).detach().cpu()
                for name in state.__dataclass_fields__
            }
        )
    c = state.cohorts
    if c.ndim != 3 or c.shape[1:] != (symbols, len(F)):
        raise ValueError("inventory cohort shape differs from the pinned universe")
    if state.claims.ndim != 3 or state.claims.shape[1:] != (symbols, 3):
        raise ValueError("inventory claim shape differs from the pinned universe")
    if (
        state.action_cursor.shape != (symbols,)
        or state.realized_net_pnl.ndim
        or state.carry_cost.ndim
        or state.decision_day.ndim
        or state.observed_day.ndim
        or state.failed.ndim
    ):
        raise ValueError("invalid inventory scalar/cursor shape")
    for n in state.__dataclass_fields__:
        tensor = getattr(state, n)
        if tensor.device != c.device or tensor.dtype != torch.float64:
            raise ValueError("inventory state must share one float64 execution device")
        _require(torch.isfinite(tensor), f"nonfinite inventory state: {n}")
    _require(state.failed == 0, "inventory trajectory failed validation")
    _physical_shares(c[..., F.SHARES])
    _require(c[..., F.SHARES] * state.shares >= 0, "opposed FIFO acquisition cohorts")
    active = c[..., F.SHARES] != 0
    _require(
        ~active | ((c[..., F.BASIS] > 0) & (c[..., F.ENTRY_PRICE] > 0)),
        "invalid inventory cost basis",
    )
    for f in (
        F.ENTRY_COST,
        F.DAY_EXIT_RATE,
        F.CARRY_EXIT_RATE,
        F.SHORT_CONVERSION_RATE,
    ):
        _finite_nonnegative(c[..., f], f.name)
    for values in (c[..., F.CONVERTED], state.claims[..., 2]):
        _require((values == 0) | (values == 1), "invalid inventory boolean state")
    _finite_nonnegative(state.carry_cost, "carry cost")
    for values in (
        c[..., F.ACQUIRED_DAY],
        c[..., F.ACCRUED_THROUGH],
        state.action_cursor,
        state.decision_day,
        state.observed_day,
    ):
        _require(
            (values >= 0) & (values == values.round()),
            "invalid inventory calendar cursor",
        )
        _require(values <= state.observed_day, "inventory state contains future events")
    _require(
        ~active | (c[..., F.ACCRUED_THROUGH] >= c[..., F.ACQUIRED_DAY]),
        "inventory accrual predates acquisition",
    )
    _require(
        c[1:, :, F.ACQUIRED_DAY] >= c[:-1, :, F.ACQUIRED_DAY],
        "inventory cohorts are not in acquisition order",
    )
    _valid_claim_terms(state.claims)


def _vector(state: DayTradeInventoryState, value: Tensor | float | int) -> Tensor:
    x = torch.as_tensor(value, device=state.cohorts.device, dtype=torch.float64)
    if x.ndim == 0:
        return x.expand(state.cohorts.shape[1])
    if x.shape != (state.cohorts.shape[1],):
        raise ValueError("inventory event must match the pinned symbol universe")
    return x


def _columns(cohorts: Tensor, updates: dict[F, Tensor]) -> Tensor:
    # PyTorch 2.11 cannot inline EnumType.__iter__ in a fullgraph CUDA kernel.
    # Materialize the fixed ABI indices at import time, outside the hot path.
    return torch.stack(
        [updates.get(f, cohorts[..., f]) for f in _COHORT_FIELD_INDICES], dim=-1
    )


def _commit_inventory(
    before: DayTradeInventoryState, after: DayTradeInventoryState, valid: Tensor
) -> DayTradeInventoryState:
    """Reject an entire invalid event without partial fills or context damage."""
    accepted = valid & (before.failed == 0) & (after.failed == 0)
    values = {}
    for name in before.__dataclass_fields__:
        old, new = getattr(before, name), getattr(after, name)
        if name in {"cohorts", "claims"} and old.shape[0] != new.shape[0]:
            old = torch.cat(
                (old, old.new_zeros((new.shape[0] - old.shape[0], *old.shape[1:])))
            )
        values[name] = torch.where(accepted, new, old)
    values["failed"] = (~accepted).to(dtype=before.failed.dtype)
    return DayTradeInventoryState(**values)


def append_inventory_fill(
    state: DayTradeInventoryState,
    *,
    signed_shares: Tensor,
    price: Tensor,
    buy_fee_rate: Tensor,
    day_sell_fee_rate: Tensor,
    normal_sell_fee_rate: Tensor,
    rebate_rate: Tensor,
    day: int,
) -> DayTradeInventoryState:
    """Append already-funded observed fills; do NOT size or invent liquidity.

    Fee inputs follow paper's GROSS rates plus a separate rebate. Averaged fill
    prices/basis are deliberately not rounded to an order-price tick grid.
    """
    q, p, buy, sell, normal, rebate = [
        _vector(state, x)
        for x in (
            signed_shares,
            price,
            buy_fee_rate,
            day_sell_fee_rate,
            normal_sell_fee_rate,
            rebate_rate,
        )
    ]
    valid = _physical_shares(q)
    valid = (
        _require(
            q * state.shares >= 0,
            "cannot open opposite inventory before FIFO reduction",
        )
        & valid
    )
    valid = (
        _require(
            (q == 0) | (torch.isfinite(p) & (p > 0)),
            "fill requires a positive observed price",
        )
        & valid
    )
    for rate in (buy - rebate, sell - rebate, normal - rebate):
        valid = _finite_nonnegative(rate, "net fee rate") & valid
    valid = _finite_nonnegative(rebate, "rebate rate") & valid
    valid = _calendar_day(state, day) & valid
    valid = (
        _require(state.action_cursor <= day, "fill precedes accepted corporate actions")
        & valid
    )
    valid = (
        _require(
            state.cohorts[..., F.ACQUIRED_DAY] <= day,
            "fill events must be chronological",
        )
        & valid
    )
    p = torch.where(q != 0, p, torch.zeros_like(p))
    entry_rate = torch.where(q >= 0, buy, sell) - rebate
    row = torch.stack(
        [
            q,
            p,
            q.abs() * p * entry_rate,
            p,
            torch.where(q >= 0, sell, buy) - rebate,
            torch.where(q >= 0, normal, buy) - rebate,
            (normal - sell).clamp_min(0) + MARGIN_SHORT_HANDLING_FEE_RATE,
            torch.full_like(q, day),
            torch.zeros_like(q),
            torch.full_like(q, day),
        ],
        dim=-1,
    )
    return _commit_inventory(
        state,
        replace(
            state,
            cohorts=torch.cat((state.cohorts, row.unsqueeze(0)), dim=0),
            observed_day=state.observed_day.new_tensor(day),
        ),
        valid,
    )


@dataclass(frozen=True)
class InventoryReduction:
    state: DayTradeInventoryState
    filled_shares: Tensor  # positive quantity per symbol
    gross_pnl: Tensor
    entry_fee_allocated: Tensor
    exit_fee: Tensor
    net_pnl: Tensor


def reduce_inventory_fifo(
    state: DayTradeInventoryState,
    *,
    requested_shares: Tensor,
    price: Tensor,
    capacity_shares: Tensor,
) -> InventoryReduction:
    """Consume one already-validated event budget once across all cohorts.

    Capacity is shared between buys/covers/sells, not multiplied by cohort
    count. Missing prices provide zero executable capacity. Exact quantities
    may include actual odd residuals under the user-approved board-price rule.
    """
    desired, p, capacity = [
        _vector(state, x) for x in (requested_shares, price, capacity_shares)
    ]
    valid = state.failed == 0
    for quantity in (desired, capacity):
        valid = _physical_shares(quantity) & valid
        valid = _finite_nonnegative(quantity, "reduction quantity/capacity") & valid
    q = state.cohorts[..., F.SHARES]
    absolute = q.abs()
    executable = torch.isfinite(p) & (p > 0)
    filled = torch.minimum(torch.minimum(desired, capacity), absolute.sum(0))
    filled = torch.where(executable & valid, filled, torch.zeros_like(filled))
    before = fifo_cumsum(absolute, 0) - absolute
    taken = torch.minimum(absolute, (filled - before).clamp_min(0))
    remaining_absolute = (absolute - taken).clamp_min(0)
    remaining_entry_cost = (
        state.cohorts[..., F.ENTRY_COST]
        * remaining_absolute
        / absolute.clamp_min(1)
    )
    # Derive the allocated fee from the nonnegative residual. Repeated partial
    # exits must not accumulate subtractive cancellation into a tiny negative
    # ENTRY_COST that passes hot-path NAV math but fails the checkpoint contract.
    entry_fee = state.cohorts[..., F.ENTRY_COST] - remaining_entry_cost
    rate = torch.where(
        state.cohorts[..., F.CONVERTED] != 0,
        state.cohorts[..., F.CARRY_EXIT_RATE],
        state.cohorts[..., F.DAY_EXIT_RATE],
    )
    safe_price = torch.where(executable, p, torch.zeros_like(p))
    gross = q.sign() * taken * (safe_price - state.cohorts[..., F.BASIS])
    exit_fee = taken * safe_price * rate
    net = net_liquidation_pnl(
        q.sign() * taken, state.cohorts[..., F.BASIS], safe_price, entry_fee, rate
    )
    cohorts = _columns(
        state.cohorts,
        {
            F.SHARES: q.sign() * remaining_absolute,
            F.ENTRY_COST: remaining_entry_cost,
        },
    )
    return InventoryReduction(
        _commit_inventory(
            state,
            replace(
                state,
                cohorts=cohorts,
                realized_net_pnl=state.realized_net_pnl + net.sum(),
            ),
            valid,
        ),
        taken.sum(0),
        gross.sum(0),
        entry_fee.sum(0),
        exit_fee.sum(0),
        net.sum(0),
    )


@dataclass(frozen=True)
class InventoryPathReduction:
    reduction: InventoryReduction
    minute_filled_shares: Tensor  # [symbol, chronological right-labelled minute]


def inventory_path_nav(
    state: DayTradeInventoryState,
    *,
    prices: Tensor,
    minute_filled_shares: Tensor,
    marks: Tensor,
    initial_capital: float,
) -> Tensor:
    """Exact minute net-liquidation NAV for a FIFO path before EOD conversion.

    No [cohort,symbol,minute] expansion is necessary. Realized and unrealized
    basis cancel algebraically, but each cohort still owns its exit fee rate.
    Caller applies the acquisition-day conversion to the final 13:30 endpoint.
    Marks must be observed prices (or explicitly identified causal carried
    last trades supplied by the outer source adapter), never interpolation.
    """
    if not math.isfinite(initial_capital) or initial_capital <= 0:
        raise ValueError("initial capital must be finite and positive")
    if (
        prices.ndim != 2
        or prices.shape[0] != state.cohorts.shape[1]
        or prices.shape[1] == 0
    ):
        raise ValueError("minute NAV requires [symbol,minute] prices")
    if marks.shape != prices.shape or minute_filled_shares.shape != prices.shape:
        raise ValueError("minute NAV requires matching price/fill/mark paths")
    prices, filled, marks = [
        x.to(device=state.cohorts.device, dtype=torch.float64)
        for x in (prices, minute_filled_shares, marks)
    ]
    valid = _physical_shares(filled) & _finite_nonnegative(filled, "minute fills")
    valid = (
        _require(
            (filled == 0) | (torch.isfinite(prices) & (prices > 0)),
            "minute fills require an observed price",
        )
        & valid
        & (state.failed == 0)
    )
    q = state.cohorts[..., F.SHARES]
    absolute = q.abs()
    closed = fifo_cumsum(filled, -1)
    total = absolute.sum(0)
    valid = (
        _require(closed <= total[:, None], "minute exits exceed held inventory") & valid
    )
    remaining = total[:, None] - closed
    observed = torch.isfinite(marks) & (marks > 0)
    mark_valid = ((remaining == 0) | observed).all(0)
    safe_marks = torch.where(observed, marks, 0)
    cash = fifo_cumsum(filled * torch.where(torch.isfinite(prices), prices, 0), -1)
    scalar = (
        initial_capital
        + state.realized_net_pnl
        + state.corporate_action_net
        - state.carry_cost
    )
    if q.shape[0] == 0:
        nav = scalar.expand(prices.shape[1])
        return torch.where(valid & mark_valid, nav, torch.full_like(nav, float("nan")))
    rate = torch.where(
        state.cohorts[..., F.CONVERTED] != 0,
        state.cohorts[..., F.CARRY_EXIT_RATE],
        state.cohorts[..., F.DAY_EXIT_RATE],
    )
    boundary = fifo_cumsum(absolute, 0).transpose(0, 1).contiguous()
    before = boundary - absolute.transpose(0, 1)
    zero = cash.new_zeros((cash.shape[0], 1))

    # Cash integral at cohort starts/ends, bounded by the final filled amount.
    def cash_at(quantity):
        x = torch.minimum(quantity, closed[:, -1:])
        index = torch.searchsorted(closed.contiguous(), x.contiguous()).clamp_max(
            prices.shape[1] - 1
        )
        left_q = torch.cat((zero, closed[:, :-1]), -1).gather(1, index)
        left_cash = torch.cat((zero, cash[:, :-1]), -1).gather(1, index)
        p = torch.where(torch.isfinite(prices), prices, 0).gather(1, index)
        return left_cash + (x - left_q) * p

    cash_before = cash_at(before)
    cohort_cash = cash_at(boundary) - cash_before
    rate = rate.transpose(0, 1)
    cohort_paid = cohort_cash * rate
    paid_before = fifo_cumsum(cohort_paid, -1) - cohort_paid
    quantity_rates = absolute.transpose(0, 1) * rate
    rate_before = fifo_cumsum(quantity_rates, -1) - quantity_rates
    index = torch.searchsorted(boundary, closed.contiguous()).clamp_max(q.shape[0] - 1)
    marginal_rate = rate.gather(1, index)
    paid = (
        paid_before.gather(1, index)
        + (cash - cash_before.gather(1, index)) * marginal_rate
    )
    closed_rate = (
        rate_before.gather(1, index)
        + (closed - before.gather(1, index)) * marginal_rate
    )
    remaining_rate = quantity_rates.sum(-1, keepdim=True) - closed_rate
    direction = state.shares.sign()[:, None]
    pnl = (
        direction * (cash + remaining * safe_marks)
        - (q * state.cohorts[..., F.BASIS]).sum(0)[:, None]
        - state.cohorts[..., F.ENTRY_COST].sum(0)[:, None]
        - paid
        - remaining_rate * safe_marks
    )
    nav = scalar + pnl.sum(0)
    return torch.where(valid & mark_valid, nav, torch.full_like(nav, float("nan")))


def reduce_inventory_fifo_path(
    state: DayTradeInventoryState,
    *,
    prices: Tensor,
    capacity_shares: Tensor,
) -> InventoryPathReduction:
    """Integrate a persistent FIFO exit over source-verified minute opportunities.

    Only the scheduler supplies opportunities (stop/limit/market/auction); this
    function must NOT treat every bar as a market order. No new inventory may
    enter inside this path. A cumulative-volume integral locates each cohort's
    boundary with searchsorted, avoiding a [cohort,symbol,minute] allocation or
    270 separate ledger updates. Fee/basis ownership is still per acquisition.
    Forward fills are physical shares; gradients follow the declared recurrent
    STE through the active piecewise-linear volume segment, not search indices.
    """
    if prices.ndim != 2 or prices.shape[0] != state.cohorts.shape[1]:
        raise ValueError("FIFO path requires [symbol,minute] prices")
    if capacity_shares.shape != prices.shape or prices.shape[1] == 0:
        raise ValueError("FIFO path requires matching nonempty minute capacities")
    prices = prices.to(device=state.cohorts.device, dtype=torch.float64)
    capacity = capacity_shares.to(device=prices.device, dtype=torch.float64)
    valid = _physical_shares(capacity) & _finite_nonnegative(capacity, "path capacity")
    valid = valid & (state.failed == 0)
    price_ok = torch.isfinite(prices) & (prices > 0)
    capacity = torch.where(price_ok & valid, capacity, 0)
    safe_price = torch.where(price_ok, prices, 0)
    cumulative = fifo_cumsum(capacity, -1)
    q = state.cohorts[..., F.SHARES]
    absolute = q.abs()
    total = absolute.sum(0)
    minute_fills = torch.minimum(
        capacity, (total[:, None] - (cumulative - capacity)).clamp_min(0)
    )
    closed = minute_fills.sum(-1)
    cohort_end = fifo_cumsum(absolute, 0)
    cohort_start = cohort_end - absolute
    taken = torch.minimum(absolute, (closed - cohort_start).clamp_min(0))

    # F(x) is the exact cash integral over the first x shares of capacity.
    # Searchsorted selects exogenous volume buckets, not future model inputs.
    cash_prefix = fifo_cumsum(capacity * safe_price, -1)
    zero = capacity.new_zeros((capacity.shape[0], 1))
    volume_left = torch.cat((zero, cumulative[:, :-1]), dim=-1)
    cash_left = torch.cat((zero, cash_prefix[:, :-1]), dim=-1)

    def integral(quantity: Tensor) -> Tensor:
        x = torch.minimum(quantity, closed).transpose(0, 1).contiguous()
        index = torch.searchsorted(cumulative.contiguous(), x, right=False).clamp_max(
            prices.shape[1] - 1
        )
        amount = cash_left.gather(1, index) + (
            x - volume_left.gather(1, index)
        ) * safe_price.gather(1, index)
        return amount.transpose(0, 1)

    proceeds = integral(cohort_end) - integral(cohort_start)
    remaining_absolute = (absolute - taken).clamp_min(0)
    remaining_entry_cost = (
        state.cohorts[..., F.ENTRY_COST]
        * remaining_absolute
        / absolute.clamp_min(1)
    )
    entry_fee = state.cohorts[..., F.ENTRY_COST] - remaining_entry_cost
    exit_rate = torch.where(
        state.cohorts[..., F.CONVERTED] != 0,
        state.cohorts[..., F.CARRY_EXIT_RATE],
        state.cohorts[..., F.DAY_EXIT_RATE],
    )
    gross = q.sign() * (proceeds - taken * state.cohorts[..., F.BASIS])
    exit_fee = proceeds * exit_rate
    net = gross - entry_fee - exit_fee
    updated = _commit_inventory(
        state,
        replace(
            state,
            realized_net_pnl=state.realized_net_pnl + net.sum(),
            cohorts=_columns(
                state.cohorts,
                {
                    F.SHARES: q.sign() * remaining_absolute,
                    F.ENTRY_COST: remaining_entry_cost,
                },
            ),
        ),
        valid,
    )
    return InventoryPathReduction(
        InventoryReduction(
            updated, closed, gross.sum(0), entry_fee.sum(0), exit_fee.sum(0), net.sum(0)
        ),
        minute_fills,
    )


def convert_inventory_to_margin(
    state: DayTradeInventoryState, *, day: int
) -> DayTradeInventoryState:
    """One-time short tax/handling conversion; day zero has no interest."""
    valid = _calendar_day(state, day)
    c = state.cohorts
    active = c[..., F.SHARES] != 0
    valid = (
        _require(
            ~active | (c[..., F.ACQUIRED_DAY] <= day), "conversion precedes acquisition"
        )
        & valid
    )
    fresh = active & (c[..., F.CONVERTED] == 0)
    valid = (
        _require(
            ~fresh | (c[..., F.ACQUIRED_DAY] == day),
            "missing acquisition-day margin conversion",
        )
        & valid
    )
    charge = torch.where(
        fresh & (c[..., F.SHARES] < 0),
        c[..., F.SHARES].abs()
        * c[..., F.ENTRY_PRICE]
        * c[..., F.SHORT_CONVERSION_RATE],
        0,
    )
    return _commit_inventory(
        state,
        replace(
            state,
            carry_cost=state.carry_cost + charge.sum(),
            observed_day=state.observed_day.new_tensor(day),
            cohorts=_columns(
                c,
                {
                    F.CONVERTED: torch.where(
                        fresh, torch.ones_like(c[..., F.CONVERTED]), c[..., F.CONVERTED]
                    ),
                    F.ACCRUED_THROUGH: torch.where(
                        fresh,
                        torch.full_like(c[..., F.ACCRUED_THROUGH], day),
                        c[..., F.ACCRUED_THROUGH],
                    ),
                },
            ),
        ),
        valid,
    )


def accrue_inventory_interest(
    state: DayTradeInventoryState, *, day: int
) -> DayTradeInventoryState:
    """Accrue actual remaining principal over calendar days, exactly once."""
    valid = _calendar_day(state, day)
    c = state.cohorts
    active = (c[..., F.SHARES] != 0) & (c[..., F.CONVERTED] != 0)
    valid = (
        _require(
            ~active | (c[..., F.ACCRUED_THROUGH] <= day),
            "interest clock moved backwards",
        )
        & valid
    )
    elapsed = torch.where(active, day - c[..., F.ACCRUED_THROUGH], 0)
    # where(bool, Python float, Python float) otherwise creates float32 even
    # for a float64 ledger, introducing a different interest rate from paper.
    rate = torch.where(
        c[..., F.SHARES] > 0,
        torch.full_like(
            c[..., F.BASIS],
            MARGIN_FINANCING_PRINCIPAL_RATIO * MARGIN_FINANCING_ANNUAL_RATE,
        ),
        torch.full_like(c[..., F.BASIS], MARGIN_SHORT_ANNUAL_BORROW_RATE),
    )
    interest = inventory_carry_interest(
        c[..., F.SHARES], c[..., F.BASIS], rate, elapsed
    )
    return _commit_inventory(
        state,
        replace(
            state,
            carry_cost=state.carry_cost + interest.sum(),
            observed_day=state.observed_day.new_tensor(day),
            cohorts=_columns(
                c,
                {
                    F.ACCRUED_THROUGH: torch.where(
                        active,
                        torch.full_like(c[..., F.ACCRUED_THROUGH], day),
                        c[..., F.ACCRUED_THROUGH],
                    ),
                },
            ),
        ),
        valid,
    )


def inventory_nav(
    state: DayTradeInventoryState, *, initial_capital: float, marks: Tensor
) -> Tensor:
    """Economic net-liquidation equity, NOT broker cash/buying power."""
    if not math.isfinite(initial_capital) or initial_capital <= 0:
        raise ValueError("initial capital must be finite and positive")
    p = _vector(state, marks)
    c = state.cohorts
    active = c[..., F.SHARES] != 0
    valid = _require(
        ~active | (torch.isfinite(p) & (p > 0)),
        "held inventory is missing a source-backed mark",
    ) & (state.failed == 0)
    p = torch.where(torch.isfinite(p) & (p > 0), p, torch.zeros_like(p))
    exit_rate = torch.where(
        c[..., F.CONVERTED] != 0, c[..., F.CARRY_EXIT_RATE], c[..., F.DAY_EXIT_RATE]
    )
    unrealized = net_liquidation_pnl(
        c[..., F.SHARES], c[..., F.BASIS], p, c[..., F.ENTRY_COST], exit_rate
    )
    nav = (
        initial_capital
        + state.realized_net_pnl
        + state.corporate_action_net
        - state.carry_cost
        + unrealized.sum()
    )
    return torch.where(valid, nav, torch.full_like(nav, float("nan")))


def apply_inventory_action(
    state: DayTradeInventoryState,
    *,
    event_day: int,
    as_of_day: int,
    event_mask: Tensor,
    share_ratio: Tensor,
    cash_per_old_share: Tensor,
    payment_day: Tensor,
) -> DayTradeInventoryState:
    """Apply a source-verified physical/cash action at its effective instant.

    The data adapter must resolve suspensions, exact terms and receipts before
    this call. For a physical replacement event_day is the resumption day, not
    the announcement or ex-date. Cash is signed on OLD physical quantities;
    basis is divided by the ratio without also subtracting cash. No shares
    acquired on/after the effective date receive an entitlement. One aggregated
    event per symbol/date is required; duplicate/revised events are rejected,
    not silently reapplied. Caller checkpointing makes retry idempotent.
    """
    valid = _calendar_day(state, as_of_day)
    if (
        not isinstance(event_day, int)
        or isinstance(event_day, bool)
        or event_day > as_of_day
        or event_day <= 0
    ):
        raise ValueError("corporate action is not yet effective")
    valid = (
        _require(
            state.decision_day < event_day,
            "corporate action must precede that session's decision",
        )
        & valid
    )
    mask = _vector(state, event_mask).bool()
    ratio, cash, payment = [
        _vector(state, x) for x in (share_ratio, cash_per_old_share, payment_day)
    ]
    valid = (
        _require(
            ~mask | (state.action_cursor < event_day),
            "duplicate or out-of-order corporate action",
        )
        & valid
    )
    valid = (
        _require(
            ~mask
            | (
                torch.isfinite(ratio) & (ratio > 0) & torch.isfinite(cash) & (cash >= 0)
            ),
            "invalid corporate-action terms",
        )
        & valid
    )
    valid = (
        _require(
            ~mask
            | (cash == 0)
            | (
                torch.isfinite(payment)
                & (payment >= event_day)
                & (payment == payment.round())
            ),
            "cash entitlement requires an exact payment date",
        )
        & valid
    )
    # Ignore absent symbols before arithmetic: 0 * NaN is still NaN.
    ratio = torch.where(mask, ratio, torch.ones_like(ratio))
    cash = torch.where(mask, cash, torch.zeros_like(cash))
    c = state.cohorts
    entitled = mask & (c[..., F.SHARES] != 0) & (c[..., F.ACQUIRED_DAY] < event_day)
    valid = (
        _require(
            ~entitled | (c[..., F.ACCRUED_THROUGH] <= event_day),
            "corporate action applied after inventory already advanced",
        )
        & valid
    )
    changed_q = c[..., F.SHARES] * ratio
    valid = (
        _physical_shares(torch.where(entitled, changed_q, torch.zeros_like(changed_q)))
        & valid
    )
    new_q = changed_q + (changed_q.round() - changed_q).detach()
    signed_claim = torch.where(entitled, c[..., F.SHARES] * cash, 0).sum(dim=0)
    paid = (payment <= as_of_day).to(dtype=c.dtype)
    claim = torch.stack(
        (
            signed_claim,
            torch.where(signed_claim != 0, payment, 0),
            torch.where(signed_claim != 0, paid, 0),
        ),
        dim=-1,
    )
    return _commit_inventory(
        state,
        replace(
            state,
            cohorts=_columns(
                c,
                {
                    F.SHARES: torch.where(entitled, new_q, c[..., F.SHARES]),
                    F.BASIS: torch.where(
                        entitled, c[..., F.BASIS] / ratio, c[..., F.BASIS]
                    ),
                },
            ),
            claims=torch.cat((state.claims, claim.unsqueeze(0)), dim=0),
            observed_day=state.observed_day.new_tensor(as_of_day),
            action_cursor=torch.where(
                mask,
                torch.full_like(state.action_cursor, event_day),
                state.action_cursor,
            ),
        ),
        valid,
    )


def settle_inventory_claims(
    state: DayTradeInventoryState, *, day: int
) -> DayTradeInventoryState:
    """Move dated entitlements to cash without recognizing the income twice."""
    valid = _calendar_day(state, day)
    claims = state.claims
    valid = valid & _valid_claim_terms(claims)
    paid = torch.maximum(claims[..., 2], (claims[..., 1] <= day).to(dtype=claims.dtype))
    return _commit_inventory(
        state,
        replace(
            state,
            claims=torch.stack((claims[..., 0], claims[..., 1], paid), dim=-1),
            observed_day=state.observed_day.new_tensor(day),
        ),
        valid,
    )


@dataclass(frozen=True)
class InventoryTargetDelta:
    reduction: Tensor
    addition: Tensor  # signed quantity; still requires funding/entry eligibility
    remaining_capacity: Tensor
    valid: Tensor


def inventory_target_delta(
    held: Tensor, target: Tensor, capacity: Tensor
) -> InventoryTargetDelta:
    """Reduce first, then add, sharing a single symbol-minute capacity budget.

    Inputs are already lot-sized; target admission/price validity/funding are
    outside this algebra. A blocked reduction cannot authorize an opposite
    side opening. No whole-lot rounding is applied to real odd residuals.
    """
    if held.shape != target.shape or held.shape != capacity.shape:
        raise ValueError("target delta requires matching symbol vectors")
    valid = _finite_nonnegative(capacity, "minute capacity")
    for quantity in (held, target, capacity):
        valid = _physical_shares(quantity) & valid
    same_side = held * target > 0
    requested_reduce = torch.where(
        same_side, (held.abs() - target.abs()).clamp_min(0), held.abs()
    )
    reduction = torch.minimum(requested_reduce, capacity)
    remainder = held - held.sign() * reduction
    unused = capacity - reduction
    addition = torch.where(
        remainder * target >= 0, (target.abs() - remainder.abs()).clamp_min(0), 0
    )
    addition = target.sign() * torch.minimum(addition, unused)
    return InventoryTargetDelta(
        torch.where(valid, reduction, 0),
        torch.where(valid, addition, 0),
        torch.where(valid, unused - addition.abs(), 0),
        valid,
    )


@dataclass(frozen=True)
class InventoryOpeningResult:
    state: DayTradeInventoryState
    sizing_nav: Tensor
    target_shares: Tensor
    reduction: InventoryReduction
    addition_shares: Tensor
    capacity_remaining: Tensor
    gross_addition_cost: Tensor


def rebalance_inventory_at_open(
    state: DayTradeInventoryState,
    *,
    weights: Tensor,
    official_open: Tensor,
    opening_marks: Tensor | None = None,
    entry_price: Tensor,
    entry_volume_shares: Tensor,
    lower_limit: Tensor,
    upper_limit: Tensor,
    can_enter: Tensor,
    buy_fee_rate: Tensor,
    day_sell_fee_rate: Tensor,
    normal_sell_fee_rate: Tensor,
    rebate_rate: Tensor,
    initial_capital: float,
    day: int,
    halted: Tensor | None = None,
    daily_proxy_mask: Tensor | None = None,
) -> InventoryOpeningResult:
    """One daily decision and its 09:01 historical inventory-delta execution.

    The caller owns accepted corporate-action/source receipts and passes the
    action-adjusted state BEFORE this function. `can_enter` is the exact-day
    side/eligibility mask, never an ex-post successful-fill mask. Halted shares
    may not be reduced either. Official open sizes the target; only the observed
    09:01 VWAP/Close prices a fill. This function is not a live broker simulator.

    ``daily_proxy_mask`` is an explicit retrospective research fallback.  It
    permits an official daily OPEN proxy to stand in for the unavailable 09:01
    execution observation without inventing a historical price-limit band.
    It is never valid for a live order or an observed minute path.

    Funding matches paper's gross-NAV risk budget, including gross commission
    reservation and unpaid corporate receivables. It is not the legacy T+2
    cash-only sizing rule or a claim to verified broker buying power.
    """
    # Reuse the existing exact-forward lot and 50%-volume kernels. The minute
    # runner will call this component only under its new stateful tape ABI.
    from stockagent.backtest.tw_day_trade_minute import _capacity, _ste_floor_lots

    w, opening, price, volume, lower, upper, eligible, buy, sell, normal, rebate = [
        _vector(state, x)
        for x in (
            weights,
            official_open,
            entry_price,
            entry_volume_shares,
            lower_limit,
            upper_limit,
            can_enter,
            buy_fee_rate,
            day_sell_fee_rate,
            normal_sell_fee_rate,
            rebate_rate,
        )
    ]
    original = state
    valid = _require(torch.isfinite(w), "nonfinite target weight")
    valid = (
        _require(state.decision_day < day, "duplicate or out-of-order daily decision")
        & valid
    )
    c = state.cohorts
    active = c[..., F.SHARES] != 0
    valid = (
        _require(
            ~active | ((c[..., F.ACQUIRED_DAY] < day) & (c[..., F.CONVERTED] != 0)),
            "opening requires converted PRIOR-session inventory, not duplicate same-day entries",
        )
        & valid
    )
    state = settle_inventory_claims(accrue_inventory_interest(state, day=day), day=day)
    valuation = opening if opening_marks is None else _vector(state, opening_marks)
    nav = inventory_nav(state, initial_capital=initial_capital, marks=valuation)
    valid = (
        _require(
            torch.isfinite(nav) & (nav > 0),
            "nonpositive account NAV; cannot reset to initial capital",
        )
        & valid
    )
    # Invalid economic/source state cannot submit or fund another order. Its
    # failure flag is absorbing; NAV is never reset to the initial capital.
    nav_for_sizing = torch.where(valid, nav, torch.zeros_like(nav))
    w = torch.where(torch.isfinite(w), w, torch.zeros_like(w))
    open_ok = torch.isfinite(opening) & (opening > 0)
    safe_open = torch.where(open_ok, opening, torch.ones_like(opening))
    halted_mask = (
        torch.zeros_like(open_ok) if halted is None else _vector(state, halted).bool()
    )
    if daily_proxy_mask is None:
        proxy = torch.zeros_like(open_ok)
    else:
        raw_proxy = _vector(state, daily_proxy_mask)
        valid = (
            _require(
                torch.isfinite(raw_proxy)
                & ((raw_proxy == 0) | (raw_proxy == 1)),
                "daily proxy mask must contain exact binary values",
            )
            & valid
        )
        proxy = raw_proxy.bool()
    enter = eligible.bool() & open_ok & ~halted_mask & valid
    target = w.sign() * _ste_floor_lots(
        w.abs() * nav_for_sizing.clamp_min(0) / safe_open
    )
    target = torch.where(enter, target, torch.zeros_like(target))
    source_limited_price = (
        torch.isfinite(price)
        & (price > 0)
        & torch.isfinite(lower)
        & torch.isfinite(upper)
        & (lower > 0)
        & (lower <= upper)
        & (price >= lower)
        & (price <= upper)
    )
    source_proven_proxy_price = proxy & torch.isfinite(price) & (price > 0)
    price_ok = (
        (source_limited_price | source_proven_proxy_price)
        & open_ok
        & ~halted_mask
    )
    capacity = torch.where(
        price_ok & valid, _capacity(volume), torch.zeros_like(volume)
    )
    delta = inventory_target_delta(state.shares, target, capacity)
    valid = valid & delta.valid
    reduction = reduce_inventory_fifo(
        state, requested_shares=delta.reduction, price=price, capacity_shares=capacity
    )
    remaining = reduction.state.shares
    # Reductions and additions share ONE observed-minute bucket. Failed exits
    # cannot free capacity or reserve short proceeds as purchasing power.
    wanted = inventory_target_delta(
        remaining, target, capacity - reduction.filled_shares
    ).addition
    wanted = torch.where(enter, wanted, torch.zeros_like(wanted))
    budget = (
        nav_for_sizing
        - (remaining.abs() * torch.where(torch.isfinite(valuation), valuation, 0)).sum()
        - reduction.exit_fee.sum()
        - state.corporate_action_receivable
    ).clamp_min(0)
    safe_price = torch.where(price_ok, price, torch.zeros_like(price))
    gross_rate = torch.where(wanted >= 0, buy, sell)
    valid = _finite_nonnegative(gross_rate, "gross funding fee") & valid
    gross_cost = (wanted.abs() * safe_price * (1 + gross_rate)).sum()
    tolerance = torch.maximum(budget.new_tensor(1e-9), budget.abs() * 1e-12)
    scale = torch.where(
        gross_cost <= budget + tolerance,
        torch.ones_like(budget),
        (budget / gross_cost.clamp_min(1e-30)).clamp(max=1),
    )
    # The existing paper integer allocator has linear fees here (minimum=0,
    # rounding=none), so one common scalar scale followed by lot flooring is
    # its exact solution. Differential tests bind this restricted kernel to
    # _scale_lot_buys_to_budget_with_fixed_fees, not a new allocation heuristic.
    steps = torch.where(
        state.shares.remainder(1000) != 0,
        torch.ones_like(wanted),
        torch.full_like(wanted, 1000),
    )
    raw = wanted.abs() * scale
    exact = torch.floor(raw / steps + 1e-12) * steps
    funded = raw + (exact - raw).detach()
    additions = wanted.sign() * funded
    funded_cost = (funded * safe_price * (1 + gross_rate)).sum()
    valid = (
        _require(
            funded_cost <= budget + tolerance, "gross-NAV funding invariant violated"
        )
        & valid
    )
    updated = append_inventory_fill(
        reduction.state,
        signed_shares=additions,
        price=safe_price,
        buy_fee_rate=buy,
        day_sell_fee_rate=sell,
        normal_sell_fee_rate=normal,
        rebate_rate=rebate,
        day=day,
    )
    updated = replace(updated, decision_day=updated.decision_day.new_tensor(day))
    updated = _commit_inventory(original, updated, valid)
    accepted = updated.failed == 0
    reduction = replace(
        reduction,
        **{
            name: torch.where(accepted, getattr(reduction, name), 0)
            for name in (
                "filled_shares",
                "gross_pnl",
                "entry_fee_allocated",
                "exit_fee",
                "net_pnl",
            )
        },
    )
    return InventoryOpeningResult(
        updated,
        nav,
        target,
        reduction,
        torch.where(accepted, additions, 0),
        torch.where(accepted, capacity - reduction.filled_shares - additions.abs(), 0),
        torch.where(accepted, funded_cost, 0),
    )
