"""Optional backward-only learning signal for unfilled sub-board-lot requests.

The authoritative account remains in run_backtest_torch. Counterfactual labels
reuse its FIFO reductions, source opportunities and fees. This is a declared
surrogate gradient, not the derivative of the discontinuous integer account.
It is restricted to flat-at-close policies with guaranteed terminal liquidation.
"""
from __future__ import annotations

from dataclasses import dataclass

import torch
from stockagent.backtest.tw_day_trade_minute import _ste_floor_lots

from stockagent.backtest.tw_day_trade_inventory import (
    DayTradeInventoryState, append_inventory_fill, reduce_inventory_fifo,
    reduce_inventory_fifo_liquidity, reduce_inventory_fifo_sparse_liquidity,
)


def _mark_extrema(session):
    if session.mark_path_valid is not None:
        return session.minimum_marks, session.maximum_marks, session.mark_path_valid.bool()
    valid = (torch.isfinite(session.marks) & (session.marks > 0)).all(dim=1)
    return session.marks.amin(dim=1), session.marks.amax(dim=1), valid


@dataclass(frozen=True)
class SubLotRecoveryLabels:
    """Exogenous one-lot outcomes that may be reused across optimizer epochs.

    These tensors contain no model action, NAV, funding or account-survival
    decision.  Those policy-dependent gates remain in
    :func:`sub_lot_recovery_delta` and are recomputed for every forward pass.
    """

    long_pnl: torch.Tensor
    long_valid: torch.Tensor
    short_pnl: torch.Tensor
    short_valid: torch.Tensor


@torch.no_grad()
def build_sub_lot_recovery_labels(
    sessions, *, buy, sell, normal, rebate,
) -> tuple[SubLotRecoveryLabels, ...]:
    """Precompute source/fee-only one-lot labels for immutable sessions."""

    labels = []
    for session in sessions:
        eligible = torch.ones_like(session.official_open, dtype=torch.bool)
        long_pnl, long_valid = one_lot_net_pnl(
            session,
            side=1,
            eligible=eligible,
            buy=buy,
            sell=sell,
            normal=normal,
            rebate=rebate,
        )
        short_pnl, short_valid = one_lot_net_pnl(
            session,
            side=-1,
            eligible=eligible,
            buy=buy,
            sell=sell,
            normal=normal,
            rebate=rebate,
        )
        labels.append(
            SubLotRecoveryLabels(
                long_pnl=long_pnl,
                long_valid=long_valid,
                short_pnl=short_pnl,
                short_valid=short_valid,
            )
        )
    return tuple(labels)


def _validate_recovery_labels(
    labels: tuple[SubLotRecoveryLabels, ...], sessions, weights: torch.Tensor,
) -> None:
    if len(labels) != len(sessions) or len(labels) != int(weights.shape[0]):
        raise ValueError("sub-lot recovery labels differ from the session axis")
    expected = (int(weights.shape[1]),)
    for label, session in zip(labels, sessions, strict=True):
        for name in ("long_pnl", "long_valid", "short_pnl", "short_valid"):
            value = getattr(label, name)
            if value.shape != expected or value.device != weights.device:
                raise ValueError(
                    f"sub-lot recovery label {name} differs from the action axis/device"
                )
        if label.long_pnl.dtype != torch.float64 or label.short_pnl.dtype != torch.float64:
            raise ValueError("sub-lot recovery PnL labels must retain exact FP64")
        if label.long_valid.dtype != torch.bool or label.short_valid.dtype != torch.bool:
            raise ValueError("sub-lot recovery validity labels must be boolean")
        if session.official_open.shape != expected:
            raise ValueError("sub-lot recovery session differs from the action axis")


@torch.no_grad()
def one_lot_net_pnl(session, *, side, eligible, buy, sell, normal, rebate):
    """Independent one-board-lot outcomes, not a jointly funded portfolio.

    Inventory reductions are separable by symbol. The caller separately gates
    affordability; these independent outcomes must never enter account state.
    """
    if session.terminal_liquidation_price is None:
        raise ValueError("sub-lot recovery requires terminal official-close liquidation")
    price = session.entry_price
    proxy = (torch.zeros_like(eligible) if session.daily_proxy_mask is None
             else session.daily_proxy_mask.bool())
    permitted = (eligible.bool() & torch.isfinite(price) & (price > 0)
                 & torch.isfinite(session.official_open) & (session.official_open > 0)
                 & torch.isfinite(session.entry_volume) & (session.entry_volume >= 2000)
                 & ~session.halted.bool())
    in_limits = (torch.isfinite(session.lower_limit) & torch.isfinite(session.upper_limit)
                 & (session.lower_limit > 0) & (price >= session.lower_limit)
                 & (price <= session.upper_limit))
    permitted &= proxy | in_limits
    _, _, path_valid = _mark_extrema(session)
    permitted &= path_valid & torch.isfinite(session.opening_marks) & (session.opening_marks > 0)
    for gap in (session.source_gap_mask, session.unresolved_action_gap_mask):
        if gap is not None:
            permitted &= ~gap.bool()
    terminal = session.terminal_liquidation_price
    permitted &= torch.isfinite(terminal) & (terminal > 0)
    quantity = permitted.to(torch.float64) * (1000 * side)
    inventory = append_inventory_fill(
        DayTradeInventoryState.empty(price.numel(), device=price.device),
        signed_shares=quantity, price=torch.where(permitted, price, 0),
        buy_fee_rate=buy, day_sell_fee_rate=sell, normal_sell_fee_rate=normal,
        rebate_rate=rebate, day=session.day,
    )
    if session.exit_symbol_indices is not None:
        reduced = reduce_inventory_fifo_sparse_liquidity(
            inventory, prices=session.exit_prices, capacity_shares=session.exit_capacity,
            event_symbol_indices=session.exit_symbol_indices, event_sides=session.exit_sides,
            symbol_event_starts=session.symbol_event_starts, symbol_event_ends=session.symbol_event_ends,
        ).reduction
    else:
        reduced = reduce_inventory_fifo_liquidity(
            inventory, prices=session.exit_prices[..., 0 if side > 0 else 1],
            capacity_shares=session.exit_capacity[..., 0 if side > 0 else 1],
        ).reduction
    remaining = reduced.state.shares.abs()
    final = reduce_inventory_fifo(
        reduced.state, requested_shares=remaining,
        price=torch.where(permitted, terminal, 0), capacity_shares=remaining,
    )
    valid = permitted & (final.state.failed == 0)
    return torch.where(valid, reduced.net_pnl + final.net_pnl, 0), valid


def sub_lot_recovery_delta(
    weights, sessions, backtest, *, can_long, can_short, buy, sell, normal, rebate,
    initial_nav, labels: tuple[SubLotRecoveryLabels, ...] | None = None,
):
    """Return exact zero with a one-lot secant gradient on sub-lot actions only.

    A candidate must fit in the unused requested cash sleeve, including gross
    entry fees. Never redistribute another stock's allocation, recover a dead
    account, or invent liquidity. Filled actions keep their existing gradient.
    """
    if labels is None:
        labels = build_sub_lot_recovery_labels(
            sessions, buy=buy, sell=sell, normal=normal, rebate=rebate
        )
    _validate_recovery_labels(labels, sessions, weights)
    slopes = []
    with torch.no_grad():
        navs = torch.cat((torch.as_tensor(initial_nav, device=weights.device, dtype=torch.float64).reshape(1),
                          backtest.minute_nav[:-1, -1].detach()))
        for row, (session, label) in enumerate(zip(sessions, labels, strict=True)):
            current = weights[row].detach().double()
            nav = navs[row]
            opening = session.official_open
            safe_open = torch.where(torch.isfinite(opening) & (opening > 0), opening, 1)
            entry = torch.nan_to_num(session.entry_price, nan=0., posinf=0., neginf=0.)
            # Conservative cash reservation for every requested sleeve before
            # capacity/lot rejection; unused fills are never reallocated here.
            rates = torch.where(current >= 0, buy, sell)
            reserved = current.abs() * nav * entry / safe_open * (1 + rates)
            free_for_candidate = nav - reserved.sum() + reserved
            long_pnl = label.long_pnl
            short_pnl = label.short_pnl
            long_valid = label.long_valid & can_long[row]
            short_valid = label.short_valid & can_short[row]
            long_valid &= entry * 1000 * (1 + buy) <= free_for_candidate
            short_valid &= entry * 1000 * (1 + sell) <= free_for_candidate
            minimum_mark, maximum_mark, _ = _mark_extrema(session)
            # Conservative extra-position solvency proof. A profitable close
            # cannot authorize a counterfactual that would default intraday.
            minimum_nav = backtest.minute_nav[row].detach().amin()
            long_worst = 1000 * (minimum_mark - entry - entry * (buy - rebate) - minimum_mark * (sell - rebate))
            short_worst = 1000 * (entry - maximum_mark - entry * (sell - rebate) - maximum_mark * (buy - rebate))
            long_valid &= minimum_nav + long_worst > 0
            short_valid &= minimum_nav + short_worst > 0
            threshold = 1000 * safe_open / nav.clamp_min(1e-30)
            long_gain = torch.where(long_valid, torch.log1p((long_pnl / nav.clamp_min(1e-30)).clamp_min(-.999999)), 0)
            short_gain = torch.where(short_valid, torch.log1p((short_pnl / nav.clamp_min(1e-30)).clamp_min(-.999999)), 0)
            at_zero = torch.where(long_gain >= short_gain, long_gain.clamp_min(0), -short_gain.clamp_min(0))
            gain = torch.where(current > 0, long_gain, torch.where(current < 0, -short_gain, at_zero))
            sub_lot = (_ste_floor_lots(current.abs() * nav.clamp_min(0) / safe_open) == 0) & (nav > 0)
            # A guaranteed terminal account is flat. Refuse to attach labels to
            # any unexpected carry/default rather than silently approximate it.
            flat = (backtest.shares_history[row] == 0).all() & ~backtest.settlement_default[row]
            slopes.append(torch.where(sub_lot & flat, gain / threshold, 0))
        slope = torch.stack(slopes).to(weights.dtype)
    return ((weights - weights.detach()) * slope).sum(dim=1)
