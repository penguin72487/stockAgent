"""Funding-aware target-weight ledger for linear crypto perpetual contracts."""

from __future__ import annotations

from dataclasses import dataclass
import os
import threading
from typing import Callable

import torch


_MIN_WEALTH_FACTOR = 1.0e-6
_DAY_KERNEL_CACHE: dict[
    tuple[object, ...], Callable[..., tuple[torch.Tensor, ...]]
] = {}
_DAY_KERNEL_LOCK = threading.Lock()


@dataclass(slots=True)
class CryptoPerpetualTensorResult:
    strategy_simple_returns: torch.Tensor
    turnovers: torch.Tensor
    executed_weights: torch.Tensor
    final_weights: torch.Tensor
    final_alive: torch.Tensor


def _gross_cap(
    previous: torch.Tensor,
    proposed: torch.Tensor,
    maximum_gross: float,
) -> torch.Tensor:
    """Apply reductions first, then fit only expansions into the gross budget.

    Scaling a complete proposed portfolio would silently sell unchanged names
    whenever another name breached the cap.  Decomposing every transition at
    zero preserves all requested reductions and scales only same-side increases
    or the opening leg of a sign flip.
    """

    cap = torch.as_tensor(maximum_gross, device=proposed.device, dtype=proposed.dtype)
    same_side = previous * proposed >= 0.0
    same_side_reduction = same_side & (proposed.abs() < previous.abs())
    crosses_zero = ~same_side
    reduced = torch.where(
        same_side_reduction,
        proposed,
        torch.where(crosses_zero, torch.zeros_like(previous), previous),
    )
    expansion = proposed - reduced
    reduced_gross = reduced.abs().sum()
    expansion_gross = expansion.abs().sum()
    reduction_scale = torch.minimum(
        torch.ones_like(reduced_gross),
        cap / reduced_gross.clamp_min(1.0e-12),
    )
    cap_breached_after_reductions = reduced_gross > cap
    reduced_at_cap = reduced * reduction_scale
    available = (cap - reduced_gross).clamp_min(0.0)
    expansion_scale = torch.minimum(
        torch.ones_like(expansion_gross),
        available / expansion_gross.clamp_min(1.0e-12),
    )
    normal = reduced + expansion * expansion_scale
    # Mark-to-market drift can push the carried portfolio above the cap before
    # the next decision.  Requested reductions run first, but if their endpoint
    # is still over budget the cap is a hard risk constraint: deleverage the
    # reduced book and admit no expansion on that row.
    return torch.where(cap_breached_after_reductions, reduced_at_cap, normal)


def _day_kernel_factory(
    *,
    buy_fee_rate: float,
    sell_fee_rate: float,
    long_only: bool,
    maximum_gross: float,
    max_turnover_ratio: float,
) -> Callable[..., tuple[torch.Tensor, ...]]:
    def day(
        previous: torch.Tensor,
        alive: torch.Tensor,
        target: torch.Tensor,
        effective_simple: torch.Tensor,
        price_simple: torch.Tensor,
        effective_finite: torch.Tensor,
        price_finite: torch.Tensor,
        tradable: torch.Tensor,
        can_buy: torch.Tensor,
        can_sell: torch.Tensor,
        can_short: torch.Tensor,
        force_exit: torch.Tensor,
        volume: torch.Tensor,
        row_advances: torch.Tensor,
    ) -> tuple[torch.Tensor, ...]:
        previous = torch.where(alive, previous, torch.zeros_like(previous))
        forced = force_exit & row_advances
        forced_delta = torch.where(forced, -previous, torch.zeros_like(previous))
        base = torch.where(forced, torch.zeros_like(previous), previous)
        requested = torch.where(
            alive & row_advances & tradable & ~forced,
            target,
            base,
        )
        if long_only:
            requested = requested.clamp_min(0.0)
        delta = requested - base
        constrained = torch.where((delta > 0.0) & ~can_buy, base, requested)
        down = constrained < base
        reduce_long = down & (base > 0.0) & (constrained >= 0.0) & ~can_sell
        constrained = torch.where(reduce_long, base, constrained)
        cross_long = down & (base > 0.0) & (constrained < 0.0)
        constrained = torch.where(cross_long & ~can_sell, base, constrained)
        constrained = torch.where(
            cross_long & can_sell & ~can_short,
            torch.zeros_like(constrained),
            constrained,
        )
        increase_short = down & (base <= 0.0) & (constrained < base) & ~can_short
        constrained = torch.where(increase_short, base, constrained)
        delta = constrained - base
        delta = delta.sign() * torch.minimum(delta.abs(), volume)
        if max_turnover_ratio > 0.0:
            turnover_before_cap = delta.abs().sum()
            scale = torch.minimum(
                torch.ones_like(turnover_before_cap),
                torch.as_tensor(
                    max_turnover_ratio,
                    device=previous.device,
                    dtype=previous.dtype,
                )
                / turnover_before_cap.clamp_min(1.0e-12),
            )
            delta = delta * scale
        executed = _gross_cap(base, base + delta, maximum_gross)
        delta = executed - base

        buy_turnover = delta.clamp_min(0.0).sum() + forced_delta.clamp_min(0.0).sum()
        sell_turnover = (-delta).clamp_min(0.0).sum() + (-forced_delta).clamp_min(
            0.0
        ).sum()
        active = row_advances & (executed.abs() > 1.0e-10)
        valuation_complete = (~active | (effective_finite & price_finite)).all()
        effective_pnl = (executed * effective_simple).sum()
        net_simple = (
            effective_pnl
            - float(buy_fee_rate) * buy_turnover
            - float(sell_fee_rate) * sell_turnover
        )
        survived_on_advance = (
            valuation_complete
            & torch.isfinite(net_simple)
            & (1.0 + net_simple > _MIN_WEALTH_FACTOR)
        )
        survived = torch.where(
            row_advances,
            survived_on_advance,
            torch.ones_like(survived_on_advance),
        )
        net_simple = torch.where(alive, net_simple, torch.zeros_like(net_simple))
        ruined_now = alive & row_advances & ~survived_on_advance
        net_simple = torch.where(
            ruined_now,
            torch.full_like(net_simple, _MIN_WEALTH_FACTOR - 1.0),
            net_simple,
        )
        executed = torch.where(alive, executed, torch.zeros_like(executed))
        turnover = torch.where(
            alive, buy_turnover + sell_turnover, torch.zeros_like(buy_turnover)
        )
        wealth = (1.0 + net_simple).clamp_min(_MIN_WEALTH_FACTOR)
        marked_notional = torch.where(
            row_advances,
            executed * (1.0 + price_simple),
            executed,
        )
        next_previous = marked_notional / wealth
        next_alive = alive & survived
        next_previous = torch.where(
            next_alive, next_previous, torch.zeros_like(next_previous)
        )
        return next_previous, next_alive, net_simple, turnover, executed

    return day


def _block_kernel_factory(
    day_kernel: Callable[..., tuple[torch.Tensor, ...]],
    block_rows: int,
) -> Callable[..., tuple[torch.Tensor, ...]]:
    def block(
        previous: torch.Tensor,
        alive: torch.Tensor,
        target: torch.Tensor,
        effective_simple: torch.Tensor,
        price_simple: torch.Tensor,
        effective_finite: torch.Tensor,
        price_finite: torch.Tensor,
        tradable: torch.Tensor,
        can_buy: torch.Tensor,
        can_sell: torch.Tensor,
        can_short: torch.Tensor,
        force_exit: torch.Tensor,
        volume: torch.Tensor,
        advance: torch.Tensor,
    ) -> tuple[torch.Tensor, ...]:
        returns: list[torch.Tensor] = []
        turnovers: list[torch.Tensor] = []
        weights: list[torch.Tensor] = []
        for row in range(block_rows):
            previous, alive, net, turnover, executed = day_kernel(
                previous,
                alive,
                target[row],
                effective_simple[row],
                price_simple[row],
                effective_finite[row],
                price_finite[row],
                tradable[row],
                can_buy[row],
                can_sell[row],
                can_short[row],
                force_exit[row],
                volume[row],
                advance[row],
            )
            returns.append(net)
            turnovers.append(turnover)
            weights.append(executed)
        return (
            previous,
            alive,
            torch.stack(returns),
            torch.stack(turnovers),
            torch.stack(weights),
        )

    return block


def _resolve_block_kernel(
    example: torch.Tensor,
    *,
    block_rows: int,
    buy_fee_rate: float,
    sell_fee_rate: float,
    long_only: bool,
    maximum_gross: float,
    max_turnover_ratio: float,
) -> tuple[
    Callable[..., tuple[torch.Tensor, ...]],
    Callable[..., tuple[torch.Tensor, ...]],
]:
    eager_day = _day_kernel_factory(
        buy_fee_rate=buy_fee_rate,
        sell_fee_rate=sell_fee_rate,
        long_only=long_only,
        maximum_gross=maximum_gross,
        max_turnover_ratio=max_turnover_ratio,
    )
    eager_block = _block_kernel_factory(eager_day, block_rows)
    compiler = getattr(torch, "compiler", None)
    is_compiling = getattr(compiler, "is_compiling", lambda: False)
    enabled = str(os.environ.get("STOCKAGENT_BACKTEST_COMPILE", "1")).lower() not in {
        "0",
        "false",
        "off",
        "no",
    }
    if (
        example.device.type != "cuda"
        or not enabled
        or not hasattr(torch, "compile")
        or bool(is_compiling())
    ):
        return eager_block, eager_day
    key = (
        "block",
        int(block_rows),
        str(example.device),
        str(example.dtype),
        int(example.numel()),
        float(buy_fee_rate),
        float(sell_fee_rate),
        bool(long_only),
        float(maximum_gross),
        float(max_turnover_ratio),
    )
    with _DAY_KERNEL_LOCK:
        cached = _DAY_KERNEL_CACHE.get(key)
        if cached is None:
            cached = torch.compile(
                eager_block,
                fullgraph=True,
                dynamic=False,
                options={"triton.cudagraphs": False},
            )
            _DAY_KERNEL_CACHE[key] = cached
    return cached, eager_day


def run_crypto_perpetual_torch(
    target_weights: torch.Tensor,
    funding_adjusted_log_returns: torch.Tensor,
    price_log_returns: torch.Tensor,
    tradable_mask: torch.Tensor,
    can_buy_mask: torch.Tensor,
    can_sell_mask: torch.Tensor,
    can_short_open_mask: torch.Tensor,
    force_exit_mask: torch.Tensor,
    *,
    buy_fee_rate: float,
    sell_fee_rate: float,
    long_only: bool,
    maximum_gross: float,
    max_turnover_ratio: float = 0.0,
    volume_limit_weights: torch.Tensor | None = None,
    state_advance_mask: torch.Tensor | None = None,
    initial_weights: torch.Tensor | None = None,
    initial_alive: torch.Tensor | None = None,
    return_weights_history: bool = True,
) -> CryptoPerpetualTensorResult:
    """Run a recurrent USDT-linear perpetual account.

    The funding-adjusted return controls NAV PnL, while the raw price return
    controls end-of-period risky notional.  Keeping those paths separate is the
    essential accounting identity: funding is a cash transfer, not a change in
    contract quantity or mark price.
    """

    if target_weights.dim() != 2:
        raise ValueError("crypto_perpetual target_weights must have shape [T,S]")
    shape = tuple(target_weights.shape)
    for name, value in (
        ("funding_adjusted_log_returns", funding_adjusted_log_returns),
        ("price_log_returns", price_log_returns),
        ("tradable_mask", tradable_mask),
        ("can_buy_mask", can_buy_mask),
        ("can_sell_mask", can_sell_mask),
        ("can_short_open_mask", can_short_open_mask),
        ("force_exit_mask", force_exit_mask),
    ):
        if tuple(value.shape) != shape:
            raise ValueError(f"{name} must match target_weights [T,S]")
    if buy_fee_rate < 0.0 or sell_fee_rate < 0.0:
        raise ValueError("crypto perpetual fee rates must be non-negative")
    if maximum_gross < 0.0 or maximum_gross > 1.0:
        raise ValueError("maximum_gross must be within [0,1]")

    target = torch.nan_to_num(
        target_weights.to(dtype=torch.float32), nan=0.0, posinf=0.0, neginf=0.0
    )
    device = target.device
    effective_log = funding_adjusted_log_returns.to(device=device, dtype=torch.float32)
    price_log = price_log_returns.to(device=device, dtype=torch.float32)
    effective_finite = torch.isfinite(effective_log)
    price_finite = torch.isfinite(price_log)
    effective_simple = torch.expm1(
        torch.where(effective_finite, effective_log, torch.zeros_like(effective_log))
    )
    price_simple = torch.expm1(
        torch.where(price_finite, price_log, torch.zeros_like(price_log))
    )
    tradable = tradable_mask.to(device=device, dtype=torch.bool)
    can_buy = can_buy_mask.to(device=device, dtype=torch.bool) & tradable
    can_sell = can_sell_mask.to(device=device, dtype=torch.bool) & tradable
    can_short = can_short_open_mask.to(device=device, dtype=torch.bool) & can_sell
    force_exit = force_exit_mask.to(device=device, dtype=torch.bool)
    advance = (
        torch.ones((shape[0],), device=device, dtype=torch.bool)
        if state_advance_mask is None
        else state_advance_mask.to(device=device, dtype=torch.bool)
    )
    if tuple(advance.shape) != (shape[0],):
        raise ValueError("state_advance_mask must have shape [T]")
    volume = (
        torch.full_like(target, float("inf"))
        if volume_limit_weights is None
        else volume_limit_weights.to(device=device, dtype=torch.float32)
    )
    if tuple(volume.shape) != shape:
        raise ValueError("volume_limit_weights must match target_weights [T,S]")
    volume = torch.nan_to_num(
        volume,
        nan=0.0,
        posinf=torch.finfo(torch.float32).max,
        neginf=0.0,
    ).clamp_min(0.0)

    previous = (
        torch.zeros((shape[1],), device=device, dtype=torch.float32)
        if initial_weights is None
        else torch.nan_to_num(
            initial_weights.to(device=device, dtype=torch.float32),
            nan=0.0,
            posinf=0.0,
            neginf=0.0,
        )
    )
    alive = (
        torch.ones((), device=device, dtype=torch.bool)
        if initial_alive is None
        else initial_alive.to(device=device, dtype=torch.bool).reshape(())
    )
    weights_rows: list[torch.Tensor] = []
    returns_rows: list[torch.Tensor] = []
    turnover_rows: list[torch.Tensor] = []
    block_rows = 4
    block_kernel, eager_day_kernel = _resolve_block_kernel(
        previous,
        block_rows=block_rows,
        buy_fee_rate=buy_fee_rate,
        sell_fee_rate=sell_fee_rate,
        long_only=long_only,
        maximum_gross=maximum_gross,
        max_turnover_ratio=max_turnover_ratio,
    )

    for start in range(0, shape[0], block_rows):
        end = min(start + block_rows, shape[0])
        if end - start == block_rows:
            previous, alive, net_block, turnover_block, executed_block = block_kernel(
                previous,
                alive,
                target[start:end],
                effective_simple[start:end],
                price_simple[start:end],
                effective_finite[start:end],
                price_finite[start:end],
                tradable[start:end],
                can_buy[start:end],
                can_sell[start:end],
                can_short[start:end],
                force_exit[start:end],
                volume[start:end],
                advance[start:end],
            )
            returns_rows.extend(net_block.unbind(0))
            turnover_rows.extend(turnover_block.unbind(0))
            if return_weights_history:
                weights_rows.extend(executed_block.unbind(0))
            continue
        for row in range(start, end):
            previous, alive, net_simple, turnover, executed = eager_day_kernel(
                previous,
                alive,
                target[row],
                effective_simple[row],
                price_simple[row],
                effective_finite[row],
                price_finite[row],
                tradable[row],
                can_buy[row],
                can_sell[row],
                can_short[row],
                force_exit[row],
                volume[row],
                advance[row],
            )
            if return_weights_history:
                weights_rows.append(executed)
            returns_rows.append(net_simple)
            turnover_rows.append(turnover)

    empty_scalar = target.new_empty((0,))
    strategy = torch.stack(returns_rows) if returns_rows else empty_scalar
    turnovers = torch.stack(turnover_rows) if turnover_rows else empty_scalar
    history = (
        torch.stack(weights_rows) if weights_rows else target.new_empty((0, shape[1]))
    )
    return CryptoPerpetualTensorResult(
        strategy_simple_returns=strategy,
        turnovers=turnovers,
        executed_weights=history,
        final_weights=previous,
        final_alive=alive,
    )


__all__ = ["CryptoPerpetualTensorResult", "run_crypto_perpetual_torch"]
