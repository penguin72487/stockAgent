"""Funding-aware target-weight ledger for linear crypto perpetual contracts."""

from __future__ import annotations

from dataclasses import dataclass
import json
import math
import os
import threading
from typing import Any, Callable, Mapping

import torch

from stockagent.backtest.portfolio_allocator import (
    stateful_proximal_target_weights,
)


# v4 carries live NAV for absolute capacity, never bypasses execution constraints
# to enforce gross, and rejects unknown held valuations instead of fabricating
# either a pre-gap liquidation (v3) or an economic default.
# v5 interprets a crypto force-exit flag as a dated, persistent zero-target
# request: side permissions and capacity still own every fill. It is not an
# exchange cash-settlement event or permission to erase residual inventory.
CRYPTO_PERPETUAL_BACKTEST_CONTRACT_VERSION = 5
# Opt-in research contract: only a dated announcement's zero-target reduction
# may ignore the volume cap. Source side permissions, turnover and fees remain.
CRYPTO_PERPETUAL_ANNOUNCED_EXIT_BACKTEST_CONTRACT_VERSION = 6
# Backward v1 preserves the exact continuous capacity map at a zero trade.
# This is a derivative correction, not a straight-through execution surrogate;
# optimizer/checkpoint fingerprints must distinguish the earlier sign/abs map.
CRYPTO_PERPETUAL_BACKWARD_CONTRACT_VERSION = 1
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
    equity_scale_history: torch.Tensor
    final_equity_scale: torch.Tensor


class CryptoPerpetualDataError(RuntimeError):
    """A held contract cannot be valued; this is not an economic loss."""

    def __init__(self, message: str, *, evidence: Mapping[str, Any] | None = None):
        super().__init__(message)
        self.message = message
        self.evidence = dict(evidence or {})

    def __str__(self) -> str:
        if not self.evidence:
            return self.message
        return f"{self.message}; evidence={json.dumps(self.evidence, sort_keys=True)}"


def _capacity_clamp(
    delta: torch.Tensor,
    capacity: torch.Tensor,
    can_increase: torch.Tensor,
    can_decrease: torch.Tensor,
) -> torch.Tensor:
    """Clip an already side-legal order without losing the derivative at zero."""

    # sign(delta) * min(abs(delta), capacity) has the same values as a
    # signed clamp, but its autograd derivative incorrectly vanishes at
    # delta == 0 even inside a positive-capacity interval. Explicit signed
    # bounds retain the true local derivative, including a usable entry
    # gradient from cash. At a one-sided boundary the map is nonsmooth;
    # clamp chooses the permitted-side derivative (a valid subgradient).
    lower = torch.where(can_decrease, -capacity, 0.0)
    upper = torch.where(can_increase, capacity, 0.0)
    return torch.where(
        lower < upper,
        torch.clamp(delta, min=lower, max=upper) + 0.0,
        # A collapsed interval is constant, not clamp's boundary slope 1.
        # Preserve the earlier map's signed zero for a capped nonzero order.
        delta.sign() * torch.zeros_like(delta),
    )


def _gross_cap(
    previous: torch.Tensor,
    proposed: torch.Tensor,
    maximum_gross: float,
    *,
    enforce_reduction: bool = True,
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
    if enforce_reduction:
        return torch.where(cap_breached_after_reductions, reduced_at_cap, normal)
    # After legality/capacity checks, the retained book cannot be sold again
    # without a fill. A blocked breach permits reductions but no expansion.
    return normal


def _day_kernel_factory(
    *,
    buy_fee_rate: float,
    sell_fee_rate: float,
    long_only: bool,
    maximum_gross: float,
    max_turnover_ratio: float,
    stateful_proximal_allocator: bool,
    proximal_cost_multiplier: float,
    announced_exit_unlimited_volume: bool = False,
) -> Callable[..., tuple[torch.Tensor, ...]]:
    def day(
        previous: torch.Tensor,
        alive: torch.Tensor,
        equity_scale: torch.Tensor,
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
        incoming_previous = previous
        previous = torch.where(alive, previous, torch.zeros_like(previous))
        exit_requested = force_exit & row_advances & alive
        base = previous
        requested = torch.where(
            alive & row_advances & tradable,
            target,
            base,
        )
        if long_only:
            requested = requested.clamp_min(0.0)
        if stateful_proximal_allocator:
            requested = stateful_proximal_target_weights(
                requested,
                base,
                buy_fee_rates=buy_fee_rate,
                sell_fee_rates=sell_fee_rate,
                cost_multiplier=proximal_cost_multiplier,
                long_only=long_only,
            )
        # A known announcement requests flat even after policy eligibility is
        # disabled. Do not let the optional proximal dead-zone retain a small
        # position forever, but keep this request upstream of every fill gate.
        requested = torch.where(exit_requested, torch.zeros_like(requested), requested)
        # Risk reductions are orders too: apply the desired gross endpoint
        # before side permissions and volume, never fabricate fills afterwards.
        requested = _gross_cap(base, requested, maximum_gross)
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
        unlimited_volume = torch.isposinf(volume)
        if announced_exit_unlimited_volume:
            # exit_requested already requires alive + an advancing row and
            # already replaced the model target with zero. It cannot expand
            # or reverse inventory; side gates above and turnover below stay.
            unlimited_volume = unlimited_volume | exit_requested
        finite_volume = torch.where(unlimited_volume, 0.0, volume)
        live_volume = (
            finite_volume.to(equity_scale.dtype)
            / equity_scale.clamp_min(torch.finfo(equity_scale.dtype).tiny)
        ).to(delta.dtype)
        # Do not divide infinity through the recurrent graph (0 * inf in its
        # inactive backward branch can poison gradients). No configured cap
        # remains genuinely unlimited even after large NAV growth.
        live_volume = torch.where(unlimited_volume, float("inf"), live_volume)
        can_decrease = torch.where(
            base > 0.0, can_sell, can_short & (not long_only)
        )
        delta = _capacity_clamp(delta, live_volume, can_buy, can_decrease)
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
        executed = _gross_cap(base, base + delta, maximum_gross, enforce_reduction=False)
        delta = executed - base

        buy_turnover = delta.clamp_min(0.0).sum()
        sell_turnover = (-delta).clamp_min(0.0).sum()
        active = alive & row_advances & (executed != 0.0)
        invalid_valuation = active & ~(effective_finite & price_finite)
        effective_pnl = (executed * effective_simple).sum()
        net_simple = (
            effective_pnl
            - float(buy_fee_rate) * buy_turnover
            - float(sell_fee_rate) * sell_turnover
        )
        survived_on_advance = (
            torch.isfinite(net_simple)
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
        # Padding is not a market session. Gating only target/mark drift is not
        # sufficient: finite duplicated labels still create PnL and gross-cap
        # enforcement can manufacture deleveraging. Freeze every output here.
        next_previous = torch.where(row_advances, next_previous, incoming_previous)
        next_alive = torch.where(row_advances, next_alive, alive)
        net_simple = torch.where(row_advances, net_simple, torch.zeros_like(net_simple))
        turnover = torch.where(row_advances, turnover, torch.zeros_like(turnover))
        executed = torch.where(row_advances, executed, incoming_previous)
        next_equity_scale = equity_scale * (1.0 + net_simple.to(equity_scale.dtype))
        return next_previous, next_alive, next_equity_scale, net_simple, turnover, executed, invalid_valuation

    return day


def _block_kernel_factory(
    day_kernel: Callable[..., tuple[torch.Tensor, ...]],
    block_rows: int,
) -> Callable[..., tuple[torch.Tensor, ...]]:
    def block(
        previous: torch.Tensor,
        alive: torch.Tensor,
        equity_scale: torch.Tensor,
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
        scales: list[torch.Tensor] = []
        invalid_rows: list[torch.Tensor] = []
        for row in range(block_rows):
            previous, alive, equity_scale, net, turnover, executed, invalid = day_kernel(
                previous,
                alive,
                equity_scale,
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
            scales.append(equity_scale)
            invalid_rows.append(invalid)
        return (
            previous,
            alive,
            equity_scale,
            torch.stack(returns),
            torch.stack(turnovers),
            torch.stack(weights),
            torch.stack(scales),
            torch.stack(invalid_rows),
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
    stateful_proximal_allocator: bool,
    proximal_cost_multiplier: float,
    announced_exit_unlimited_volume: bool = False,
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
        stateful_proximal_allocator=stateful_proximal_allocator,
        proximal_cost_multiplier=proximal_cost_multiplier,
        announced_exit_unlimited_volume=announced_exit_unlimited_volume,
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
        (CRYPTO_PERPETUAL_ANNOUNCED_EXIT_BACKTEST_CONTRACT_VERSION
         if announced_exit_unlimited_volume else CRYPTO_PERPETUAL_BACKTEST_CONTRACT_VERSION),
        CRYPTO_PERPETUAL_BACKWARD_CONTRACT_VERSION,
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
        bool(stateful_proximal_allocator),
        float(proximal_cost_multiplier),
        bool(announced_exit_unlimited_volume),
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
    stateful_proximal_allocator: bool = False,
    proximal_cost_multiplier: float = 1.0,
    announced_exit_unlimited_volume: bool = False,
    volume_limit_weights: torch.Tensor | None = None,
    state_advance_mask: torch.Tensor | None = None,
    initial_weights: torch.Tensor | None = None,
    initial_alive: torch.Tensor | None = None,
    initial_equity_scale: torch.Tensor | None = None,
    return_weights_history: bool = True,
) -> CryptoPerpetualTensorResult:
    """Run a recurrent USDT-linear perpetual account.

    The funding-adjusted return controls NAV PnL, while the raw price return
    controls end-of-period risky notional.  Keeping those paths separate is the
    essential accounting identity: funding is a cash transfer, not a change in
    contract quantity or mark price. ``volume_limit_weights`` is denominated
    in initial reference capital; it is divided by recurrent live NAV here.
    ``force_exit_mask`` requests a zero target for an already-known dated
    lifecycle event, subject to the original source-backed side permissions,
    volume and turnover limits. Keep it set on later rows to retry a residual;
    it never manufactures a fill, skips valuation, or encodes future missingness.
    The explicit ``announced_exit_unlimited_volume`` research option exempts
    only that reduction from volume limits, not side permissions or turnover.
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
    if not torch.compiler.is_compiling() and (
        not isinstance(stateful_proximal_allocator, bool)
    ):
        raise TypeError("stateful_proximal_allocator must be bool")
    if not torch.compiler.is_compiling() and not isinstance(announced_exit_unlimited_volume, bool):
        raise TypeError("announced_exit_unlimited_volume must be bool")

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
    effective_finite = effective_finite & torch.isfinite(effective_simple)
    price_finite = price_finite & torch.isfinite(price_simple)
    effective_simple = torch.where(effective_finite, effective_simple, 0.0)
    price_simple = torch.where(price_finite, price_simple, 0.0)
    tradable = tradable_mask.to(device=device, dtype=torch.bool)
    force_exit = force_exit_mask.to(device=device, dtype=torch.bool)
    # Only an explicit reduction request may use executable source side masks
    # outside the model's eligible universe. False side evidence stays false.
    order_eligible = tradable | force_exit
    can_buy = can_buy_mask.to(device=device, dtype=torch.bool) & order_eligible
    can_sell = can_sell_mask.to(device=device, dtype=torch.bool) & order_eligible
    can_short = can_short_open_mask.to(device=device, dtype=torch.bool) & can_sell
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
        posinf=float("inf"),
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
    equity_scale = (
        torch.ones((), device=device, dtype=torch.float64)
        if initial_equity_scale is None
        else initial_equity_scale.to(device=device, dtype=torch.float64).reshape(())
    )
    if not bool(torch.isfinite(equity_scale) & (equity_scale > 0.0)):
        raise ValueError("crypto initial_equity_scale must be finite and positive")
    weights_rows: list[torch.Tensor] = []
    returns_rows: list[torch.Tensor] = []
    turnover_rows: list[torch.Tensor] = []
    equity_rows: list[torch.Tensor] = []
    invalid_rows: list[torch.Tensor] = []
    block_rows = 4
    block_kernel, eager_day_kernel = _resolve_block_kernel(
        previous,
        block_rows=block_rows,
        buy_fee_rate=buy_fee_rate,
        sell_fee_rate=sell_fee_rate,
        long_only=long_only,
        maximum_gross=maximum_gross,
        max_turnover_ratio=max_turnover_ratio,
        stateful_proximal_allocator=stateful_proximal_allocator,
        proximal_cost_multiplier=proximal_cost_multiplier,
        announced_exit_unlimited_volume=announced_exit_unlimited_volume,
    )

    for start in range(0, shape[0], block_rows):
        end = min(start + block_rows, shape[0])
        if end - start == block_rows:
            previous, alive, equity_scale, net_block, turnover_block, executed_block, scales, invalid = block_kernel(
                previous,
                alive,
                equity_scale,
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
            equity_rows.extend(scales.unbind(0))
            invalid_rows.extend(invalid.unbind(0))
            if return_weights_history:
                weights_rows.extend(executed_block.unbind(0))
            continue
        for row in range(start, end):
            previous, alive, equity_scale, net_simple, turnover, executed, invalid = eager_day_kernel(
                previous,
                alive,
                equity_scale,
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
            equity_rows.append(equity_scale)
            invalid_rows.append(invalid)

    # Host-side boundary check, outside compiled kernels and before loss or
    # backward. Never poison a CUDA context with a device assertion, or turn
    # absent observations into a profitable exit / a ruin-clamped loss.
    if invalid_rows:
        invalid = torch.stack(invalid_rows)
        if bool(invalid.any()):
            row, symbol = invalid.nonzero()[0].tolist()
            # Failure-only scalar transfers. Do not retain extra portfolio
            # histories or synchronize successful rows for diagnostics.
            incoming_scale = (
                equity_rows[row - 1] if row > 0
                else initial_equity_scale if initial_equity_scale is not None
                else target.new_tensor(1.0, dtype=torch.float64)
            ).detach().reshape(()).to(device=target.device, dtype=torch.float64)
            capacity_reference = volume[row, symbol].detach()
            capacity_live = (
                capacity_reference.to(dtype=torch.float64)
                / incoming_scale.clamp_min(torch.finfo(torch.float64).tiny)
            ).to(dtype=torch.float32)
            reference_value, live_value, scale_value = (
                float(capacity_reference), float(capacity_live), float(incoming_scale)
            )
            evidence = {
                "row": row,
                "symbol_index": symbol,
                "price_valuation_available": bool(price_finite[row, symbol]),
                "funding_adjusted_valuation_available": bool(effective_finite[row, symbol]),
                "valuation_availability_note": "funding-adjusted label combines price and funding; raw funding availability is not passed to this ledger",
                "tradable": bool(tradable[row, symbol]),
                "force_exit": bool(force_exit[row, symbol]),
                "announced_exit_volume_exempt": bool(
                    announced_exit_unlimited_volume and force_exit[row, symbol] and advance[row]
                ),
                "can_buy_source": bool(can_buy_mask[row, symbol]),
                "can_sell_source": bool(can_sell_mask[row, symbol]),
                "can_short_open_source": bool(can_short_open_mask[row, symbol]),
                "can_buy_effective": bool(can_buy[row, symbol]),
                "can_sell_effective": bool(can_sell[row, symbol]),
                "can_short_open_effective": bool(can_short[row, symbol]),
                "target_weight": float(target[row, symbol].detach()),
                "volume_capacity_weight_reference": reference_value if math.isfinite(reference_value) else None,
                "volume_capacity_weight_live": live_value if math.isfinite(live_value) else None,
                "volume_capacity_unlimited": math.isinf(reference_value) and reference_value > 0.0,
                "incoming_equity_scale": scale_value if math.isfinite(scale_value) else None,
                "remaining_executed_weight": (
                    float(weights_rows[row][symbol].detach())
                    if return_weights_history else None
                ),
                "remaining_executed_weight_status": (
                    "retained_execution_history" if return_weights_history else "history_not_requested"
                ),
            }
            raise CryptoPerpetualDataError(
                f"crypto held valuation unavailable: row={row}, symbol_index={symbol}; "
                "repair price/funding or provide dated settlement evidence; "
                "missing future data does not authorize a prior-mark liquidation",
                evidence=evidence,
            )

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
        equity_scale_history=(torch.stack(equity_rows) if equity_rows else equity_scale.new_empty((0,))),
        final_equity_scale=equity_scale,
    )


__all__ = ["CryptoPerpetualDataError", "CryptoPerpetualTensorResult", "run_crypto_perpetual_torch"]
