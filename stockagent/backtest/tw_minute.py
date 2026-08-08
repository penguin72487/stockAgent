from __future__ import annotations

from dataclasses import dataclass
import math

import torch


@dataclass(frozen=True, slots=True)
class MinuteExecutionConfig:
    initial_equity: float
    gross_exposure: float
    maximum_name_weight: float | None
    maximum_volume_participation: float
    maximum_order_notional: float | None
    slippage_bps_per_side: float
    outside_cash_logit: float | None = None
    long_only: bool = True

    def __post_init__(self) -> None:
        positive = {"initial_equity": self.initial_equity}
        for name, value in positive.items():
            if not math.isfinite(float(value)) or float(value) <= 0.0:
                raise ValueError(f"{name} must be positive and finite")
        if self.maximum_order_notional is not None and (
            not math.isfinite(float(self.maximum_order_notional))
            or float(self.maximum_order_notional) <= 0.0
        ):
            raise ValueError(
                "maximum_order_notional must be positive and finite when enabled"
            )
        fractions = {
            "gross_exposure": self.gross_exposure,
            "maximum_volume_participation": self.maximum_volume_participation,
        }
        for name, value in fractions.items():
            if not math.isfinite(float(value)) or not 0.0 < float(value) <= 1.0:
                raise ValueError(f"{name} must be in (0, 1]")
        if self.maximum_name_weight is not None and (
            not math.isfinite(float(self.maximum_name_weight))
            or not 0.0 < float(self.maximum_name_weight) <= 1.0
        ):
            raise ValueError(
                "maximum_name_weight must be in (0, 1] when enabled"
            )
        if (
            not math.isfinite(float(self.slippage_bps_per_side))
            or float(self.slippage_bps_per_side) < 0.0
        ):
            raise ValueError("slippage_bps_per_side must be finite and nonnegative")
        if self.outside_cash_logit is not None and not math.isfinite(
            float(self.outside_cash_logit)
        ):
            raise ValueError("outside_cash_logit must be finite when enabled")


@dataclass(slots=True)
class MinuteExecutionState:
    """Differentiable minute ledger in initial-equity-normalized units.

    ``cash`` and ``equity`` are NAV ratios whose initial value is one.
    ``shares`` stores actual shares divided by ``config.initial_equity`` so
    price times shares is also a NAV ratio. Public turnover, fee, and slippage
    results remain denominated in TWD. This keeps the CUDA hot path in FP32
    without subtracting two approximately NT$10m values to recover a tiny P&L.
    """

    cash: torch.Tensor
    shares: torch.Tensor
    last_prices: torch.Tensor
    equity: torch.Tensor

    def detached(self, *, clone: bool = True) -> "MinuteExecutionState":
        def _detach(value: torch.Tensor) -> torch.Tensor:
            detached = value.detach()
            return detached.clone() if clone else detached

        return MinuteExecutionState(
            cash=_detach(self.cash),
            shares=_detach(self.shares),
            last_prices=_detach(self.last_prices),
            equity=_detach(self.equity),
        )


@dataclass(frozen=True, slots=True)
class MinuteStepResult:
    state: MinuteExecutionState
    net_return: torch.Tensor
    turnover_notional: torch.Tensor
    explicit_fees: torch.Tensor
    slippage_cost: torch.Tensor


@dataclass(frozen=True, slots=True)
class MinuteCloseResult:
    state: MinuteExecutionState
    net_return: torch.Tensor
    turnover_notional: torch.Tensor
    explicit_fees: torch.Tensor
    slippage_cost: torch.Tensor
    forced_exit_over_capacity_notional: torch.Tensor


def initialize_minute_execution_state(
    *,
    num_symbols: int,
    config: MinuteExecutionConfig,
    device: torch.device,
    dtype: torch.dtype = torch.float32,
    batch_size: int | None = None,
) -> MinuteExecutionState:
    if int(num_symbols) <= 0:
        raise ValueError("num_symbols must be positive")
    if batch_size is not None and int(batch_size) <= 0:
        raise ValueError("batch_size must be positive when provided")
    batch_shape = () if batch_size is None else (int(batch_size),)
    cash = torch.full(
        batch_shape,
        1.0,
        device=device,
        dtype=dtype,
    )
    return MinuteExecutionState(
        cash=cash,
        shares=torch.zeros(
            (*batch_shape, int(num_symbols)), device=device, dtype=dtype
        ),
        last_prices=torch.zeros(
            (*batch_shape, int(num_symbols)), device=device, dtype=dtype
        ),
        equity=cash.clone(),
    )


def minute_target_weights_from_logits(
    logits: torch.Tensor,
    tradable_mask: torch.Tensor,
    *,
    gross_exposure: float,
    maximum_name_weight: float | None,
    outside_cash_logit: float | None = None,
    long_only: bool = True,
    shortable_mask: torch.Tensor | None = None,
) -> torch.Tensor:
    if logits.ndim != 2 or tradable_mask.shape != logits.shape:
        raise ValueError("minute logits and tradable mask must both have shape [B,S]")
    mask = tradable_mask.to(dtype=torch.bool, device=logits.device)
    if shortable_mask is not None and shortable_mask.shape != logits.shape:
        raise ValueError("minute shortable mask must have shape [B,S]")
    float_logits = logits.float()
    # Reuse one integer reduction for both predicates and normalization.
    # Inductor can lower a boolean ``any`` reduction to an int8 Triton value
    # and then pass it directly to tl.where, which is deprecated in Triton.
    valid_count_int = mask.sum(dim=1, keepdim=True)
    any_valid = valid_count_int > 0
    if not bool(long_only):
        valid_count = valid_count_int.clamp_min(1).float()
        raw_scores = torch.where(mask, float_logits, torch.zeros_like(float_logits))
        shortable = (
            mask
            if shortable_mask is None
            else shortable_mask.to(device=logits.device, dtype=torch.bool) & mask
        )
        signed_scores = torch.where(
            (raw_scores >= 0.0) | shortable,
            raw_scores,
            torch.zeros_like(raw_scores),
        )
        l1 = signed_scores.abs().sum(dim=1, keepdim=True)
        weights = torch.where(
            l1 > 0.0,
            signed_scores / torch.clamp_min(l1, torch.finfo(torch.float32).tiny),
            torch.zeros_like(signed_scores),
        )
        if outside_cash_logit is None:
            risky_exposure = torch.full_like(
                any_valid,
                float(gross_exposure),
                dtype=torch.float32,
            )
        else:
            # Optional legacy cash gate. The active day-trade-equivalent
            # minute contract disables it and uses raw signed-score L1.
            magnitude = signed_scores.abs()
            row_max = magnitude.max(dim=1, keepdim=True).values
            magnitude_exp = torch.exp(magnitude - row_max).masked_fill(~mask, 0.0)
            log_mean_exp = row_max + torch.log(
                magnitude_exp.sum(dim=1, keepdim=True).clamp_min(
                    torch.finfo(torch.float32).tiny
                )
                / valid_count
            )
            risky_exposure = float(gross_exposure) * torch.sigmoid(
                log_mean_exp - float(outside_cash_logit)
            )
        weights = weights * torch.where(
            any_valid,
            risky_exposure,
            torch.zeros_like(risky_exposure),
        )
        if maximum_name_weight is not None:
            weights = torch.clamp(
                weights,
                min=-float(maximum_name_weight),
                max=float(maximum_name_weight),
            )
        return torch.where(any_valid, weights, torch.zeros_like(weights))
    safe_logits = float_logits.masked_fill(~mask, torch.finfo(torch.float32).min)
    safe_logits = torch.where(any_valid, safe_logits, torch.zeros_like(safe_logits))
    if outside_cash_logit is None:
        weights = torch.softmax(safe_logits, dim=1)
        weights = weights.masked_fill(~mask, 0.0)
        risky_exposure = torch.full_like(
            any_valid,
            float(gross_exposure),
            dtype=torch.float32,
        )
    else:
        # A raw cash class in softmax would become negligible as the symbol
        # universe grows. Compare cash against log-mean-exp stock evidence so
        # identical logits imply identical risky exposure for any universe S.
        # Reuse the same stable exponentials for allocation and log-mean-exp;
        # separate softmax + logsumexp would repeat both wide reductions.
        valid_count = valid_count_int.clamp_min(1).float()
        row_max = safe_logits.max(dim=1, keepdim=True).values
        shifted_exp = torch.exp(safe_logits - row_max).masked_fill(~mask, 0.0)
        exp_sum = shifted_exp.sum(dim=1, keepdim=True).clamp_min(
            torch.finfo(torch.float32).tiny
        )
        weights = shifted_exp / exp_sum
        log_mean_exp = row_max + torch.log(exp_sum / valid_count)
        risky_exposure = float(gross_exposure) * torch.sigmoid(
            log_mean_exp - float(outside_cash_logit)
        )
        risky_exposure = torch.where(
            any_valid,
            risky_exposure,
            torch.zeros_like(risky_exposure),
        )
    weights = weights * risky_exposure
    # Clamping leaves unused exposure as cash. Renormalizing here would violate
    # the configured per-name ceiling for a concentrated cross-section.
    if maximum_name_weight is not None:
        weights = torch.clamp(weights, min=0.0, max=float(maximum_name_weight))
    return torch.where(any_valid, weights, torch.zeros_like(weights))


def execute_minute_target(
    state: MinuteExecutionState,
    *,
    target_weights: torch.Tensor,
    execution_mask: torch.Tensor,
    execution_open: torch.Tensor,
    exit_close: torch.Tensor,
    future_volume_shares: torch.Tensor,
    buy_fee_rates: torch.Tensor,
    sell_fee_rates: torch.Tensor,
    config: MinuteExecutionConfig,
    allow_new_entries: bool | torch.Tensor,
    short_open_mask: torch.Tensor | None = None,
    short_capacity_shares: torch.Tensor | None = None,
) -> MinuteStepResult:
    """Execute one completed-bar decision at the next bar's open proxy."""

    if state.shares.ndim < 1:
        raise ValueError("minute executor shares must end in a symbol axis")
    symbols = int(state.shares.size(-1))
    batch_shape = tuple(state.shares.shape[:-1])
    if tuple(state.cash.shape) != batch_shape or tuple(state.equity.shape) != batch_shape:
        raise ValueError("minute executor cash/equity batch axes are inconsistent")
    if tuple(state.last_prices.shape) != tuple(state.shares.shape):
        raise ValueError("minute executor last_prices must match shares")
    aligned = (
        target_weights,
        execution_mask,
        execution_open,
        exit_close,
        future_volume_shares,
    )
    if any(tuple(value.shape) != tuple(state.shares.shape) for value in aligned):
        raise ValueError(
            "minute executor market inputs must match shares on [...,S]"
        )
    for rates in (buy_fee_rates, sell_fee_rates):
        if tuple(rates.shape) not in {(symbols,), tuple(state.shares.shape)}:
            raise ValueError("minute fee rates must have shape [S] or [...,S]")
    mask = execution_mask.to(device=state.shares.device, dtype=torch.bool)
    opens = execution_open.float().to(state.shares.device)
    closes = exit_close.float().to(state.shares.device)
    volumes = future_volume_shares.float().to(state.shares.device)
    valid = (
        mask
        & torch.isfinite(opens)
        & torch.isfinite(closes)
        & torch.isfinite(volumes)
        & (opens > 0.0)
        & (closes > 0.0)
        & (volumes > 0.0)
    )
    valuation_open = torch.where(valid, opens, state.last_prices)
    mark_to_open_pnl = torch.sum(
        state.shares * (valuation_open - state.last_prices), dim=-1
    )
    equity_open = state.equity + mark_to_open_pnl
    # Avoid a device synchronization on every minute. CPU oracle/tests still
    # fail at the exact event; CUDA training is guarded by the finite log-loss
    # check before every backward pass.
    if state.shares.device.type == "cpu" and (
        not bool(torch.all(torch.isfinite(equity_open)).item())
        or bool(torch.any(equity_open.detach() <= 0.0).item())
    ):
        raise RuntimeError("minute account equity became nonpositive before execution")

    clean_target = torch.where(
        valid,
        torch.nan_to_num(target_weights.float(), nan=0.0, posinf=0.0, neginf=0.0),
        torch.zeros_like(target_weights, dtype=torch.float32),
    )
    # State shares are normalized by initial equity. Target weights therefore
    # map directly from normalized NAV to normalized shares.
    target_shares = (
        clean_target
        * equity_open.unsqueeze(-1)
        / torch.clamp_min(opens, 1e-12)
    )
    if bool(config.long_only):
        target_shares = torch.clamp_min(target_shares, 0.0)
    else:
        if short_open_mask is None or short_capacity_shares is None:
            shortable = torch.zeros_like(valid)
            short_capacity = torch.zeros_like(opens)
        else:
            if tuple(short_open_mask.shape) != tuple(state.shares.shape):
                raise ValueError("minute short-open mask must match shares")
            if tuple(short_capacity_shares.shape) != tuple(state.shares.shape):
                raise ValueError("minute short capacity must match shares")
            shortable = (
                short_open_mask.to(device=state.shares.device, dtype=torch.bool)
                & valid
            )
            short_capacity = torch.nan_to_num(
                short_capacity_shares.float().to(state.shares.device),
                nan=0.0,
                posinf=0.0,
                neginf=0.0,
            ).clamp_min(0.0) / float(config.initial_equity)
        target_shares = torch.where(
            shortable,
            torch.maximum(target_shares, -short_capacity),
            torch.clamp_min(target_shares, 0.0),
        )
    if isinstance(allow_new_entries, bool):
        if not allow_new_entries:
            target_shares = torch.where(
                state.shares > 0.0,
                torch.minimum(torch.clamp_min(target_shares, 0.0), state.shares),
                torch.where(
                    state.shares < 0.0,
                    torch.maximum(torch.clamp_max(target_shares, 0.0), state.shares),
                    torch.zeros_like(target_shares),
                ),
            )
    else:
        entry_flag = allow_new_entries.to(
            device=state.shares.device, dtype=torch.bool
        )
        reduce_only = torch.where(
            state.shares > 0.0,
            torch.minimum(torch.clamp_min(target_shares, 0.0), state.shares),
            torch.where(
                state.shares < 0.0,
                torch.maximum(torch.clamp_max(target_shares, 0.0), state.shares),
                torch.zeros_like(target_shares),
            ),
        )
        target_shares = torch.where(entry_flag.unsqueeze(-1), target_shares, reduce_only)
    delta = target_shares - state.shares
    volume_capacity = torch.where(
        valid,
        volumes
        * (float(config.maximum_volume_participation) / float(config.initial_equity)),
        torch.zeros_like(volumes),
    )
    capacity = volume_capacity
    if config.maximum_order_notional is not None:
        notional_capacity = torch.where(
            valid,
            (
                float(config.maximum_order_notional)
                / float(config.initial_equity)
            )
            / torch.clamp_min(opens, 1e-12),
            torch.zeros_like(opens),
        )
        capacity = torch.minimum(capacity, notional_capacity)
    slippage = float(config.slippage_bps_per_side) / 10_000.0
    buy_rates = buy_fee_rates.float().to(state.shares.device)
    sell_rates = sell_fee_rates.float().to(state.shares.device)

    sell_quantity = torch.minimum(torch.clamp_min(-delta, 0.0), capacity)
    if bool(config.long_only):
        sell_quantity = torch.minimum(sell_quantity, state.shares)
    sell_execution = opens * (1.0 - slippage)
    sell_fees = sell_quantity * sell_execution * sell_rates
    cash_after_sell = state.cash + torch.sum(
        sell_quantity * sell_execution - sell_fees, dim=-1
    )
    shares_after_sell = state.shares - sell_quantity

    buy_quantity = torch.minimum(torch.clamp_min(delta, 0.0), capacity)
    buy_execution = opens * (1.0 + slippage)
    buy_unit_cost = buy_execution * (1.0 + buy_rates)
    requested_buy_cost = torch.sum(buy_quantity * buy_unit_cost, dim=-1)
    affordable_scale = torch.where(
        requested_buy_cost > 0.0,
        torch.clamp(cash_after_sell / torch.clamp_min(requested_buy_cost, 1e-12), 0.0, 1.0),
        torch.zeros_like(requested_buy_cost),
    )
    buy_quantity = buy_quantity * affordable_scale.unsqueeze(-1)
    buy_fees = buy_quantity * buy_execution * buy_rates
    cash = cash_after_sell - torch.sum(
        buy_quantity * buy_execution + buy_fees, dim=-1
    )
    shares = shares_after_sell + buy_quantity
    last_prices = torch.where(valid, closes, state.last_prices)
    normalized_pnl = torch.sum(
        state.shares * (last_prices - state.last_prices)
        + sell_quantity * (sell_execution - last_prices)
        + buy_quantity * (last_prices - buy_execution)
        - sell_fees
        - buy_fees,
        dim=-1,
    )
    equity = state.equity + normalized_pnl
    net_return = normalized_pnl / torch.clamp_min(state.equity, 1e-12)
    currency_scale = float(config.initial_equity)
    turnover = (
        torch.sum((sell_quantity + buy_quantity) * opens, dim=-1)
        * currency_scale
    )
    fees = torch.sum(sell_fees + buy_fees, dim=-1) * currency_scale
    slippage_cost = turnover * slippage
    return MinuteStepResult(
        state=MinuteExecutionState(
            cash=cash,
            shares=shares,
            last_prices=last_prices,
            equity=equity,
        ),
        net_return=net_return,
        turnover_notional=turnover,
        explicit_fees=fees,
        slippage_cost=slippage_cost,
    )


def force_close_minute_session(
    state: MinuteExecutionState,
    *,
    session_close: torch.Tensor,
    session_exit_mask: torch.Tensor,
    last_minute_volume_shares: torch.Tensor,
    sell_fee_rates: torch.Tensor,
    buy_fee_rates: torch.Tensor | None = None,
    config: MinuteExecutionConfig,
) -> MinuteCloseResult:
    if state.shares.ndim < 1:
        raise ValueError("minute force-close shares must end in a symbol axis")
    symbols = int(state.shares.size(-1))
    aligned_shape = tuple(state.shares.shape)
    if any(
        tuple(value.shape) != aligned_shape
        for value in (session_close, session_exit_mask, last_minute_volume_shares)
    ):
        raise ValueError("minute force-close market inputs must match shares on [...,S]")
    if tuple(sell_fee_rates.shape) not in {(symbols,), aligned_shape}:
        raise ValueError("minute force-close fee rates must have shape [S] or [...,S]")
    if buy_fee_rates is not None and tuple(buy_fee_rates.shape) not in {
        (symbols,),
        aligned_shape,
    }:
        raise ValueError(
            "minute force-close buy fee rates must have shape [S] or [...,S]"
        )
    held = state.shares != 0.0
    valid_exit = (
        session_exit_mask.to(device=state.shares.device, dtype=torch.bool)
        & torch.isfinite(session_close)
        & (session_close > 0.0)
    )
    if state.shares.device.type == "cpu" and bool(torch.any(held & ~valid_exit).item()):
        raise RuntimeError("minute session cannot force-close held symbols without a close")
    reference = session_close.float().to(state.shares.device)
    long_quantity = torch.clamp_min(state.shares, 0.0)
    short_quantity = torch.clamp_min(-state.shares, 0.0)
    quantity = long_quantity + short_quantity
    slippage = float(config.slippage_bps_per_side) / 10_000.0
    sell_execution = reference * (1.0 - slippage)
    buy_execution = reference * (1.0 + slippage)
    sell_rates = sell_fee_rates.float().to(state.shares.device)
    buy_rates = (
        sell_rates
        if buy_fee_rates is None
        else buy_fee_rates.float().to(state.shares.device)
    )
    sell_fees = long_quantity * sell_execution * sell_rates
    buy_fees = short_quantity * buy_execution * buy_rates
    proceeds = long_quantity * sell_execution - sell_fees
    cover_cost = short_quantity * buy_execution + buy_fees
    normalized_cash = state.cash + torch.sum(proceeds - cover_cost, dim=-1)
    normalized_pnl = torch.sum(
        long_quantity * (sell_execution - state.last_prices)
        + short_quantity * (state.last_prices - buy_execution)
        - sell_fees
        - buy_fees,
        dim=-1,
    )
    equity = state.equity + normalized_pnl
    # The forced close leaves no inventory, so cash and equity must be the
    # same canonical normalized value. The independently updated cash is kept
    # only as a finite guard against implementation mistakes on CPU.
    if state.shares.device.type == "cpu" and not bool(
        torch.all(torch.isfinite(normalized_cash)).item()
    ):
        raise RuntimeError("minute session close produced non-finite cash")
    cash = equity
    currency_scale = float(config.initial_equity)
    turnover = torch.sum(quantity * reference, dim=-1) * currency_scale
    capacity = (
        last_minute_volume_shares.float().to(state.shares.device)
        * (float(config.maximum_volume_participation) / currency_scale)
    )
    over_capacity = torch.sum(
        torch.clamp_min(quantity - capacity, 0.0) * reference, dim=-1
    ) * currency_scale
    net_return = normalized_pnl / torch.clamp_min(state.equity, 1e-12)
    return MinuteCloseResult(
        state=MinuteExecutionState(
            cash=cash,
            shares=torch.zeros_like(state.shares),
            last_prices=reference,
            equity=equity,
        ),
        net_return=net_return,
        turnover_notional=turnover,
        explicit_fees=(
            torch.sum(sell_fees + buy_fees, dim=-1) * currency_scale
        ),
        slippage_cost=turnover * slippage,
        forced_exit_over_capacity_notional=over_capacity,
    )


__all__ = [
    "MinuteCloseResult",
    "MinuteExecutionConfig",
    "MinuteExecutionState",
    "MinuteStepResult",
    "execute_minute_target",
    "force_close_minute_session",
    "initialize_minute_execution_state",
    "minute_target_weights_from_logits",
]
