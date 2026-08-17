from __future__ import annotations

import time

import torch
import torch.nn.functional as F
from torch import Tensor

from stockagent.backtest.simulator import (
    _apply_min_trade_weight_torch,
    _normalize_target_weights_torch,
    _resolve_exposure_budget,
    run_backtest_torch,
)
from stockagent.backtest.tw_execution import (
    TW_CARRYING_EXECUTION_MODES,
    normalize_execution_mode,
)
from stockagent.data.tw_index_derivatives_day import (
    TAIFEX_INDEX_DERIVATIVE_ACTION_COUNT_V4,
)
from stockagent.data.tw_index_futures import TAIFEX_INDEX_FUTURES_ACTION_COUNT
from stockagent.models.normalization import DEFAULT_PORTFOLIO_ACTIVATION


_LOSS_RUNTIME_STATS: dict[str, float] = {
    "initial_weights_clone_s": 0.0,
    "initial_weights_clone_calls": 0.0,
    "final_weights_clone_s": 0.0,
    "final_weights_clone_calls": 0.0,
    "clone_s": 0.0,
    "clone_calls": 0.0,
    "prepare_inputs_s": 0.0,
    "prepare_inputs_calls": 0.0,
    "normalize_weights_s": 0.0,
    "normalize_weights_calls": 0.0,
    "build_orders_s": 0.0,
    "build_orders_calls": 0.0,
    "backtest_s": 0.0,
    "backtest_calls": 0.0,
    "returns_postprocess_s": 0.0,
    "returns_postprocess_calls": 0.0,
    "log_utility_s": 0.0,
    "log_utility_calls": 0.0,
    "nan_to_num_s": 0.0,
    "nan_to_num_calls": 0.0,
    "mask_apply_s": 0.0,
    "mask_apply_calls": 0.0,
    "reduce_s": 0.0,
    "reduce_calls": 0.0,
    "state_update_s": 0.0,
    "state_update_calls": 0.0,
    "autograd_graph_build_s": 0.0,
    "autograd_graph_build_calls": 0.0,
}


def _execution_and_safe_returns(
    future_log_returns: Tensor,
    *,
    reference: Tensor,
    execution_mode: str,
) -> tuple[Tensor, Tensor]:
    """Keep invalid Taiwan labels observable to the canonical executor.

    Naive accounting historically treats every missing return as zero.  Taiwan
    ledgers instead decide validity from recurrent state: an inactive cell may
    be absent, while a held cash position or an entered day trade must fail
    closed.  Return a separate sanitized tensor for rank/benchmark statistics
    so those non-execution calculations cannot erase the raw ledger contract.
    """

    # Finance labels/reductions stay FP32 even when model logits arrive from
    # BF16 autocast.  Casting through the model dtype first would irreversibly
    # quantize returns before the canonical backtest promotes its state.
    raw = future_log_returns.to(device=reference.device, dtype=torch.float32)
    safe = torch.nan_to_num(raw, nan=0.0, posinf=0.0, neginf=0.0)
    execution = safe if normalize_execution_mode(execution_mode) == "naive" else raw
    return execution, safe


def _add_loss_runtime_stat(key: str, value: float = 1.0) -> None:
    _LOSS_RUNTIME_STATS[key] = float(_LOSS_RUNTIME_STATS.get(key, 0.0)) + float(value)


def _loss_timer_start() -> float | None:
    if _torch_is_compiling():
        return None
    return time.perf_counter()


def _loss_timer_stop(stat_prefix: str, start: float | None) -> None:
    if start is None:
        return
    _add_loss_runtime_stat(f"{stat_prefix}_s", time.perf_counter() - start)
    _add_loss_runtime_stat(f"{stat_prefix}_calls")


def _torch_is_compiling() -> bool:
    compiler = getattr(torch, "compiler", None)
    is_compiling = getattr(compiler, "is_compiling", None)
    if callable(is_compiling):
        try:
            return bool(is_compiling())
        except Exception:
            return False
    dynamo = getattr(torch, "_dynamo", None)
    is_compiling = getattr(dynamo, "is_compiling", None)
    if callable(is_compiling):
        try:
            return bool(is_compiling())
        except Exception:
            return False
    return False


def _clone_portfolio_state_for_loss(tensor: Tensor, *, stat_prefix: str) -> Tensor:
    if _torch_is_compiling():
        return tensor.detach().clone(memory_format=torch.contiguous_format)
    clone_start = time.perf_counter()
    cloned = tensor.detach().clone(memory_format=torch.contiguous_format)
    elapsed = time.perf_counter() - clone_start
    _add_loss_runtime_stat(f"{stat_prefix}_s", elapsed)
    _add_loss_runtime_stat(f"{stat_prefix}_calls")
    _add_loss_runtime_stat("clone_s", elapsed)
    _add_loss_runtime_stat("clone_calls")
    return cloned


def get_loss_runtime_stats(reset: bool = False) -> dict[str, float]:
    stats = dict(_LOSS_RUNTIME_STATS)
    if reset:
        for key in _LOSS_RUNTIME_STATS:
            _LOSS_RUNTIME_STATS[key] = 0.0
    return stats


def _rank_from_sorted_indices(sorted_idx: Tensor, dtype: torch.dtype) -> Tensor:
    """Construct row-wise rank tensor from argsort indices using scatter."""
    row_rank = torch.arange(sorted_idx.size(1), device=sorted_idx.device, dtype=dtype)
    row_rank = row_rank.unsqueeze(0).expand_as(sorted_idx)
    ranks = torch.empty(sorted_idx.shape, device=sorted_idx.device, dtype=dtype)
    ranks.scatter_(1, sorted_idx, row_rank)
    return ranks


def masked_mse_loss(predictions: Tensor, targets: Tensor, mask: Tensor) -> Tensor:
    mask_f = mask.to(dtype=predictions.dtype)
    error = (predictions - targets).pow(2) * mask_f
    return error.sum() / mask_f.sum().clamp_min(1.0)


def masked_ic_loss(predictions: Tensor, targets: Tensor, mask: Tensor) -> Tensor:
    mask_bool = mask.to(dtype=torch.bool, device=predictions.device)
    mask_f = mask_bool.to(dtype=torch.float32)
    valid_count = mask_f.sum(dim=1)
    valid_rows = (valid_count >= 2).to(dtype=torch.float32)

    pred = torch.nan_to_num(predictions.float(), nan=0.0, posinf=20.0, neginf=-20.0)
    target = torch.nan_to_num(targets.to(device=predictions.device).float(), nan=0.0, posinf=0.0, neginf=0.0)

    rank_fill = target.masked_fill(~mask_bool, float("inf"))
    rank_sorted_idx = rank_fill.argsort(dim=1)
    target_rank = _rank_from_sorted_indices(rank_sorted_idx, dtype=pred.dtype)

    count = valid_count.clamp_min(1.0).unsqueeze(1)
    pred_mean = (pred * mask_f).sum(dim=1, keepdim=True) / count
    rank_mean = (target_rank * mask_f).sum(dim=1, keepdim=True) / count

    pred_centered = (pred - pred_mean) * mask_f
    rank_centered = (target_rank - rank_mean) * mask_f
    denom = torch.sqrt(
        (pred_centered.pow(2).sum(dim=1) * rank_centered.pow(2).sum(dim=1)).clamp_min(1e-8)
    )
    ic = (pred_centered * rank_centered).sum(dim=1) / denom
    return -(ic * valid_rows).sum() / valid_rows.sum().clamp_min(1.0)


def _apply_sample_mask_to_asset_mask(mask: Tensor, sample_mask: Tensor | None) -> Tensor:
    """Exclude synthetic/padded rows from cross-sectional auxiliary losses."""
    mask_bool = mask.to(dtype=torch.bool)
    if sample_mask is None:
        return mask_bool
    valid_rows = sample_mask.to(device=mask_bool.device, dtype=torch.bool)
    if valid_rows.dim() != 1 or int(valid_rows.size(0)) != int(mask_bool.size(0)):
        raise ValueError(
            "sample_mask must be one-dimensional and match the batch rows: "
            f"{tuple(valid_rows.shape)} != ({int(mask_bool.size(0))},)"
        )
    return mask_bool & valid_rows.unsqueeze(1)


def _resolve_rank_scores(
    weights: Tensor,
    aux_outputs: dict[str, Tensor] | None,
) -> Tensor:
    """Return the canonical cross-sectional score without requiring a rank head.

    Models with an explicit ranking or score head expose it through auxiliary
    outputs.  Portfolio weights are deliberately the canonical score for models
    without either head; this is part of the objective contract, not an error
    fallback.
    """
    if aux_outputs:
        rank_logits = aux_outputs.get("rank_logits")
        if isinstance(rank_logits, Tensor):
            return rank_logits
        score_logits = aux_outputs.get("score_logits")
        if isinstance(score_logits, Tensor):
            return score_logits
    return weights


def _masked_zscore(values: Tensor, mask: Tensor) -> Tensor:
    mask_bool = mask.to(dtype=torch.bool, device=values.device)
    mask_f = mask_bool.to(dtype=values.dtype)
    count = mask_f.sum(dim=1, keepdim=True).clamp_min(1.0)
    clean = torch.nan_to_num(values, nan=0.0, posinf=20.0, neginf=-20.0).clamp(min=-20.0, max=20.0)
    mean = (clean * mask_f).sum(dim=1, keepdim=True) / count
    centered = (clean - mean) * mask_f
    var = centered.pow(2).sum(dim=1, keepdim=True) / count.clamp_min(2.0)
    z = centered / torch.sqrt(var + 1e-6)
    return torch.where(mask_bool, z, torch.zeros_like(z))


def _masked_corr_per_row(predictions: Tensor, targets: Tensor, mask: Tensor) -> Tensor:
    mask_bool = mask.to(dtype=torch.bool, device=predictions.device)
    mask_f = mask_bool.to(dtype=torch.float32)
    valid_count = mask_f.sum(dim=1)
    valid_rows = valid_count >= 2

    pred = torch.nan_to_num(predictions.float(), nan=0.0, posinf=20.0, neginf=-20.0)
    target = torch.nan_to_num(targets.to(device=predictions.device).float(), nan=0.0, posinf=0.0, neginf=0.0)
    count = valid_count.clamp_min(1.0).unsqueeze(1)
    pred_mean = (pred * mask_f).sum(dim=1, keepdim=True) / count
    target_mean = (target * mask_f).sum(dim=1, keepdim=True) / count
    pred_centered = (pred - pred_mean) * mask_f
    target_centered = (target - target_mean) * mask_f
    denom = torch.sqrt(
        (pred_centered.pow(2).sum(dim=1) * target_centered.pow(2).sum(dim=1)).clamp_min(1e-8)
    )
    corr = (pred_centered * target_centered).sum(dim=1) / denom
    return torch.where(valid_rows, corr, torch.zeros_like(corr))


def _fama_macbeth_slope_per_row(scores: Tensor, returns: Tensor, mask: Tensor) -> Tensor:
    mask_bool = mask.to(dtype=torch.bool, device=scores.device)
    mask_f = mask_bool.to(dtype=torch.float32)
    valid_count = mask_f.sum(dim=1)
    valid_rows = valid_count >= 2

    score = _masked_zscore(scores.float(), mask_bool)
    ret = torch.nan_to_num(returns.to(device=scores.device).float(), nan=0.0, posinf=0.0, neginf=0.0)
    count = valid_count.clamp_min(1.0).unsqueeze(1)
    ret_mean = (ret * mask_f).sum(dim=1, keepdim=True) / count
    ret_centered = (ret - ret_mean) * mask_f
    denom = score.pow(2).sum(dim=1).clamp_min(1e-6)
    beta = (score * ret_centered).sum(dim=1) / denom
    return torch.where(valid_rows, beta, torch.zeros_like(beta))


def _stable_negative_tstat(values: Tensor, valid_mask: Tensor | None = None) -> Tensor:
    values = torch.nan_to_num(values.float(), nan=0.0, posinf=0.0, neginf=0.0)
    if valid_mask is not None:
        mask_f = valid_mask.to(device=values.device, dtype=values.dtype)
        raw_count = mask_f.sum()
        count = raw_count.clamp_min(1.0)
        mean = (values * mask_f).sum() / count
        variance = ((values - mean).pow(2) * mask_f).sum() / count
        std = torch.sqrt(variance + 1e-8)
        stat = -(mean / std) * torch.sqrt(count)
        return torch.where(raw_count > 0.0, stat, values.sum() * 0.0)
    if values.numel() == 0:
        return values.sum() * 0.0
    mean = values.mean()
    std = torch.sqrt((values - mean).pow(2).mean() + 1e-8)
    n_eff = torch.sqrt(values.new_tensor(float(max(1, int(values.numel())))))
    return -(mean / std) * n_eff


def _block_stability_loss(values: Tensor, block_count: int, worst_fraction: float) -> Tensor:
    values = torch.nan_to_num(values.float(), nan=0.0, posinf=0.0, neginf=0.0)
    n_rows = int(values.numel())
    if n_rows < 2:
        return values.sum() * 0.0
    blocks = max(2, min(max(1, int(block_count)), n_rows))
    padded_rows = ((n_rows + blocks - 1) // blocks) * blocks
    if padded_rows > n_rows:
        pad = values[-1:].expand(padded_rows - n_rows)
        values = torch.cat([values, pad], dim=0)
    block_values = values.reshape(blocks, -1).mean(dim=1)
    mean = block_values.mean()
    std = torch.sqrt((block_values - mean).pow(2).mean() + 1e-8)
    k = max(1, int(round(blocks * min(max(float(worst_fraction), 0.0), 1.0))))
    worst_mean = torch.topk(block_values, k=k, largest=False).values.mean()
    return -mean + std - worst_mean


def _regime_stability_loss(values: Tensor, benchmark: Tensor, down_threshold: float, up_threshold: float) -> Tensor:
    values = torch.nan_to_num(values.float(), nan=0.0, posinf=0.0, neginf=0.0)
    bench = torch.nan_to_num(benchmark.to(device=values.device).float(), nan=0.0, posinf=0.0, neginf=0.0)
    regime_id = torch.ones_like(bench, dtype=torch.long)
    regime_id = torch.where(bench <= float(down_threshold), torch.zeros_like(regime_id), regime_id)
    regime_id = torch.where(bench >= float(up_threshold), torch.full_like(regime_id, 2), regime_id)
    regime_f = F.one_hot(regime_id, num_classes=3).to(dtype=values.dtype)
    counts = regime_f.sum(dim=0)
    means = (regime_f * values.unsqueeze(1)).sum(dim=0) / counts.clamp_min(1.0)
    present = counts > 0
    present_f = present.to(dtype=values.dtype)
    present_count = present_f.sum()
    mean = (means * present_f).sum() / present_count.clamp_min(1.0)
    std = torch.sqrt(((means - mean).pow(2) * present_f).sum() / present_count.clamp_min(1.0) + 1e-8)
    worst = torch.where(present, means, means.new_full(means.shape, float("inf"))).min()
    loss = -mean + std + F.softplus(-worst)
    return torch.where(present_count >= 2.0, loss, values.sum() * 0.0)


def factor_generalization_loss(
    weights: Tensor,
    future_log_returns: Tensor,
    tradable_mask: Tensor,
    benchmark_returns: Tensor | None = None,
    can_buy_mask: Tensor | None = None,
    can_sell_mask: Tensor | None = None,
    can_short_open_mask: Tensor | None = None,
    force_short_cover_mask: Tensor | None = None,
    force_exit_mask: Tensor | None = None,
    sample_mask: Tensor | None = None,
    long_only: bool = True,
    buy_fee_rate: float = 0.0,
    sell_fee_rate: float = 0.0,
    max_turnover_ratio: float = 0.0,
    volume_limit_weights: Tensor | None = None,
    short_margin_rate: Tensor | float | None = None,
    short_capacity_weights: Tensor | None = None,
    short_maintenance_ratio: float = 1.30,
    short_handling_fee_rate: Tensor | float = 0.0,
    gross_leverage: float = 1.0,
    min_trade_weight: float = 0.0,
    portfolio_activation: str = DEFAULT_PORTFOLIO_ACTIVATION,
    execution_mode: str = "naive",
    buy_fee_rates: Tensor | None = None,
    sell_fee_rates: Tensor | None = None,
    normal_sell_fee_rates: Tensor | None = None,
    day_trade_unlimited_margin_conversion: bool = False,
    day_trade_margin_financing_ratio: float = 0.60,
    day_trade_margin_financing_annual_rate: float = 0.16,
    day_trade_margin_short_handling_fee_rate: float = 0.001,
    day_trade_margin_short_annual_borrow_rate: float = 0.20,
    commission_rebate_rates: Tensor | None = None,
    commission_rebate_timing: str = "daily_close",
    session_month_ids: Tensor | None = None,
    commission_rebate_payment_eligible_mask: Tensor | None = None,
    settlement_lag_sessions: int = 2,
    day_trade_eligible_mask: Tensor | None = None,
    day_trade_can_buy_open_mask: Tensor | None = None,
    day_trade_can_sell_open_mask: Tensor | None = None,
    unresolved_corporate_action_mask: Tensor | None = None,
    cash_dividend_yield: Tensor | None = None,
    cash_dividend_payment_delay_sessions: Tensor | None = None,
    claim_queue_sessions: int | None = None,
    state_advance_mask: Tensor | None = None,
    symbol_indices: Tensor | None = None,
    aux_outputs: dict[str, Tensor] | None = None,
    slope_tstat_weight: float = 1.0,
    rank_ic_weight: float = 0.5,
    factor_sharpe_weight: float = 0.25,
    block_stability_weight: float = 0.20,
    regime_stability_weight: float = 0.20,
    consistency_weight: float = 0.05,
    net_exposure_weight: float = 0.05,
    gross_exposure_weight: float = 0.02,
    concentration_weight: float = 0.02,
    turnover_weight: float = 0.02,
    score_l2_weight: float = 0.001,
    factor_temperature: float = 1.0,
    block_count: int = 4,
    worst_fraction: float = 0.25,
    regime_up_threshold: float = 0.002,
    regime_down_threshold: float = -0.002,
    can_short_open_open_mask: Tensor | None = None,
) -> Tensor:
    """Train scores as a stable, tradable cross-sectional characteristic factor."""
    weights = torch.nan_to_num(weights, nan=0.0, posinf=0.0, neginf=0.0)
    execution_returns, returns = _execution_and_safe_returns(
        future_log_returns,
        reference=weights,
        execution_mode=execution_mode,
    )
    tradable = tradable_mask.to(dtype=torch.bool, device=weights.device)
    if sample_mask is None:
        valid_rows = torch.ones(weights.size(0), device=weights.device, dtype=torch.bool)
    else:
        valid_rows = sample_mask.to(device=weights.device, dtype=torch.bool)
    valid_f = valid_rows.to(dtype=returns.dtype)
    valid_count = valid_f.sum().clamp_min(1.0)
    if benchmark_returns is None:
        tradable_f = tradable.to(dtype=returns.dtype)
        benchmark = (returns * tradable_f).sum(dim=1) / tradable_f.sum(dim=1).clamp_min(1.0)
    else:
        benchmark = torch.nan_to_num(
            benchmark_returns.to(device=weights.device, dtype=returns.dtype),
            nan=0.0,
            posinf=0.0,
            neginf=0.0,
        )

    scores = _resolve_rank_scores(weights, aux_outputs).to(device=weights.device)
    scores_z = _masked_zscore(scores.float(), tradable)

    total = returns.new_zeros(())
    slope = _fama_macbeth_slope_per_row(scores_z, returns, tradable)
    if float(slope_tstat_weight) > 0.0:
        total = total + float(slope_tstat_weight) * _stable_negative_tstat(slope, valid_rows)

    if float(rank_ic_weight) > 0.0:
        rank_ic = _masked_corr_per_row(scores_z, returns, tradable)
        total = total + float(rank_ic_weight) * _stable_negative_tstat(rank_ic, valid_rows)

    # Keep factor scores as logits here.  The canonical backtest is the single
    # owner of activation, long-only clipping, L1 normalization, and gross
    # budgeting.  Pre-normalizing here used to apply nonlinear activations a
    # second time inside ``run_backtest_torch`` and made the factor objective
    # disagree with evaluation.
    factor_scores = torch.where(
        tradable,
        scores_z / max(float(factor_temperature), 0.05),
        torch.zeros_like(scores_z),
    )
    can_buy = can_buy_mask.to(dtype=torch.bool, device=weights.device) if can_buy_mask is not None else tradable
    needs_penalty_weights = (
        float(net_exposure_weight) > 0.0
        or float(gross_exposure_weight) > 0.0
        or float(concentration_weight) > 0.0
    )
    initial_weights = aux_outputs.get("initial_weights") if aux_outputs else None
    initial_alive = aux_outputs.get("initial_alive") if aux_outputs else None
    initial_cash = aux_outputs.get("initial_cash") if aux_outputs else None
    initial_payables = aux_outputs.get("initial_payables") if aux_outputs else None
    initial_receivables = aux_outputs.get("initial_receivables") if aux_outputs else None
    initial_commission_rebate_current = (
        aux_outputs.get("initial_commission_rebate_current")
        if aux_outputs
        else None
    )
    initial_commission_rebate_due = (
        aux_outputs.get("initial_commission_rebate_due") if aux_outputs else None
    )
    initial_commission_rebate_month_id = (
        aux_outputs.get("initial_commission_rebate_month_id")
        if aux_outputs
        else None
    )
    initial_equity_scale = (
        aux_outputs.get("initial_equity_scale") if aux_outputs else None
    )
    initial_short_sale_collateral = (
        aux_outputs.get("initial_short_sale_collateral") if aux_outputs else None
    )
    initial_short_margin_collateral = (
        aux_outputs.get("initial_short_margin_collateral") if aux_outputs else None
    )
    factor_backtest = run_backtest_torch(
        factor_scores,
        execution_returns,
        tradable,
        benchmark,
        buy_fee_rate,
        sell_fee_rate,
        long_only=long_only,
        max_turnover_ratio=max_turnover_ratio,
        volume_limit_weights=volume_limit_weights,
        short_margin_rate=short_margin_rate,
        short_capacity_weights=short_capacity_weights,
        short_maintenance_ratio=short_maintenance_ratio,
        short_handling_fee_rate=short_handling_fee_rate,
        gross_leverage=gross_leverage,
        min_trade_weight=min_trade_weight,
        portfolio_activation=portfolio_activation,
        can_buy_mask=can_buy,
        can_sell_mask=can_sell_mask.to(dtype=torch.bool, device=weights.device) if can_sell_mask is not None else None,
        can_short_open_mask=(
            can_short_open_mask.to(dtype=torch.bool, device=weights.device)
            if can_short_open_mask is not None
            else None
        ),
        can_short_open_open_mask=(
            can_short_open_open_mask.to(dtype=torch.bool, device=weights.device)
            if can_short_open_open_mask is not None
            else None
        ),
        force_short_cover_mask=(
            force_short_cover_mask.to(dtype=torch.bool, device=weights.device)
            if force_short_cover_mask is not None
            else None
        ),
        force_exit_mask=(
            force_exit_mask.to(dtype=torch.bool, device=weights.device)
            if force_exit_mask is not None
            else None
        ),
        return_weights_history=needs_penalty_weights,
        initial_weights=initial_weights,
        initial_alive=initial_alive,
        execution_mode=execution_mode,
        buy_fee_rates=buy_fee_rates,
        sell_fee_rates=sell_fee_rates,
        normal_sell_fee_rates=normal_sell_fee_rates,
        day_trade_unlimited_margin_conversion=(
            day_trade_unlimited_margin_conversion
        ),
        day_trade_margin_financing_ratio=day_trade_margin_financing_ratio,
        day_trade_margin_financing_annual_rate=(
            day_trade_margin_financing_annual_rate
        ),
        day_trade_margin_short_handling_fee_rate=(
            day_trade_margin_short_handling_fee_rate
        ),
        day_trade_margin_short_annual_borrow_rate=(
            day_trade_margin_short_annual_borrow_rate
        ),
        commission_rebate_rates=commission_rebate_rates,
        commission_rebate_timing=commission_rebate_timing,
        session_month_ids=session_month_ids,
        commission_rebate_payment_eligible_mask=(
            commission_rebate_payment_eligible_mask
        ),
        settlement_lag_sessions=settlement_lag_sessions,
        day_trade_eligible_mask=day_trade_eligible_mask,
        day_trade_can_buy_open_mask=day_trade_can_buy_open_mask,
        day_trade_can_sell_open_mask=day_trade_can_sell_open_mask,
        unresolved_corporate_action_mask=unresolved_corporate_action_mask,
        cash_dividend_yield=cash_dividend_yield,
        cash_dividend_payment_delay_sessions=(
            cash_dividend_payment_delay_sessions
        ),
        claim_queue_sessions=claim_queue_sessions,
        state_advance_mask=state_advance_mask if state_advance_mask is not None else sample_mask,
        initial_cash=initial_cash,
        initial_payables=initial_payables,
        initial_receivables=initial_receivables,
        initial_commission_rebate_current=initial_commission_rebate_current,
        initial_commission_rebate_due=initial_commission_rebate_due,
        initial_commission_rebate_month_id=initial_commission_rebate_month_id,
        initial_equity_scale=initial_equity_scale,
        initial_short_sale_collateral=initial_short_sale_collateral,
        initial_short_margin_collateral=initial_short_margin_collateral,
        symbol_indices=symbol_indices,
    )
    if aux_outputs is not None and factor_backtest.final_weights is not None:
        aux_outputs["_final_weights"] = _clone_portfolio_state_for_loss(
            factor_backtest.final_weights,
            stat_prefix="final_weights_clone",
        )
    if aux_outputs is not None and factor_backtest.final_alive is not None:
        aux_outputs["_final_alive"] = _clone_portfolio_state_for_loss(
            factor_backtest.final_alive,
            stat_prefix="final_alive_clone",
        )
    if aux_outputs is not None:
        for public_name, private_name, value in (
            ("cash", "_final_cash", factor_backtest.final_cash),
            ("payables", "_final_payables", factor_backtest.final_payables),
            ("receivables", "_final_receivables", factor_backtest.final_receivables),
            (
                "commission_rebate_current",
                "_final_commission_rebate_current",
                factor_backtest.final_commission_rebate_current,
            ),
            (
                "commission_rebate_due",
                "_final_commission_rebate_due",
                factor_backtest.final_commission_rebate_due,
            ),
            (
                "commission_rebate_month_id",
                "_final_commission_rebate_month_id",
                factor_backtest.final_commission_rebate_month_id,
            ),
            (
                "equity_scale",
                "_final_equity_scale",
                factor_backtest.final_equity_scale,
            ),
            (
                "short_sale_collateral",
                "_final_short_sale_collateral",
                factor_backtest.final_short_sale_collateral,
            ),
            (
                "short_margin_collateral",
                "_final_short_margin_collateral",
                factor_backtest.final_short_margin_collateral,
            ),
        ):
            if value is not None:
                aux_outputs[private_name] = _clone_portfolio_state_for_loss(
                    value,
                    stat_prefix=f"final_{public_name}_clone",
                )
    factor_returns = torch.nan_to_num(factor_backtest.strategy_returns, nan=0.0, posinf=0.0, neginf=0.0)
    factor_returns_valid = factor_returns[valid_rows]
    if factor_returns_valid.numel() > 0 and float(factor_sharpe_weight) > 0.0:
        total = total + float(factor_sharpe_weight) * _stable_negative_tstat(factor_returns_valid)
    if factor_returns_valid.numel() > 1 and float(block_stability_weight) > 0.0:
        total = total + float(block_stability_weight) * _block_stability_loss(
            factor_returns_valid,
            block_count=block_count,
            worst_fraction=worst_fraction,
        )
    if factor_returns_valid.numel() > 1 and float(regime_stability_weight) > 0.0:
        total = total + float(regime_stability_weight) * _regime_stability_loss(
            factor_returns[valid_rows],
            benchmark[valid_rows],
            down_threshold=regime_down_threshold,
            up_threshold=regime_up_threshold,
        )

    if aux_outputs and float(consistency_weight) > 0.0:
        aug_scores = aux_outputs.get("aug_score_logits")
        if aug_scores is not None:
            aug_scores_z = _masked_zscore(aug_scores.to(device=weights.device).float(), tradable)
            consistency = 1.0 - _masked_corr_per_row(scores_z, aug_scores_z, tradable)
            total = total + float(consistency_weight) * (consistency * valid_f).sum() / valid_count

    if needs_penalty_weights:
        weights_safe = _resolved_penalty_weights(
            factor_backtest.weights_history,
            factor_scores,
            long_only=long_only,
            gross_leverage=gross_leverage,
            min_trade_weight=min_trade_weight,
            portfolio_activation=portfolio_activation,
        ).to(dtype=returns.dtype)
        if float(net_exposure_weight) > 0.0:
            net_exposure = weights_safe.sum(dim=1).pow(2)
            total = total + float(net_exposure_weight) * (net_exposure * valid_f).sum() / valid_count
        if float(gross_exposure_weight) > 0.0:
            total = total + float(gross_exposure_weight) * _portfolio_gross_exposure_error(
                weights_safe,
                valid_rows,
                gross_leverage=gross_leverage,
            )
        if float(concentration_weight) > 0.0:
            total = total + float(concentration_weight) * _portfolio_concentration_penalty(
                weights_safe,
                tradable,
                valid_rows,
            )
    if float(turnover_weight) > 0.0:
        turnover_mean, _ = _dense_masked_clean_mean(factor_backtest.turnovers, valid_rows)
        total = total + float(turnover_weight) * turnover_mean
    if float(score_l2_weight) > 0.0:
        score_l2 = (scores_z.pow(2) * tradable.to(dtype=scores_z.dtype)).sum(dim=1)
        denom = tradable.to(dtype=scores_z.dtype).sum(dim=1).clamp_min(1.0)
        total = total + float(score_l2_weight) * ((score_l2 / denom) * valid_f).sum() / valid_count

    return total


def portfolio_autoencoder_loss(
    weights: Tensor,
    future_log_returns: Tensor,
    tradable_mask: Tensor,
    benchmark_returns: Tensor | None = None,
    can_buy_mask: Tensor | None = None,
    can_sell_mask: Tensor | None = None,
    can_short_open_mask: Tensor | None = None,
    force_short_cover_mask: Tensor | None = None,
    force_exit_mask: Tensor | None = None,
    sample_mask: Tensor | None = None,
    long_only: bool = True,
    buy_fee_rate: float = 0.0,
    sell_fee_rate: float = 0.0,
    max_turnover_ratio: float = 0.0,
    volume_limit_weights: Tensor | None = None,
    short_margin_rate: Tensor | float | None = None,
    short_capacity_weights: Tensor | None = None,
    short_maintenance_ratio: float = 1.30,
    short_handling_fee_rate: Tensor | float = 0.0,
    gross_leverage: float = 1.0,
    min_trade_weight: float = 0.0,
    portfolio_activation: str = DEFAULT_PORTFOLIO_ACTIVATION,
    execution_mode: str = "naive",
    buy_fee_rates: Tensor | None = None,
    sell_fee_rates: Tensor | None = None,
    normal_sell_fee_rates: Tensor | None = None,
    commission_rebate_rates: Tensor | None = None,
    commission_rebate_timing: str = "daily_close",
    session_month_ids: Tensor | None = None,
    commission_rebate_payment_eligible_mask: Tensor | None = None,
    settlement_lag_sessions: int = 2,
    day_trade_eligible_mask: Tensor | None = None,
    day_trade_can_buy_open_mask: Tensor | None = None,
    day_trade_can_sell_open_mask: Tensor | None = None,
    unresolved_corporate_action_mask: Tensor | None = None,
    cash_dividend_yield: Tensor | None = None,
    cash_dividend_payment_delay_sessions: Tensor | None = None,
    claim_queue_sessions: int | None = None,
    state_advance_mask: Tensor | None = None,
    symbol_indices: Tensor | None = None,
    aux_outputs: dict[str, Tensor] | None = None,
    autoencoder_lambda_turnover: float = 0.1,
    autoencoder_lambda_concentration: float = 0.01,
    autoencoder_lambda_latent: float = 0.001,
    can_short_open_open_mask: Tensor | None = None,
) -> Tensor:
    """Portfolio-level objective evaluated by the canonical execution simulator."""

    weights_safe = torch.nan_to_num(weights, nan=0.0, posinf=0.0, neginf=0.0)
    execution_returns, returns = _execution_and_safe_returns(
        future_log_returns,
        reference=weights_safe,
        execution_mode=execution_mode,
    )
    tradable = tradable_mask.to(device=weights.device, dtype=torch.bool)
    if sample_mask is None:
        valid_rows = torch.ones(weights_safe.size(0), device=weights.device, dtype=torch.bool)
    else:
        valid_rows = sample_mask.to(device=weights.device, dtype=torch.bool)
    valid_f = valid_rows.to(dtype=weights_safe.dtype)
    valid_count = valid_f.sum().clamp_min(1.0)

    if benchmark_returns is None:
        benchmark = returns.new_zeros((returns.size(0),))
    else:
        benchmark = torch.nan_to_num(
            benchmark_returns.to(device=weights.device, dtype=returns.dtype),
            nan=0.0,
            posinf=0.0,
            neginf=0.0,
        )
    initial_weights = aux_outputs.get("initial_weights") if aux_outputs else None
    initial_alive = aux_outputs.get("initial_alive") if aux_outputs else None
    initial_cash = aux_outputs.get("initial_cash") if aux_outputs else None
    initial_payables = aux_outputs.get("initial_payables") if aux_outputs else None
    initial_receivables = aux_outputs.get("initial_receivables") if aux_outputs else None
    initial_commission_rebate_current = (
        aux_outputs.get("initial_commission_rebate_current")
        if aux_outputs
        else None
    )
    initial_commission_rebate_due = (
        aux_outputs.get("initial_commission_rebate_due") if aux_outputs else None
    )
    initial_commission_rebate_month_id = (
        aux_outputs.get("initial_commission_rebate_month_id")
        if aux_outputs
        else None
    )
    initial_equity_scale = (
        aux_outputs.get("initial_equity_scale") if aux_outputs else None
    )
    initial_short_sale_collateral = (
        aux_outputs.get("initial_short_sale_collateral") if aux_outputs else None
    )
    initial_short_margin_collateral = (
        aux_outputs.get("initial_short_margin_collateral") if aux_outputs else None
    )
    backtest = run_backtest_torch(
        weights_safe,
        execution_returns,
        tradable,
        benchmark,
        buy_fee_rate,
        sell_fee_rate,
        long_only=long_only,
        max_turnover_ratio=max_turnover_ratio,
        volume_limit_weights=volume_limit_weights,
        short_margin_rate=short_margin_rate,
        short_capacity_weights=short_capacity_weights,
        short_maintenance_ratio=short_maintenance_ratio,
        short_handling_fee_rate=short_handling_fee_rate,
        gross_leverage=gross_leverage,
        min_trade_weight=min_trade_weight,
        portfolio_activation=portfolio_activation,
        can_buy_mask=(
            can_buy_mask.to(device=weights.device, dtype=torch.bool)
            if can_buy_mask is not None
            else tradable
        ),
        can_sell_mask=(
            can_sell_mask.to(device=weights.device, dtype=torch.bool)
            if can_sell_mask is not None
            else tradable
        ),
        can_short_open_mask=(
            can_short_open_mask.to(device=weights.device, dtype=torch.bool)
            if can_short_open_mask is not None
            else None
        ),
        can_short_open_open_mask=(
            can_short_open_open_mask.to(device=weights.device, dtype=torch.bool)
            if can_short_open_open_mask is not None
            else None
        ),
        force_short_cover_mask=(
            force_short_cover_mask.to(device=weights.device, dtype=torch.bool)
            if force_short_cover_mask is not None
            else None
        ),
        force_exit_mask=(
            force_exit_mask.to(device=weights.device, dtype=torch.bool)
            if force_exit_mask is not None
            else None
        ),
        initial_weights=initial_weights,
        initial_alive=initial_alive,
        execution_mode=execution_mode,
        buy_fee_rates=buy_fee_rates,
        sell_fee_rates=sell_fee_rates,
        normal_sell_fee_rates=normal_sell_fee_rates,
        commission_rebate_rates=commission_rebate_rates,
        commission_rebate_timing=commission_rebate_timing,
        session_month_ids=session_month_ids,
        commission_rebate_payment_eligible_mask=(
            commission_rebate_payment_eligible_mask
        ),
        settlement_lag_sessions=settlement_lag_sessions,
        day_trade_eligible_mask=day_trade_eligible_mask,
        day_trade_can_buy_open_mask=day_trade_can_buy_open_mask,
        day_trade_can_sell_open_mask=day_trade_can_sell_open_mask,
        unresolved_corporate_action_mask=unresolved_corporate_action_mask,
        cash_dividend_yield=cash_dividend_yield,
        cash_dividend_payment_delay_sessions=(
            cash_dividend_payment_delay_sessions
        ),
        claim_queue_sessions=claim_queue_sessions,
        state_advance_mask=state_advance_mask if state_advance_mask is not None else sample_mask,
        initial_cash=initial_cash,
        initial_payables=initial_payables,
        initial_receivables=initial_receivables,
        initial_commission_rebate_current=initial_commission_rebate_current,
        initial_commission_rebate_due=initial_commission_rebate_due,
        initial_commission_rebate_month_id=initial_commission_rebate_month_id,
        initial_equity_scale=initial_equity_scale,
        initial_short_sale_collateral=initial_short_sale_collateral,
        initial_short_margin_collateral=initial_short_margin_collateral,
        symbol_indices=symbol_indices,
        return_weights_history=True,
    )
    if aux_outputs is not None and backtest.final_weights is not None:
        aux_outputs["_final_weights"] = _clone_portfolio_state_for_loss(
            backtest.final_weights,
            stat_prefix="final_weights_clone",
        )
    if aux_outputs is not None and backtest.final_alive is not None:
        aux_outputs["_final_alive"] = _clone_portfolio_state_for_loss(
            backtest.final_alive,
            stat_prefix="final_alive_clone",
        )
    if aux_outputs is not None:
        for public_name, private_name, value in (
            ("cash", "_final_cash", backtest.final_cash),
            ("payables", "_final_payables", backtest.final_payables),
            ("receivables", "_final_receivables", backtest.final_receivables),
            (
                "commission_rebate_current",
                "_final_commission_rebate_current",
                backtest.final_commission_rebate_current,
            ),
            (
                "commission_rebate_due",
                "_final_commission_rebate_due",
                backtest.final_commission_rebate_due,
            ),
            (
                "commission_rebate_month_id",
                "_final_commission_rebate_month_id",
                backtest.final_commission_rebate_month_id,
            ),
            ("equity_scale", "_final_equity_scale", backtest.final_equity_scale),
            (
                "short_sale_collateral",
                "_final_short_sale_collateral",
                backtest.final_short_sale_collateral,
            ),
            (
                "short_margin_collateral",
                "_final_short_margin_collateral",
                backtest.final_short_margin_collateral,
            ),
        ):
            if value is not None:
                aux_outputs[private_name] = _clone_portfolio_state_for_loss(
                    value,
                    stat_prefix=f"final_{public_name}_clone",
                )
    net_return = torch.nan_to_num(backtest.strategy_returns, nan=0.0, posinf=0.0, neginf=0.0)
    turnover = torch.nan_to_num(backtest.turnovers, nan=0.0, posinf=0.0, neginf=0.0)

    mean_return = (net_return * valid_f).sum() / valid_count
    centered = (net_return - mean_return) * valid_f
    std_return = torch.sqrt(centered.pow(2).sum() / valid_count + 1e-8)
    annualizer = torch.sqrt(torch.as_tensor(252.0, device=weights.device, dtype=net_return.dtype))
    sharpe = mean_return / std_return * annualizer

    total = -sharpe
    if float(autoencoder_lambda_turnover) > 0.0:
        total = total + float(autoencoder_lambda_turnover) * (turnover * valid_f).sum() / valid_count
    if float(autoencoder_lambda_concentration) > 0.0:
        concentration = backtest.weights_history.pow(2).sum(dim=1)
        total = total + float(autoencoder_lambda_concentration) * (concentration * valid_f).sum() / valid_count
    if aux_outputs and float(autoencoder_lambda_latent) > 0.0:
        latent = aux_outputs.get("latent_z")
        if latent is None:
            latent = aux_outputs.get("z")
        if latent is not None:
            latent_safe = torch.nan_to_num(latent.to(device=weights.device).float(), nan=0.0, posinf=0.0, neginf=0.0)
            if latent_safe.dim() == 0 or int(latent_safe.size(0)) != int(weights_safe.size(0)):
                raise ValueError(
                    "autoencoder latent tensor must have one leading row per sample: "
                    f"shape={tuple(latent_safe.shape)}, rows={int(weights_safe.size(0))}"
                )
            latent_l2_per_row = latent_safe.reshape(latent_safe.size(0), -1).pow(2).mean(dim=1)
            total = total + float(autoencoder_lambda_latent) * (
                (latent_l2_per_row * valid_f).sum() / valid_count
            )
    return total


def sharpe_aware_loss(
    weights: Tensor,
    future_log_returns: Tensor,
    tradable_mask: Tensor,
    can_buy_mask: Tensor | None = None,
    can_sell_mask: Tensor | None = None,
    can_short_open_mask: Tensor | None = None,
    force_short_cover_mask: Tensor | None = None,
    force_exit_mask: Tensor | None = None,
    sample_mask: Tensor | None = None,
    long_only: bool = True,
    buy_fee_rate: float = 0.0,
    sell_fee_rate: float = 0.0,
    max_turnover_ratio: float = 0.0,
    volume_limit_weights: Tensor | None = None,
    gross_leverage: float = 1.0,
    min_trade_weight: float = 0.0,
    portfolio_activation: str = DEFAULT_PORTFOLIO_ACTIVATION,
    gamma_sharpe: float = 1.0,
    gamma_turnover: float = 0.1,
) -> Tensor:
    """Sharpe-aware loss driven by the same backtest kernel used in evaluation."""
    weights = torch.nan_to_num(weights, nan=0.0, posinf=0.0, neginf=0.0)
    returns = torch.nan_to_num(
        future_log_returns, nan=0.0, posinf=0.0, neginf=0.0
    )
    tradable = tradable_mask.to(dtype=torch.bool, device=weights.device)
    can_buy = (
        can_buy_mask.to(dtype=torch.bool, device=weights.device)
        if can_buy_mask is not None
        else tradable
    )
    benchmark_zeros = torch.zeros(weights.size(0), device=weights.device, dtype=returns.dtype)

    backtest = run_backtest_torch(
        weights,
        returns,
        tradable,
        benchmark_zeros,
        buy_fee_rate,
        sell_fee_rate,
        long_only=long_only,
        max_turnover_ratio=max_turnover_ratio,
        volume_limit_weights=volume_limit_weights,
        gross_leverage=gross_leverage,
        min_trade_weight=min_trade_weight,
        portfolio_activation=portfolio_activation,
        can_buy_mask=can_buy,
        can_sell_mask=can_sell_mask.to(dtype=torch.bool, device=weights.device) if can_sell_mask is not None else None,
        can_short_open_mask=(
            can_short_open_mask.to(dtype=torch.bool, device=weights.device)
            if can_short_open_mask is not None
            else None
        ),
        force_short_cover_mask=(
            force_short_cover_mask.to(dtype=torch.bool, device=weights.device)
            if force_short_cover_mask is not None
            else None
        ),
        force_exit_mask=(
            force_exit_mask.to(dtype=torch.bool, device=weights.device)
            if force_exit_mask is not None
            else None
        ),
        return_weights_history=False,
    )

    if sample_mask is None:
        valid_mask = torch.ones(weights.size(0), device=weights.device, dtype=torch.bool)
    else:
        valid_mask = sample_mask.to(device=weights.device, dtype=torch.bool)

    valid_returns = backtest.strategy_returns[valid_mask]
    valid_returns = torch.nan_to_num(valid_returns, nan=0.0, posinf=0.0, neginf=0.0)
    if valid_returns.numel() == 0:
        return weights.sum() * 0.0

    mean_return = valid_returns.mean()
    variance = (valid_returns - mean_return).pow(2).mean()
    std_return = torch.sqrt(variance + 1e-8)
    annualizer = torch.sqrt(torch.as_tensor(252.0, device=weights.device, dtype=valid_returns.dtype))
    sharpe = mean_return / std_return * annualizer

    # Keep turnover regularization, but source it from the same backtest path.
    if gamma_turnover == 0.0:
        return -gamma_sharpe * sharpe

    valid_turnovers = backtest.turnovers[valid_mask]
    valid_turnovers = torch.nan_to_num(valid_turnovers, nan=0.0, posinf=0.0, neginf=0.0)
    turnover_penalty = valid_turnovers.mean() if valid_turnovers.numel() > 0 else valid_returns.new_zeros(())
    return -gamma_sharpe * sharpe + gamma_turnover * turnover_penalty


def _compute_risk_ratio(valid_returns: Tensor, objective: str) -> Tensor:
    objective_norm = objective.strip().lower()
    mean_return = valid_returns.mean()
    annualizer = torch.sqrt(torch.as_tensor(252.0, device=valid_returns.device, dtype=valid_returns.dtype))

    if objective_norm == "sortino":
        downside = torch.minimum(valid_returns, torch.zeros_like(valid_returns))
        downside_dev = torch.sqrt(downside.pow(2).mean() + 1e-8)
        return mean_return / downside_dev * annualizer

    variance = (valid_returns - mean_return).pow(2).mean()
    std_return = torch.sqrt(variance + 1e-8)
    return mean_return / std_return * annualizer


def _compute_log_utility(valid_returns: Tensor) -> Tensor:
    """Annualized expected net log utility from canonical backtest log returns."""
    valid_returns = torch.nan_to_num(valid_returns.float(), nan=0.0, posinf=0.0, neginf=0.0)
    annualizer = torch.as_tensor(252.0, device=valid_returns.device, dtype=valid_returns.dtype)
    return valid_returns.mean() * annualizer


def _valid_row_mask_like(weights: Tensor, sample_mask: Tensor | None) -> Tensor:
    if sample_mask is None:
        return torch.ones(weights.size(0), device=weights.device, dtype=torch.bool)
    return sample_mask.to(device=weights.device, dtype=torch.bool)


def _canonical_target_weights_for_loss(
    weights: Tensor,
    *,
    long_only: bool,
    gross_leverage: float,
    min_trade_weight: float,
    portfolio_activation: str,
) -> Tensor:
    """Apply the same target-weight transform used by the canonical backtest."""
    gross_budget = float(_resolve_exposure_budget(gross_leverage))
    target = _normalize_target_weights_torch(
        torch.nan_to_num(weights, nan=0.0, posinf=0.0, neginf=0.0),
        long_only=long_only,
        gross_budget=gross_budget,
        portfolio_activation=portfolio_activation,
    )
    return _apply_min_trade_weight_torch(target, min_trade_weight)


def _resolved_penalty_weights(
    backtest_weights_history: Tensor | None,
    raw_weights: Tensor,
    *,
    long_only: bool,
    gross_leverage: float,
    min_trade_weight: float,
    portfolio_activation: str,
) -> Tensor:
    if isinstance(backtest_weights_history, torch.Tensor) and backtest_weights_history.numel() > 0:
        return torch.nan_to_num(backtest_weights_history, nan=0.0, posinf=0.0, neginf=0.0)
    return _canonical_target_weights_for_loss(
        raw_weights,
        long_only=long_only,
        gross_leverage=gross_leverage,
        min_trade_weight=min_trade_weight,
        portfolio_activation=portfolio_activation,
    )


def _portfolio_concentration_penalty(weights: Tensor, tradable: Tensor, valid_mask: Tensor) -> Tensor:
    weights_safe = torch.nan_to_num(weights, nan=0.0, posinf=0.0, neginf=0.0)
    tradable_f = tradable.to(device=weights_safe.device, dtype=weights_safe.dtype)
    valid_f = valid_mask.to(device=weights_safe.device, dtype=weights_safe.dtype)
    active_count = tradable_f.sum(dim=1).clamp_min(1.0)
    concentration = (weights_safe.pow(2) * tradable_f).sum(dim=1) * active_count
    return (concentration * valid_f).sum() / valid_f.sum().clamp_min(1.0)


def _portfolio_net_exposure_penalty(weights: Tensor, valid_mask: Tensor) -> Tensor:
    weights_safe = torch.nan_to_num(weights, nan=0.0, posinf=0.0, neginf=0.0)
    valid_f = valid_mask.to(device=weights_safe.device, dtype=weights_safe.dtype)
    net_exposure = weights_safe.sum(dim=1).pow(2)
    return (net_exposure * valid_f).sum() / valid_f.sum().clamp_min(1.0)


def _portfolio_gross_exposure_error(
    weights: Tensor,
    valid_mask: Tensor,
    *,
    gross_leverage: float,
) -> Tensor:
    weights_safe = torch.nan_to_num(weights, nan=0.0, posinf=0.0, neginf=0.0)
    valid_f = valid_mask.to(device=weights_safe.device, dtype=weights_safe.dtype)
    gross_target = float(_resolve_exposure_budget(gross_leverage))
    gross_error = (weights_safe.abs().sum(dim=1) - gross_target).pow(2)
    return (gross_error * valid_f).sum() / valid_f.sum().clamp_min(1.0)


def _dense_masked_clean_mean(values: Tensor, valid_mask: Tensor) -> tuple[Tensor, Tensor]:
    """Mean of values[valid_mask] after nan_to_num, without dynamic-shape indexing."""
    mask_bool = valid_mask.to(device=values.device, dtype=torch.bool)
    mask_f = mask_bool.to(dtype=values.dtype)
    clean = torch.nan_to_num(values, nan=0.0, posinf=0.0, neginf=0.0)
    total = (clean * mask_f).sum()
    count = mask_f.sum()
    return total / count.clamp_min(1.0), count


def _compute_excess_risk_terms(
    valid_returns: Tensor,
    valid_benchmark: Tensor,
    cvar_alpha: float,
    drawdown_target: float,
) -> tuple[Tensor, Tensor, Tensor, Tensor, Tensor]:
    excess_returns = valid_returns - valid_benchmark
    mean_excess = excess_returns.mean()

    alpha = min(max(float(cvar_alpha), 1e-6), 1.0 - 1e-6)
    losses = -excess_returns
    var_alpha = torch.quantile(losses, alpha)
    tail_excess = torch.relu(losses - var_alpha)
    cvar = var_alpha + tail_excess.mean() / (1.0 - alpha)

    rel_log_equity = torch.cumsum(excess_returns, dim=0)
    rel_equity = torch.exp(torch.clamp(rel_log_equity, -60.0, 60.0))
    running_max = torch.cummax(rel_equity, dim=0).values
    drawdowns = 1.0 - rel_equity / running_max.clamp_min(1e-12)
    mdd = drawdowns.max() if drawdowns.numel() > 0 else mean_excess.new_zeros(())
    drawdown_penalty = F.softplus((mdd - float(drawdown_target)) * 20.0) / 20.0
    return excess_returns, mean_excess, cvar, mdd, drawdown_penalty


def risk_aware_loss(
    weights: Tensor,
    future_log_returns: Tensor,
    tradable_mask: Tensor,
    benchmark_returns: Tensor | None = None,
    can_buy_mask: Tensor | None = None,
    can_sell_mask: Tensor | None = None,
    can_short_open_mask: Tensor | None = None,
    force_short_cover_mask: Tensor | None = None,
    force_exit_mask: Tensor | None = None,
    sample_mask: Tensor | None = None,
    long_only: bool = True,
    buy_fee_rate: float = 0.0,
    sell_fee_rate: float = 0.0,
    max_turnover_ratio: float = 0.0,
    volume_limit_weights: Tensor | None = None,
    short_margin_rate: Tensor | float | None = None,
    short_capacity_weights: Tensor | None = None,
    short_maintenance_ratio: float = 1.30,
    short_handling_fee_rate: Tensor | float = 0.0,
    gross_leverage: float = 1.0,
    min_trade_weight: float = 0.0,
    portfolio_activation: str = DEFAULT_PORTFOLIO_ACTIVATION,
    execution_mode: str = "naive",
    buy_fee_rates: Tensor | None = None,
    sell_fee_rates: Tensor | None = None,
    normal_sell_fee_rates: Tensor | None = None,
    day_trade_unlimited_margin_conversion: bool = False,
    day_trade_margin_financing_ratio: float = 0.60,
    day_trade_margin_financing_annual_rate: float = 0.16,
    day_trade_margin_short_handling_fee_rate: float = 0.001,
    day_trade_margin_short_annual_borrow_rate: float = 0.20,
    commission_rebate_rates: Tensor | None = None,
    commission_rebate_timing: str = "daily_close",
    session_month_ids: Tensor | None = None,
    commission_rebate_payment_eligible_mask: Tensor | None = None,
    settlement_lag_sessions: int = 2,
    day_trade_eligible_mask: Tensor | None = None,
    day_trade_can_buy_open_mask: Tensor | None = None,
    day_trade_can_sell_open_mask: Tensor | None = None,
    unresolved_corporate_action_mask: Tensor | None = None,
    cash_dividend_yield: Tensor | None = None,
    cash_dividend_payment_delay_sessions: Tensor | None = None,
    claim_queue_sessions: int | None = None,
    state_advance_mask: Tensor | None = None,
    symbol_indices: Tensor | None = None,
    gamma_sharpe: float = 1.0,
    gamma_excess: float = 1.0,
    gamma_cvar: float = 1.0,
    cvar_alpha: float = 0.95,
    gamma_drawdown: float = 0.0,
    drawdown_target: float = 0.2,
    gamma_turnover: float = 0.1,
    gamma_underperformance: float = 1.0,
    excess_target: float = 0.0,
    cvar_budget: float = 0.03,
    drawdown_budget: float = 0.2,
    turnover_budget: float = 0.3,
    gamma_cvar_budget: float = 1.0,
    gamma_drawdown_budget: float = 1.0,
    gamma_turnover_budget: float = 0.0,
    objective: str = "sharpe",
    aux_outputs: dict[str, Tensor] | None = None,
    rank_ic_weight: float = 1.0,
    return_rank_ic_weight: float = 0.0,
    direction_weight: float = 0.05,
    volatility_regime_weight: float = 0.05,
    concentration_weight: float = 0.005,
    net_exposure_weight: float = 0.0,
    regime_up_threshold: float = 0.002,
    regime_down_threshold: float = -0.002,
    factor_slope_tstat_weight: float = 1.0,
    factor_rank_ic_weight: float = 0.5,
    factor_sharpe_weight: float = 0.25,
    factor_block_stability_weight: float = 0.20,
    factor_regime_stability_weight: float = 0.20,
    factor_consistency_weight: float = 0.05,
    factor_net_exposure_weight: float = 0.05,
    factor_gross_exposure_weight: float = 0.02,
    factor_concentration_weight: float = 0.02,
    factor_turnover_weight: float = 0.02,
    factor_score_l2_weight: float = 0.001,
    factor_temperature: float = 1.0,
    factor_block_count: int = 4,
    factor_worst_fraction: float = 0.25,
    autoencoder_lambda_turnover: float = 0.1,
    autoencoder_lambda_concentration: float = 0.01,
    autoencoder_lambda_latent: float = 0.001,
    overnight_log_returns: Tensor | None = None,
    can_short_open_open_mask: Tensor | None = None,
    day_trade_execution_initial_capital: float = 1_000_000.0,
) -> Tensor:
    """Risk-aware loss with configurable objective, including excess-CVaR-drawdown."""
    normalize_start = _loss_timer_start()
    weights = torch.nan_to_num(weights, nan=0.0, posinf=0.0, neginf=0.0)
    _loss_timer_stop("normalize_weights", normalize_start)

    prepare_start = _loss_timer_start()
    mode = normalize_execution_mode(execution_mode)
    execution_returns, returns = _execution_and_safe_returns(
        future_log_returns,
        reference=weights,
        execution_mode=mode,
    )
    tradable = tradable_mask.to(dtype=torch.bool, device=weights.device)
    objective_norm = objective.strip().lower()
    if mode == "tw_index_derivatives_day":
        if weights.dim() != 2 or int(weights.size(1)) != TAIFEX_INDEX_DERIVATIVE_ACTION_COUNT_V4:
            raise ValueError(
                "tw_index_derivatives_day requires direct model actions "
                "[T,D_derivatives], "
                f"got {tuple(weights.shape)}"
            )
        if objective_norm not in {
            "log_utility",
            "log_util",
            "kelly",
            "growth",
            "mean_log_return",
        }:
            raise ValueError(
                "tw_index_derivatives_day supports only canonical log utility"
            )
        if overnight_log_returns is None:
            raise ValueError(
                "tw_index_derivatives_day requires aligned derivative simple returns"
            )
    if mode == "tw_index_futures_day":
        if weights.dim() != 2 or int(weights.size(1)) != TAIFEX_INDEX_FUTURES_ACTION_COUNT:
            raise ValueError(
                "tw_index_futures_day requires direct model actions [T,18], "
                f"got {tuple(weights.shape)}"
            )
        if objective_norm not in {
            "log_utility",
            "log_util",
            "kelly",
            "growth",
            "mean_log_return",
        }:
            raise ValueError(
                "tw_index_futures_day supports only canonical log utility"
            )
        if overnight_log_returns is None or tuple(overnight_log_returns.shape) != (
            int(weights.size(0)),
            TAIFEX_INDEX_FUTURES_ACTION_COUNT,
            3,
        ):
            raise ValueError(
                "tw_index_futures_day requires exact execution tensor [T,18,3]"
            )
    if mode in TW_CARRYING_EXECUTION_MODES:
        phase_actions = mode == "tw_overnight" or weights.dim() == 3
        expected_channels = 2 if mode == "tw_cash" else 3
        if phase_actions and (
            weights.dim() != 3 or int(weights.size(1)) != expected_channels
        ):
            raise ValueError(
                f"{mode} requires model actions with shape "
                f"[T,{expected_channels},S], got {tuple(weights.shape)}"
            )
        if not phase_actions and (mode != "tw_cash" or weights.dim() != 2):
            raise ValueError(
                f"{mode} received an unsupported action shape {tuple(weights.shape)}"
            )
        if phase_actions and objective_norm not in {
            "log_utility",
            "log_util",
            "kelly",
            "growth",
            "mean_log_return",
        }:
            raise ValueError(
                f"{mode} currently supports only canonical log-utility "
                "objectives; phase logits do not have a single unbiased "
                "cross-sectional rank label"
            )
        if phase_actions and overnight_log_returns is None:
            raise ValueError(
                f"{mode} requires prior-close-to-open overnight_log_returns"
            )
    _loss_timer_stop("prepare_inputs", prepare_start)

    if objective_norm in {"portfolio_autoencoder", "bottleneck_portfolio_autoencoder", "autoencoder_portfolio"}:
        return portfolio_autoencoder_loss(
            weights,
            execution_returns,
            tradable,
            benchmark_returns=benchmark_returns,
            can_buy_mask=can_buy_mask,
            can_sell_mask=can_sell_mask,
            can_short_open_mask=can_short_open_mask,
            can_short_open_open_mask=can_short_open_open_mask,
            force_short_cover_mask=force_short_cover_mask,
            force_exit_mask=force_exit_mask,
            sample_mask=sample_mask,
            long_only=long_only,
            buy_fee_rate=buy_fee_rate,
            sell_fee_rate=sell_fee_rate,
            max_turnover_ratio=max_turnover_ratio,
            volume_limit_weights=volume_limit_weights,
            short_margin_rate=short_margin_rate,
            short_capacity_weights=short_capacity_weights,
            short_maintenance_ratio=short_maintenance_ratio,
            short_handling_fee_rate=short_handling_fee_rate,
            gross_leverage=gross_leverage,
            min_trade_weight=min_trade_weight,
            portfolio_activation=portfolio_activation,
            execution_mode=execution_mode,
            buy_fee_rates=buy_fee_rates,
            sell_fee_rates=sell_fee_rates,
            normal_sell_fee_rates=normal_sell_fee_rates,
            commission_rebate_rates=commission_rebate_rates,
            commission_rebate_timing=commission_rebate_timing,
            session_month_ids=session_month_ids,
            commission_rebate_payment_eligible_mask=(
                commission_rebate_payment_eligible_mask
            ),
            settlement_lag_sessions=settlement_lag_sessions,
            day_trade_eligible_mask=day_trade_eligible_mask,
            day_trade_can_buy_open_mask=day_trade_can_buy_open_mask,
            day_trade_can_sell_open_mask=day_trade_can_sell_open_mask,
            unresolved_corporate_action_mask=unresolved_corporate_action_mask,
            cash_dividend_yield=cash_dividend_yield,
            cash_dividend_payment_delay_sessions=(
                cash_dividend_payment_delay_sessions
            ),
            claim_queue_sessions=claim_queue_sessions,
            state_advance_mask=state_advance_mask,
            symbol_indices=symbol_indices,
            aux_outputs=aux_outputs,
            autoencoder_lambda_turnover=autoencoder_lambda_turnover,
            autoencoder_lambda_concentration=autoencoder_lambda_concentration,
            autoencoder_lambda_latent=autoencoder_lambda_latent,
        )

    if objective_norm in {"factor_generalization", "factor", "factor_ic", "characteristic_factor"}:
        return factor_generalization_loss(
            weights,
            execution_returns,
            tradable,
            benchmark_returns=benchmark_returns,
            can_buy_mask=can_buy_mask,
            can_sell_mask=can_sell_mask,
            can_short_open_mask=can_short_open_mask,
            can_short_open_open_mask=can_short_open_open_mask,
            force_short_cover_mask=force_short_cover_mask,
            force_exit_mask=force_exit_mask,
            sample_mask=sample_mask,
            long_only=long_only,
            buy_fee_rate=buy_fee_rate,
            sell_fee_rate=sell_fee_rate,
            max_turnover_ratio=max_turnover_ratio,
            volume_limit_weights=volume_limit_weights,
            short_margin_rate=short_margin_rate,
            short_capacity_weights=short_capacity_weights,
            short_maintenance_ratio=short_maintenance_ratio,
            short_handling_fee_rate=short_handling_fee_rate,
            gross_leverage=gross_leverage,
            min_trade_weight=min_trade_weight,
            portfolio_activation=portfolio_activation,
            execution_mode=execution_mode,
            buy_fee_rates=buy_fee_rates,
            sell_fee_rates=sell_fee_rates,
            normal_sell_fee_rates=normal_sell_fee_rates,
            commission_rebate_rates=commission_rebate_rates,
            commission_rebate_timing=commission_rebate_timing,
            session_month_ids=session_month_ids,
            commission_rebate_payment_eligible_mask=(
                commission_rebate_payment_eligible_mask
            ),
            settlement_lag_sessions=settlement_lag_sessions,
            day_trade_eligible_mask=day_trade_eligible_mask,
            day_trade_can_buy_open_mask=day_trade_can_buy_open_mask,
            day_trade_can_sell_open_mask=day_trade_can_sell_open_mask,
            unresolved_corporate_action_mask=unresolved_corporate_action_mask,
            cash_dividend_yield=cash_dividend_yield,
            cash_dividend_payment_delay_sessions=(
                cash_dividend_payment_delay_sessions
            ),
            claim_queue_sessions=claim_queue_sessions,
            state_advance_mask=state_advance_mask,
            symbol_indices=symbol_indices,
            aux_outputs=aux_outputs,
            slope_tstat_weight=factor_slope_tstat_weight,
            rank_ic_weight=factor_rank_ic_weight,
            factor_sharpe_weight=factor_sharpe_weight,
            block_stability_weight=factor_block_stability_weight,
            regime_stability_weight=factor_regime_stability_weight,
            consistency_weight=factor_consistency_weight,
            net_exposure_weight=factor_net_exposure_weight,
            gross_exposure_weight=factor_gross_exposure_weight,
            concentration_weight=factor_concentration_weight,
            turnover_weight=factor_turnover_weight,
            score_l2_weight=factor_score_l2_weight,
            factor_temperature=factor_temperature,
            block_count=factor_block_count,
            worst_fraction=factor_worst_fraction,
            regime_up_threshold=regime_up_threshold,
            regime_down_threshold=regime_down_threshold,
        )

    rank_tradable = _apply_sample_mask_to_asset_mask(
        tradable & torch.isfinite(execution_returns),
        sample_mask,
    )

    if objective_norm in {"pure_rank", "rank_only", "score_rank"}:
        rank_logits = _resolve_rank_scores(weights, aux_outputs)
        return float(rank_ic_weight) * masked_ic_loss(rank_logits, returns, rank_tradable)

    if objective_norm in {"rank", "rank_ic", "ic", "multitask_rank_ic"}:
        total_loss = returns.new_zeros(())

        rank_logits = _resolve_rank_scores(weights, aux_outputs)
        total_loss = total_loss + float(rank_ic_weight) * masked_ic_loss(rank_logits, returns, rank_tradable)

        if aux_outputs:
            score_logits = aux_outputs.get("score_logits")
            if score_logits is not None and float(direction_weight) > 0.0:
                direction_target = (returns > 0.0).to(dtype=score_logits.dtype)
                mask_f = rank_tradable.to(dtype=score_logits.dtype)
                bce = F.binary_cross_entropy_with_logits(score_logits, direction_target, reduction="none")
                total_loss = total_loss + float(direction_weight) * (bce * mask_f).sum() / mask_f.sum().clamp_min(1.0)

            volatility_pred = aux_outputs.get("volatility_pred")
            if volatility_pred is not None and float(volatility_regime_weight) > 0.0:
                vol_target = returns.abs()
                mask_f = rank_tradable.to(dtype=volatility_pred.dtype)
                vol_loss = ((volatility_pred - vol_target).pow(2) * mask_f).sum() / mask_f.sum().clamp_min(1.0)
                total_loss = total_loss + float(volatility_regime_weight) * vol_loss

            regime_logits = aux_outputs.get("regime_logits")
            if regime_logits is not None and float(volatility_regime_weight) > 0.0:
                if benchmark_returns is None:
                    tradable_f = tradable.to(dtype=returns.dtype)
                    bench_for_regime = (returns * tradable_f).sum(dim=1) / tradable_f.sum(dim=1).clamp_min(1.0)
                else:
                    bench_for_regime = torch.nan_to_num(
                        benchmark_returns.to(device=weights.device, dtype=returns.dtype),
                        nan=0.0,
                        posinf=0.0,
                        neginf=0.0,
                    )
                down_th = float(regime_down_threshold)
                up_th = float(regime_up_threshold)
                regime_target = torch.full_like(bench_for_regime, fill_value=1, dtype=torch.long)
                regime_target = torch.where(bench_for_regime <= down_th, torch.zeros_like(regime_target), regime_target)
                regime_target = torch.where(bench_for_regime >= up_th, torch.full_like(regime_target, 2), regime_target)
                regime_ce = F.cross_entropy(regime_logits, regime_target, reduction="none")
                valid_rows_f = rank_tradable.any(dim=1).to(dtype=regime_ce.dtype)
                total_loss = total_loss + float(volatility_regime_weight) * (
                    (regime_ce * valid_rows_f).sum() / valid_rows_f.sum().clamp_min(1.0)
                )

        return total_loss

    build_orders_start = _loss_timer_start()
    can_buy = (
        can_buy_mask.to(dtype=torch.bool, device=weights.device)
        if can_buy_mask is not None
        else tradable
    )
    if benchmark_returns is None:
        tradable_f = tradable.to(dtype=returns.dtype)
        denom = tradable_f.sum(dim=1).clamp_min(1.0)
        benchmark = (returns * tradable_f).sum(dim=1) / denom
    else:
        benchmark = torch.nan_to_num(
            benchmark_returns.to(device=weights.device, dtype=returns.dtype),
            nan=0.0,
            posinf=0.0,
            neginf=0.0,
        )
    _loss_timer_stop("build_orders", build_orders_start)

    initial_weights = None
    initial_alive = None
    initial_cash = None
    initial_payables = None
    initial_receivables = None
    initial_commission_rebate_current = None
    initial_commission_rebate_due = None
    initial_commission_rebate_month_id = None
    initial_equity_scale = None
    initial_short_sale_collateral = None
    initial_short_margin_collateral = None
    if aux_outputs:
        initial_weights = aux_outputs.get("initial_weights")
        if isinstance(initial_weights, torch.Tensor):
            # Keep a private, contiguous buffer for recurrent state so we don't
            # hold references to graph-managed outputs across compiled replays.
            initial_weights = _clone_portfolio_state_for_loss(
                initial_weights,
                stat_prefix="initial_weights_clone",
            )
        initial_alive = aux_outputs.get("initial_alive")
        if isinstance(initial_alive, torch.Tensor):
            initial_alive = _clone_portfolio_state_for_loss(
                initial_alive,
                stat_prefix="initial_alive_clone",
            )
        initial_cash = aux_outputs.get("initial_cash")
        if isinstance(initial_cash, torch.Tensor):
            initial_cash = _clone_portfolio_state_for_loss(
                initial_cash,
                stat_prefix="initial_cash_clone",
            )
        initial_payables = aux_outputs.get("initial_payables")
        if isinstance(initial_payables, torch.Tensor):
            initial_payables = _clone_portfolio_state_for_loss(
                initial_payables,
                stat_prefix="initial_payables_clone",
            )
        initial_receivables = aux_outputs.get("initial_receivables")
        if isinstance(initial_receivables, torch.Tensor):
            initial_receivables = _clone_portfolio_state_for_loss(
                initial_receivables,
                stat_prefix="initial_receivables_clone",
            )
        initial_commission_rebate_current = aux_outputs.get(
            "initial_commission_rebate_current"
        )
        if isinstance(initial_commission_rebate_current, torch.Tensor):
            initial_commission_rebate_current = _clone_portfolio_state_for_loss(
                initial_commission_rebate_current,
                stat_prefix="initial_commission_rebate_current_clone",
            )
        initial_commission_rebate_due = aux_outputs.get(
            "initial_commission_rebate_due"
        )
        if isinstance(initial_commission_rebate_due, torch.Tensor):
            initial_commission_rebate_due = _clone_portfolio_state_for_loss(
                initial_commission_rebate_due,
                stat_prefix="initial_commission_rebate_due_clone",
            )
        initial_commission_rebate_month_id = aux_outputs.get(
            "initial_commission_rebate_month_id"
        )
        if isinstance(initial_commission_rebate_month_id, torch.Tensor):
            initial_commission_rebate_month_id = (
                _clone_portfolio_state_for_loss(
                    initial_commission_rebate_month_id,
                    stat_prefix="initial_commission_rebate_month_id_clone",
                )
            )
        initial_equity_scale = aux_outputs.get("initial_equity_scale")
        if isinstance(initial_equity_scale, torch.Tensor):
            initial_equity_scale = _clone_portfolio_state_for_loss(
                initial_equity_scale,
                stat_prefix="initial_equity_scale_clone",
            )
        initial_short_sale_collateral = aux_outputs.get(
            "initial_short_sale_collateral"
        )
        if isinstance(initial_short_sale_collateral, torch.Tensor):
            initial_short_sale_collateral = _clone_portfolio_state_for_loss(
                initial_short_sale_collateral,
                stat_prefix="initial_short_sale_collateral_clone",
            )
        initial_short_margin_collateral = aux_outputs.get(
            "initial_short_margin_collateral"
        )
        if isinstance(initial_short_margin_collateral, torch.Tensor):
            initial_short_margin_collateral = _clone_portfolio_state_for_loss(
                initial_short_margin_collateral,
                stat_prefix="initial_short_margin_collateral_clone",
            )

    backtest_start = _loss_timer_start()
    needs_penalty_weights = float(concentration_weight) > 0.0 or float(net_exposure_weight) > 0.0
    backtest = run_backtest_torch(
        weights,
        execution_returns,
        tradable,
        benchmark,
        buy_fee_rate,
        sell_fee_rate,
        long_only=long_only,
        max_turnover_ratio=max_turnover_ratio,
        volume_limit_weights=volume_limit_weights,
        short_margin_rate=short_margin_rate,
        short_capacity_weights=short_capacity_weights,
        short_maintenance_ratio=short_maintenance_ratio,
        short_handling_fee_rate=short_handling_fee_rate,
        gross_leverage=gross_leverage,
        min_trade_weight=min_trade_weight,
        portfolio_activation=portfolio_activation,
        can_buy_mask=can_buy,
        can_sell_mask=can_sell_mask.to(dtype=torch.bool, device=weights.device) if can_sell_mask is not None else None,
        can_short_open_mask=(
            can_short_open_mask.to(dtype=torch.bool, device=weights.device)
            if can_short_open_mask is not None
            else None
        ),
        can_short_open_open_mask=(
            can_short_open_open_mask.to(dtype=torch.bool, device=weights.device)
            if can_short_open_open_mask is not None
            else None
        ),
        force_short_cover_mask=(
            force_short_cover_mask.to(dtype=torch.bool, device=weights.device)
            if force_short_cover_mask is not None
            else None
        ),
        force_exit_mask=(
            force_exit_mask.to(dtype=torch.bool, device=weights.device)
            if force_exit_mask is not None
            else None
        ),
        return_weights_history=needs_penalty_weights,
        initial_weights=initial_weights,
        initial_alive=initial_alive,
        execution_mode=execution_mode,
        buy_fee_rates=buy_fee_rates,
        sell_fee_rates=sell_fee_rates,
        normal_sell_fee_rates=normal_sell_fee_rates,
        day_trade_unlimited_margin_conversion=(
            day_trade_unlimited_margin_conversion
        ),
        day_trade_margin_financing_ratio=day_trade_margin_financing_ratio,
        day_trade_margin_financing_annual_rate=(
            day_trade_margin_financing_annual_rate
        ),
        day_trade_margin_short_handling_fee_rate=(
            day_trade_margin_short_handling_fee_rate
        ),
        day_trade_margin_short_annual_borrow_rate=(
            day_trade_margin_short_annual_borrow_rate
        ),
        commission_rebate_rates=commission_rebate_rates,
        commission_rebate_timing=commission_rebate_timing,
        session_month_ids=session_month_ids,
        commission_rebate_payment_eligible_mask=(
            commission_rebate_payment_eligible_mask
        ),
        settlement_lag_sessions=settlement_lag_sessions,
        day_trade_eligible_mask=day_trade_eligible_mask,
        day_trade_can_buy_open_mask=day_trade_can_buy_open_mask,
        day_trade_can_sell_open_mask=day_trade_can_sell_open_mask,
        unresolved_corporate_action_mask=unresolved_corporate_action_mask,
        cash_dividend_yield=cash_dividend_yield,
        cash_dividend_payment_delay_sessions=(
            cash_dividend_payment_delay_sessions
        ),
        claim_queue_sessions=claim_queue_sessions,
        state_advance_mask=state_advance_mask if state_advance_mask is not None else sample_mask,
        initial_cash=initial_cash,
        initial_payables=initial_payables,
        initial_receivables=initial_receivables,
        initial_commission_rebate_current=initial_commission_rebate_current,
        initial_commission_rebate_due=initial_commission_rebate_due,
        initial_commission_rebate_month_id=initial_commission_rebate_month_id,
        initial_equity_scale=initial_equity_scale,
        initial_short_sale_collateral=initial_short_sale_collateral,
        initial_short_margin_collateral=initial_short_margin_collateral,
        symbol_indices=symbol_indices,
        overnight_returns=overnight_log_returns,
        day_trade_execution_initial_capital=day_trade_execution_initial_capital,
    )
    _loss_timer_stop("backtest", backtest_start)

    if aux_outputs is not None and backtest.final_weights is not None:
        state_update_start = _loss_timer_start()
        # Avoid carrying graph-owned output storage into the next step.
        aux_outputs["_final_weights"] = _clone_portfolio_state_for_loss(
            backtest.final_weights,
            stat_prefix="final_weights_clone",
        )
        if backtest.final_alive is not None:
            aux_outputs["_final_alive"] = _clone_portfolio_state_for_loss(
                backtest.final_alive,
                stat_prefix="final_alive_clone",
            )
        for public_name, private_name, value in (
            ("cash", "_final_cash", backtest.final_cash),
            ("payables", "_final_payables", backtest.final_payables),
            ("receivables", "_final_receivables", backtest.final_receivables),
            (
                "commission_rebate_current",
                "_final_commission_rebate_current",
                backtest.final_commission_rebate_current,
            ),
            (
                "commission_rebate_due",
                "_final_commission_rebate_due",
                backtest.final_commission_rebate_due,
            ),
            (
                "commission_rebate_month_id",
                "_final_commission_rebate_month_id",
                backtest.final_commission_rebate_month_id,
            ),
            ("equity_scale", "_final_equity_scale", backtest.final_equity_scale),
            (
                "short_sale_collateral",
                "_final_short_sale_collateral",
                backtest.final_short_sale_collateral,
            ),
            (
                "short_margin_collateral",
                "_final_short_margin_collateral",
                backtest.final_short_margin_collateral,
            ),
        ):
            if value is not None:
                aux_outputs[private_name] = _clone_portfolio_state_for_loss(
                    value,
                    stat_prefix=f"final_{public_name}_clone",
                )
        _loss_timer_stop("state_update", state_update_start)

    postprocess_start = _loss_timer_start()
    if sample_mask is None:
        valid_mask = torch.ones(weights.size(0), device=weights.device, dtype=torch.bool)
    else:
        valid_mask = sample_mask.to(device=weights.device, dtype=torch.bool)
    _loss_timer_stop("returns_postprocess", postprocess_start)

    dense_valid_count: Tensor | None = None
    if objective_norm in {"log_utility", "log_util", "kelly", "growth", "mean_log_return"}:
        graph_start = _loss_timer_start()
        nan_start = _loss_timer_start()
        clean_returns = torch.nan_to_num(backtest.strategy_returns.float(), nan=0.0, posinf=0.0, neginf=0.0)
        _loss_timer_stop("nan_to_num", nan_start)

        mask_start = _loss_timer_start()
        valid_f = valid_mask.to(dtype=clean_returns.dtype)
        valid_count = valid_f.sum()
        masked_returns = clean_returns * valid_f
        _loss_timer_stop("mask_apply", mask_start)

        reduce_start = _loss_timer_start()
        mean_return = masked_returns.sum() / valid_count.clamp_min(1.0)
        annualizer = torch.as_tensor(252.0, device=clean_returns.device, dtype=clean_returns.dtype)
        _loss_timer_stop("reduce", reduce_start)

        log_utility_start = _loss_timer_start()
        objective_value = -float(gamma_sharpe) * (mean_return * annualizer)
        _loss_timer_stop("log_utility", log_utility_start)

        if gamma_turnover == 0.0:
            turnover_term = clean_returns.new_zeros(())
        else:
            turnover_nan_start = _loss_timer_start()
            clean_turnovers = torch.nan_to_num(backtest.turnovers.float(), nan=0.0, posinf=0.0, neginf=0.0)
            _loss_timer_stop("nan_to_num", turnover_nan_start)
            turnover_reduce_start = _loss_timer_start()
            turnover_mean = (clean_turnovers * valid_f).sum() / valid_count.clamp_min(1.0)
            turnover_term = gamma_turnover * turnover_mean
            _loss_timer_stop("reduce", turnover_reduce_start)

        total_loss = objective_value + turnover_term
        if concentration_weight > 0.0:
            penalty_weights = _resolved_penalty_weights(
                backtest.weights_history,
                weights,
                long_only=long_only,
                gross_leverage=gross_leverage,
                min_trade_weight=min_trade_weight,
                portfolio_activation=portfolio_activation,
            )
            total_loss = total_loss + float(concentration_weight) * _portfolio_concentration_penalty(
                penalty_weights,
                tradable,
                valid_mask,
            )
        if net_exposure_weight > 0.0:
            penalty_weights = _resolved_penalty_weights(
                backtest.weights_history,
                weights,
                long_only=long_only,
                gross_leverage=gross_leverage,
                min_trade_weight=min_trade_weight,
                portfolio_activation=portfolio_activation,
            )
            total_loss = total_loss + float(net_exposure_weight) * _portfolio_net_exposure_penalty(
                penalty_weights,
                valid_mask,
            )
        if float(return_rank_ic_weight) > 0.0:
            rank_logits = _resolve_rank_scores(weights, aux_outputs)
            total_loss = total_loss + float(return_rank_ic_weight) * masked_ic_loss(
                rank_logits,
                returns,
                rank_tradable,
            )
        valid_returns = clean_returns
        dense_valid_count = valid_count
    else:
        valid_returns = backtest.strategy_returns[valid_mask]
        valid_returns = torch.nan_to_num(valid_returns, nan=0.0, posinf=0.0, neginf=0.0)
        if valid_returns.numel() == 0:
            return weights.sum() * 0.0

        graph_start = _loss_timer_start()
        if objective_norm in {
        "excess_cvar_drawdown",
        "cvar",
        "cvar_drawdown",
        "excess_cvar",
        "outperformance_risk_budget",
        "outperformance_budget",
        "outperformance_first",
        }:
            valid_benchmark = backtest.benchmark_returns[valid_mask]
            valid_benchmark = torch.nan_to_num(valid_benchmark, nan=0.0, posinf=0.0, neginf=0.0)
            _, mean_excess, cvar, mdd, drawdown_penalty = _compute_excess_risk_terms(
                valid_returns,
                valid_benchmark,
                cvar_alpha=cvar_alpha,
                drawdown_target=drawdown_target,
            )

            if objective_norm in {"outperformance_risk_budget", "outperformance_budget", "outperformance_first"}:
                underperformance = torch.relu(float(excess_target) - (valid_returns - valid_benchmark)).mean()
                turnover_mean = backtest.turnovers[valid_mask]
                turnover_mean = torch.nan_to_num(turnover_mean, nan=0.0, posinf=0.0, neginf=0.0)
                turnover_mean = turnover_mean.mean() if turnover_mean.numel() > 0 else mean_excess.new_zeros(())

                cvar_budget_penalty = torch.relu(cvar - float(cvar_budget))
                drawdown_budget_penalty = torch.relu(mdd - float(drawdown_budget))
                turnover_budget_penalty = torch.relu(turnover_mean - float(turnover_budget))

                objective_value = (
                    -gamma_excess * mean_excess
                    + gamma_underperformance * underperformance
                    + gamma_cvar_budget * cvar_budget_penalty
                    + gamma_drawdown_budget * drawdown_budget_penalty
                    + gamma_turnover_budget * turnover_budget_penalty
                    + gamma_cvar * cvar
                    + gamma_drawdown * drawdown_penalty
                )
            else:
                objective_value = (
                    -gamma_excess * mean_excess
                    + gamma_cvar * cvar
                    + gamma_drawdown * drawdown_penalty
                )
        else:
            risk_ratio = _compute_risk_ratio(valid_returns, objective)
            objective_value = -gamma_sharpe * risk_ratio

        if gamma_turnover == 0.0:
            turnover_term = valid_returns.new_zeros(())
        else:
            valid_turnovers = backtest.turnovers[valid_mask]
            valid_turnovers = torch.nan_to_num(valid_turnovers, nan=0.0, posinf=0.0, neginf=0.0)
            turnover_term = gamma_turnover * (valid_turnovers.mean() if valid_turnovers.numel() > 0 else valid_returns.new_zeros(()))

        total_loss = objective_value + turnover_term

        if concentration_weight > 0.0:
            penalty_weights = _resolved_penalty_weights(
                backtest.weights_history,
                weights,
                long_only=long_only,
                gross_leverage=gross_leverage,
                min_trade_weight=min_trade_weight,
                portfolio_activation=portfolio_activation,
            )
            total_loss = total_loss + float(concentration_weight) * _portfolio_concentration_penalty(
                penalty_weights,
                tradable,
                valid_mask,
            )
        if net_exposure_weight > 0.0:
            penalty_weights = _resolved_penalty_weights(
                backtest.weights_history,
                weights,
                long_only=long_only,
                gross_leverage=gross_leverage,
                min_trade_weight=min_trade_weight,
                portfolio_activation=portfolio_activation,
            )
            total_loss = total_loss + float(net_exposure_weight) * _portfolio_net_exposure_penalty(
                penalty_weights,
                valid_mask,
            )
        if float(return_rank_ic_weight) > 0.0:
            rank_logits = _resolve_rank_scores(weights, aux_outputs)
            total_loss = total_loss + float(return_rank_ic_weight) * masked_ic_loss(
                rank_logits,
                returns,
                rank_tradable,
            )

    if not aux_outputs:
        if dense_valid_count is not None:
            total_loss = torch.where(dense_valid_count > 0.0, total_loss, weights.sum() * 0.0)
        _loss_timer_stop("autograd_graph_build", graph_start)
        return total_loss

    aux_loss = valid_returns.new_zeros(())

    score_logits = aux_outputs.get("score_logits")
    if score_logits is not None and float(direction_weight) > 0.0:
        direction_target = (returns > 0.0).to(dtype=score_logits.dtype)
        mask_f = rank_tradable.to(dtype=score_logits.dtype)
        bce = F.binary_cross_entropy_with_logits(score_logits, direction_target, reduction="none")
        direction_loss = (bce * mask_f).sum() / mask_f.sum().clamp_min(1.0)
        aux_loss = aux_loss + float(direction_weight) * direction_loss

    volatility_pred = aux_outputs.get("volatility_pred")
    regime_logits = aux_outputs.get("regime_logits")
    vol_regime_loss = valid_returns.new_zeros(())
    has_vol_regime = False

    if volatility_pred is not None:
        vol_target = returns.abs()
        mask_f = rank_tradable.to(dtype=volatility_pred.dtype)
        vol_loss = ((volatility_pred - vol_target).pow(2) * mask_f).sum() / mask_f.sum().clamp_min(1.0)
        vol_regime_loss = vol_regime_loss + vol_loss
        has_vol_regime = True

    if regime_logits is not None:
        if benchmark_returns is None:
            tradable_f = tradable.to(dtype=returns.dtype)
            bench_for_regime = (returns * tradable_f).sum(dim=1) / tradable_f.sum(dim=1).clamp_min(1.0)
        else:
            bench_for_regime = benchmark.to(dtype=returns.dtype)

        down_th = float(regime_down_threshold)
        up_th = float(regime_up_threshold)
        regime_target = torch.full_like(bench_for_regime, fill_value=1, dtype=torch.long)
        regime_target = torch.where(bench_for_regime <= down_th, torch.zeros_like(regime_target), regime_target)
        regime_target = torch.where(bench_for_regime >= up_th, torch.full_like(regime_target, 2), regime_target)
        regime_ce_per_row = F.cross_entropy(regime_logits, regime_target, reduction="none")
        valid_rows_f = rank_tradable.any(dim=1).to(dtype=regime_ce_per_row.dtype)
        regime_ce = (regime_ce_per_row * valid_rows_f).sum() / valid_rows_f.sum().clamp_min(1.0)
        vol_regime_loss = vol_regime_loss + regime_ce
        has_vol_regime = True

    if has_vol_regime and float(volatility_regime_weight) > 0.0:
        aux_loss = aux_loss + float(volatility_regime_weight) * vol_regime_loss

    total_loss = total_loss + aux_loss
    if dense_valid_count is not None:
        total_loss = torch.where(dense_valid_count > 0.0, total_loss, weights.sum() * 0.0)
    _loss_timer_stop("autograd_graph_build", graph_start)
    return total_loss
