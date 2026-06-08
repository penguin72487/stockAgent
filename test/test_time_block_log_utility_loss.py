import torch

from stockagent.training.loss import risk_aware_loss
from stockagent.training.trainer import _detach_portfolio_state


def _log_utility_loss(
    weights: torch.Tensor,
    returns: torch.Tensor,
    mask: torch.Tensor,
    benchmark: torch.Tensor,
    aux_outputs: dict[str, torch.Tensor] | None = None,
) -> torch.Tensor:
    return risk_aware_loss(
        weights,
        returns,
        mask,
        benchmark_returns=benchmark,
        can_buy_mask=mask,
        can_sell_mask=mask,
        long_only=False,
        buy_fee_rate=0.001,
        sell_fee_rate=0.003,
        max_turnover_ratio=0.0,
        gross_leverage=1.0,
        gamma_sharpe=1.0,
        gamma_excess=0.0,
        gamma_cvar=0.0,
        gamma_drawdown=0.0,
        gamma_turnover=0.0,
        gamma_underperformance=0.0,
        gamma_cvar_budget=0.0,
        gamma_drawdown_budget=0.0,
        gamma_turnover_budget=0.0,
        objective="log_utility",
        aux_outputs=aux_outputs,
    )


def test_sequential_log_utility_chunk_state_matches_full_loss() -> None:
    torch.manual_seed(23)
    rows, symbols = 6, 5
    raw = torch.randn(rows, symbols)
    weights = torch.tanh(raw)
    weights = weights / weights.abs().sum(dim=1, keepdim=True).clamp_min(1e-6)
    returns = torch.randn(rows, symbols) * 0.01
    mask = torch.ones(rows, symbols, dtype=torch.bool)
    benchmark = returns.mean(dim=1)

    full_loss = _log_utility_loss(weights, returns, mask, benchmark, aux_outputs={})

    aux1: dict[str, torch.Tensor] = {}
    loss1 = _log_utility_loss(weights[:3], returns[:3], mask[:3], benchmark[:3], aux_outputs=aux1)
    prev = _detach_portfolio_state(aux1["_final_weights"])
    aux2: dict[str, torch.Tensor] = {"initial_weights": prev}
    loss2 = _log_utility_loss(weights[3:], returns[3:], mask[3:], benchmark[3:], aux_outputs=aux2)
    chunked_loss = (loss1 * 3.0 + loss2 * 3.0) / 6.0

    assert prev is not None
    assert prev.data_ptr() != aux1["_final_weights"].data_ptr()
    assert torch.allclose(chunked_loss, full_loss, atol=1e-7, rtol=1e-6)
