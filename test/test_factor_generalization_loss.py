#!/usr/bin/env python3
"""Smoke test for the characteristic-factor generalization loss."""

import torch

from stockagent.models.efficient_tcn_tabular_set_portfolio import EfficientTCNTabularSetPortfolioModel
from stockagent.training.loss import risk_aware_loss


def main() -> None:
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    torch.manual_seed(11)
    if device.type == "cuda":
        torch.cuda.manual_seed_all(11)

    batch, lookback, symbols, features = 8, 10, 40, 12
    model = EfficientTCNTabularSetPortfolioModel(
        lookback=lookback,
        num_features=features,
        num_symbols=symbols,
        temporal_dim=8,
        temporal_hidden_channels=16,
        tabular_dim=32,
        tabular_hidden_dim=64,
        tabular_blocks=1,
        model_dim=32,
        num_inducing_points=8,
        num_heads=4,
        head_hidden_dim=32,
        portfolio_mode="long_short",
        return_aux=True,
    ).to(device)
    x = torch.randn(batch, lookback, symbols, features, device=device)
    mask = torch.ones(batch, symbols, dtype=torch.bool, device=device)
    returns = torch.randn(batch, symbols, device=device) * 0.01
    benchmark = returns.mean(dim=1)

    output = model(x, mask)
    weights = output["weights"]
    aux = dict(output)
    aug_output = model(x + torch.randn_like(x) * 0.01, mask)
    aux["aug_score_logits"] = aug_output["score_logits"]

    loss = risk_aware_loss(
        weights,
        returns,
        mask,
        benchmark_returns=benchmark,
        can_buy_mask=mask,
        can_sell_mask=mask,
        long_only=False,
        buy_fee_rate=0.0005,
        sell_fee_rate=0.0005,
        objective="factor_generalization",
        aux_outputs=aux,
        factor_slope_tstat_weight=1.0,
        factor_rank_ic_weight=0.5,
        factor_sharpe_weight=0.25,
        factor_block_stability_weight=0.20,
        factor_regime_stability_weight=0.20,
        factor_consistency_weight=0.05,
        factor_net_exposure_weight=0.05,
        factor_gross_exposure_weight=0.02,
        factor_concentration_weight=0.02,
        factor_turnover_weight=0.02,
    )
    if not torch.isfinite(loss).all():
        raise AssertionError(f"non-finite factor loss: {loss}")
    loss.backward()
    finite_grads = [
        param.grad.detach().isfinite().all()
        for param in model.parameters()
        if param.grad is not None
    ]
    if not finite_grads or not all(bool(ok.item()) for ok in finite_grads):
        raise AssertionError("factor loss gradients are missing or non-finite")
    print(f"Device: {device}")
    print(f"factor_generalization_loss={float(loss.detach().cpu()):.6f}")
    print("SUCCESS")


if __name__ == "__main__":
    main()
