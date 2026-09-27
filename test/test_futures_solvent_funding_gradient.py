"""Funding recovery must not oppose fee-adjusted utility inside its feasible set."""
from dataclasses import fields

import pytest
import torch

from stockagent.backtest.tw_futures_portfolio import run_tw_futures_portfolio_integer_torch


def _round_trip(direction, profit=200.0):
    tape = torch.zeros(1, 1, 11)
    tape[..., 1:3] = 1
    tape[..., 3] = 100_000
    tape[..., 4] = 100_000 + direction * profit
    tape[..., 5] = 40
    tape[..., 8] = 10_000
    return tape


def _execute(weight, tape, recovery):
    return run_tw_futures_portfolio_integer_torch(
        weight, tape, initial_capital=100_000_000,
        recoverable_backward=recovery,
    )


@pytest.mark.parametrize("direction", [-1.0, 1.0])
@pytest.mark.parametrize("gross", [0.5, 0.7, 0.75, 0.9])
@pytest.mark.parametrize("profit", [0.0, 80.0, 200.0])
def test_funded_gradient_agrees_with_net_profit_and_preserves_forward(direction, gross, profit):
    tape = _round_trip(direction, profit)
    gradients, results = [], []
    for recovery in (False, True):
        weight = torch.tensor([[direction * gross]], requires_grad=True)
        result = _execute(weight, tape, recovery)
        result.strategy_returns.sum().backward()
        gradients.append(weight.grad.item())
        results.append(result)
        assert result.final_alive
    for field in fields(results[0]):
        if field.name.startswith("_"):
            continue
        before, after = getattr(results[0], field.name), getattr(results[1], field.name)
        if isinstance(before, torch.Tensor):
            assert torch.equal(before, after), field.name
        else:
            assert before is after
    # The unchanged smooth wealth map differs by about 5.04e-5 for the
    # fee-only loss case; its utility slope must remain within 0.01%.
    assert gradients[1] == pytest.approx(gradients[0], rel=1e-4, abs=1e-9)
    if profit == 80:
        assert abs(gradients[1]) < 1e-7
    else:
        assert gradients[1] * direction * (profit - 80) > 0
        with torch.no_grad():
            larger = _execute(torch.tensor([[direction * (gross + 0.002)]]), tape, True)
        difference = larger.strategy_returns.item() - results[1].strategy_returns.item()
        assert difference * (profit - 80) > 0


@pytest.mark.parametrize("direction", [-1.0, 1.0])
def test_funding_excess_still_receives_a_reduction_gradient(direction):
    weight = torch.tensor([[direction * 1.2]], requires_grad=True)
    result = _execute(weight, _round_trip(direction), True)
    result.strategy_returns.sum().backward()
    assert torch.isfinite(weight.grad).all()
    assert weight.grad.item() * direction < 0


def test_funding_gradient_change_rejects_old_optimizer_contract(monkeypatch, tmp_path):
    import stockagent.training.checkpoint_contract as contract
    from stockagent.config import load_config
    from test_tw_stock_context_futures_portfolio import _stock_panel

    config = load_config("configs/markets/tw_futures_v8_general_tradable_capital100m_funding.yaml")
    panel = _stock_panel()
    current = contract.build_checkpoint_manifest(panel, config, include_data_content=False)
    with monkeypatch.context() as patch:
        patch.setattr(contract, "TW_FUTURES_PORTFOLIO_INTEGER_RECOVERABLE_TRAINING_SURROGATE",
                      "grouped_fake_floor_cash_solvency_recovery_surrogate_v6")
        previous = contract.build_checkpoint_manifest(panel, config, include_data_content=False)
    assert previous["fingerprints"]["model"] == current["fingerprints"]["model"]
    assert previous["fingerprints"]["data"] == current["fingerprints"]["data"]
    assert previous["fingerprints"]["trading"] != current["fingerprints"]["trading"]
    with pytest.raises(RuntimeError, match="semantic fingerprint mismatch"):
        contract._validate_checkpoint_manifest({"experiment_manifest": previous}, current,
            checkpoint_path=tmp_path / "old_funding.pt", scope="resume")
    contract._validate_checkpoint_manifest({"experiment_manifest": current}, current,
        checkpoint_path=tmp_path / "current_funding.pt", scope="resume")
