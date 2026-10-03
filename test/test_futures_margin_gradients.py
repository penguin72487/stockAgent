"""Analytic checks of the declared margin STE, not derivatives of integer jumps."""
import pytest
import torch

from stockagent.backtest.tw_futures_portfolio import (
    run_tw_futures_portfolio_integer_surrogate_torch as shadow,
    run_tw_futures_portfolio_integer_torch as exact,
)
from stockagent.config import load_config
from stockagent.data import tw_futures_margin as m
from stockagent.training.checkpoint_contract import (
    _training_checkpoint_contract, _trading_checkpoint_contract,
)
from test_tw_futures_margin import tape


@pytest.mark.parametrize("sign", [1., -1.])
@pytest.mark.parametrize("gap", [-100., 100.])
def test_opening_funding_uses_marked_losses_but_not_unsettled_gains(sign, gap):
    x = tape(2)
    x[1, 0, 3] += sign * gap
    x[1, 0, 4] = x[1, 0, 3] + sign * 50
    weights = torch.tensor([[sign * .4], [sign * .5]], requires_grad=True)
    result = shadow(weights, x, initial_capital=1000.)
    oracle = exact(weights.detach(), x, initial_capital=1000.)
    # Day 1: four contracts. Day 2: start NAV 1000, opening NAV
    # 600/1400, new allocation NAV 600/1000; three/five contracts earn 50.
    wealth = .75 if gap < 0 else 1.65
    torch.testing.assert_close(result.equity_scale_history, oracle.equity_scale_history)
    assert result.final_equity_scale.item() == pytest.approx(wealth)
    result.strategy_returns.sum().backward()
    allocation = .6 if gap < 0 else 1.
    assert weights.grad[1].item() == pytest.approx(sign * allocation * .5 / wealth)
    prior_slope = gap / 100 * (1.25 if gap < 0 else 1.)
    assert weights.grad[0].item() == pytest.approx(sign * prior_slope / wealth)


@pytest.mark.parametrize("sign", [1., -1.])
def test_expiry_gradient_uses_official_cash_settlement(sign):
    x = tape()
    x[..., 2] = 1
    x[..., 4] += sign * 50
    x[..., m.TERMINAL_MARK] += sign * 100
    x[..., m.CASH_SETTLEMENT] = 1
    weights = torch.tensor([[sign * .5]], requires_grad=True)
    result = shadow(weights, x, initial_capital=1000.)
    assert result.final_equity_scale.item() == pytest.approx(1.5)
    result.strategy_returns.sum().backward()
    assert weights.grad.item() == pytest.approx(sign / 1.5)


@pytest.mark.parametrize("sign", [1., -1.])
def test_forbidden_order_has_no_phantom_payoff_gradient(sign):
    x = tape()
    x[..., 4] += sign * 50
    x[..., m.CAN_BUY if sign > 0 else m.CAN_SELL] = 0
    weights = torch.tensor([[sign * .5]], requires_grad=True)
    result = shadow(weights, x, initial_capital=1000.)
    assert result.final_equity_scale.item() == 1.
    result.strategy_returns.sum().backward()
    assert weights.grad.item() == 0.


@pytest.mark.parametrize("recover", [False, True])
def test_margin_call_does_not_teach_new_entry_when_account_must_flatten(recover):
    x = tape(2)
    x[0, :, 4] = 860  # Five contracts lose 700; equity 300 < maintenance 375.
    x[1, :, 3] = x[1, :, m.PREVIOUS_MARK] = 860
    x[1, :, 4] = 960  # Unavailable profit: today's policy cannot reopen.
    weights = torch.tensor([[.5], [.5]], requires_grad=True)
    result = shadow(weights, x, initial_capital=1000.,
                    recover_after_default_for_backward=recover)
    assert result.final_equity_scale.item() == pytest.approx(.3)
    result.strategy_returns.sum().backward()
    assert weights.grad[1].item() == 0.
    assert torch.isfinite(weights.grad).all()


@pytest.mark.parametrize("recover", [False, True])
def test_gradient_changes_preserve_every_exact_forward_field(recover):
    from dataclasses import fields
    generator = torch.Generator().manual_seed(931)
    x = tape(12, 3)
    x[..., 3:5] += torch.randn(12, 3, 2, generator=generator) * 20
    x[1:, :, m.PREVIOUS_MARK] = x[:-1, :, 4]
    x[..., 5:8] = 1
    x[4, 0, m.CAN_BUY] = 0
    x[5, 1, m.CAN_SELL] = 0
    x[8, :, 2] = x[8, :, m.CASH_SETTLEMENT] = 1
    w = (torch.rand(12, 3, generator=generator) - .5) * .5
    oracle = exact(w, x, initial_capital=1000., recoverable_backward=False)
    candidate = exact(w.requires_grad_(), x, initial_capital=1000.,
                      recoverable_backward=recover)
    for field in fields(oracle):
        expected, actual = getattr(oracle, field.name), getattr(candidate, field.name)
        if isinstance(expected, torch.Tensor):
            torch.testing.assert_close(actual, expected, rtol=0, atol=0)
    candidate.strategy_returns.sum().backward()
    assert torch.isfinite(w.grad).all()
    assert w.grad.abs().sum() > 0


def test_margin_backward_fingerprint_preserves_forward_replay(monkeypatch):
    c = load_config('configs/markets/tw_futures_v8_margin_verified_2011_capital100m_tx_front_roll_gradient_v3.yaml')
    new_training = _training_checkpoint_contract(c)
    forward = _trading_checkpoint_contract(c)
    assert new_training['futures_margin_backward_contract_version'] == 6
    monkeypatch.setattr(m, 'MARGIN_TRAINING_GRADIENT_CONTRACT_VERSION', 5)
    assert _training_checkpoint_contract(c) != new_training
    assert _trading_checkpoint_contract(c) == forward
    assert c.runner.start_fold == 1 and c.training.epochs == 1000
    assert c.training.early_stopping_no_improve_ratio == .1
    assert c.training.loss_type == 'log_utility'


def test_physical_gradient_rejects_old_optimizer_but_allows_inference(monkeypatch, tmp_path):
    from stockagent.training.checkpoint_contract import build_checkpoint_manifest, _validate_checkpoint_manifest
    from test_tw_stock_context_futures_portfolio import _stock_panel
    config = load_config('configs/markets/tw_futures_v8_margin_verified_2011_capital100m_tx_front_roll_physical_gradient_v5.yaml')
    panel = _stock_panel()
    current = build_checkpoint_manifest(panel, config, include_data_content=False)
    with monkeypatch.context() as patch:
        patch.setattr(m, 'MARGIN_TRAINING_GRADIENT_CONTRACT_VERSION', 5)
        previous = build_checkpoint_manifest(panel, config, include_data_content=False)
    assert previous['fingerprints']['trading'] == current['fingerprints']['trading']
    assert previous['fingerprints']['model'] == current['fingerprints']['model']
    with pytest.raises(RuntimeError, match='semantic fingerprint mismatch'):
        _validate_checkpoint_manifest({'experiment_manifest': previous}, current,
                                     checkpoint_path=tmp_path/'old.pt', scope='resume')
    _validate_checkpoint_manifest({'experiment_manifest': previous}, current,
                                 checkpoint_path=tmp_path/'old.pt', scope='inference')
