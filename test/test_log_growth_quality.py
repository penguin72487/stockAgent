from dataclasses import replace
from functools import partial
from pathlib import Path

import pytest
import torch

from stockagent.config import load_config
from stockagent.models.normalization import masked_score_entmax_scale_separated_cash_weights as allocate
from stockagent.training.checkpoint_contract import _evaluation_checkpoint_contract, _training_checkpoint_contract
from stockagent.training.log_growth_risk import risk_adjusted_log_growth
from stockagent.training.loss import risk_aware_loss
from stockagent.training.trainer import _loss_from_backtest_series, _batched_loss_from_backtest_segments
from test_tw_day_trade_carry import session, v, run


def loss_options():
    from stockagent.config import EvaluationConfig
    c = EvaluationConfig()
    return {k: getattr(c, k) for k in (
        'gamma_sharpe', 'gamma_excess', 'gamma_cvar', 'cvar_alpha', 'gamma_drawdown',
        'drawdown_target', 'gamma_turnover', 'gamma_underperformance', 'excess_target',
        'cvar_budget', 'drawdown_budget', 'turnover_budget', 'gamma_cvar_budget',
        'gamma_drawdown_budget', 'gamma_turnover_budget')}


@pytest.mark.parametrize('dense', [False, True])
def test_sparse_forward_mask_and_zero_scores_remain_exact(dense):
    for z in (torch.zeros(3, 17), torch.randn(3, 17) * 3):
        mask = torch.rand(3, 17) > .3
        mask[1] = False
        x = z.requires_grad_()
        w = allocate(x, mask, dense_backward=dense)
        torch.testing.assert_close(w, allocate(x, mask), atol=0, rtol=0)
        assert (w[~mask] == 0).all() and (w.abs().sum(1) <= 1).all()
        w.square().sum().backward()
        assert torch.isfinite(x.grad).all() and (x.grad[~mask] == 0).all()
        with torch.no_grad():
            torch.testing.assert_close(allocate(x, mask, dense_backward=True), w, atol=0, rtol=0)


def test_sparse_support_cannot_receive_excluded_payoff_but_dense_adjoint_can():
    # The excluded stock has no direct learning signal through sparse support.
    # RMS affects selected coordinates but cannot reveal an excluded payoff.
    x = torch.tensor([[4., .1, -.2, .05] + [.01] * 13], requires_grad=True)
    sparse = allocate(x, None)
    assert sparse[0, 1] == 0
    g0, = torch.autograd.grad(sparse[0, 1], x)
    dense = allocate(x, None, dense_backward=True)
    g1, = torch.autograd.grad(dense[0, 1], x)
    torch.testing.assert_close(dense, sparse, atol=0, rtol=0)
    assert g0.abs().sum() == 0 and g1[0, 1] > 0


def test_date_weighted_risk_utility_is_value_and_gradient_decomposable():
    r = torch.tensor([.02, -.03, .01, -.04, .02], dtype=torch.float64, requires_grad=True)
    b = torch.tensor([.01, -.01, -.02, .01, .03], dtype=torch.float64)
    fn = partial(risk_adjusted_log_growth, risk_aversion=10, downside_aversion=20, benchmark_shortfall_aversion=15)
    whole = -252 * fn(r, b).mean()
    chunks = sum(-252 * fn(r[a:z], b[a:z]).mean() * ((z-a)/len(r)) for a,z in ((0,2),(2,5)))
    gw, = torch.autograd.grad(whole, r, retain_graph=True)
    gc, = torch.autograd.grad(chunks, r)
    torch.testing.assert_close(whole, chunks, atol=1e-14, rtol=1e-14)
    torch.testing.assert_close(gw, gc, atol=1e-14, rtol=1e-14)
    assert gw[1] < -252 / len(r) and gw[3] < gw[1]


def test_eval_segments_use_same_risk_objective_without_changing_return_tape():
    r = torch.tensor([.01, -.02, .03, -.04, .01], dtype=torch.float64, requires_grad=True)
    b = torch.tensor([.02, .01, -.01, .01, .01], dtype=torch.float64)
    original = r.detach().clone()
    risk = dict(log_utility_risk_aversion=10., log_utility_downside_aversion=20., log_utility_benchmark_shortfall_aversion=15.)
    options = dict(loss_options(), objective='log_utility', **risk)
    actual = _batched_loss_from_backtest_segments(r, b, torch.zeros_like(r), [0, 2, 5], **options)
    expected = torch.stack([_loss_from_backtest_series(r[a:z], b[a:z], torch.zeros(z-a), **options) for a,z in ((0,2),(2,5))])
    torch.testing.assert_close(actual.float(), expected, atol=1e-6, rtol=1e-6)
    ga, = torch.autograd.grad(actual.sum(), r, retain_graph=True)
    ge, = torch.autograd.grad(expected.sum(), r)
    torch.testing.assert_close(ga, ge, atol=1e-5, rtol=1e-6)
    torch.testing.assert_close(r.detach(), original, atol=0, rtol=0)


@pytest.mark.parametrize('recovery', [False, True])
@pytest.mark.parametrize('weight', [.01, .2, -.2])
def test_physical_fifo_train_and_eval_risk_value_match_exact_account(recovery, weight):
    s = replace(session(), terminal_liquidation_price=v(1010.))
    w = v(weight).reshape(1, 1).requires_grad_()
    mask = torch.ones_like(w, dtype=torch.bool)
    risk = dict(log_utility_risk_aversion=10., log_utility_downside_aversion=20., log_utility_benchmark_shortfall_aversion=15.)
    loss = risk_aware_loss(w, torch.zeros_like(w), mask,
        objective='log_utility', long_only=False, gamma_sharpe=1., gamma_turnover=0.,
        execution_mode='tw_day_trade', portfolio_activation='pre_normalized',
        day_trade_unlimited_margin_conversion=True, day_trade_execution_initial_capital=10_000_000.,
        day_trade_carry_sessions=(s,), day_trade_eligible_mask=mask,
        day_trade_can_buy_open_mask=mask, day_trade_can_sell_open_mask=mask, can_short_open_mask=mask,
        normal_sell_fee_rates=v(.004425), buy_fee_rates=v(.001425), sell_fee_rates=v(.002925),
        commission_rebate_rates=v(.00114), benchmark_returns=v(.02),
        day_trade_sub_lot_recovery=recovery, **risk)
    exact = run(w, [s])
    expected = _loss_from_backtest_series(exact.strategy_returns, v(.02), exact.turnovers,
        objective='log_utility', **loss_options(), **risk)
    torch.testing.assert_close(loss.float(), expected, atol=1e-6, rtol=1e-6)
    loss.backward()
    assert torch.isfinite(w.grad).all()
    if recovery and weight == .01:
        assert w.grad.item() < 0


def test_new_controls_default_to_historical_resume_contract_and_fingerprint_opt_ins():
    c = load_config('configs/deployments/tw_day_trade_factorized_values_20261007_scale_separated_cash_annual_v1.yaml')
    base = _evaluation_checkpoint_contract(c)
    assert not any(k.startswith('log_utility_') for k in base)
    c.evaluation.log_utility_downside_aversion = 20
    assert _evaluation_checkpoint_contract(c)['log_utility_downside_aversion'] == 20
    before = _training_checkpoint_contract(c)
    c.training.day_trade_dense_allocation_backward = True
    after = _training_checkpoint_contract(c)
    assert before != after
    assert 'day_trade_dense_allocation_backward' in after['fold_continuation']


def test_model_factory_uses_dense_backward_only_during_training():
    from stockagent.models.transformer_base_portfolio import TransformerBasePortfolioModel
    model = TransformerBasePortfolioModel(num_features=2, num_symbols=17, lookback=2,
        d_model=4, temporal_heads=1, cross_heads=1, joint_heads=1,
        portfolio_output_mode='score_entmax_scale_separated_cash',
        portfolio_mode='long_short', entmax_dense_backward=True)
    x = torch.tensor([[4., .1, -.2, .05] + [.01] * 13], requires_grad=True)
    model.train()
    w = model.postprocess_action_logits(x)
    gd, = torch.autograd.grad(w[0,1], x)
    model.eval()
    hard = model.postprocess_action_logits(x)
    gs, = torch.autograd.grad(hard[0,1], x)
    torch.testing.assert_close(w, hard, atol=0, rtol=0)
    assert gd[0,1] > 0 and gs.abs().sum() == 0


@pytest.mark.parametrize('value', [-1., float('nan'), float('inf'), True])
def test_loader_rejects_invalid_risk_preferences(tmp_path, value):
    import yaml
    path = tmp_path / 'invalid.yaml'
    path.write_text(yaml.safe_dump({
        'base_config': str(Path('configs/deployments/tw_day_trade_factorized_values_20261007_scale_separated_cash_annual_v1.yaml').resolve()),
        'evaluation': {'log_utility_downside_aversion': value},
    }))
    with pytest.raises(ValueError, match='nonnegative and finite'):
        load_config(path)


def test_loader_rejects_dense_discovery_without_physical_payoff_signal(tmp_path):
    import yaml
    path = tmp_path / 'invalid.yaml'
    path.write_text(yaml.safe_dump({
        'base_config': str(Path('configs/deployments/tw_day_trade_factorized_values_20261007_scale_separated_cash_annual_v1.yaml').resolve()),
        'training': {'day_trade_dense_allocation_backward': True,
                     'day_trade_sub_lot_recovery': False},
    }))
    with pytest.raises(ValueError, match='requires physical sub-lot recovery'):
        load_config(path)
