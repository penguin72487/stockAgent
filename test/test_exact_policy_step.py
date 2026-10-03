"""Accept actual whole-account improvements, and roll back rejected Adam state."""
from copy import deepcopy
import json

import pytest
import torch

from stockagent.config import load_config, _load_raw_config
from stockagent.training.exact_policy_step import ExactPolicyStep, PolicyEvaluation
from stockagent.training.exact_policy_step import maximum_drawdown_span, bind_return_weighted_loss
from stockagent.training.exact_policy_step import resolve_training_drawdown_budget
from stockagent.training.checkpoint_contract import _training_checkpoint_contract, _trading_checkpoint_contract
from stockagent.backtest.tw_futures_portfolio import run_tw_futures_portfolio_integer_torch
from stockagent.data import tw_futures_margin as margin
from test_tw_futures_margin import tape

CONFIG = 'configs/markets/tw_futures_v8_margin_verified_2011_capital100m_tx_front_roll_risk_step_v7.yaml'


def state(lr=.4):
    m = torch.nn.Linear(1, 1, bias=False)
    torch.nn.init.zeros_(m.weight)
    opt = torch.optim.AdamW(m.parameters(), lr=lr, weight_decay=.01)
    scheduler = torch.optim.lr_scheduler.LambdaLR(opt, lambda _: 1.)
    return m, opt, scheduler


def propose(m, opt, scheduler):
    m.weight.grad = -torch.ones_like(m.weight)
    opt.step()
    scheduler.step()


def test_backtracking_respects_real_whole_contract_drawdown_without_changing_execution():
    m, opt, scheduler = state()
    x = tape(3)
    x[:, 0, 3] = torch.tensor([1000., 1100., 1000.])
    x[:, 0, 4] = torch.tensor([1100., 1000., 1200.])
    x[:, 0, margin.PREVIOUS_MARK] = x[:, 0, 3]

    def evaluate():
        r = run_tw_futures_portfolio_integer_torch(m.weight.detach().expand(3, 1), x, initial_capital=1000.)
        cum = r.strategy_returns.double().cumsum(0)
        peaks = torch.cat([torch.zeros(1), cum]).cummax(0).values[1:]
        return PolicyEvaluation(-float(r.strategy_returns.mean())*252,
                                float(torch.expm1(cum-peaks).min()), bool(r.default_history.any()))

    guard = ExactPolicyStep(m, opt, scheduler, evaluate(), .2)
    propose(m, opt, scheduler)
    unguarded = evaluate()
    assert unguarded.max_drawdown < -.2
    accepted, receipt = guard.resolve(evaluate)
    assert receipt['accepted'] and 0 < receipt['fraction'] < 1
    assert -.2 <= accepted.max_drawdown <= 0
    assert accepted.loss < 0
    assert opt.state[m.weight]['step'] == 1 and scheduler.last_epoch == 1
    assert receipt['trials'][0]['max_drawdown'] == unguarded.max_drawdown


@pytest.mark.parametrize('failure', ['worse_loss', 'default', 'drawdown', 'nan'])
def test_total_rejection_restores_parameters_optimizer_scheduler_and_serializable_receipt(failure):
    m, opt, scheduler = state(lr=1.)
    before_optimizer = deepcopy(opt.state_dict())
    before_scheduler = deepcopy(scheduler.state_dict())
    guard = ExactPolicyStep(m, opt, scheduler, PolicyEvaluation(0., 0., False), .2)
    propose(m, opt, scheduler)

    def evaluate():
        return PolicyEvaluation(float('nan') if failure=='nan' else 1. if failure=='worse_loss' else -1.,
                                -.3 if failure=='drawdown' else 0., failure=='default')

    after, receipt = guard.resolve(evaluate)
    assert not receipt['accepted'] and receipt['fraction'] == 0
    assert len(receipt['trials']) == 9
    assert after == PolicyEvaluation(0., 0., False)
    assert m.weight.count_nonzero() == 0
    assert opt.state_dict() == before_optimizer
    assert scheduler.state_dict() == before_scheduler
    json.dumps(receipt, allow_nan=False)


def test_exception_rolls_back_a_previously_populated_optimizer():
    m, opt, scheduler = state()
    propose(m, opt, scheduler)
    before = m.weight.detach().clone()
    old_moment = opt.state[m.weight]['exp_avg'].clone()
    old_step = opt.state[m.weight]['step'].clone()
    guard = ExactPolicyStep(m, opt, scheduler, PolicyEvaluation(0., 0., False), .2)
    propose(m, opt, scheduler)
    def fail():
        raise RuntimeError('bad source')
    with pytest.raises(RuntimeError, match='bad source'):
        guard.resolve(fail)
    torch.testing.assert_close(m.weight, before, rtol=0, atol=0)
    torch.testing.assert_close(opt.state[m.weight]['exp_avg'], old_moment, rtol=0, atol=0)
    torch.testing.assert_close(opt.state[m.weight]['step'], old_step, rtol=0, atol=0)


def test_sub_lot_plateau_can_advance_to_its_first_executable_contract():
    m, opt, scheduler = state()
    guard = ExactPolicyStep(m, opt, scheduler, PolicyEvaluation(0., 0., False), .2)
    propose(m, opt, scheduler)
    _, receipt = guard.resolve(lambda: PolicyEvaluation(0., 0., False))
    assert receipt['fraction'] == 1 and m.weight.abs().sum() > 0


def test_guard_is_training_only_and_disabled_default_keeps_old_contract():
    cfg = load_config(CONFIG)
    guarded = _training_checkpoint_contract(cfg)
    forward = _trading_checkpoint_contract(cfg)
    cfg.training.futures_training_max_drawdown = None
    assert 'exact_policy_step' not in _training_checkpoint_contract(cfg)
    assert guarded['exact_policy_step']['training_max_drawdown'] == .2
    assert _trading_checkpoint_contract(cfg) == forward


@pytest.mark.parametrize('field,value', [('futures_training_max_drawdown',1.), ('futures_training_max_drawdown',float('nan')), ('futures_portfolio_optimizer_step_per_trajectory',False), ('loss_type','sharpe'), ('futures_training_stop_on_rejected_step','false')])
def test_invalid_guard_configs_fail_closed(tmp_path, field, value):
    import yaml
    raw = _load_raw_config(CONFIG)
    raw['training'][field] = value
    path=tmp_path/'bad.yaml'
    path.write_text(yaml.safe_dump(raw))
    with pytest.raises(ValueError):
        load_config(path)


@pytest.mark.parametrize('returns,span', [([-.1,-.2,.5],(0,2)),
                                       ([.3,-.1,-.15,.1],(1,3)),
                                       ([.1,.2],(0,0)), ([],(0,0))])
def test_drawdown_gradient_span_includes_initial_capital(returns, span):
    assert maximum_drawdown_span(torch.tensor(returns)) == span


def test_projection_can_improve_along_boundary_when_every_adam_step_is_infeasible():
    model = torch.nn.Linear(2, 1, bias=False)
    with torch.no_grad():
        model.weight.copy_(torch.tensor([[1., 0.]]))
    opt = torch.optim.AdamW(model.parameters(), lr=.1, weight_decay=0.)
    sched = torch.optim.lr_scheduler.LambdaLR(opt, lambda _: 1.)
    def evaluate():
        x, y = model.weight.detach().flatten().tolist()
        return PolicyEvaluation(-y, -.2 * x, False)
    guard = ExactPolicyStep(model, opt, sched, evaluate(), .2)
    model.weight.grad = torch.tensor([[-1., -1.]])
    opt.step()
    sched.step()
    def gradient(baseline):
        torch.testing.assert_close(model.weight, torch.tensor([[1., 0.]]), rtol=0, atol=0)
        assert opt.state[model.weight]['step'] == 1
        return [torch.tensor([[1., 0.]])]
    accepted, receipt = guard.resolve(evaluate, gradient)
    assert receipt['accepted'] and receipt['direction'] == 'risk_tangent'
    assert accepted.loss < 0 and accepted.max_drawdown == -.2
    assert len(receipt['trials']) == 2 and not receipt['trials'][0]['accepted']
    assert opt.state[model.weight]['step'] == 1 and sched.last_epoch == 1


def test_nonfinite_risk_gradient_rolls_back_every_state():
    m, opt, sched = state()
    guard = ExactPolicyStep(m, opt, sched, PolicyEvaluation(0., -.1, False), .2)
    propose(m, opt, sched)
    with pytest.raises(FloatingPointError, match='projection'):
        guard.resolve(lambda: PolicyEvaluation(-1., -.3, False),
                      lambda _: [torch.full_like(m.weight, float('nan'))])
    assert m.weight.count_nonzero() == 0 and not opt.state and sched.last_epoch == 0


def test_return_weights_slice_global_dates_and_zero_pad_ddp_tail():
    weights = torch.tensor([0., 1., 1., 0., 1.])
    seen = []
    def record(*, log_return_weights):
        seen.extend(log_return_weights.tolist())
    for start in range(0, 5, 3):
        bind_return_weighted_loss(record, weights, start, 3)()
    assert seen == [0., 1., 1., 0., 1., 0.]


def test_risk_tangent_changes_training_contract_only():
    c = load_config(CONFIG)
    training = _training_checkpoint_contract(c)
    forward = _trading_checkpoint_contract(c)
    c.training.futures_training_risk_tangent = True
    assert _training_checkpoint_contract(c)['exact_policy_step']['version'] == 2
    assert _training_checkpoint_contract(c) != training
    assert _trading_checkpoint_contract(c) == forward


def test_weighted_loss_vjp_preserves_whole_account_state_and_includes_prior_holdings():
    from stockagent.training.loss import risk_aware_loss
    x = tape(3, 1936)
    x[:, 1:, 1] = 0
    x[:, 0, 3] = torch.tensor([1000., 1010., 960.])
    x[:, 0, 4] = torch.tensor([1010., 960., 1020.])
    x[:, 0, margin.PREVIOUS_MARK] = x[:, 0, 3]
    w = torch.zeros(3, 1936)
    w[:, 0] = torch.tensor([.3, .4, .3])
    w.requires_grad_()
    mask = x[..., 1].bool()
    coefficients = torch.tensor([0., 1., 0.])
    oracle = run_tw_futures_portfolio_integer_torch(w, x, initial_capital=1000., recoverable_backward=True)
    expected = -(oracle.strategy_returns * coefficients).sum() / 3 * 252
    expected_gradient, = torch.autograd.grad(expected, w)
    aux = {}
    actual = risk_aware_loss(
        w, torch.zeros_like(w), mask, overnight_log_returns=x,
        execution_mode='tw_stock_context_futures_portfolio',
        portfolio_activation='pre_normalized', long_only=False,
        day_trade_execution_initial_capital=1000.,
        futures_portfolio_recoverable_backward=True,
        objective='log_utility', log_return_weights=coefficients,
        gamma_turnover=0., rank_ic_weight=0., return_rank_ic_weight=0.,
        direction_weight=0., volatility_regime_weight=0., concentration_weight=0.,
        aux_outputs=aux,
    )
    actual_gradient, = torch.autograd.grad(actual, w)
    torch.testing.assert_close(actual, expected)
    torch.testing.assert_close(actual_gradient, expected_gradient)
    assert actual_gradient[0, 0] != 0  # account carries the pre-drawdown history
    torch.testing.assert_close(aux['_final_equity_scale'], oracle.final_equity_scale)


@pytest.mark.parametrize('interior', [False, True])
def test_curved_risk_boundary_needs_an_interior_candidate(interior):
    model = torch.nn.Linear(2, 1, bias=False)
    with torch.no_grad():
        model.weight.copy_(torch.tensor([[1., 0.]]))
    opt = torch.optim.AdamW(model.parameters(), lr=.1, weight_decay=0.)
    sched = torch.optim.lr_scheduler.LambdaLR(opt, lambda _: 1.)
    def evaluate():
        x, y = model.weight.detach().double().flatten().tolist()
        return PolicyEvaluation(-y, -.2*(x*x+y*y), False)
    guard = ExactPolicyStep(model, opt, sched, evaluate(), .2)
    model.weight.grad = -torch.ones_like(model.weight)
    opt.step(); sched.step()
    result, receipt = guard.resolve(evaluate, lambda _: [torch.tensor([[1., 0.]])], risk_interior=interior)
    assert receipt['accepted'] is interior
    if interior:
        assert receipt['direction'] == 'risk_interior'
        assert result.loss < 0 and result.max_drawdown > -.2
        assert len(receipt['trials']) == 3
    else:
        torch.testing.assert_close(model.weight, torch.tensor([[1., 0.]]), atol=0, rtol=0)


def test_benchmark_budget_uses_only_supplied_training_account_and_changes_training_abi():
    baseline = PolicyEvaluation(0., 0., False, benchmark_max_drawdown=-.2875)
    assert resolve_training_drawdown_budget('benchmark', baseline) == .2875
    assert resolve_training_drawdown_budget(.2, baseline) == .2
    c = load_config('configs/markets/tw_futures_v8_margin_verified_2011_capital100m_tx_front_roll_benchmark_risk_v10.yaml')
    assert c.training.futures_training_max_drawdown == 'benchmark'
    # The old search-exhaustion stop retains its historical training contract.
    c.training.futures_training_stop_on_rejected_step = True
    assert _training_checkpoint_contract(c)['exact_policy_step']['version'] == 4
    before = _trading_checkpoint_contract(c)
    c.training.futures_training_max_drawdown = .2
    assert _training_checkpoint_contract(c)['exact_policy_step']['version'] == 3
    assert _trading_checkpoint_contract(c) == before


def test_validation_only_stopping_changes_no_candidate_acceptance_or_forward_rules():
    c = load_config('configs/markets/tw_futures_v8_margin_verified_2011_capital100m_tx_front_roll_benchmark_risk_v10.yaml')
    assert c.training.epochs == 1000
    assert c.training.early_stopping_no_improve_ratio == .1
    assert c.training.val_interval_epochs == 1
    assert c.training.early_stopping_min_delta == .0001
    assert c.training.best_checkpoint_max_epoch == 0
    assert c.training.futures_training_stop_on_rejected_step is False
    current = _training_checkpoint_contract(c)
    forward = _trading_checkpoint_contract(c)
    c.training.futures_training_stop_on_rejected_step = True
    historical = _training_checkpoint_contract(c)
    assert _trading_checkpoint_contract(c) == forward
    step = current['exact_policy_step']
    assert step.pop('stop_on_rejected_step') is False
    assert step['version'] == 5
    step['version'] = 4
    assert current == historical
    assert load_config(CONFIG).training.futures_training_stop_on_rejected_step is True


@pytest.mark.parametrize('drawdown',[None, 0., -.0, float('nan'), float('-inf'), -1., .1])
def test_unusable_training_benchmark_cannot_silently_relax_risk(drawdown):
    with pytest.raises(ValueError, match='training benchmark'):
        resolve_training_drawdown_budget('benchmark', PolicyEvaluation(0., 0., False, benchmark_max_drawdown=drawdown))
