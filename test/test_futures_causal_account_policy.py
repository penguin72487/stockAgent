"""Causal feedback must use the account that actually traded, on every path."""
from dataclasses import asdict, replace

import numpy as np
import polars as pl
import pytest
import torch

from stockagent.backtest.simulator import run_backtest_torch
from stockagent.backtest.tw_futures_portfolio import run_tw_futures_portfolio_integer_torch
from stockagent.config import load_config
from stockagent.data import tw_futures_margin as margin
from stockagent.data.tw_stock_context_futures_portfolio import TW_STOCK_CONTEXT_FUTURES_MODEL_FEATURE_COLUMNS
from stockagent.models.financial_transformer_futures import FinancialTransformerFuturesModel
from stockagent.models.futures_account_policy import (
    ACCOUNT_COEFFICIENT_START, ACCOUNT_RMS, ACCOUNT_SIDE_COST,
    FUTURES_ACCOUNT_PACKET_WIDTH, FUTURES_ACCOUNT_STATE_COLUMNS,
    FUTURES_NET_EXPOSURE_PACKET_WIDTH, futures_account_policy_version,
    FUTURES_BETA_EXPOSURE_PACKET_WIDTH, ACCOUNT_TX_BETA,
    futures_account_policy_width,
    pack_futures_account_policy, resolve_futures_account_policy, project_prior_net_notional,
)
from stockagent.models.normalization import masked_score_entmax_log_cash_weights
from stockagent.training.checkpoint_contract import (
    _project_temporal_basis_model_config, _validate_checkpoint_manifest, build_checkpoint_manifest,
)
from stockagent.training.loss import risk_aware_loss
from test_tw_futures_margin import tape, rule_panel, write_rules
from test_tw_stock_context_futures_portfolio import _stock_panel

CONFIG = 'configs/markets/tw_futures_v8_margin_components_causal_account_capital100m_v5.yaml'


def packet(rows=3, slots=2, *, feedback=0.0, device='cpu', net_cap=None, beta_cap=None):
    logits = torch.tensor([.4, -.2], device=device)[:slots].expand(rows, slots).clone()
    coefficients = torch.zeros(rows, slots, len(FUTURES_ACCOUNT_STATE_COLUMNS), device=device)
    coefficients[..., 0] = feedback
    mask = torch.ones(rows, slots, dtype=torch.bool, device=device)
    base = masked_score_entmax_log_cash_weights(logits, mask)
    ones = torch.ones_like(base)
    return pack_futures_account_policy(base, logits, coefficients, mask,
        ones * 1000, ones * 100, ones * 75, ones * 2, ones * .02, ones,
        net_notional_cap=net_cap, beta_notional_cap=beta_cap,
        prior_beta=torch.tensor([2., .5], device=device)[:slots].expand_as(base)
                   if beta_cap is not None else None)


def account(policy, execution, **kw):
    return run_tw_futures_portfolio_integer_torch(policy, execution,
                                                 initial_capital=10_000, **kw)


def test_zero_residual_reproduces_exact_legacy_account_without_cash_initialization():
    p = packet()
    x = tape(3, 2)
    x[..., 5:7] = 1
    x[0, 0, 4] += 40
    x[1, :, margin.PREVIOUS_MARK] = x[0, :, 4]
    a, b = account(p[..., 0], x), account(p, x)
    assert a.contract_quantities_history.count_nonzero() > 0
    for field in ('strategy_returns', 'turnovers', 'contract_quantities_history',
                  'margin_audit_history', 'final_equity_scale', 'final_alive'):
        torch.testing.assert_close(getattr(a, field), getattr(b, field), rtol=0, atol=0)
    torch.testing.assert_close(b.requested_weights_history, p[..., 0], rtol=0, atol=0)


@pytest.mark.parametrize('cap', [0., .4, 100.])
def test_prior_net_projection_uses_notional_units_retains_budget_and_gradients(cap):
    weights = torch.tensor([[.4, .1, -.2], [-.1, .3, .2]], requires_grad=True)
    mask = torch.tensor([[True, True, False], [True, True, True]])
    notional = torch.tensor([[1000., 500., 300.]]).expand_as(weights)
    initial = torch.tensor([[100., 200., 50.]]).expand_as(weights)
    projected = project_prior_net_notional(weights, mask, notional, initial, torch.tensor(cap))
    assert ((projected * notional / initial).sum(-1).abs() <= cap + 1e-6).all()
    assert (projected.abs().sum(-1) <= (weights * mask).abs().sum(-1) + 1e-6).all()
    assert projected[0, 2] == 0
    if cap == 100:
        torch.testing.assert_close(projected, weights * mask, rtol=0, atol=0)
    permutation = torch.tensor([2, 0, 1])
    shuffled = project_prior_net_notional(weights[:, permutation], mask[:, permutation],
        notional[:, permutation], initial[:, permutation], torch.tensor(cap))
    torch.testing.assert_close(shuffled, projected[:, permutation])
    payoff = torch.tensor([[.02, -.01, .04], [-.03, .04, .01]])
    gradient, = torch.autograd.grad((projected * payoff).sum(), weights)
    assert torch.isfinite(gradient).all() and gradient.abs().sum() > 0
    for index in ((0, 0), (1, 1)):
        delta = torch.zeros_like(weights)
        delta[index] = 1e-3
        plus = project_prior_net_notional(weights.detach() + delta, mask, notional, initial, torch.tensor(cap))
        minus = project_prior_net_notional(weights.detach() - delta, mask, notional, initial, torch.tensor(cap))
        finite_difference = ((plus - minus) * payoff).sum() / .002
        torch.testing.assert_close(gradient[index], finite_difference, rtol=2e-3, atol=2e-6)


def test_neutral_projection_is_after_account_feedback_and_obeys_no_trade_masks():
    policy = packet(feedback=.7, net_cap=0.)
    assert policy.shape[-1] == FUTURES_NET_EXPOSURE_PACKET_WIDTH
    assert futures_account_policy_version(policy.shape[-1]) == 2
    q = torch.tensor([3, -2])
    kw = dict(initial_capital=10_000., alive=torch.tensor(True), advance=torch.tensor(True))
    request = resolve_futures_account_policy(policy[0], q, torch.tensor(13_000.), **kw)
    assert abs(float(request.sum())) < 1e-6
    assert request.abs().sum() > 0
    for disabled in ('alive', 'advance'):
        values = {**kw, disabled: torch.tensor(False)}
        assert resolve_futures_account_policy(policy[0], q, torch.tensor(13_000.), **values).count_nonzero() == 0
    # The canonical exact account still owns integer quantities, fees and marks.
    result = account(policy, tape(3, 2), recoverable_backward=True)
    assert result.contract_quantities_history.count_nonzero() > 0
    assert result.requested_weights_history[:, 0].gt(0).all()
    assert result.requested_weights_history[:, 1].lt(0).all()
    torch.testing.assert_close(result.requested_weights_history.sum(-1), torch.zeros(3), atol=1e-6, rtol=0)


def test_already_neutral_requests_have_no_common_notional_gradient():
    # A zero-width clamp has an unsuitable subgradient exactly at zero.
    weights = torch.tensor([[.1, -.4]], requires_grad=True)
    notional, initial = torch.tensor([[1000., 500.]]), torch.tensor([[100., 200.]])
    mask = torch.ones_like(weights, dtype=torch.bool)
    projected = project_prior_net_notional(weights, mask, notional, initial, torch.tensor(0.))
    net = (projected * notional / initial).sum()
    gradient, = torch.autograd.grad(net, weights)
    torch.testing.assert_close(net, torch.tensor(0.), rtol=0, atol=1e-7)
    torch.testing.assert_close(gradient, torch.zeros_like(weights), rtol=0, atol=1e-6)


def test_beta_projection_matches_independent_oracle_and_has_correct_adjoint():
    weights = torch.tensor([[.4, -.2, .05]], requires_grad=True)
    notional, initial = torch.tensor([[1000., 500., 200.]]), torch.tensor([[100., 100., 100.]])
    beta = torch.tensor([[2., .5, 1.]], requires_grad=True)
    mask = torch.ones_like(weights, dtype=torch.bool)
    projected = project_prior_net_notional(weights, mask, notional, initial, torch.tensor(0.), factor_beta=beta)
    n = (weights.detach() * notional / initial).numpy()
    b = beta.detach().numpy()
    n -= b * (n * b).sum(-1, keepdims=True) / (b * b).sum(-1, keepdims=True)
    expected = n * (initial / notional).numpy()
    expected *= min(1., float(weights.detach().abs().sum()) / np.abs(expected).sum())
    np.testing.assert_allclose(projected.detach().numpy(), expected, atol=1e-7, rtol=1e-6)
    exposure = (projected * notional / initial * beta.detach()).sum()
    assert abs(float(exposure.detach())) < 1e-6
    torch.testing.assert_close(torch.autograd.grad(exposure, weights, retain_graph=True)[0],
                               torch.zeros_like(weights), rtol=0, atol=2e-6)
    payoff = torch.tensor([[.02, -.01, .03]])
    gradient = torch.autograd.grad((projected * payoff).sum(), weights)[0]
    assert beta.grad is None and gradient.abs().sum() > 0
    for index in range(3):
        delta = torch.zeros_like(weights); delta[0, index] = 1e-3
        plus = project_prior_net_notional(weights.detach() + delta, mask, notional, initial, torch.tensor(0.), factor_beta=beta)
        minus = project_prior_net_notional(weights.detach() - delta, mask, notional, initial, torch.tensor(0.), factor_beta=beta)
        torch.testing.assert_close(gradient[0, index], ((plus - minus) * payoff).sum() / .002,
                                   rtol=2e-3, atol=2e-6)
    # A zero loading constrains no direction and cannot erase the learned budget.
    zero = project_prior_net_notional(weights, mask, notional, initial, torch.tensor(0.), factor_beta=torch.zeros_like(beta))
    torch.testing.assert_close(zero, weights, rtol=0, atol=0)


def test_beta_packet_is_applied_after_actual_account_feedback():
    p = packet(feedback=.7, beta_cap=0.)
    assert p.shape[-1] == FUTURES_BETA_EXPOSURE_PACKET_WIDTH
    assert futures_account_policy_version(p.shape[-1]) == 3
    kwargs = dict(initial_capital=10_000., alive=torch.tensor(True), advance=torch.tensor(True))
    requested = resolve_futures_account_policy(p[0], torch.tensor([3, -2]), torch.tensor(13_000.), **kwargs)
    assert abs(float((requested * p[0, :, ACCOUNT_TX_BETA]).sum())) < 1e-6
    assert abs(float(requested.sum())) > 1e-4  # dollar-neutral is a different constraint
    result = account(p, tape(3, 2), recoverable_backward=True)
    assert result.contract_quantities_history.count_nonzero() > 0
    assert (result.contract_quantities_history > 0).any() and (result.contract_quantities_history < 0).any()
    torch.testing.assert_close((result.requested_weights_history * p[..., ACCOUNT_TX_BETA]).sum(-1),
                               torch.zeros(3), rtol=0, atol=2e-6)


@pytest.mark.parametrize('cap', [0., .25, 100.])
def test_beta_projection_handles_signed_loadings_empty_masks_and_compiled_graph(cap):
    w = torch.tensor([[.3, -.1, .2], [.1, .2, -.1]], requires_grad=True)
    mask = torch.tensor([[True, True, False], [False, False, False]])
    notional = torch.tensor([[1000., 500., 200.]]).expand_as(w)
    initial = torch.tensor([[100., 200., 50.]]).expand_as(w)
    beta = torch.tensor([[-.5, 2., 1.]]).expand_as(w)
    compiled = torch.compile(project_prior_net_notional, backend='eager', fullgraph=True)
    result = compiled(w, mask, notional, initial, torch.tensor(cap), factor_beta=beta)
    exposure = (result * notional / initial * beta).sum(-1)
    assert (exposure.detach().abs() <= cap + 1e-6).all()
    assert result[1].count_nonzero() == 0 and result[0, 2] == 0
    assert (result.detach().abs().sum(-1) <= (w.detach()*mask).abs().sum(-1)+1e-6).all()
    gradient, = torch.autograd.grad((result*torch.tensor([.03, -.01, .02])).sum(), w)
    assert torch.isfinite(gradient).all()
    assert gradient[1].count_nonzero() == 0 and gradient[0].abs().sum() > 0


def test_neutral_output_cannot_resume_unconstrained_checkpoint(tmp_path):
    config = load_config(CONFIG)
    legacy_values = asdict(config.training.financial_transformer)
    modern_values = dict(legacy_values)
    legacy_values.pop('futures_net_notional_cap')
    assert _project_temporal_basis_model_config(legacy_values) == _project_temporal_basis_model_config(modern_values)
    old = build_checkpoint_manifest(_stock_panel(), config, include_data_content=False)
    config.training.financial_transformer.futures_net_notional_cap = 0.
    new = build_checkpoint_manifest(_stock_panel(), config, include_data_content=False)
    for key in ('model', 'trading'):
        assert old['fingerprints'][key] != new['fingerprints'][key]
    with pytest.raises(RuntimeError, match='semantic fingerprint mismatch'):
        _validate_checkpoint_manifest({'experiment_manifest': old}, new,
            checkpoint_path=tmp_path/'old.pt', scope='resume')


def test_beta_policy_has_a_distinct_checkpoint_and_validated_config(tmp_path):
    import yaml
    from pathlib import Path
    c = load_config(CONFIG)
    old = build_checkpoint_manifest(_stock_panel(), c, include_data_content=False)
    c.training.financial_transformer.futures_beta_notional_cap = 0.
    new = build_checkpoint_manifest(_stock_panel(), c, include_data_content=False)
    for key in ('model', 'trading'):
        assert old['fingerprints'][key] != new['fingerprints'][key]
    with pytest.raises(RuntimeError, match='semantic fingerprint mismatch'):
        _validate_checkpoint_manifest({'experiment_manifest': old}, new,
            checkpoint_path=tmp_path/'old.pt', scope='resume')
    values = asdict(load_config(CONFIG).training.financial_transformer)
    legacy = dict(values)
    legacy.pop('futures_beta_notional_cap')
    assert _project_temporal_basis_model_config(values) == _project_temporal_basis_model_config(legacy)
    for fields in ({'futures_beta_notional_cap': -.1}, {'futures_beta_notional_cap': True},
                   {'futures_beta_notional_cap': float('nan')},
                   {'futures_beta_notional_cap': 0., 'futures_net_notional_cap': 0.}):
        p = tmp_path/'bad.yaml'
        p.write_text(yaml.safe_dump({'base_config': str(Path(CONFIG).resolve()),
            'training': {'financial_transformer': fields}}))
        with pytest.raises(ValueError, match='beta notional cap'):
            load_config(p)


def test_actual_inventory_nav_cost_and_risk_can_change_learned_actions():
    p = packet(1)[0]
    q = torch.tensor([5, 0])
    kw = dict(initial_capital=10_000, alive=torch.tensor(True), advance=torch.tensor(True))
    for feature, changed_state in ((0, 'q'), (8, 'nav'), (5, 'cost'), (14, 'risk')):
        policy = p.clone()
        policy[:, ACCOUNT_COEFFICIENT_START + feature] = .5
        baseline = resolve_futures_account_policy(policy, q, torch.tensor(10_000.), **kw)
        other, other_q, other_nav = policy.clone(), q.clone(), torch.tensor(10_000.)
        if changed_state == 'q':
            other_q.zero_()
        elif changed_state == 'nav':
            other_nav *= 2
        elif changed_state == 'cost':
            other[:, ACCOUNT_SIDE_COST] *= 3
        else:
            other[:, ACCOUNT_RMS] *= 3
        changed = resolve_futures_account_policy(other, other_q, other_nav, **kw)
        assert not torch.equal(baseline, changed), changed_state
        assert changed.abs().sum() <= 1 + 1e-6


def test_current_and_future_execution_cannot_change_the_current_request():
    p, x = packet(feedback=-.4), tape(3, 2)
    a = account(p, x)
    changed = x.clone()
    changed[0, :, 3:5] *= 1.02
    changed[0, :, 4] *= 1.01
    changed[1:, :, 3:5] *= .98
    b = account(p, changed)
    torch.testing.assert_close(a.requested_weights_history[0], b.requested_weights_history[0], rtol=0, atol=0)
    assert not torch.equal(a.requested_weights_history[1], b.requested_weights_history[1])
    # Alter only the future: the complete account prefix must remain identical.
    future = x.clone()
    future[2, :, 3:5] *= 2
    c = account(p, future)
    torch.testing.assert_close(a.requested_weights_history[:2], c.requested_weights_history[:2], rtol=0, atol=0)
    torch.testing.assert_close(a.equity_scale_history[:2], c.equity_scale_history[:2], rtol=0, atol=0)


def test_chunked_state_feedback_and_padding_match_uninterrupted_account():
    p, x = packet(feedback=-.3), tape(3, 2)
    x[..., 5:7] = 1
    x[0, 0, 4] += 40
    x[1, :, margin.PREVIOUS_MARK] = x[0, :, 4]
    full = account(p, x)
    first = account(p[:1], x[:1])
    rest = account(p[1:], x[1:], initial_quantities=first.final_weights,
                   initial_equity_scale=first.final_equity_scale, initial_alive=first.final_alive)
    for field in ('requested_weights_history', 'strategy_returns', 'contract_quantities_history', 'margin_audit_history'):
        torch.testing.assert_close(getattr(rest, field), getattr(full, field)[1:], rtol=0, atol=0)
    padded = account(torch.cat((p[:1], p[:1], p[1:])), torch.cat((x[:1], x[:1], x[1:])),
                     state_advance_mask=torch.tensor([True, False, True, True]))
    torch.testing.assert_close(padded.requested_weights_history[[0, 2, 3]], full.requested_weights_history, rtol=0, atol=0)
    torch.testing.assert_close(padded.final_equity_scale, full.final_equity_scale, rtol=0, atol=0)
    assert padded.requested_weights_history[1].count_nonzero() == 0


def test_absorbing_ruin_and_corporate_inventory_mapping_remain_exact():
    p, x = packet(slots=1, feedback=.2), tape(3, 1)
    x[1, :, 3:5] = 10
    x[1, :, margin.CAN_SELL] = 0
    ruined = account(p, x)
    assert not ruined.final_alive
    assert ruined.requested_weights_history[2].count_nonzero() == 0
    assert ruined.final_equity_scale == 0
    # A known one-to-one corporate conversion must supply the actual new slot.
    p, x = packet(1, feedback=.2), tape(1, 2)
    x = torch.cat((x, torch.zeros(1, 2, margin.MARGIN_CORPORATE_EXECUTION_WIDTH - x.size(-1))), -1)
    x[0, 1, margin.CARRY_SOURCE_SLOT] = 1
    x[0, 1, margin.CARRY_QUANTITY_NUMERATOR] = x[0, 1, margin.CARRY_QUANTITY_DENOMINATOR] = 1
    result = account(p, x, initial_quantities=torch.tensor([3, 0]))
    expected = resolve_futures_account_policy(p[0], torch.tensor([0, 3]), torch.tensor(10_000.),
        initial_capital=10_000, alive=torch.tensor(True), advance=torch.tensor(True))
    torch.testing.assert_close(result.requested_weights_history[0], expected, rtol=0, atol=0)


def test_public_backtest_loss_use_actual_requests_and_finite_response_gradients():
    small = packet(feedback=.1)
    p = torch.zeros(3, 1936, FUTURES_ACCOUNT_PACKET_WIDTH)
    p[:, :2] = small
    p.requires_grad_()
    x = tape(3, 1936)
    x[:, 2:, 1] = 0
    x[0, 0, 4] += 40
    x[0, 1, 4] -= 10
    mask = torch.ones(3, 1936, dtype=torch.bool)
    returns = torch.zeros(3, 1936)
    benchmark = torch.zeros(3)
    kw = dict(execution_mode='tw_stock_context_futures_portfolio', overnight_returns=x,
              portfolio_activation='pre_normalized', day_trade_execution_initial_capital=10_000,
              futures_portfolio_recoverable_backward=True, long_only=False)
    result = run_backtest_torch(p, returns, mask, benchmark, 0., 0., **kw)
    assert result.requested_weights_history.shape == (3, 1936)
    assert result.weights_history.shape == (3, 1936)
    with pytest.raises(ValueError, match='cannot fall back'):
        run_backtest_torch(p, returns, mask, benchmark, 0., 0.,
                           **dict(kw, overnight_returns=x[..., :4]))
    loss = risk_aware_loss(p, returns, mask, benchmark_returns=benchmark, buy_fee_rate=0., sell_fee_rate=0., objective='log_utility',
        execution_mode=kw['execution_mode'], overnight_log_returns=x,
        portfolio_activation='pre_normalized', day_trade_execution_initial_capital=10_000,
        futures_portfolio_recoverable_backward=True, long_only=False, concentration_weight=0., gamma_turnover=0.)
    torch.testing.assert_close(loss.detach(), -252 * result.strategy_returns.mean(), rtol=1e-6, atol=1e-6)
    loss.backward()
    assert torch.isfinite(p.grad).all()
    assert p.grad[..., 1].abs().sum() > 0
    assert p.grad[..., ACCOUNT_COEFFICIENT_START:ACCOUNT_COEFFICIENT_START + 16].abs().sum() > 0


def actor(enabled, net_cap=None, beta_cap=None):
    torch.set_num_threads(2)
    return FinancialTransformerFuturesModel(
        lookback=4, num_features=3, num_symbols=2, d_model=8,
        attention_mode='market_token', num_market_tokens=2, temporal_heads=2,
        temporal_layers=1, temporal_pooling='last', temporal_query_mode='last_only',
        use_symbol_pos=False, portfolio_mode='long_short', portfolio_output_mode='score_entmax_log_cash',
        center_long_short_logits=False, futures_denomination_hard_projection=False,
        futures_margin_budget_output=True, futures_margin_amount_context=True,
        futures_causal_account_policy=enabled, feature_bottleneck_dim=2, dropout=0., return_aux=False,
        futures_net_notional_cap=net_cap,
        futures_beta_notional_cap=beta_cap,
    ).eval()


@pytest.mark.parametrize('net_cap,beta_cap', [(None, None), (0., None), (None, 0.)])
def test_model_seed_base_policy_preserved_and_response_head_learns_through_ledger(net_cap, beta_cap):
    torch.manual_seed(41)
    old = actor(False)
    old_rng = torch.get_rng_state()
    torch.manual_seed(41)
    new = actor(True, net_cap=net_cap, beta_cap=beta_cap)
    assert torch.equal(old_rng, torch.get_rng_state())
    for key, value in old.state_dict().items():
        torch.testing.assert_close(value, new.state_dict()[key], rtol=0, atol=0)
    features = torch.zeros(2, 1936, len(TW_STOCK_CONTEXT_FUTURES_MODEL_FEATURE_COLUMNS) + 3)
    features[:, :2, 1:18] = torch.randn(2, 2, 17)
    features[..., -3:] = torch.tensor([100., .1, .75])
    features[..., len(TW_STOCK_CONTEXT_FUTURES_MODEL_FEATURE_COLUMNS) - 1] = 1004
    mask = torch.zeros(2, 1936, dtype=torch.bool)
    mask[:, :2] = True
    x, stock_mask = torch.randn(2, 4, 2, 3), torch.ones(2, 2, dtype=torch.bool)
    a = old(x, stock_mask, portfolio_context=dict(candidate_features=features, candidate_mask=mask))
    expanded = torch.cat((features, torch.full_like(features[..., :1], .02), torch.ones_like(features[..., :1])), -1)
    if beta_cap is not None:
        beta = torch.ones_like(features[..., :1]); beta[:, :2, 0] = torch.tensor([2., .5])
        expanded = torch.cat((expanded, beta), -1)
    b = new(x, stock_mask, portfolio_context=dict(candidate_features=expanded, candidate_mask=mask))
    width = futures_account_policy_width(net_cap, beta_cap)
    assert b.shape == (2, 1936, width)
    if net_cap is None and beta_cap is None:
        torch.testing.assert_close(a, b[..., 0], rtol=1e-6, atol=1e-7)
    elif net_cap is not None:
        torch.testing.assert_close(b[..., 0].sum(-1), torch.zeros(2), rtol=0, atol=1e-6)
    else:
        torch.testing.assert_close((b[..., 0] * b[..., ACCOUNT_TX_BETA]).sum(-1), torch.zeros(2), rtol=0, atol=1e-6)
    assert a.abs().sum() > 0
    execution = tape(2, 1936)
    execution[:, 2:, 1] = 0
    execution[0, 0, 4] += 40
    execution[0, 1, 4] -= 10
    result = account(b, execution, recoverable_backward=True)
    (-result.strategy_returns.mean()).backward()
    for parameter in (new.futures_action_head.weight, new.futures_account_response_head.weight,
                      new.futures_continuous_encoder[0].weight):
        assert torch.isfinite(parameter.grad).all()
        assert parameter.grad.abs().sum() > 0


@pytest.mark.parametrize('bad', [-.1, float('nan'), float('inf'), True])
def test_invalid_requested_net_cap_is_rejected(bad):
    with pytest.raises(ValueError, match='net notional cap'):
        actor(True, net_cap=bad)
    with pytest.raises(ValueError, match='net notional cap'):
        packet(net_cap=bad)


def test_risk_observation_is_prior_only_and_missing_rows_are_not_zero_returns(tmp_path):
    panel, rows = rule_panel(tmp_path)
    source = panel.stock_context_futures_portfolio_daily.source_path
    pl.read_parquet(source).with_columns(
        pl.Series('source_row_observed', [True, True, False]),
        pl.Series('same_contract_as_previous_session', [False, True, True]),
        pl.Series('taifex_settlement_logret_1d', [0., .1, .9]),
    ).write_parquet(source)
    path = write_rules(tmp_path, panel, rows)
    attached = margin.attach_futures_margin_rules(panel, path, include_margin_amount=True,
                    include_account_policy_observations=True, account_policy_lookback=2)
    risk = attached.stock_context_futures_portfolio_daily.candidate_features[:, 0, -2:]
    np.testing.assert_allclose(risk, [[0, 0], [0, 0], [.1, .5]])
    from stockagent.training.dataset import CrossSectionalDataset
    from stockagent.training.windowed import dataset_to_windowed_tensors
    split = dataset_to_windowed_tensors(CrossSectionalDataset(attached, np.arange(3), lookback=1,
                                            execution_mode='tw_stock_context_futures_portfolio'))
    assert split.derivative_candidate_features.shape[-1] == 27
    np.testing.assert_array_equal(split.derivative_candidate_features.numpy(),
                                  attached.stock_context_futures_portfolio_daily.candidate_features)
    pl.read_parquet(source).with_columns(pl.when(pl.col('date') == pl.col('date').max())
        .then(2.).otherwise(pl.col('taifex_settlement_logret_1d')).alias('taifex_settlement_logret_1d')).write_parquet(source)
    path = write_rules(tmp_path, panel, rows)
    changed = margin.attach_futures_margin_rules(panel, path, include_margin_amount=True,
                    include_account_policy_observations=True, account_policy_lookback=2)
    np.testing.assert_array_equal(attached.stock_context_futures_portfolio_daily.candidate_features,
                                 changed.stock_context_futures_portfolio_daily.candidate_features)
    returns = np.array([[.1], [9], [.2], [.3]], dtype=np.float32)
    observed = np.array([[True], [False], [True], [True]])
    result = margin.prior_observed_return_risk(returns, observed, lookback=2)
    np.testing.assert_allclose(result[:, 0, 1], [.5, .5, .5, 1])
    np.testing.assert_allclose(result[:3, 0, 0], [.1, .1, .2], rtol=1e-6)


def test_prior_beta_matches_pairwise_ols_with_missingness_and_explicit_prior():
    rng = np.random.default_rng(28)
    benchmark = rng.normal(0, .02, 30)
    returns = benchmark[:, None] * np.array([2., -.5, 0.]) + rng.normal(0, .005, (30, 3))
    observed = rng.uniform(size=returns.shape) > .2
    benchmark_observed = rng.uniform(size=benchmark.shape) > .1
    returns[4, 1] = np.nan
    benchmark[8] = np.nan
    expected = np.ones_like(returns)
    for end in range(30):
        begin = max(0, end - 6)
        for slot in range(3):
            valid = (observed[begin:end+1, slot] & benchmark_observed[begin:end+1]
                     & np.isfinite(returns[begin:end+1, slot]) & np.isfinite(benchmark[begin:end+1]))
            x = np.expm1(returns[begin:end+1, slot][valid])
            y = np.expm1(benchmark[begin:end+1][valid])
            if len(y) >= 2 and np.var(y) > 1e-12:
                cov = np.sum((x-x.mean()) * (y-y.mean()))
                expected[end, slot] = (cov + 8*np.var(y)) / (np.sum((y-y.mean())**2) + 8*np.var(y))
    result = margin.prior_observed_tx_beta(returns, observed, benchmark, benchmark_observed, lookback=7)
    np.testing.assert_allclose(result, expected, rtol=1e-6, atol=1e-7)
    future = returns.copy(); future[20:] = 4.
    changed = margin.prior_observed_tx_beta(future, observed, benchmark, benchmark_observed, lookback=7)
    np.testing.assert_array_equal(changed[:20], result[:20])
    unknown = margin.prior_observed_tx_beta(returns, observed, np.zeros(30), np.zeros(30, bool), lookback=7)
    np.testing.assert_array_equal(unknown, np.ones_like(result))


def test_beta_observation_uses_completed_sessions_without_changing_execution(tmp_path):
    panel, rows = rule_panel(tmp_path)
    daily = panel.stock_context_futures_portfolio_daily
    pl.read_parquet(daily.source_path).with_columns(
        pl.Series('source_row_observed', [True, True, True]),
        pl.Series('same_contract_as_previous_session', [True, True, True]),
        pl.Series('taifex_settlement_logret_1d', [.02, -.04, .5]),
    ).write_parquet(daily.source_path)
    daily = replace(daily, benchmark_log_returns=np.array([.01, -.02, .9], np.float32),
                    benchmark_observed_returns_mask=np.ones(3, bool))
    panel = replace(panel, stock_context_futures_portfolio_daily=daily)
    rules = write_rules(tmp_path, panel, rows)
    common = dict(include_margin_amount=True, include_account_policy_observations=True,
                  account_policy_lookback=2, benchmark_mode='tx_front_rolling_1x_gross')
    old = margin.attach_futures_margin_rules(panel, rules, **common)
    new = margin.attach_futures_margin_rules(panel, rules, include_beta_observation=True, **common)
    a, b = old.stock_context_futures_portfolio_daily, new.stock_context_futures_portfolio_daily
    np.testing.assert_array_equal(a.integer_execution, b.integer_execution)
    np.testing.assert_array_equal(a.candidate_features, b.candidate_features[..., :-1])
    # At row 2, only the previous observed same-contract return exists in this fixture.
    np.testing.assert_array_equal(b.candidate_features[:2, :, -1], 1.)
    future = replace(daily, benchmark_log_returns=np.array([.01, -.02, -3.], np.float32))
    changed = margin.attach_futures_margin_rules(replace(panel, stock_context_futures_portfolio_daily=future),
                                                rules, include_beta_observation=True, **common)
    np.testing.assert_array_equal(b.candidate_features,
                                 changed.stock_context_futures_portfolio_daily.candidate_features)


def test_policy_and_observations_have_distinct_resume_contract(tmp_path):
    c = load_config(CONFIG)
    new = build_checkpoint_manifest(_stock_panel(), c, include_data_content=False)
    c.training.financial_transformer.futures_causal_account_policy = False
    old = build_checkpoint_manifest(_stock_panel(), c, include_data_content=False)
    for key in ('model', 'trading'):
        assert new['fingerprints'][key] != old['fingerprints'][key]
    with pytest.raises(RuntimeError, match='semantic fingerprint mismatch'):
        _validate_checkpoint_manifest({'experiment_manifest': old}, new,
                                        checkpoint_path=tmp_path/'old.pt', scope='resume')
    values = asdict(c.training.financial_transformer)
    legacy = dict(values)
    legacy.pop('futures_causal_account_policy')
    assert _project_temporal_basis_model_config(values) == _project_temporal_basis_model_config(legacy)


def test_canonical_chunked_evaluator_exports_resolved_actions_instead_of_packets():
    from stockagent.training.trainer import (
        _ExecutionRuntime, TimingBreakdown, _run_eval_backtest_from_weight_buffers,
    )
    p = torch.zeros(3, 1936, FUTURES_ACCOUNT_PACKET_WIDTH)
    p[:, :2] = packet(feedback=-.2)
    x = tape(3, 1936)
    x[:, 2:, 1] = 0
    x[0, 0, 4] += 40
    mask = torch.ones(3, 1936, dtype=torch.bool)
    zeros = torch.zeros_like(mask)
    result, _ = _run_eval_backtest_from_weight_buffers(
        p, torch.zeros(3, 1936), mask, mask, mask, mask, zeros, zeros, torch.zeros(3),
        device=torch.device('cpu'), non_blocking=False, long_only=False,
        buy_fee_rate=0., sell_fee_rate=0., max_turnover_ratio=0., gross_leverage=1.,
        min_trade_weight=0., backtest_chunk_rows=2, compute_metrics_summary=False,
        return_weights_history=True, profile_timing=False, progress_label=None,
        timing=TimingBreakdown(), reset_at_rows=None, portfolio_activation='pre_normalized',
        overnight_log_returns_all=x,
        execution_runtime=_ExecutionRuntime(mode='tw_stock_context_futures_portfolio',
            buy_fee_rates=None, sell_fee_rates=None, lot_sizes=None,
            settlement_lag_sessions=0, futures_initial_capital=10_000.),
    )
    expected = account(p, x)
    assert result.requested_weights_history.shape == (3, 1936)
    torch.testing.assert_close(result.futures_account_policy_packet, p, rtol=0, atol=0)
    assert result.weights_history.shape == (3, 1936)
    # The canonical chunk boundary serializes NAV as a scale then restores TWD;
    # allow FP32 roundoff, but executed whole contracts must be identical.
    torch.testing.assert_close(result.requested_weights_history, expected.requested_weights_history, rtol=1e-6, atol=1e-7)
    torch.testing.assert_close(result.weights_history, expected.contract_quantities_history.float(), rtol=0, atol=0)
    torch.testing.assert_close(result.strategy_returns, expected.strategy_returns, rtol=1e-6, atol=1e-7)


@pytest.mark.parametrize('net_cap,beta_cap', [(None, None), (100., None), (None, 100.)])
def test_stitched_policy_resolves_again_from_carried_book_and_nav(tmp_path, monkeypatch, net_cap, beta_cap):
    from types import SimpleNamespace
    from stockagent.training import trainer as trainer
    panel, rules = rule_panel(tmp_path)
    panel = margin.attach_futures_margin_rules(panel, write_rules(tmp_path, panel, rules),
                                               include_margin_amount=True)
    execution = tape(3, 1936)
    execution[:, 1:] = 0
    execution[0, 0, 4] = 1040
    execution[1:, 0, 3:5] = 1040
    execution[1:, 0, margin.PREVIOUS_MARK] = 1040
    panel.stock_context_futures_portfolio_daily = replace(
        panel.stock_context_futures_portfolio_daily, integer_execution=execution.numpy())
    small = packet(slots=1, feedback=-.3, net_cap=net_cap, beta_cap=beta_cap)
    p = torch.zeros(3, 1936, small.shape[-1])
    p[:, :1] = small
    if net_cap is not None:
        p[..., -1] = net_cap
    p[:, :1, ACCOUNT_COEFFICIENT_START + 8] = .9
    config = load_config(CONFIG)
    config.training.financial_transformer.futures_net_notional_cap = net_cap
    config.training.financial_transformer.futures_beta_notional_cap = beta_cap
    config.trading.tw_futures_portfolio_integer_initial_capital = 10_000.
    config.trading.volume_participation_equity = 10_000.
    config.trading.max_volume_participation = 0.
    config.trading.tw_futures_portfolio_benchmark_mode = 'flat_cash'
    for name in ('plot_equity_curve', 'plot_equity_curve_log', 'plot_annual_performance'):
        monkeypatch.setattr(trainer, name, lambda *a, **kw: None)

    def evaluate(policy, x):
        rows = len(policy)
        return run_backtest_torch(policy, torch.zeros(rows, 1936),
            torch.ones(rows, 1936, dtype=torch.bool), torch.zeros(rows), 0., 0.,
            execution_mode='tw_stock_context_futures_portfolio', overnight_returns=x,
            portfolio_activation='pre_normalized', day_trade_execution_initial_capital=10_000.,
            long_only=False).to_numpy()

    for fold_id, sl in ((1, slice(0, 1)), (2, slice(1, 3))):
        (tmp_path/f'fold_{fold_id:02d}').mkdir()
        trainer._save_deployment_test_artifacts(tmp_path/f'fold_{fold_id:02d}',
            evaluate(p[sl], execution[sl]), panel.dates[sl],
            symbols=list(panel.stock_context_futures_portfolio_daily.symbols),
            backtest_artifact_compression='none', write_plots=False)
    standalone_second, _ = trainer._load_backtest_artifact(tmp_path/'fold_02/deployment_test_backtest.npz')
    results = [SimpleNamespace(fold_id=1), SimpleNamespace(fold_id=2)]
    stitched = trainer._replay_taiwan_stitched_deployment(tmp_path, results, panel=panel, config=config)
    expected = evaluate(p, execution)
    np.testing.assert_array_equal(stitched.futures_contract_quantities_history,
                                  expected.futures_contract_quantities_history)
    np.testing.assert_allclose(stitched.requested_weights_history,
                              expected.requested_weights_history, rtol=0, atol=0)
    assert not np.allclose(stitched.requested_weights_history[1],
                           standalone_second.requested_weights_history[0])
    reloaded, dates = trainer._load_backtest_artifact(tmp_path/'fold_02/deployment_test_backtest.npz')
    np.testing.assert_array_equal(reloaded.futures_account_policy_packet, p[1:].numpy())
    np.testing.assert_array_equal(dates, panel.dates[1:])
    np.testing.assert_allclose(reloaded.futures_margin_audit[0, 0], expected.futures_margin_audit[1, 0])
    # An old open-loop artifact must fail closed, even if its NAV path is valid.
    trainer._save_backtest_artifact(tmp_path/'fold_02/deployment_test_backtest.npz',
        reloaded, dates, persist_futures_account_policy=False)
    with pytest.raises(RuntimeError, match='owned policy packet'):
        trainer._replay_taiwan_stitched_deployment(tmp_path, results, panel=panel, config=config)


def test_unadmitted_actual_inventory_still_contributes_to_global_risk():
    from stockagent.models.futures_account_policy import futures_account_state_features
    p = packet(1)[0]
    p[1].zero_()
    q = torch.tensor([0, 5])
    marks = torch.tensor([[1000., 100., 75.], [1000., 100., 75.]])
    state = futures_account_state_features(p, q, torch.tensor(10_000.),
                    initial_capital=10_000, previous_inventory_marks=marks)
    torch.testing.assert_close(state[0, 9], torch.tensor(.5).asinh())
    torch.testing.assert_close(state[0, 11], torch.tensor(.05).asinh())


@pytest.mark.skipif(not torch.cuda.is_available(), reason='CUDA unavailable')
def test_cuda_graph_amp_account_values_and_feedback_gradients_match_eager(monkeypatch):
    from stockagent.backtest.futures_cuda_graph import clear_futures_cuda_graph_cache
    outputs = []
    for graph in (False, True):
        monkeypatch.setenv('STOCKAGENT_FUTURES_CUDA_GRAPH', str(int(graph)))
        p, x = packet(feedback=-.2, device='cuda').requires_grad_(), tape(3, 2).cuda()
        x[0, 0, 4] += 40
        x[0, 1, 4] -= 10
        with torch.autocast('cuda', dtype=torch.bfloat16):
            result = account(p, x, recoverable_backward=True)
            loss = -result.strategy_returns.mean()
        loss.backward()
        outputs.append((result.strategy_returns.detach(), result.contract_quantities_history,
                        result.requested_weights_history, p.grad))
    for eager, graphed in zip(*outputs, strict=True):
        torch.testing.assert_close(eager, graphed, rtol=1e-6, atol=1e-7)
    assert torch.isfinite(outputs[1][-1]).all()
    assert outputs[1][-1][..., ACCOUNT_COEFFICIENT_START:ACCOUNT_COEFFICIENT_START + 16].abs().sum() > 0
    clear_futures_cuda_graph_cache()
