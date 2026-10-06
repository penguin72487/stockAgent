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
    pack_futures_account_policy, resolve_futures_account_policy,
)
from stockagent.models.normalization import masked_score_entmax_log_cash_weights
from stockagent.training.checkpoint_contract import (
    _project_temporal_basis_model_config, _validate_checkpoint_manifest, build_checkpoint_manifest,
)
from stockagent.training.loss import risk_aware_loss
from test_tw_futures_margin import tape, rule_panel, write_rules
from test_tw_stock_context_futures_portfolio import _stock_panel

CONFIG = 'configs/markets/tw_futures_v8_margin_components_causal_account_capital100m_v5.yaml'


def packet(rows=3, slots=2, *, feedback=0.0, device='cpu'):
    logits = torch.tensor([.4, -.2], device=device)[:slots].expand(rows, slots).clone()
    coefficients = torch.zeros(rows, slots, len(FUTURES_ACCOUNT_STATE_COLUMNS), device=device)
    coefficients[..., 0] = feedback
    mask = torch.ones(rows, slots, dtype=torch.bool, device=device)
    base = masked_score_entmax_log_cash_weights(logits, mask)
    ones = torch.ones_like(base)
    return pack_futures_account_policy(base, logits, coefficients, mask,
        ones * 1000, ones * 100, ones * 75, ones * 2, ones * .02, ones)


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


def actor(enabled):
    torch.set_num_threads(2)
    return FinancialTransformerFuturesModel(
        lookback=4, num_features=3, num_symbols=2, d_model=8,
        attention_mode='market_token', num_market_tokens=2, temporal_heads=2,
        temporal_layers=1, temporal_pooling='last', temporal_query_mode='last_only',
        use_symbol_pos=False, portfolio_mode='long_short', portfolio_output_mode='score_entmax_log_cash',
        center_long_short_logits=False, futures_denomination_hard_projection=False,
        futures_margin_budget_output=True, futures_margin_amount_context=True,
        futures_causal_account_policy=enabled, feature_bottleneck_dim=2, dropout=0., return_aux=False,
    ).eval()


def test_model_seed_base_policy_preserved_and_response_head_learns_through_ledger():
    torch.manual_seed(41)
    old = actor(False)
    old_rng = torch.get_rng_state()
    torch.manual_seed(41)
    new = actor(True)
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
    b = new(x, stock_mask, portfolio_context=dict(candidate_features=expanded, candidate_mask=mask))
    assert b.shape == (2, 1936, FUTURES_ACCOUNT_PACKET_WIDTH)
    torch.testing.assert_close(a, b[..., 0], rtol=1e-6, atol=1e-7)
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


def test_stitched_policy_resolves_again_from_carried_book_and_nav(tmp_path, monkeypatch):
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
    p = torch.zeros(3, 1936, FUTURES_ACCOUNT_PACKET_WIDTH)
    p[:, :1] = packet(slots=1, feedback=-.3)
    p[:, :1, ACCOUNT_COEFFICIENT_START + 8] = .9
    config = load_config(CONFIG)
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
