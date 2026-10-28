"""Factor signals must stay causal and must never alter the financial tape."""
from dataclasses import replace
from pathlib import Path

import numpy as np
import polars as pl
import pytest
import torch
import yaml

from stockagent.config import load_config
from stockagent.data import tw_futures_margin as margin
from stockagent.models.financial_transformer_futures import FinancialTransformerFuturesModel
from stockagent.data.tw_stock_context_futures_portfolio import TW_STOCK_CONTEXT_FUTURES_MODEL_FEATURE_COLUMNS
from stockagent.training.checkpoint_contract import build_checkpoint_manifest, _validate_checkpoint_manifest
from test_tw_futures_margin import rule_panel, write_rules, tape
from test_tw_stock_context_futures_portfolio import _stock_panel
from test_futures_causal_account_policy import account


def test_residual_statistics_match_independent_window_oracle_with_missing_pairs():
    rng = np.random.default_rng(312)
    b = rng.normal(0, .015, 47)
    r = b[:, None]*np.array([1.7, -.4, .5]) + rng.normal(0, .007, (47, 3))
    observed = rng.uniform(size=r.shape) > .18
    bm = rng.uniform(size=b.shape) > .12
    r[4, 1] = np.nan
    b[8] = np.nan
    result = margin.prior_observed_tx_residual_signals(r, observed, b, bm, lookback=11)
    beta = margin.prior_observed_tx_beta(r, observed, b, bm, lookback=11)
    expected = np.zeros_like(result)
    for end in range(len(r)):
        for slot in range(3):
            def pairs(window):
                rows = slice(max(0, end-window+1), end+1)
                valid = observed[rows, slot] & bm[rows] & np.isfinite(r[rows, slot]) & np.isfinite(b[rows])
                return np.expm1(r[rows, slot][valid]), np.expm1(b[rows][valid])

            loading = float(beta[end, slot])
            x, y = pairs(11)
            rms = np.sqrt(np.mean((x-loading*y)**2)) if len(x) else 0.
            mrms = np.sqrt(np.mean(y*y)) if len(y) else 0.
            def mean(window, market=False):
                u, v = pairs(window)
                denom = mrms if market else rms
                return (np.mean(v if market else u-loading*v)/denom
                        if len(u) and denom > 1e-8 else 0.)
            expected[end, slot] = [loading, mean(1), mean(5), mean(20), 100*rms,
                len(pairs(5)[0])/5, len(pairs(20)[0])/20, len(pairs(1)[0]),
                mean(1, True), mean(20, True)]
    np.testing.assert_allclose(result, expected, rtol=2e-6, atol=2e-6)
    future_r, future_b = r.copy(), b.copy()
    future_r[30:] = 1.5
    future_b[30:] = -.8
    changed = margin.prior_observed_tx_residual_signals(future_r, observed, future_b, bm, lookback=11)
    np.testing.assert_array_equal(result[:30], changed[:30])


def test_missing_or_constant_market_has_explicit_prior_and_no_fake_observations():
    r = np.full((9, 2), np.nan)
    result = margin.prior_observed_tx_residual_signals(r, np.zeros_like(r, bool),
        np.zeros(9), np.zeros(9, bool), lookback=4)
    np.testing.assert_array_equal(result[..., 0], 1.)
    np.testing.assert_array_equal(result[..., 1:], 0.)
    zero = margin.prior_observed_tx_residual_signals(np.zeros((9, 2)), np.ones((9, 2), bool),
        np.zeros(9), np.ones(9, bool), lookback=4)
    assert np.isfinite(zero).all()
    np.testing.assert_array_equal(zero[..., 1:5], 0.)
    np.testing.assert_array_equal(zero[..., 8:], 0.)
    np.testing.assert_array_equal(zero[-1, :, 5], 1.)
    np.testing.assert_allclose(zero[-1, :, 6], 9/20, rtol=1e-7)


def test_residual_sidecar_shifts_once_and_preserves_execution_and_existing_features(tmp_path):
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
    kw = dict(include_margin_amount=True, include_account_policy_observations=True,
              account_policy_lookback=2, benchmark_mode='tx_front_rolling_1x_gross')
    a = margin.attach_futures_margin_rules(panel, rules, **kw).stock_context_futures_portfolio_daily
    b = margin.attach_futures_margin_rules(panel, rules, include_residual_signals=True,
                                         **kw).stock_context_futures_portfolio_daily
    width = len(margin.FUTURES_RESIDUAL_SIGNAL_COLUMNS)
    np.testing.assert_array_equal(a.integer_execution, b.integer_execution)
    np.testing.assert_array_equal(a.candidate_mask, b.candidate_mask)
    np.testing.assert_array_equal(a.candidate_features, b.candidate_features[..., :-width])
    # Today's/future settlement and benchmark cannot enter today's policy.
    pl.read_parquet(daily.source_path).with_columns(
        pl.Series('taifex_settlement_logret_1d', [.02, -.04, -3.]),
    ).write_parquet(daily.source_path)
    future = replace(daily, benchmark_log_returns=np.array([.01, -.02, -3.], np.float32))
    rules = write_rules(tmp_path, panel, rows)
    c = margin.attach_futures_margin_rules(replace(panel, stock_context_futures_portfolio_daily=future),
        rules, include_residual_signals=True, **kw).stock_context_futures_portfolio_daily
    np.testing.assert_array_equal(b.candidate_features, c.candidate_features)


def model(residual=False, beta_cap=None, joint_tokens=4):
    return FinancialTransformerFuturesModel(lookback=4, num_features=3, num_symbols=2,
        d_model=8, attention_mode='market_token', num_market_tokens=2, temporal_heads=2,
        temporal_layers=1, temporal_pooling='last', temporal_query_mode='last_only',
        use_symbol_pos=False, portfolio_mode='long_short', portfolio_output_mode='score_entmax_log_cash',
        center_long_short_logits=False, futures_denomination_hard_projection=False,
        futures_margin_budget_output=True, futures_margin_amount_context=True,
        futures_causal_account_policy=True, feature_bottleneck_dim=2, dropout=0., return_aux=False,
        futures_beta_notional_cap=beta_cap, futures_residual_signal_context=residual,
        futures_joint_market_tokens=joint_tokens).eval()


@pytest.mark.parametrize('beta', [False, True])
@pytest.mark.parametrize('opening_reference', [False, True])
def test_residual_context_survives_dataset_windowed_transport_and_slot_layouts(
    tmp_path, beta, opening_reference,
):
    from stockagent.data.tw_futures_decision_clock import OPENING_REFERENCE_FEATURES
    from stockagent.training.dataset import CrossSectionalDataset
    from stockagent.training.windowed import dataset_to_windowed_tensors

    panel, rows = rule_panel(tmp_path)
    source = panel.stock_context_futures_portfolio_daily.source_path
    pl.read_parquet(source).with_columns(
        pl.Series('source_row_observed', [True, True, True]),
        pl.Series('same_contract_as_previous_session', [True, True, True]),
        pl.Series('taifex_settlement_logret_1d', [.02, -.04, .05]),
    ).write_parquet(source)
    daily = replace(panel.stock_context_futures_portfolio_daily,
        benchmark_log_returns=np.array([.01, -.02, .03], np.float32),
        benchmark_observed_returns_mask=np.ones(3, bool))
    panel = replace(panel, stock_context_futures_portfolio_daily=daily)
    rules = write_rules(tmp_path, panel, rows)
    attached = margin.attach_futures_margin_rules(panel, rules, include_margin_amount=True,
        include_account_policy_observations=True, include_beta_observation=beta,
        include_residual_signals=True, benchmark_mode='tx_front_rolling_1x_gross')
    daily = attached.stock_context_futures_portfolio_daily
    if opening_reference:
        extra = np.zeros((*daily.candidate_mask.shape, len(OPENING_REFERENCE_FEATURES)), np.float32)
        daily = replace(daily, candidate_features=np.concatenate((daily.candidate_features, extra), -1))
        attached = replace(attached, stock_context_futures_portfolio_daily=daily)
    split = dataset_to_windowed_tensors(CrossSectionalDataset(attached, np.arange(3),
        lookback=1, execution_mode='tw_stock_context_futures_portfolio'))
    np.testing.assert_array_equal(split.derivative_candidate_features.numpy(), daily.candidate_features)
    np.testing.assert_array_equal(split.derivative_candidate_mask.numpy(), daily.candidate_mask)
    np.testing.assert_array_equal(split.overnight_log_returns.numpy(), daily.integer_execution)
    # Exercise the versioned larger layout rejected in the actual remote run,
    # plus dataclass reconstruction used by cache and split transformations.
    for slots in (1936, 2816):
        def pad(value):
            shape = list(value.shape)
            shape[1] = slots - value.shape[1]
            return torch.cat((value, torch.zeros(shape, dtype=value.dtype)), 1)
        expanded = replace(split, derivative_candidate_features=pad(split.derivative_candidate_features),
            derivative_candidate_mask=pad(split.derivative_candidate_mask),
            overnight_log_returns=pad(split.overnight_log_returns))
        assert expanded.derivative_candidate_features.shape[1] == slots
        restored = replace(expanded)
        torch.testing.assert_close(restored.derivative_candidate_features,
                                   expanded.derivative_candidate_features, rtol=0, atol=0)
        with pytest.raises(ValueError, match='model context must have shape'):
            replace(expanded, derivative_candidate_features=torch.nn.functional.pad(
                expanded.derivative_candidate_features, (0, 7)))


@pytest.mark.parametrize('beta_cap', [None, 0.])
@pytest.mark.parametrize('joint_tokens', [4, 16])
def test_zero_signal_encoder_preserves_control_initialization_and_learns_through_exact_account(beta_cap, joint_tokens):
    torch.manual_seed(41)
    a = model(beta_cap=beta_cap, joint_tokens=joint_tokens)
    rng = torch.get_rng_state()
    torch.manual_seed(41)
    b = model(True, beta_cap, joint_tokens=joint_tokens)
    assert torch.equal(rng, torch.get_rng_state())
    for key, value in a.state_dict().items():
        torch.testing.assert_close(value, b.state_dict()[key], rtol=0, atol=0)
    width = len(TW_STOCK_CONTEXT_FUTURES_MODEL_FEATURE_COLUMNS) + 5 + int(beta_cap is not None)
    features = torch.zeros(2, 1936, width)
    features[:, :2, 1:18] = torch.randn(2, 2, 17)
    start = len(TW_STOCK_CONTEXT_FUTURES_MODEL_FEATURE_COLUMNS)
    features[..., start:start+5] = torch.tensor([100., .1, .75, .02, 1.])
    features[..., start-1] = 1004.
    if beta_cap is not None:
        features[..., -1] = 1.
    mask = torch.zeros(2, 1936, dtype=torch.bool)
    mask[:, :2] = True
    x, stock_mask = torch.randn(2, 4, 2, 3), torch.ones(2, 2, dtype=torch.bool)
    output_a = a(x, stock_mask, portfolio_context=dict(candidate_features=features, candidate_mask=mask))
    signals = torch.randn(2, 1936, len(margin.FUTURES_RESIDUAL_SIGNAL_COLUMNS))
    expanded = torch.cat((features, signals), -1)
    output_b = b(x, stock_mask, portfolio_context=dict(candidate_features=expanded, candidate_mask=mask))
    torch.testing.assert_close(output_a, output_b, rtol=0, atol=0)
    execution = tape(2, 1936)
    execution[:, 2:, 1] = 0
    execution[0, 0, 4] += 40
    result = account(output_b, execution, recoverable_backward=True)
    (-result.strategy_returns.sum()).backward()
    grad = b.futures_residual_signal_encoder.weight.grad
    assert grad is not None and torch.isfinite(grad).all() and grad.abs().sum() > 0
    factor_grad = b.futures_joint_market_queries.grad
    assert factor_grad is not None and torch.isfinite(factor_grad).all() and factor_grad.abs().sum() > 0
    with pytest.raises(ValueError, match='shape|context contract'):
        b(x, stock_mask, portfolio_context=dict(candidate_features=features, candidate_mask=mask))


def test_config_and_resume_fingerprint_distinguish_signal_information(tmp_path):
    source = Path('configs/markets/tw_futures_v8_margin_components_causal_account_capital100m_v5.yaml').resolve()
    config = load_config(source)
    old = build_checkpoint_manifest(_stock_panel(), config, include_data_content=False)
    selected = tmp_path/'signals.yaml'
    selected.write_text(yaml.safe_dump({'base_config': str(source), 'training': {
        'financial_transformer': {'futures_residual_signal_context': True}}}))
    config = load_config(selected)
    new = build_checkpoint_manifest(_stock_panel(), config, include_data_content=False)
    assert old['fingerprints']['model'] != new['fingerprints']['model']
    assert old['fingerprints']['trading'] != new['fingerprints']['trading']
    section = new['contracts']['trading']['taiwan_stock_context_futures_portfolio']
    assert section['residual_signal_context']['contract'] == margin.FUTURES_RESIDUAL_SIGNAL_CONTRACT
    for name in margin.FUTURES_RESIDUAL_SIGNAL_COLUMNS:
        assert name in section['candidate_feature_columns']
    with pytest.raises(RuntimeError, match='semantic fingerprint mismatch'):
        _validate_checkpoint_manifest({'experiment_manifest': old}, new,
            checkpoint_path=tmp_path/'old.pt', scope='resume')


def test_joint_market_token_shape_is_configured_and_rejects_incompatible_resume(tmp_path):
    from stockagent.models.factory import build_model
    from stockagent.models.transformer_base_portfolio import _temporal_basis_matrix

    source = Path('configs/markets/tw_futures_v8_margin_components_causal_account_capital100m_v5.yaml').resolve()
    config = load_config(source)
    panel = _stock_panel()
    old = build_checkpoint_manifest(panel, config, include_data_content=False)
    selected = tmp_path/'factors.yaml'
    selected.write_text(yaml.safe_dump({'base_config': str(source), 'training': {
        'financial_transformer': {'futures_joint_market_tokens': 16}}}))
    config = load_config(selected)
    new = build_checkpoint_manifest(panel, config, include_data_content=False)
    assert old['fingerprints']['model'] != new['fingerprints']['model']
    with pytest.raises(RuntimeError, match='semantic fingerprint mismatch'):
        _validate_checkpoint_manifest({'experiment_manifest': old}, new,
            checkpoint_path=tmp_path/'old.pt', scope='resume')
    built = build_model(config=config, lookback=config.training.lookback,
        num_features=len(panel.feature_names), num_symbols=panel.num_symbols,
        feature_names=panel.feature_names, temporal_basis_overrides={'pca_klt':
            _temporal_basis_matrix('dct', steps=config.training.lookback, components=31)})
    assert built.futures_joint_market_queries.shape == (1, 16, config.training.financial_transformer.d_model)
    for bad in (True, 0, 65, 4.5):
        selected.write_text(yaml.safe_dump({'base_config': str(source), 'training': {
            'financial_transformer': {'futures_joint_market_tokens': bad}}}))
        with pytest.raises(ValueError, match='futures_joint_market_tokens'):
            load_config(selected)
