"""Cash initialization must preserve a trainable, unconstrained policy head."""
from dataclasses import asdict

import pytest
import torch

from stockagent.backtest.tw_futures_portfolio import run_tw_futures_portfolio_integer_torch
from stockagent.config import load_config, _load_raw_config
from stockagent.data import tw_futures_margin as margin
from stockagent.data.tw_stock_context_futures_portfolio import TW_STOCK_CONTEXT_FUTURES_MODEL_FEATURE_COLUMNS
from stockagent.models.financial_transformer_futures import FinancialTransformerFuturesModel
from stockagent.training.checkpoint_contract import _project_temporal_basis_model_config, build_checkpoint_manifest, _validate_checkpoint_manifest
from test_tw_futures_margin import tape
from test_tw_stock_context_futures_portfolio import _stock_panel


CONFIG = 'configs/markets/tw_futures_v8_margin_verified_2011_capital100m_tx_front_roll_cash_init_v6.yaml'


def model(flat=True):
    torch.set_num_threads(2)
    return FinancialTransformerFuturesModel(
        lookback=4, num_features=3, num_symbols=2, d_model=8,
        attention_mode='market_token', num_market_tokens=2,
        temporal_heads=2, temporal_layers=1, temporal_pooling='last',
        temporal_query_mode='last_only', use_symbol_pos=False,
        portfolio_mode='long_short', portfolio_output_mode='score_entmax_log_cash',
        center_long_short_logits=False, futures_denomination_hard_projection=False,
        futures_margin_budget_output=True, feature_bottleneck_dim=2,
        futures_flat_action_initialization=flat, dropout=0., return_aux=False,
    ).eval()


def inputs():
    x = torch.randn(2, 4, 2, 3)
    features = torch.zeros(2, 1936, len(TW_STOCK_CONTEXT_FUTURES_MODEL_FEATURE_COLUMNS) + 2)
    features[:, :2, 1:18] = torch.randn(2, 2, 17)
    features[..., -2:] = torch.tensor([.1, .75])
    mask = torch.zeros(2, 1936, dtype=torch.bool)
    mask[:, :2] = True
    return x, torch.ones(2, 2, dtype=torch.bool), dict(candidate_features=features, candidate_mask=mask)


def test_flat_start_is_exact_cash_with_trainable_head_and_then_backbone():
    torch.manual_seed(119)
    m = model()
    x, stock_mask, context = inputs()
    weights = m(x, stock_mask, portfolio_context=context)
    assert weights.count_nonzero() == 0
    execution = tape(2, 1936)
    execution[:, 2:, 1] = 0
    execution[0, 0, 4] += 30
    execution[0, 1, 4] -= 10
    execution[1, :, margin.PREVIOUS_MARK] = execution[0, :, 4]
    account = run_tw_futures_portfolio_integer_torch(weights[:1], execution[:1], initial_capital=1000., recoverable_backward=True)
    assert account.strategy_returns.count_nonzero() == 0
    assert account.contract_quantities_history.count_nonzero() == 0
    assert account.final_equity_scale.item() == 1.
    (-account.strategy_returns.mean()).backward()
    assert m.futures_action_head.weight.grad.abs().sum() > 0
    assert m.futures_continuous_encoder[0].weight.grad.abs().sum() == 0
    torch.optim.SGD(m.parameters(), lr=.01).step()
    m.zero_grad(set_to_none=True)
    next_weights = m(x, stock_mask, portfolio_context=context)
    assert next_weights.abs().sum() > 0
    next_weights[:, 0].sum().backward()
    assert m.futures_continuous_encoder[0].weight.grad.abs().sum() > 0
    assert all(torch.isfinite(p.grad).all() for p in m.parameters() if p.grad is not None)


def test_only_existing_final_head_changes_and_loading_does_not_flatten_trained_policy():
    torch.manual_seed(41)
    old = model(False)
    torch.manual_seed(41)
    new = model(True)
    assert set(old.state_dict()) == set(new.state_dict())
    for key, value in old.state_dict().items():
        if not key.startswith('futures_action_head.'):
            torch.testing.assert_close(value, new.state_dict()[key], rtol=0, atol=0)
    new.load_state_dict(old.state_dict())
    x, mask, context = inputs()
    torch.testing.assert_close(new(x, mask, portfolio_context=context), old(x, mask, portfolio_context=context), rtol=0, atol=0)


def test_normal_initialization_trades_and_trains_backbone_on_first_action():
    torch.manual_seed(41)
    m = model(False)
    x, mask, context = inputs()
    weights = m(x, mask, portfolio_context=context)
    assert weights[:, :2].abs().sum() > 0
    assert weights[:, 2:].count_nonzero() == 0
    execution = tape(1, 1936)
    execution[:, 2:, 1] = 0
    execution[0, 0, 4] += 30
    execution[0, 1, 4] -= 10
    account = run_tw_futures_portfolio_integer_torch(
        weights[:1], execution, initial_capital=10000., recoverable_backward=True)
    assert account.contract_quantities_history.count_nonzero() > 0
    (-account.strategy_returns.mean()).backward()
    assert m.futures_action_head.weight.grad.abs().sum() > 0
    assert m.futures_continuous_encoder[0].weight.grad.abs().sum() > 0
    assert all(torch.isfinite(p.grad).all() for p in m.parameters() if p.grad is not None)


def test_default_contract_is_legacy_compatible_and_flat_resume_is_rejected(tmp_path):
    cfg = load_config(CONFIG)
    cfg.training.financial_transformer.futures_feature_rms_normalization = False
    current = build_checkpoint_manifest(_stock_panel(), cfg, include_data_content=False)
    cfg.training.financial_transformer.futures_flat_action_initialization = False
    old = build_checkpoint_manifest(_stock_panel(), cfg, include_data_content=False)
    assert current['fingerprints']['trading'] == old['fingerprints']['trading']
    assert current['fingerprints']['model'] != old['fingerprints']['model']
    with pytest.raises(RuntimeError, match='semantic fingerprint mismatch'):
        _validate_checkpoint_manifest({'experiment_manifest': old}, current, checkpoint_path=tmp_path/'old.pt', scope='resume')
    values = asdict(cfg.training.financial_transformer)
    legacy = dict(values)
    legacy.pop('futures_flat_action_initialization')
    assert _project_temporal_basis_model_config(values) == _project_temporal_basis_model_config(legacy)


def test_flat_initialization_is_factory_owned():
    from stockagent.models.factory import build_model
    cfg = load_config(CONFIG)
    cfg.training.financial_transformer.temporal_basis_families = []
    cfg.training.financial_transformer.temporal_basis_components_by_family = {}
    m = build_model(config=cfg, lookback=4, num_features=3, num_symbols=2)
    assert m.futures_action_head.weight.count_nonzero() == 0
    assert m.futures_action_head.bias.count_nonzero() == 0
    assert m.futures_feature_rms_normalization


def test_flat_option_fails_closed_on_an_unsupported_output(tmp_path):
    import yaml
    raw = _load_raw_config(CONFIG)
    raw['training']['financial_transformer']['portfolio_output_mode'] = 'learned_cash'
    path = tmp_path/'bad.yaml'
    path.write_text(yaml.safe_dump(raw))
    with pytest.raises(ValueError, match='flat futures initialization'):
        load_config(path)
