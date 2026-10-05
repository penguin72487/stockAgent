"""Action coordinates change conditioning, never the reachable margin budget."""
from dataclasses import asdict

import pytest
import torch

from stockagent.config import load_config
from stockagent.models.factory import build_model
from stockagent.training.checkpoint_contract import (
    _project_temporal_basis_model_config, build_checkpoint_manifest,
    _validate_checkpoint_manifest,
)
from test_futures_flat_initialization import model, inputs
from test_tw_stock_context_futures_portfolio import _stock_panel

CONFIG = 'configs/markets/tw_futures_v8_margin_verified_2011_capital10m_tx_front_roll_feasible_gradient_v14.yaml'


def test_prior_notional_coordinates_remove_local_margin_leverage_from_jacobian():
    m = model(False)
    m.futures_notional_score_coordinates = True
    x, mask, context = inputs()
    context['candidate_mask'][:, 1:] = False
    with torch.no_grad():
        m.futures_action_head.weight.zero_()
        m.futures_action_head.bias.zero_()
    for margin_ratio in (.02, .1, .25):
        context['candidate_features'][..., -2] = margin_ratio
        m.zero_grad(set_to_none=True)
        weight = m(x, mask, portfolio_context=context)[0, 0]
        contracts = weight * 10000 / (1000 * margin_ratio)
        contracts.backward()
        # d(q)/d(score) = E/prior_notional, independent of margin rate at zero.
        assert m.futures_action_head.bias.grad.item() == pytest.approx(10., rel=1e-5)


def test_coordinate_transform_is_invertible_without_an_exposure_cap():
    m = model(False)
    m.futures_notional_score_coordinates = True
    x, mask, context = inputs()
    context['candidate_mask'][:, 1:] = False
    with torch.no_grad():
        m.futures_action_head.weight.zero_()
    for margin_ratio in (.02, .1, .25):
        context['candidate_features'][..., -2] = margin_ratio
        for score in (-20., -.5, .5, 20.):
            with torch.no_grad():
                m.futures_action_head.bias.fill_(score / margin_ratio)
                weight = m(x, mask, portfolio_context=context)[0, 0]
            assert weight.item() == pytest.approx(score / (1 + abs(score)), rel=1e-5)


def test_nonzero_initialization_and_first_action_backbone_gradients_are_retained():
    torch.manual_seed(41)
    m = model(False)
    keys = set(m.state_dict())
    m.futures_notional_score_coordinates = True
    x, mask, context = inputs()
    weights = m(x, mask, portfolio_context=context)
    assert weights[:, :2].abs().sum() > 0
    assert weights[:, 2:].count_nonzero() == 0
    weights[:, 0].sum().backward()
    assert m.futures_action_head.weight.grad.abs().sum() > 0
    assert m.futures_continuous_encoder[0].weight.grad.abs().sum() > 0
    assert set(m.state_dict()) == keys


def test_new_coordinates_require_new_model_checkpoint_and_factory_wiring(tmp_path):
    cfg = load_config(CONFIG)
    old = build_checkpoint_manifest(_stock_panel(), cfg, include_data_content=False)
    values = asdict(cfg.training.financial_transformer)
    legacy = dict(values)
    legacy.pop('futures_notional_score_coordinates')
    assert _project_temporal_basis_model_config(values) == _project_temporal_basis_model_config(legacy)
    cfg.training.financial_transformer.futures_notional_score_coordinates = True
    new = build_checkpoint_manifest(_stock_panel(), cfg, include_data_content=False)
    assert new['fingerprints']['trading'] == old['fingerprints']['trading']
    assert new['fingerprints']['model'] != old['fingerprints']['model']
    with pytest.raises(RuntimeError, match='semantic fingerprint mismatch'):
        _validate_checkpoint_manifest({'experiment_manifest': old}, new, checkpoint_path=tmp_path/'old.pt', scope='resume')
    cfg.training.financial_transformer.temporal_basis_families = []
    cfg.training.financial_transformer.temporal_basis_components_by_family = {}
    built = build_model(config=cfg, lookback=4, num_features=3, num_symbols=2)
    assert built.futures_notional_score_coordinates
    assert built.futures_action_head.weight.count_nonzero() > 0


@pytest.mark.parametrize('invalid_option', ['output', 'centering'])
def test_unsupported_policy_coordinates_fail_closed(tmp_path, invalid_option):
    import yaml
    from stockagent.config import _load_raw_config
    raw = _load_raw_config(CONFIG)
    raw['training']['financial_transformer']['futures_notional_score_coordinates'] = True
    if invalid_option == 'output':
        raw['training']['financial_transformer']['portfolio_output_mode'] = 'learned_cash'
    else:
        raw['training']['financial_transformer']['center_long_short_logits'] = True
    path = tmp_path/'bad.yaml'
    path.write_text(yaml.safe_dump(raw))
    with pytest.raises(ValueError, match='notional score coordinates'):
        load_config(path)
