"""The actor may observe its known contract denomination, never future prices."""
from copy import deepcopy
from dataclasses import asdict

import numpy as np
import polars as pl
import pytest
import torch

from stockagent.config import load_config
from stockagent.data import tw_futures_margin as margin
from stockagent.training.checkpoint_contract import _project_temporal_basis_model_config, build_checkpoint_manifest
from stockagent.training.trainer import _expand_pretrained_margin_amount_column
from test_tw_futures_margin import rule_panel, write_rules
from test_futures_flat_initialization import model, inputs
from test_tw_stock_context_futures_portfolio import _stock_panel


@pytest.mark.parametrize('kind', ['fixed_twd', 'notional_rate'])
def test_new_amount_feature_uses_only_known_rules_and_previous_settlement(tmp_path, kind):
    panel, rows = rule_panel(tmp_path)
    if kind == 'notional_rate':
        for row in rows:
            row.update(margin_kind=kind, initial=.1, maintenance=.075,
                       settlement_initial=.1, settlement_maintenance=.075)
    path = write_rules(tmp_path, panel, rows)
    old = margin.attach_futures_margin_rules(panel, path).stock_context_futures_portfolio_daily
    attached = margin.attach_futures_margin_rules(panel, path, include_margin_amount=True)
    new = attached.stock_context_futures_portfolio_daily
    np.testing.assert_array_equal(old.integer_execution, new.integer_execution)
    np.testing.assert_array_equal(old.candidate_features[..., -2:], new.candidate_features[..., -2:])
    np.testing.assert_array_equal(new.candidate_features[:, 0, -3], np.full(3, 100.))
    # Exercise the canonical dataset/windowed transport, not just the sidecar.
    from stockagent.training.dataset import CrossSectionalDataset
    from stockagent.training.windowed import dataset_to_windowed_tensors
    dataset = CrossSectionalDataset(attached, np.arange(3), lookback=1,
                                   execution_mode='tw_stock_context_futures_portfolio')
    split = dataset_to_windowed_tensors(dataset)
    np.testing.assert_array_equal(split.derivative_candidate_features.numpy(), new.candidate_features)
    assert split.overnight_log_returns.shape[-1] == margin.MARGIN_EXECUTION_WIDTH
    source = new.source_path
    pl.read_parquet(source).with_columns(pl.lit(105.).alias('open'), pl.lit(108.).alias('close'),
                                        pl.lit(107.).alias('settlement')).write_parquet(source)
    path = write_rules(tmp_path, panel, rows)
    changed = margin.attach_futures_margin_rules(panel, path, include_margin_amount=True).stock_context_futures_portfolio_daily
    np.testing.assert_array_equal(new.candidate_features, changed.candidate_features)


def test_zero_extended_encoder_preserves_policy_then_learns_the_new_observation():
    torch.manual_seed(41)
    old = model(False)
    new = deepcopy(old)
    new.futures_margin_amount_context = True
    new.futures_margin_encoder = torch.nn.Linear(3, old.d_model, bias=False)
    extended = _expand_pretrained_margin_amount_column('futures_margin_encoder.weight',
        old.futures_margin_encoder.weight, new.futures_margin_encoder.weight, new)
    with torch.no_grad():
        new.futures_margin_encoder.weight.copy_(extended)
    x, mask, context = inputs()
    features = context['candidate_features']
    expanded = torch.cat((features[..., :-2], torch.full_like(features[..., :1], 100000.), features[..., -2:]), dim=-1)
    new_context = dict(context, candidate_features=expanded)
    a, b = old(x, mask, portfolio_context=context), new(x, mask, portfolio_context=new_context)
    torch.testing.assert_close(a, b, rtol=0, atol=0)
    assert b.abs().sum() > 0
    b[:, 0].sum().backward()
    assert new.futures_margin_encoder.weight.grad[:, 2].abs().sum() > 0
    # A changed observation becomes visible once its column is learned.
    with torch.no_grad():
        new.futures_margin_encoder.weight[:, 2].copy_(torch.linspace(-.2, .2, old.d_model))
    doubled = dict(new_context, candidate_features=expanded.clone())
    doubled['candidate_features'][..., -3] *= 2
    assert not torch.equal(new(x, mask, portfolio_context=new_context), new(x, mask, portfolio_context=doubled))
    with pytest.raises(ValueError, match=r'candidate_features must have shape .*25'):
        new(x, mask, portfolio_context=context)


def test_amount_context_is_explicit_in_model_and_candidate_feature_contracts():
    c = load_config('configs/markets/tw_futures_v8_margin_verified_2011_capital10m_tx_front_roll_feasible_gradient_v14.yaml')
    old = build_checkpoint_manifest(_stock_panel(), c, include_data_content=False)
    values = asdict(c.training.financial_transformer)
    legacy = dict(values)
    legacy.pop('futures_margin_amount_context')
    assert _project_temporal_basis_model_config(values) == _project_temporal_basis_model_config(legacy)
    c.training.financial_transformer.futures_margin_amount_context = True
    new = build_checkpoint_manifest(_stock_panel(), c, include_data_content=False)
    assert new['fingerprints']['model'] != old['fingerprints']['model']
    assert new['fingerprints']['trading'] != old['fingerprints']['trading']
