"""Causal contract costs and persistent train-only futures feature scaling."""
from datetime import date
import hashlib
import json

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import torch

from stockagent.data.tw_futures_portfolio_daily import (
    FUTURES_MODEL_FEATURE_COLUMNS,
    TAIFEX_FUTURES_PORTFOLIO_DATA_CONTRACT_VERSION,
    TAIFEX_FUTURES_PORTFOLIO_FEATURE_CONTRACT_VERSION,
    TAIFEX_FUTURES_PORTFOLIO_FIXED_SLOT_COUNT,
)
from stockagent.data.tw_stock_context_futures_portfolio import (
    TW_STOCK_CONTEXT_FUTURES_MODEL_FEATURE_COLUMNS,
    TW_STOCK_CONTEXT_FUTURES_PORTFOLIO_PRIOR_DENOMINATION_CONTRACT_VERSION,
    attach_stock_context_futures_portfolio_daily,
)
from stockagent.models.financial_transformer_futures import (
    FinancialTransformerFuturesModel,
)
from stockagent.research.taifex_transaction_tax import stock_index_futures_tax_rate
from test_tw_stock_context_futures_portfolio import _stock_panel


def _source(tmp_path, *, perturb_current=False, missing_prior=False):
    tmp_path.mkdir(parents=True, exist_ok=True)
    rows = []
    for index in range(3):
        row = dict(date=date(2026, 1, 2 + index), product="TX",
            symbol="TAIFEX_SLOT_0001", tenor_rank=1,
            open=10000.0 + 100 * index, close=10050.0 + 100 * index,
            volume=100.0, holding_log_return=.01, executable=True,
            must_liquidate=index == 2, can_hold_overnight=index < 2,
            same_contract_as_previous_session=index > 0,
            contract_multiplier=200.0, sinopac_network_fee_group="large",
            underlying_symbol="S0", contract="202601", physical_contract="TX:202601",
            asset_class="index_future", previous_volume=100.0,
            previous_settlement=None if index == 0 else 9900.0 + 100 * index)
        for j, name in enumerate(FUTURES_MODEL_FEATURE_COLUMNS):
            row[name] = 1 if j == 0 else .001 * (j + index)
        if index == 1 and perturb_current:
            row.update(open=25000., close=26000., volume=90000., holding_log_return=-.4)
            row["taifex_volume_log1p"] = 11.0
        if index == 1 and missing_prior:
            row["previous_settlement"] = None
        rows.append(row)
    path = tmp_path / "continuous_daily.parquet"
    pq.write_table(pa.Table.from_pylist(rows), path)
    (tmp_path / "manifest.json").write_text(json.dumps(dict(
        contract_version=TAIFEX_FUTURES_PORTFOLIO_DATA_CONTRACT_VERSION,
        feature_contract_version=TAIFEX_FUTURES_PORTFOLIO_FEATURE_CONTRACT_VERSION,
        fixed_model_output_slots=TAIFEX_FUTURES_PORTFOLIO_FIXED_SLOT_COUNT,
        outputs={"continuous_daily": {"sha256": hashlib.sha256(path.read_bytes()).hexdigest()}},
    )))
    return path


def _attach(path, basis="prior_settlement"):
    return attach_stock_context_futures_portfolio_daily(
        _stock_panel(), path, fee_per_side_twd_by_group={"large": 40.},
        integer_contracts=True, denomination_context_basis=basis,
        integer_fee_per_contract_per_side_twd=40., max_volume_participation=.5,
    ).stock_context_futures_portfolio_daily


def _model(*, rms=True, denomination=True):
    torch.set_num_threads(2)
    return FinancialTransformerFuturesModel(
        lookback=2, num_features=3, num_symbols=2, d_model=8,
        attention_mode="market_token", num_market_tokens=2,
        temporal_heads=2, temporal_layers=1, temporal_pooling="last",
        temporal_query_mode="last_only", use_symbol_pos=False,
        portfolio_mode="long_short", portfolio_output_mode="score_entmax_log_cash",
        center_long_short_logits=False,
        futures_denomination_aware_output=denomination,
        futures_denomination_hard_projection=False,
        futures_feature_rms_normalization=rms,
        feature_bottleneck_dim=2, dropout=0., return_aux=False,
    ).eval()


def test_prior_denomination_has_known_cost_and_unchanged_execution(tmp_path):
    path = _source(tmp_path)
    prior = _attach(path)
    legacy = _attach(path, "current_open")
    assert prior.contract_version == TW_STOCK_CONTEXT_FUTURES_PORTFOLIO_PRIOR_DENOMINATION_CONTRACT_VERSION
    assert prior.denomination_context_basis == "prior_settlement"
    np.testing.assert_array_equal(prior.integer_execution, legacy.integer_execution)
    np.testing.assert_array_equal(prior.candidate_features[..., :-1], legacy.candidate_features[..., :-1])
    np.testing.assert_array_equal(prior.candidate_mask, legacy.candidate_mask)
    prior_notional = 10000. * 200.
    tax = np.floor(prior_notional * stock_index_futures_tax_rate(date(2026, 1, 3)) + .5)
    assert prior.candidate_features[1, 0, -1] == prior_notional + 80 + 2 * tax
    assert prior.candidate_features[1, 0, -1] != legacy.candidate_features[1, 0, -1]
    assert prior.integer_execution[1, 0, 3] == 10100. * 200.


def test_current_ohlcv_cannot_change_prior_context_or_same_decision(tmp_path):
    original = _attach(_source(tmp_path / "a"))
    changed = _attach(_source(tmp_path / "b", perturb_current=True))
    np.testing.assert_array_equal(original.candidate_features[:2], changed.candidate_features[:2])
    np.testing.assert_array_equal(original.candidate_mask[:2], changed.candidate_mask[:2])
    assert original.integer_execution[1, 0, 3] != changed.integer_execution[1, 0, 3]
    assert original.candidate_features[2, 0, 2] != changed.candidate_features[2, 0, 2]
    torch.manual_seed(71)
    model = _model()
    model.set_futures_feature_rms_normalizer(torch.ones(17), torch.ones(17, dtype=torch.bool))
    x = torch.randn(1, 2, 2, 3)
    context = lambda d: dict(candidate_features=torch.from_numpy(d.candidate_features[1:2]),
                             candidate_mask=torch.from_numpy(d.candidate_mask[1:2]))
    with torch.no_grad():
        a = model(x, torch.ones(1, 2, dtype=torch.bool), portfolio_context=context(original))
        b = model(x, torch.ones(1, 2, dtype=torch.bool), portfolio_context=context(changed))
    torch.testing.assert_close(a, b, rtol=0, atol=0)


def test_missing_prior_mark_fails_without_using_current_open(tmp_path):
    path = _source(tmp_path, missing_prior=True)
    with pytest.raises(ValueError, match="previous same-contract settlement"):
        _attach(path)
    assert _attach(path, "current_open") is not None


def test_prior_basis_rejects_incompatible_clock(tmp_path):
    with pytest.raises(ValueError, match="excludes current OPEN"):
        attach_stock_context_futures_portfolio_daily(
            _stock_panel(), _source(tmp_path), fee_per_side_twd_by_group={"large": 40.},
            integer_contracts=True, current_open_feature=True,
            denomination_context_basis="prior_settlement", max_volume_participation=.5,
        )


def test_futures_rms_checkpoint_retains_scales_and_unseen_mask(tmp_path):
    torch.manual_seed(52)
    model = _model()
    scale = torch.arange(1, 18, dtype=torch.float32)
    active = torch.ones(17, dtype=torch.bool)
    active[2] = False
    model.set_futures_feature_rms_normalizer(scale, active)
    raw = 2 * scale
    raw[2] = 1e20
    expected = torch.full((17,), 2.)
    expected[2] = 0
    torch.testing.assert_close(model._normalize_futures_continuous_features(raw), expected, rtol=0, atol=0)
    path = tmp_path / "state.pt"
    torch.save(model.state_dict(), path)
    restored = _model()
    restored.load_state_dict(torch.load(path, weights_only=True))
    torch.testing.assert_close(restored.futures_feature_rms_scale, scale, rtol=0, atol=0)
    assert torch.equal(restored.futures_feature_active_mask, active)
    assert "futures_feature_rms_scale" not in dict(model.named_parameters())
    x = torch.randn(1, 2, 2, 3)
    features = torch.ones(1, 1936, len(TW_STOCK_CONTEXT_FUTURES_MODEL_FEATURE_COLUMNS))
    features[..., -1] = 200_000.
    mask = torch.zeros(1, 1936, dtype=torch.bool)
    mask[:, :2] = True
    kwargs = dict(portfolio_context=dict(candidate_features=features, candidate_mask=mask))
    with torch.no_grad():
        a = model(x, torch.ones(1, 2, dtype=torch.bool), **kwargs)
        b = restored(x, torch.ones(1, 2, dtype=torch.bool), **kwargs)
        features[..., 3] = 1e10  # Train-unseen continuous column stays inert.
        c = restored(x, torch.ones(1, 2, dtype=torch.bool), **kwargs)
    torch.testing.assert_close(a, b, rtol=0, atol=0)
    torch.testing.assert_close(b, c, rtol=0, atol=0)


def test_disabled_futures_rms_preserves_legacy_state_and_values():
    model = _model(rms=False, denomination=False)
    assert not any(name.startswith("futures_feature_") for name in model.state_dict())
    values = torch.randn(2, 1936, 17)
    assert model._normalize_futures_continuous_features(values) is values
    with pytest.raises(RuntimeError, match="disabled"):
        model.set_futures_feature_rms_normalizer(torch.ones(17), torch.ones(17, dtype=torch.bool))


def test_prior_denomination_is_context_without_a_second_integer_projection():
    model = _model()
    with torch.no_grad():
        model.futures_action_head.weight.zero_()
        model.futures_action_head.bias.fill_(.01)
    features = torch.zeros(1, 1936, len(TW_STOCK_CONTEXT_FUTURES_MODEL_FEATURE_COLUMNS))
    features[..., -1] = 1_000_000.
    mask = torch.zeros(1, 1936, dtype=torch.bool)
    mask[:, 0] = True
    with torch.no_grad():
        weights = model(torch.zeros(1, 2, 2, 3), torch.ones(1, 2, dtype=torch.bool),
            portfolio_context=dict(candidate_features=features, candidate_mask=mask))
    assert 0 < weights[0, 0] < .1  # Below one lot remains a learnable request.
    assert weights[0, 0].item() == pytest.approx(.01 / 1.01, rel=1e-6)


@pytest.mark.parametrize("scale", [torch.ones(16), torch.zeros(17), torch.full((17,), float("nan"))])
def test_invalid_futures_rms_scale_rejected(scale):
    with pytest.raises(ValueError, match="futures RMS"):
        _model().set_futures_feature_rms_normalizer(scale, torch.ones(17, dtype=torch.bool))
