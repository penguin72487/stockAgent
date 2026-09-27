"""Prior-volume action eligibility must never mask existing ledger positions."""
from dataclasses import fields
import hashlib
import json

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import torch

from stockagent.backtest.tw_futures_portfolio import run_tw_futures_portfolio_integer_torch
from stockagent.data.tw_stock_context_futures_portfolio import (
    TW_STOCK_CONTEXT_FUTURES_PORTFOLIO_PRIOR_CAPACITY_CONTRACT_VERSION,
    attach_stock_context_futures_portfolio_daily,
)
from stockagent.training.dataset import CrossSectionalDataset
from test_futures_causal_input_contract import _source
from test_tw_stock_context_futures_portfolio import _stock_panel


def _capacity_source(tmp_path, *, prior_volume=0., perturb_current_volume=False):
    path = _source(tmp_path)
    rows = pq.read_table(path).to_pylist()
    rows[1]["previous_volume"] = prior_volume
    if perturb_current_volume:
        rows[1]["volume"] = 0.
        rows[1]["taifex_volume_log1p"] = 0.
        rows[2]["previous_volume"] = 0.
    pq.write_table(pa.Table.from_pylist(rows), path)
    manifest_path = path.parent / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["outputs"]["continuous_daily"]["sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
    manifest_path.write_text(json.dumps(manifest))
    return path


def _attach(path, *, require_prior_capacity=False, participation=.5, **kwargs):
    return attach_stock_context_futures_portfolio_daily(
        _stock_panel(), path, fee_per_side_twd_by_group={"large": 40.},
        integer_contracts=True, require_prior_capacity=require_prior_capacity,
        max_volume_participation=participation, **kwargs,
    )


@pytest.mark.parametrize("prior_volume,expected", [(0., False), (1., False), (2., True)])
def test_prior_volume_fractional_capacity_only_changes_action_mask(tmp_path, prior_volume, expected):
    path = _capacity_source(tmp_path, prior_volume=prior_volume)
    legacy = _attach(path).stock_context_futures_portfolio_daily
    filtered = _attach(path, require_prior_capacity=True).stock_context_futures_portfolio_daily
    assert legacy.candidate_mask[1, 0]
    assert bool(filtered.candidate_mask[1, 0]) is expected
    assert filtered.contract_version == TW_STOCK_CONTEXT_FUTURES_PORTFOLIO_PRIOR_CAPACITY_CONTRACT_VERSION
    assert filtered.require_prior_capacity
    assert not legacy.require_prior_capacity
    assert filtered.integer_execution[1, 0, 8] == np.floor(prior_volume * .5)
    # Candidate features, all marking prices, carry/exit flags, returns and
    # benchmark are identical. Only the action mask and its contract may differ.
    for field in fields(legacy):
        if field.name in {"candidate_mask", "require_prior_capacity", "contract_version"}:
            continue
        left, right = getattr(legacy, field.name), getattr(filtered, field.name)
        if isinstance(left, np.ndarray):
            np.testing.assert_array_equal(left, right)
        else:
            assert left == right


def test_omitted_capacity_filter_is_exact_legacy_behavior(tmp_path):
    path = _capacity_source(tmp_path)
    default = attach_stock_context_futures_portfolio_daily(
        _stock_panel(), path, fee_per_side_twd_by_group={"large": 40.},
        integer_contracts=True, max_volume_participation=.5,
    ).stock_context_futures_portfolio_daily
    explicit = _attach(path, require_prior_capacity=False).stock_context_futures_portfolio_daily
    for field in fields(default):
        left, right = getattr(default, field.name), getattr(explicit, field.name)
        if isinstance(left, np.ndarray):
            np.testing.assert_array_equal(left, right)
        else:
            assert left == right


@pytest.mark.parametrize("participation", [0., -1.])
@pytest.mark.parametrize("enabled", [False, True])
def test_nonpositive_participation_keeps_existing_integer_rejection(tmp_path, participation, enabled):
    with pytest.raises(ValueError, match="max_volume_participation must be in"):
        _attach(_capacity_source(tmp_path), require_prior_capacity=enabled,
                participation=participation)


def test_capacity_filter_rejects_fractional_contract_mode(tmp_path):
    with pytest.raises(ValueError, match="prior capacity candidates require integer contracts"):
        attach_stock_context_futures_portfolio_daily(
            _stock_panel(), _capacity_source(tmp_path),
            fee_per_side_twd_by_group={"large": 40.}, require_prior_capacity=True,
        )


def test_current_volume_only_affects_following_decision(tmp_path):
    original = _attach(_capacity_source(tmp_path / "a", prior_volume=2.),
                       require_prior_capacity=True).stock_context_futures_portfolio_daily
    changed = _attach(_capacity_source(tmp_path / "b", prior_volume=2., perturb_current_volume=True),
                      require_prior_capacity=True).stock_context_futures_portfolio_daily
    np.testing.assert_array_equal(original.candidate_mask[:2], changed.candidate_mask[:2])
    np.testing.assert_array_equal(original.candidate_features[:2], changed.candidate_features[:2])
    np.testing.assert_array_equal(original.integer_execution[:2], changed.integer_execution[:2])
    assert original.candidate_mask[2, 0] and not changed.candidate_mask[2, 0]


def test_zero_capacity_mask_preserves_carried_position_and_mark_to_market(tmp_path):
    path = _capacity_source(tmp_path, prior_volume=0.)
    legacy = _attach(path)
    filtered = _attach(path, require_prior_capacity=True)
    outcomes = []
    for panel in (legacy, filtered):
        dataset = CrossSectionalDataset(
            panel, np.arange(3, dtype=np.int64), lookback=1,
            execution_mode="tw_stock_context_futures_portfolio",
        )
        assert 1 in dataset.valid_indices  # Not removed because the model has no action.
        tape = dataset.overnight_log_returns_t[1:2, :1]
        assert tape[0, 0, 1] == 1 and tape[0, 0, 2] == 0
        assert tape[0, 0, 8] == 0
        result = run_tw_futures_portfolio_integer_torch(
            torch.zeros(1, 1), tape, initial_capital=10_000_000.,
            initial_quantities=torch.ones(1),
        )
        assert result.contract_quantities_history.item() == 1
        assert result.final_weights.item() == 1
        assert result.turnovers.item() == 0
        expected = 1 + float(tape[0, 0, 4] - tape[0, 0, 3]) / 10_000_000.
        assert result.final_equity_scale.item() == pytest.approx(expected, abs=1e-7)
        assert result.strategy_returns.item() > 0 and bool(result.final_alive)
        outcomes.append(result)
    assert not filtered.stock_context_futures_portfolio_daily.candidate_mask[1, 0]
    for field in fields(outcomes[0]):
        left, right = getattr(outcomes[0], field.name), getattr(outcomes[1], field.name)
        if isinstance(left, torch.Tensor):
            torch.testing.assert_close(left, right, rtol=0, atol=0, equal_nan=True)
        else:
            assert left == right


def test_capacity_contract_supersedes_prior_denomination_without_changing_features(tmp_path):
    path = _capacity_source(tmp_path)
    prior = _attach(path, denomination_context_basis="prior_settlement").stock_context_futures_portfolio_daily
    filtered = _attach(path, require_prior_capacity=True,
                       denomination_context_basis="prior_settlement").stock_context_futures_portfolio_daily
    assert prior.contract_version == 7 and filtered.contract_version == 8
    np.testing.assert_array_equal(prior.candidate_features, filtered.candidate_features)
    np.testing.assert_array_equal(prior.integer_execution, filtered.integer_execution)
