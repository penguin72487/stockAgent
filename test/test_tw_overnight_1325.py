from __future__ import annotations

import copy
import hashlib
import json
from datetime import datetime
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import torch

from stockagent.backtest.simulator import run_backtest_integer_shares, run_backtest_torch
from stockagent.config import load_config
from stockagent.data.panel import PanelData
from stockagent.data.tw_overnight import (
    OVERNIGHT_1325_FEATURE, attach_overnight_1325, attach_overnight_prices,
)
from stockagent.models.factory import build_model
from stockagent.models.temporal_basis_fit import fit_training_only_pca_klt
from stockagent.training.dataset import CrossSectionalDataset
from stockagent.training.checkpoint_contract import build_checkpoint_manifest, validate_checkpoint_manifest
from stockagent.training.loss import risk_aware_loss
from stockagent.training.trainer import _build_execution_runtime

CONFIG = "configs/markets/tw_overnight_1325_multi_basis_22_capital10m.yaml"


def panel_fixture(dates=None, symbols=2, features=23):
    dates = np.asarray(dates if dates is not None else ["2024-01-04", "2024-01-05", "2024-01-08", "2024-01-09"], dtype="datetime64[D]")
    shape = (len(dates), symbols)
    masks = np.ones(shape, dtype=bool)
    prices = np.full(shape, 100., dtype=np.float32)
    return PanelData(
        dates=dates, symbols=["2330", "2317"][:symbols],
        feature_names=[f"f{i}" for i in range(features)],
        features=np.random.default_rng(42).normal(0, .1, (*shape, features)).astype(np.float32),
        returns_1d=np.zeros(shape, dtype=np.float32), tradable_mask=masks.copy(),
        alive_mask=masks.copy(), benchmark_returns=np.zeros(len(dates), dtype=np.float32),
        close_prices=prices, open_prices=prices.copy(), daily_volumes=prices * 100000,
        intraday_returns=np.zeros(shape, dtype=np.float32),
        raw_close_returns_1d=np.zeros(shape, dtype=np.float32),
        can_buy_mask=masks.copy(), can_sell_mask=masks.copy(),
        day_trade_can_buy_open_mask=masks.copy(), day_trade_can_sell_open_mask=masks.copy(),
        unresolved_corporate_action_mask=np.zeros(shape, dtype=bool),
    )


def execute(actions, *, masks=None, open_sell=None, buy_fee=0., sell_fee=0.):
    rows = len(actions)
    side = torch.ones((rows, 1), dtype=torch.bool)
    gaps = torch.zeros((rows, 1))
    if rows > 1:
        gaps[1] = np.log(1.1)
    return run_backtest_torch(
        actions, torch.full((rows, 1), np.log(.5)), side if masks is None else masks,
        torch.zeros(rows), 0., 0., execution_mode="tw_overnight", long_only=True,
        overnight_fixed_close_to_open=True, portfolio_activation="pre_normalized",
        overnight_returns=gaps, can_buy_mask=side, can_sell_mask=side,
        day_trade_can_buy_open_mask=side,
        day_trade_can_sell_open_mask=side if open_sell is None else open_sell,
        unresolved_corporate_action_mask=~side, buy_fee_rates=torch.tensor([buy_fee]),
        sell_fee_rates=torch.tensor([sell_fee]),
    )


def test_fixed_auctions_ignore_morning_policy_and_intraday_return():
    actions = torch.tensor([[[0.], [0.], [.4]], [[0.], [.9], [0.]]], requires_grad=True)
    result = execute(actions, masks=torch.tensor([[True], [False]]))
    torch.testing.assert_close(result.executed_long_buy_weights[0, :, 0], torch.tensor([0., .4]))
    assert result.executed_long_sell_weights[1, 0, 0] > 0
    assert result.executed_long_sell_weights[1, 1, 0] == 0
    assert result.final_weights.abs().max() == 0
    assert np.expm1(float(result.strategy_returns.detach().sum())) == pytest.approx(.04, abs=2e-7)
    result.strategy_returns.sum().backward()
    assert actions.grad[0, 2, 0] > 0
    assert actions.grad[:, :2].abs().max() == 0


def test_unused_morning_channels_cannot_consume_close_budget():
    clean = torch.tensor([[[1.], [0.], [.4]], [[1.], [0.], [0.]]])
    arbitrary = clean.clone()
    arbitrary[:, 0] = -100.
    arbitrary[:, 1] = 100.
    torch.testing.assert_close(execute(clean).strategy_returns,
                               execute(arbitrary).strategy_returns)


def test_missing_open_exit_is_absorbing_and_never_falls_back_to_close():
    actions = torch.tensor([[[1.], [0.], [.4]], [[1.], [0.], [.4]], [[1.], [0.], [.4]]])
    result = execute(actions, open_sell=torch.tensor([[True], [False], [True]]))
    assert not result.final_alive
    assert result.executed_long_sell_weights[1:].abs().max() == 0
    assert result.executed_long_buy_weights[1:].abs().max() == 0


def test_fees_use_normal_overnight_stock_tax_and_gross_legs():
    result = execute(torch.tensor([[[1.], [0.], [.4]], [[1.], [0.], [0.]]]),
                     buy_fee=.000285, sell_fee=.003285)
    expected = .4 * (.1 - .000285 - 1.1 * .003285)
    assert np.expm1(float(result.strategy_returns.sum())) == pytest.approx(expected, abs=3e-7)


def test_loss_uses_same_fixed_auction_return_and_gradient():
    actions = torch.tensor([[[1.], [0.], [.4]], [[1.], [0.], [0.]]], requires_grad=True)
    side = torch.ones(2, 1, dtype=torch.bool)
    loss = risk_aware_loss(
        actions, torch.full((2, 1), np.log(.5)), side, torch.zeros(2),
        can_buy_mask=side, can_sell_mask=side, long_only=True,
        objective="log_utility", execution_mode="tw_overnight",
        gamma_turnover=0., concentration_weight=0., direction_weight=0.,
        volatility_regime_weight=0.,
        overnight_fixed_close_to_open=True, portfolio_activation="pre_normalized",
        overnight_log_returns=torch.tensor([[0.], [np.log(1.1)]], dtype=torch.float32),
        day_trade_can_buy_open_mask=side, day_trade_can_sell_open_mask=side,
        unresolved_corporate_action_mask=~side,
        buy_fee_rates=torch.zeros(1), sell_fee_rates=torch.zeros(1),
    )
    expected = -execute(actions).strategy_returns.mean() * 252
    torch.testing.assert_close(loss, expected)
    loss.backward()
    assert actions.grad[0, 2, 0] < 0


def test_integer_and_tensor_auctions_agree_and_do_not_use_intraday_profit():
    actions = np.array([[[1.], [0.], [.4]], [[1.], [0.], [0.]]])
    side = np.ones((2, 1), dtype=bool)
    result, _ = run_backtest_integer_shares(
        actions, np.array([[np.log(1.1)], [0.]]), side, np.zeros(2),
        can_buy_mask=side, can_sell_mask=side, close_prices=np.array([[100.], [55.]]),
        open_prices=np.array([[200.], [110.]]), symbols=["2330"],
        day_trade_can_buy_open_mask=side, day_trade_can_sell_open_mask=side,
        unresolved_corporate_action_mask=~side, buy_fee_rates=np.zeros(1),
        sell_fee_rates=np.zeros(1), lot_sizes=np.array([1000]),
        initial_capital=1_000_000, collect_holdings=False,
        execution_mode="tw_overnight", overnight_fixed_close_to_open=True,
        portfolio_activation="pre_normalized", long_only=True,
    )
    assert np.expm1(result.strategy_returns.sum()) == pytest.approx(.04, abs=1e-7)
    assert result.shares_history[0, 0] == 4000
    assert result.shares_history[1, 0] == 0


def test_runtime_uses_stock_tax_and_refuses_unprepared_panel():
    c = load_config(CONFIG)
    p = panel_fixture()
    with pytest.raises(ValueError, match="13:25 panel context"):
        _build_execution_runtime(p, c, torch.device("cpu"))
    p = attach_overnight_prices(p, p.close_prices.copy())
    c.data.overnight_1325_missing_price_policy = "reject"
    runtime = _build_execution_runtime(p, c, torch.device("cpu"))
    assert runtime.overnight_fixed_close_to_open
    assert float(runtime.sell_fee_rates[0] - runtime.buy_fee_rates[0]) == pytest.approx(.003)
    assert runtime.commission_rebate_timing == "daily_close"


def test_1325_feature_is_causal_and_missing_input_does_not_block_due_exit():
    p = panel_fixture()
    p2 = copy.deepcopy(p)
    p2.close_prices[2:] *= 3
    prices = np.full(p.close_prices.shape, 102.)
    prices[2, 1] = np.nan
    a = attach_overnight_prices(p, prices)
    b = attach_overnight_prices(p2, prices)
    assert a.feature_names[-1] == OVERNIGHT_1325_FEATURE
    np.testing.assert_array_equal(a.features[:2], b.features[:2])
    assert a.features[1, 0, -1] == pytest.approx(np.log(1.02))
    ds = CrossSectionalDataset(a, np.arange(4), 1, execution_mode="tw_overnight")
    assert not ds.tradable_mask_t[2, 1]
    assert ds.day_trade_can_sell_open_mask_t[2, 1]
    assert not ds.tradable_mask_t[ds.valid_indices[-1]].any()
    assert ds.day_trade_can_sell_open_mask_t[ds.valid_indices[-1]].all()


def source_fixture(tmp_path, *, minute=25):
    dates = ["2024-01-05", "2024-01-08", "2024-01-09"]
    parts = []
    for day in dates:
        path = tmp_path / f"trade_date={day}" / "data.parquet"
        path.parent.mkdir()
        pq.write_table(pa.Table.from_pylist([{
            "ts": datetime.fromisoformat(day + f"T13:{minute}:00"),
            "symbol": "2330", "Close": 101., "minutes_from_open": 265,
        }]), path)
        parts.append({"trade_date": day, "output_sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
    (tmp_path / "manifest.json").write_text(json.dumps({
        "schema_version": 4, "source": "shioaji_kbars_1m", "research_ready": True,
        "status": "research_ready", "decision_clock": "completed_right_labelled_1m_bar",
        "dates": dates, "partitions": parts,
    }))


def test_source_hash_and_clock_are_required(tmp_path):
    source_fixture(tmp_path)
    p = attach_overnight_1325(panel_fixture(), tmp_path)
    assert p.overnight_1325_available[1:, 0].all()
    assert not p.overnight_1325_available[:, 1].any()
    assert p.overnight_1325_source['manifest_sha256'] == hashlib.sha256(
        (tmp_path / 'manifest.json').read_bytes()).hexdigest()
    path = tmp_path / "trade_date=2024-01-05/data.parquet"
    path.write_bytes(path.read_bytes() + b"tamper")
    with pytest.raises(RuntimeError, match="SHA256"):
        attach_overnight_1325(panel_fixture(), tmp_path)


def test_source_identity_is_bound_to_prepared_panel_not_later_live_manifest(tmp_path):
    source_fixture(tmp_path)
    config = load_config(CONFIG)
    config.data.overnight_1325_root = str(tmp_path)
    panel = attach_overnight_1325(panel_fixture(), tmp_path)
    before = build_checkpoint_manifest(panel, config)
    (tmp_path / 'manifest.json').write_text('{}')
    assert build_checkpoint_manifest(panel, config) == before
    panel.overnight_1325_source = dict(panel.overnight_1325_source, source_root='/another/node')
    assert build_checkpoint_manifest(panel, config) == before
    panel.overnight_1325_source = dict(panel.overnight_1325_source, manifest_sha256='f' * 64)
    assert build_checkpoint_manifest(panel, config) != before


def test_source_cannot_relabel_1330_as_1325(tmp_path):
    source_fixture(tmp_path, minute=30)
    with pytest.raises(RuntimeError, match="timestamp"):
        attach_overnight_1325(panel_fixture(), tmp_path)


def test_missing_sessions_do_not_shorten_horizon(tmp_path):
    source_fixture(tmp_path)
    p = panel_fixture(["2014-01-02", "2014-01-03"])
    with pytest.raises(RuntimeError, match="No daily-price fallback"):
        attach_overnight_1325(p, tmp_path)


def test_close_fallback_prefers_observed_1325_and_marks_each_substitution(tmp_path):
    source_fixture(tmp_path)
    p = panel_fixture()
    p.close_prices[1, :] = 110.
    p = attach_overnight_1325(p, tmp_path, missing_price_policy="same_session_close")
    assert p.features[0, 0, -1] == pytest.approx(np.log(1.01))
    assert p.features[0, 1, -1] == pytest.approx(np.log(1.1))
    assert not p.overnight_1325_close_fallback_mask[:, 0].any()
    assert p.overnight_1325_close_fallback_mask[1:, 1].all()
    assert p.overnight_1325_source['close_fallback_symbol_sessions'] == 3
    assert len(p.feature_names) == 24
    assert len(p.dates) == 4


@pytest.mark.parametrize('source_present', [False, True])
def test_close_fallback_preserves_early_history_without_minute_partitions(tmp_path, source_present):
    if source_present:
        source_fixture(tmp_path)
    p = panel_fixture(['2014-01-02', '2014-01-03', '2014-01-06'])
    p.close_prices[1] = 105.
    p.close_prices[2, 1] = np.nan
    p = attach_overnight_1325(p, tmp_path, missing_price_policy='same_session_close')
    assert p.features[0, 0, -1] == pytest.approx(np.log(1.05))
    assert p.overnight_1325_close_fallback_mask[1].all()
    assert p.overnight_1325_available[2, 0]
    assert not p.overnight_1325_available[2, 1]
    assert p.overnight_1325_source['missing_partition_dates'] == ['2014-01-03', '2014-01-06']
    assert p.overnight_1325_source['verified_session_partitions'] == 0


def test_missing_file_uses_close_but_corrupt_file_still_fails(tmp_path):
    source_fixture(tmp_path)
    path = tmp_path / 'trade_date=2024-01-08/data.parquet'
    path.unlink()
    p = attach_overnight_1325(panel_fixture(), tmp_path, missing_price_policy='same_session_close')
    assert p.overnight_1325_close_fallback_mask[2].all()
    path.write_bytes(b'corrupt')
    with pytest.raises(RuntimeError, match='SHA256'):
        attach_overnight_1325(panel_fixture(), tmp_path, missing_price_policy='same_session_close')


def test_fallback_cannot_hide_invalid_clock_or_manifest(tmp_path):
    source_fixture(tmp_path, minute=30)
    with pytest.raises(RuntimeError, match='timestamp'):
        attach_overnight_1325(panel_fixture(), tmp_path, missing_price_policy='same_session_close')
    (tmp_path / 'manifest.json').write_text('{}')
    with pytest.raises(RuntimeError, match='receipt-verified'):
        attach_overnight_1325(panel_fixture(), tmp_path, missing_price_policy='same_session_close')


def test_fallback_policy_is_bound_to_runtime_and_checkpoint(tmp_path):
    c = load_config(CONFIG)
    assert c.data.overnight_1325_missing_price_policy == 'same_session_close'
    p = attach_overnight_prices(panel_fixture(), np.full((4, 2), np.nan),
                                missing_price_policy='same_session_close')
    _build_execution_runtime(p, c, torch.device('cpu'))
    approximate = build_checkpoint_manifest(p, c)
    c.data.overnight_1325_missing_price_policy = 'reject'
    strict = build_checkpoint_manifest(p, c)
    assert strict != approximate
    with pytest.raises(RuntimeError, match='semantic fingerprint mismatch'):
        validate_checkpoint_manifest({'experiment_manifest': approximate}, strict,
                                     checkpoint_path=tmp_path / 'checkpoint.pt', scope='resume')
    with pytest.raises(ValueError, match='fallback policy disagrees'):
        _build_execution_runtime(p, c, torch.device('cpu'))


def test_frozen_remote_model_and_fixed_action_head():
    config = load_config(CONFIG)
    assert config.training.lookback == 32
    assert config.training.epochs == 1000
    assert len(config.training.financial_transformer.temporal_basis_families) == 22
    assert config.trading.volume_participation_equity == 10_000_000
    assert len(config.data.feature_include) == 23
    fit = fit_training_only_pca_klt(torch.randn(40, 2, 24), np.arange(32, 40),
                                    lookback=32, feature_lag=1, components=31)
    model = build_model(config=config, lookback=32, num_features=24, num_symbols=2,
                        temporal_basis_overrides={"pca_klt": fit.basis})
    model.eval()
    x = torch.randn(2, 32, 2, 24)
    with torch.no_grad():
        out = model(x, torch.ones(2, 2, dtype=torch.bool))
    if isinstance(out, tuple):
        out = out[0]
    assert out.shape == (2, 3, 2)
    assert (out[:, 0] == 1).all()
    assert (out[:, 1] == 0).all()
    assert (out[:, 2] >= 0).all()
    assert (out[:, 2].sum(-1) <= 1 + 1e-6).all()
