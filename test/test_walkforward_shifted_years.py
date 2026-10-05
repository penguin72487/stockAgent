"""Annual ownership must shift once, in sessions, throughout training/reporting."""
from copy import deepcopy
import json
from pathlib import Path

import numpy as np
import pytest

from stockagent.backtest.report import compute_metrics_by_year, generate_annual_report
from stockagent.backtest.simulator import BacktestResult
from stockagent.config import load_config
from stockagent.data.panel import PanelData
from stockagent.data.walkforward import (
    annual_period_years, build_checkpoint_inference_fold, build_expanding_year_folds,
    period_labels_from_contract, year_boundary_offset_sessions, year_period_contract,
)
from stockagent.training.dataset import CrossSectionalDataset
from stockagent.training.checkpoint_contract import build_checkpoint_manifest, validate_checkpoint_manifest
from stockagent.training.trainer import _deployment_test_indices, _split_valid_indices
import stockagent.training.trainer as trainer


def calendar():
    dates = np.arange('2019-01-01', '2024-10-01', dtype='datetime64[D]')
    # Deliberately variable closures: offsets must use the actual observed calendar.
    dates = dates[np.is_busday(dates)]
    return dates[~np.isin(dates, np.array(['2021-01-04', '2022-01-03', '2022-01-04'], dtype='datetime64[D]'))]


def panel(dates):
    values = np.arange(len(dates), dtype=np.float32)[:, None]
    return PanelData(dates=dates, symbols=['context'], feature_names=['date_index'],
                     features=values[:, :, None], returns_1d=np.zeros_like(values),
                     tradable_mask=np.ones_like(values, dtype=bool), alive_mask=np.ones_like(values, dtype=bool),
                     benchmark_returns=np.zeros(len(dates), dtype=np.float32), close_prices=np.ones_like(values),
                     can_buy_mask=np.ones_like(values, dtype=bool), can_sell_mask=np.ones_like(values, dtype=bool))


def test_shifted_boundaries_partition_once_without_gaps_or_overlaps():
    dates = calendar()
    years = dates.astype('datetime64[Y]').astype(int) + 1970
    labels = annual_period_years(dates, 32)
    folds = build_expanding_year_folds(dates, 1, split_start_year=2020, year_boundary_offset_sessions=32)
    first = folds[0]
    assert first.train_years == [2020] and first.val_years == [2021] and first.test_years[0] == 2022
    for name, year in [('train_indices', 2020), ('val_indices', 2021), ('test_indices', 2022)]:
        indices = getattr(first, name)
        expected = np.flatnonzero(years == year)[0] + 32
        assert indices[0] == expected
        assert labels[indices[0]] == year
        assert labels[expected - 1] == year - 1
    assert first.train_indices[-1] + 1 == first.val_indices[0]
    assert first.val_indices[-1] + 1 == first.test_indices[0]
    owned = np.concatenate([first.train_indices, first.val_indices, first.test_indices])
    np.testing.assert_array_equal(owned, np.arange(first.train_indices[0], len(dates)))
    assert dates[first.val_indices[-1]].astype('datetime64[Y]') == np.datetime64('2022', 'Y')


def test_dataset_keeps_owned_first_day_and_uses_earlier_context_without_second_warmup():
    dates = calendar()
    p = panel(dates)
    fold = build_expanding_year_folds(dates, 1, split_start_year=2020, year_boundary_offset_sessions=32)[0]
    dataset = CrossSectionalDataset(p, fold.val_indices, 32, lookback_context='panel_history',
                                   include_volume_notional=False)
    assert dataset.valid_indices[0] == fold.val_indices[0]
    features = dataset[0]['x']
    np.testing.assert_array_equal(np.asarray(features)[:, 0, 0], np.arange(fold.val_indices[0] - 31, fold.val_indices[0] + 1))
    np.testing.assert_array_equal(_split_valid_indices(p, fold.val_indices, 32,
                                 'tw_stock_context_futures_portfolio', 'panel_history'), fold.val_indices)


def test_deployment_uses_previous_model_until_next_shifted_boundary():
    dates = calendar()
    folds = build_expanding_year_folds(dates, 1, split_start_year=2020, year_boundary_offset_sessions=32)
    first, second = folds[:2]
    p = panel(dates)
    owned = _deployment_test_indices(p, first, second, 32, 'tw_stock_context_futures_portfolio')
    assert owned[-1] + 1 == second.test_indices[0]
    assert dates[owned[-1]].astype('datetime64[Y]') == np.datetime64('2023', 'Y')
    assert not np.intersect1d(owned, second.test_indices).size


def test_checkpoint_restores_shifted_ownership_and_rejects_calendar_request():
    dates = calendar()
    saved = dict(fold_id=7, train_years=[2020], val_years=[2021], test_years=[2022, 2023, 2024],
                 experiment_manifest={'contracts': {'walk_forward': {'year_boundary_mode': 'lookback_shifted',
                                                                       'year_boundary_offset_sessions': 32}}})
    restored = build_checkpoint_inference_fold(dates, saved)
    expected = build_expanding_year_folds(dates, 1, split_start_year=2020, year_boundary_offset_sessions=32)[0]
    assert restored.fold_id == 7
    np.testing.assert_array_equal(restored.val_indices, expected.val_indices)
    np.testing.assert_array_equal(restored.test_indices, expected.test_indices)
    with pytest.raises(ValueError, match='annual boundary differs'):
        build_checkpoint_inference_fold(dates, saved, year_boundary_offset_sessions=0)


def test_report_subset_uses_full_calendar_not_its_own_first_date():
    dates = calendar()
    contract = year_period_contract(dates, 32)
    labels = annual_period_years(dates, 32)
    idx = np.flatnonzero(np.isin(labels, [2021, 2022]))
    returns = np.linspace(-.001, .002, len(idx))
    bt = BacktestResult(strategy_returns=returns, benchmark_returns=np.zeros(len(idx)),
                        turnovers=np.zeros(len(idx)), weights_history=np.zeros((len(idx), 1)))
    np.testing.assert_array_equal(period_labels_from_contract(dates[idx], contract), labels[idx])
    metrics = compute_metrics_by_year(bt, dates[idx], period_contract=contract)
    assert sorted(metrics) == [2021, 2022]
    for year in metrics:
        assert metrics[year]['cumulative_return'] == pytest.approx(np.expm1(returns[labels[idx] == year].sum()))
    report = generate_annual_report(bt, dates[idx], period_contract=contract)
    assert 'first trading session + 32 sessions' in report
    assert not any(line.startswith('2023') for line in report.splitlines())
    assert sum(np.log1p(metrics[y]['cumulative_return']) for y in metrics) == pytest.approx(returns.sum())


def test_first_period_curve_retains_next_january_until_model_switch(tmp_path, monkeypatch):
    dates = calendar()
    labels = annual_period_years(dates, 32)
    idx = np.flatnonzero(labels >= 2022)
    selected_dates = dates[idx]
    returns = np.linspace(-.001, .002, len(idx))
    bt = BacktestResult(strategy_returns=returns, benchmark_returns=np.zeros(len(idx)),
                        turnovers=np.zeros(len(idx)), weights_history=np.zeros((len(idx), 1)))
    fold_dir = trainer._fold_dir(tmp_path, 1)
    fold_dir.mkdir(parents=True)
    trainer._save_backtest_artifact(fold_dir / 'test_backtest.npz', bt, selected_dates)
    (tmp_path / 'walkforward_period_boundaries.json').write_text(json.dumps(year_period_contract(dates, 32)))
    captured = {}

    def capture(date_arrays, return_arrays, *args, **kwargs):
        captured['dates'] = np.concatenate(date_arrays)
        captured['returns'] = np.concatenate(return_arrays)

    monkeypatch.setattr(trainer, 'plot_fold_first_year_returns', capture)
    for name in ['plot_fold_first_year_returns_log10', 'plot_first_year_fold_metric_bars',
                 'plot_first_year_turnover_concentration']:
        monkeypatch.setattr(trainer, name, lambda *args, **kwargs: None)
    fold = trainer.FoldResult(fold_id=1, train_years=[2020], val_years=[2021],
                              test_years=[2022, 2023, 2024], best_val_loss=0.,
                              val_ic={}, val_metrics={}, test_ic={}, test_metrics={})
    trainer._refresh_walkforward_artifacts(tmp_path, [fold])
    mask = labels[idx] == 2022
    np.testing.assert_array_equal(captured['dates'], selected_dates[mask])
    np.testing.assert_array_equal(captured['returns'], returns[mask])
    assert np.any(captured['dates'].astype('datetime64[M]') == np.datetime64('2023-01'))
    assert captured['dates'][-1] + np.timedelta64(1, 'D') <= dates[np.flatnonzero(labels == 2023)[0]]


def test_partial_final_year_does_not_fabricate_a_new_model_boundary():
    dates = calendar()
    dates = dates[dates <= np.datetime64('2024-01-10')]
    labels = annual_period_years(dates, 32)
    assert labels[-1] == 2023
    folds = build_expanding_year_folds(dates, 1, split_start_year=2020, year_boundary_offset_sessions=32)
    assert folds[-1].test_years == [2023]


@pytest.mark.parametrize('offset', [-1, 1.5, True])
def test_invalid_offsets_rejected(offset):
    with pytest.raises(ValueError, match='nonnegative integer'):
        year_period_contract(calendar(), offset)


def test_calendar_mode_preserves_existing_first_session_contract():
    dates = calendar()
    fold = build_expanding_year_folds(dates, 1, split_start_year=2020)[0]
    assert dates[fold.val_indices[0]] == dates[np.flatnonzero(dates.astype('datetime64[Y]') == np.datetime64('2021', 'Y'))[0]]


def test_new_partition_changes_only_walk_forward_contract_and_rejects_old_resume():
    config = Path(__file__).resolve().parents[1] / 'configs/markets/tw_futures_v8_margin_components_repaired_year_shift32_capital100m_fold14_v3.yaml'
    shifted = load_config(config)
    # Test manifest compatibility without constructing a physical futures source.
    shifted.trading.execution_mode = 'naive'
    previous = deepcopy(shifted)
    previous.walk_forward.year_boundary_mode = 'calendar'
    p = panel(calendar())
    old = build_checkpoint_manifest(p, previous, include_data_content=False)
    new = build_checkpoint_manifest(p, shifted, include_data_content=False)
    assert 'year_boundary_mode' not in old['contracts']['walk_forward']
    assert new['contracts']['walk_forward']['year_boundary_offset_sessions'] == 32
    for layer in old['fingerprints']:
        assert (old['fingerprints'][layer] == new['fingerprints'][layer]) == (layer != 'walk_forward')
    with pytest.raises(RuntimeError, match='semantic fingerprint mismatch.*walk_forward'):
        validate_checkpoint_manifest({'experiment_manifest': old}, new, checkpoint_path=Path('calendar.pt'))


def test_new_config_offset_tracks_lookback_and_rejects_double_shift(tmp_path):
    config = Path(__file__).resolve().parents[1] / 'configs/markets/tw_futures_v8_margin_components_repaired_year_shift32_capital100m_fold14_v3.yaml'
    cfg = load_config(config)
    assert year_boundary_offset_sessions(cfg) == 32
    changed = deepcopy(cfg)
    changed.training.lookback = 16
    assert year_boundary_offset_sessions(changed) == 16
    invalid = tmp_path / 'double_shift.yaml'
    invalid.write_text(f'base_config: {config}\nwalk_forward:\n  lookback_context: split_only\n')
    with pytest.raises(ValueError, match='discard the warmup twice'):
        load_config(invalid)
