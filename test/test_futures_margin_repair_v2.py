"""Regressions for the source-reviewed Fold 14 margin repair, without training."""
from copy import deepcopy
from datetime import date
import hashlib
import json
from pathlib import Path
import shutil
from types import SimpleNamespace

import numpy as np
import polars as pl
import pytest
import torch

from stockagent.backtest.tw_futures_portfolio import run_tw_futures_portfolio_integer_torch as run
from stockagent.config import load_config
from stockagent.data import tw_futures_margin as m
from stockagent.data.tw_futures_benchmark import load_tx_front_rolling_benchmark, resolve_tx_benchmark_path
from stockagent.data.tw_futures_position_review import apply_position_cap_review, reviewed_cap
from stockagent.data.panel import PanelData, slice_panel_end
from stockagent.training.checkpoint_contract import _active_scheduler_checkpoint_contract, _trading_checkpoint_contract
from stockagent.training.trainer import _benchmark_reporting_details, _create_lr_scheduler, _save_stock_context_tx_rolling_benchmark_audit


ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT / 'configs/markets/tw_futures_v8_margin_components_repaired_capital100m_fold14_v2.yaml'


def physical_tape(rows=2, slots=2):
    x = torch.zeros(rows, slots, m.MARGIN_EXECUTION_WIDTH)
    x[..., 1] = 1
    x[..., 3:5] = 1000
    x[..., 8] = 1000
    x[..., 9] = torch.arange(slots)
    for field in (m.INITIAL, m.END_INITIAL, m.PREVIOUS_INITIAL):
        x[..., field] = 100
    for field in (m.MAINTENANCE, m.END_MAINTENANCE, m.PREVIOUS_MAINTENANCE):
        x[..., field] = 75
    x[..., m.PREVIOUS_MARK] = x[..., m.TERMINAL_MARK] = 1000
    for field in (m.CAN_BUY, m.CAN_SELL, m.TERMINAL_CAN_BUY, m.TERMINAL_CAN_SELL):
        x[..., field] = 1
    x[..., m.POSITION_GROUP] = torch.arange(slots)
    x[..., m.POSITION_UNIT] = 1
    x[..., m.POSITION_LIMIT] = 1000
    x[..., m.LIQUIDATION_RATIO] = .25
    x[..., m.TERMINAL_CAPACITY] = 1000
    return x


@pytest.mark.parametrize('sign', [-1., 1.])
def test_failed_terminal_does_not_penalize_another_ordinary_overnight_position(sign):
    x = physical_tape()
    x[1, 0, 2] = 1
    x[1, 0, 8] = 0
    x[1, 0, m.TERMINAL_CAPACITY] = 0
    x[1, 1, 8] = 0
    gradients = []
    for overnight in (0., .2):
        w = torch.tensor([[sign * .3, overnight], [0., overnight]], requires_grad=True)
        result = run(w, x, initial_capital=1000., recoverable_backward=True)
        result.strategy_returns.sum().backward()
        assert result.default_reason_history.tolist() == [0, 4]
        assert result.margin_audit_history[-1, 2].item() == 1000.
        assert not result.final_alive
        assert torch.isfinite(w.grad).all()
        assert w.grad[0, 0].item() * sign < 0  # Only the failed delivery must reduce.
        assert w.grad[:, 1].abs().max().item() == 0
        gradients.append(w.grad[0, 0].item())
    assert gradients[0] == pytest.approx(gradients[1], abs=1e-6)


@pytest.mark.parametrize('sign', [-1., 1.])
def test_real_nonpositive_equity_remains_absorbing(sign):
    x = physical_tape(3, 1)
    x[1, :, 3:5] = 1000 - sign * 300
    x[2, :, 3:5] = 1000 + sign * 1000
    x[2, :, m.PREVIOUS_MARK] = x[1, :, 4]
    w = torch.tensor([[sign * .5], [sign * .5], [sign * .9]], requires_grad=True)
    result = run(w, x, initial_capital=1000., recoverable_backward=True)
    result.strategy_returns.sum().backward()
    assert result.margin_audit_history[1, 2].item() == -500
    assert not result.final_alive
    assert result.final_equity_scale.item() == 0
    assert result.contract_quantities_history[2].count_nonzero() == 0
    assert w.grad[2].count_nonzero() == 0


def review_fixture():
    review = json.loads((ROOT / 'configs/data/tw_futures_odf_position_review_20261003.json').read_text())
    review['expected_rows'] = 2
    rows = pl.DataFrame({
        'date': [date(2015, 10, 8), date(2015, 10, 8), date(2011, 6, 29), date(2015, 11, 19)],
        'product': ['OD1', 'ODF', 'HOF', 'ODF'],
        'position_group': ['ODF', 'ODF', 'HOF', 'ODF'],
        'position_unit': [2299.6612, 2000., 1., 2000.],
        'position_limit': [4599.323, 4599.323, 720958.42, 4000000.],
        'unrelated_accounting_field': [10., 11., 12., 13.],
    }).with_columns([
        pl.col('position_' + name).alias('second_position_' + name)
        for name in ('group', 'unit', 'limit')
    ])
    return rows, review


def test_review_changes_only_explicit_cells_and_preserves_valid_fractional_shares():
    rows, review = review_fixture()
    original = rows.clone()
    corrected, receipt = apply_position_cap_review(rows, review)
    assert rows.equals(original)
    assert corrected['position_limit'].to_list() == [4599323., 4599323., 720958.42, 4000000.]
    assert corrected['second_position_limit'].to_list() == corrected['position_limit'].to_list()
    assert receipt['changed_cells'] == {'position_limit': 2, 'second_position_limit': 2}
    assert receipt['other_accounting_fields_equal'] and receipt['unaffected_rows_equal']
    with pytest.raises(ValueError, match='old cap'):
        apply_position_cap_review(corrected, review)


@pytest.mark.parametrize('key,value,message', [
    ('known_at', '2015-10-09T00:00:00+08:00', 'publication clock'),
    ('expected_rows', 3, 'scope/row count'),
    ('position_units', {'OD1': 1., 'ODF': 2000.}, 'conversion unit'),
    ('official_natural_person_cap', '4,599.323.0', 'ambiguous'),
    ('parent_rules_sha256', 'bad', 'sha256'),
])
def test_review_rejects_ambiguous_or_mismatched_evidence(key, value, message):
    rows, review = review_fixture()
    review[key] = value
    with pytest.raises(ValueError, match=message):
        apply_position_cap_review(rows, review)


def test_fractional_official_cap_is_accepted_when_explicitly_reviewed():
    _, review = review_fixture()
    review['official_natural_person_cap'] = '720,958.42'
    assert reviewed_cap(review)[-1] == 720958.42


def tx_source(tmp_path):
    path = tmp_path / 'tx.parquet'
    pl.DataFrame({
        'date': [date(2026, 1, 2), date(2026, 1, 2), date(2026, 1, 5), date(2026, 1, 5)],
        'product': ['TX'] * 4,
        'contract': ['202601', '202602', '202601', '202602'],
        'tenor_rank': [1, 2, 2, 1],
        'close': [100., 200., 110., 210.],
        'source_row_observed': [True] * 4,
    }).write_parquet(path)
    return path


def test_benchmark_roll_prices_same_contract_and_pin_rejects_changed_source(tmp_path):
    path = tx_source(tmp_path)
    dates = np.array(['2026-01-02', '2026-01-05'], dtype='datetime64[D]')
    result = load_tx_front_rolling_benchmark(path, dates)
    assert result['benchmark_log_returns'][1] == pytest.approx(np.log(210 / 200))
    assert result['front_month_roll_mask'].tolist() == [False, True]
    assert result['prior_same_contract_close'][1] == 200
    trading = SimpleNamespace(tw_futures_portfolio_data_path='unused_action_universe',
                              tw_futures_portfolio_benchmark_data_path=str(path),
                              tw_futures_portfolio_benchmark_sha256=hashlib.sha256(path.read_bytes()).hexdigest())
    assert resolve_tx_benchmark_path(trading) == path
    trading.tw_futures_portfolio_benchmark_sha256 = '0' * 64
    with pytest.raises(ValueError, match='SHA-256 mismatch'):
        resolve_tx_benchmark_path(trading)


def test_benchmark_rejects_missing_own_prior_mark_and_missing_dates(tmp_path):
    path = tx_source(tmp_path)
    dates = np.array(['2026-01-02', '2026-01-05'], dtype='datetime64[D]')
    pl.read_parquet(path).filter(~((pl.col('date') == date(2026, 1, 2)) & (pl.col('contract') == '202602'))).write_parquet(path)
    with pytest.raises(ValueError, match='own close'):
        load_tx_front_rolling_benchmark(path, dates)
    with pytest.raises(ValueError, match='source ends'):
        load_tx_front_rolling_benchmark(path, np.array(['2026-01-06'], dtype='datetime64[D]'))


def test_signed_loss_plateau_respects_absolute_improvement_and_configured_floor():
    cfg = load_config(CONFIG)
    cfg.training.lr_scheduler_patience = 1
    optimizer = torch.optim.SGD([torch.nn.Parameter(torch.ones(1))], lr=cfg.training.learning_rate)
    scheduler, _, needs_metric, interval = _create_lr_scheduler(optimizer, cfg, steps_per_epoch=1)
    assert needs_metric and interval == 'epoch'
    scheduler.step(-1.)
    for _ in range(30):
        scheduler.step(-.99995)
    assert scheduler.best == -1.
    assert optimizer.param_groups[0]['lr'] == pytest.approx(1e-6)
    assert _active_scheduler_checkpoint_contract(cfg)['threshold_mode'] == 'abs'
    assert _active_scheduler_checkpoint_contract(cfg)['min_lr'] == 1e-6
    scheduler.step(-1.001)
    assert scheduler.best == -1.001 and scheduler.num_bad_epochs == 0


def test_repaired_contract_names_tx_source_and_changes_with_pin():
    cfg = load_config(CONFIG)
    assert cfg.training.epochs == 1000
    assert cfg.training.early_stopping_no_improve_ratio == .1
    assert cfg.trading.tw_futures_portfolio_integer_initial_capital == 100000000
    assert m.MARGIN_TRAINING_GRADIENT_CONTRACT_VERSION == 12
    assert _benchmark_reporting_details(cfg)['benchmark_exposure'] == 'continuous_1x_long_not_integer_account'
    before = _trading_checkpoint_contract(cfg)
    assert before['futures_benchmark_contract']['source_sha256'] == cfg.trading.tw_futures_portfolio_benchmark_sha256
    changed = deepcopy(cfg)
    changed.trading.tw_futures_portfolio_benchmark_sha256 = '0' * 64
    assert _trading_checkpoint_contract(changed) != before


def test_benchmark_artifact_checks_return_values_not_only_label(tmp_path):
    cfg = load_config(CONFIG)
    path = tx_source(tmp_path)
    cfg.trading.tw_futures_portfolio_benchmark_data_path = str(path)
    cfg.trading.tw_futures_portfolio_benchmark_sha256 = hashlib.sha256(path.read_bytes()).hexdigest()
    dates = np.array(['2026-01-02', '2026-01-05'], dtype='datetime64[D]')
    returns = load_tx_front_rolling_benchmark(path, dates)['benchmark_log_returns']
    output = tmp_path / 'benchmark_audit.npz'
    _save_stock_context_tx_rolling_benchmark_audit(output, dates=dates, benchmark_returns=returns, config=cfg)
    with np.load(output) as receipt:
        assert receipt['contract_months'].tolist() == ['202601', '202602']
        np.testing.assert_array_equal(receipt['benchmark_log_returns'], returns)
    with pytest.raises(ValueError, match='differs from verified'):
        _save_stock_context_tx_rolling_benchmark_audit(output, dates=dates, benchmark_returns=np.zeros(2), config=cfg)


def test_verified_source_boundary_slices_all_basic_panel_rows():
    zero = np.zeros((3, 1), dtype=np.float32)
    panel = PanelData(
        dates=np.array(['2026-01-02', '2026-01-05', '2026-01-06'], dtype='datetime64[D]'),
        symbols=['stock_context'], feature_names=['f'], features=zero[:, :, None],
        returns_1d=zero, tradable_mask=zero.astype(bool), alive_mask=zero.astype(bool),
        benchmark_returns=zero[:, 0], close_prices=zero,
    )
    trimmed = slice_panel_end(panel, '2026-01-05')
    assert len(panel.dates) == 3
    assert len(trimmed.dates) == len(trimmed.features) == len(trimmed.benchmark_returns) == 2
    assert slice_panel_end(trimmed, '2026-01-05') is trimmed
    with pytest.raises(ValueError, match='precedes every'):
        slice_panel_end(panel, '2025-12-31')


def test_loader_envelope_reuse_requires_identical_source_bound_market_bytes(tmp_path):
    from scripts.repair_tw_futures_position_transcription import complete_training_envelope
    from stockagent.data.tw_futures_portfolio_daily import futures_slot_layout_version, TAIFEX_FUTURES_PORTFOLIO_FEATURE_CONTRACT_VERSION
    prepared, output = tmp_path / 'parent', tmp_path / 'reviewed'
    old = prepared / 'release/daily/continuous_daily.parquet'
    new = output / 'release/daily/continuous_daily.parquet'
    old.parent.mkdir(parents=True)
    new.parent.mkdir(parents=True)
    pl.DataFrame({'symbol': ['TAIFEX_SLOT_0001', 'TAIFEX_SLOT_2816']}).write_parquet(old)
    shutil.copyfile(old, new)
    parent = dict(contract_version=futures_slot_layout_version(2816),
                  feature_contract_version=TAIFEX_FUTURES_PORTFOLIO_FEATURE_CONTRACT_VERSION,
                  fixed_model_output_slots=2816,
                  outputs={'continuous_daily': {'sha256': hashlib.sha256(old.read_bytes()).hexdigest()}})
    old.with_name('manifest.json').write_text(json.dumps(parent))
    new.with_name('manifest.json').write_text(json.dumps({'status': 'complete', 'execution_rules_sha256': 'reviewed'}))
    receipt = complete_training_envelope(prepared, output)
    assert receipt['daily_bytes_equal']
    manifest = json.loads(new.with_name('manifest.json').read_text())
    assert manifest['fixed_model_output_slots'] == 2816
    assert manifest['execution_rules_sha256'] == 'reviewed'
    pl.DataFrame({'symbol': ['TAIFEX_SLOT_0002']}).write_parquet(new)
    with pytest.raises(ValueError, match='identical verified market bytes'):
        complete_training_envelope(prepared, output)
