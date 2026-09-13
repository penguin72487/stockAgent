from functools import partial
from types import SimpleNamespace

import numpy as np
import pytest
import torch
from torch.amp import GradScaler

from stockagent.backtest.futures_data_validity import FuturesCarryDataError
from stockagent.training.loss import risk_aware_loss
from stockagent.training.trainer import _carry_loss_data_guard, _train_epoch_windowed_tensor
from stockagent.training.windowed import WindowedSplitTensors
from test_tw_stock_futures_carry import tape, bar, run, TAPE_FIELDS
from test_futures_trajectory_optimizer import _RecordedScalarPolicy, _CountingSGD, _CountingScheduler


@pytest.mark.parametrize('action', [-.05, 0., .05])
@pytest.mark.parametrize('exit_price', [99., 99.5, 100., 100.5, 101.])
def test_cash_boundary_gradient_uses_both_fees_and_integer_alternatives(action, exit_price):
    x = tape(1)
    bar(x, 0, exit_price, 10, event=6)
    w = torch.tensor([[action]], requires_grad=True)
    result = run(w, x, initial_capital=100000., use_compile=False)
    pnl = (result.final_equity_scale - 1) * 100000
    gradient = torch.autograd.grad(pnl, w)[0].item()
    long_pnl = 100 * (exit_price - 100) - 80
    short_pnl = 100 * (100 - exit_price) - 80
    width = 10080 / 100000
    expected = (long_pnl if action > 0 else -short_pnl) / width
    if action == 0:
        expected = (max(long_pnl, 0) if long_pnl >= short_pnl else -max(short_pnl, 0)) / width
    assert pnl == 0  # No forward trade or invented fee.
    assert gradient == pytest.approx(expected, rel=2e-6, abs=1e-5)
    # Check the independent scalar economics against actually filled one-lot runs.
    for direction, expected_pnl in ((1, long_pnl), (-1, short_pnl)):
        with torch.no_grad():
            actual = run(torch.tensor([[direction * .101]]), x, initial_capital=100000., use_compile=False)
        assert float(actual.final_equity_scale * 100000 - 100000) == pytest.approx(expected_pnl, abs=.01)


def test_diagnostic_unknown_tail_is_nan_and_canonical_loss_raises():
    x = tape(4); x[1, 0, 0, TAPE_FIELDS + 6] = 0
    w = torch.full((4, 1), .11, requires_grad=True)
    diagnostic = run(w, x, initial_capital=100000., diagnostic_only=True, use_compile=False)
    assert torch.isfinite(diagnostic.strategy_returns[0])
    assert torch.isnan(diagnostic.strategy_returns[1:]).all()
    assert diagnostic.final_carry_state[0, 0, 0] == 1
    with pytest.raises(FuturesCarryDataError, match='held_minute_source_missing'):
        risk_aware_loss(w, torch.zeros_like(w), torch.ones_like(w, dtype=torch.bool),
                        overnight_log_returns=x, objective='log_utility',
                        execution_mode='tw_stock_futures_day_trade_0845_minute',
                        portfolio_activation='pre_normalized', day_trade_execution_initial_capital=100000.)


@pytest.mark.parametrize('batch_size', [1, 2, 4])
def test_invalid_trajectory_never_updates_optimizer_or_scheduler(batch_size):
    x = tape(5); x[3, 0, 0, TAPE_FIELDS + 3] = 0.
    split = WindowedSplitTensors(
        features=torch.ones(5, 1, 1), valid_indices=torch.arange(1, 5),
        future_log_returns=torch.zeros(5, 1), tradable_mask=torch.ones(5, 1, dtype=torch.bool),
        can_buy_mask=torch.ones(5, 1, dtype=torch.bool), can_sell_mask=torch.ones(5, 1, dtype=torch.bool),
        benchmark=torch.zeros(5), lookback=1, overnight_log_returns=x,
        execution_mode='tw_stock_futures_day_trade_0845_minute',
    )
    model = _RecordedScalarPolicy(); model.scale.data.fill_(.11)
    optimizer = _CountingSGD(model.parameters()); scheduler = _CountingScheduler()
    loss_fn = partial(risk_aware_loss, execution_mode=split.execution_mode,
                      portfolio_activation='pre_normalized', day_trade_execution_initial_capital=100000.)
    kwargs = dict(long_only=False, buy_fee_rate=0., sell_fee_rate=0., max_turnover_ratio=0.,
        gross_leverage=1., gamma_sharpe=0., gamma_excess=0., gamma_cvar=0., cvar_alpha=.05,
        gamma_drawdown=0., drawdown_target=0., gamma_turnover=0., gamma_underperformance=0.,
        excess_target=0., cvar_budget=0., drawdown_budget=0., turnover_budget=0.,
        gamma_cvar_budget=0., gamma_drawdown_budget=0., gamma_turnover_budget=0.,
        objective='log_utility', grad_clip_norm=1., rank_ic_weight=0., return_rank_ic_weight=0.,
        direction_weight=0., volatility_regime_weight=0., concentration_weight=0.)
    with pytest.raises(FuturesCarryDataError) as failure:
        _train_epoch_windowed_tensor(model, None, loss_fn, split, optimizer, GradScaler('cpu', enabled=False),
            batch_size=batch_size, device=torch.device('cpu'), amp_dtype=None, non_blocking=False,
            optimizer_step_per_trajectory=True, lr_scheduler=scheduler, lr_scheduler_interval='step', **kwargs)
    assert failure.value.evidence['panel_row'] == 3
    assert optimizer.step_calls == scheduler.step_calls == 0
    assert model.scale.item() == pytest.approx(.11)
    assert all(p.grad is None for p in model.parameters())


def _one_bad_rank(rank, rendezvous):
    import torch.distributed as dist
    from datetime import timedelta
    dist.init_process_group('gloo', init_method=rendezvous, rank=rank, world_size=2,
                            timeout=timedelta(seconds=20))
    try:
        p = torch.nn.Parameter(torch.tensor(1.)); p.grad = torch.tensor(4.)
        opt = torch.optim.AdamW([p])
        split = SimpleNamespace(execution_mode='tw_stock_futures_day_trade_0845_minute',
            overnight_log_returns=tape(2), _valid_indices_cpu=torch.tensor([10, 11]))
        with pytest.raises(FuturesCarryDataError) as failure:
            with _carry_loss_data_guard(split, opt, torch.device('cpu'), 0, distributed=True):
                if rank == 1:
                    raise FuturesCarryDataError({'row': 1, 'reason': 'held_settlement_missing'})
        assert failure.value.evidence['panel_row'] == 11
        assert p.grad is None and p.item() == 1. and not opt.state
        from stockagent.training.trainer import _capture_carry_data_error, _synchronize_carry_data_error
        errors = []
        with _capture_carry_data_error(errors):
            if rank == 0:  # Independent validation, with no loss/backward collective.
                raise FuturesCarryDataError({'scope': 'evaluation', 'row': 0, 'reason': 'held_settlement_missing'})
        evidence = _synchronize_carry_data_error(errors[0] if errors else None, torch.device('cpu'), distributed=True)
        assert evidence['scope'] == 'evaluation'
        from stockagent.training.trainer import (
            _run_rank0_store_synchronized_phase, _begin_rank0_store_synchronized_phase,
        )
        def invalid_final_artifact():
            raise FuturesCarryDataError({'scope': 'evaluation', 'panel_row': 12,
                                         'reason': 'held_minute_source_missing'})
        with pytest.raises(FuturesCarryDataError) as final_error:
            _run_rank0_store_synchronized_phase('test_invalid_artifact', invalid_final_artifact)
        assert final_error.value.evidence['panel_row'] == 12
        phase = _begin_rank0_store_synchronized_phase('test_invalid_artifact_handle')
        error = FuturesCarryDataError({'scope': 'evaluation', 'panel_row': 13,
                                      'reason': 'held_settlement_missing'}) if rank == 0 else None
        with pytest.raises(FuturesCarryDataError) as final_error:
            phase.finish(error)
        assert final_error.value.evidence['panel_row'] == 13
    finally:
        dist.destroy_process_group()


def test_one_rank_data_error_stops_every_rank_before_backward(tmp_path):
    torch.multiprocessing.spawn(_one_bad_rank, args=(f'file://{tmp_path}/ddp',), nprocs=2, join=True)


def test_old_failure_marker_cannot_be_saved_or_reported_as_financial_return(tmp_path):
    from stockagent.backtest.simulator import run_backtest_torch
    from stockagent.backtest.report import compute_metrics
    from stockagent.training.trainer import _save_backtest_artifact
    w = torch.full((2, 1), .11)
    result = run_backtest_torch(w, torch.zeros_like(w), torch.ones_like(w, dtype=torch.bool),
        torch.zeros(2), 0., 0., execution_mode='tw_stock_futures_day_trade_0845_minute',
        overnight_returns=tape(2), portfolio_activation='pre_normalized',
        day_trade_execution_initial_capital=100000.).to_numpy()
    result.default_reason_history[1] = 2
    result.strategy_returns[1] = np.log(1e-7)  # An old v8 artifact's finite sentinel.
    for call in (lambda: compute_metrics(result),
                 lambda: _save_backtest_artifact(tmp_path/'invalid.npz', result,
                    np.array(['2026-01-19', '2026-01-20'], dtype='datetime64[D]'))):
        with pytest.raises(FuturesCarryDataError):
            call()
    assert not (tmp_path/'invalid.npz').exists()


def test_evaluation_error_keeps_original_panel_row_across_backtest_chunks():
    from stockagent.training.trainer import (
        _ExecutionRuntime, _run_eval_backtest_from_weight_buffers, TimingBreakdown,
    )
    x = tape(2); x[1, 0, 0, TAPE_FIELDS + 6] = 0
    w = torch.full((2, 1), .11); mask = torch.ones_like(w, dtype=torch.bool)
    runtime = _ExecutionRuntime(mode='tw_stock_futures_day_trade_0845_minute',
        buy_fee_rates=None, sell_fee_rates=None, lot_sizes=None, settlement_lag_sessions=0)
    with pytest.raises(FuturesCarryDataError) as failure:
        _run_eval_backtest_from_weight_buffers(
            w, torch.zeros_like(w), mask, mask, mask, mask, ~mask, ~mask, torch.zeros(2),
            device=torch.device('cpu'), non_blocking=False, long_only=False,
            buy_fee_rate=0., sell_fee_rate=0., max_turnover_ratio=0., gross_leverage=1.,
            min_trade_weight=0., backtest_chunk_rows=1, compute_metrics_summary=False,
            return_weights_history=True, profile_timing=False, progress_label='test evaluation',
            timing=TimingBreakdown(), reset_at_rows=None, portfolio_activation='pre_normalized',
            overnight_log_returns_all=x, execution_runtime=runtime,
            volume_participation_equity=100000., panel_row_indices=torch.tensor([12, 15]))
    assert failure.value.evidence['row'] == 0
    assert failure.value.evidence['split_row'] == 1
    assert failure.value.evidence['panel_row'] == 15


@pytest.mark.parametrize('sign', [-1, 1])
@pytest.mark.parametrize('opening_capacity', [0, 10])
def test_exdiv_credit_cancels_price_adjustment_once_for_old_inventory(sign, opening_capacity):
    x = tape(3)
    for day in (1, 2):
        bar(x, day, 98, opening_capacity)
        x[day, 0, 0, TAPE_FIELDS + 3] = 9800
    x[1, 0, 0, TAPE_FIELDS + 7] = 200
    w = torch.full((3, 1), sign * .11, requires_grad=True)
    out = run(w, x, initial_capital=100000., use_compile=False)
    torch.testing.assert_close(out.equity_scale_history, torch.full((3,), .9996))
    assert out.residual_contract_quantities_history[:, 0, 0].tolist() == [sign] * 3
    out.strategy_returns.sum().backward()
    assert torch.isfinite(w.grad).all()


def test_new_exdiv_position_receives_no_prior_holder_credit():
    x = tape(1); x[0, 0, 0, TAPE_FIELDS + 7] = 200
    out = run(torch.tensor([[.11]]), x, initial_capital=100000., use_compile=False)
    assert out.final_equity_scale.item() == pytest.approx(.9996)


def test_unresolved_corporate_transition_is_not_silent_old_code_rollover():
    x = tape(2); x[1, 0, 0, TAPE_FIELDS + 8] = 1
    with pytest.raises(FuturesCarryDataError, match='unresolved_corporate_contract_transition'):
        run(torch.full((2, 1), .11), x, initial_capital=100000., use_compile=False)
    # The source event does not become a look-ahead mask excluding new orders.
    out = run(torch.tensor([[0.], [.11]]), x, initial_capital=100000., use_compile=False)
    assert out.final_carry_state[0, 0, 0] == 1


def test_carry_failure_symbol_mapping_retains_local_index_and_input():
    from stockagent.backtest.futures_data_validity import map_carry_failure_symbols
    evidence = {'row':0, 'positions':[{'symbol_index':1, 'held_quantity':-1}]}
    mapped = map_carry_failure_symbols(evidence, torch.tensor([5,2021]))
    assert mapped['positions'][0]['panel_symbol_index'] == 2021
    assert mapped['positions'][0]['symbol_index'] == 1
    assert 'panel_symbol_index' not in evidence['positions'][0]
    assert map_carry_failure_symbols(evidence)['positions'][0]['panel_symbol_index'] == 1


def test_cash_adjustment_rounds_down_per_contract_before_applying_position_sign():
    from stockagent.data.tw_stock_futures_carry import cash_equity_adjustment
    assert cash_equity_adjustment(2.99892738, 2000) == 5997
    assert cash_equity_adjustment(.36, 2000) == 720
    assert cash_equity_adjustment(.29, 100) == 29


def test_carry_lifecycle_describes_actual_inventory_and_data_contract():
    from stockagent.config import load_config
    from stockagent.training.trainer import _mode_artifact_contract_for_config
    c = load_config('configs/markets/tw_stock_futures_day_trade_0845_carry_v9.yaml')
    mode = _mode_artifact_contract_for_config(c)
    assert 'physical_signed_contracts' in mode['recurrent_state_scope']
    assert mode['mode_details']['execution_contract_version'] == 4
    assert mode['mode_details']['daily_proxy_before'] is None
    assert 'cash_dividend_rule' in mode['mode_details']
