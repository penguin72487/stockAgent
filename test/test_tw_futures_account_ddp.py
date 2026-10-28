from datetime import timedelta
from functools import partial
from types import SimpleNamespace

import pytest
import torch
import torch.distributed as dist
import torch.multiprocessing as mp

from stockagent.backtest.tw_futures_account_calendar import run_futures_account_calendar_torch
from stockagent.training.futures_account_bridge import bind_padded_futures_account_loss
from test_tw_futures_account_bridge import _source


def _account_loss(actions, returns, mask, **kwargs):
    result = run_futures_account_calendar_torch(
        kwargs["futures_account_calendar"], actions, kwargs["overnight_log_returns"],
        kwargs["account_only_execution"], initial_capital=1000.,
    )
    return -result.decision_log_returns.sum()


def _split():
    return SimpleNamespace(_valid_indices_cpu=torch.tensor([0, 1, 1, 1]),
                           sample_mask=torch.tensor([True, True, False, False]),
                           execution_mode="tw_stock_context_futures_portfolio")


def _run_loss(actions):
    daily, source = _source()
    split = _split()
    loss = bind_padded_futures_account_loss(
        _account_loss, source=source, split=split, start=0, stop=4, device=torch.device("cpu"),
    )
    return loss(actions, torch.zeros(4, 1), torch.ones(4, 1, dtype=torch.bool),
                objective="log_utility", sample_mask=split.sample_mask,
                state_advance_mask=split.sample_mask.clone(),
                overnight_log_returns=torch.from_numpy(daily.integer_execution[[0, 1, 1, 1]]))


def _worker(rank, init_file, results):
    torch.set_num_threads(1)
    dist.init_process_group("gloo", init_method=f"file://{init_file}", rank=rank,
                            world_size=2, timeout=timedelta(seconds=30))
    try:
        from stockagent.training.trainer import _all_gather_autograd
        model = torch.nn.Linear(1, 1, bias=False)
        model.weight.data.fill_(.5)
        model = torch.nn.parallel.DistributedDataParallel(model)
        local_input = (torch.tensor([[1.], [0.]]) if rank == 0
                       else torch.tensor([[100.], [-100.]]))
        actions = _all_gather_autograd(model(local_input))
        loss = _run_loss(actions)
        loss.backward()
        results.put((rank, (loss.item(), model.module.weight.grad.item())))
    except BaseException as exc:
        results.put((rank, f"{type(exc).__name__}: {exc}"))
    finally:
        dist.destroy_process_group()


def test_real_two_rank_gather_has_unpadded_loss_and_parameter_gradient(tmp_path):
    torch.set_num_threads(1)
    model = torch.nn.Linear(1, 1, bias=False)
    model.weight.data.fill_(.5)
    reference = _run_loss(model(torch.tensor([[1.], [0.], [100.], [-100.]])))
    reference.backward()
    expected = (reference.item(), model.weight.grad.item())
    context = mp.get_context("spawn")
    results = context.Queue()
    workers = [context.Process(target=_worker, args=(rank, str(tmp_path / "rendezvous"), results))
               for rank in range(2)]
    try:
        for worker in workers:
            worker.start()
        messages = [results.get(timeout=60) for _ in workers]
        assert sorted(rank for rank, _ in messages) == [0, 1]
        for rank, actual in messages:
            assert not isinstance(actual, str), f"rank {rank}: {actual}"
            assert actual == pytest.approx(expected, rel=2e-5, abs=2e-7)
        for worker in workers:
            # Gloo teardown and Python subprocess cleanup can take longer on a
            # busy shared host even after both ranks have returned their results.
            worker.join(timeout=30)
            assert worker.exitcode == 0
    finally:
        for worker in workers:
            if worker.is_alive():
                worker.terminate()
            worker.join(timeout=5)
        results.close()


class _FuturesPolicy(torch.nn.Module):
    def __init__(self):
        super().__init__()
        self.scale = torch.nn.Parameter(torch.tensor(.5))
        self.register_buffer("slots", torch.nn.functional.one_hot(torch.tensor(0), 1936).float())

    def forward(self, x, mask, portfolio_context=None):
        return self.scale * x[:, -1, 0, 0, None] * self.slots[None, :]


def _trainer_update(distributed, prepared=False):
    from torch.amp import GradScaler
    from stockagent.data import tw_futures_margin as margin
    from stockagent.data.tw_futures_account_calendar import build_futures_account_calendar
    from stockagent.data.tw_stock_context_futures_portfolio import (
        TW_STOCK_CONTEXT_FUTURES_PRIOR_MARKET_FEATURE_COLUMNS,
    )
    from stockagent.training import trainer
    from stockagent.training.futures_account_bridge import PreparedFuturesAccountSource
    from stockagent.training.loss import risk_aware_loss
    from stockagent.training.windowed import WindowedSplitTensors
    from test_tw_futures_margin import tape

    dates = build_futures_account_calendar(
        ["2016-02-01", "2016-02-02", "2016-02-03", "2016-02-15"],
        ["2016-02-01", "2016-02-02", "2016-02-03", "2016-02-04", "2016-02-15"],
        [False, False, False, True, False],
    )
    decisions, events = tape(rows=4, slots=1936), tape(rows=1, slots=1936)
    events[..., 1] = events[..., 8] = 0
    events[0, 0, 2] = events[0, 0, margin.CASH_SETTLEMENT] = 1
    events[0, 0, margin.TERMINAL_MARK] = 1100
    events[0, 0, 5] = 2
    events[0, 0, 7] = 3
    daily = SimpleNamespace(futures_account_calendar=dates, integer_execution=decisions.numpy(),
                            account_only_integer_execution=events.numpy(),
                            symbols=tuple(f"slot{i:04}" for i in range(1936)),
                            must_liquidate_mask=(decisions[..., 2] > .5).numpy(),
                            margin_contract_version=1)
    source = PreparedFuturesAccountSource.from_daily(
        daily, dates.decision_dates, prepare_execution=prepared,
    )
    yes = torch.ones(4, 1, dtype=torch.bool)
    split = WindowedSplitTensors(
        features=torch.tensor([[[0.]], [[1.]], [[0.]], [[0.]]]), valid_indices=torch.arange(1, 4),
        future_log_returns=torch.zeros(4, 1), overnight_log_returns=decisions,
        derivative_candidate_features=torch.zeros(
            4, 1936, len(TW_STOCK_CONTEXT_FUTURES_PRIOR_MARKET_FEATURE_COLUMNS)),
        derivative_candidate_mask=torch.ones(4, 1936, dtype=torch.bool),
        tradable_mask=yes, can_buy_mask=yes, can_sell_mask=yes,
        benchmark=torch.zeros(4), lookback=1, execution_mode="tw_stock_context_futures_portfolio",
        futures_account_source_fingerprint=source.fingerprint,
        futures_execution_release_fingerprint=source.execution_release_fingerprint,
    )
    split = trainer._pad_windowed_training_split(split, batch_size=2)
    runtime = trainer._ExecutionRuntime(mode="tw_stock_context_futures_portfolio", buy_fee_rates=None,
        sell_fee_rates=None, lot_sizes=None, settlement_lag_sessions=0, futures_account_source=source)
    policy = _FuturesPolicy()
    model = torch.nn.parallel.DistributedDataParallel(policy) if distributed else policy
    optimizer = torch.optim.SGD(model.parameters(), lr=.01)
    loss_fn = partial(risk_aware_loss, execution_mode="tw_stock_context_futures_portfolio",
                      portfolio_activation="pre_normalized", day_trade_execution_initial_capital=1000.,
                      log_utility_periods_per_year=1.)
    options = dict(batch_size=2, device=torch.device("cpu"), amp_dtype=None,
        non_blocking=False, long_only=False, buy_fee_rate=0., sell_fee_rate=0.,
        max_turnover_ratio=0., gross_leverage=1., gamma_sharpe=1., gamma_excess=0.,
        gamma_cvar=0., cvar_alpha=.05, gamma_drawdown=0., drawdown_target=0.,
        gamma_turnover=0., gamma_underperformance=0., excess_target=0., cvar_budget=0.,
        drawdown_budget=0., turnover_budget=0., gamma_cvar_budget=0.,
        gamma_drawdown_budget=0., gamma_turnover_budget=0., objective="log_utility",
        rank_ic_weight=0., return_rank_ic_weight=0., direction_weight=0.,
        volatility_regime_weight=0., concentration_weight=0., grad_clip_norm=100.,
        execution_runtime=runtime, optimizer_step_per_trajectory=True)
    args = (loss_fn, split, optimizer, GradScaler("cpu", enabled=False))
    original_move = trainer._move_windowed_batch_to_device
    staged_execution = []

    def record_move(batch, *move_args, **move_kwargs):
        staged_execution.append("overnight_log_returns" in batch)
        return original_move(batch, *move_args, **move_kwargs)

    trainer._move_windowed_batch_to_device = record_move
    try:
        if distributed:
            loss, timing = trainer._train_epoch_windowed_tensor_ddp(model, *args, **options)
        else:
            loss, timing = trainer._train_epoch_windowed_tensor(model, None, *args, **options)
    finally:
        trainer._move_windowed_batch_to_device = original_move
    assert staged_execution and all(value is not prepared for value in staged_execution)
    assert timing.batches == 2 and timing.optimizer_steps == 1
    assert policy.scale.grad is not None and torch.isfinite(policy.scale.grad)
    return (loss.item(), policy.scale.item(), policy.scale.grad.item())


def _trainer_worker(rank, init_file, results, prepared):
    torch.set_num_threads(1)
    dist.init_process_group("gloo", init_method=f"file://{init_file}", rank=rank,
                            world_size=2, timeout=timedelta(seconds=40))
    try:
        results.put((rank, _trainer_update(distributed=True, prepared=prepared)))
    except BaseException as exc:
        results.put((rank, f"{type(exc).__name__}: {exc}"))
    finally:
        dist.destroy_process_group()


def _whole_sequence_reference():
    from stockagent.data import tw_futures_margin as margin
    from stockagent.data.tw_futures_account_calendar import build_futures_account_calendar
    from test_tw_futures_margin import tape

    dates = build_futures_account_calendar(
        ["2016-02-02", "2016-02-03", "2016-02-15"],
        ["2016-02-02", "2016-02-03", "2016-02-04", "2016-02-15"],
        [False, False, True, False],
    )
    decisions, events = tape(rows=3, slots=1936), tape(rows=1, slots=1936)
    events[..., 1] = events[..., 8] = 0
    events[0, 0, 2] = events[0, 0, margin.CASH_SETTLEMENT] = 1
    events[0, 0, margin.TERMINAL_MARK] = 1100
    events[0, 0, 5], events[0, 0, 7] = 2, 3
    scale = torch.tensor(.5, requires_grad=True)
    features = torch.tensor([0., 1., 0.])[:, None]
    slots = torch.nn.functional.one_hot(torch.tensor(0), 1936).float()[None, :]
    result = run_futures_account_calendar_torch(
        dates, scale * features * slots, decisions, events, initial_capital=1000.,
    )
    loss = -result.decision_log_returns.sum() / 3
    loss.backward()
    gradient = scale.grad.item()
    return loss.item(), scale.item() - .01 * gradient, gradient


@pytest.mark.parametrize("prepared", [False, True])
def test_real_trainer_carries_holiday_settlement_across_two_rank_batches(tmp_path, prepared):
    torch.set_num_threads(1)
    expected = _trainer_update(distributed=False, prepared=prepared)
    assert expected[0] < 0
    assert abs(expected[2]) > 0
    assert expected == pytest.approx(_whole_sequence_reference(), rel=2e-5, abs=2e-7)
    context = mp.get_context("spawn")
    results = context.Queue()
    workers = [context.Process(target=_trainer_worker, args=(rank, str(tmp_path / "rendezvous"), results, prepared))
               for rank in range(2)]
    try:
        for worker in workers:
            worker.start()
        messages = [results.get(timeout=90) for _ in workers]
        assert sorted(rank for rank, _ in messages) == [0, 1]
        for rank, actual in messages:
            assert not isinstance(actual, str), f"rank {rank}: {actual}"
            assert actual == pytest.approx(expected, rel=2e-5, abs=2e-7)
        for worker in workers:
            worker.join(timeout=5)
            assert worker.exitcode == 0
    finally:
        for worker in workers:
            if worker.is_alive():
                worker.terminate()
            worker.join(timeout=5)
        results.close()
