"""Two real CPU ranks reproduce one canonical crypto trajectory update."""

from datetime import timedelta
from functools import partial
import multiprocessing as mp

import pytest
import torch
import torch.distributed as dist
from torch.amp import GradScaler
from torch.nn.parallel import DistributedDataParallel

from stockagent.training import trainer
from stockagent.training.loss import risk_aware_loss
from stockagent.training.windowed import WindowedSplitTensors


class _TinyPolicy(torch.nn.Module):
    def __init__(self, initial):
        super().__init__()
        self.scale = torch.nn.Parameter(torch.tensor(initial))
        self.seen = []

    def forward(self, x, mask, **kwargs):
        self.seen.append(self.scale.detach().item())
        return self.scale * x[:, -1, :, 0]


class _StepCounter(torch.optim.SGD):
    def __init__(self, parameters):
        super().__init__(parameters, lr=0.1)
        self.calls = 0

    def step(self, closure=None):
        self.calls += 1
        return super().step(closure)


class _SchedulerCounter:
    def __init__(self):
        self.calls = 0

    def step(self):
        self.calls += 1


def _split(batch_size, announced_exit=False):
    price = torch.tensor([[.01, -.02], [-.03, .01], [.02, .04], [.01, -.03], [-.02, .01]])
    yes = torch.ones_like(price, dtype=torch.bool)
    buy = yes.clone()
    buy[2, 0] = False
    policy, force_exit = yes.clone(), ~yes
    volume = torch.full_like(price, 6_000_000.)
    if announced_exit:
        policy[3:, 0] = False
        force_exit[3:, 0] = True
        # The prior inventory cannot exit through the ordinary volume budget.
        volume[3:, 0] = 0.0
    split = WindowedSplitTensors(
        features=torch.tensor([[1., -.4], [-.3, .8], [.7, -.8], [.2, -.5], [-.8, .6]]).unsqueeze(-1),
        valid_indices=torch.arange(5), future_log_returns=torch.log1p(price - .0003),
        overnight_log_returns=torch.log1p(price), tradable_mask=policy,
        can_buy_mask=buy, can_sell_mask=yes, can_short_open_mask=yes,
        force_exit_mask=force_exit,
        benchmark=torch.zeros(5), volume_notional=volume,
        lookback=1, execution_mode="crypto_perpetual",
    )
    return trainer._pad_windowed_training_split(split, batch_size=batch_size)


def _update(initial, batch_size, clipping, local_metadata, announced_exit=False, *, distributed):
    policy = _TinyPolicy(initial)
    model = DistributedDataParallel(policy) if distributed else policy
    optimizer = _StepCounter(model.parameters())
    scheduler = _SchedulerCounter()
    loss_fn = partial(
        risk_aware_loss, execution_mode="crypto_perpetual",
        portfolio_activation="pre_normalized", crypto_stateful_proximal_allocator=True,
        crypto_announced_exit_unlimited_volume=announced_exit,
        log_utility_periods_per_year=365.,
    )
    options = dict(
        batch_size=batch_size, device=torch.device("cpu"), amp_dtype=None,
        non_blocking=False, long_only=False, buy_fee_rate=.00055, sell_fee_rate=.00055,
        max_turnover_ratio=0., gross_leverage=1., gamma_sharpe=1., gamma_excess=0.,
        gamma_cvar=0., cvar_alpha=.05, gamma_drawdown=0., drawdown_target=0.,
        gamma_turnover=0., gamma_underperformance=0., excess_target=0., cvar_budget=0.,
        drawdown_budget=0., turnover_budget=0., gamma_cvar_budget=0.,
        gamma_drawdown_budget=0., gamma_turnover_budget=0., objective="log_utility",
        rank_ic_weight=0., return_rank_ic_weight=0., direction_weight=0.,
        volatility_regime_weight=0., concentration_weight=0.,
        grad_clip_norm=clipping, max_volume_participation=.01,
        volume_participation_equity=1_000_000., lr_scheduler=scheduler,
        lr_scheduler_interval="step", optimizer_step_per_trajectory=True,
        finite_check_interval_steps=0,
    )
    args = (loss_fn, _split(batch_size, announced_exit), optimizer, GradScaler("cpu", enabled=False))
    if distributed:
        loss, timing = trainer._train_epoch_windowed_tensor_ddp(
            model, *args, replicated_ledger_local_metadata=local_metadata, **options,
        )
    else:
        loss, timing = trainer._train_epoch_windowed_tensor(model, None, *args, **options)
    assert optimizer.calls == scheduler.calls == timing.optimizer_steps == 1
    assert timing.gradient_norm_observations == 1
    assert policy.seen == pytest.approx([initial] * timing.batches)
    assert timing.batches == (5 + batch_size - 1) // batch_size
    assert policy.scale.grad is not None and torch.isfinite(policy.scale.grad)
    assert timing.gradient_norm_before_clip_sum > 0.
    return (loss.item(), policy.scale.item(), policy.scale.grad.item(),
            timing.gradient_norm_before_clip_sum)


_CASES = [(*case, local_metadata, announced_exit)
          for case in [(0., 2, 100.), (.1, 2, 100.), (0., 4, .03), (.1, 4, .03)]
          for local_metadata in (False, True)
          for announced_exit in (False, True)]


def _worker(rank, init_file, results):
    torch.set_num_threads(1)
    dist.init_process_group("gloo", init_method=f"file://{init_file}", rank=rank,
                            world_size=2, timeout=timedelta(seconds=20))
    try:
        observed = [_update(*case, distributed=True) for case in _CASES]
        results.put((rank, observed))
    except BaseException as exc:
        results.put((rank, f"{type(exc).__name__}: {exc}"))
    finally:
        dist.destroy_process_group()


def test_real_two_rank_gradient_aggregation_clipping_tail_and_one_update(tmp_path, monkeypatch):
    monkeypatch.setenv("CUDA_VISIBLE_DEVICES", "")
    reference = [_update(*case, distributed=False) for case in _CASES]
    context = mp.get_context("spawn")
    results = context.Queue()
    workers = [context.Process(target=_worker,
                               args=(rank, str(tmp_path / "rendezvous"), results))
               for rank in range(2)]
    try:
        for worker in workers:
            worker.start()
        messages = [results.get(timeout=40) for _ in workers]
        assert sorted(rank for rank, _ in messages) == [0, 1]
        for rank, observed in messages:
            assert not isinstance(observed, str), f"rank {rank}: {observed}"
            for actual, expected in zip(observed, reference, strict=True):
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
