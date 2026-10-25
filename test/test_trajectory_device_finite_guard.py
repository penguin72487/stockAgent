from __future__ import annotations

import json
from pathlib import Path

import pytest
import torch
from torch import nn

from stockagent.config import load_config
from stockagent.training.checkpoint_contract import _training_checkpoint_contract
from stockagent.training.finite_checks import (
    TRAJECTORY_DEVICE_FINITE_GUARD_ABI,
    TRAJECTORY_DEVICE_FINITE_GUARD_ENV,
    TrajectoryFiniteGuard,
    trajectory_device_finite_guard_enabled,
)
from test_crypto_trajectory_optimizer import (
    _CountingSGD, _Policy, _Scheduler, _crypto_loss, _run, _split,
)


def test_device_observations_do_not_read_host_scalars(monkeypatch):
    model = nn.Linear(2, 1)
    model.weight.grad = torch.ones_like(model.weight)
    guard = TrajectoryFiniteGuard(torch.device("cpu"))
    with monkeypatch.context() as patch:
        def forbidden(*args, **kwargs):
            raise AssertionError("per-batch observation read a host scalar")
        patch.setattr(torch.Tensor, "item", forbidden)
        guard.observe_loss(torch.tensor(0.5), 1)
        guard.observe_gradients(model, 1)
    guard.require_finite(torch.optim.SGD(model.parameters(), lr=0.1))


def test_device_guard_preserves_valid_trajectory_update(monkeypatch):
    monkeypatch.delenv(TRAJECTORY_DEVICE_FINITE_GUARD_ENV, raising=False)
    reference = _Policy()
    reference_optimizer = _CountingSGD(reference.parameters())
    reference_scheduler = _Scheduler()
    loss_a, timing_a = _run(_split(), reference, reference_optimizer, reference_scheduler)
    monkeypatch.setenv(TRAJECTORY_DEVICE_FINITE_GUARD_ENV, "1")
    candidate = _Policy()
    optimizer, scheduler = _CountingSGD(candidate.parameters()), _Scheduler()
    loss_b, timing_b = _run(_split(), candidate, optimizer, scheduler)
    torch.testing.assert_close(loss_a, loss_b, rtol=0, atol=0)
    torch.testing.assert_close(reference.scale, candidate.scale, rtol=0, atol=0)
    assert optimizer.calls == scheduler.calls == timing_b.optimizer_steps == 1
    assert timing_b.batches == timing_a.batches == 3


def test_device_guard_keeps_every_batch_loss_without_per_batch_host_progress(monkeypatch):
    import stockagent.training.trainer as trainer

    monkeypatch.setenv(TRAJECTORY_DEVICE_FINITE_GUARD_ENV, "1")
    model = _Policy()
    optimizer, scheduler = _CountingSGD(model.parameters()), _Scheduler()
    messages, observed = [], []
    monkeypatch.setattr(trainer, "_progress", messages.append)
    original_cpu = torch.Tensor.cpu
    progress_copies = []

    def counted_cpu(value, *args, **kwargs):
        if value.shape == (3,):
            progress_copies.append(value.detach().clone())
        return original_cpu(value, *args, **kwargs)

    monkeypatch.setattr(torch.Tensor, "cpu", counted_cpu)

    def observed_loss(*args, **kwargs):
        assert not any(" done loss=" in line for line in messages)
        result = _crypto_loss(*args, **kwargs)
        observed.append(float(result.detach()))
        return result

    _run(_split(), model, optimizer, scheduler, observed_loss, progress_label="guard-test")
    batch_messages = [line for line in messages if " done loss=" in line]
    assert len(batch_messages) == len(observed) == 3
    assert len(progress_copies) == 1
    torch.testing.assert_close(progress_copies[0], torch.tensor(observed), rtol=0, atol=0)
    for batch, (line, value) in enumerate(zip(batch_messages, observed), 1):
        assert f"batch {batch}/3" in line and f"loss={value:.8f}" in line
        assert "deferred_trajectory_progress=true" in line
    assert optimizer.calls == scheduler.calls == 1


@pytest.mark.parametrize("bad_value", ["loss", "gradient"])
def test_device_guard_rejects_invalid_intermediate_batch_before_update(monkeypatch, bad_value):
    monkeypatch.setenv(TRAJECTORY_DEVICE_FINITE_GUARD_ENV, "1")
    model = _Policy()
    optimizer, scheduler = _CountingSGD(model.parameters()), _Scheduler()
    calls = 0

    def broken_loss(*args, **kwargs):
        nonlocal calls
        calls += 1
        loss = _crypto_loss(*args, **kwargs)
        if calls == 2:
            if bad_value == "loss":
                return loss * float("nan")
            loss.register_hook(lambda gradient: gradient * float("nan"))
        return loss

    with pytest.raises(FloatingPointError, match="first local bad batch=2"):
        _run(_split(), model, optimizer, scheduler, broken_loss)
    assert calls == 3  # Observations stay on device until the update boundary.
    assert optimizer.calls == scheduler.calls == 0
    assert model.scale.grad is None
    assert model.scale.item() == pytest.approx(0.1)


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), -float("inf")])
def test_device_guard_retains_erased_invalid_gradient(bad):
    model = nn.Linear(2, 1)
    optimizer = torch.optim.SGD(model.parameters(), lr=0.1)
    guard = TrajectoryFiniteGuard(torch.device("cpu"))
    model.weight.grad = torch.full_like(model.weight, bad)
    guard.observe_gradients(model, 2)
    optimizer.zero_grad(set_to_none=True)
    guard.observe_loss(torch.tensor(0.5), 3)
    with pytest.raises(FloatingPointError, match="first local bad batch=2"):
        guard.require_finite(optimizer)


def _guard_worker(rank, rendezvous, destination):
    import torch.distributed as dist
    dist.init_process_group("gloo", rank=rank, world_size=2, init_method=rendezvous)
    try:
        model = nn.Linear(2, 1)
        optimizer = torch.optim.SGD(model.parameters(), lr=0.1)
        guard = TrajectoryFiniteGuard(torch.device("cpu"))
        guard.observe_loss(torch.tensor(float("nan") if rank else 0.5), 2)
        try:
            guard.require_finite(optimizer, distributed=True)
        except FloatingPointError:
            Path(destination, f"rank-{rank}.json").write_text(json.dumps({"rejected": True}))
        else:
            Path(destination, f"rank-{rank}.json").write_text(json.dumps({"rejected": False}))
    finally:
        dist.destroy_process_group()


def test_device_guard_rejects_on_both_ranks_when_only_one_rank_is_invalid(tmp_path):
    torch.multiprocessing.spawn(
        _guard_worker, args=("file://" + str(tmp_path / "rendezvous"), str(tmp_path)),
        nprocs=2, join=True,
    )
    assert all(json.loads((tmp_path / f"rank-{rank}.json").read_text())["rejected"]
               for rank in range(2))


def test_device_guard_is_opt_in_and_changes_checkpoint_contract(monkeypatch):
    config = load_config("configs/markets/tw_stock_futures_carry_0845_history2013_20261023_v2.yaml")
    monkeypatch.delenv(TRAJECTORY_DEVICE_FINITE_GUARD_ENV, raising=False)
    assert not trajectory_device_finite_guard_enabled()
    reference = _training_checkpoint_contract(config)
    monkeypatch.setenv(TRAJECTORY_DEVICE_FINITE_GUARD_ENV, "0")
    assert _training_checkpoint_contract(config) == reference
    monkeypatch.setenv(TRAJECTORY_DEVICE_FINITE_GUARD_ENV, "1")
    candidate = _training_checkpoint_contract(config)
    assert candidate["finite_checks"]["trajectory_device_guard"] == TRAJECTORY_DEVICE_FINITE_GUARD_ABI
    assert candidate != reference
    monkeypatch.setenv(TRAJECTORY_DEVICE_FINITE_GUARD_ENV, "unknown")
    with pytest.raises(ValueError, match="invalid boolean"):
        trajectory_device_finite_guard_enabled()
