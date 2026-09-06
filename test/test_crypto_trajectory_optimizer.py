from __future__ import annotations

from dataclasses import asdict
from functools import partial
from pathlib import Path

import pytest
import torch
import yaml
from torch import nn
from torch.amp import GradScaler

from stockagent.config import load_config
from stockagent.backtest.crypto_perpetual import CRYPTO_PERPETUAL_BACKTEST_CONTRACT_VERSION
import stockagent.training.trainer as trainer
from stockagent.training.loss import risk_aware_loss
from stockagent.training.windowed import WindowedSplitTensors


class _Policy(nn.Module):
    def __init__(self) -> None:
        super().__init__()
        self.scale = nn.Parameter(torch.tensor(0.1))
        self.seen: list[float] = []

    def forward(self, x, mask, **kwargs):
        self.seen.append(self.scale.detach().item())
        return self.scale * x[:, -1, :, 0]


class _CountingSGD(torch.optim.SGD):
    def __init__(self, params) -> None:
        super().__init__(params, lr=0.1)
        self.calls = 0

    def step(self, closure=None):
        self.calls += 1
        return super().step(closure)


class _Scheduler:
    calls = 0

    def step(self) -> None:
        self.calls += 1


def _split() -> WindowedSplitTensors:
    price_returns = torch.tensor(
        [[0.01, -0.02], [-0.03, 0.01], [0.02, 0.04], [0.01, -0.03], [-0.02, 0.01]]
    )
    mask = torch.ones_like(price_returns, dtype=torch.bool)
    can_buy = mask.clone()
    can_buy[2, 0] = False
    return WindowedSplitTensors(
        features=torch.tensor(
            [[1.0, -0.4], [-0.3, 0.8], [0.7, -0.8], [0.2, -0.5], [-0.8, 0.6]]
        ).unsqueeze(-1),
        valid_indices=torch.arange(5),
        future_log_returns=torch.log1p(price_returns - torch.tensor([0.0003, -0.0002])),
        overnight_log_returns=torch.log1p(price_returns),
        tradable_mask=mask,
        can_buy_mask=can_buy,
        can_sell_mask=mask.clone(),
        can_short_open_mask=mask.clone(),
        benchmark=torch.zeros(5),
        volume_notional=torch.full_like(price_returns, 6_000_000.0),
        lookback=1,
        execution_mode="crypto_perpetual",
    )


_LOSS_OPTIONS = dict(
    long_only=False, buy_fee_rate=0.00055, sell_fee_rate=0.00055,
    max_turnover_ratio=0.0, gross_leverage=1.0, gamma_sharpe=1.0,
    gamma_excess=0.0, gamma_cvar=0.0, cvar_alpha=0.05,
    gamma_drawdown=0.0, drawdown_target=0.0, gamma_turnover=0.0,
    gamma_underperformance=0.0, excess_target=0.0, cvar_budget=0.0,
    drawdown_budget=0.0, turnover_budget=0.0, gamma_cvar_budget=0.0,
    gamma_drawdown_budget=0.0, gamma_turnover_budget=0.0,
    objective="log_utility", rank_ic_weight=0.0, return_rank_ic_weight=0.0,
    direction_weight=0.0, volatility_regime_weight=0.0, concentration_weight=0.0,
)
_crypto_loss = partial(
    risk_aware_loss, execution_mode="crypto_perpetual",
    portfolio_activation="pre_normalized", crypto_stateful_proximal_allocator=True,
    crypto_proximal_cost_multiplier=1.0, log_utility_periods_per_year=365.0,
)


def _reference_loss(split, scale, start, end, aux):
    return _crypto_loss(
        scale * split.features[start:end, :, 0],
        split.future_log_returns[start:end], split.tradable_mask[start:end],
        overnight_log_returns=split.overnight_log_returns[start:end],
        benchmark_returns=split.benchmark[start:end],
        can_buy_mask=split.can_buy_mask[start:end],
        can_sell_mask=split.can_sell_mask[start:end],
        can_short_open_mask=split.can_short_open_mask[start:end],
        volume_limit_weights=split.volume_notional[start:end] * 0.01 / 1_000_000.0,
        aux_outputs=aux, **_LOSS_OPTIONS,
    )


def _run(split, model, optimizer, scheduler, loss_fn=_crypto_loss, *, ddp=False, **overrides):
    options = dict(
        **_LOSS_OPTIONS, batch_size=2, device=torch.device("cpu"), amp_dtype=None,
        non_blocking=False, grad_clip_norm=100.0,
        max_volume_participation=0.01, volume_participation_equity=1_000_000.0,
        lr_scheduler=scheduler, lr_scheduler_interval="step",
        optimizer_step_per_trajectory=True, finite_check_interval_steps=100,
    )
    options.update(overrides)
    args = (loss_fn, split, optimizer, GradScaler("cpu", enabled=False))
    if ddp:
        return trainer._train_epoch_windowed_tensor_ddp(
            model, *args, replicated_ledger_local_metadata=True, **options
        )
    return trainer._train_epoch_windowed_tensor(model, None, *args, **options)


@pytest.mark.parametrize("pad", [False, True])
def test_crypto_trajectory_preserves_account_and_valid_date_weighted_truncated_gradient(pad):
    split = _split()
    oracle_aux = {}
    expected_loss = _reference_loss(split, torch.tensor(0.1), 0, 5, oracle_aux)
    # Gradient oracle deliberately detaches carry at the same boundaries. This
    # is not a claim of full-history BPTT equivalence.
    reference_scale = torch.tensor(0.1, requires_grad=True)
    previous = {}
    for start in range(0, 5, 2):
        end = min(start + 2, 5)
        aux = dict(previous)
        loss = _reference_loss(split, reference_scale, start, end, aux)
        (loss * ((end - start) / 5)).backward()
        previous = {
            "initial_weights": aux["_final_weights"].detach().clone(),
            "initial_alive": aux["_final_alive"].detach().clone(),
        }
    expected_scale = reference_scale.detach() - 0.1 * reference_scale.grad
    observed_states = []

    def record_loss(*args, **kwargs):
        value = _crypto_loss(*args, **kwargs)
        aux = kwargs["aux_outputs"]
        observed_states.append(aux["_final_weights"].detach().clone())
        return value

    if pad:
        split = trainer._pad_windowed_training_split(split, batch_size=2)
        assert split.sample_mask.tolist() == [True] * 5 + [False]
    model = _Policy()
    optimizer, scheduler = _CountingSGD(model.parameters()), _Scheduler()
    loss, timing = _run(split, model, optimizer, scheduler, record_loss)
    assert loss.item() == pytest.approx(expected_loss.item(), abs=2e-6)
    torch.testing.assert_close(observed_states[-1], oracle_aux["_final_weights"])
    torch.testing.assert_close(model.scale.detach(), expected_scale)
    assert model.seen == pytest.approx([0.1] * 3)
    assert optimizer.calls == scheduler.calls == timing.optimizer_steps == 1
    assert timing.batches == 3
    assert timing.gradient_norm_observations == 1


def test_crypto_trajectory_ddp_routing_keeps_one_policy_and_tail_mask(monkeypatch):
    # Exercise the actual DDP executor with synthetic collectives, not GPUs or
    # train.py. Communication/reduction correctness is outside this unit test.
    original = _split()
    split = trainer._pad_windowed_training_split(original, batch_size=2)
    model = _Policy()
    monkeypatch.setattr(trainer, "_distributed_world_size", lambda: 2)
    monkeypatch.setattr(trainer, "_distributed_rank", lambda: 0)
    monkeypatch.setattr(trainer, "_distributed_is_initialized", lambda: True)
    monkeypatch.setattr(trainer, "_distributed_is_rank0", lambda: False)
    gather_calls = 0

    def gather(local):
        nonlocal gather_calls
        remote_row = split.valid_indices[2 * gather_calls + 1]
        remote = model.scale * split.features[remote_row:remote_row + 1, :, 0]
        gather_calls += 1
        return torch.cat((local, remote), dim=0)

    monkeypatch.setattr(trainer, "_all_gather_autograd", gather)
    optimizer, scheduler = _CountingSGD(model.parameters()), _Scheduler()
    expected = _reference_loss(original, model.scale.detach(), 0, 5, {})
    loss, timing = _run(split, model, optimizer, scheduler, ddp=True)
    assert loss.item() == pytest.approx(expected.item(), abs=2e-6)
    assert model.seen == pytest.approx([0.1] * 3)
    assert optimizer.calls == scheduler.calls == timing.optimizer_steps == 1
    assert gather_calls == timing.batches == 3


@pytest.mark.parametrize("bad_value", ["loss", "gradient"])
def test_crypto_trajectory_aborts_on_nonfinite_intermediate_batch(bad_value):
    model = _Policy()
    optimizer, scheduler = _CountingSGD(model.parameters()), _Scheduler()
    calls = 0

    def broken_loss(*args, **kwargs):
        nonlocal calls
        calls += 1
        value = _crypto_loss(*args, **kwargs)
        if calls == 2:
            if bad_value == "loss":
                return value * float("nan")
            value.register_hook(lambda grad: grad * float("nan"))
        return value

    with pytest.raises(FloatingPointError, match="full-trajectory"):
        _run(_split(), model, optimizer, scheduler, broken_loss)
    assert calls == 2  # Even though the normal finite-check interval is 100.
    assert optimizer.calls == scheduler.calls == 0
    assert model.scale.item() == pytest.approx(0.1)
    assert model.scale.grad is None


def test_crypto_trajectory_rejects_nonadditive_objective():
    model = _Policy()
    optimizer, scheduler = _CountingSGD(model.parameters()), _Scheduler()
    with pytest.raises(RuntimeError, match="decomposable log_utility"):
        _run(_split(), model, optimizer, scheduler, objective="sharpe")
    assert optimizer.calls == 0


def test_crypto_trajectory_config_changes_only_update_cadence_and_output():
    control = load_config("configs/markets/bybit_perpetual_daily_0000_deterministic.yaml")
    candidate = load_config("configs/markets/bybit_perpetual_daily_0000_trajectory.yaml")
    assert candidate.training.epochs == 1000
    assert candidate.training.crypto_optimizer_step_per_trajectory is True
    assert candidate.runner.output_dir == "artifacts/markets/bybit_perpetual_daily_0000_trajectory_v1"
    expected = asdict(control)
    expected["training"]["crypto_optimizer_step_per_trajectory"] = True
    expected["experiment_name"] = candidate.experiment_name
    expected["runner"]["output_dir"] = candidate.runner.output_dir
    assert asdict(candidate) == expected
    details = trainer._mode_artifact_contract_for_config(candidate)["mode_details"]
    assert details["crypto_backtest_contract_version"] == CRYPTO_PERPETUAL_BACKTEST_CONTRACT_VERSION
    assert details["crypto_execution_minute_utc"] == 0


@pytest.mark.parametrize("mode,objective,error", [
    ("naive", "log_utility", "requires trading.execution_mode"),
    ("crypto_perpetual", "sharpe", "requires canonical path-dependent log utility"),
])
def test_crypto_trajectory_config_rejects_wrong_execution_or_objective(tmp_path, mode, objective, error):
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump({
        "base_config": str(Path("configs/markets/bybit_perpetual_daily_0000_trajectory.yaml").resolve()),
        "trading": {"execution_mode": mode},
        "training": {"loss_type": objective},
    }), encoding="utf-8")
    with pytest.raises(ValueError, match=error):
        load_config(path)
