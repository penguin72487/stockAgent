"""Synthetic end-to-end plumbing for the explicit announced-exit assumption."""
from __future__ import annotations

import ast
from dataclasses import replace
import inspect
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest
import torch

from stockagent.backtest.crypto_perpetual import CryptoPerpetualDataError
from stockagent.backtest.simulator import run_backtest, run_backtest_torch
from stockagent.config import load_config
from stockagent.training.loss import risk_aware_loss
from stockagent.training import trainer


ROOT = Path(__file__).resolve().parents[1]
CONTROL = ROOT / "configs/markets/bybit_perpetual_daily_0000_historical_public_pit_score_cash_trajectory_v7.yaml"
FLAG = "crypto_announced_exit_unlimited_volume"


def _config(tmp_path, trading, training=""):
    path = tmp_path / "config.yaml"
    path.write_text(f"base_config: {CONTROL}\ntrading:\n{trading}\n{training}")
    return load_config(path)


def test_config_bool_is_opt_in_and_rejects_non_crypto_or_string(tmp_path):
    assert getattr(load_config(CONTROL).trading, FLAG) is False
    assert _config(tmp_path, f"  {FLAG}: true").trading.crypto_announced_exit_unlimited_volume
    with pytest.raises(ValueError, match="crypto_announced_exit_unlimited_volume.*requires"):
        _config(tmp_path, f"  execution_mode: naive\n  {FLAG}: true",
                "training:\n  crypto_optimizer_step_per_trajectory: false\n")
    with pytest.raises((TypeError, ValueError), match="crypto_announced_exit_unlimited_volume"):
        _config(tmp_path, f"  {FLAG}: 'false'")


@pytest.mark.parametrize("enabled", [False, True])
def test_config_flag_reaches_runtime(tmp_path, enabled):
    config = _config(tmp_path, f"  {FLAG}: {str(enabled).lower()}")
    panel = SimpleNamespace(dates=np.array(["2024-01-01"], dtype="datetime64[D]"), symbols=["COINUSDT"])
    runtime = trainer._build_execution_runtime(panel, config, torch.device("cpu"))
    assert runtime.crypto_announced_exit_unlimited_volume is enabled


def _one_row(*, numpy=False):
    target = torch.zeros((1, 1))
    yes = torch.ones_like(target, dtype=torch.bool)
    args = (target, torch.zeros_like(target), ~yes, torch.zeros(1))
    kwargs = dict(
        can_buy_mask=yes, can_sell_mask=yes, can_short_open_mask=yes,
        force_exit_mask=yes, volume_limit_weights=torch.full_like(target, 0.1),
        initial_weights=torch.tensor([0.5]), initial_equity_scale=torch.tensor(1.0),
        overnight_returns=torch.zeros_like(target), execution_mode="crypto_perpetual",
        portfolio_activation="pre_normalized", long_only=False,
        buy_fee_rate=0.00055, sell_fee_rate=0.00055,
    )
    if numpy:
        args = tuple(value.numpy() for value in args)
        kwargs = {key: value.numpy() if isinstance(value, torch.Tensor) else value
                  for key, value in kwargs.items()}
    return args, kwargs


@pytest.mark.parametrize("numpy", [False, True])
def test_canonical_public_wrappers_forward_flag_and_preserve_false(numpy):
    args, kwargs = _one_row(numpy=numpy)
    run = run_backtest if numpy else run_backtest_torch
    default = run(*args, **kwargs)
    disabled = run(*args, **kwargs, **{FLAG: False})
    enabled = run(*args, **kwargs, **{FLAG: True})
    np.testing.assert_array_equal(default.weights_history, disabled.weights_history)
    np.testing.assert_array_equal(default.strategy_returns, disabled.strategy_returns)
    assert default.weights_history.item() == pytest.approx(0.4)
    assert enabled.weights_history.item() == 0.0
    assert enabled.turnovers.item() == pytest.approx(0.5)
    assert np.expm1(enabled.strategy_returns.item()) == pytest.approx(-0.5 * 0.00055)


@pytest.mark.parametrize("api", [run_backtest, run_backtest_torch, risk_aware_loss])
def test_non_crypto_public_entrypoints_reject_opt_in(api):
    args, _ = _one_row(numpy=api is run_backtest)
    with pytest.raises(ValueError, match="requires crypto_perpetual"):
        api(*args, buy_fee_rate=0.0, sell_fee_rate=0.0, execution_mode="naive", **{FLAG: True})


def test_loss_passes_assumption_and_updates_recurrent_state_without_future_valuation():
    weights = torch.zeros((1, 1), requires_grad=True)
    yes = torch.ones_like(weights, dtype=torch.bool)
    kwargs = dict(
        benchmark_returns=torch.zeros(1), can_buy_mask=yes, can_sell_mask=yes,
        can_short_open_mask=yes, force_exit_mask=yes,
        volume_limit_weights=torch.full_like(weights, 0.1),
        long_only=False, buy_fee_rate=0.00055, sell_fee_rate=0.00055,
        execution_mode="crypto_perpetual", portfolio_activation="pre_normalized",
        overnight_log_returns=torch.zeros_like(weights), objective="log_utility",
        gamma_turnover=0.0, concentration_weight=0.0,
    )
    future = torch.full_like(weights, float("nan"))
    with pytest.raises(CryptoPerpetualDataError):
        risk_aware_loss(weights, future, ~yes, **kwargs,
                        aux_outputs={"initial_weights": torch.tensor([0.5])})
    aux = {"initial_weights": torch.tensor([0.5])}
    loss = risk_aware_loss(weights, future, ~yes, **kwargs, aux_outputs=aux, **{FLAG: True})
    assert torch.isfinite(loss)
    assert aux["_final_weights"].item() == 0.0
    loss.backward()
    assert weights.grad is not None and torch.isfinite(weights.grad).all()


@pytest.mark.parametrize("chunk_rows", [1, 2, 8])
@pytest.mark.parametrize("history", [False, True])
@pytest.mark.parametrize("participation", [0.01, 0.5])
def test_epoch_eval_and_artifact_runner_preserve_flag_across_chunks(chunk_rows, history, participation):
    target = torch.tensor([[0.5], [0.0], [0.0]])
    yes = torch.ones_like(target, dtype=torch.bool)
    policy = torch.tensor([[True], [False], [False]])
    force = torch.tensor([[False], [True], [False]])
    effective = torch.tensor([[0.0], [float("nan")], [float("nan")]])
    # Ordinary entry must actually hit the configured cap; the announcement
    # exit alone can exceed it. Cover both the legacy and user-corrected ratio.
    volume = torch.tensor([[0.2], [0.1], [0.0]])
    runtime = trainer._ExecutionRuntime(
        mode="crypto_perpetual", buy_fee_rates=None, sell_fee_rates=None,
        lot_sizes=None, settlement_lag_sessions=0,
        crypto_announced_exit_unlimited_volume=True,
    )
    args = (target, effective, policy, yes, yes, yes, ~yes, force, torch.zeros(3))
    kwargs = dict(
        device=torch.device("cpu"), non_blocking=False, long_only=False,
        buy_fee_rate=0.00055, sell_fee_rate=0.00055, max_turnover_ratio=0.0,
        gross_leverage=1.0, min_trade_weight=0.0, backtest_chunk_rows=chunk_rows,
        compute_metrics_summary=True, return_weights_history=history,
        profile_timing=False, progress_label=None, timing=trainer.TimingBreakdown(),
        reset_at_rows=None, portfolio_activation="pre_normalized",
        overnight_log_returns_all=torch.zeros_like(target),
        volume_notional_all=volume * (1e6 / participation),
        max_volume_participation=participation, volume_participation_equity=1e6,
        execution_runtime=runtime,
    )
    actual, _ = trainer._run_eval_backtest_from_weight_buffers(*args, **kwargs)
    direct = run_backtest_torch(
        target, effective, policy, torch.zeros(3), buy_fee_rate=0.00055, sell_fee_rate=0.00055,
        can_buy_mask=yes, can_sell_mask=yes, can_short_open_mask=yes, force_exit_mask=force,
        volume_limit_weights=volume, overnight_returns=torch.zeros_like(target),
        long_only=False, execution_mode="crypto_perpetual", portfolio_activation="pre_normalized",
        **{FLAG: True},
    )
    torch.testing.assert_close(actual.strategy_returns, direct.strategy_returns)
    torch.testing.assert_close(actual.final_equity_scale, direct.final_equity_scale)
    assert actual.final_weights.item() == 0.0
    if history:
        assert actual.weights_history[0, 0].item() == pytest.approx(0.2)
        torch.testing.assert_close(actual.weights_history, direct.weights_history)
    kwargs["execution_runtime"] = replace(runtime, crypto_announced_exit_unlimited_volume=False)
    with pytest.raises(CryptoPerpetualDataError):
        trainer._run_eval_backtest_from_weight_buffers(*args, **kwargs)


def test_full_training_and_replay_callsites_have_explicit_wiring_without_running_training():
    # Lock the real callsites, including the full-training kwargs dictionary;
    # dynamic tests above validate the common canonical loss/eval executors.
    tree = ast.parse(inspect.getsource(trainer))
    callsites = []
    dictionaries = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            names = {kw.arg for kw in node.keywords}
            if "crypto_stateful_proximal_allocator" in names:
                assert FLAG in names, node.lineno
                callsites.append(node.lineno)
        elif isinstance(node, ast.Dict):
            keys = {key.value for key in node.keys if isinstance(key, ast.Constant)}
            if "crypto_stateful_proximal_allocator" in keys:
                assert FLAG in keys, node.lineno
                dictionaries.append(node.lineno)
    assert len(callsites) >= 4  # Runtime construction, fallback loss, eval, stitched replay.
    assert dictionaries  # Full training's risk-aware-loss kwargs.
    auxiliary_source = ast.parse(inspect.getsource(trainer._evaluate_windowed_aux_objective_loss))
    calls = [node for node in ast.walk(auxiliary_source)
             if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
             and node.func.id == "risk_aware_loss"]
    assert len(calls) == 1 and FLAG in {kw.arg for kw in calls[0].keywords}
