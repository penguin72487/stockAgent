from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "benchmark_tw_day_trade_batch_sizes.py"
SPEC = importlib.util.spec_from_file_location("benchmark_tw_day_trade_batch_sizes", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
benchmark = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(benchmark)


def _epoch(epoch: int, *, wall: float, train: float, grad: float = 2.0) -> dict:
    return {
        "epoch": epoch,
        "epoch_wall_s": wall,
        "train_total_s": train,
        "train_loss": -0.1,
        "val_mean": -0.2,
        "test_mean": -0.3,
        "train_batches": 3,
        "train_zero_grad_batches": 0,
        "train_grad_norm_before_clip_mean": grad,
        "dynamo_unique_graphs_epoch_delta": 0,
        "bt_compile_failures": 0,
        "bt_prep_compile_failures": 0,
        "bt_runtime_fallback_calls": 0,
        "bt_eager_runner_calls": 0,
        "bt_prep_compile_nonhit": 0,
        "bt_compile_nonhit": 0,
    }


def _memory(*, peak: float = 0.5, headroom: float = 16.0) -> dict:
    return {
        "ok": True,
        "per_gpu": {},
        "max_peak_fraction": peak,
        "min_headroom_gib": headroom,
    }


def test_parse_batch_sizes_requires_power_of_two_globally_and_per_rank() -> None:
    assert benchmark._parse_batch_sizes("512,64,128,128", world_size=2) == [64, 128, 512]
    with pytest.raises(ValueError, match="power of two"):
        benchmark._parse_batch_sizes("96", world_size=2)
    with pytest.raises(ValueError, match="divisible"):
        benchmark._parse_batch_sizes("64", world_size=3)


def test_parse_batch_sizes_can_search_between_power_of_two_frontiers() -> None:
    assert benchmark._parse_batch_sizes(
        "192,160,160",
        world_size=2,
        require_power_of_two=False,
    ) == [160, 192]
    with pytest.raises(ValueError, match="positive"):
        benchmark._parse_batch_sizes(
            "0",
            world_size=2,
            require_power_of_two=False,
        )


def test_source_contract_fails_closed_on_another_executor() -> None:
    config = {
        "data": {},
        "trading": {"execution_mode": "tw_day_trade", "frequency": "daily"},
        "training": {"model_name": "financial_transformer", "loss_type": "log_utility"},
    }
    assert benchmark._validate_source_contract(config)["trading.execution_mode"] == "tw_day_trade"
    config["trading"]["execution_mode"] = "tw_minute"
    with pytest.raises(ValueError, match="not the requested daily day-trade contract"):
        benchmark._validate_source_contract(config)


def test_source_contract_accepts_only_strict_minute_executable_pairing() -> None:
    config = {
        "data": {
            "day_trade_minute_execution_root": "minute",
            "day_trade_minute_execution_allow_daily_proxy": False,
        },
        "trading": {
            "execution_mode": "tw_day_trade",
            "frequency": "daily",
            "tw_day_trade_unlimited_margin_conversion": False,
            "tw_short_capacity_limit_enabled": False,
        },
        "training": {
            "model_name": "executable_portfolio_transformer",
            "loss_type": "log_utility",
        },
    }

    contract = benchmark._validate_source_contract(config)
    assert contract["objective"] == "strict_minute"

    config["data"]["day_trade_minute_execution_allow_daily_proxy"] = True
    with pytest.raises(ValueError, match="allow_daily_proxy=false"):
        benchmark._validate_source_contract(config)


def test_score_curve_uses_real_rows_and_complete_epoch_median() -> None:
    rows = [
        _epoch(1, wall=99.0, train=90.0),
        _epoch(2, wall=10.0, train=8.0),
        _epoch(3, wall=4.0, train=3.0),
        _epoch(4, wall=5.0, train=4.0),
        _epoch(5, wall=6.0, train=5.0),
    ]
    score = benchmark._score_curve(
        rows,
        skip_epochs=2,
        minimum_steady_epochs=3,
        train_rows=289,
        global_batch_size=128,
        memory=_memory(),
        max_peak_fraction=0.9,
        min_headroom_gib=3.0,
    )
    assert score["ok"] is True
    assert score["train_batches"] == 3
    assert score["median_epoch_wall_s"] == pytest.approx(5.0)
    assert score["complete_epoch_real_rows_per_s"] == pytest.approx(289 / 5)
    assert score["padding_fraction"] == pytest.approx((384 - 289) / 384)


@pytest.mark.parametrize(
    ("mutate", "reason"),
    [
        (lambda rows: rows[0].update(train_zero_grad_batches=1), "zero-gradient"),
        (lambda rows: rows[0].update(bt_runtime_fallback_calls=1), "fallback"),
        (lambda rows: rows[0].update(dynamo_unique_graphs_epoch_delta=1), "Dynamo"),
        (lambda rows: rows[0].update(train_loss=float("nan")), "invalid train_loss"),
    ],
)
def test_score_curve_rejects_invalid_training(mutate, reason: str) -> None:
    rows = [_epoch(3, wall=4.0, train=3.0), _epoch(4, wall=4.0, train=3.0), _epoch(5, wall=4.0, train=3.0)]
    mutate(rows)
    score = benchmark._score_curve(
        rows,
        skip_epochs=2,
        minimum_steady_epochs=3,
        train_rows=289,
        global_batch_size=128,
        memory=_memory(),
        max_peak_fraction=0.9,
        min_headroom_gib=3.0,
    )
    assert score["ok"] is False
    assert any(reason in item for item in score["reasons"])


def test_score_curve_rejects_unsafe_vram_and_winner_uses_complete_epoch_rate() -> None:
    rows = [_epoch(3, wall=4.0, train=3.0), _epoch(4, wall=4.0, train=3.0), _epoch(5, wall=4.0, train=3.0)]
    score = benchmark._score_curve(
        rows,
        skip_epochs=2,
        minimum_steady_epochs=3,
        train_rows=289,
        global_batch_size=128,
        memory=_memory(peak=0.95, headroom=1.0),
        max_peak_fraction=0.9,
        min_headroom_gib=3.0,
    )
    assert score["ok"] is False
    winner = benchmark._select_winner(
        [
            {"ok": True, "batch_size": 128, "complete_epoch_real_rows_per_s": 100.0},
            {"ok": True, "batch_size": 256, "complete_epoch_real_rows_per_s": 120.0},
            {"ok": False, "batch_size": 512, "complete_epoch_real_rows_per_s": 999.0},
        ]
    )
    assert winner["batch_size"] == 256
