import math
from pathlib import Path

import numpy as np
import pytest
import torch
import yaml

from stockagent.backtest.simulator import _resolve_exposure_budget, run_backtest, run_backtest_torch
from stockagent.config import load_config


def _write_minimal_config(tmp_path: Path, *, training_overrides: dict | None = None) -> Path:
    config_path = tmp_path / "config.yaml"
    training = {
        "backend": "pytorch",
        "target": "next_1d_rank",
        "batch_mode": "time_window_x_all_symbols",
        "non_blocking_transfer": True,
        "model_name": "transformer_base_portfolio",
    }
    training.update(training_overrides or {})
    config_path.write_text(
        yaml.safe_dump(
            {
                "experiment_name": "gross-leverage-test",
                "environment": {
                    "conda_env": "fintech",
                    "device": "cuda",
                    "use_tensor_cores": True,
                    "amp_dtype": "bf16",
                },
                "data": {
                    "parquet_root": "data_yahoo/tw_stocks",
                    "benchmark_name": "2330",
                    "benchmark_required": False,
                    "benchmark_source": "derived_from_panel",
                    "universe_mode": "all_daily_symbols",
                },
                "walk_forward": {
                    "min_train_years": 1,
                    "val_years": 1,
                    "require_future_test_year": False,
                },
                "trading": {
                    "frequency": "daily",
                    "buy_fee_rate": 0.000855,
                    "sell_fee_rate": 0.003855,
                    "long_only": False,
                    "cash_allowed": True,
                    "gross_leverage": 2.5,
                },
                "training": training,
                "evaluation": {
                    # Legacy key should be ignored; market benchmark lives in data.benchmark_name.
                    "primary_baseline": "universe_average",
                    "metrics": ["cumulative_return"],
                },
            }
        ),
        encoding="utf-8",
    )
    return config_path


def test_load_config_migrates_legacy_gross_leverage_to_reporting_leverage(tmp_path: Path) -> None:
    config_path = _write_minimal_config(tmp_path)

    config = load_config(config_path)

    assert config.trading.leverage == 2.5
    assert not hasattr(config.trading, "gross_leverage")
    assert not hasattr(config.evaluation, "primary_baseline")
    assert not hasattr(config.evaluation, "metrics")


def test_load_config_preserves_lr_scheduler_warmup_fields(tmp_path: Path) -> None:
    config_path = _write_minimal_config(
        tmp_path,
        training_overrides={
            "enable_lr_scheduler": True,
            "lr_scheduler": "warmup_cosine",
            "lr_scheduler_warmup_steps": 123,
            "lr_scheduler_interval": "step",
        },
    )

    config = load_config(config_path)

    assert config.training.lr_scheduler == "warmup_cosine"
    assert config.training.lr_scheduler_warmup_steps == 123
    assert config.training.lr_scheduler_interval == "step"


def test_backtest_exposure_budget_caps_multiplier_at_one() -> None:
    assert _resolve_exposure_budget(2.5) == 1.0

    weights = torch.tensor([[1.0, -1.0]], dtype=torch.float32)
    returns = torch.zeros_like(weights)
    tradable = torch.ones_like(weights, dtype=torch.bool)
    benchmark = torch.zeros((1,), dtype=torch.float32)

    result = run_backtest_torch(
        weights,
        returns,
        tradable,
        benchmark,
        buy_fee_rate=0.0,
        sell_fee_rate=0.0,
        long_only=False,
        gross_leverage=2.5,
    )

    expected_weights = torch.tensor([[0.5, -0.5]], dtype=torch.float32)
    assert torch.allclose(result.weights_history.cpu(), expected_weights, atol=1e-7, rtol=1e-6)
    assert torch.allclose(result.weights_history.abs().sum(dim=1).cpu(), torch.tensor([1.0]), atol=1e-7, rtol=1e-6)


def test_backtest_converts_asset_log_returns_to_portfolio_log_return() -> None:
    asset_log_return = math.log(0.4)
    expected_strategy_log_return = math.log1p(0.6)

    weights_np = np.array([[-1.0]], dtype=np.float32)
    returns_np = np.array([[asset_log_return]], dtype=np.float32)
    tradable_np = np.ones_like(weights_np, dtype=bool)
    benchmark_np = np.zeros((1,), dtype=np.float32)

    numpy_result = run_backtest(
        weights_np,
        returns_np,
        tradable_np,
        benchmark_np,
        buy_fee_rate=0.0,
        sell_fee_rate=0.0,
        long_only=False,
        gross_leverage=1.0,
    )

    weights_t = torch.from_numpy(weights_np)
    returns_t = torch.from_numpy(returns_np)
    tradable_t = torch.from_numpy(tradable_np)
    benchmark_t = torch.from_numpy(benchmark_np)
    torch_result = run_backtest_torch(
        weights_t,
        returns_t,
        tradable_t,
        benchmark_t,
        buy_fee_rate=0.0,
        sell_fee_rate=0.0,
        long_only=False,
        gross_leverage=1.0,
    )
    dense_result = run_backtest_torch(
        weights_t,
        returns_t,
        tradable_t,
        benchmark_t,
        buy_fee_rate=0.0,
        sell_fee_rate=0.0,
        long_only=False,
        gross_leverage=1.0,
        can_buy_mask=tradable_t,
        can_sell_mask=tradable_t,
        dense_mask_constraints=True,
    )

    assert math.isclose(float(numpy_result.strategy_returns[0]), expected_strategy_log_return, rel_tol=1e-6)
    assert math.isclose(float(torch_result.strategy_returns[0]), expected_strategy_log_return, rel_tol=1e-6)
    assert math.isclose(float(dense_result.strategy_returns[0]), expected_strategy_log_return, rel_tol=1e-6)


def test_load_config_defaults_best_val_artifact_switches_off(tmp_path: Path) -> None:
    config_path = _write_minimal_config(tmp_path)

    config = load_config(config_path)

    assert config.training.save_best_val_artifacts is False
    assert config.training.save_best_val_fold_artifacts is False
    assert config.training.save_best_val_fold_plots is False


def test_load_config_best_val_artifacts_master_switch_enables_fold_artifacts(tmp_path: Path) -> None:
    config_path = _write_minimal_config(tmp_path, training_overrides={"save_best_val_artifacts": True})

    config = load_config(config_path)

    assert config.training.save_best_val_artifacts is True
    assert config.training.save_best_val_fold_artifacts is True
    assert config.training.save_best_val_fold_plots is True


@pytest.mark.parametrize(
    ("raw_mode", "expected_mode"),
    [
        ("activation-l1", "activation_l1"),
        ("raw_l1", "l1"),
        ("raw-scores", "logits"),
        ("signed-action-softmax", "signed_softmax"),
        ("signed-action-sparsemax", "signed_sparsemax"),
        ("signed-action-entmax", "signed_entmax15"),
        ("differentiable-projection", "projection_l1"),
    ],
)
def test_load_config_normalizes_portfolio_output_mode_aliases(
    tmp_path: Path,
    raw_mode: str,
    expected_mode: str,
) -> None:
    config_path = _write_minimal_config(
        tmp_path,
        training_overrides={
            "transformer_base_portfolio": {
                "portfolio_output_mode": raw_mode,
            }
        },
    )

    config = load_config(config_path)

    assert config.training.transformer_base_portfolio.portfolio_output_mode == expected_mode


def test_load_config_rejects_unknown_portfolio_output_mode(tmp_path: Path) -> None:
    config_path = _write_minimal_config(
        tmp_path,
        training_overrides={
            "transformer_base_portfolio": {
                "portfolio_output_mode": "mystery_mode",
            }
        },
    )

    with pytest.raises(ValueError, match="portfolio_output_mode"):
        load_config(config_path)
