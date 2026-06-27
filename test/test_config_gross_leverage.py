from pathlib import Path

import yaml

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
                    "primary_baseline": "universe_average",
                    "metrics": ["cumulative_return"],
                },
            }
        ),
        encoding="utf-8",
    )
    return config_path


def test_load_config_preserves_gross_leverage_multiplier_above_one(tmp_path: Path) -> None:
    config_path = _write_minimal_config(tmp_path)

    config = load_config(config_path)

    assert config.trading.gross_leverage == 2.5


def test_load_config_defaults_best_val_artifact_switches_on(tmp_path: Path) -> None:
    config_path = _write_minimal_config(tmp_path)

    config = load_config(config_path)

    assert config.training.save_best_val_artifacts is True
    assert config.training.save_best_val_fold_artifacts is True
    assert config.training.save_best_val_fold_plots is True


def test_load_config_best_val_artifacts_master_switch_disables_fold_artifacts(tmp_path: Path) -> None:
    config_path = _write_minimal_config(tmp_path, training_overrides={"save_best_val_artifacts": False})

    config = load_config(config_path)

    assert config.training.save_best_val_artifacts is False
    assert config.training.save_best_val_fold_artifacts is False
    assert config.training.save_best_val_fold_plots is False
