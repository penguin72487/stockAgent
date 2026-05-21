from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass(slots=True)
class EnvironmentConfig:
    conda_env: str
    device: str
    use_tensor_cores: bool
    amp_dtype: str
    target_vram_fraction: float = 1


@dataclass(slots=True)
class DataConfig:
    parquet_root: str
    benchmark_name: str
    benchmark_required: bool
    benchmark_source: str
    universe_mode: str


@dataclass(slots=True)
class WalkForwardConfig:
    min_train_years: int
    val_years: int = 1
    require_future_test_year: bool = True


@dataclass(slots=True)
class TradingConfig:
    frequency: str
    fee_per_side: float
    long_only: bool
    cash_allowed: bool
    use_all_tradable_symbols: bool
    execution_mode: str = "overnight_tplus2"
    lot_size: int = 1000
    min_fee: float = 20.0
    intraday_buy_fee_rate: float = 0.001425
    intraday_sell_fee_rate: float = 0.002925
    overnight_buy_fee_rate: float = 0.001425
    overnight_sell_fee_rate: float = 0.004425
    settlement_delay_days: int = 2


@dataclass(slots=True)
class TrainingConfig:
    backend: str
    target: str
    batch_mode: str
    non_blocking_transfer: bool
    model_name: str = "mlp"
    num_layers: int = 1
    residual_norm: bool = True
    enable_torch_compile: bool = False
    chunk_rows: int = 0
    lookback: int = 1
    batch_size: int = 32
    batch_size_train: int = 32
    batch_size_eval: int = 32
    min_batch_size: int = 1
    auto_batch_size: bool = False
    vram_budget_gb: float = 8.0
    vram_safety_margin_gb: float = 1.0
    target_vram_fraction: float = 1
    epochs: int = 1000
    learning_rate: float = 1e-3
    hidden_dim: int = 1024
    dropout: float = 0.1
    top_k: int = 20
    num_workers: int = 0
    weight_decay: float = 1e-5
    loss_type: str = "mse"  # "mse" or "sharpe"
    xgb_n_estimators: int = 600
    xgb_max_depth: int = 6
    xgb_learning_rate: float = 0.05
    xgb_subsample: float = 0.8
    xgb_colsample_bytree: float = 0.8
    xgb_reg_lambda: float = 1.0
    ridge_alpha: float = 1.0
    ridge_fit_intercept: bool = True
    elasticnet_alpha: float = 1.0
    elasticnet_l1_ratio: float = 0.5
    elasticnet_fit_intercept: bool = True
    elasticnet_max_iter: int = 2000
    elasticnet_tol: float = 1e-4
    rl_total_timesteps: int = 20000
    rl_batch_size: int = 256
    rl_n_steps: int = 2048
    rl_buffer_size: int = 200000
    rl_learning_starts: int = 1000
    rl_gamma: float = 0.99
    rl_policy_hidden_dim: int = 256
    rl_max_symbols: int = 64
    rl_device: str = ""


@dataclass(slots=True)
class EvaluationConfig:
    primary_baseline: str
    metrics: list[str]
    gamma_sharpe: float = 1.0
    gamma_turnover: float = 0.1


@dataclass(slots=True)
class ExperimentConfig:
    experiment_name: str
    environment: EnvironmentConfig
    data: DataConfig
    walk_forward: WalkForwardConfig
    trading: TradingConfig
    training: TrainingConfig
    evaluation: EvaluationConfig


def _merge_defaults(raw: dict[str, Any]) -> dict[str, Any]:
    walk_forward = raw.setdefault("walk_forward", {})
    walk_forward.setdefault("min_train_years", 1)
    walk_forward.setdefault("val_years", 1)
    walk_forward.setdefault("require_future_test_year", True)

    trading = raw.setdefault("trading", {})
    trading.setdefault("execution_mode", "overnight_tplus2")
    trading.setdefault("lot_size", 1000)
    trading.setdefault("min_fee", 20.0)
    trading.setdefault("intraday_buy_fee_rate", 0.001425)
    trading.setdefault("intraday_sell_fee_rate", 0.002925)
    trading.setdefault("overnight_buy_fee_rate", 0.001425)
    trading.setdefault("overnight_sell_fee_rate", 0.004425)
    trading.setdefault("settlement_delay_days", 2)

    training = raw.setdefault("training", {})
    training.setdefault("model_name", "mlp")
    training.setdefault("num_layers", 1)
    training.setdefault("residual_norm", True)
    training.setdefault("lookback", 1)
    training.setdefault("batch_size", 32)
    training.setdefault("batch_size_train", training.get("batch_size", 32))
    training.setdefault("batch_size_eval", training.get("batch_size", 32))
    training.setdefault("min_batch_size", 1)
    training.setdefault("auto_batch_size", False)
    training.setdefault("enable_torch_compile", False)
    training.setdefault("chunk_rows", 0)
    training.setdefault("vram_budget_gb", 8.0)
    training.setdefault("vram_safety_margin_gb", 1.0)
    training.setdefault("target_vram_fraction", 0.85)
    training.setdefault("epochs", 10)
    training.setdefault("learning_rate", 1e-3)
    training.setdefault("hidden_dim", 128)
    training.setdefault("dropout", 0.1)
    training.setdefault("top_k", 20)
    training.setdefault("num_workers", 0)
    training.setdefault("weight_decay", 1e-5)
    training.setdefault("loss_type", "mse")
    training.setdefault("xgb_n_estimators", 600)
    training.setdefault("xgb_max_depth", 6)
    training.setdefault("xgb_learning_rate", 0.05)
    training.setdefault("xgb_subsample", 0.8)
    training.setdefault("xgb_colsample_bytree", 0.8)
    training.setdefault("xgb_reg_lambda", 1.0)
    training.setdefault("ridge_alpha", 1.0)
    training.setdefault("ridge_fit_intercept", True)
    training.setdefault("elasticnet_alpha", 1.0)
    training.setdefault("elasticnet_l1_ratio", 0.5)
    training.setdefault("elasticnet_fit_intercept", True)
    training.setdefault("elasticnet_max_iter", 2000)
    training.setdefault("elasticnet_tol", 1e-4)
    training.setdefault("rl_total_timesteps", 20000)
    training.setdefault("rl_batch_size", 256)
    training.setdefault("rl_n_steps", 2048)
    training.setdefault("rl_buffer_size", 200000)
    training.setdefault("rl_learning_starts", 1000)
    training.setdefault("rl_gamma", 0.99)
    training.setdefault("rl_policy_hidden_dim", 256)
    training.setdefault("rl_max_symbols", 64)
    training.setdefault("rl_device", "")

    evaluation = raw.setdefault("evaluation", {})
    evaluation.setdefault("gamma_sharpe", 1.0)
    evaluation.setdefault("gamma_turnover", 0.1)
    return raw


def _apply_model_configs(raw: dict[str, Any]) -> dict[str, Any]:
    training = raw.setdefault("training", {})
    model_name = str(training.get("model_name", "mlp")).strip().lower()
    model_configs = training.pop("model_configs", None)
    if not isinstance(model_configs, dict):
        return raw

    lowered_map = {str(key).strip().lower(): value for key, value in model_configs.items()}
    selected = lowered_map.get(model_name)
    if not isinstance(selected, dict):
        return raw

    valid_fields = set(TrainingConfig.__dataclass_fields__.keys())
    unknown = [key for key in selected.keys() if key not in valid_fields]
    if unknown:
        raise ValueError(
            f"Unknown keys in training.model_configs.{model_name}: {unknown}. "
            f"Valid keys: {sorted(valid_fields)}"
        )

    training.update(selected)

    # Preserve batch_size convenience behavior for profile overrides.
    if "batch_size" in selected and "batch_size_train" not in selected:
        training["batch_size_train"] = training["batch_size"]
    if "batch_size" in selected and "batch_size_eval" not in selected:
        training["batch_size_eval"] = training["batch_size"]

    return raw


def load_config(path: str | Path) -> ExperimentConfig:
    with Path(path).open("r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle)

    raw = _merge_defaults(raw)
    raw = _apply_model_configs(raw)
    return ExperimentConfig(
        experiment_name=raw["experiment_name"],
        environment=EnvironmentConfig(**raw["environment"]),
        data=DataConfig(**raw["data"]),
        walk_forward=WalkForwardConfig(**raw["walk_forward"]),
        trading=TradingConfig(**raw["trading"]),
        training=TrainingConfig(**raw["training"]),
        evaluation=EvaluationConfig(**raw["evaluation"]),
    )
