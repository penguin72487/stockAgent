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


@dataclass(slots=True)
class DataConfig:
    parquet_root: str
    benchmark_name: str
    benchmark_required: bool
    benchmark_source: str
    universe_mode: str


@dataclass(slots=True)
class WalkForwardConfig:
    mode: str
    min_train_years: int
    val_years: int
    test_years: str
    require_future_test_year: bool


@dataclass(slots=True)
class TradingConfig:
    frequency: str
    fee_per_side: float
    long_only: bool
    cash_allowed: bool
    use_all_tradable_symbols: bool


@dataclass(slots=True)
class TrainingConfig:
    backend: str
    target: str
    batch_mode: str
    non_blocking_transfer: bool
    lookback: int = 1
    batch_size: int = 32
    epochs: int = 1000
    learning_rate: float = 1e-3
    hidden_dim: int = 1024
    dropout: float = 0.1
    top_k: int = 20
    num_workers: int = 0
    weight_decay: float = 1e-5
    loss_type: str = "mse"  # "mse" or "sharpe"


@dataclass(slots=True)
class EvaluationConfig:
    primary_baseline: str
    metrics: list[str]


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
    training = raw.setdefault("training", {})
    training.setdefault("lookback", 1)
    training.setdefault("batch_size", 32)
    training.setdefault("epochs", 10)
    training.setdefault("learning_rate", 1e-3)
    training.setdefault("hidden_dim", 128)
    training.setdefault("dropout", 0.1)
    training.setdefault("top_k", 20)
    training.setdefault("num_workers", 0)
    training.setdefault("weight_decay", 1e-5)
    training.setdefault("loss_type", "mse")
    return raw


def load_config(path: str | Path) -> ExperimentConfig:
    with Path(path).open("r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle)

    raw = _merge_defaults(raw)
    return ExperimentConfig(
        experiment_name=raw["experiment_name"],
        environment=EnvironmentConfig(**raw["environment"]),
        data=DataConfig(**raw["data"]),
        walk_forward=WalkForwardConfig(**raw["walk_forward"]),
        trading=TradingConfig(**raw["trading"]),
        training=TrainingConfig(**raw["training"]),
        evaluation=EvaluationConfig(**raw["evaluation"]),
    )
