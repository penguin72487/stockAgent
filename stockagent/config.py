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

    evaluation = raw.setdefault("evaluation", {})
    evaluation.setdefault("gamma_sharpe", 1.0)
    evaluation.setdefault("gamma_turnover", 0.1)
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
