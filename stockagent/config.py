from __future__ import annotations

from dataclasses import dataclass, field
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
    use_rapids: bool = False
    usd_only_trading_pairs: bool = False


@dataclass(slots=True)
class WalkForwardConfig:
    min_train_years: int
    val_years: int = 1
    require_future_test_year: bool = True


@dataclass(slots=True)
class TradingConfig:
    frequency: str
    buy_fee_rate: float
    sell_fee_rate: float
    long_only: bool
    cash_allowed: bool
    use_all_tradable_symbols: bool
    max_turnover_ratio: float = 0.0


@dataclass(slots=True)
class MLPModelConfig:
    hidden_dim: int = 1024
    hidden_layers: int = 2
    embedding_dim: int = 64
    dropout: float = 0.1


@dataclass(slots=True)
class FTTransformerModelConfig:
    d_token: int = 64
    n_layers: int = 2
    n_heads: int = 4
    ffn_dim: int = 256
    dropout: float = 0.1
    use_cls_token: bool = True


@dataclass(slots=True)
class TrainingConfig:
    backend: str
    target: str
    batch_mode: str
    non_blocking_transfer: bool
    model_name: str
    enable_torch_compile: bool = False
    warm_start_from_previous_fold: bool = False
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
    early_stopping_no_improve_ratio: float = 0.2
    learning_rate: float = 1e-3
    top_k: int = 20
    num_workers: int = 0
    weight_decay: float = 1e-5
    loss_type: str = "mse"  # "mse" or "sharpe"
    mlp: MLPModelConfig = field(default_factory=MLPModelConfig)
    ft_transformer: FTTransformerModelConfig = field(default_factory=FTTransformerModelConfig)


@dataclass(slots=True)
class EvaluationConfig:
    primary_baseline: str
    metrics: list[str]
    gamma_sharpe: float = 1.0
    gamma_turnover: float = 0.0


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

    training = raw.setdefault("training", {})
    training.setdefault("lookback", 1)
    training.setdefault("batch_size", 32)
    training.setdefault("batch_size_train", training.get("batch_size", 32))
    training.setdefault("batch_size_eval", training.get("batch_size", 32))
    training.setdefault("min_batch_size", 1)
    training.setdefault("auto_batch_size", False)
    training.setdefault("enable_torch_compile", False)
    training.setdefault("warm_start_from_previous_fold", False)
    training.setdefault("chunk_rows", 0)
    training.setdefault("vram_budget_gb", 8.0)
    training.setdefault("vram_safety_margin_gb", 1.0)
    training.setdefault("target_vram_fraction", 0.85)
    training.setdefault("epochs", 10)
    training.setdefault("early_stopping_no_improve_ratio", 0.2)
    training.setdefault("learning_rate", 1e-3)
    training.setdefault("model_name", "mlp")
    training.setdefault("top_k", 20)
    training.setdefault("num_workers", 0)
    training.setdefault("weight_decay", 1e-5)
    training.setdefault("loss_type", "mse")

    # Model-specific blocks.
    legacy_hidden_dim = training.get("hidden_dim", 128)
    legacy_hidden_layers = training.get("hidden_layers", 2)
    legacy_embedding_dim = training.get("embedding_dim", 64)
    legacy_transformer_layers = training.get("transformer_layers", 2)
    legacy_transformer_heads = training.get("transformer_heads", 4)
    legacy_transformer_ffn_dim = training.get("transformer_ffn_dim", 256)
    legacy_transformer_use_cls_token = training.get("transformer_use_cls_token", True)
    legacy_dropout = training.get("dropout", 0.1)

    mlp = training.setdefault("mlp", {})
    mlp.setdefault("hidden_dim", legacy_hidden_dim)
    mlp.setdefault("hidden_layers", legacy_hidden_layers)
    mlp.setdefault("embedding_dim", legacy_embedding_dim)
    mlp.setdefault("dropout", legacy_dropout)

    ft_transformer = training.setdefault("ft_transformer", {})
    ft_transformer.setdefault("d_token", legacy_embedding_dim)
    ft_transformer.setdefault("n_layers", legacy_transformer_layers)
    ft_transformer.setdefault("n_heads", legacy_transformer_heads)
    ft_transformer.setdefault("ffn_dim", legacy_transformer_ffn_dim)
    ft_transformer.setdefault("dropout", legacy_dropout)
    ft_transformer.setdefault("use_cls_token", legacy_transformer_use_cls_token)

    # Remove legacy flat model keys from normalized payload.
    training.pop("hidden_dim", None)
    training.pop("hidden_layers", None)
    training.pop("embedding_dim", None)
    training.pop("transformer_layers", None)
    training.pop("transformer_heads", None)
    training.pop("transformer_ffn_dim", None)
    training.pop("transformer_use_cls_token", None)
    training.pop("dropout", None)

    evaluation = raw.setdefault("evaluation", {})
    evaluation.setdefault("gamma_sharpe", 1.0)
    evaluation.setdefault("gamma_turnover", 0.0)

    data = raw.setdefault("data", {})
    data.setdefault("use_rapids", False)
    data.setdefault("usd_only_trading_pairs", False)

    trading = raw.setdefault("trading", {})
    trading.setdefault("max_turnover_ratio", 0.0)
    fee_per_side_raw = trading.get("fee_per_side", None)
    buy_fee_raw = trading.get("buy_fee_rate", None)
    sell_fee_raw = trading.get("sell_fee_rate", None)

    if buy_fee_raw is None and sell_fee_raw is None:
        fee = float(fee_per_side_raw or 0.0)
        trading["buy_fee_rate"] = fee
        trading["sell_fee_rate"] = fee
    else:
        trading["buy_fee_rate"] = float(buy_fee_raw if buy_fee_raw is not None else fee_per_side_raw or 0.0)
        trading["sell_fee_rate"] = float(sell_fee_raw if sell_fee_raw is not None else fee_per_side_raw or 0.0)

    # Legacy key is accepted as input but removed from the normalized config payload.
    trading.pop("fee_per_side", None)
    return raw


def load_config(path: str | Path) -> ExperimentConfig:
    with Path(path).open("r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle)

    raw = _merge_defaults(raw)
    training_raw = raw["training"]
    return ExperimentConfig(
        experiment_name=raw["experiment_name"],
        environment=EnvironmentConfig(**raw["environment"]),
        data=DataConfig(**raw["data"]),
        walk_forward=WalkForwardConfig(**raw["walk_forward"]),
        trading=TradingConfig(**raw["trading"]),
        training=TrainingConfig(
            backend=training_raw["backend"],
            target=training_raw["target"],
            batch_mode=training_raw["batch_mode"],
            non_blocking_transfer=training_raw["non_blocking_transfer"],
            model_name=training_raw["model_name"],
            enable_torch_compile=training_raw["enable_torch_compile"],
            warm_start_from_previous_fold=training_raw["warm_start_from_previous_fold"],
            chunk_rows=training_raw["chunk_rows"],
            lookback=training_raw["lookback"],
            batch_size=training_raw["batch_size"],
            batch_size_train=training_raw["batch_size_train"],
            batch_size_eval=training_raw["batch_size_eval"],
            min_batch_size=training_raw["min_batch_size"],
            auto_batch_size=training_raw["auto_batch_size"],
            vram_budget_gb=training_raw["vram_budget_gb"],
            vram_safety_margin_gb=training_raw["vram_safety_margin_gb"],
            target_vram_fraction=training_raw["target_vram_fraction"],
            epochs=training_raw["epochs"],
            early_stopping_no_improve_ratio=training_raw["early_stopping_no_improve_ratio"],
            learning_rate=training_raw["learning_rate"],
            top_k=training_raw["top_k"],
            num_workers=training_raw["num_workers"],
            weight_decay=training_raw["weight_decay"],
            loss_type=training_raw["loss_type"],
            mlp=MLPModelConfig(**training_raw["mlp"]),
            ft_transformer=FTTransformerModelConfig(**training_raw["ft_transformer"]),
        ),
        evaluation=EvaluationConfig(**raw["evaluation"]),
    )
