from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


@dataclass(slots=True)
class RunnerConfig:
    output_dir: str = "artifacts"
    require_cuda: bool = True
    mode: str = "train"
    resume: bool = True
    post_train_infer: bool = True
    start_fold: int | None = None


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
    tw_limit_up_down_guard: bool = False


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
class TabularResNetModelConfig:
    embedding_dim: int = 128
    hidden_dim: int = 256
    n_blocks: int = 4
    dropout: float = 0.1


@dataclass(slots=True)
class LightGBMModelConfig:
    use_gpu: bool = True
    gpu_device_id: int = 0
    n_estimators: int = 300
    num_leaves: int = 63
    max_depth: int = -1
    learning_rate: float = 0.05
    subsample: float = 0.9
    colsample_bytree: float = 0.9
    reg_lambda: float = 1.0
    n_jobs: int = -1
    random_state: int = 42


@dataclass(slots=True)
class XGBoostModelConfig:
    use_gpu: bool = True
    gpu_device_id: int = 0
    n_estimators: int = 300
    max_depth: int = 8
    learning_rate: float = 0.05
    subsample: float = 0.9
    colsample_bytree: float = 0.9
    reg_lambda: float = 1.0
    n_jobs: int = -1
    random_state: int = 42


@dataclass(slots=True)
class TrainingConfig:
    backend: str
    target: str
    batch_mode: str
    non_blocking_transfer: bool
    model_name: str
    enable_torch_compile: bool = False
    auto_torch_compile_sharpe: bool = True
    compile_loss: bool | None = None
    warm_start_from_previous_fold: bool = False
    chunk_rows: int = 0
    train_symbol_subsample_ratio: float = 1.0
    detach_prev_state: bool = True
    prefer_fp16: bool = True
    backtest_autotune: bool = True
    backtest_compile: bool = True
    backtest_verbose: bool = False
    backtest_checkpoint_chunk_rows: int = 0
    runtime_shape_check: bool = False
    allow_dynamic_symbols: bool = True
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
    val_interval_epochs: int = 1
    learning_rate: float = 1e-3
    enable_lr_scheduler: bool = True
    lr_scheduler: str = "none"  # "none", "cosine", "step", "plateau"
    lr_scheduler_t_max: int = 0
    lr_scheduler_eta_min: float = 1e-5
    lr_scheduler_step_size: int = 50
    lr_scheduler_gamma: float = 0.5
    lr_scheduler_patience: int = 5
    lr_scheduler_threshold: float = 1e-4
    top_k: int = 20
    num_workers: int = 0
    weight_decay: float = 1e-5
    grad_clip_norm: float = 1.0
    loss_type: str = "mse"  # "mse" or "sharpe"
    mlp: MLPModelConfig = field(default_factory=MLPModelConfig)
    ft_transformer: FTTransformerModelConfig = field(default_factory=FTTransformerModelConfig)
    tabular_resnet: TabularResNetModelConfig = field(default_factory=TabularResNetModelConfig)
    lightgbm: LightGBMModelConfig = field(default_factory=LightGBMModelConfig)
    xgboost: XGBoostModelConfig = field(default_factory=XGBoostModelConfig)


@dataclass(slots=True)
class EvaluationConfig:
    primary_baseline: str
    metrics: list[str]
    gamma_sharpe: float = 1.0
    gamma_turnover: float = 0.0


@dataclass(slots=True)
class ExperimentConfig:
    experiment_name: str
    runner: RunnerConfig
    environment: EnvironmentConfig
    data: DataConfig
    walk_forward: WalkForwardConfig
    trading: TradingConfig
    training: TrainingConfig
    evaluation: EvaluationConfig


def _merge_defaults(raw: dict[str, Any]) -> dict[str, Any]:
    runner = raw.setdefault("runner", {})
    runner.setdefault("output_dir", "artifacts")
    runner.setdefault("require_cuda", True)
    runner.setdefault("mode", "train")
    runner.setdefault("resume", True)
    runner.setdefault("post_train_infer", True)
    runner.setdefault("start_fold", None)

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
    training.setdefault("auto_torch_compile_sharpe", True)
    training.setdefault("compile_loss", None)
    training.setdefault("warm_start_from_previous_fold", False)
    training.setdefault("chunk_rows", 0)
    training.setdefault("train_symbol_subsample_ratio", 1.0)
    training.setdefault("detach_prev_state", True)
    training.setdefault("prefer_fp16", True)
    training.setdefault("backtest_autotune", True)
    training.setdefault("backtest_compile", True)
    training.setdefault("backtest_verbose", False)
    training.setdefault("backtest_checkpoint_chunk_rows", 0)
    training.setdefault("runtime_shape_check", False)
    training.setdefault("allow_dynamic_symbols", True)
    training.setdefault("vram_budget_gb", 8.0)
    training.setdefault("vram_safety_margin_gb", 1.0)
    training.setdefault("target_vram_fraction", 0.85)
    training.setdefault("epochs", 10)
    training.setdefault("early_stopping_no_improve_ratio", 0.2)
    training.setdefault("val_interval_epochs", 1)
    training.setdefault("learning_rate", 1e-3)
    training.setdefault("enable_lr_scheduler", True)
    training.setdefault("lr_scheduler", "none")
    training.setdefault("lr_scheduler_t_max", 0)
    training.setdefault("lr_scheduler_eta_min", 1e-5)
    training.setdefault("lr_scheduler_step_size", 50)
    training.setdefault("lr_scheduler_gamma", 0.5)
    training.setdefault("lr_scheduler_patience", 5)
    training.setdefault("lr_scheduler_threshold", 1e-4)
    training.setdefault("model_name", "mlp")
    training.setdefault("top_k", 20)
    training.setdefault("num_workers", 0)
    training.setdefault("weight_decay", 1e-5)
    training.setdefault("grad_clip_norm", 1.0)
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

    tabular_resnet = training.setdefault("tabular_resnet", {})
    tabular_resnet.setdefault("embedding_dim", max(64, int(legacy_embedding_dim)))
    tabular_resnet.setdefault("hidden_dim", max(128, int(legacy_hidden_dim)))
    tabular_resnet.setdefault("n_blocks", 4)
    tabular_resnet.setdefault("dropout", legacy_dropout)

    lightgbm = training.setdefault("lightgbm", {})
    lightgbm.setdefault("use_gpu", True)
    lightgbm.setdefault("gpu_device_id", 0)
    lightgbm.setdefault("n_estimators", 300)
    lightgbm.setdefault("num_leaves", 63)
    lightgbm.setdefault("max_depth", -1)
    lightgbm.setdefault("learning_rate", 0.05)
    lightgbm.setdefault("subsample", 0.9)
    lightgbm.setdefault("colsample_bytree", 0.9)
    lightgbm.setdefault("reg_lambda", 1.0)
    lightgbm.setdefault("n_jobs", -1)
    lightgbm.setdefault("random_state", 42)

    xgboost = training.setdefault("xgboost", {})
    xgboost.setdefault("use_gpu", True)
    xgboost.setdefault("gpu_device_id", 0)
    xgboost.setdefault("n_estimators", 300)
    xgboost.setdefault("max_depth", 8)
    xgboost.setdefault("learning_rate", 0.05)
    xgboost.setdefault("subsample", 0.9)
    xgboost.setdefault("colsample_bytree", 0.9)
    xgboost.setdefault("reg_lambda", 1.0)
    xgboost.setdefault("n_jobs", -1)
    xgboost.setdefault("random_state", 42)

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
    data.setdefault("tw_limit_up_down_guard", False)

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
        runner=RunnerConfig(**raw["runner"]),
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
            auto_torch_compile_sharpe=training_raw["auto_torch_compile_sharpe"],
            compile_loss=training_raw["compile_loss"],
            warm_start_from_previous_fold=training_raw["warm_start_from_previous_fold"],
            chunk_rows=training_raw["chunk_rows"],
            train_symbol_subsample_ratio=training_raw["train_symbol_subsample_ratio"],
            detach_prev_state=training_raw["detach_prev_state"],
            prefer_fp16=training_raw["prefer_fp16"],
            backtest_autotune=training_raw["backtest_autotune"],
            backtest_compile=training_raw["backtest_compile"],
            backtest_verbose=training_raw["backtest_verbose"],
            backtest_checkpoint_chunk_rows=training_raw["backtest_checkpoint_chunk_rows"],
            runtime_shape_check=training_raw["runtime_shape_check"],
            allow_dynamic_symbols=training_raw["allow_dynamic_symbols"],
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
            val_interval_epochs=training_raw["val_interval_epochs"],
            learning_rate=training_raw["learning_rate"],
            enable_lr_scheduler=training_raw["enable_lr_scheduler"],
            lr_scheduler=training_raw["lr_scheduler"],
            lr_scheduler_t_max=training_raw["lr_scheduler_t_max"],
            lr_scheduler_eta_min=training_raw["lr_scheduler_eta_min"],
            lr_scheduler_step_size=training_raw["lr_scheduler_step_size"],
            lr_scheduler_gamma=training_raw["lr_scheduler_gamma"],
            lr_scheduler_patience=training_raw["lr_scheduler_patience"],
            lr_scheduler_threshold=training_raw["lr_scheduler_threshold"],
            top_k=training_raw["top_k"],
            num_workers=training_raw["num_workers"],
            weight_decay=training_raw["weight_decay"],
            grad_clip_norm=training_raw["grad_clip_norm"],
            loss_type=training_raw["loss_type"],
            mlp=MLPModelConfig(**training_raw["mlp"]),
            ft_transformer=FTTransformerModelConfig(**training_raw["ft_transformer"]),
            tabular_resnet=TabularResNetModelConfig(**training_raw["tabular_resnet"]),
            lightgbm=LightGBMModelConfig(**training_raw["lightgbm"]),
            xgboost=XGBoostModelConfig(**training_raw["xgboost"]),
        ),
        evaluation=EvaluationConfig(**raw["evaluation"]),
    )
