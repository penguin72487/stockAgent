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
    tradable_mode: str = "tradable"
    panel_backend: str = "auto"
    panel_load_workers: int = 4


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
    max_turnover_ratio: float = 0.0
    gross_leverage: float = 1.0


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
class TemporalTabularResNetModelConfig:
    temporal_hidden_dim: int = 128
    temporal_layers: int = 1
    temporal_dropout: float = 0.1
    embedding_dim: int = 128
    hidden_dim: int = 256
    n_blocks: int = 4
    dropout: float = 0.1


@dataclass(slots=True)
class TCNHybridTabularResNetModelConfig:
    embedding_dim: int = 128
    encoder_hidden_dim: int = 256
    encoder_blocks: int = 2
    tcn_blocks: int = 3
    tcn_kernel_size: int = 3
    dropout: float = 0.1


@dataclass(slots=True)
class MultiStockTCNModelConfig:
    hidden_channels: int = 64
    embedding_dim: int = 64
    tcn_blocks: int = 4
    tcn_kernel_size: int = 3
    head_hidden_dim: int = 64
    head_layers: int = 1
    dropout: float = 0.1
    tcn_conv_mode: str = "separable"
    conv_layers_per_block: int = 1
    norm_type: str = "none"
    sanitize_inputs: bool = False


@dataclass(slots=True)
class EfficientTCNTabularSetPortfolioModelConfig:
    temporal_enabled: bool = True
    temporal_dim: int = 16
    temporal_hidden_channels: int = 32
    temporal_dilations: list[int] = field(default_factory=lambda: [1, 2])
    temporal_kernel_size: int = 3
    tabular_dim: int = 64
    tabular_hidden_dim: int = 128
    tabular_blocks: int = 2
    model_dim: int = 64
    set_enabled: bool = True
    num_inducing_points: int = 16
    num_heads: int = 4
    ffn_mult: int = 2
    head_hidden_dim: int = 64
    head_layers: int = 1
    dropout: float = 0.1
    residual_scale: float = 0.5
    default_temperature: float = 1.0
    portfolio_mode: str = "auto"
    return_aux: bool = True


@dataclass(slots=True)
class LatentFactorMarketTokenPortfolioModelConfig:
    temporal_enabled: bool = True
    temporal_dim: int = 16
    temporal_hidden_channels: int = 32
    temporal_dilations: list[int] = field(default_factory=lambda: [1, 2])
    temporal_kernel_size: int = 3
    tabular_dim: int = 64
    tabular_hidden_dim: int = 128
    tabular_blocks: int = 2
    stock_embedding_dim: int = 64
    num_latent_factors: int = 32
    num_market_tokens: int = 4
    num_heads: int = 4
    ffn_mult: int = 2
    head_hidden_dim: int = 64
    head_layers: int = 1
    dropout: float = 0.1
    residual_scale: float = 0.5
    default_temperature: float = 1.0
    portfolio_mode: str = "auto"
    return_aux: bool = True


@dataclass(slots=True)
class LowRankMarketTransformerPortfolioModelConfig:
    feature_dim: int = 24
    temporal_mixer: str = "conv"
    temporal_layers: int = 1
    temporal_heads: int = 2
    temporal_ffn_dim: int = 48
    temporal_dropout: float = 0.1
    temporal_pooling: str = "last"
    temporal_kernel_size: int = 5
    temporal_dilations: list[int] = field(default_factory=lambda: [1])
    temporal_checkpoint: bool = True
    stock_embedding_dim: int = 24
    num_latent_factors: int = 8
    num_market_tokens: int = 4
    cross_heads: int = 2
    cross_ffn_mult: int = 1
    head_hidden_dim: int = 24
    head_layers: int = 1
    dropout: float = 0.1
    default_temperature: float = 1.0
    portfolio_mode: str = "auto"
    return_aux: bool = True
    return_aux_details: bool = False


@dataclass(slots=True)
class TransformerBasePortfolioModelConfig:
    d_model: int = 64
    attention_mode: str = "latent"
    use_flash_attention: bool = True
    use_time_pos: bool = True
    use_symbol_pos: bool = True
    input_dropout: float = 0.0
    sdpa_batch_limit: int = 4096
    norm_type: str = "rmsnorm"
    ffn_type: str = "swiglu"
    qk_norm: bool = True
    rope_temporal: bool = True
    rope_base: float = 10000.0
    temporal_layers: int = 2
    temporal_heads: int = 4
    temporal_ffn_mult: int = 2
    temporal_pooling: str = "attention"
    cross_layers: int = 1
    cross_heads: int = 4
    cross_ffn_mult: int = 2
    joint_layers: int = 2
    joint_heads: int = 4
    joint_ffn_mult: int = 2
    latent_layers: int = 1
    num_latent_factors: int = 16
    num_market_tokens: int = 4
    market_layers: int = 1
    dynamic_latent_tokens: bool = True
    dynamic_market_tokens: bool = True
    dynamic_token_hidden_mult: int = 2
    dynamic_token_gate_init: float = 0.1
    dynamic_token_dropout: float = 0.1
    head_hidden_dim: int = 64
    head_layers: int = 1
    dropout: float = 0.1
    default_temperature: float = 1.0
    portfolio_mode: str = "auto"
    max_full_tokens: int = 4096
    checkpoint_blocks: bool = False
    return_aux: bool = True
    return_aux_details: bool = False


@dataclass(slots=True)
class BottleneckPortfolioAutoencoderConfig:
    d_model: int = 128
    z_dim: int = 32
    temporal_type: str = "gru"
    temporal_layers: int = 1
    asset_encoder_type: str = "transformer"
    asset_encoder_layers: int = 2
    n_heads: int = 4
    num_inducing_points: int = 32
    ffn_mult: int = 2
    dropout: float = 0.1
    long_short: bool = True
    noise_std: float = 0.01
    return_aux: bool = True


@dataclass(slots=True)
class CrossSectionalTemporalPortfolioModelConfig:
    stock_embedding_dim: int = 128
    stock_hidden_dim: int = 128
    stock_n_blocks: int = 2
    temporal_hidden_dim: int = 128
    temporal_blocks: int = 2
    temporal_kernel_size: int = 3
    cross_hidden_dim: int = 128
    cross_heads: int = 4
    cross_layers: int = 2
    dropout: float = 0.1
    regime_classes: int = 3
    candidate_top_m: int = 64
    portfolio_top_k: int = 10
    candidate_k: int = 64
    trade_k: int = 10
    scorer: str = "tabular_resnet"
    scorer_hidden: int = 128
    scorer_blocks: int = 2
    reranker: str = "set_transformer"
    d_model: int = 128
    heads: int = 4
    layers: int = 2


@dataclass(slots=True)
class MultitaskLossConfig:
    rank_ic_weight: float = 0.20
    direction_weight: float = 0.05
    volatility_regime_weight: float = 0.05
    concentration_weight: float = 0.005
    regime_up_threshold: float = 0.002
    regime_down_threshold: float = -0.002


@dataclass(slots=True)
class FactorGeneralizationLossConfig:
    slope_tstat_weight: float = 1.0
    rank_ic_weight: float = 0.5
    factor_sharpe_weight: float = 0.25
    block_stability_weight: float = 0.20
    regime_stability_weight: float = 0.20
    consistency_weight: float = 0.05
    net_exposure_weight: float = 0.05
    gross_exposure_weight: float = 0.02
    concentration_weight: float = 0.02
    turnover_weight: float = 0.02
    score_l2_weight: float = 0.001
    factor_temperature: float = 1.0
    block_count: int = 4
    worst_fraction: float = 0.25
    augmentation_feature_dropout: float = 0.10
    augmentation_stock_dropout: float = 0.05
    augmentation_time_dropout: float = 0.05
    augmentation_noise_std: float = 0.01


@dataclass(slots=True)
class PortfolioAutoencoderLossConfig:
    cost_rate: float = 0.001425
    lambda_turnover: float = 0.1
    lambda_concentration: float = 0.01
    lambda_latent: float = 0.001


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
    prefer_fp16: bool = False
    backtest_autotune: bool = True
    backtest_compile: bool = True
    backtest_compile_stateful: bool = True
    backtest_cpp_ext: bool = False
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
    curve_test_interval: int = 100
    curve_plot_interval: int = 1
    curve_plot_async: bool = True
    epoch_test_curve: bool = True
    defer_epoch_curve_plot_until_end: bool = False
    explain_after_each_fold: bool = True
    explain_first_test_year_only: bool = True
    explain_top_k: int = 20
    explain_max_rows: int = 32
    explain_ig_steps: int = 8
    explain_sample_method: str = "even"
    explain_perturb: bool = True
    explain_write_plots: bool = True
    cache_train_tensors_on_gpu: bool = True
    cache_eval_tensors_on_gpu: bool = True
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
    finite_check_interval_steps: int = 0
    materialize_window_tensors: bool = False
    loss_type: str = "mse"  # "mse", "pure_rank", "rank_ic", "sharpe", "sortino", "log_utility", etc.
    mlp: MLPModelConfig = field(default_factory=MLPModelConfig)
    ft_transformer: FTTransformerModelConfig = field(default_factory=FTTransformerModelConfig)
    tabular_resnet: TabularResNetModelConfig = field(default_factory=TabularResNetModelConfig)
    multi_stock_tcn: MultiStockTCNModelConfig = field(default_factory=MultiStockTCNModelConfig)
    efficient_tcn_tabular_set_portfolio: EfficientTCNTabularSetPortfolioModelConfig = field(
        default_factory=EfficientTCNTabularSetPortfolioModelConfig
    )
    latent_factor_market_token_portfolio: LatentFactorMarketTokenPortfolioModelConfig = field(
        default_factory=LatentFactorMarketTokenPortfolioModelConfig
    )
    low_rank_market_transformer_portfolio: LowRankMarketTransformerPortfolioModelConfig = field(
        default_factory=LowRankMarketTransformerPortfolioModelConfig
    )
    transformer_base_portfolio: TransformerBasePortfolioModelConfig = field(
        default_factory=TransformerBasePortfolioModelConfig
    )
    bottleneck_portfolio_autoencoder: BottleneckPortfolioAutoencoderConfig = field(default_factory=BottleneckPortfolioAutoencoderConfig)
    tcn_hybrid_tabular_resnet: TCNHybridTabularResNetModelConfig = field(default_factory=TCNHybridTabularResNetModelConfig)
    temporal_tabular_resnet: TemporalTabularResNetModelConfig = field(default_factory=TemporalTabularResNetModelConfig)
    cross_sectional_temporal_portfolio_model: CrossSectionalTemporalPortfolioModelConfig = field(default_factory=CrossSectionalTemporalPortfolioModelConfig)
    multitask_loss: MultitaskLossConfig = field(default_factory=MultitaskLossConfig)
    factor_generalization_loss: FactorGeneralizationLossConfig = field(default_factory=FactorGeneralizationLossConfig)
    portfolio_autoencoder_loss: PortfolioAutoencoderLossConfig = field(default_factory=PortfolioAutoencoderLossConfig)
    lightgbm: LightGBMModelConfig = field(default_factory=LightGBMModelConfig)
    xgboost: XGBoostModelConfig = field(default_factory=XGBoostModelConfig)


@dataclass(slots=True)
class EvaluationConfig:
    primary_baseline: str
    metrics: list[str]
    gamma_sharpe: float = 1.0
    gamma_excess: float = 1.0
    gamma_cvar: float = 1.0
    cvar_alpha: float = 0.95
    gamma_drawdown: float = 0.0
    drawdown_target: float = 0.2
    gamma_turnover: float = 0.0
    gamma_underperformance: float = 1.0
    excess_target: float = 0.0
    cvar_budget: float = 0.03
    drawdown_budget: float = 0.2
    turnover_budget: float = 0.3
    gamma_cvar_budget: float = 1.0
    gamma_drawdown_budget: float = 1.0
    gamma_turnover_budget: float = 0.0


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
    training.setdefault("prefer_fp16", False)
    training.setdefault("backtest_autotune", True)
    training.setdefault("backtest_compile", True)
    training.setdefault("backtest_compile_stateful", True)
    training.setdefault("backtest_cpp_ext", False)
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
    training.setdefault("curve_test_interval", 100)
    training.setdefault("curve_plot_interval", 1)
    training.setdefault("curve_plot_async", True)
    training.setdefault("epoch_test_curve", True)
    training.setdefault("defer_epoch_curve_plot_until_end", False)
    training.setdefault("explain_after_each_fold", True)
    training.setdefault("explain_first_test_year_only", True)
    training.setdefault("explain_top_k", 20)
    training.setdefault("explain_max_rows", 32)
    training.setdefault("explain_ig_steps", 8)
    training.setdefault("explain_sample_method", "even")
    training.setdefault("explain_perturb", True)
    training.setdefault("explain_write_plots", True)
    training.setdefault("cache_train_tensors_on_gpu", True)
    training.setdefault("cache_eval_tensors_on_gpu", True)
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
    training.setdefault("finite_check_interval_steps", 0)
    training.setdefault("materialize_window_tensors", False)
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

    multi_stock_tcn = training.setdefault("multi_stock_tcn", {})
    multi_stock_tcn.setdefault("hidden_channels", max(32, int(legacy_embedding_dim)))
    multi_stock_tcn.setdefault("embedding_dim", max(32, int(legacy_embedding_dim)))
    multi_stock_tcn.setdefault("tcn_blocks", 4)
    multi_stock_tcn.setdefault("tcn_kernel_size", 3)
    multi_stock_tcn.setdefault("head_hidden_dim", max(64, int(legacy_embedding_dim)))
    multi_stock_tcn.setdefault("head_layers", 1)
    multi_stock_tcn.setdefault("dropout", legacy_dropout)
    multi_stock_tcn.setdefault("tcn_conv_mode", "separable")
    multi_stock_tcn.setdefault("conv_layers_per_block", 1)
    multi_stock_tcn.setdefault("norm_type", "none")
    multi_stock_tcn.setdefault("sanitize_inputs", False)

    efficient_tcn_tabular_set_portfolio = training.setdefault("efficient_tcn_tabular_set_portfolio", {})
    efficient_tcn_tabular_set_portfolio.setdefault("temporal_enabled", True)
    efficient_tcn_tabular_set_portfolio.setdefault("temporal_dim", 16)
    efficient_tcn_tabular_set_portfolio.setdefault("temporal_hidden_channels", 32)
    efficient_tcn_tabular_set_portfolio.setdefault("temporal_dilations", [1, 2])
    efficient_tcn_tabular_set_portfolio.setdefault("temporal_kernel_size", 3)
    efficient_tcn_tabular_set_portfolio.setdefault("tabular_dim", 64)
    efficient_tcn_tabular_set_portfolio.setdefault("tabular_hidden_dim", 128)
    efficient_tcn_tabular_set_portfolio.setdefault("tabular_blocks", 2)
    efficient_tcn_tabular_set_portfolio.setdefault("model_dim", 64)
    efficient_tcn_tabular_set_portfolio.setdefault("set_enabled", True)
    efficient_tcn_tabular_set_portfolio.setdefault("num_inducing_points", 16)
    efficient_tcn_tabular_set_portfolio.setdefault("num_heads", 4)
    efficient_tcn_tabular_set_portfolio.setdefault("ffn_mult", 2)
    efficient_tcn_tabular_set_portfolio.setdefault("head_hidden_dim", 64)
    efficient_tcn_tabular_set_portfolio.setdefault("head_layers", 1)
    efficient_tcn_tabular_set_portfolio.setdefault("dropout", legacy_dropout)
    efficient_tcn_tabular_set_portfolio.setdefault("residual_scale", 0.5)
    efficient_tcn_tabular_set_portfolio.setdefault("default_temperature", 1.0)
    efficient_tcn_tabular_set_portfolio.setdefault("portfolio_mode", "auto")
    efficient_tcn_tabular_set_portfolio.setdefault("return_aux", True)

    latent_factor_market_token_portfolio = training.setdefault("latent_factor_market_token_portfolio", {})
    latent_factor_market_token_portfolio.setdefault("temporal_enabled", True)
    latent_factor_market_token_portfolio.setdefault("temporal_dim", 16)
    latent_factor_market_token_portfolio.setdefault("temporal_hidden_channels", 32)
    latent_factor_market_token_portfolio.setdefault("temporal_dilations", [1, 2])
    latent_factor_market_token_portfolio.setdefault("temporal_kernel_size", 3)
    latent_factor_market_token_portfolio.setdefault("tabular_dim", 64)
    latent_factor_market_token_portfolio.setdefault("tabular_hidden_dim", 128)
    latent_factor_market_token_portfolio.setdefault("tabular_blocks", 2)
    latent_factor_market_token_portfolio.setdefault("stock_embedding_dim", 64)
    latent_factor_market_token_portfolio.setdefault("num_latent_factors", 32)
    latent_factor_market_token_portfolio.setdefault("num_market_tokens", 4)
    latent_factor_market_token_portfolio.setdefault("num_heads", 4)
    latent_factor_market_token_portfolio.setdefault("ffn_mult", 2)
    latent_factor_market_token_portfolio.setdefault("head_hidden_dim", 64)
    latent_factor_market_token_portfolio.setdefault("head_layers", 1)
    latent_factor_market_token_portfolio.setdefault("dropout", legacy_dropout)
    latent_factor_market_token_portfolio.setdefault("residual_scale", 0.5)
    latent_factor_market_token_portfolio.setdefault("default_temperature", 1.0)
    latent_factor_market_token_portfolio.setdefault("portfolio_mode", "auto")
    latent_factor_market_token_portfolio.setdefault("return_aux", True)

    low_rank_market_transformer_portfolio = training.setdefault("low_rank_market_transformer_portfolio", {})
    low_rank_market_transformer_portfolio.setdefault("feature_dim", 24)
    low_rank_market_transformer_portfolio.setdefault("temporal_mixer", "conv")
    low_rank_market_transformer_portfolio.setdefault("temporal_layers", 1)
    low_rank_market_transformer_portfolio.setdefault("temporal_heads", 2)
    low_rank_market_transformer_portfolio.setdefault("temporal_ffn_dim", 48)
    low_rank_market_transformer_portfolio.setdefault("temporal_dropout", legacy_dropout)
    low_rank_market_transformer_portfolio.setdefault("temporal_pooling", "last")
    low_rank_market_transformer_portfolio.setdefault("temporal_kernel_size", 5)
    low_rank_market_transformer_portfolio.setdefault("temporal_dilations", [1])
    low_rank_market_transformer_portfolio.setdefault("temporal_checkpoint", True)
    low_rank_market_transformer_portfolio.setdefault("stock_embedding_dim", 24)
    low_rank_market_transformer_portfolio.setdefault("num_latent_factors", 8)
    low_rank_market_transformer_portfolio.setdefault("num_market_tokens", 4)
    low_rank_market_transformer_portfolio.setdefault("cross_heads", 2)
    low_rank_market_transformer_portfolio.setdefault("cross_ffn_mult", 1)
    low_rank_market_transformer_portfolio.setdefault("head_hidden_dim", 24)
    low_rank_market_transformer_portfolio.setdefault("head_layers", 1)
    low_rank_market_transformer_portfolio.setdefault("dropout", legacy_dropout)
    low_rank_market_transformer_portfolio.setdefault("default_temperature", 1.0)
    low_rank_market_transformer_portfolio.setdefault("portfolio_mode", "auto")
    low_rank_market_transformer_portfolio.setdefault("return_aux", True)
    low_rank_market_transformer_portfolio.setdefault("return_aux_details", False)

    transformer_base_portfolio = training.setdefault("transformer_base_portfolio", {})
    transformer_base_portfolio.setdefault("d_model", 64)
    transformer_base_portfolio.setdefault("attention_mode", "latent")
    transformer_base_portfolio.setdefault("use_flash_attention", True)
    transformer_base_portfolio.setdefault("use_time_pos", True)
    transformer_base_portfolio.setdefault("use_symbol_pos", True)
    transformer_base_portfolio.setdefault("input_dropout", 0.0)
    transformer_base_portfolio.setdefault("sdpa_batch_limit", 4096)
    transformer_base_portfolio.setdefault("norm_type", "rmsnorm")
    transformer_base_portfolio.setdefault("ffn_type", "swiglu")
    transformer_base_portfolio.setdefault("qk_norm", True)
    transformer_base_portfolio.setdefault("rope_temporal", True)
    transformer_base_portfolio.setdefault("rope_base", 10000.0)
    transformer_base_portfolio.setdefault("temporal_layers", 2)
    transformer_base_portfolio.setdefault("temporal_heads", 4)
    transformer_base_portfolio.setdefault("temporal_ffn_mult", 2)
    transformer_base_portfolio.setdefault("temporal_pooling", "attention")
    transformer_base_portfolio.setdefault("cross_layers", 1)
    transformer_base_portfolio.setdefault("cross_heads", 4)
    transformer_base_portfolio.setdefault("cross_ffn_mult", 2)
    transformer_base_portfolio.setdefault("joint_layers", 2)
    transformer_base_portfolio.setdefault("joint_heads", 4)
    transformer_base_portfolio.setdefault("joint_ffn_mult", 2)
    transformer_base_portfolio.setdefault("latent_layers", 1)
    transformer_base_portfolio.setdefault("num_latent_factors", 16)
    transformer_base_portfolio.setdefault("num_market_tokens", 4)
    transformer_base_portfolio.setdefault("market_layers", 1)
    transformer_base_portfolio.setdefault("dynamic_latent_tokens", True)
    transformer_base_portfolio.setdefault("dynamic_market_tokens", True)
    transformer_base_portfolio.setdefault("dynamic_token_hidden_mult", 2)
    transformer_base_portfolio.setdefault("dynamic_token_gate_init", 0.1)
    transformer_base_portfolio.setdefault("dynamic_token_dropout", 0.1)
    transformer_base_portfolio.setdefault("head_hidden_dim", 64)
    transformer_base_portfolio.setdefault("head_layers", 1)
    transformer_base_portfolio.setdefault("dropout", legacy_dropout)
    transformer_base_portfolio.setdefault("default_temperature", 1.0)
    transformer_base_portfolio.setdefault("portfolio_mode", "auto")
    transformer_base_portfolio.setdefault("max_full_tokens", 4096)
    transformer_base_portfolio.setdefault("checkpoint_blocks", False)
    transformer_base_portfolio.setdefault("return_aux", True)
    transformer_base_portfolio.setdefault("return_aux_details", False)

    bottleneck_portfolio_autoencoder = training.setdefault("bottleneck_portfolio_autoencoder", {})
    bottleneck_portfolio_autoencoder.setdefault("d_model", 128)
    bottleneck_portfolio_autoencoder.setdefault("z_dim", 32)
    bottleneck_portfolio_autoencoder.setdefault("temporal_type", "gru")
    bottleneck_portfolio_autoencoder.setdefault("temporal_layers", 1)
    bottleneck_portfolio_autoencoder.setdefault("asset_encoder_type", "transformer")
    bottleneck_portfolio_autoencoder.setdefault("asset_encoder_layers", 2)
    bottleneck_portfolio_autoencoder.setdefault("n_heads", 4)
    bottleneck_portfolio_autoencoder.setdefault("num_inducing_points", 32)
    bottleneck_portfolio_autoencoder.setdefault("ffn_mult", 2)
    bottleneck_portfolio_autoencoder.setdefault("dropout", legacy_dropout)
    bottleneck_portfolio_autoencoder.setdefault("long_short", True)
    bottleneck_portfolio_autoencoder.setdefault("noise_std", 0.01)
    bottleneck_portfolio_autoencoder.setdefault("return_aux", True)

    tcn_hybrid_tabular_resnet = training.setdefault("tcn_hybrid_tabular_resnet", {})
    tcn_hybrid_tabular_resnet.setdefault("embedding_dim", max(64, int(legacy_embedding_dim)))
    tcn_hybrid_tabular_resnet.setdefault("encoder_hidden_dim", max(128, int(legacy_hidden_dim)))
    tcn_hybrid_tabular_resnet.setdefault("encoder_blocks", 2)
    tcn_hybrid_tabular_resnet.setdefault("tcn_blocks", 3)
    tcn_hybrid_tabular_resnet.setdefault("tcn_kernel_size", 3)
    tcn_hybrid_tabular_resnet.setdefault("dropout", legacy_dropout)

    temporal_tabular_resnet = training.setdefault("temporal_tabular_resnet", {})
    temporal_tabular_resnet.setdefault("temporal_hidden_dim", max(64, int(legacy_embedding_dim)))
    temporal_tabular_resnet.setdefault("temporal_layers", 1)
    temporal_tabular_resnet.setdefault("temporal_dropout", legacy_dropout)
    temporal_tabular_resnet.setdefault("embedding_dim", max(64, int(legacy_embedding_dim)))
    temporal_tabular_resnet.setdefault("hidden_dim", max(128, int(legacy_hidden_dim)))
    temporal_tabular_resnet.setdefault("n_blocks", 4)
    temporal_tabular_resnet.setdefault("dropout", legacy_dropout)

    cross_sectional_temporal_portfolio_model = training.setdefault("cross_sectional_temporal_portfolio_model", {})
    cross_sectional_temporal_portfolio_model.setdefault("candidate_k", 64)
    cross_sectional_temporal_portfolio_model.setdefault("trade_k", 10)
    cross_sectional_temporal_portfolio_model.setdefault("scorer", "tabular_resnet")
    cross_sectional_temporal_portfolio_model.setdefault("scorer_hidden", 128)
    cross_sectional_temporal_portfolio_model.setdefault("scorer_blocks", 2)
    cross_sectional_temporal_portfolio_model.setdefault("reranker", "set_transformer")
    cross_sectional_temporal_portfolio_model.setdefault("d_model", 128)
    cross_sectional_temporal_portfolio_model.setdefault("heads", 4)
    cross_sectional_temporal_portfolio_model.setdefault("layers", 2)
    cross_sectional_temporal_portfolio_model.setdefault("stock_embedding_dim", int(cross_sectional_temporal_portfolio_model["d_model"]))
    cross_sectional_temporal_portfolio_model.setdefault("stock_hidden_dim", int(cross_sectional_temporal_portfolio_model["scorer_hidden"]))
    cross_sectional_temporal_portfolio_model.setdefault("stock_n_blocks", int(cross_sectional_temporal_portfolio_model["scorer_blocks"]))
    cross_sectional_temporal_portfolio_model.setdefault("temporal_hidden_dim", max(64, int(legacy_embedding_dim)))
    cross_sectional_temporal_portfolio_model.setdefault("temporal_blocks", 2)
    cross_sectional_temporal_portfolio_model.setdefault("temporal_kernel_size", 3)
    cross_sectional_temporal_portfolio_model.setdefault("cross_hidden_dim", int(cross_sectional_temporal_portfolio_model["d_model"]))
    cross_sectional_temporal_portfolio_model.setdefault("cross_heads", int(cross_sectional_temporal_portfolio_model["heads"]))
    cross_sectional_temporal_portfolio_model.setdefault("cross_layers", int(cross_sectional_temporal_portfolio_model["layers"]))
    cross_sectional_temporal_portfolio_model.setdefault("dropout", legacy_dropout)
    cross_sectional_temporal_portfolio_model.setdefault("regime_classes", 3)
    cross_sectional_temporal_portfolio_model.setdefault("candidate_top_m", int(cross_sectional_temporal_portfolio_model["candidate_k"]))
    cross_sectional_temporal_portfolio_model.setdefault("portfolio_top_k", int(cross_sectional_temporal_portfolio_model["trade_k"]))

    multitask_loss = training.setdefault("multitask_loss", {})
    multitask_loss.setdefault("rank_ic_weight", 0.20)
    multitask_loss.setdefault("direction_weight", 0.05)
    multitask_loss.setdefault("volatility_regime_weight", 0.05)
    multitask_loss.setdefault("concentration_weight", 0.005)
    multitask_loss.setdefault("regime_up_threshold", 0.002)
    multitask_loss.setdefault("regime_down_threshold", -0.002)

    factor_generalization_loss = training.setdefault("factor_generalization_loss", {})
    factor_generalization_loss.setdefault("slope_tstat_weight", 1.0)
    factor_generalization_loss.setdefault("rank_ic_weight", 0.5)
    factor_generalization_loss.setdefault("factor_sharpe_weight", 0.25)
    factor_generalization_loss.setdefault("block_stability_weight", 0.20)
    factor_generalization_loss.setdefault("regime_stability_weight", 0.20)
    factor_generalization_loss.setdefault("consistency_weight", 0.05)
    factor_generalization_loss.setdefault("net_exposure_weight", 0.05)
    factor_generalization_loss.setdefault("gross_exposure_weight", 0.02)
    factor_generalization_loss.setdefault("concentration_weight", 0.02)
    factor_generalization_loss.setdefault("turnover_weight", 0.02)
    factor_generalization_loss.setdefault("score_l2_weight", 0.001)
    factor_generalization_loss.setdefault("factor_temperature", 1.0)
    factor_generalization_loss.setdefault("block_count", 4)
    factor_generalization_loss.setdefault("worst_fraction", 0.25)
    factor_generalization_loss.setdefault("augmentation_feature_dropout", 0.10)
    factor_generalization_loss.setdefault("augmentation_stock_dropout", 0.05)
    factor_generalization_loss.setdefault("augmentation_time_dropout", 0.05)
    factor_generalization_loss.setdefault("augmentation_noise_std", 0.01)

    portfolio_autoencoder_loss = training.setdefault("portfolio_autoencoder_loss", {})
    portfolio_autoencoder_loss.setdefault("cost_rate", 0.001425)
    portfolio_autoencoder_loss.setdefault("lambda_turnover", 0.1)
    portfolio_autoencoder_loss.setdefault("lambda_concentration", 0.01)
    portfolio_autoencoder_loss.setdefault("lambda_latent", 0.001)

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
    evaluation.setdefault("gamma_excess", 1.0)
    evaluation.setdefault("gamma_cvar", 1.0)
    evaluation.setdefault("cvar_alpha", 0.95)
    evaluation.setdefault("gamma_drawdown", 0.0)
    evaluation.setdefault("drawdown_target", 0.2)
    evaluation.setdefault("gamma_turnover", 0.0)
    evaluation.setdefault("gamma_underperformance", 1.0)
    evaluation.setdefault("excess_target", 0.0)
    evaluation.setdefault("cvar_budget", 0.03)
    evaluation.setdefault("drawdown_budget", 0.2)
    evaluation.setdefault("turnover_budget", 0.3)
    evaluation.setdefault("gamma_cvar_budget", 1.0)
    evaluation.setdefault("gamma_drawdown_budget", 1.0)
    evaluation.setdefault("gamma_turnover_budget", 0.0)

    data = raw.setdefault("data", {})
    data.setdefault("use_rapids", False)
    data.setdefault("usd_only_trading_pairs", False)
    data.setdefault("panel_backend", "auto")
    data.setdefault("panel_load_workers", 4)

    trading = raw.setdefault("trading", {})

    # Legacy migration:
    # - data.tw_limit_up_down_guard=true  -> buy/sell use TW limit guard
    # - trading.use_all_tradable_symbols was previously not wired at runtime,
    #   so we keep historical behavior by deriving modes from tw_limit_up_down_guard.
    legacy_tw_guard = bool(data.pop("tw_limit_up_down_guard", False))
    trading.pop("use_all_tradable_symbols", None)

    raw_tradable_mode = data.get("tradable_mode", None)
    raw_buy_mode = data.pop("buy_tradable_mode", None)
    raw_sell_mode = data.pop("sell_tradable_mode", None)

    if raw_tradable_mode is not None:
        data["tradable_mode"] = raw_tradable_mode
    elif raw_buy_mode is not None and raw_sell_mode is not None:
        buy_mode_normalized = str(raw_buy_mode).strip().lower()
        sell_mode_normalized = str(raw_sell_mode).strip().lower()
        if buy_mode_normalized != sell_mode_normalized:
            raise ValueError(
                "data.buy_tradable_mode and data.sell_tradable_mode must be identical; "
                f"got {raw_buy_mode!r} and {raw_sell_mode!r}"
            )
        data["tradable_mode"] = buy_mode_normalized
    elif raw_buy_mode is not None:
        data["tradable_mode"] = raw_buy_mode
    elif raw_sell_mode is not None:
        data["tradable_mode"] = raw_sell_mode
    elif legacy_tw_guard:
        data["tradable_mode"] = "tw_limit_guard"
    else:
        data["tradable_mode"] = "tradable"

    valid_tradable_modes = {"tradable", "tw_limit_guard"}
    mode = str(data.get("tradable_mode", "")).strip().lower()
    if mode not in valid_tradable_modes:
        raise ValueError(
            f"data.tradable_mode must be one of {sorted(valid_tradable_modes)}, got {data.get('tradable_mode')!r}"
        )
    data["tradable_mode"] = mode
    panel_backend = str(data.get("panel_backend", "auto")).strip().lower()
    valid_panel_backends = {"auto", "pandas", "polars"}
    if panel_backend not in valid_panel_backends:
        raise ValueError(
            f"data.panel_backend must be one of {sorted(valid_panel_backends)}, got {data.get('panel_backend')!r}"
        )
    data["panel_backend"] = panel_backend
    data["panel_load_workers"] = max(0, int(data.get("panel_load_workers", 4)))
    trading.setdefault("max_turnover_ratio", 0.0)
    trading.setdefault("gross_leverage", 1.0)
    trading["gross_leverage"] = min(1.0, max(0.0, float(trading.get("gross_leverage", 1.0))))
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
            backtest_compile_stateful=training_raw["backtest_compile_stateful"],
            backtest_cpp_ext=training_raw["backtest_cpp_ext"],
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
            curve_test_interval=training_raw["curve_test_interval"],
            curve_plot_interval=training_raw["curve_plot_interval"],
            curve_plot_async=training_raw["curve_plot_async"],
            epoch_test_curve=training_raw["epoch_test_curve"],
            defer_epoch_curve_plot_until_end=training_raw["defer_epoch_curve_plot_until_end"],
            cache_train_tensors_on_gpu=training_raw["cache_train_tensors_on_gpu"],
            cache_eval_tensors_on_gpu=training_raw["cache_eval_tensors_on_gpu"],
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
            finite_check_interval_steps=training_raw["finite_check_interval_steps"],
            materialize_window_tensors=training_raw["materialize_window_tensors"],
            loss_type=training_raw["loss_type"],
            mlp=MLPModelConfig(**training_raw["mlp"]),
            ft_transformer=FTTransformerModelConfig(**training_raw["ft_transformer"]),
            tabular_resnet=TabularResNetModelConfig(**training_raw["tabular_resnet"]),
            multi_stock_tcn=MultiStockTCNModelConfig(**training_raw["multi_stock_tcn"]),
            efficient_tcn_tabular_set_portfolio=EfficientTCNTabularSetPortfolioModelConfig(
                **training_raw["efficient_tcn_tabular_set_portfolio"]
            ),
            latent_factor_market_token_portfolio=LatentFactorMarketTokenPortfolioModelConfig(
                **training_raw["latent_factor_market_token_portfolio"]
            ),
            low_rank_market_transformer_portfolio=LowRankMarketTransformerPortfolioModelConfig(
                **training_raw["low_rank_market_transformer_portfolio"]
            ),
            transformer_base_portfolio=TransformerBasePortfolioModelConfig(
                **training_raw["transformer_base_portfolio"]
            ),
            bottleneck_portfolio_autoencoder=BottleneckPortfolioAutoencoderConfig(
                **training_raw["bottleneck_portfolio_autoencoder"]
            ),
            tcn_hybrid_tabular_resnet=TCNHybridTabularResNetModelConfig(**training_raw["tcn_hybrid_tabular_resnet"]),
            temporal_tabular_resnet=TemporalTabularResNetModelConfig(**training_raw["temporal_tabular_resnet"]),
            cross_sectional_temporal_portfolio_model=CrossSectionalTemporalPortfolioModelConfig(**training_raw["cross_sectional_temporal_portfolio_model"]),
            multitask_loss=MultitaskLossConfig(**training_raw["multitask_loss"]),
            factor_generalization_loss=FactorGeneralizationLossConfig(**training_raw["factor_generalization_loss"]),
            portfolio_autoencoder_loss=PortfolioAutoencoderLossConfig(**training_raw["portfolio_autoencoder_loss"]),
            lightgbm=LightGBMModelConfig(**training_raw["lightgbm"]),
            xgboost=XGBoostModelConfig(**training_raw["xgboost"]),
        ),
        evaluation=EvaluationConfig(**raw["evaluation"]),
    )
