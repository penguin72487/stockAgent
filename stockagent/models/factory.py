from __future__ import annotations

from torch import nn

from stockagent.config import ExperimentConfig
from stockagent.models.bottleneck_portfolio_autoencoder import BottleneckPortfolioAutoencoder
from stockagent.models.cross_sectional_temporal_portfolio_model import CrossSectionalTemporalPortfolioModel
from stockagent.models.efficient_tcn_tabular_set_portfolio import EfficientTCNTabularSetPortfolioModel
from stockagent.models.ft_transformer import CrossSectionalFTTransformer
from stockagent.models.latent_factor_market_token_portfolio import LatentFactorMarketTokenPortfolioModel
from stockagent.models.low_rank_market_transformer_portfolio import LowRankMarketTransformerPortfolioModel
from stockagent.models.mlp import CrossSectionalMLP
from stockagent.models.multi_stock_tcn import CrossSectionalMultiStockTCN
from stockagent.models.tabular_resnet import CrossSectionalTabularResNet
from stockagent.models.tcn_hybrid_tabular_resnet import CrossSectionalTCNHybridTabularResNet
from stockagent.models.temporal_tabular_resnet import CrossSectionalTemporalTabularResNet
from stockagent.models.transformer_base_portfolio import TransformerBasePortfolioModel
from stockagent.models.tree_models import CrossSectionalLightGBM, CrossSectionalXGBoost


def _normalized_model_name(model_name: str) -> str:
    return model_name.strip().lower().replace("-", "_")


_EFFICIENT_TCN_TABULAR_SET_NAMES = {
    "efficient_tcn_tabular_set_portfolio",
    "efficient_tcn_tabular_set_portfolio_model",
    "efficient_portfolio",
    "lite_isab_portfolio",
}

_BOTTLENECK_PORTFOLIO_AUTOENCODER_NAMES = {
    "bottleneck_portfolio_autoencoder",
    "bottleneck_autoencoder",
    "portfolio_autoencoder",
    "bpae",
}

_LATENT_FACTOR_MARKET_TOKEN_NAMES = {
    "latent_factor_market_token_portfolio",
    "latent_factor_market_token_portfolio_model",
    "latent_factor_market_token",
    "lfmt_portfolio",
    "latent_market_token_portfolio",
}

_LOW_RANK_MARKET_TRANSFORMER_NAMES = {
    "low_rank_market_transformer_portfolio",
    "low_rank_market_transformer_portfolio_model",
    "temporal_latent_factor_market_transformer_portfolio",
    "factorized_market_transformer_portfolio",
    "market_transformer_portfolio",
    "lrmt_portfolio",
}

_TRANSFORMER_BASE_PORTFOLIO_NAMES = {
    "transformer_base_portfolio",
    "transformer_base_portfolio_model",
    "flash_transformer_portfolio",
    "scalable_transformer_portfolio",
    "multi_axis_transformer_portfolio",
    "tbp",
}


def model_hidden_dim_hint(config: ExperimentConfig) -> int:
    """Return a representative hidden width for VRAM/sample-size estimation."""
    model_name = _normalized_model_name(config.training.model_name)
    if model_name in {"mlp", "cross_sectional_mlp"}:
        return int(config.training.mlp.hidden_dim)
    if model_name in {"ft_transformer", "ft", "transformer"}:
        return int(config.training.ft_transformer.ffn_dim)
    if model_name in {"tabular_resnet", "tabresnet", "resnet"}:
        return int(config.training.tabular_resnet.hidden_dim)
    if model_name in {"multi_stock_tcn", "simple_multi_stock_tcn", "mean_pool_tcn"}:
        return int(config.training.multi_stock_tcn.embedding_dim)
    if model_name in _EFFICIENT_TCN_TABULAR_SET_NAMES:
        return int(config.training.efficient_tcn_tabular_set_portfolio.model_dim)
    if model_name in _LATENT_FACTOR_MARKET_TOKEN_NAMES:
        return int(config.training.latent_factor_market_token_portfolio.stock_embedding_dim)
    if model_name in _LOW_RANK_MARKET_TRANSFORMER_NAMES:
        return int(config.training.low_rank_market_transformer_portfolio.stock_embedding_dim)
    if model_name in _TRANSFORMER_BASE_PORTFOLIO_NAMES:
        return int(config.training.transformer_base_portfolio.d_model)
    if model_name in _BOTTLENECK_PORTFOLIO_AUTOENCODER_NAMES:
        return int(config.training.bottleneck_portfolio_autoencoder.d_model)
    if model_name in {"tcn_hybrid_tabular_resnet", "tcn_hybrid", "tcn_tabresnet"}:
        return int(config.training.tcn_hybrid_tabular_resnet.embedding_dim)
    if model_name in {"cross_sectional_temporal_portfolio_model", "portfolio_multitask", "cstpm"}:
        cstpm_cfg = config.training.cross_sectional_temporal_portfolio_model
        return int(getattr(cstpm_cfg, "d_model", cstpm_cfg.cross_hidden_dim))
    if model_name in {"temporal_tabular_resnet", "temporal_resnet", "temporal_tabresnet"}:
        return int(config.training.temporal_tabular_resnet.hidden_dim)
    if model_name in {"lightgbm", "lgbm"}:
        return 128
    if model_name in {"xgboost", "xgb"}:
        return 128
    raise ValueError(f"Unsupported model_name='{config.training.model_name}'")


def build_model(
    *,
    config: ExperimentConfig,
    lookback: int,
    num_features: int,
    num_symbols: int,
) -> nn.Module:
    model_name = _normalized_model_name(config.training.model_name)

    if model_name in {"mlp", "cross_sectional_mlp"}:
        mlp_cfg = config.training.mlp
        return CrossSectionalMLP(
            lookback=lookback,
            num_features=num_features,
            num_symbols=num_symbols,
            hidden_dim=mlp_cfg.hidden_dim,
            dropout=mlp_cfg.dropout,
            embedding_dim=mlp_cfg.embedding_dim,
            hidden_layers=mlp_cfg.hidden_layers,
            long_only=config.trading.long_only,
        )

    if model_name in {"ft_transformer", "ft", "transformer"}:
        ft_cfg = config.training.ft_transformer
        return CrossSectionalFTTransformer(
            lookback=lookback,
            num_features=num_features,
            num_symbols=num_symbols,
            d_token=ft_cfg.d_token,
            n_heads=ft_cfg.n_heads,
            n_layers=ft_cfg.n_layers,
            ffn_dim=ft_cfg.ffn_dim,
            dropout=ft_cfg.dropout,
            long_only=config.trading.long_only,
            use_cls_token=ft_cfg.use_cls_token,
        )

    if model_name in {"tabular_resnet", "tabresnet", "resnet"}:
        tab_cfg = config.training.tabular_resnet
        return CrossSectionalTabularResNet(
            lookback=lookback,
            num_features=num_features,
            num_symbols=num_symbols,
            embedding_dim=tab_cfg.embedding_dim,
            hidden_dim=tab_cfg.hidden_dim,
            n_blocks=tab_cfg.n_blocks,
            dropout=tab_cfg.dropout,
            long_only=config.trading.long_only,
            runtime_shape_check=config.training.runtime_shape_check,
            allow_dynamic_symbols=config.training.allow_dynamic_symbols,
        )

    if model_name in {"multi_stock_tcn", "simple_multi_stock_tcn", "mean_pool_tcn"}:
        tcn_cfg = config.training.multi_stock_tcn
        return CrossSectionalMultiStockTCN(
            lookback=lookback,
            num_features=num_features,
            num_symbols=num_symbols,
            hidden_channels=tcn_cfg.hidden_channels,
            embedding_dim=tcn_cfg.embedding_dim,
            tcn_blocks=tcn_cfg.tcn_blocks,
            tcn_kernel_size=tcn_cfg.tcn_kernel_size,
            head_hidden_dim=tcn_cfg.head_hidden_dim,
            head_layers=tcn_cfg.head_layers,
            dropout=tcn_cfg.dropout,
            tcn_conv_mode=tcn_cfg.tcn_conv_mode,
            conv_layers_per_block=tcn_cfg.conv_layers_per_block,
            norm_type=tcn_cfg.norm_type,
            sanitize_inputs=tcn_cfg.sanitize_inputs,
            long_only=config.trading.long_only,
            runtime_shape_check=config.training.runtime_shape_check,
            allow_dynamic_symbols=config.training.allow_dynamic_symbols,
        )

    if model_name in _EFFICIENT_TCN_TABULAR_SET_NAMES:
        efficient_cfg = config.training.efficient_tcn_tabular_set_portfolio
        portfolio_mode = str(efficient_cfg.portfolio_mode).strip().lower().replace("-", "_")
        if portfolio_mode in {"", "auto"}:
            portfolio_mode = "long_only" if config.trading.long_only else "long_short"
        return EfficientTCNTabularSetPortfolioModel(
            lookback=lookback,
            num_features=num_features,
            num_symbols=num_symbols,
            temporal_enabled=efficient_cfg.temporal_enabled,
            temporal_dim=efficient_cfg.temporal_dim,
            temporal_hidden_channels=efficient_cfg.temporal_hidden_channels,
            temporal_dilations=efficient_cfg.temporal_dilations,
            temporal_kernel_size=efficient_cfg.temporal_kernel_size,
            tabular_dim=efficient_cfg.tabular_dim,
            tabular_hidden_dim=efficient_cfg.tabular_hidden_dim,
            tabular_blocks=efficient_cfg.tabular_blocks,
            model_dim=efficient_cfg.model_dim,
            set_enabled=efficient_cfg.set_enabled,
            num_inducing_points=efficient_cfg.num_inducing_points,
            num_heads=efficient_cfg.num_heads,
            ffn_mult=efficient_cfg.ffn_mult,
            head_hidden_dim=efficient_cfg.head_hidden_dim,
            head_layers=efficient_cfg.head_layers,
            dropout=efficient_cfg.dropout,
            residual_scale=efficient_cfg.residual_scale,
            default_temperature=efficient_cfg.default_temperature,
            portfolio_mode=portfolio_mode,
            return_aux=efficient_cfg.return_aux,
            runtime_shape_check=config.training.runtime_shape_check,
            allow_dynamic_symbols=config.training.allow_dynamic_symbols,
        )

    if model_name in _LATENT_FACTOR_MARKET_TOKEN_NAMES:
        lfmt_cfg = config.training.latent_factor_market_token_portfolio
        portfolio_mode = str(lfmt_cfg.portfolio_mode).strip().lower().replace("-", "_")
        if portfolio_mode in {"", "auto"}:
            portfolio_mode = "long_only" if config.trading.long_only else "long_short"
        return LatentFactorMarketTokenPortfolioModel(
            lookback=lookback,
            num_features=num_features,
            num_symbols=num_symbols,
            temporal_enabled=lfmt_cfg.temporal_enabled,
            temporal_dim=lfmt_cfg.temporal_dim,
            temporal_hidden_channels=lfmt_cfg.temporal_hidden_channels,
            temporal_dilations=lfmt_cfg.temporal_dilations,
            temporal_kernel_size=lfmt_cfg.temporal_kernel_size,
            tabular_dim=lfmt_cfg.tabular_dim,
            tabular_hidden_dim=lfmt_cfg.tabular_hidden_dim,
            tabular_blocks=lfmt_cfg.tabular_blocks,
            stock_embedding_dim=lfmt_cfg.stock_embedding_dim,
            num_latent_factors=lfmt_cfg.num_latent_factors,
            num_market_tokens=lfmt_cfg.num_market_tokens,
            num_heads=lfmt_cfg.num_heads,
            ffn_mult=lfmt_cfg.ffn_mult,
            head_hidden_dim=lfmt_cfg.head_hidden_dim,
            head_layers=lfmt_cfg.head_layers,
            dropout=lfmt_cfg.dropout,
            residual_scale=lfmt_cfg.residual_scale,
            default_temperature=lfmt_cfg.default_temperature,
            portfolio_mode=portfolio_mode,
            return_aux=lfmt_cfg.return_aux,
            runtime_shape_check=config.training.runtime_shape_check,
            allow_dynamic_symbols=config.training.allow_dynamic_symbols,
        )

    if model_name in _LOW_RANK_MARKET_TRANSFORMER_NAMES:
        lrmt_cfg = config.training.low_rank_market_transformer_portfolio
        portfolio_mode = str(lrmt_cfg.portfolio_mode).strip().lower().replace("-", "_")
        if portfolio_mode in {"", "auto"}:
            portfolio_mode = "long_only" if config.trading.long_only else "long_short"
        return LowRankMarketTransformerPortfolioModel(
            lookback=lookback,
            num_features=num_features,
            num_symbols=num_symbols,
            feature_dim=lrmt_cfg.feature_dim,
            temporal_mixer=lrmt_cfg.temporal_mixer,
            temporal_layers=lrmt_cfg.temporal_layers,
            temporal_heads=lrmt_cfg.temporal_heads,
            temporal_ffn_dim=lrmt_cfg.temporal_ffn_dim,
            temporal_dropout=lrmt_cfg.temporal_dropout,
            temporal_pooling=lrmt_cfg.temporal_pooling,
            temporal_kernel_size=lrmt_cfg.temporal_kernel_size,
            temporal_dilations=lrmt_cfg.temporal_dilations,
            temporal_checkpoint=lrmt_cfg.temporal_checkpoint,
            stock_embedding_dim=lrmt_cfg.stock_embedding_dim,
            num_latent_factors=lrmt_cfg.num_latent_factors,
            num_market_tokens=lrmt_cfg.num_market_tokens,
            cross_heads=lrmt_cfg.cross_heads,
            cross_ffn_mult=lrmt_cfg.cross_ffn_mult,
            head_hidden_dim=lrmt_cfg.head_hidden_dim,
            head_layers=lrmt_cfg.head_layers,
            dropout=lrmt_cfg.dropout,
            default_temperature=lrmt_cfg.default_temperature,
            portfolio_mode=portfolio_mode,
            return_aux=lrmt_cfg.return_aux,
            return_aux_details=lrmt_cfg.return_aux_details,
            runtime_shape_check=config.training.runtime_shape_check,
            allow_dynamic_symbols=config.training.allow_dynamic_symbols,
        )

    if model_name in _TRANSFORMER_BASE_PORTFOLIO_NAMES:
        tbp_cfg = config.training.transformer_base_portfolio
        portfolio_mode = str(tbp_cfg.portfolio_mode).strip().lower().replace("-", "_")
        if portfolio_mode in {"", "auto"}:
            portfolio_mode = "long_only" if config.trading.long_only else "long_short"
        return TransformerBasePortfolioModel(
            lookback=lookback,
            num_features=num_features,
            num_symbols=num_symbols,
            d_model=tbp_cfg.d_model,
            attention_mode=tbp_cfg.attention_mode,
            use_flash_attention=tbp_cfg.use_flash_attention,
            use_time_pos=tbp_cfg.use_time_pos,
            use_symbol_pos=tbp_cfg.use_symbol_pos,
            input_dropout=tbp_cfg.input_dropout,
            sdpa_batch_limit=tbp_cfg.sdpa_batch_limit,
            norm_type=tbp_cfg.norm_type,
            ffn_type=tbp_cfg.ffn_type,
            qk_norm=tbp_cfg.qk_norm,
            rope_temporal=tbp_cfg.rope_temporal,
            rope_base=tbp_cfg.rope_base,
            temporal_layers=tbp_cfg.temporal_layers,
            temporal_heads=tbp_cfg.temporal_heads,
            temporal_ffn_mult=tbp_cfg.temporal_ffn_mult,
            temporal_pooling=tbp_cfg.temporal_pooling,
            cross_layers=tbp_cfg.cross_layers,
            cross_heads=tbp_cfg.cross_heads,
            cross_ffn_mult=tbp_cfg.cross_ffn_mult,
            joint_layers=tbp_cfg.joint_layers,
            joint_heads=tbp_cfg.joint_heads,
            joint_ffn_mult=tbp_cfg.joint_ffn_mult,
            latent_layers=tbp_cfg.latent_layers,
            num_latent_factors=tbp_cfg.num_latent_factors,
            num_market_tokens=tbp_cfg.num_market_tokens,
            market_layers=tbp_cfg.market_layers,
            dynamic_latent_tokens=tbp_cfg.dynamic_latent_tokens,
            dynamic_market_tokens=tbp_cfg.dynamic_market_tokens,
            dynamic_token_hidden_mult=tbp_cfg.dynamic_token_hidden_mult,
            dynamic_token_gate_init=tbp_cfg.dynamic_token_gate_init,
            dynamic_token_dropout=tbp_cfg.dynamic_token_dropout,
            head_hidden_dim=tbp_cfg.head_hidden_dim,
            head_layers=tbp_cfg.head_layers,
            dropout=tbp_cfg.dropout,
            default_temperature=tbp_cfg.default_temperature,
            portfolio_mode=portfolio_mode,
            max_full_tokens=tbp_cfg.max_full_tokens,
            checkpoint_blocks=tbp_cfg.checkpoint_blocks,
            return_aux=tbp_cfg.return_aux,
            return_aux_details=tbp_cfg.return_aux_details,
            runtime_shape_check=config.training.runtime_shape_check,
            allow_dynamic_symbols=config.training.allow_dynamic_symbols,
        )

    if model_name in _BOTTLENECK_PORTFOLIO_AUTOENCODER_NAMES:
        bpae_cfg = config.training.bottleneck_portfolio_autoencoder
        return BottleneckPortfolioAutoencoder(
            lookback=lookback,
            num_features=num_features,
            num_symbols=num_symbols,
            d_model=bpae_cfg.d_model,
            z_dim=bpae_cfg.z_dim,
            temporal_type=bpae_cfg.temporal_type,
            temporal_layers=bpae_cfg.temporal_layers,
            asset_encoder_type=bpae_cfg.asset_encoder_type,
            asset_encoder_layers=bpae_cfg.asset_encoder_layers,
            n_heads=bpae_cfg.n_heads,
            num_inducing_points=bpae_cfg.num_inducing_points,
            ffn_mult=bpae_cfg.ffn_mult,
            dropout=bpae_cfg.dropout,
            long_short=bpae_cfg.long_short if not config.trading.long_only else False,
            noise_std=bpae_cfg.noise_std,
            return_aux=bpae_cfg.return_aux,
            runtime_shape_check=config.training.runtime_shape_check,
            allow_dynamic_symbols=config.training.allow_dynamic_symbols,
        )

    if model_name in {"tcn_hybrid_tabular_resnet", "tcn_hybrid", "tcn_tabresnet"}:
        tcn_cfg = config.training.tcn_hybrid_tabular_resnet
        return CrossSectionalTCNHybridTabularResNet(
            lookback=lookback,
            num_features=num_features,
            num_symbols=num_symbols,
            embedding_dim=tcn_cfg.embedding_dim,
            encoder_hidden_dim=tcn_cfg.encoder_hidden_dim,
            encoder_blocks=tcn_cfg.encoder_blocks,
            tcn_blocks=tcn_cfg.tcn_blocks,
            tcn_kernel_size=tcn_cfg.tcn_kernel_size,
            dropout=tcn_cfg.dropout,
            long_only=config.trading.long_only,
            runtime_shape_check=config.training.runtime_shape_check,
            allow_dynamic_symbols=config.training.allow_dynamic_symbols,
        )

    if model_name in {"temporal_tabular_resnet", "temporal_resnet", "temporal_tabresnet"}:
        ttab_cfg = config.training.temporal_tabular_resnet
        return CrossSectionalTemporalTabularResNet(
            lookback=lookback,
            num_features=num_features,
            num_symbols=num_symbols,
            temporal_hidden_dim=ttab_cfg.temporal_hidden_dim,
            temporal_layers=ttab_cfg.temporal_layers,
            temporal_dropout=ttab_cfg.temporal_dropout,
            embedding_dim=ttab_cfg.embedding_dim,
            hidden_dim=ttab_cfg.hidden_dim,
            n_blocks=ttab_cfg.n_blocks,
            dropout=ttab_cfg.dropout,
            long_only=config.trading.long_only,
            runtime_shape_check=config.training.runtime_shape_check,
            allow_dynamic_symbols=config.training.allow_dynamic_symbols,
        )

    if model_name in {"cross_sectional_temporal_portfolio_model", "portfolio_multitask", "cstpm"}:
        cstpm_cfg = config.training.cross_sectional_temporal_portfolio_model
        return CrossSectionalTemporalPortfolioModel(
            lookback=lookback,
            num_features=num_features,
            num_symbols=num_symbols,
            stock_embedding_dim=int(getattr(cstpm_cfg, "d_model", cstpm_cfg.stock_embedding_dim)),
            stock_hidden_dim=int(getattr(cstpm_cfg, "scorer_hidden", cstpm_cfg.stock_hidden_dim)),
            stock_n_blocks=int(getattr(cstpm_cfg, "scorer_blocks", cstpm_cfg.stock_n_blocks)),
            temporal_hidden_dim=cstpm_cfg.temporal_hidden_dim,
            temporal_blocks=cstpm_cfg.temporal_blocks,
            temporal_kernel_size=cstpm_cfg.temporal_kernel_size,
            cross_hidden_dim=int(getattr(cstpm_cfg, "d_model", cstpm_cfg.cross_hidden_dim)),
            cross_heads=int(getattr(cstpm_cfg, "heads", cstpm_cfg.cross_heads)),
            cross_layers=int(getattr(cstpm_cfg, "layers", cstpm_cfg.cross_layers)),
            dropout=cstpm_cfg.dropout,
            regime_classes=cstpm_cfg.regime_classes,
            long_only=config.trading.long_only,
            runtime_shape_check=config.training.runtime_shape_check,
            allow_dynamic_symbols=config.training.allow_dynamic_symbols,
            candidate_top_m=int(getattr(cstpm_cfg, "candidate_k", cstpm_cfg.candidate_top_m)),
            portfolio_top_k=int(getattr(cstpm_cfg, "trade_k", cstpm_cfg.portfolio_top_k)),
        )

    if model_name in {"lightgbm", "lgbm"}:
        lgbm_cfg = config.training.lightgbm
        return CrossSectionalLightGBM(
            lookback=lookback,
            num_features=num_features,
            num_symbols=num_symbols,
            long_only=config.trading.long_only,
            use_gpu=lgbm_cfg.use_gpu,
            gpu_device_id=lgbm_cfg.gpu_device_id,
            n_estimators=lgbm_cfg.n_estimators,
            num_leaves=lgbm_cfg.num_leaves,
            max_depth=lgbm_cfg.max_depth,
            learning_rate=lgbm_cfg.learning_rate,
            subsample=lgbm_cfg.subsample,
            colsample_bytree=lgbm_cfg.colsample_bytree,
            reg_lambda=lgbm_cfg.reg_lambda,
            n_jobs=lgbm_cfg.n_jobs,
            random_state=lgbm_cfg.random_state,
        )

    if model_name in {"xgboost", "xgb"}:
        xgb_cfg = config.training.xgboost
        return CrossSectionalXGBoost(
            lookback=lookback,
            num_features=num_features,
            num_symbols=num_symbols,
            long_only=config.trading.long_only,
            use_gpu=xgb_cfg.use_gpu,
            gpu_device_id=xgb_cfg.gpu_device_id,
            n_estimators=xgb_cfg.n_estimators,
            max_depth=xgb_cfg.max_depth,
            learning_rate=xgb_cfg.learning_rate,
            subsample=xgb_cfg.subsample,
            colsample_bytree=xgb_cfg.colsample_bytree,
            reg_lambda=xgb_cfg.reg_lambda,
            n_jobs=xgb_cfg.n_jobs,
            random_state=xgb_cfg.random_state,
        )

    raise ValueError(
        "Unsupported training.model_name='"
        f"{config.training.model_name}'. "
        "Supported values: mlp, ft_transformer, tabular_resnet, multi_stock_tcn, "
        "efficient_tcn_tabular_set_portfolio, tcn_hybrid_tabular_resnet, "
        "latent_factor_market_token_portfolio, low_rank_market_transformer_portfolio, "
        "transformer_base_portfolio, "
        "bottleneck_portfolio_autoencoder, temporal_tabular_resnet, "
        "cross_sectional_temporal_portfolio_model, lightgbm, xgboost"
    )
