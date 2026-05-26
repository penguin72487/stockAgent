from __future__ import annotations

from torch import nn

from stockagent.config import ExperimentConfig
from stockagent.models.ft_transformer import CrossSectionalFTTransformer
from stockagent.models.mlp import CrossSectionalMLP
from stockagent.models.tabular_resnet import CrossSectionalTabularResNet
from stockagent.models.tree_models import CrossSectionalLightGBM, CrossSectionalXGBoost


def _normalized_model_name(model_name: str) -> str:
    return model_name.strip().lower().replace("-", "_")


def model_hidden_dim_hint(config: ExperimentConfig) -> int:
    """Return a representative hidden width for VRAM/sample-size estimation."""
    model_name = _normalized_model_name(config.training.model_name)
    if model_name in {"mlp", "cross_sectional_mlp"}:
        return int(config.training.mlp.hidden_dim)
    if model_name in {"ft_transformer", "ft", "transformer"}:
        return int(config.training.ft_transformer.ffn_dim)
    if model_name in {"tabular_resnet", "tabresnet", "resnet"}:
        return int(config.training.tabular_resnet.hidden_dim)
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
        "Supported values: mlp, ft_transformer, tabular_resnet, lightgbm, xgboost"
    )
