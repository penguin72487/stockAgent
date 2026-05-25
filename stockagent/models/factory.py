from __future__ import annotations

from torch import nn

from stockagent.config import ExperimentConfig
from stockagent.models.ft_transformer import CrossSectionalFTTransformer
from stockagent.models.mlp import CrossSectionalMLP


def _normalized_model_name(model_name: str) -> str:
    return model_name.strip().lower().replace("-", "_")


def model_hidden_dim_hint(config: ExperimentConfig) -> int:
    """Return a representative hidden width for VRAM/sample-size estimation."""
    model_name = _normalized_model_name(config.training.model_name)
    if model_name in {"mlp", "cross_sectional_mlp"}:
        return int(config.training.mlp.hidden_dim)
    if model_name in {"ft_transformer", "ft", "transformer"}:
        return int(config.training.ft_transformer.ffn_dim)
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

    raise ValueError(
        "Unsupported training.model_name='"
        f"{config.training.model_name}'. "
        "Supported values: mlp, ft_transformer"
    )
