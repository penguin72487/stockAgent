from __future__ import annotations

from typing import Callable

from stockagent.config import ExperimentConfig
from stockagent.models.base import PortfolioModel
from stockagent.models.gru import CrossSectionalGRU
from stockagent.models.mlp import CrossSectionalMLP
from stockagent.models.transformer import CrossSectionalTransformer

ModelBuilder = Callable[[ExperimentConfig, int, int], PortfolioModel]


def _build_mlp(config: ExperimentConfig, num_features: int, num_symbols: int) -> PortfolioModel:
    return CrossSectionalMLP(
        lookback=config.training.lookback,
        num_features=num_features,
        num_symbols=num_symbols,
        hidden_dim=config.training.hidden_dim,
        dropout=config.training.dropout,
        num_layers=config.training.num_layers,
        residual_norm=config.training.residual_norm,
    )


def _build_transformer(config: ExperimentConfig, num_features: int, num_symbols: int) -> PortfolioModel:
    return CrossSectionalTransformer(
        lookback=config.training.lookback,
        num_features=num_features,
        num_symbols=num_symbols,
        hidden_dim=config.training.hidden_dim,
        dropout=config.training.dropout,
        num_layers=config.training.num_layers,
        residual_norm=config.training.residual_norm,
    )


def _build_gru(config: ExperimentConfig, num_features: int, num_symbols: int) -> PortfolioModel:
    return CrossSectionalGRU(
        lookback=config.training.lookback,
        num_features=num_features,
        num_symbols=num_symbols,
        hidden_dim=config.training.hidden_dim,
        dropout=config.training.dropout,
        num_layers=config.training.num_layers,
        residual_norm=config.training.residual_norm,
    )


MODEL_REGISTRY: dict[str, ModelBuilder] = {
    "gru": _build_gru,
    "mlp": _build_mlp,
    "transformer": _build_transformer,
}


def register_model(name: str, builder: ModelBuilder) -> None:
    key = name.strip().lower()
    if not key:
        raise ValueError("Model name cannot be empty.")
    MODEL_REGISTRY[key] = builder


def build_model(config: ExperimentConfig, num_features: int, num_symbols: int) -> PortfolioModel:
    model_name = config.training.model_name.strip().lower()
    builder = MODEL_REGISTRY.get(model_name)
    if builder is None:
        available = ", ".join(sorted(MODEL_REGISTRY))
        raise ValueError(f"Unknown model '{config.training.model_name}'. Available: {available}")
    return builder(config, num_features, num_symbols)
