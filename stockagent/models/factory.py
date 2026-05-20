from __future__ import annotations

from typing import Callable

from stockagent.config import ExperimentConfig
from stockagent.models.base import PortfolioModel
from stockagent.models.mlp import CrossSectionalMLP

ModelBuilder = Callable[[ExperimentConfig, int, int], PortfolioModel]


def _build_mlp(config: ExperimentConfig, num_features: int, num_symbols: int) -> PortfolioModel:
    return CrossSectionalMLP(
        lookback=config.training.lookback,
        num_features=num_features,
        num_symbols=num_symbols,
        hidden_dim=config.training.hidden_dim,
        dropout=config.training.dropout,
    )


MODEL_REGISTRY: dict[str, ModelBuilder] = {
    "mlp": _build_mlp,
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
