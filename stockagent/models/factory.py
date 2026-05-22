from __future__ import annotations

from typing import Any

from torch import nn

from stockagent.models.mlp import CrossSectionalMLP
from stockagent.models.portfolio_transformer import PortfolioTransformerModel
from stockagent.models.temporal_cross_asset import TemporalCrossAssetModel


def build_model(
    *,
    model_name: str,
    num_features: int,
    num_symbols: int,
    long_only: bool,
    model_params: dict[str, Any] | None,
) -> nn.Module:
    params = dict(model_params or {})
    key = model_name.strip().lower()

    if key == "mlp":
        return CrossSectionalMLP(
            num_features=num_features,
            num_symbols=num_symbols,
            long_only=long_only,
            hidden_dim=int(params.get("hidden_dim", 512)),
            hidden_layers=int(params.get("hidden_layers", 2)),
            dropout=float(params.get("dropout", 0.1)),
            embedding_dim=int(params.get("embedding_dim", 64)),
            lookback=int(params.get("lookback", 1)),
        )

    if key in {"temporal_cross_asset", "cross_asset_transformer"}:
        return TemporalCrossAssetModel(
            num_features=num_features,
            num_symbols=num_symbols,
            long_only=long_only,
            lookback=int(params.get("lookback", 20)),
            d_model=int(params.get("d_model", 128)),
            temporal_encoder=str(params.get("temporal_encoder", "gru")),
            temporal_layers=int(params.get("temporal_layers", 2)),
            temporal_nhead=int(params.get("temporal_nhead", 4)),
            temporal_ff_dim=int(params.get("temporal_ff_dim", 256)),
            tcn_kernel_size=int(params.get("tcn_kernel_size", 3)),
            cross_asset_encoder=str(params.get("cross_asset_encoder", "transformer")),
            cross_asset_layers=int(params.get("cross_asset_layers", 2)),
            cross_asset_nhead=int(params.get("cross_asset_nhead", 4)),
            cross_asset_ff_dim=int(params.get("cross_asset_ff_dim", 256)),
            set_inducing_points=int(params.get("set_inducing_points", 16)),
            dropout=float(params.get("dropout", 0.1)),
        )

    if key in {"portfolio_transformer", "portfolio_tf"}:
        return PortfolioTransformerModel(
            num_features=num_features,
            num_symbols=num_symbols,
            long_only=long_only,
            lookback=int(params.get("lookback", 20)),
            d_model=int(params.get("d_model", 128)),
            time_layers=int(params.get("time_layers", 2)),
            time_nhead=int(params.get("time_nhead", 4)),
            time_ff_dim=int(params.get("time_ff_dim", 256)),
            cross_layers=int(params.get("cross_layers", 2)),
            cross_nhead=int(params.get("cross_nhead", 4)),
            cross_ff_dim=int(params.get("cross_ff_dim", 256)),
            decoder_queries=int(params.get("decoder_queries", 4)),
            max_time_batch=int(params.get("max_time_batch", 32768)),
            attention_backend=str(params.get("attention_backend", "auto")),
            use_transformer_engine=bool(params.get("use_transformer_engine", False)),
            use_fp8=bool(params.get("use_fp8", False)),
            dropout=float(params.get("dropout", 0.1)),
        )

    raise ValueError(f"Unsupported model name: {model_name}")
