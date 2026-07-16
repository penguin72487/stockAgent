from __future__ import annotations

from collections.abc import Sequence

import torch
from torch import nn

from stockagent.models.transformer_base_portfolio import (
    GatedProjection,
    TransformerBasePortfolioModel,
    _make_norm,
)


class CandleEncoder(nn.Module):
    """Jointly encode every candle feature into one abstract model vector."""

    def __init__(
        self,
        *,
        num_features: int,
        d_model: int,
        dropout: float,
        norm_type: str,
        ffn_type: str,
        sanitize_inputs: bool,
        categorical_feature_indices: Sequence[int] | None = None,
        categorical_embedding_dim: int = 4,
        categorical_embedding_cardinality: int = 512,
    ) -> None:
        super().__init__()
        self.num_features = int(num_features)
        self.d_model = int(d_model)
        self.sanitize_inputs = bool(sanitize_inputs)
        self.categorical_embedding_dim = max(1, int(categorical_embedding_dim))
        self.categorical_embedding_cardinality = max(
            2, int(categorical_embedding_cardinality)
        )

        categorical_indices = tuple(int(idx) for idx in (categorical_feature_indices or ()))
        categorical_index_set = set(categorical_indices)
        continuous_indices = tuple(
            idx for idx in range(self.num_features) if idx not in categorical_index_set
        )
        self.categorical_feature_indices = categorical_indices
        self.register_buffer(
            "categorical_feature_index_tensor",
            torch.tensor(categorical_indices, dtype=torch.long),
            persistent=False,
        )
        self.register_buffer(
            "continuous_feature_index_tensor",
            torch.tensor(continuous_indices, dtype=torch.long),
            persistent=False,
        )
        self.categorical_embeddings = nn.ModuleList(
            [
                nn.Embedding(
                    self.categorical_embedding_cardinality + 1,
                    self.categorical_embedding_dim,
                )
                for _ in categorical_indices
            ]
        )

        self.joint_input_dim = len(continuous_indices) + (
            len(categorical_indices) * self.categorical_embedding_dim
        )
        if self.joint_input_dim <= 0:
            raise ValueError("CandleEncoder requires at least one input feature")

        self.input_norm = _make_norm(self.joint_input_dim, norm_type)
        self.joint_projection = GatedProjection(
            self.joint_input_dim,
            self.d_model,
            float(dropout),
            ffn_type,
        )
        self.candle_query = nn.Parameter(torch.randn(1, self.d_model) * 0.02)
        self.output_norm = _make_norm(self.d_model, norm_type)

    def _joint_features(self, x: torch.Tensor) -> torch.Tensor:
        model_dtype = self.joint_projection.proj.weight.dtype
        model_device = self.joint_projection.proj.weight.device
        clean_fp32 = x.to(device=model_device, dtype=torch.float32)
        if self.sanitize_inputs:
            clean_fp32 = torch.nan_to_num(clean_fp32, nan=0.0, posinf=0.0, neginf=0.0)

        continuous = clean_fp32.index_select(
            -1, self.continuous_feature_index_tensor
        ).to(dtype=model_dtype)
        if not self.categorical_feature_indices:
            return continuous

        categorical_values = clean_fp32.index_select(
            -1, self.categorical_feature_index_tensor
        )
        categorical_ids = (
            torch.round(categorical_values)
            .to(dtype=torch.long)
            .clamp_(0, self.categorical_embedding_cardinality)
        )
        categorical = torch.cat(
            [
                embedding(categorical_ids[..., idx])
                for idx, embedding in enumerate(self.categorical_embeddings)
            ],
            dim=-1,
        ).to(dtype=model_dtype)
        return torch.cat((continuous, categorical), dim=-1)

    def forward(
        self,
        x: torch.Tensor,
        *,
        return_aux: bool = False,
    ) -> tuple[torch.Tensor, dict[str, torch.Tensor]]:
        joint_features = self._joint_features(x)
        projected = self.joint_projection(self.input_norm(joint_features))
        candle_query = self.candle_query.to(dtype=projected.dtype)
        embedding = self.output_norm(projected + candle_query)
        if not return_aux:
            return embedding, {}
        return embedding, {
            "candle_tokens": embedding.unsqueeze(-2),
            "candle_token_weights": embedding.new_ones(*embedding.shape[:-1], 1),
            "candle_embedding": embedding,
        }


class FinancialTransformerModel(TransformerBasePortfolioModel):
    """Transformer whose raw-feature stem is a learned joint Candle Encoder."""

    def __init__(
        self,
        *args,
        candle_dropout: float = 0.0,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.candle_encoder = CandleEncoder(
            num_features=self.num_features,
            d_model=self.d_model,
            dropout=candle_dropout,
            norm_type=self.norm_type,
            ffn_type=self.ffn_type,
            sanitize_inputs=self.sanitize_inputs,
            categorical_feature_indices=self.categorical_feature_indices,
            categorical_embedding_dim=self.categorical_embedding_dim,
            categorical_embedding_cardinality=self.categorical_embedding_cardinality,
        )

        # CandleEncoder replaces the inherited F -> D projection completely.
        # Removing these modules also prevents unused parameters entering the
        # optimizer or checkpoint.
        del self.feature_proj
        del self.categorical_embeddings
        del self.categorical_proj
        del self.categorical_feature_index_tensor

    def _candle_project_features(
        self,
        x: torch.Tensor,
        *,
        return_token_aux: bool,
    ) -> tuple[torch.Tensor, dict[str, torch.Tensor]]:
        return self.candle_encoder(x, return_aux=return_token_aux)

    def _project_features(self, x: torch.Tensor) -> torch.Tensor:
        projected, _aux = self._candle_project_features(x, return_token_aux=False)
        return projected

    @staticmethod
    def _attach_candle_aux(output, token_aux: dict[str, torch.Tensor], return_aux: bool | None):
        if not token_aux:
            return output
        if return_aux is True and isinstance(output, tuple) and len(output) == 3:
            weights, scores, aux = output
            merged = dict(aux)
            merged.update(token_aux)
            return weights, scores, merged
        if return_aux is None and isinstance(output, dict):
            output = dict(output)
            if "aux" in output and isinstance(output["aux"], dict):
                aux = dict(output["aux"])
                aux.update(token_aux)
                output["aux"] = aux
                output.update(token_aux)
            return output
        return output

    def forward(
        self,
        x: torch.Tensor,
        mask: torch.Tensor | None = None,
        temperature: float | torch.Tensor | None = None,
        return_aux: bool | None = None,
        symbol_indices: torch.Tensor | None = None,
    ):
        self._check_shapes(x, mask, symbol_indices)
        if mask is None:
            mask_bool = torch.ones(x.size(0), x.size(2), dtype=torch.bool, device=x.device)
        else:
            mask_bool = mask.to(device=x.device, dtype=torch.bool)
        collect_token_aux = bool(
            return_aux is True or (return_aux is None and self.return_aux and self.return_aux_details)
        )
        h, token_aux = self._candle_project_features(
            x,
            return_token_aux=collect_token_aux,
        )
        h = self._add_window_positions(h, int(x.size(2)), symbol_indices)
        output = self._forward_embedded(
            h,
            mask_bool,
            temperature=temperature,
            return_aux=return_aux,
        )
        return self._attach_candle_aux(output, token_aux, return_aux)
