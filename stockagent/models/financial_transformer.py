from __future__ import annotations

from collections.abc import Sequence
from copy import deepcopy

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
        daily_context_num_features: int = 0,
        daily_context_categorical_feature_indices: Sequence[int] | None = None,
        daily_context_lookback: int = 1,
        daily_context_layers: int = 0,
        daily_context_pooling: str = "last",
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

        self.daily_context_num_features = max(0, int(daily_context_num_features))
        self.daily_context_lookback = max(1, int(daily_context_lookback))
        self.daily_context_layers = max(0, int(daily_context_layers))
        self.daily_context_pooling = self._normalize_pooling(daily_context_pooling)
        if self.daily_context_lookback > self.lookback:
            raise ValueError(
                "daily_context_lookback cannot exceed the model lookback; "
                f"got daily={self.daily_context_lookback} model={self.lookback}"
            )
        if self.daily_context_layers > len(self.temporal_blocks):
            raise ValueError(
                "daily_context_layers cannot exceed temporal_layers; "
                f"got daily={self.daily_context_layers} "
                f"temporal={len(self.temporal_blocks)}"
            )
        if self.daily_context_num_features > 0:
            self.daily_context_encoder: CandleEncoder | None = CandleEncoder(
                num_features=self.daily_context_num_features,
                d_model=self.d_model,
                dropout=candle_dropout,
                norm_type=self.norm_type,
                ffn_type=self.ffn_type,
                sanitize_inputs=self.sanitize_inputs,
                categorical_feature_indices=(
                    daily_context_categorical_feature_indices or ()
                ),
                categorical_embedding_dim=self.categorical_embedding_dim,
                categorical_embedding_cardinality=(
                    self.categorical_embedding_cardinality
                ),
            )
            # A separate encoder preserves the ordinary daily feature contract
            # without copying its wide input across all minute rows.  The
            # learned per-channel gate lets optimization decide how much daily
            # state to mix into the causal minute embedding.
            self.daily_context_gate: nn.Parameter | None = nn.Parameter(
                torch.zeros(self.d_model)
            )
            self.daily_context_fusion_norm: nn.Module | None = _make_norm(
                self.d_model, self.norm_type
            )
            if self.daily_context_lookback > 1:
                # A distinct encoder is essential: sharing the minute temporal
                # blocks would force one set of weights to model two clocks
                # with very different autocorrelation and signal-to-noise.
                self.daily_context_temporal_blocks = nn.ModuleList(
                    [
                        deepcopy(block)
                        for block in list(self.temporal_blocks)[
                            : self.daily_context_layers
                        ]
                    ]
                )
                self.daily_context_time_position: nn.Parameter | None = (
                    nn.Parameter(
                        torch.randn(
                            1,
                            self.daily_context_lookback,
                            1,
                            self.d_model,
                        )
                        * 0.02
                    )
                    if self.use_time_pos
                    else None
                )
                self.daily_context_pool_score: nn.Linear | None = (
                    nn.Linear(self.d_model, 1)
                    if self.daily_context_pooling == "attention"
                    else None
                )
                self.daily_context_output_norm: nn.Module | None = _make_norm(
                    self.d_model, self.norm_type
                )
            else:
                self.daily_context_temporal_blocks = nn.ModuleList()
                self.register_parameter("daily_context_time_position", None)
                self.daily_context_pool_score = None
                self.daily_context_output_norm = None
        else:
            self.daily_context_encoder = None
            self.register_parameter("daily_context_gate", None)
            self.daily_context_fusion_norm = None
            self.daily_context_temporal_blocks = nn.ModuleList()
            self.register_parameter("daily_context_time_position", None)
            self.daily_context_pool_score = None
            self.daily_context_output_norm = None

    def _encode_daily_context_history(
        self,
        daily_context_features: torch.Tensor,
    ) -> torch.Tensor:
        if self.daily_context_encoder is None:
            raise RuntimeError(
                "financial transformer was not built with daily context features"
            )
        if self.daily_context_lookback == 1:
            if daily_context_features.dim() != 3:
                raise ValueError(
                    "single-row daily context must have shape [D,S,Fd]"
                )
            projected, _aux = self.daily_context_encoder(
                daily_context_features,
                return_aux=False,
            )
            return projected
        if daily_context_features.dim() != 4:
            raise ValueError(
                "daily context history must have shape [D,Ld,S,Fd]"
            )
        days, steps, symbols, features = daily_context_features.shape
        if int(steps) != self.daily_context_lookback:
            raise ValueError(
                f"expected daily context lookback={self.daily_context_lookback}, "
                f"got {int(steps)}"
            )
        if int(features) != self.daily_context_num_features:
            raise ValueError(
                f"expected {self.daily_context_num_features} daily features, "
                f"got {int(features)}"
            )
        projected, _aux = self.daily_context_encoder(
            daily_context_features,
            return_aux=False,
        )
        if self.daily_context_time_position is not None:
            projected = projected + self.daily_context_time_position.to(
                device=projected.device,
                dtype=projected.dtype,
            )
        sequence = (
            projected.permute(0, 2, 1, 3)
            .contiguous()
            .reshape(days * symbols, steps, self.d_model)
        )
        rope_positions = self._temporal_rope_positions(
            int(steps), sequence.device
        )
        for block in self.daily_context_temporal_blocks:
            sequence = self._run_block(
                block,
                sequence,
                None,
                None,
                rope_positions,
            )
        if self.daily_context_pooling == "last":
            pooled = sequence[:, -1]
        elif self.daily_context_pooling == "mean":
            pooled = sequence.mean(dim=1)
        else:
            if self.daily_context_pool_score is None:
                raise RuntimeError(
                    "daily context attention pooling is unexpectedly missing"
                )
            scores = self.daily_context_pool_score(sequence).squeeze(-1)
            weights = torch.softmax(scores, dim=1)
            pooled = torch.sum(sequence * weights.unsqueeze(-1), dim=1)
        if self.daily_context_output_norm is not None:
            pooled = self.daily_context_output_norm(pooled)
        return pooled.reshape(days, symbols, self.d_model)

    def encode_daily_context_history(
        self,
        daily_context_features: torch.Tensor,
    ) -> torch.Tensor:
        """Encode one invariant daily history per independent minute session."""

        if self.daily_context_encoder is None:
            raise RuntimeError(
                "financial transformer was not built with daily context features"
            )
        if self.daily_context_lookback == 1:
            expected_rank = 3
        else:
            expected_rank = 4
        if daily_context_features.dim() != expected_rank:
            raise ValueError(
                "daily context features have the wrong history rank; "
                f"expected={expected_rank} got={daily_context_features.dim()}"
            )
        if int(daily_context_features.size(-1)) != self.daily_context_num_features:
            raise ValueError(
                "daily context feature width does not match the model contract"
            )
        if (
            self.daily_context_lookback > 1
            and int(daily_context_features.size(1)) != self.daily_context_lookback
        ):
            raise ValueError(
                "daily context history length does not match the model contract"
            )
        return self._encode_daily_context_history(daily_context_features)

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
        raw_basis_source = self._prepare_raw_temporal_basis_source(x)
        output = self._forward_embedded(
            h,
            mask_bool,
            temperature=temperature,
            return_aux=return_aux,
            temporal_basis_source=raw_basis_source,
        )
        return self._attach_candle_aux(output, token_aux, return_aux)

    def forward_from_batched_panel_slabs_with_daily_context(
        self,
        feature_slabs: torch.Tensor,
        daily_context_features: torch.Tensor,
        mask: torch.Tensor | None = None,
        temperature: float | torch.Tensor | None = None,
        return_aux: bool | None = None,
        symbol_indices: torch.Tensor | None = None,
    ):
        """Fuse a separately encoded causal daily history into minute slabs.

        ``feature_slabs`` is ``[D,U,S,Fm]``. Daily context is ``[D,S,Fd]`` for
        the legacy one-row contract or ``[D,Ld,S,Fd]`` for the dual-timescale
        contract. The daily branch is pooled once before its embedding is
        broadcast across minute rows.
        """

        self._check_batched_panel_slab_shapes(feature_slabs, mask, symbol_indices)
        if self.daily_context_encoder is None or self.daily_context_gate is None:
            raise RuntimeError(
                "financial transformer was not built with daily context features"
            )
        expected = (
            (
                int(feature_slabs.size(0)),
                int(feature_slabs.size(2)),
                self.daily_context_num_features,
            )
            if self.daily_context_lookback == 1
            else (
                int(feature_slabs.size(0)),
                self.daily_context_lookback,
                int(feature_slabs.size(2)),
                self.daily_context_num_features,
            )
        )
        if tuple(daily_context_features.shape) != expected:
            raise ValueError(
                "daily context features do not match the configured causal "
                "history shape; "
                f"expected {expected}, got {tuple(daily_context_features.shape)}"
            )
        context_projected = self.encode_daily_context_history(
            daily_context_features
        )
        return self.forward_from_batched_panel_slabs_with_encoded_daily_context(
            feature_slabs,
            context_projected,
            mask,
            temperature=temperature,
            return_aux=return_aux,
            symbol_indices=symbol_indices,
        )

    def forward_from_batched_panel_slabs_with_encoded_daily_context(
        self,
        feature_slabs: torch.Tensor,
        encoded_daily_context: torch.Tensor,
        mask: torch.Tensor | None = None,
        temperature: float | torch.Tensor | None = None,
        return_aux: bool | None = None,
        symbol_indices: torch.Tensor | None = None,
    ):
        """Fuse one pre-encoded daily state into all minute decision rows."""

        self._check_batched_panel_slab_shapes(feature_slabs, mask, symbol_indices)
        expected = (
            int(feature_slabs.size(0)),
            int(feature_slabs.size(2)),
            self.d_model,
        )
        if tuple(encoded_daily_context.shape) != expected:
            raise ValueError(
                "encoded daily context does not match minute slabs; "
                f"expected {expected}, got {tuple(encoded_daily_context.shape)}"
            )
        minute_projected = self._project_features(feature_slabs)
        gate = torch.sigmoid(self.daily_context_gate).to(
            dtype=minute_projected.dtype
        )
        fused = minute_projected + (
            encoded_daily_context.to(dtype=minute_projected.dtype).unsqueeze(1)
            * gate.view(1, 1, 1, -1)
        )
        if self.daily_context_fusion_norm is not None:
            fused = self.daily_context_fusion_norm(fused)
        windows = fused.unfold(1, self.lookback, 1)
        h = windows.permute(0, 1, 4, 2, 3).contiguous()
        days, decision_rows, lookback, symbols, width = h.shape
        h = h.reshape(days * decision_rows, lookback, symbols, width)
        h = self._add_window_positions(
            h,
            int(feature_slabs.size(2)),
            symbol_indices,
        )
        if mask is None:
            mask_bool = torch.ones(
                h.size(0), h.size(2), dtype=torch.bool, device=h.device
            )
        else:
            mask_bool = mask.to(device=h.device, dtype=torch.bool).reshape(
                h.size(0), h.size(2)
            )
        raw_basis_source = (
            self._raw_temporal_basis_windows_from_batched_panel_slabs(
                feature_slabs
            )
        )
        return self._forward_embedded(
            h,
            mask_bool,
            temperature=temperature,
            return_aux=return_aux,
            temporal_basis_source=raw_basis_source,
        )
