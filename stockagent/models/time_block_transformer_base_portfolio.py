from __future__ import annotations

import torch
from torch import nn
from torch.utils.checkpoint import checkpoint as activation_checkpoint

from stockagent.models.latent_factor_market_token_portfolio import _safe_attention_mask
from stockagent.models.normalization import dual_branch_softmax, masked_cross_sectional_mean, masked_softmax
from stockagent.models.transformer_base_portfolio import (
    DynamicTokenGenerator,
    GatedProjection,
    TransformerPortfolioBlock,
    _make_norm,
    _normalize_ffn_type,
    _normalize_norm_type,
    local_causal_mask,
)


class TimeBlockTransformerBasePortfolioModel(nn.Module):
    """Streaming/time-block Transformer portfolio model.

    The model encodes a contiguous context ``[C,S,F]`` once and emits portfolio
    weights for target days inside that context. Temporal attention is local and
    causal, while cross-sectional attention is applied only within each target
    day.
    """

    def __init__(
        self,
        lookback: int,
        num_features: int,
        num_symbols: int,
        d_model: int = 64,
        attention_mode: str = "market_token",
        use_flash_attention: bool = True,
        use_time_pos: bool = False,
        use_symbol_pos: bool = True,
        input_dropout: float = 0.0,
        sdpa_batch_limit: int = 4096,
        norm_type: str = "rmsnorm",
        ffn_type: str = "swiglu",
        qk_norm: bool = True,
        rope_temporal: bool = True,
        rope_base: float = 10000.0,
        temporal_layers: int = 2,
        temporal_heads: int = 4,
        temporal_ffn_mult: int = 2,
        temporal_pooling: str = "last",
        cross_layers: int = 1,
        cross_heads: int = 4,
        cross_ffn_mult: int = 2,
        latent_layers: int = 1,
        num_latent_factors: int = 16,
        num_market_tokens: int = 4,
        market_layers: int = 1,
        dynamic_latent_tokens: bool = True,
        dynamic_market_tokens: bool = True,
        dynamic_token_hidden_mult: int = 2,
        dynamic_token_gate_init: float = 0.1,
        dynamic_token_dropout: float = 0.1,
        head_hidden_dim: int = 64,
        head_layers: int = 1,
        dropout: float = 0.1,
        default_temperature: float = 1.0,
        portfolio_mode: str = "long_short",
        checkpoint_blocks: bool = False,
        return_aux: bool = True,
        return_aux_details: bool = False,
        temporal_causal: bool = True,
        temporal_local_window: int = 0,
        use_flex_temporal_attention: bool = False,
        time_block_mode: bool = True,
        runtime_shape_check: bool = False,
        allow_dynamic_symbols: bool = True,
        **_: object,
    ) -> None:
        super().__init__()
        if use_time_pos:
            raise ValueError(
                "TimeBlockTransformerBasePortfolioModel requires use_time_pos=false; "
                "use RoPE/local causal attention for streaming time positions."
            )
        if use_flex_temporal_attention:
            raise ValueError(
                "use_flex_temporal_attention is not implemented yet for time-block training; "
                "set it to false to use SDPA local causal attention."
            )
        self.lookback = int(lookback)
        self.num_features = int(num_features)
        self.num_symbols = int(num_symbols)
        self.d_model = int(d_model)
        self.attention_mode = self._normalize_attention_mode(attention_mode)
        self.temporal_pooling = self._normalize_pooling(temporal_pooling)
        self.default_temperature = float(default_temperature)
        self.portfolio_mode = self._normalize_portfolio_mode(portfolio_mode)
        self.checkpoint_blocks = bool(checkpoint_blocks)
        self.return_aux = bool(return_aux)
        self.return_aux_details = bool(return_aux_details)
        self.temporal_causal = bool(temporal_causal)
        self.temporal_local_window = int(temporal_local_window) if int(temporal_local_window) > 0 else self.lookback
        self.use_flex_temporal_attention = bool(use_flex_temporal_attention)
        self.time_block_mode = bool(time_block_mode)
        self.runtime_shape_check = bool(runtime_shape_check)
        self.allow_dynamic_symbols = bool(allow_dynamic_symbols)
        self.use_symbol_pos = bool(use_symbol_pos)
        self.rope_temporal = bool(rope_temporal)
        self.norm_type = _normalize_norm_type(norm_type)
        self.ffn_type = _normalize_ffn_type(ffn_type)

        self.feature_proj = nn.Linear(self.num_features, self.d_model)
        self.input_dropout = nn.Dropout(float(input_dropout))
        self.symbol_position = nn.Parameter(torch.randn(1, self.num_symbols, self.d_model) * 0.02)

        def make_block(num_heads: int, ffn_mult: int) -> TransformerPortfolioBlock:
            return TransformerPortfolioBlock(
                dim=self.d_model,
                num_heads=int(num_heads),
                ffn_mult=int(ffn_mult),
                dropout=float(dropout),
                use_flash_attention=bool(use_flash_attention),
                sdpa_batch_limit=int(sdpa_batch_limit),
                norm_type=self.norm_type,
                ffn_type=self.ffn_type,
                qk_norm=bool(qk_norm),
                rope_base=float(rope_base),
            )

        self.temporal_blocks = nn.ModuleList(
            [make_block(int(temporal_heads), int(temporal_ffn_mult)) for _ in range(max(0, int(temporal_layers)))]
        )
        self.cross_blocks = nn.ModuleList(
            [make_block(int(cross_heads), int(cross_ffn_mult)) for _ in range(max(0, int(cross_layers)))]
        )
        self.latent_blocks = nn.ModuleList(
            [make_block(int(cross_heads), int(cross_ffn_mult)) for _ in range(max(1, int(latent_layers)))]
        )
        self.market_blocks = nn.ModuleList(
            [make_block(int(cross_heads), int(cross_ffn_mult)) for _ in range(max(1, int(market_layers)))]
        )
        self.stock_read_latent_blocks = nn.ModuleList(
            [make_block(int(cross_heads), int(cross_ffn_mult)) for _ in range(max(1, int(market_layers)))]
        )
        self.stock_read_market_blocks = nn.ModuleList(
            [make_block(int(cross_heads), int(cross_ffn_mult)) for _ in range(max(1, int(market_layers)))]
        )

        latent_count = max(1, int(num_latent_factors))
        market_count = max(1, int(num_market_tokens))
        self.latent_queries = nn.Parameter(torch.randn(1, latent_count, self.d_model) * 0.02)
        self.market_queries = nn.Parameter(torch.randn(1, market_count, self.d_model) * 0.02)
        self.dynamic_latent_generator = (
            DynamicTokenGenerator(
                dim=self.d_model,
                num_tokens=latent_count,
                hidden_mult=int(dynamic_token_hidden_mult),
                dropout=float(dynamic_token_dropout),
                gate_init=float(dynamic_token_gate_init),
                norm_type=self.norm_type,
                ffn_type=self.ffn_type,
            )
            if bool(dynamic_latent_tokens)
            else None
        )
        self.dynamic_market_generator = (
            DynamicTokenGenerator(
                dim=self.d_model,
                num_tokens=market_count,
                hidden_mult=int(dynamic_token_hidden_mult),
                dropout=float(dynamic_token_dropout),
                gate_init=float(dynamic_token_gate_init),
                norm_type=self.norm_type,
                ffn_type=self.ffn_type,
            )
            if bool(dynamic_market_tokens)
            else None
        )

        self.output_norm = _make_norm(self.d_model, self.norm_type)
        self.portfolio_fusion = nn.Sequential(
            GatedProjection(self.d_model * 3, self.d_model, float(dropout), self.ffn_type),
            _make_norm(self.d_model, self.norm_type),
        )

        head: list[nn.Module] = []
        in_dim = self.d_model
        for _idx in range(max(0, int(head_layers))):
            head.append(GatedProjection(in_dim, int(head_hidden_dim), float(dropout), self.ffn_type))
            in_dim = int(head_hidden_dim)
        head.append(nn.Linear(in_dim, 1))
        self.score_head = nn.Sequential(*head)

    @staticmethod
    def _normalize_attention_mode(attention_mode: str) -> str:
        normalized = str(attention_mode).strip().lower().replace("-", "_")
        aliases = {
            "market": "market_token",
            "market_tokens": "market_token",
            "low_rank": "latent",
            "latent_factor": "latent",
            "none": "temporal_only",
            "temporal": "temporal_only",
        }
        normalized = aliases.get(normalized, normalized)
        if normalized in {"latent", "market_token", "temporal_only"}:
            return normalized
        raise ValueError("time-block attention_mode must be one of: latent, market_token, temporal_only")

    @staticmethod
    def _normalize_pooling(pooling: str) -> str:
        normalized = str(pooling).strip().lower().replace("-", "_")
        if normalized in {"last", "mean", "attention", "attn"}:
            return "attention" if normalized == "attn" else normalized
        raise ValueError("temporal_pooling must be one of: last, mean, attention")

    @staticmethod
    def _normalize_portfolio_mode(portfolio_mode: str) -> str:
        normalized = str(portfolio_mode).strip().lower().replace("-", "_")
        if normalized in {"long", "long_only", "longonly"}:
            return "long_only"
        if normalized in {"long_short", "longshort", "short", "dual_branch", "long_and_short"}:
            return "long_short"
        raise ValueError("portfolio_mode must be 'long_only' or 'long_short'")

    def _symbol_position(self, n_symbols: int) -> torch.Tensor:
        if n_symbols <= int(self.symbol_position.size(1)):
            return self.symbol_position[:, :n_symbols, :]
        extra = self.symbol_position.new_zeros(1, n_symbols - int(self.symbol_position.size(1)), self.d_model)
        return torch.cat([self.symbol_position, extra], dim=1)

    def _embed_context(self, x_context: torch.Tensor) -> torch.Tensor:
        h = self.feature_proj(torch.nan_to_num(x_context, nan=0.0, posinf=0.0, neginf=0.0))
        if self.use_symbol_pos:
            h = h + self._symbol_position(int(x_context.size(1)))
        return self.input_dropout(h)

    def _run_block(
        self,
        block: TransformerPortfolioBlock,
        query: torch.Tensor,
        context: torch.Tensor | None = None,
        key_mask: torch.Tensor | None = None,
        rope_positions: torch.Tensor | None = None,
        attn_mask: torch.Tensor | None = None,
        is_causal: bool = False,
    ) -> torch.Tensor:
        if self.checkpoint_blocks and self.training and torch.is_grad_enabled():
            return activation_checkpoint(
                block,
                query,
                context,
                key_mask,
                rope_positions,
                attn_mask,
                is_causal,
                use_reentrant=False,
            )
        return block(
            query,
            context,
            key_mask=key_mask,
            rope_positions=rope_positions,
            attn_mask=attn_mask,
            is_causal=is_causal,
        )

    def _temporal_rope_positions(
        self,
        steps: int,
        device: torch.device,
        context_positions: torch.Tensor | None,
    ) -> torch.Tensor | None:
        if not self.rope_temporal:
            return None
        if context_positions is not None:
            return context_positions.to(device=device, dtype=torch.float32)
        return torch.arange(int(steps), device=device, dtype=torch.float32)

    def _apply_temporal_context(
        self,
        h: torch.Tensor,
        context_positions: torch.Tensor | None,
    ) -> torch.Tensor:
        steps, n_symbols, dim = h.shape
        seq = h.permute(1, 0, 2).contiguous()
        rope_positions = self._temporal_rope_positions(steps, h.device, context_positions)
        attn_mask = local_causal_mask(steps, self.temporal_local_window, h.device) if self.temporal_causal else None
        for block in self.temporal_blocks:
            seq = self._run_block(
                block,
                seq,
                key_mask=None,
                rope_positions=rope_positions,
                attn_mask=attn_mask,
                is_causal=self.temporal_causal and attn_mask is None,
            )
        return seq.permute(1, 0, 2).contiguous()

    def _prefixed_aux(self, prefix: str, values: dict[str, torch.Tensor]) -> dict[str, torch.Tensor]:
        return {f"{prefix}_{name}": value for name, value in values.items()}

    def _cross_sectional_encode(
        self,
        z_base: torch.Tensor,
        safe_mask: torch.Tensor,
        collect_aux: bool,
    ) -> tuple[torch.Tensor, dict[str, torch.Tensor]]:
        aux: dict[str, torch.Tensor] = {}
        z_base = self.output_norm(z_base).masked_fill(~safe_mask.unsqueeze(-1), 0.0)
        if self.attention_mode == "temporal_only":
            z_market_context = z_base
            z_factor_context = z_base
            latent = z_base.new_empty(int(z_base.size(0)), 0, self.d_model)
            market_tokens = z_base.new_empty(int(z_base.size(0)), 0, self.d_model)
        else:
            for block in self.cross_blocks:
                z_base = self._run_block(block, z_base, key_mask=safe_mask)
            if self.attention_mode == "latent":
                if self.dynamic_latent_generator is not None:
                    latent, dynamic_aux = self.dynamic_latent_generator(
                        self.latent_queries,
                        z_base,
                        safe_mask,
                        collect_aux=collect_aux,
                    )
                    if collect_aux:
                        aux.update(self._prefixed_aux("dynamic_latent", dynamic_aux))
                else:
                    latent = self.latent_queries.expand(int(z_base.size(0)), -1, -1)
                for block in self.latent_blocks:
                    latent = self._run_block(block, latent, z_base, key_mask=safe_mask)
                z_factor_context = z_base
                for block in self.stock_read_latent_blocks:
                    z_factor_context = self._run_block(block, z_factor_context, latent)
                market_context = latent
                market_key_mask = None
            else:
                latent = z_base.new_empty(int(z_base.size(0)), 0, self.d_model)
                z_factor_context = z_base
                market_context = z_base
                market_key_mask = safe_mask

            if self.dynamic_market_generator is not None:
                market_tokens, dynamic_aux = self.dynamic_market_generator(
                    self.market_queries,
                    z_base,
                    safe_mask,
                    collect_aux=collect_aux,
                )
                if collect_aux:
                    aux.update(self._prefixed_aux("dynamic_market", dynamic_aux))
            else:
                market_tokens = self.market_queries.expand(int(z_base.size(0)), -1, -1)
            for block in self.market_blocks:
                market_tokens = self._run_block(block, market_tokens, market_context, key_mask=market_key_mask)
            z_market_context = z_base
            for block in self.stock_read_market_blocks:
                z_market_context = self._run_block(block, z_market_context, market_tokens)

        z_stock = self.portfolio_fusion(torch.cat([z_base, z_factor_context, z_market_context], dim=-1))
        z_stock = z_stock.masked_fill(~safe_mask.unsqueeze(-1), 0.0)
        if collect_aux:
            aux.update(
                {
                    "stock_embedding": z_base,
                    "latent_factors": latent,
                    "market_tokens": market_tokens,
                    "z_factor_context": z_factor_context,
                    "z_market_context": z_market_context,
                }
            )
        return z_stock, aux

    def _scores_to_output(
        self,
        z_stock: torch.Tensor,
        mask_bool: torch.Tensor,
        temperature: float | torch.Tensor | None,
        aux: dict[str, torch.Tensor],
        return_aux: bool | None,
    ):
        scores = self.score_head(z_stock).squeeze(-1)
        masked_scores = scores.masked_fill(~mask_bool, -1e9)
        if temperature is None:
            temp = masked_scores.new_tensor(self.default_temperature)
        elif isinstance(temperature, torch.Tensor):
            temp = temperature.to(device=masked_scores.device, dtype=masked_scores.dtype)
        else:
            temp = masked_scores.new_tensor(float(temperature))
        temp = torch.clamp(temp, min=0.05)

        if self.portfolio_mode == "long_only":
            weights = masked_softmax(masked_scores / temp, mask_bool)
        else:
            relative_scores = scores - masked_cross_sectional_mean(scores, mask_bool)
            weights = dual_branch_softmax(relative_scores / temp, mask_bool)
        weights = weights.masked_fill(~mask_bool, 0.0)

        if return_aux is True:
            aux = dict(aux)
            aux.update({"z_stock": z_stock, "score_logits": scores, "rank_logits": scores})
            return weights, masked_scores, aux
        if return_aux is False:
            return weights
        output = {
            "weights": weights,
            "scores": masked_scores,
            "score_logits": scores,
            "rank_logits": scores,
        }
        if return_aux is None and self.return_aux and self.return_aux_details:
            aux = dict(aux)
            aux.update({"z_stock": z_stock, "score_logits": scores, "rank_logits": scores})
            output["aux"] = aux
            output.update(aux)
        return output

    def forward_time_block(
        self,
        x_context: torch.Tensor,
        target_mask: torch.Tensor,
        target_offset: int,
        target_len: int | None = None,
        temperature: float | torch.Tensor | None = None,
        return_aux: bool | None = None,
        context_positions: torch.Tensor | None = None,
    ):
        if x_context.dim() == 4:
            if int(x_context.size(0)) != 1:
                raise ValueError("x_context with batch dimension must have shape [1,C,S,F]")
            x_context = x_context.squeeze(0)
        if x_context.dim() != 3:
            raise ValueError(f"Expected x_context shape [C,S,F], got {tuple(x_context.shape)}")
        if (not self.allow_dynamic_symbols) and int(x_context.size(1)) != self.num_symbols:
            raise ValueError(f"Expected num_symbols={self.num_symbols}, got {int(x_context.size(1))}")
        if int(x_context.size(2)) != self.num_features:
            raise ValueError(f"Expected num_features={self.num_features}, got {int(x_context.size(2))}")

        target_offset = int(target_offset)
        if target_len is None:
            target_len = int(x_context.size(0)) - target_offset
        target_len = int(target_len)
        if target_offset < 0 or target_len < 1 or target_offset + target_len > int(x_context.size(0)):
            raise ValueError("target_offset/target_len are outside x_context")
        expected_mask_shape = (target_len, int(x_context.size(1)))
        if tuple(target_mask.shape) != expected_mask_shape:
            raise ValueError(f"Expected target_mask shape {expected_mask_shape}, got {tuple(target_mask.shape)}")

        mask_bool = target_mask.to(device=x_context.device, dtype=torch.bool)
        safe_mask = _safe_attention_mask(mask_bool)
        collect_aux = bool(return_aux is True or (return_aux is None and self.return_aux and self.return_aux_details))

        h = self._embed_context(x_context)
        h = self._apply_temporal_context(h, context_positions)
        h_target = h[target_offset : target_offset + target_len]
        z_stock, aux = self._cross_sectional_encode(h_target, safe_mask, collect_aux=collect_aux)
        if collect_aux:
            aux["token_embedding"] = h_target
        return self._scores_to_output(z_stock, mask_bool, temperature, aux, return_aux)

    def forward(
        self,
        x: torch.Tensor,
        mask: torch.Tensor | None = None,
        temperature: float | torch.Tensor | None = None,
        return_aux: bool | None = None,
    ):
        if x.dim() != 4:
            raise ValueError(f"Expected x shape [B,L,S,F], got {tuple(x.shape)}")
        if mask is None:
            mask = torch.ones(x.size(0), x.size(2), device=x.device, dtype=torch.bool)
        outputs = [
            self.forward_time_block(
                x[index],
                mask[index : index + 1],
                target_offset=int(x.size(1)) - 1,
                target_len=1,
                temperature=temperature,
                return_aux=return_aux,
            )
            for index in range(int(x.size(0)))
        ]
        if return_aux is True:
            weights = torch.cat([item[0] for item in outputs], dim=0)
            scores = torch.cat([item[1] for item in outputs], dim=0)
            return weights, scores, {}
        if isinstance(outputs[0], dict):
            return {
                key: torch.cat([item[key] for item in outputs], dim=0)
                for key in ("weights", "scores", "score_logits", "rank_logits")
            }
        return torch.cat(outputs, dim=0)
