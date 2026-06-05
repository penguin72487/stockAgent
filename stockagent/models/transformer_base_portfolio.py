from __future__ import annotations

import torch
import torch.nn.functional as F
from torch import nn
from torch.utils.checkpoint import checkpoint as activation_checkpoint

from stockagent.models.latent_factor_market_token_portfolio import _safe_attention_mask
from stockagent.models.normalization import dual_branch_softmax, masked_cross_sectional_mean, masked_softmax


class FlashSDPAAttention(nn.Module):
    """Multi-head attention backed by PyTorch SDPA.

    On CUDA, PyTorch chooses flash / memory-efficient / math kernels according
    to dtype, shape, and backend flags. The module also has a manual fallback so
    tests can disable SDPA deterministically.
    """

    def __init__(self, dim: int, num_heads: int, dropout: float, use_flash_attention: bool = True) -> None:
        super().__init__()
        self.dim = int(dim)
        self.num_heads = max(1, int(num_heads))
        if self.dim % self.num_heads != 0:
            raise ValueError("dim must be divisible by num_heads")
        self.head_dim = self.dim // self.num_heads
        self.scale = float(self.head_dim) ** -0.5
        self.use_flash_attention = bool(use_flash_attention)

        self.q_proj = nn.Linear(self.dim, self.dim)
        self.k_proj = nn.Linear(self.dim, self.dim)
        self.v_proj = nn.Linear(self.dim, self.dim)
        self.out_proj = nn.Linear(self.dim, self.dim)
        self.dropout_p = float(dropout)

    def _project(self, tensor: torch.Tensor, proj: nn.Linear) -> torch.Tensor:
        bsz, steps, _ = tensor.shape
        return (
            proj(tensor)
            .reshape(bsz, steps, self.num_heads, self.head_dim)
            .transpose(1, 2)
            .contiguous()
        )

    def forward(
        self,
        query: torch.Tensor,
        context: torch.Tensor,
        key_mask: torch.Tensor | None = None,
    ) -> torch.Tensor:
        bsz, query_steps, _ = query.shape
        key_steps = int(context.size(1))
        q = self._project(query, self.q_proj)
        k = self._project(context, self.k_proj)
        v = self._project(context, self.v_proj)

        attn_mask = None
        if key_mask is not None:
            key_mask = key_mask.to(device=query.device, dtype=torch.bool)
            attn_mask = key_mask[:, None, None, :].expand(bsz, self.num_heads, query_steps, key_steps)

        if self.use_flash_attention:
            y = F.scaled_dot_product_attention(
                q,
                k,
                v,
                attn_mask=attn_mask,
                dropout_p=self.dropout_p if self.training else 0.0,
                is_causal=False,
            )
        else:
            scores = torch.matmul(q, k.transpose(-2, -1)) * self.scale
            if attn_mask is not None:
                scores = scores.masked_fill(~attn_mask, torch.finfo(scores.dtype).min)
            attn = torch.softmax(scores, dim=-1)
            attn = F.dropout(attn, p=self.dropout_p, training=self.training)
            y = torch.matmul(attn, v)

        y = y.transpose(1, 2).contiguous().reshape(bsz, query_steps, self.dim)
        return self.out_proj(y)


class TransformerPortfolioBlock(nn.Module):
    """Pre-norm self/cross attention block for portfolio token mixing."""

    def __init__(
        self,
        *,
        dim: int,
        num_heads: int,
        ffn_mult: int,
        dropout: float,
        use_flash_attention: bool,
    ) -> None:
        super().__init__()
        self.norm_query = nn.LayerNorm(int(dim))
        self.norm_context = nn.LayerNorm(int(dim))
        self.attn = FlashSDPAAttention(
            dim=int(dim),
            num_heads=int(num_heads),
            dropout=float(dropout),
            use_flash_attention=bool(use_flash_attention),
        )
        self.resid_dropout = nn.Dropout(float(dropout))
        self.norm_ffn = nn.LayerNorm(int(dim))
        hidden_dim = max(int(dim), int(dim) * max(1, int(ffn_mult)))
        self.ffn = nn.Sequential(
            nn.Linear(int(dim), hidden_dim),
            nn.GELU(),
            nn.Dropout(float(dropout)),
            nn.Linear(hidden_dim, int(dim)),
            nn.Dropout(float(dropout)),
        )

    def forward(
        self,
        query: torch.Tensor,
        context: torch.Tensor | None = None,
        key_mask: torch.Tensor | None = None,
    ) -> torch.Tensor:
        context = query if context is None else context
        attn_out = self.attn(self.norm_query(query), self.norm_context(context), key_mask=key_mask)
        query = query + self.resid_dropout(attn_out)
        query = query + self.ffn(self.norm_ffn(query))
        return query


class TransformerBasePortfolioModel(nn.Module):
    """Configurable Transformer-base portfolio model.

    The same class can represent a full joint spatio-temporal Transformer or
    progressively cheaper approximations:

    - full:       O((L*S)^2), most complete, for small universes.
    - axial:      O(S*L^2 + L*S^2), temporal then cross-stock attention.
    - latent:     O(S*L^2 + S*K + K*M + S*(K+M)), low-rank bottleneck.
    - market_token: O(S*L^2 + S*M), market-token bottleneck.
    - temporal_only: O(S*L^2), no cross-stock attention.
    """

    def __init__(
        self,
        lookback: int,
        num_features: int,
        num_symbols: int,
        d_model: int = 64,
        attention_mode: str = "latent",
        use_flash_attention: bool = True,
        use_time_pos: bool = True,
        use_symbol_pos: bool = True,
        input_dropout: float = 0.0,
        temporal_layers: int = 2,
        temporal_heads: int = 4,
        temporal_ffn_mult: int = 2,
        temporal_pooling: str = "attention",
        cross_layers: int = 1,
        cross_heads: int = 4,
        cross_ffn_mult: int = 2,
        joint_layers: int = 2,
        joint_heads: int = 4,
        joint_ffn_mult: int = 2,
        latent_layers: int = 1,
        num_latent_factors: int = 16,
        num_market_tokens: int = 4,
        market_layers: int = 1,
        head_hidden_dim: int = 64,
        head_layers: int = 1,
        dropout: float = 0.1,
        default_temperature: float = 1.0,
        portfolio_mode: str = "long_short",
        max_full_tokens: int = 4096,
        checkpoint_blocks: bool = False,
        return_aux: bool = True,
        return_aux_details: bool = False,
        runtime_shape_check: bool = False,
        allow_dynamic_symbols: bool = True,
    ) -> None:
        super().__init__()
        self.lookback = int(lookback)
        self.num_features = int(num_features)
        self.num_symbols = int(num_symbols)
        self.d_model = int(d_model)
        self.attention_mode = self._normalize_attention_mode(attention_mode)
        self.temporal_pooling = self._normalize_pooling(temporal_pooling)
        self.default_temperature = float(default_temperature)
        self.portfolio_mode = self._normalize_portfolio_mode(portfolio_mode)
        self.max_full_tokens = int(max_full_tokens)
        self.checkpoint_blocks = bool(checkpoint_blocks)
        self.return_aux = bool(return_aux)
        self.return_aux_details = bool(return_aux_details)
        self.runtime_shape_check = bool(runtime_shape_check)
        self.allow_dynamic_symbols = bool(allow_dynamic_symbols)
        self.use_time_pos = bool(use_time_pos)
        self.use_symbol_pos = bool(use_symbol_pos)

        self.feature_proj = nn.Linear(self.num_features, self.d_model)
        self.input_dropout = nn.Dropout(float(input_dropout))
        self.time_position = nn.Parameter(torch.randn(1, self.lookback, 1, self.d_model) * 0.02)
        self.symbol_position = nn.Parameter(torch.randn(1, 1, self.num_symbols, self.d_model) * 0.02)

        self.temporal_blocks = nn.ModuleList(
            [
                TransformerPortfolioBlock(
                    dim=self.d_model,
                    num_heads=int(temporal_heads),
                    ffn_mult=int(temporal_ffn_mult),
                    dropout=float(dropout),
                    use_flash_attention=bool(use_flash_attention),
                )
                for _ in range(max(0, int(temporal_layers)))
            ]
        )
        self.cross_blocks = nn.ModuleList(
            [
                TransformerPortfolioBlock(
                    dim=self.d_model,
                    num_heads=int(cross_heads),
                    ffn_mult=int(cross_ffn_mult),
                    dropout=float(dropout),
                    use_flash_attention=bool(use_flash_attention),
                )
                for _ in range(max(0, int(cross_layers)))
            ]
        )
        self.joint_blocks = nn.ModuleList(
            [
                TransformerPortfolioBlock(
                    dim=self.d_model,
                    num_heads=int(joint_heads),
                    ffn_mult=int(joint_ffn_mult),
                    dropout=float(dropout),
                    use_flash_attention=bool(use_flash_attention),
                )
                for _ in range(max(0, int(joint_layers)))
            ]
        )

        self.latent_queries = nn.Parameter(torch.randn(1, max(1, int(num_latent_factors)), self.d_model) * 0.02)
        self.market_queries = nn.Parameter(torch.randn(1, max(1, int(num_market_tokens)), self.d_model) * 0.02)
        self.latent_blocks = nn.ModuleList(
            [
                TransformerPortfolioBlock(
                    dim=self.d_model,
                    num_heads=int(cross_heads),
                    ffn_mult=int(cross_ffn_mult),
                    dropout=float(dropout),
                    use_flash_attention=bool(use_flash_attention),
                )
                for _ in range(max(1, int(latent_layers)))
            ]
        )
        self.market_blocks = nn.ModuleList(
            [
                TransformerPortfolioBlock(
                    dim=self.d_model,
                    num_heads=int(cross_heads),
                    ffn_mult=int(cross_ffn_mult),
                    dropout=float(dropout),
                    use_flash_attention=bool(use_flash_attention),
                )
                for _ in range(max(1, int(market_layers)))
            ]
        )
        self.stock_read_latent_blocks = nn.ModuleList(
            [
                TransformerPortfolioBlock(
                    dim=self.d_model,
                    num_heads=int(cross_heads),
                    ffn_mult=int(cross_ffn_mult),
                    dropout=float(dropout),
                    use_flash_attention=bool(use_flash_attention),
                )
                for _ in range(max(1, int(market_layers)))
            ]
        )
        self.stock_read_market_blocks = nn.ModuleList(
            [
                TransformerPortfolioBlock(
                    dim=self.d_model,
                    num_heads=int(cross_heads),
                    ffn_mult=int(cross_ffn_mult),
                    dropout=float(dropout),
                    use_flash_attention=bool(use_flash_attention),
                )
                for _ in range(max(1, int(market_layers)))
            ]
        )

        self.temporal_pool_score = nn.Linear(self.d_model, 1) if self.temporal_pooling == "attention" else None
        self.output_norm = nn.LayerNorm(self.d_model)
        self.portfolio_fusion = nn.Sequential(
            nn.Linear(self.d_model * 3, self.d_model),
            nn.LayerNorm(self.d_model),
            nn.GELU(),
            nn.Dropout(float(dropout)),
        )

        head: list[nn.Module] = []
        in_dim = self.d_model
        for _ in range(max(0, int(head_layers))):
            head.extend(
                [
                    nn.Linear(in_dim, int(head_hidden_dim)),
                    nn.GELU(),
                    nn.Dropout(float(dropout)),
                ]
            )
            in_dim = int(head_hidden_dim)
        head.append(nn.Linear(in_dim, 1))
        self.score_head = nn.Sequential(*head)

    @staticmethod
    def _normalize_attention_mode(attention_mode: str) -> str:
        normalized = str(attention_mode).strip().lower().replace("-", "_")
        aliases = {
            "complete": "full",
            "joint": "full",
            "joint_full": "full",
            "factorized": "axial",
            "axis": "axial",
            "low_rank": "latent",
            "latent_factor": "latent",
            "market": "market_token",
            "market_tokens": "market_token",
            "none": "temporal_only",
            "temporal": "temporal_only",
        }
        normalized = aliases.get(normalized, normalized)
        if normalized in {"full", "axial", "latent", "market_token", "temporal_only"}:
            return normalized
        raise ValueError(
            "attention_mode must be one of: full, axial, latent, market_token, temporal_only"
        )

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

    def _check_shapes(self, x: torch.Tensor, mask: torch.Tensor | None) -> None:
        if x.dim() != 4:
            raise ValueError(f"Expected x shape [B,L,S,F], got ndim={x.dim()}")
        if int(x.size(1)) != self.lookback:
            raise ValueError(f"Expected lookback={self.lookback}, got {int(x.size(1))}")
        if (not self.allow_dynamic_symbols) and int(x.size(2)) != self.num_symbols:
            raise ValueError(f"Expected num_symbols={self.num_symbols}, got {int(x.size(2))}")
        if int(x.size(3)) != self.num_features:
            raise ValueError(f"Expected num_features={self.num_features}, got {int(x.size(3))}")
        if mask is not None and tuple(mask.shape) != (int(x.size(0)), int(x.size(2))):
            raise ValueError(f"Expected mask shape {(int(x.size(0)), int(x.size(2)))}, got {tuple(mask.shape)}")

    def _run_block(self, block: TransformerPortfolioBlock, *args) -> torch.Tensor:
        if self.checkpoint_blocks and self.training and torch.is_grad_enabled():
            return activation_checkpoint(block, *args, use_reentrant=False)
        return block(*args)

    def _symbol_position(self, n_symbols: int) -> torch.Tensor:
        if n_symbols <= int(self.symbol_position.size(2)):
            return self.symbol_position[:, :, :n_symbols, :]
        extra = self.symbol_position.new_zeros(
            1,
            1,
            n_symbols - int(self.symbol_position.size(2)),
            self.d_model,
        )
        return torch.cat([self.symbol_position, extra], dim=2)

    def _embed_inputs(self, x: torch.Tensor) -> torch.Tensor:
        h = self.feature_proj(torch.nan_to_num(x, nan=0.0, posinf=0.0, neginf=0.0))
        if self.use_time_pos:
            h = h + self.time_position[:, : int(x.size(1)), :, :]
        if self.use_symbol_pos:
            h = h + self._symbol_position(int(x.size(2)))
        return self.input_dropout(h)

    def _apply_temporal_blocks(self, h: torch.Tensor) -> torch.Tensor:
        bsz, steps, n_symbols, dim = h.shape
        seq = h.permute(0, 2, 1, 3).contiguous().reshape(bsz * n_symbols, steps, dim)
        for block in self.temporal_blocks:
            seq = self._run_block(block, seq, None, None)
        return seq.reshape(bsz, n_symbols, steps, dim).permute(0, 2, 1, 3).contiguous()

    def _apply_cross_blocks(self, h: torch.Tensor, safe_mask: torch.Tensor) -> torch.Tensor:
        bsz, steps, n_symbols, dim = h.shape
        seq = h.reshape(bsz * steps, n_symbols, dim)
        key_mask = safe_mask[:, None, :].expand(bsz, steps, n_symbols).reshape(bsz * steps, n_symbols)
        for block in self.cross_blocks:
            seq = self._run_block(block, seq, None, key_mask)
        return seq.reshape(bsz, steps, n_symbols, dim)

    def _pool_temporal(self, h: torch.Tensor, mask_bool: torch.Tensor) -> torch.Tensor:
        if self.temporal_pooling == "last":
            pooled = h[:, -1, :, :]
        elif self.temporal_pooling == "mean":
            pooled = h.mean(dim=1)
        else:
            if self.temporal_pool_score is None:
                raise RuntimeError("temporal_pool_score is unexpectedly None")
            scores = self.temporal_pool_score(h).squeeze(-1).transpose(1, 2)
            weights = torch.softmax(scores, dim=-1)
            pooled = (h.permute(0, 2, 1, 3) * weights.unsqueeze(-1)).sum(dim=2)
        return self.output_norm(pooled).masked_fill(~mask_bool.unsqueeze(-1), 0.0)

    def _forward_full(self, h: torch.Tensor, safe_mask: torch.Tensor) -> tuple[torch.Tensor, dict[str, torch.Tensor]]:
        bsz, steps, n_symbols, dim = h.shape
        token_count = steps * n_symbols
        if self.max_full_tokens > 0 and token_count > self.max_full_tokens:
            raise ValueError(
                f"attention_mode=full would create {token_count} tokens; "
                f"increase max_full_tokens={self.max_full_tokens} only if VRAM is sufficient"
            )
        tokens = h.reshape(bsz, token_count, dim)
        key_mask = safe_mask[:, None, :].expand(bsz, steps, n_symbols).reshape(bsz, token_count)
        for block in self.joint_blocks:
            tokens = self._run_block(block, tokens, None, key_mask)
        h_full = tokens.reshape(bsz, steps, n_symbols, dim)
        return self._pool_temporal(h_full, safe_mask), {"token_embedding": h_full}

    def _forward_axial(self, h: torch.Tensor, safe_mask: torch.Tensor) -> tuple[torch.Tensor, dict[str, torch.Tensor]]:
        h = self._apply_temporal_blocks(h)
        h = self._apply_cross_blocks(h, safe_mask)
        return self._pool_temporal(h, safe_mask), {"token_embedding": h}

    def _forward_temporal_only(
        self,
        h: torch.Tensor,
        safe_mask: torch.Tensor,
    ) -> tuple[torch.Tensor, dict[str, torch.Tensor]]:
        h = self._apply_temporal_blocks(h)
        return self._pool_temporal(h, safe_mask), {"token_embedding": h}

    def _forward_latent_or_market(
        self,
        h: torch.Tensor,
        safe_mask: torch.Tensor,
        *,
        use_latent: bool,
    ) -> tuple[torch.Tensor, dict[str, torch.Tensor]]:
        h = self._apply_temporal_blocks(h)
        z_base = self._pool_temporal(h, safe_mask)
        bsz = int(h.size(0))

        if use_latent:
            latent = self.latent_queries.expand(bsz, -1, -1)
            for block in self.latent_blocks:
                latent = self._run_block(block, latent, z_base, safe_mask)
            market_context = latent
            market_key_mask = None
            z_factor_context = z_base
            for block in self.stock_read_latent_blocks:
                z_factor_context = self._run_block(block, z_factor_context, latent, None)
        else:
            latent = z_base.new_empty(bsz, 0, self.d_model)
            market_context = z_base
            market_key_mask = safe_mask
            z_factor_context = z_base

        market_tokens = self.market_queries.expand(bsz, -1, -1)
        for block in self.market_blocks:
            market_tokens = self._run_block(block, market_tokens, market_context, market_key_mask)

        z_market_context = z_base
        for block in self.stock_read_market_blocks:
            z_market_context = self._run_block(block, z_market_context, market_tokens, None)

        z_stock = self.portfolio_fusion(torch.cat([z_base, z_factor_context, z_market_context], dim=-1))
        z_stock = z_stock.masked_fill(~safe_mask.unsqueeze(-1), 0.0)
        return z_stock, {
            "token_embedding": h,
            "stock_embedding": z_base,
            "latent_factors": latent,
            "market_tokens": market_tokens,
            "z_factor_context": z_factor_context,
            "z_market_context": z_market_context,
        }

    def forward(
        self,
        x: torch.Tensor,
        mask: torch.Tensor | None = None,
        temperature: float | torch.Tensor | None = None,
        return_aux: bool | None = None,
    ):
        self._check_shapes(x, mask)
        if mask is None:
            mask_bool = torch.ones(x.size(0), x.size(2), dtype=torch.bool, device=x.device)
        else:
            mask_bool = mask.to(device=x.device, dtype=torch.bool)
        safe_mask = _safe_attention_mask(mask_bool)

        h = self._embed_inputs(x)
        if self.attention_mode == "full":
            z_stock, aux = self._forward_full(h, safe_mask)
        elif self.attention_mode == "axial":
            z_stock, aux = self._forward_axial(h, safe_mask)
        elif self.attention_mode == "latent":
            z_stock, aux = self._forward_latent_or_market(h, safe_mask, use_latent=True)
        elif self.attention_mode == "market_token":
            z_stock, aux = self._forward_latent_or_market(h, safe_mask, use_latent=False)
        else:
            z_stock, aux = self._forward_temporal_only(h, safe_mask)

        z_stock = z_stock.masked_fill(~mask_bool.unsqueeze(-1), 0.0)
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

        aux = dict(aux)
        aux.update(
            {
                "z_stock": z_stock,
                "score_logits": scores,
                "rank_logits": scores,
            }
        )

        if return_aux is True:
            return weights, masked_scores, aux
        if return_aux is None and self.return_aux:
            output = {
                "weights": weights,
                "scores": masked_scores,
                "score_logits": scores,
                "rank_logits": scores,
            }
            if self.return_aux_details:
                output["aux"] = aux
                output.update(aux)
            return output
        return weights
