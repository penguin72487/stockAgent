from __future__ import annotations

import math

import torch
import torch.nn.functional as F
from torch import nn
from torch.utils.checkpoint import checkpoint as activation_checkpoint

from stockagent.models.latent_factor_market_token_portfolio import _safe_attention_mask
from stockagent.models.normalization import dual_branch_softmax, masked_cross_sectional_mean, masked_softmax


class PortfolioRMSNorm(nn.Module):
    """RMSNorm for transformer blocks without forcing a PyTorch version dependency."""

    def __init__(self, dim: int, eps: float = 1e-6) -> None:
        super().__init__()
        self.weight = nn.Parameter(torch.ones(int(dim)))
        self.eps = float(eps)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        variance = x.float().pow(2).mean(dim=-1, keepdim=True)
        x_norm = x * torch.rsqrt(variance + self.eps).to(dtype=x.dtype)
        return x_norm * self.weight.to(device=x.device, dtype=x.dtype)


def _normalize_norm_type(norm_type: str) -> str:
    normalized = str(norm_type).strip().lower().replace("-", "_")
    if normalized in {"rms", "rms_norm", "rmsnorm"}:
        return "rmsnorm"
    if normalized in {"layer", "layer_norm", "layernorm"}:
        return "layernorm"
    raise ValueError("norm_type must be 'rmsnorm' or 'layernorm'")


def _normalize_ffn_type(ffn_type: str) -> str:
    normalized = str(ffn_type).strip().lower().replace("-", "_")
    if normalized in {"swiglu", "swi_glu", "silu_glu"}:
        return "swiglu"
    if normalized in {"gelu", "mlp"}:
        return "gelu"
    raise ValueError("ffn_type must be 'swiglu' or 'gelu'")


def _make_norm(dim: int, norm_type: str) -> nn.Module:
    norm_type = _normalize_norm_type(norm_type)
    if norm_type == "rmsnorm":
        return PortfolioRMSNorm(int(dim))
    return nn.LayerNorm(int(dim))


def _round_up_to_multiple(value: float, multiple: int = 8) -> int:
    multiple = max(1, int(multiple))
    return int(math.ceil(float(value) / multiple) * multiple)


def _ffn_hidden_dim(dim: int, ffn_mult: int, ffn_type: str) -> int:
    dim = int(dim)
    ffn_mult = max(1, int(ffn_mult))
    if _normalize_ffn_type(ffn_type) == "swiglu":
        return max(8, _round_up_to_multiple(dim * ffn_mult * 2.0 / 3.0, 8))
    return max(dim, dim * ffn_mult)


class SwiGLUFeedForward(nn.Module):
    def __init__(self, dim: int, hidden_dim: int, dropout: float) -> None:
        super().__init__()
        self.gate_proj = nn.Linear(int(dim), int(hidden_dim))
        self.value_proj = nn.Linear(int(dim), int(hidden_dim))
        self.out_proj = nn.Linear(int(hidden_dim), int(dim))
        self.dropout = nn.Dropout(float(dropout))
        self.out_dropout = nn.Dropout(float(dropout))

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        hidden = F.silu(self.gate_proj(x)) * self.value_proj(x)
        hidden = self.dropout(hidden)
        return self.out_dropout(self.out_proj(hidden))


class GELUFeedForward(nn.Module):
    def __init__(self, dim: int, hidden_dim: int, dropout: float) -> None:
        super().__init__()
        self.net = nn.Sequential(
            nn.Linear(int(dim), int(hidden_dim)),
            nn.GELU(),
            nn.Dropout(float(dropout)),
            nn.Linear(int(hidden_dim), int(dim)),
            nn.Dropout(float(dropout)),
        )

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return self.net(x)


class GatedProjection(nn.Module):
    def __init__(self, in_dim: int, out_dim: int, dropout: float, ffn_type: str) -> None:
        super().__init__()
        self.ffn_type = _normalize_ffn_type(ffn_type)
        if self.ffn_type == "swiglu":
            self.proj = nn.Linear(int(in_dim), int(out_dim) * 2)
        else:
            self.proj = nn.Linear(int(in_dim), int(out_dim))
        self.dropout = nn.Dropout(float(dropout))

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        projected = self.proj(x)
        if self.ffn_type == "swiglu":
            gate, value = projected.chunk(2, dim=-1)
            projected = F.silu(gate) * value
        else:
            projected = F.gelu(projected)
        return self.dropout(projected)


def _apply_rope(x: torch.Tensor, positions: torch.Tensor, base: float = 10000.0) -> torch.Tensor:
    rot_dim = (int(x.size(-1)) // 2) * 2
    if rot_dim <= 0:
        return x
    positions = positions.to(device=x.device, dtype=torch.float32)
    inv_freq = torch.arange(0, rot_dim, 2, device=x.device, dtype=torch.float32)
    inv_freq = torch.pow(float(base), -inv_freq / float(rot_dim))
    angles = positions[:, None] * inv_freq[None, :]
    cos = angles.cos().to(dtype=x.dtype)[None, None, :, :]
    sin = angles.sin().to(dtype=x.dtype)[None, None, :, :]

    x_rot = x[..., :rot_dim]
    x_pass = x[..., rot_dim:]
    x_even = x_rot[..., 0::2]
    x_odd = x_rot[..., 1::2]
    rotated = torch.stack((x_even * cos - x_odd * sin, x_even * sin + x_odd * cos), dim=-1)
    rotated = rotated.flatten(start_dim=-2)
    if x_pass.numel() == 0:
        return rotated
    return torch.cat([rotated, x_pass], dim=-1)


def _build_rope_cache(max_steps: int, dim: int, base: float) -> tuple[torch.Tensor, torch.Tensor]:
    rot_dim = (int(dim) // 2) * 2
    max_steps = max(0, int(max_steps))
    if max_steps <= 0 or rot_dim <= 0:
        empty = torch.empty(0, dtype=torch.float32)
        return empty, empty
    positions = torch.arange(max_steps, dtype=torch.float32)
    inv_freq = torch.arange(0, rot_dim, 2, dtype=torch.float32)
    inv_freq = torch.pow(float(base), -inv_freq / float(rot_dim))
    angles = positions[:, None] * inv_freq[None, :]
    return angles.cos(), angles.sin()


def _rms_normalize_last_dim(x: torch.Tensor, eps: float = 1e-6) -> torch.Tensor:
    variance = x.float().pow(2).mean(dim=-1, keepdim=True)
    return x * torch.rsqrt(variance + eps).to(dtype=x.dtype)


def _masked_market_summary_parts(z_stock: torch.Tensor, mask: torch.Tensor) -> torch.Tensor:
    mask_bool = mask.to(device=z_stock.device, dtype=torch.bool)
    weights = mask_bool.to(dtype=z_stock.dtype).unsqueeze(-1)
    denom = weights.sum(dim=1).clamp_min(1.0)
    mean = (z_stock * weights).sum(dim=1) / denom
    centered = (z_stock - mean.unsqueeze(1)) * weights
    variance = centered.float().pow(2).sum(dim=1) / denom.float()
    std = torch.sqrt(variance.clamp_min(0.0) + 1e-6).to(dtype=z_stock.dtype)
    return torch.stack([mean, std], dim=1)


def _gate_logit(init_value: float) -> float:
    value = min(max(float(init_value), 1e-4), 1.0 - 1e-4)
    return math.log(value / (1.0 - value))


class DynamicTokenGenerator(nn.Module):
    """Generate input-conditioned latent or market query deltas from market summary."""

    def __init__(
        self,
        *,
        dim: int,
        num_tokens: int,
        hidden_mult: int,
        dropout: float,
        gate_init: float,
        norm_type: str,
        ffn_type: str,
    ) -> None:
        super().__init__()
        self.dim = int(dim)
        self.num_tokens = max(1, int(num_tokens))
        summary_dim = self.dim * 2
        hidden_dim = max(self.dim, _round_up_to_multiple(self.dim * max(1, int(hidden_mult)), 8))
        self.summary_norm = _make_norm(summary_dim, norm_type)
        self.summary_proj = GatedProjection(summary_dim, hidden_dim, dropout, ffn_type)
        self.out_proj = nn.Linear(hidden_dim, self.num_tokens * self.dim)
        self.delta_dropout = nn.Dropout(float(dropout))
        self.gate_logit = nn.Parameter(torch.tensor(_gate_logit(gate_init), dtype=torch.float32))

    def forward(
        self,
        base_queries: torch.Tensor,
        z_stock: torch.Tensor,
        mask: torch.Tensor,
        *,
        collect_aux: bool = True,
    ) -> tuple[torch.Tensor, dict[str, torch.Tensor]]:
        bsz = int(z_stock.size(0))
        summary_parts = _masked_market_summary_parts(z_stock, mask)
        summary = summary_parts.flatten(start_dim=1)
        hidden = self.summary_proj(self.summary_norm(summary))
        delta = self.out_proj(hidden).reshape(bsz, self.num_tokens, self.dim)
        delta = self.delta_dropout(delta)
        gate = torch.sigmoid(self.gate_logit).to(device=delta.device, dtype=delta.dtype)
        base = base_queries.expand(bsz, -1, -1)
        dynamic = base + gate * delta
        if not collect_aux:
            return dynamic, {}
        return dynamic, {
            "delta": delta,
            "gate": gate.reshape(1),
            "summary_parts": summary_parts,
            "queries": dynamic,
        }


class FlashSDPAAttention(nn.Module):
    """Multi-head attention backed by PyTorch SDPA.

    On CUDA, PyTorch chooses flash / memory-efficient / math kernels according
    to dtype, shape, and backend flags. The module also has a manual fallback so
    tests can disable SDPA deterministically.
    """

    def __init__(
        self,
        dim: int,
        num_heads: int,
        dropout: float,
        use_flash_attention: bool = True,
        sdpa_batch_limit: int = 4096,
        qk_norm: bool = True,
        rope_base: float = 10000.0,
        max_rope_steps: int = 0,
    ) -> None:
        super().__init__()
        self.dim = int(dim)
        self.num_heads = max(1, int(num_heads))
        if self.dim % self.num_heads != 0:
            raise ValueError("dim must be divisible by num_heads")
        self.head_dim = self.dim // self.num_heads
        self.scale = float(self.head_dim) ** -0.5
        self.use_flash_attention = bool(use_flash_attention)
        self.sdpa_batch_limit = int(sdpa_batch_limit)
        self.qk_norm = bool(qk_norm)
        self.rope_base = float(rope_base)

        self.in_proj = nn.Linear(self.dim, self.dim * 3)
        self.out_proj = nn.Linear(self.dim, self.dim)
        self.dropout_p = float(dropout)
        rope_cos, rope_sin = _build_rope_cache(int(max_rope_steps), self.head_dim, self.rope_base)
        self.register_buffer("rope_cos_cached", rope_cos, persistent=False)
        self.register_buffer("rope_sin_cached", rope_sin, persistent=False)

    def _reshape_heads(self, tensor: torch.Tensor) -> torch.Tensor:
        bsz, steps, _ = tensor.shape
        return (
            tensor
            .reshape(bsz, steps, self.num_heads, self.head_dim)
            .transpose(1, 2)
        )

    def _project_self(self, tensor: torch.Tensor) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
        bsz, steps, _ = tensor.shape
        qkv = self.in_proj(tensor).reshape(bsz, steps, 3, self.num_heads, self.head_dim)
        qkv = qkv.permute(2, 0, 3, 1, 4)
        q, k, v = qkv.unbind(dim=0)
        return q, k, v

    def _project_cross(
        self,
        query: torch.Tensor,
        context: torch.Tensor,
    ) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
        q_weight = self.in_proj.weight[: self.dim]
        kv_weight = self.in_proj.weight[self.dim :]
        if self.in_proj.bias is None:
            q_bias = None
            kv_bias = None
        else:
            q_bias = self.in_proj.bias[: self.dim]
            kv_bias = self.in_proj.bias[self.dim :]
        q = self._reshape_heads(F.linear(query, q_weight, q_bias))
        bsz, key_steps, _ = context.shape
        kv = F.linear(context, kv_weight, kv_bias).reshape(bsz, key_steps, 2, self.num_heads, self.head_dim)
        kv = kv.permute(2, 0, 3, 1, 4)
        k, v = kv.unbind(dim=0)
        return q, k, v

    def _apply_cached_rope(self, x: torch.Tensor, positions: torch.Tensor) -> torch.Tensor:
        rot_dim = (int(x.size(-1)) // 2) * 2
        if rot_dim <= 0:
            return x
        positions = positions.to(device=x.device)
        if int(self.rope_cos_cached.numel()) == 0:
            positions = positions.to(dtype=torch.float32)
            return _apply_rope(x, positions, base=self.rope_base)
        pos_idx = positions.to(dtype=torch.long)
        cos_cache = self.rope_cos_cached.to(device=x.device, dtype=x.dtype)
        sin_cache = self.rope_sin_cached.to(device=x.device, dtype=x.dtype)
        cos = cos_cache.index_select(0, pos_idx)[None, None, :, :]
        sin = sin_cache.index_select(0, pos_idx)[None, None, :, :]
        x_rot = x[..., :rot_dim]
        x_pass = x[..., rot_dim:]
        x_even = x_rot[..., 0::2]
        x_odd = x_rot[..., 1::2]
        rotated = torch.stack((x_even * cos - x_odd * sin, x_even * sin + x_odd * cos), dim=-1)
        rotated = rotated.flatten(start_dim=-2)
        if x_pass.numel() == 0:
            return rotated
        return torch.cat([rotated, x_pass], dim=-1)

    def forward(
        self,
        query: torch.Tensor,
        context: torch.Tensor | None = None,
        key_mask: torch.Tensor | None = None,
        rope_positions: torch.Tensor | None = None,
        query_rope_positions: torch.Tensor | None = None,
        key_rope_positions: torch.Tensor | None = None,
    ) -> torch.Tensor:
        bsz, query_steps, _ = query.shape
        if context is None:
            key_steps = query_steps
            q, k, v = self._project_self(query)
        else:
            key_steps = int(context.size(1))
            q, k, v = self._project_cross(query, context)
        if rope_positions is not None and int(rope_positions.numel()) >= max(query_steps, key_steps):
            query_positions = rope_positions[:query_steps] if query_rope_positions is None else query_rope_positions
            key_positions = rope_positions[:key_steps] if key_rope_positions is None else key_rope_positions
            q = self._apply_cached_rope(q, query_positions)
            k = self._apply_cached_rope(k, key_positions)
        if self.qk_norm:
            q = _rms_normalize_last_dim(q)
            k = _rms_normalize_last_dim(k)

        attn_mask = None
        if key_mask is not None:
            key_mask = key_mask.to(device=query.device, dtype=torch.bool)
            attn_mask = key_mask[:, None, None, :].expand(bsz, self.num_heads, query_steps, key_steps)

        if self.use_flash_attention:
            if self.sdpa_batch_limit > 0 and int(q.size(0)) > self.sdpa_batch_limit:
                chunks: list[torch.Tensor] = []
                for start in range(0, int(q.size(0)), self.sdpa_batch_limit):
                    end = min(start + self.sdpa_batch_limit, int(q.size(0)))
                    mask_chunk = attn_mask[start:end] if attn_mask is not None else None
                    chunks.append(
                        F.scaled_dot_product_attention(
                            q[start:end],
                            k[start:end],
                            v[start:end],
                            attn_mask=mask_chunk,
                            dropout_p=self.dropout_p if self.training else 0.0,
                            is_causal=False,
                        )
                    )
                y = torch.cat(chunks, dim=0)
            else:
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
        sdpa_batch_limit: int,
        norm_type: str,
        ffn_type: str,
        qk_norm: bool,
        rope_base: float,
        max_rope_steps: int = 0,
    ) -> None:
        super().__init__()
        norm_type = _normalize_norm_type(norm_type)
        ffn_type = _normalize_ffn_type(ffn_type)
        self.norm_query = _make_norm(int(dim), norm_type)
        self.norm_context = _make_norm(int(dim), norm_type)
        self.attn = FlashSDPAAttention(
            dim=int(dim),
            num_heads=int(num_heads),
            dropout=float(dropout),
            use_flash_attention=bool(use_flash_attention),
            sdpa_batch_limit=int(sdpa_batch_limit),
            qk_norm=bool(qk_norm),
            rope_base=float(rope_base),
            max_rope_steps=int(max_rope_steps),
        )
        self.resid_dropout = nn.Dropout(float(dropout))
        self.norm_ffn = _make_norm(int(dim), norm_type)
        hidden_dim = _ffn_hidden_dim(int(dim), int(ffn_mult), ffn_type)
        if ffn_type == "swiglu":
            self.ffn = SwiGLUFeedForward(int(dim), hidden_dim, float(dropout))
        else:
            self.ffn = GELUFeedForward(int(dim), hidden_dim, float(dropout))

    def forward(
        self,
        query: torch.Tensor,
        context: torch.Tensor | None = None,
        key_mask: torch.Tensor | None = None,
        rope_positions: torch.Tensor | None = None,
        query_rope_positions: torch.Tensor | None = None,
        key_rope_positions: torch.Tensor | None = None,
    ) -> torch.Tensor:
        if context is None:
            attn_out = self.attn(
                self.norm_query(query),
                None,
                key_mask=key_mask,
                rope_positions=rope_positions,
                query_rope_positions=query_rope_positions,
                key_rope_positions=key_rope_positions,
            )
        else:
            attn_out = self.attn(
                self.norm_query(query),
                self.norm_context(context),
                key_mask=key_mask,
                rope_positions=rope_positions,
                query_rope_positions=query_rope_positions,
                key_rope_positions=key_rope_positions,
            )
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
        sdpa_batch_limit: int = 4096,
        norm_type: str = "rmsnorm",
        ffn_type: str = "swiglu",
        qk_norm: bool = True,
        rope_temporal: bool = True,
        rope_base: float = 10000.0,
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
        self.sdpa_batch_limit = int(sdpa_batch_limit)
        self.norm_type = _normalize_norm_type(norm_type)
        self.ffn_type = _normalize_ffn_type(ffn_type)
        self.qk_norm = bool(qk_norm)
        self.rope_temporal = bool(rope_temporal)
        self.rope_base = float(rope_base)
        self.dynamic_latent_tokens = bool(dynamic_latent_tokens)
        self.dynamic_market_tokens = bool(dynamic_market_tokens)

        self.feature_proj = nn.Linear(self.num_features, self.d_model)
        self.input_dropout = nn.Dropout(float(input_dropout))
        self.time_position = nn.Parameter(torch.randn(1, self.lookback, 1, self.d_model) * 0.02)
        self.symbol_position = nn.Parameter(torch.randn(1, 1, self.num_symbols, self.d_model) * 0.02)
        self.register_buffer(
            "temporal_rope_positions",
            torch.arange(self.lookback, dtype=torch.float32),
            persistent=False,
        )

        def make_block(num_heads: int, ffn_mult: int) -> TransformerPortfolioBlock:
            return TransformerPortfolioBlock(
                dim=self.d_model,
                num_heads=int(num_heads),
                ffn_mult=int(ffn_mult),
                dropout=float(dropout),
                use_flash_attention=bool(use_flash_attention),
                sdpa_batch_limit=self.sdpa_batch_limit,
                norm_type=self.norm_type,
                ffn_type=self.ffn_type,
                qk_norm=self.qk_norm,
                rope_base=self.rope_base,
                max_rope_steps=self.lookback,
            )

        self.temporal_blocks = nn.ModuleList(
            [
                make_block(int(temporal_heads), int(temporal_ffn_mult))
                for _ in range(max(0, int(temporal_layers)))
            ]
        )
        self.cross_blocks = nn.ModuleList(
            [
                make_block(int(cross_heads), int(cross_ffn_mult))
                for _ in range(max(0, int(cross_layers)))
            ]
        )
        self.joint_blocks = nn.ModuleList(
            [
                make_block(int(joint_heads), int(joint_ffn_mult))
                for _ in range(max(0, int(joint_layers)))
            ]
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
            if self.dynamic_latent_tokens
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
            if self.dynamic_market_tokens
            else None
        )
        self.latent_blocks = nn.ModuleList(
            [
                make_block(int(cross_heads), int(cross_ffn_mult))
                for _ in range(max(1, int(latent_layers)))
            ]
        )
        self.market_blocks = nn.ModuleList(
            [
                make_block(int(cross_heads), int(cross_ffn_mult))
                for _ in range(max(1, int(market_layers)))
            ]
        )
        self.stock_read_latent_blocks = nn.ModuleList(
            [
                make_block(int(cross_heads), int(cross_ffn_mult))
                for _ in range(max(1, int(market_layers)))
            ]
        )
        self.stock_read_market_blocks = nn.ModuleList(
            [
                make_block(int(cross_heads), int(cross_ffn_mult))
                for _ in range(max(1, int(market_layers)))
            ]
        )

        self.temporal_pool_score = nn.Linear(self.d_model, 1) if self.temporal_pooling == "attention" else None
        self.output_norm = _make_norm(self.d_model, self.norm_type)
        self.portfolio_fusion = nn.Sequential(
            GatedProjection(self.d_model * 3, self.d_model, float(dropout), self.ffn_type),
            _make_norm(self.d_model, self.norm_type),
        )

        head: list[nn.Module] = []
        in_dim = self.d_model
        for _ in range(max(0, int(head_layers))):
            head.extend(
                [
                    GatedProjection(in_dim, int(head_hidden_dim), float(dropout), self.ffn_type),
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

    def _temporal_rope_positions(self, steps: int, device: torch.device) -> torch.Tensor | None:
        if not self.rope_temporal:
            return None
        if steps <= int(self.temporal_rope_positions.numel()):
            return self.temporal_rope_positions[:steps].to(device=device)
        return torch.arange(steps, device=device, dtype=torch.float32)

    @staticmethod
    def _prefixed_aux(prefix: str, values: dict[str, torch.Tensor]) -> dict[str, torch.Tensor]:
        return {f"{prefix}_{name}": value for name, value in values.items()}

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

    def _apply_temporal_blocks(self, h: torch.Tensor, *, keep_all_steps: bool = False) -> torch.Tensor:
        bsz, steps, n_symbols, dim = h.shape
        seq = h.permute(0, 2, 1, 3).contiguous().reshape(bsz * n_symbols, steps, dim)
        rope_positions = self._temporal_rope_positions(steps, h.device)
        use_last_query_fast_path = (
            self.temporal_pooling == "last"
            and not bool(keep_all_steps)
            and len(self.temporal_blocks) > 0
            and steps > 1
        )
        blocks = list(self.temporal_blocks)
        full_blocks = blocks[:-1] if use_last_query_fast_path else blocks
        for block in full_blocks:
            seq = self._run_block(block, seq, None, None, rope_positions)
        if use_last_query_fast_path:
            last_query = seq[:, -1:, :]
            last_pos = None if rope_positions is None else rope_positions[-1:]
            seq = self._run_block(
                blocks[-1],
                last_query,
                seq,
                None,
                rope_positions,
                last_pos,
                rope_positions,
            )
            steps = 1
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

    def _forward_full(
        self,
        h: torch.Tensor,
        safe_mask: torch.Tensor,
        *,
        collect_aux: bool,
    ) -> tuple[torch.Tensor, dict[str, torch.Tensor]]:
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
        aux = {"token_embedding": h_full} if collect_aux else {}
        return self._pool_temporal(h_full, safe_mask), aux

    def _forward_axial(
        self,
        h: torch.Tensor,
        safe_mask: torch.Tensor,
        *,
        collect_aux: bool,
    ) -> tuple[torch.Tensor, dict[str, torch.Tensor]]:
        h = self._apply_temporal_blocks(h, keep_all_steps=collect_aux)
        h = self._apply_cross_blocks(h, safe_mask)
        aux = {"token_embedding": h} if collect_aux else {}
        return self._pool_temporal(h, safe_mask), aux

    def _forward_temporal_only(
        self,
        h: torch.Tensor,
        safe_mask: torch.Tensor,
        *,
        collect_aux: bool,
    ) -> tuple[torch.Tensor, dict[str, torch.Tensor]]:
        h = self._apply_temporal_blocks(h, keep_all_steps=collect_aux)
        aux = {"token_embedding": h} if collect_aux else {}
        return self._pool_temporal(h, safe_mask), aux

    def _forward_latent_or_market(
        self,
        h: torch.Tensor,
        safe_mask: torch.Tensor,
        *,
        use_latent: bool,
        collect_aux: bool,
    ) -> tuple[torch.Tensor, dict[str, torch.Tensor]]:
        h = self._apply_temporal_blocks(h, keep_all_steps=collect_aux)
        z_base = self._pool_temporal(h, safe_mask)
        bsz = int(h.size(0))
        aux: dict[str, torch.Tensor] = {}

        if use_latent:
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
            market_tokens = self.market_queries.expand(bsz, -1, -1)
        for block in self.market_blocks:
            market_tokens = self._run_block(block, market_tokens, market_context, market_key_mask)

        z_market_context = z_base
        for block in self.stock_read_market_blocks:
            z_market_context = self._run_block(block, z_market_context, market_tokens, None)

        z_stock = self.portfolio_fusion(torch.cat([z_base, z_factor_context, z_market_context], dim=-1))
        z_stock = z_stock.masked_fill(~safe_mask.unsqueeze(-1), 0.0)
        if collect_aux:
            aux.update({
                "token_embedding": h,
                "stock_embedding": z_base,
                "latent_factors": latent,
                "market_tokens": market_tokens,
                "z_factor_context": z_factor_context,
                "z_market_context": z_market_context,
            })
        return z_stock, aux

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
        collect_aux = bool(return_aux is True or (return_aux is None and self.return_aux and self.return_aux_details))

        h = self._embed_inputs(x)
        if self.attention_mode == "full":
            z_stock, aux = self._forward_full(h, safe_mask, collect_aux=collect_aux)
        elif self.attention_mode == "axial":
            z_stock, aux = self._forward_axial(h, safe_mask, collect_aux=collect_aux)
        elif self.attention_mode == "latent":
            z_stock, aux = self._forward_latent_or_market(h, safe_mask, use_latent=True, collect_aux=collect_aux)
        elif self.attention_mode == "market_token":
            z_stock, aux = self._forward_latent_or_market(h, safe_mask, use_latent=False, collect_aux=collect_aux)
        else:
            z_stock, aux = self._forward_temporal_only(h, safe_mask, collect_aux=collect_aux)

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

        if return_aux is True:
            aux = dict(aux)
            aux.update(
                {
                    "z_stock": z_stock,
                    "score_logits": scores,
                    "rank_logits": scores,
                }
            )
            return weights, masked_scores, aux
        if return_aux is None and self.return_aux:
            output = {
                "weights": weights,
                "scores": masked_scores,
                "score_logits": scores,
                "rank_logits": scores,
            }
            if self.return_aux_details:
                aux = dict(aux)
                aux.update(
                    {
                        "z_stock": z_stock,
                        "score_logits": scores,
                        "rank_logits": scores,
                    }
                )
                output["aux"] = aux
                output.update(aux)
            return output
        return weights
