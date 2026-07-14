from __future__ import annotations

import math
from collections.abc import Sequence

import torch
import torch.nn.functional as F
from torch import nn
from torch.utils.checkpoint import checkpoint as activation_checkpoint

from stockagent.models.latent_factor_market_token_portfolio import _safe_attention_mask
from stockagent.models.normalization import (
    dual_branch_softmax,
    finite_mask_fill_value,
    masked_l1_projection_weights,
    masked_cross_sectional_mean_finite,
    masked_signed_action_weights,
    masked_softmax,
    normalize_portfolio_activation,
)
from stockagent.portfolio_contract import normalize_portfolio_mode, normalize_portfolio_output_mode
from stockagent.profiling import PROFILE_RANGES_ENABLED, _torch_is_compiling, profile_range


def _sanitize_scores_to_dtype(scores: torch.Tensor) -> torch.Tensor:
    """Keep finite scores unchanged and replace non-finite values within dtype bounds."""
    return torch.nan_to_num(scores, nan=0.0)


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


def _masked_market_summary_parts(z_stock: torch.Tensor, mask: torch.Tensor) -> torch.Tensor:
    mask_bool = mask.to(device=z_stock.device, dtype=torch.bool)
    weights = mask_bool.to(dtype=z_stock.dtype).unsqueeze(-1)
    denom = weights.sum(dim=1).clamp_min(1.0)
    mean = (z_stock * weights).sum(dim=1) / denom
    centered = (z_stock - mean.unsqueeze(1)) * weights
    variance = centered.float().pow(2).sum(dim=1) / denom.float()
    std = torch.sqrt(variance.clamp_min(0.0) + 1e-6).to(dtype=z_stock.dtype)
    dispersion = centered.abs().sum(dim=1) / denom
    return torch.stack([mean, std, dispersion], dim=1)


class LegacyDynamicTokenGenerator(nn.Module):
    """Exact inference compatibility for checkpoints trained before static tokens."""

    def __init__(
        self,
        *,
        dim: int,
        num_tokens: int,
        hidden_dim: int,
        norm_type: str,
        ffn_type: str,
    ) -> None:
        super().__init__()
        summary_dim = int(dim) * 3
        self.dim = int(dim)
        self.num_tokens = int(num_tokens)
        self.summary_norm = _make_norm(summary_dim, norm_type)
        self.summary_proj = GatedProjection(summary_dim, int(hidden_dim), 0.0, ffn_type)
        self.out_proj = nn.Linear(int(hidden_dim), self.num_tokens * self.dim)
        self.delta_dropout = nn.Dropout(0.0)
        self.gate_logit = nn.Parameter(torch.zeros((), dtype=torch.float32))

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
        dynamic = base_queries.expand(bsz, -1, -1) + gate * delta
        if not collect_aux:
            return dynamic, {}
        return dynamic, {
            "delta": delta,
            "gate": gate.reshape(1),
            "summary_parts": summary_parts,
            "queries": dynamic,
        }


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


def _compiled_cross_attention_requires_blackwell_workaround(
    device: torch.device | str | int | None = None,
) -> bool:
    """Keep the known compiled-BF16 workaround scoped to Blackwell GPUs."""
    if not torch.cuda.is_available():
        return False
    if isinstance(device, (torch.device, str)) and torch.device(device).type != "cuda":
        return False
    try:
        major, _minor = (
            torch.cuda.get_device_capability()
            if device is None
            else torch.cuda.get_device_capability(device)
        )
    except (AssertionError, RuntimeError):
        return False
    return int(major) >= 12


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
        self.compiled_cross_attention_blackwell_workaround = (
            _compiled_cross_attention_requires_blackwell_workaround()
        )
        self.compiled_cross_attention_backend = "auto"

        self.in_proj = nn.Linear(self.dim, self.dim * 3)
        self.out_proj = nn.Linear(self.dim, self.dim)
        self.dropout_p = float(dropout)
        rope_cos, rope_sin = _build_rope_cache(int(max_rope_steps), self.head_dim, self.rope_base)
        self.register_buffer("rope_cos_cached", rope_cos, persistent=False)
        self.register_buffer("rope_sin_cached", rope_sin, persistent=False)
        self.capture_attention = False
        self.capture_name = ""
        self.capture_max_rows = 4
        self.capture_max_elements = 2_000_000
        self.captured_attention: torch.Tensor | None = None
        self.captured_attention_shape: tuple[int, ...] | None = None

    def _apply(self, fn, recurse: bool = True):
        module = super()._apply(fn, recurse=recurse)
        module.compiled_cross_attention_blackwell_workaround = (
            _compiled_cross_attention_requires_blackwell_workaround(module.in_proj.weight.device)
        )
        return module

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
            attn_mask = key_mask[:, None, None, :]

        self.captured_attention = None
        self.captured_attention_shape = None
        if bool(self.capture_attention):
            cap_rows = max(1, min(int(self.capture_max_rows), int(q.size(0))))
            capture_elements = cap_rows * int(query_steps) * int(key_steps)
            if capture_elements <= max(1, int(self.capture_max_elements)):
                capture_scores = torch.matmul(q[:cap_rows], k[:cap_rows].transpose(-2, -1)) * self.scale
                if attn_mask is not None:
                    capture_scores = capture_scores.masked_fill(
                        ~attn_mask[:cap_rows],
                        torch.finfo(capture_scores.dtype).min,
                    )
                capture_attn = torch.softmax(capture_scores, dim=-1)
                self.captured_attention = capture_attn.mean(dim=1).detach().float().cpu()
                self.captured_attention_shape = tuple(int(dim) for dim in capture_attn.shape)

        use_sdpa_attention = bool(self.use_flash_attention)
        if (
            use_sdpa_attention
            and context is not None
            and query.device.type == "cuda"
            and _torch_is_compiling()
            and q.dtype in {torch.float16, torch.bfloat16}
        ):
            # Small temporal last-query attention is faster through the explicit
            # path on Ada, while the large market-token cross-attention benefits
            # from SDPA. Blackwell keeps the conservative workaround because its
            # compiled BF16 cross-attention kernel is unstable in this stack.
            attention_elements = int(query_steps) * int(key_steps)
            backend = str(self.compiled_cross_attention_backend)
            if (
                backend == "manual"
                or self.compiled_cross_attention_blackwell_workaround
                or (backend == "auto" and attention_elements <= 4096)
            ):
                use_sdpa_attention = False

        if use_sdpa_attention:
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
            if query_steps == 1 and _torch_is_compiling() and not bool(self.capture_attention):
                # The active temporal fast path has N=B*S independent
                # [1 x L] attentions. Two batched GEMMs materialize tiny matrices
                # and scale poorly at that very large N, while direct reductions
                # express the identical dot-product/weighted-sum algebra and let
                # Inductor fuse the elementwise work around them.
                scores = (q * k).sum(dim=-1) * self.scale
                if attn_mask is not None:
                    scores = scores.masked_fill(
                        ~attn_mask.squeeze(-2),
                        torch.finfo(scores.dtype).min,
                    )
                attn = torch.softmax(scores, dim=-1)
                attn = F.dropout(attn, p=self.dropout_p, training=self.training)
                y = (attn.unsqueeze(-1) * v).sum(dim=-2, keepdim=True)
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
        context_is_self_sequence: bool = False,
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
            normalized_context = (
                self.norm_query(context)
                if bool(context_is_self_sequence)
                else self.norm_context(context)
            )
            attn_out = self.attn(
                self.norm_query(query),
                normalized_context,
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
    - latent:     O(S*L^2 + S*K + K*M + S*(K+M)), factor + market bottlenecks.
    - latent_only: O(S*L^2 + S*K), latent-factor bottleneck without market tokens.
    - market_token: O(S*L^2 + S*M), market-token bottleneck.
    - temporal_only: O(S*L^2), no cross-stock attention.

    ``use_latent_factors`` and ``use_market_tokens`` independently select the
    compact bottlenecks.  When omitted, ``attention_mode`` keeps its historical
    preset semantics so existing configs and checkpoints remain compatible.
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
        symbol_position_capacity: int | None = None,
        input_dropout: float = 0.0,
        sanitize_inputs: bool = True,
        amp_native_position_add: bool = False,
        temporal_self_attention_fast_path: bool = False,
        compiled_cross_attention_backend: str = "auto",
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
        temporal_query_mode: str = "full_then_last",
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
        portfolio_activation: str = "identity",
        portfolio_output_mode: str = "activation_l1",
        center_long_short_logits: bool = True,
        max_full_tokens: int = 4096,
        checkpoint_blocks: bool = False,
        return_aux: bool = True,
        return_aux_details: bool = False,
        runtime_shape_check: bool = False,
        allow_dynamic_symbols: bool = True,
        categorical_feature_indices: Sequence[int] | None = None,
        categorical_embedding_dim: int = 4,
        categorical_embedding_cardinality: int = 512,
        use_latent_factors: bool | None = None,
        use_market_tokens: bool | None = None,
    ) -> None:
        super().__init__()
        self.lookback = int(lookback)
        self.num_features = int(num_features)
        self.num_symbols = int(num_symbols)
        self.d_model = int(d_model)
        resolved_symbol_capacity = (
            self.num_symbols if symbol_position_capacity is None else int(symbol_position_capacity)
        )
        if resolved_symbol_capacity <= 0:
            raise ValueError("symbol_position_capacity must be positive")
        self.symbol_position_capacity = resolved_symbol_capacity
        self.requested_attention_mode = self._normalize_attention_mode(attention_mode)
        self.attention_mode = self._resolve_attention_mode(
            self.requested_attention_mode,
            use_latent_factors=use_latent_factors,
            use_market_tokens=use_market_tokens,
        )
        self.use_latent_factors = self.attention_mode in {"latent", "latent_only"}
        self.use_market_tokens = self.attention_mode in {"latent", "market_token"}
        self.temporal_pooling = self._normalize_pooling(temporal_pooling)
        self.temporal_query_mode = self._normalize_temporal_query_mode(temporal_query_mode)
        self.default_temperature = float(default_temperature)
        self.portfolio_mode = normalize_portfolio_mode(portfolio_mode)
        self.portfolio_activation = normalize_portfolio_activation(portfolio_activation)
        self.portfolio_output_mode = normalize_portfolio_output_mode(portfolio_output_mode)
        self.center_long_short_logits = bool(center_long_short_logits)
        self.max_full_tokens = int(max_full_tokens)
        self.checkpoint_blocks = bool(checkpoint_blocks)
        self.return_aux = bool(return_aux)
        self.return_aux_details = bool(return_aux_details)
        self.runtime_shape_check = bool(runtime_shape_check)
        self.allow_dynamic_symbols = bool(allow_dynamic_symbols)
        self.use_time_pos = bool(use_time_pos)
        self.use_symbol_pos = bool(use_symbol_pos)
        self.sanitize_inputs = bool(sanitize_inputs)
        self.amp_native_position_add = bool(amp_native_position_add)
        self.temporal_self_attention_fast_path = bool(temporal_self_attention_fast_path)
        compiled_cross_backend = str(compiled_cross_attention_backend).strip().lower().replace("-", "_")
        if compiled_cross_backend not in {"auto", "manual", "sdpa"}:
            raise ValueError("compiled_cross_attention_backend must be 'auto', 'manual', or 'sdpa'")
        self.compiled_cross_attention_backend = compiled_cross_backend
        self.sdpa_batch_limit = int(sdpa_batch_limit)
        self.norm_type = _normalize_norm_type(norm_type)
        self.ffn_type = _normalize_ffn_type(ffn_type)
        self.qk_norm = bool(qk_norm)
        self.rope_temporal = bool(rope_temporal)
        self.rope_base = float(rope_base)

        raw_categorical_indices = tuple(int(idx) for idx in (categorical_feature_indices or ()))
        categorical_indices: list[int] = []
        seen_categorical: set[int] = set()
        for idx in raw_categorical_indices:
            if idx < 0 or idx >= self.num_features or idx in seen_categorical:
                continue
            categorical_indices.append(idx)
            seen_categorical.add(idx)
        self.categorical_feature_indices = tuple(categorical_indices)
        self.categorical_embedding_dim = max(1, int(categorical_embedding_dim))
        self.categorical_embedding_cardinality = max(2, int(categorical_embedding_cardinality))

        self.feature_proj = nn.Linear(self.num_features, self.d_model)
        if self.categorical_feature_indices:
            self.register_buffer(
                "categorical_feature_index_tensor",
                torch.tensor(self.categorical_feature_indices, dtype=torch.long),
                persistent=False,
            )
            self.categorical_embeddings = nn.ModuleList(
                [
                    nn.Embedding(self.categorical_embedding_cardinality + 1, self.categorical_embedding_dim)
                    for _ in self.categorical_feature_indices
                ]
            )
            self.categorical_proj = nn.Linear(
                len(self.categorical_feature_indices) * self.categorical_embedding_dim,
                self.d_model,
                bias=False,
            )
        else:
            self.register_buffer(
                "categorical_feature_index_tensor",
                torch.empty((0,), dtype=torch.long),
                persistent=False,
            )
            self.categorical_embeddings = nn.ModuleList()
            self.categorical_proj = None
        self.input_dropout = nn.Dropout(float(input_dropout))
        self.time_position = nn.Parameter(torch.randn(1, self.lookback, 1, self.d_model) * 0.02)
        self.symbol_position = nn.Parameter(
            torch.randn(1, 1, self.symbol_position_capacity, self.d_model) * 0.02
        )
        self.register_buffer(
            "temporal_rope_positions",
            torch.arange(self.lookback, dtype=torch.float32),
            persistent=False,
        )

        def make_block(num_heads: int, ffn_mult: int) -> TransformerPortfolioBlock:
            block = TransformerPortfolioBlock(
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
            block.attn.compiled_cross_attention_backend = self.compiled_cross_attention_backend
            return block

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
            if self.attention_mode == "axial"
            else []
        )
        self.joint_blocks = nn.ModuleList(
            [
                make_block(int(joint_heads), int(joint_ffn_mult))
                for _ in range(max(0, int(joint_layers)))
            ]
            if self.attention_mode == "full"
            else []
        )

        latent_count = max(1, int(num_latent_factors))
        market_count = max(1, int(num_market_tokens))
        self.latent_queries = (
            nn.Parameter(torch.randn(1, latent_count, self.d_model) * 0.02)
            if self.use_latent_factors
            else None
        )
        self.market_queries = (
            nn.Parameter(torch.randn(1, market_count, self.d_model) * 0.02)
            if self.use_market_tokens
            else None
        )
        # Populated only by strict legacy-checkpoint reconstruction.
        self.dynamic_latent_generator: LegacyDynamicTokenGenerator | None = None
        self.dynamic_market_generator: LegacyDynamicTokenGenerator | None = None
        self.latent_blocks = nn.ModuleList(
            [
                make_block(int(cross_heads), int(cross_ffn_mult))
                for _ in range(max(1, int(latent_layers)))
            ]
            if self.use_latent_factors
            else []
        )
        self.market_blocks = nn.ModuleList(
            [
                make_block(int(cross_heads), int(cross_ffn_mult))
                for _ in range(max(1, int(market_layers)))
            ]
            if self.use_market_tokens
            else []
        )
        self.stock_read_latent_blocks = nn.ModuleList(
            [
                make_block(int(cross_heads), int(cross_ffn_mult))
                for _ in range(max(1, int(market_layers)))
            ]
            if self.use_latent_factors
            else []
        )
        self.stock_read_market_blocks = nn.ModuleList(
            [
                make_block(int(cross_heads), int(cross_ffn_mult))
                for _ in range(max(1, int(market_layers)))
            ]
            if self.use_market_tokens
            else []
        )

        self.temporal_pool_score = nn.Linear(self.d_model, 1) if self.temporal_pooling == "attention" else None
        self.output_norm = _make_norm(self.d_model, self.norm_type)
        self.stock_market_gate = nn.Sequential(
            GatedProjection(self.d_model * 2, self.d_model, float(dropout), self.ffn_type),
            nn.Linear(self.d_model, 1),
        )
        self.stock_market_norm = _make_norm(self.d_model, self.norm_type)
        if self.attention_mode == "latent_only":
            # The new factor-only path deliberately bypasses the market gate.
            # Freeze only this new mode so every historical preset retains its
            # exact trainable-parameter contract.
            self.stock_market_gate.requires_grad_(False)

        def make_scalar_head() -> nn.Sequential:
            head: list[nn.Module] = []
            in_dim = self.d_model
            for _ in range(max(0, int(head_layers))):
                head.append(GatedProjection(in_dim, int(head_hidden_dim), float(dropout), self.ffn_type))
                in_dim = int(head_hidden_dim)
            head.append(nn.Linear(in_dim, 1))
            return nn.Sequential(*head)

        self.score_head = make_scalar_head()

    def enable_legacy_dynamic_token_checkpoint_compatibility(
        self,
        state_dict: dict[str, torch.Tensor],
    ) -> None:
        """Reconstruct removed token generators from their complete saved schema."""

        def build(prefix: str, base_queries: torch.Tensor | None) -> LegacyDynamicTokenGenerator | None:
            keys = {
                "gate_logit",
                "summary_norm.weight",
                "summary_proj.proj.weight",
                "summary_proj.proj.bias",
                "out_proj.weight",
                "out_proj.bias",
            }
            present = {key[len(prefix) :] for key in state_dict if key.startswith(prefix)}
            if not present:
                return None
            if present != keys:
                missing = sorted(keys - present)
                unexpected = sorted(present - keys)
                raise RuntimeError(
                    f"Incomplete legacy dynamic-token checkpoint schema for {prefix}: "
                    f"missing={missing}, unexpected={unexpected}"
                )
            if base_queries is None:
                raise RuntimeError(f"Legacy dynamic-token checkpoint has {prefix} but no base queries")

            summary_dim = self.d_model * 3
            norm_weight = state_dict[prefix + "summary_norm.weight"]
            proj_weight = state_dict[prefix + "summary_proj.proj.weight"]
            out_weight = state_dict[prefix + "out_proj.weight"]
            out_bias = state_dict[prefix + "out_proj.bias"]
            gate = state_dict[prefix + "gate_logit"]
            num_tokens = int(base_queries.size(1))
            if tuple(norm_weight.shape) != (summary_dim,) or gate.numel() != 1:
                raise RuntimeError(f"Invalid legacy dynamic-token norm/gate shape for {prefix}")
            if int(out_weight.size(0)) != num_tokens * self.d_model:
                raise RuntimeError(f"Invalid legacy dynamic-token output shape for {prefix}")
            hidden_dim = int(out_weight.size(1))
            if tuple(out_bias.shape) != (num_tokens * self.d_model,):
                raise RuntimeError(f"Invalid legacy dynamic-token output bias shape for {prefix}")
            if int(proj_weight.size(1)) != summary_dim:
                raise RuntimeError(f"Invalid legacy dynamic-token projection input shape for {prefix}")
            if int(proj_weight.size(0)) == hidden_dim * 2:
                ffn_type = "swiglu"
            elif int(proj_weight.size(0)) == hidden_dim:
                ffn_type = "gelu"
            else:
                raise RuntimeError(f"Invalid legacy dynamic-token projection output shape for {prefix}")
            if tuple(state_dict[prefix + "summary_proj.proj.bias"].shape) != (int(proj_weight.size(0)),):
                raise RuntimeError(f"Invalid legacy dynamic-token projection bias shape for {prefix}")

            module = LegacyDynamicTokenGenerator(
                dim=self.d_model,
                num_tokens=num_tokens,
                hidden_dim=hidden_dim,
                norm_type=self.norm_type,
                ffn_type=ffn_type,
            )
            return module.to(device=base_queries.device, dtype=base_queries.dtype)

        latent = build("dynamic_latent_generator.", self.latent_queries)
        market = build("dynamic_market_generator.", self.market_queries)
        if latent is not None:
            self.dynamic_latent_generator = latent
        if market is not None:
            self.dynamic_market_generator = market

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
            "factor_only": "latent_only",
            "latent_factor_only": "latent_only",
            "market": "market_token",
            "market_tokens": "market_token",
            "none": "temporal_only",
            "temporal": "temporal_only",
        }
        normalized = aliases.get(normalized, normalized)
        if normalized in {
            "full",
            "axial",
            "latent",
            "latent_only",
            "market_token",
            "temporal_only",
        }:
            return normalized
        raise ValueError(
            "attention_mode must be one of: full, axial, latent, latent_only, "
            "market_token, temporal_only"
        )

    @classmethod
    def _resolve_attention_mode(
        cls,
        attention_mode: str,
        *,
        use_latent_factors: bool | None,
        use_market_tokens: bool | None,
    ) -> str:
        """Resolve legacy presets plus the two independent bottleneck switches."""
        requested = cls._normalize_attention_mode(attention_mode)
        for name, value in (
            ("use_latent_factors", use_latent_factors),
            ("use_market_tokens", use_market_tokens),
        ):
            if value is not None and type(value) is not bool:
                raise TypeError(f"{name} must be bool or None, got {type(value).__name__}")

        preset_latent = requested in {"latent", "latent_only"}
        preset_market = requested in {"latent", "market_token"}
        resolved_latent = preset_latent if use_latent_factors is None else use_latent_factors
        resolved_market = preset_market if use_market_tokens is None else use_market_tokens

        if requested in {"full", "axial"}:
            if resolved_latent or resolved_market:
                raise ValueError(
                    "use_latent_factors/use_market_tokens cannot enable compact bottlenecks "
                    f"with attention_mode={requested}; set both to false/null or choose a "
                    "compact attention_mode"
                )
            return requested
        if resolved_latent and resolved_market:
            return "latent"
        if resolved_latent:
            return "latent_only"
        if resolved_market:
            return "market_token"
        return "temporal_only"

    @staticmethod
    def _normalize_pooling(pooling: str) -> str:
        normalized = str(pooling).strip().lower().replace("-", "_")
        if normalized in {"last", "mean", "attention", "attn"}:
            return "attention" if normalized == "attn" else normalized
        raise ValueError("temporal_pooling must be one of: last, mean, attention")

    @staticmethod
    def _normalize_temporal_query_mode(mode: str) -> str:
        normalized = str(mode).strip().lower().replace("-", "_")
        if normalized in {"full_then_last", "default", "current"}:
            return "full_then_last"
        if normalized in {"last_only", "last_query_only"}:
            return "last_only"
        raise ValueError("temporal_query_mode must be 'full_then_last' or 'last_only'")

    def _check_symbol_indices(self, symbol_indices: torch.Tensor | None, n_symbols: int) -> None:
        if symbol_indices is None:
            return
        if symbol_indices.dim() != 1:
            raise ValueError(f"Expected symbol_indices shape [S], got ndim={symbol_indices.dim()}")
        if int(symbol_indices.numel()) != int(n_symbols):
            raise ValueError(
                f"Expected symbol_indices length {int(n_symbols)}, got {int(symbol_indices.numel())}"
            )
        if int(symbol_indices.numel()) == 0:
            return
        if _torch_is_compiling():
            return
        idx_cpu = symbol_indices.detach().to(device="cpu", dtype=torch.long)
        min_idx = int(idx_cpu.min().item())
        max_idx = int(idx_cpu.max().item())
        if min_idx < 0 or max_idx >= self.num_symbols:
            raise ValueError(
                f"symbol_indices must be in [0, {self.num_symbols}), got min={min_idx}, max={max_idx}"
            )

    def _check_shapes(
        self,
        x: torch.Tensor,
        mask: torch.Tensor | None,
        symbol_indices: torch.Tensor | None = None,
    ) -> None:
        if x.dim() != 4:
            raise ValueError(f"Expected x shape [B,L,S,F], got ndim={x.dim()}")
        if int(x.size(1)) != self.lookback:
            raise ValueError(f"Expected lookback={self.lookback}, got {int(x.size(1))}")
        if (not self.allow_dynamic_symbols) and symbol_indices is None and int(x.size(2)) != self.num_symbols:
            raise ValueError(f"Expected num_symbols={self.num_symbols}, got {int(x.size(2))}")
        self._check_symbol_indices(symbol_indices, int(x.size(2)))
        if int(x.size(3)) != self.num_features:
            raise ValueError(f"Expected num_features={self.num_features}, got {int(x.size(3))}")
        if mask is not None and tuple(mask.shape) != (int(x.size(0)), int(x.size(2))):
            raise ValueError(f"Expected mask shape {(int(x.size(0)), int(x.size(2)))}, got {tuple(mask.shape)}")

    def _check_panel_shapes(
        self,
        features: torch.Tensor,
        date_indices: torch.Tensor,
        mask: torch.Tensor | None,
        symbol_indices: torch.Tensor | None = None,
    ) -> None:
        if features.dim() != 3:
            raise ValueError(f"Expected features shape [T,S,F], got ndim={features.dim()}")
        if date_indices.dim() != 1:
            raise ValueError(f"Expected date_indices shape [B], got ndim={date_indices.dim()}")
        if (not self.allow_dynamic_symbols) and symbol_indices is None and int(features.size(1)) != self.num_symbols:
            raise ValueError(f"Expected num_symbols={self.num_symbols}, got {int(features.size(1))}")
        self._check_symbol_indices(symbol_indices, int(features.size(1)))
        if int(features.size(2)) != self.num_features:
            raise ValueError(f"Expected num_features={self.num_features}, got {int(features.size(2))}")
        if mask is not None and tuple(mask.shape) != (int(date_indices.numel()), int(features.size(1))):
            raise ValueError(
                f"Expected mask shape {(int(date_indices.numel()), int(features.size(1)))}, got {tuple(mask.shape)}"
            )
        if int(date_indices.numel()) == 0:
            return
        idx_cpu = date_indices.detach().to(device="cpu", dtype=torch.long)
        min_idx = int(idx_cpu.min().item())
        max_idx = int(idx_cpu.max().item())
        if min_idx < self.lookback - 1:
            raise ValueError(
                f"date_indices must be >= lookback - 1 ({self.lookback - 1}), got min={min_idx}"
            )
        if max_idx >= int(features.size(0)):
            raise ValueError(f"date_indices must be < T ({int(features.size(0))}), got max={max_idx}")

    def _check_panel_slab_shapes(
        self,
        feature_slab: torch.Tensor,
        mask: torch.Tensor | None,
        symbol_indices: torch.Tensor | None = None,
    ) -> None:
        if feature_slab.dim() != 3:
            raise ValueError(f"Expected feature_slab shape [U,S,F], got ndim={feature_slab.dim()}")
        if int(feature_slab.size(0)) < self.lookback:
            raise ValueError(
                f"Expected feature_slab rows >= lookback={self.lookback}, got {int(feature_slab.size(0))}"
            )
        if (not self.allow_dynamic_symbols) and symbol_indices is None and int(feature_slab.size(1)) != self.num_symbols:
            raise ValueError(f"Expected num_symbols={self.num_symbols}, got {int(feature_slab.size(1))}")
        self._check_symbol_indices(symbol_indices, int(feature_slab.size(1)))
        if int(feature_slab.size(2)) != self.num_features:
            raise ValueError(f"Expected num_features={self.num_features}, got {int(feature_slab.size(2))}")
        batch_rows = int(feature_slab.size(0)) - self.lookback + 1
        if mask is not None and tuple(mask.shape) != (batch_rows, int(feature_slab.size(1))):
            raise ValueError(f"Expected mask shape {(batch_rows, int(feature_slab.size(1)))}, got {tuple(mask.shape)}")

    def configure_attention_capture(
        self,
        enabled: bool,
        *,
        max_rows: int = 4,
        max_elements: int = 2_000_000,
    ) -> None:
        """Opt-in attention capture for explainability without changing normal training."""
        for name, module in self.named_modules():
            if isinstance(module, FlashSDPAAttention):
                module.capture_attention = bool(enabled)
                module.capture_name = str(name)
                module.capture_max_rows = max(1, int(max_rows))
                module.capture_max_elements = max(1, int(max_elements))
                module.captured_attention = None
                module.captured_attention_shape = None

    def pop_attention_capture(self) -> list[dict[str, object]]:
        captures: list[dict[str, object]] = []
        for name, module in self.named_modules():
            if isinstance(module, FlashSDPAAttention) and module.captured_attention is not None:
                captures.append(
                    {
                        "name": module.capture_name or name,
                        "attention": module.captured_attention,
                        "shape": module.captured_attention_shape,
                    }
                )
                module.captured_attention = None
                module.captured_attention_shape = None
        return captures

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

    def _symbol_position(self, n_symbols: int, symbol_indices: torch.Tensor | None = None) -> torch.Tensor:
        if symbol_indices is not None:
            indices = symbol_indices.to(device=self.symbol_position.device, dtype=torch.long)
            capacity = int(self.symbol_position.size(2))
            valid = indices.ge(0) & indices.lt(capacity)
            safe_indices = indices.clamp(0, capacity - 1)
            positions = self.symbol_position.index_select(2, safe_indices)
            return positions * valid.view(1, 1, -1, 1).to(dtype=positions.dtype)
        if n_symbols <= int(self.symbol_position.size(2)):
            return self.symbol_position[:, :, :n_symbols, :]
        extra = self.symbol_position.new_zeros(
            1,
            1,
            n_symbols - int(self.symbol_position.size(2)),
            self.d_model,
        )
        return torch.cat([self.symbol_position, extra], dim=2)

    def _project_features(self, x: torch.Tensor) -> torch.Tensor:
        model_device = self.feature_proj.weight.device
        if not self.sanitize_inputs and not self.categorical_feature_indices:
            return self.feature_proj(x.to(device=model_device))
        clean_fp32 = x.to(device=model_device, dtype=torch.float32)
        if self.sanitize_inputs:
            clean_fp32 = torch.nan_to_num(
                clean_fp32,
                nan=0.0,
                posinf=0.0,
                neginf=0.0,
            )
        clean = clean_fp32.to(dtype=self.feature_proj.weight.dtype)
        if not self.categorical_feature_indices:
            return self.feature_proj(clean)

        cat_idx = self.categorical_feature_index_tensor
        cat_values = clean_fp32.index_select(-1, cat_idx)
        continuous = clean.clone()
        continuous.index_fill_(-1, cat_idx, 0.0)
        projected = self.feature_proj(continuous)

        cat_ids = torch.round(cat_values).to(dtype=torch.long).clamp_(0, self.categorical_embedding_cardinality)
        cat_parts = [
            embedding(cat_ids[..., idx])
            for idx, embedding in enumerate(self.categorical_embeddings)
        ]
        cat_embedding = torch.cat(cat_parts, dim=-1)
        if self.categorical_proj is None:
            return projected
        return projected + self.categorical_proj(cat_embedding).to(dtype=projected.dtype)

    def _embed_inputs(self, x: torch.Tensor, symbol_indices: torch.Tensor | None = None) -> torch.Tensor:
        h = self._project_features(x)
        if self.use_time_pos:
            time_position = self.time_position[:, : int(x.size(1)), :, :]
            if self.amp_native_position_add:
                time_position = time_position.to(dtype=h.dtype)
            h = h + time_position
        if self.use_symbol_pos:
            symbol_position = self._symbol_position(int(x.size(2)), symbol_indices)
            if self.amp_native_position_add:
                symbol_position = symbol_position.to(dtype=h.dtype)
            h = h + symbol_position
        return self.input_dropout(h)

    def _add_window_positions(
        self,
        h: torch.Tensor,
        n_symbols: int,
        symbol_indices: torch.Tensor | None = None,
    ) -> torch.Tensor:
        if self.use_time_pos:
            time_position = self.time_position[:, : self.lookback, :, :]
            if self.amp_native_position_add:
                time_position = time_position.to(dtype=h.dtype)
            h = h + time_position
        if self.use_symbol_pos:
            symbol_position = self._symbol_position(int(n_symbols), symbol_indices)
            if self.amp_native_position_add:
                symbol_position = symbol_position.to(dtype=h.dtype)
            h = h + symbol_position
        return self.input_dropout(h)

    def _project_panel_rows(self, features: torch.Tensor, row_indices: torch.Tensor) -> torch.Tensor:
        row_indices = row_indices.to(device=features.device, dtype=torch.long)
        selected = features.index_select(0, row_indices)
        return self._project_features(selected)

    def _embed_windowed_from_panel_slab(
        self,
        feature_slab: torch.Tensor,
        symbol_indices: torch.Tensor | None = None,
    ) -> torch.Tensor:
        projected = self._project_features(feature_slab)
        h = projected.unfold(0, self.lookback, 1).permute(0, 3, 1, 2).contiguous()
        return self._add_window_positions(h, int(feature_slab.size(1)), symbol_indices)

    def _embed_windowed_from_panel(
        self,
        features: torch.Tensor,
        date_indices: torch.Tensor,
        symbol_indices: torch.Tensor | None = None,
    ) -> torch.Tensor:
        date_indices_source = date_indices.to(device=features.device, dtype=torch.long)
        batch_rows = int(date_indices_source.numel())
        if batch_rows <= 0:
            h = self.feature_proj.weight.new_empty(
                (0, self.lookback, int(features.size(1)), self.d_model)
            )
        else:
            is_contiguous = True
            if batch_rows > 1:
                diffs = date_indices_source[1:] - date_indices_source[:-1]
                is_contiguous = bool(torch.all(diffs == 1).detach().cpu().item())

            if is_contiguous:
                start = int(date_indices_source[0].detach().cpu().item()) - self.lookback + 1
                end = int(date_indices_source[-1].detach().cpu().item()) + 1
                return self._embed_windowed_from_panel_slab(
                    features.narrow(0, start, end - start),
                    symbol_indices,
                )
            else:
                offsets = torch.arange(
                    self.lookback - 1,
                    -1,
                    -1,
                    device=features.device,
                    dtype=torch.long,
                )
                window_idx = date_indices_source[:, None] - offsets[None, :]
                unique_idx, inverse = torch.unique(window_idx.reshape(-1), sorted=True, return_inverse=True)
                projected = self._project_panel_rows(features, unique_idx)
                h = projected.index_select(0, inverse.to(device=projected.device, dtype=torch.long))
                h = h.reshape(batch_rows, self.lookback, int(features.size(1)), self.d_model).contiguous()

        return self._add_window_positions(h, int(features.size(1)), symbol_indices)

    def _apply_temporal_blocks(self, h: torch.Tensor, *, keep_all_steps: bool = False) -> torch.Tensor:
        bsz, steps, n_symbols, dim = h.shape
        seq = h.permute(0, 2, 1, 3).contiguous().reshape(bsz * n_symbols, steps, dim)
        rope_positions = self._temporal_rope_positions(steps, h.device)
        if (
            self.temporal_query_mode == "last_only"
            and self.temporal_pooling == "last"
            and not bool(keep_all_steps)
            and len(self.temporal_blocks) > 0
            and steps > 1
        ):
            last_query = seq[:, -1:, :]
            last_pos = None if rope_positions is None else rope_positions[-1:]
            for block in self.temporal_blocks:
                last_query = self._run_block(
                    block,
                    last_query,
                    seq,
                    None,
                    rope_positions,
                    last_pos,
                    rope_positions,
                    self.temporal_self_attention_fast_path,
                )
            return last_query.reshape(bsz, n_symbols, 1, dim).permute(0, 2, 1, 3).contiguous()
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
                self.temporal_self_attention_fast_path,
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

    def _apply_stock_market_gate(
        self,
        z_base: torch.Tensor,
        z_market_context: torch.Tensor,
        safe_mask: torch.Tensor,
    ) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
        market_delta = z_market_context - z_base
        gate_logits = self.stock_market_gate(torch.cat([z_base, z_market_context], dim=-1))
        gate = torch.sigmoid(gate_logits)
        z_stock = self.stock_market_norm(z_base + gate * market_delta)
        z_stock = z_stock.masked_fill(~safe_mask.unsqueeze(-1), 0.0)
        return z_stock, gate.masked_fill(~safe_mask.unsqueeze(-1), 0.0), market_delta

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
        h = self._apply_temporal_blocks(
            h,
            keep_all_steps=(collect_aux and self.temporal_query_mode != "last_only"),
        )
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
        h = self._apply_temporal_blocks(
            h,
            keep_all_steps=(collect_aux and self.temporal_query_mode != "last_only"),
        )
        aux = {"token_embedding": h} if collect_aux else {}
        return self._pool_temporal(h, safe_mask), aux

    def _forward_latent_or_market(
        self,
        h: torch.Tensor,
        safe_mask: torch.Tensor,
        *,
        use_latent: bool,
        collect_aux: bool,
        use_market: bool = True,
    ) -> tuple[torch.Tensor, dict[str, torch.Tensor]]:
        h = self._apply_temporal_blocks(
            h,
            keep_all_steps=(collect_aux and self.temporal_query_mode != "last_only"),
        )
        z_base = self._pool_temporal(h, safe_mask)
        bsz = int(h.size(0))
        aux: dict[str, torch.Tensor] = {}

        if use_latent:
            if self.latent_queries is None:
                raise RuntimeError("latent_queries are required for attention_mode=latent")
            if self.dynamic_latent_generator is not None:
                factor_tokens, dynamic_aux = self.dynamic_latent_generator(
                    self.latent_queries,
                    z_base,
                    safe_mask,
                    collect_aux=collect_aux,
                )
                if collect_aux:
                    aux.update(self._prefixed_aux("dynamic_latent", dynamic_aux))
            else:
                factor_tokens = self.latent_queries.expand(bsz, -1, -1)
            for block in self.latent_blocks:
                factor_tokens = self._run_block(block, factor_tokens, z_base, safe_mask)
            market_context = factor_tokens
            market_key_mask = None
            z_factor_context = z_base
            for block in self.stock_read_latent_blocks:
                z_factor_context = self._run_block(block, z_factor_context, factor_tokens, None)
            z_gate_base = z_factor_context
        else:
            factor_tokens = z_base.new_empty(bsz, 0, self.d_model)
            market_context = z_base
            market_key_mask = safe_mask
            z_factor_context = z_base
            z_gate_base = z_base

        if not use_market:
            if not use_latent:
                raise RuntimeError("latent/market forward requires at least one enabled bottleneck")
            z_stock = self.stock_market_norm(z_factor_context)
            z_stock = z_stock.masked_fill(~safe_mask.unsqueeze(-1), 0.0)
            if collect_aux:
                aux.update(
                    {
                        "token_embedding": h,
                        "stock_embedding": z_base,
                        "factor_tokens": factor_tokens,
                        "latent_factors": factor_tokens,
                        "z_factor_context": z_factor_context,
                    }
                )
            return z_stock, aux

        if self.market_queries is None:
            raise RuntimeError("market_queries are required for latent/market_token attention")
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

        z_stock, stock_market_gate, z_market_delta = self._apply_stock_market_gate(
            z_gate_base,
            z_market_context,
            safe_mask,
        )
        if collect_aux:
            aux.update({
                "token_embedding": h,
                "stock_embedding": z_base,
                "factor_tokens": factor_tokens,
                "latent_factors": factor_tokens,
                "market_tokens": market_tokens,
                "z_factor_context": z_factor_context,
                "z_market_context": z_market_context,
                "z_market_delta": z_market_delta,
                "stock_market_gate": stock_market_gate,
            })
        return z_stock, aux

    def _forward_market_token_fast(
        self,
        h: torch.Tensor,
        safe_mask: torch.Tensor,
        *,
        collect_aux: bool,
    ) -> tuple[torch.Tensor, dict[str, torch.Tensor]]:
        h = self._apply_temporal_blocks(
            h,
            keep_all_steps=(collect_aux and self.temporal_query_mode != "last_only"),
        )
        z_base = self._pool_temporal(h, safe_mask)
        bsz = int(h.size(0))
        aux: dict[str, torch.Tensor] = {}

        if self.market_queries is None:
            raise RuntimeError("market_queries are required for attention_mode=market_token")
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
            market_tokens = self._run_block(block, market_tokens, z_base, safe_mask)

        z_market_context = z_base
        for block in self.stock_read_market_blocks:
            z_market_context = self._run_block(block, z_market_context, market_tokens, None)

        z_stock, stock_market_gate, z_market_delta = self._apply_stock_market_gate(
            z_base,
            z_market_context,
            safe_mask,
        )
        if collect_aux:
            aux.update(
                {
                    "token_embedding": h,
                    "stock_embedding": z_base,
                    "market_tokens": market_tokens,
                    "z_market_context": z_market_context,
                    "z_market_delta": z_market_delta,
                    "stock_market_gate": stock_market_gate,
                }
            )
        return z_stock, aux

    def _forward_embedded(
        self,
        h: torch.Tensor,
        mask_bool: torch.Tensor,
        temperature: float | torch.Tensor | None = None,
        return_aux: bool | None = None,
    ):
        safe_mask = _safe_attention_mask(mask_bool)
        collect_aux = bool(return_aux is True or (return_aux is None and self.return_aux and self.return_aux_details))

        if self.attention_mode == "full":
            z_stock, aux = self._forward_full(h, safe_mask, collect_aux=collect_aux)
        elif self.attention_mode == "axial":
            z_stock, aux = self._forward_axial(h, safe_mask, collect_aux=collect_aux)
        elif self.attention_mode == "latent":
            z_stock, aux = self._forward_latent_or_market(
                h,
                safe_mask,
                use_latent=True,
                use_market=True,
                collect_aux=collect_aux,
            )
        elif self.attention_mode == "latent_only":
            z_stock, aux = self._forward_latent_or_market(
                h,
                safe_mask,
                use_latent=True,
                use_market=False,
                collect_aux=collect_aux,
            )
        elif self.attention_mode == "market_token":
            z_stock, aux = self._forward_market_token_fast(h, safe_mask, collect_aux=collect_aux)
        else:
            z_stock, aux = self._forward_temporal_only(h, safe_mask, collect_aux=collect_aux)

        if PROFILE_RANGES_ENABLED:
            with profile_range("model.portfolio.mask_z_stock"):
                z_stock = z_stock.masked_fill(~mask_bool.unsqueeze(-1), 0.0)
            with profile_range("model.portfolio.score_head"):
                scores = self.score_head(z_stock).squeeze(-1)
            with profile_range("model.portfolio.score_sanitize"):
                scores = _sanitize_scores_to_dtype(scores)
            with profile_range("model.portfolio.score_mask_fill"):
                masked_scores = scores.masked_fill(~mask_bool, finite_mask_fill_value(scores))
        else:
            z_stock = z_stock.masked_fill(~mask_bool.unsqueeze(-1), 0.0)
            scores = self.score_head(z_stock).squeeze(-1)
            scores = _sanitize_scores_to_dtype(scores)
            masked_scores = scores.masked_fill(~mask_bool, finite_mask_fill_value(scores))

        if temperature is None:
            temp = masked_scores.new_tensor(self.default_temperature)
        elif isinstance(temperature, torch.Tensor):
            temp = temperature.to(device=masked_scores.device, dtype=masked_scores.dtype)
        else:
            temp = masked_scores.new_tensor(float(temperature))
        temp = torch.clamp(temp, min=0.05)

        output_aux: dict[str, torch.Tensor] = {}
        include_action_aux = bool(return_aux is True or (return_aux is None and self.return_aux and self.return_aux_details))

        if self.portfolio_mode == "long_only":
            centered_scores = scores
            if PROFILE_RANGES_ENABLED:
                with profile_range("model.portfolio.target_logits"):
                    target_logits = (scores / temp).masked_fill(~mask_bool, 0.0)
            else:
                target_logits = (scores / temp).masked_fill(~mask_bool, 0.0)
            if self.portfolio_output_mode == "logits":
                weights = target_logits
            elif self.portfolio_output_mode == "signed_softmax":
                action_output = masked_signed_action_weights(
                    target_logits,
                    mask_bool,
                    transform="softmax",
                    long_only=True,
                    return_parts=include_action_aux,
                )
                if include_action_aux:
                    weights, output_aux = action_output
                else:
                    weights = action_output
            elif self.portfolio_output_mode == "signed_sparsemax":
                action_output = masked_signed_action_weights(
                    target_logits,
                    mask_bool,
                    transform="sparsemax",
                    long_only=True,
                    return_parts=include_action_aux,
                )
                if include_action_aux:
                    weights, output_aux = action_output
                else:
                    weights = action_output
            elif self.portfolio_output_mode == "signed_entmax15":
                action_output = masked_signed_action_weights(
                    target_logits,
                    mask_bool,
                    transform="entmax15",
                    long_only=True,
                    return_parts=include_action_aux,
                )
                if include_action_aux:
                    weights, output_aux = action_output
                else:
                    weights = action_output
            elif self.portfolio_output_mode == "projection_l1":
                weights = masked_l1_projection_weights(target_logits, mask_bool, long_only=True)
                if include_action_aux:
                    output_aux = {
                        "projection_gross_exposure": weights.abs().sum(dim=1),
                        "implicit_cash_weight": (1.0 - weights.abs().sum(dim=1)).clamp_min(0.0),
                    }
            else:
                weight_activation = "identity" if self.portfolio_output_mode == "l1" else self.portfolio_activation
                weights = masked_softmax(masked_scores / temp, mask_bool, activation=weight_activation)
        else:
            if PROFILE_RANGES_ENABLED:
                with profile_range("model.portfolio.center_scores"):
                    centered_scores = (
                        scores - masked_cross_sectional_mean_finite(scores, mask_bool)
                        if self.center_long_short_logits
                        else scores
                    )
                with profile_range("model.portfolio.target_logits"):
                    target_logits = (centered_scores / temp).masked_fill(~mask_bool, 0.0)
            else:
                centered_scores = (
                    scores - masked_cross_sectional_mean_finite(scores, mask_bool)
                    if self.center_long_short_logits
                    else scores
                )
                target_logits = (centered_scores / temp).masked_fill(~mask_bool, 0.0)
            if self.portfolio_output_mode == "logits":
                weights = target_logits
            elif self.portfolio_output_mode == "signed_softmax":
                action_output = masked_signed_action_weights(
                    target_logits,
                    mask_bool,
                    transform="softmax",
                    long_only=False,
                    return_parts=include_action_aux,
                )
                if include_action_aux:
                    weights, output_aux = action_output
                else:
                    weights = action_output
            elif self.portfolio_output_mode == "signed_sparsemax":
                action_output = masked_signed_action_weights(
                    target_logits,
                    mask_bool,
                    transform="sparsemax",
                    long_only=False,
                    return_parts=include_action_aux,
                )
                if include_action_aux:
                    weights, output_aux = action_output
                else:
                    weights = action_output
            elif self.portfolio_output_mode == "signed_entmax15":
                action_output = masked_signed_action_weights(
                    target_logits,
                    mask_bool,
                    transform="entmax15",
                    long_only=False,
                    return_parts=include_action_aux,
                )
                if include_action_aux:
                    weights, output_aux = action_output
                else:
                    weights = action_output
            elif self.portfolio_output_mode == "projection_l1":
                weights = masked_l1_projection_weights(target_logits, mask_bool, long_only=False)
                if include_action_aux:
                    output_aux = {
                        "projection_gross_exposure": weights.abs().sum(dim=1),
                        "implicit_cash_weight": (1.0 - weights.abs().sum(dim=1)).clamp_min(0.0),
                    }
            else:
                weight_activation = "identity" if self.portfolio_output_mode == "l1" else self.portfolio_activation
                weights = dual_branch_softmax(centered_scores / temp, mask_bool, activation=weight_activation)
        if PROFILE_RANGES_ENABLED:
            with profile_range("model.portfolio.weights_final_mask"):
                weights = weights.masked_fill(~mask_bool, 0.0)
        else:
            weights = weights.masked_fill(~mask_bool, 0.0)

        if return_aux is True:
            aux = dict(aux)
            aux.update(
                {
                    "z_stock": z_stock,
                    "score_logits": scores,
                    "rank_logits": scores,
                    "centered_score_logits": centered_scores,
                }
            )
            aux.update(output_aux)
            return weights, masked_scores, aux
        if return_aux is None and self.return_aux:
            output = {
                "weights": weights,
                "scores": masked_scores,
                "score_logits": scores,
                "rank_logits": scores,
                "centered_score_logits": centered_scores,
            }
            if self.return_aux_details:
                aux = dict(aux)
                aux.update(
                    {
                        "z_stock": z_stock,
                        "score_logits": scores,
                        "rank_logits": scores,
                        "centered_score_logits": centered_scores,
                    }
                )
                aux.update(output_aux)
                output["aux"] = aux
                output.update(aux)
            return output
        return weights

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
        h = self._embed_inputs(x, symbol_indices)
        return self._forward_embedded(
            h,
            mask_bool,
            temperature=temperature,
            return_aux=return_aux,
        )

    def forward_from_panel(
        self,
        features: torch.Tensor,
        date_indices: torch.Tensor,
        mask: torch.Tensor | None = None,
        temperature: float | torch.Tensor | None = None,
        return_aux: bool | None = None,
        symbol_indices: torch.Tensor | None = None,
    ):
        self._check_panel_shapes(features, date_indices, mask, symbol_indices)
        h = self._embed_windowed_from_panel(features, date_indices, symbol_indices)
        if mask is None:
            mask_bool = torch.ones(h.size(0), h.size(2), dtype=torch.bool, device=h.device)
        else:
            mask_bool = mask.to(device=h.device, dtype=torch.bool)
        return self._forward_embedded(
            h,
            mask_bool,
            temperature=temperature,
            return_aux=return_aux,
        )

    def forward_from_panel_slab(
        self,
        feature_slab: torch.Tensor,
        mask: torch.Tensor | None = None,
        temperature: float | torch.Tensor | None = None,
        return_aux: bool | None = None,
        symbol_indices: torch.Tensor | None = None,
    ):
        self._check_panel_slab_shapes(feature_slab, mask, symbol_indices)
        h = self._embed_windowed_from_panel_slab(feature_slab, symbol_indices)
        if mask is None:
            mask_bool = torch.ones(h.size(0), h.size(2), dtype=torch.bool, device=h.device)
        else:
            mask_bool = mask.to(device=h.device, dtype=torch.bool)
        return self._forward_embedded(
            h,
            mask_bool,
            temperature=temperature,
            return_aux=return_aux,
        )
