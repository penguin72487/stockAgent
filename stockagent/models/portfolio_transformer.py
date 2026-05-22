from __future__ import annotations

import warnings

import torch
import torch.nn.functional as F
from torch import nn

from stockagent.models.base import AlphaModel, normalize_portfolio

flash_attn_func = None
xops = None
te = None


def _get_flash_attn_func():
    global flash_attn_func
    if flash_attn_func is not None:
        return flash_attn_func
    try:
        from flash_attn import flash_attn_func as _flash_attn_func  # type: ignore
        flash_attn_func = _flash_attn_func
        return flash_attn_func
    except Exception:
        return None


def _get_xops():
    global xops
    if xops is not None:
        return xops
    try:
        import xformers.ops as _xops  # type: ignore
        xops = _xops
        return xops
    except Exception:
        return None


def _get_transformer_engine():
    global te
    if te is not None:
        return te
    try:
        import transformer_engine.pytorch as _te  # type: ignore
        te = _te
        return te
    except Exception:
        return None


def _cuda_sm() -> int:
    if not torch.cuda.is_available():
        return 0
    major, minor = torch.cuda.get_device_capability()
    return major * 10 + minor


class FastAttention(nn.Module):
    def __init__(
        self,
        d_model: int,
        nhead: int,
        dropout: float,
        backend: str = "auto",
    ) -> None:
        super().__init__()
        if d_model % nhead != 0:
            raise ValueError(f"d_model ({d_model}) must be divisible by nhead ({nhead})")
        self.d_model = int(d_model)
        self.nhead = int(nhead)
        self.head_dim = d_model // nhead
        self.dropout = float(dropout)
        self.backend = str(backend).lower()

        self.q_proj = nn.Linear(d_model, d_model)
        self.k_proj = nn.Linear(d_model, d_model)
        self.v_proj = nn.Linear(d_model, d_model)
        self.out_proj = nn.Linear(d_model, d_model)

    def _choose_backend(self, query: torch.Tensor) -> str:
        if not query.is_cuda:
            return "sdpa"
        if self.backend == "sdpa":
            return "sdpa"
        if self.backend == "xformers":
            return "xformers" if _get_xops() is not None else "sdpa"
        if self.backend in {"flash", "flash_attn", "flash_attn_2"}:
            return "flash_attn_2" if _get_flash_attn_func() is not None and _cuda_sm() >= 80 else "sdpa"

        # auto mode: prefer FlashAttention2, then xFormers, then SDPA
        if _get_flash_attn_func() is not None and _cuda_sm() >= 80:
            return "flash_attn_2"
        if _get_xops() is not None:
            return "xformers"
        return "sdpa"

    def _shape(self, x: torch.Tensor) -> torch.Tensor:
        # [B, T, D] -> [B, T, H, Hd]
        bsz, seq, _ = x.shape
        return x.view(bsz, seq, self.nhead, self.head_dim)

    def forward(
        self,
        query: torch.Tensor,
        key: torch.Tensor | None = None,
        value: torch.Tensor | None = None,
    ) -> torch.Tensor:
        if key is None:
            key = query
        if value is None:
            value = key

        q = self._shape(self.q_proj(query))
        k = self._shape(self.k_proj(key))
        v = self._shape(self.v_proj(value))

        backend = self._choose_backend(query)
        p = self.dropout if self.training else 0.0

        if backend == "flash_attn_2":
            try:
                flash = _get_flash_attn_func()
                if flash is None:
                    raise RuntimeError("flash_attn_func unavailable")
                attn = flash(q, k, v, dropout_p=p, causal=False)
            except Exception:
                backend = "sdpa"
        if backend == "xformers":
            try:
                xops_mod = _get_xops()
                if xops_mod is None:
                    raise RuntimeError("xformers.ops unavailable")
                attn = xops_mod.memory_efficient_attention(q, k, v, p=p)
            except Exception:
                backend = "sdpa"
        if backend == "sdpa":
            # [B, T, H, Hd] -> [B, H, T, Hd]
            qh = q.transpose(1, 2)
            kh = k.transpose(1, 2)
            vh = v.transpose(1, 2)
            attn = F.scaled_dot_product_attention(
                qh,
                kh,
                vh,
                attn_mask=None,
                dropout_p=p,
                is_causal=False,
            )
            attn = attn.transpose(1, 2)

        out = attn.reshape(query.size(0), query.size(1), self.d_model)
        return self.out_proj(out)


class FastTransformerBlock(nn.Module):
    def __init__(
        self,
        d_model: int,
        nhead: int,
        ff_dim: int,
        dropout: float,
        attention_backend: str,
    ) -> None:
        super().__init__()
        self.norm1 = nn.LayerNorm(d_model)
        self.norm2 = nn.LayerNorm(d_model)
        self.attn = FastAttention(
            d_model=d_model,
            nhead=nhead,
            dropout=dropout,
            backend=attention_backend,
        )
        self.ffn = nn.Sequential(
            nn.Linear(d_model, ff_dim),
            nn.GELU(),
            nn.Dropout(dropout),
            nn.Linear(ff_dim, d_model),
            nn.Dropout(dropout),
        )

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        x = x + self.attn(self.norm1(x))
        x = x + self.ffn(self.norm2(x))
        return x


class TimeTransformerEncoder(nn.Module):
    def __init__(
        self,
        d_model: int,
        nhead: int,
        ff_dim: int,
        layers: int,
        dropout: float,
        max_lookback: int,
        attention_backend: str,
    ) -> None:
        super().__init__()
        self.position = nn.Parameter(torch.randn(1, max_lookback, d_model) * 0.02)
        self.blocks = nn.ModuleList(
            [
                FastTransformerBlock(
                    d_model=d_model,
                    nhead=nhead,
                    ff_dim=ff_dim,
                    dropout=dropout,
                    attention_backend=attention_backend,
                )
                for _ in range(max(1, layers))
            ]
        )
        self.norm = nn.LayerNorm(d_model)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        # x: [N, L, D]
        seq_len = x.size(1)
        if seq_len > self.position.size(1):
            raise ValueError(f"Input lookback {seq_len} exceeds configured max_lookback {self.position.size(1)}")
        h = x + self.position[:, :seq_len, :]
        for block in self.blocks:
            h = block(h)
        return self.norm(h[:, -1, :])


class PortfolioTransformerModel(AlphaModel):
    """Portfolio Transformer style model with direct weight output and mask-aware normalization.

    Flow:
    [B, L, S, F] -> per-asset time embedding -> time Transformer -> [B, S, D]
    -> cross-asset attention decoder -> [B, S] weights
    """

    def __init__(
        self,
        *,
        num_features: int,
        num_symbols: int,
        long_only: bool,
        lookback: int = 20,
        d_model: int = 128,
        time_layers: int = 2,
        time_nhead: int = 4,
        time_ff_dim: int = 256,
        cross_layers: int = 2,
        cross_nhead: int = 4,
        cross_ff_dim: int = 256,
        decoder_queries: int = 4,
        max_time_batch: int = 32768,
        attention_backend: str = "auto",
        use_transformer_engine: bool = False,
        use_fp8: bool = False,
        dropout: float = 0.1,
    ) -> None:
        super().__init__(lookback=lookback, long_only=long_only)
        self.num_symbols = int(num_symbols)
        self.max_time_batch = max(1, int(max_time_batch))
        self.attention_backend = str(attention_backend).lower()

        if use_transformer_engine:
            if _get_transformer_engine() is None:
                warnings.warn("Transformer Engine requested but not installed; falling back to native PyTorch kernels.")
            elif _cuda_sm() < 80:
                warnings.warn("Transformer Engine requested but GPU SM < 80; falling back to native PyTorch kernels.")

        if use_fp8:
            if _get_transformer_engine() is None:
                warnings.warn("FP8 requested but Transformer Engine is unavailable; falling back to bf16/fp16 kernels.")
            elif _cuda_sm() < 89:
                warnings.warn("FP8 requested but GPU SM < 89; falling back to bf16/fp16 kernels.")

        self.input_proj = nn.Sequential(
            nn.Linear(num_features, d_model),
            nn.GELU(),
            nn.Dropout(dropout),
        )

        self.time_encoder = TimeTransformerEncoder(
            d_model=d_model,
            nhead=time_nhead,
            ff_dim=time_ff_dim,
            layers=time_layers,
            dropout=dropout,
            max_lookback=max(lookback, 1),
            attention_backend=self.attention_backend,
        )

        self.cross_blocks = nn.ModuleList(
            [
                FastTransformerBlock(
                    d_model=d_model,
                    nhead=cross_nhead,
                    ff_dim=cross_ff_dim,
                    dropout=dropout,
                    attention_backend=self.attention_backend,
                )
                for _ in range(max(1, cross_layers))
            ]
        )
        self.cross_norm = nn.LayerNorm(d_model)

        q = max(1, int(decoder_queries))
        self.portfolio_queries = nn.Parameter(torch.randn(q, d_model) * 0.02)
        self.decoder_attn = FastAttention(
            d_model=d_model,
            nhead=cross_nhead,
            dropout=dropout,
            backend=self.attention_backend,
        )
        self.asset_gate = nn.Sequential(
            nn.LayerNorm(d_model),
            nn.Linear(d_model, d_model),
            nn.GELU(),
            nn.Dropout(dropout),
            nn.Linear(d_model, 1),
        )
        self.context_gate = nn.Sequential(
            nn.LayerNorm(d_model),
            nn.Linear(d_model, d_model),
            nn.GELU(),
            nn.Dropout(dropout),
            nn.Linear(d_model, 1),
        )

    def _encode_time(self, x: torch.Tensor) -> torch.Tensor:
        # x: [B, L, S, F]
        bsz, lookback, symbols, feats = x.shape
        if symbols != self.num_symbols:
            raise ValueError(f"Expected symbols={self.num_symbols}, got {symbols}")

        h = x.reshape(bsz * symbols, lookback, feats)
        h = self.input_proj(h)

        # Efficient attention kernels on CUDA can fail when the batch dimension
        # is too large. Chunking keeps batch within a safe range while preserving
        # sequence semantics for each asset.
        total = int(h.size(0))
        if total <= self.max_time_batch:
            encoded = self.time_encoder(h)
        else:
            chunks: list[torch.Tensor] = []
            for start in range(0, total, self.max_time_batch):
                end = min(start + self.max_time_batch, total)
                chunks.append(self.time_encoder(h[start:end]))
            encoded = torch.cat(chunks, dim=0)

        return encoded.reshape(bsz, symbols, -1)

    def forward(self, x: torch.Tensor, tradable_mask: torch.Tensor | None = None) -> torch.Tensor:
        if x.dim() != 4:
            raise ValueError(f"Expected [B, L, S, F], got shape {tuple(x.shape)}")

        per_asset = self._encode_time(x)  # [B, S, D]
        cross_asset = per_asset
        for block in self.cross_blocks:
            cross_asset = block(cross_asset)
        cross_asset = self.cross_norm(cross_asset)

        queries = self.portfolio_queries.unsqueeze(0).expand(cross_asset.size(0), -1, -1)
        decoded = self.decoder_attn(queries, cross_asset, cross_asset)
        context = decoded.mean(dim=1, keepdim=True)  # [B, 1, D]

        asset_logits = self.asset_gate(cross_asset).squeeze(-1)
        context_logits = self.context_gate(context).squeeze(-1)  # [B, 1]
        logits = asset_logits + context_logits

        return normalize_portfolio(logits, tradable_mask, self.long_only)
