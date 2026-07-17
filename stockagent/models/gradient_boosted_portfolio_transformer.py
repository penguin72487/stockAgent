from __future__ import annotations

from collections.abc import Sequence

import torch
from torch import nn

from stockagent.models.normalization import (
    finite_mask_fill_value,
    masked_activation_l1_weights,
    masked_cross_sectional_mean,
    masked_l1_projection_weights,
    masked_signed_action_weights,
    normalize_portfolio_activation,
)
from stockagent.portfolio_contract import normalize_portfolio_mode, normalize_portfolio_output_mode


def _repeat_or_trim_eta(values: Sequence[float], count: int) -> list[float]:
    if count <= 0:
        return []
    raw = [float(value) for value in values]
    if not raw:
        raw = [0.5]
    while len(raw) < count:
        raw.append(raw[-1])
    return raw[:count]


def _masked_mean(values: torch.Tensor, mask: torch.Tensor) -> torch.Tensor:
    mask_f = mask.to(dtype=values.dtype).unsqueeze(-1)
    denom = mask_f.sum(dim=1, keepdim=True).clamp_min(1.0)
    return (values * mask_f).sum(dim=1, keepdim=True) / denom


def _safe_attention_mask(mask: torch.Tensor) -> torch.Tensor:
    """Keep at least one unmasked key per row for attention softmax stability."""
    mask_bool = mask.bool()
    if mask_bool.size(1) == 0:
        return mask_bool
    empty_rows = ~mask_bool.any(dim=1)
    fallback = torch.zeros_like(mask_bool)
    fallback[:, 0] = empty_rows
    return mask_bool | fallback


class _RMSNorm(nn.Module):
    def __init__(self, dim: int, eps: float = 1e-6) -> None:
        super().__init__()
        self.weight = nn.Parameter(torch.ones(dim))
        self.eps = float(eps)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        scale = torch.rsqrt(x.float().pow(2).mean(dim=-1, keepdim=True) + self.eps).to(dtype=x.dtype)
        return x * scale * self.weight.to(dtype=x.dtype)


class _FeedForward(nn.Module):
    def __init__(self, dim: int, hidden_dim: int, dropout: float) -> None:
        super().__init__()
        self.net = nn.Sequential(
            nn.Linear(dim, hidden_dim * 2),
            nn.SiLU(),
            nn.Dropout(float(dropout)),
            nn.Linear(hidden_dim * 2, dim),
            nn.Dropout(float(dropout)),
        )

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return self.net(x)


class _SelfAttentionBlock(nn.Module):
    def __init__(
        self,
        dim: int,
        heads: int,
        ffn_mult: int,
        dropout: float,
        *,
        batch_limit: int = 16384,
    ) -> None:
        super().__init__()
        self.batch_limit = int(batch_limit)
        self.norm_attn = _RMSNorm(dim)
        self.attn = nn.MultiheadAttention(
            dim,
            max(1, int(heads)),
            dropout=float(dropout),
            batch_first=True,
        )
        self.dropout = nn.Dropout(float(dropout))
        self.norm_ffn = _RMSNorm(dim)
        self.ffn = _FeedForward(dim, max(dim, int(dim * ffn_mult)), dropout)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        h = self.norm_attn(x)
        if self.batch_limit > 0 and int(h.size(0)) > self.batch_limit:
            chunks: list[torch.Tensor] = []
            for start in range(0, int(h.size(0)), self.batch_limit):
                end = min(start + self.batch_limit, int(h.size(0)))
                attn_chunk, _ = self.attn(h[start:end], h[start:end], h[start:end], need_weights=False)
                chunks.append(attn_chunk)
            attn = torch.cat(chunks, dim=0)
        else:
            attn, _ = self.attn(h, h, h, need_weights=False)
        x = x + self.dropout(attn)
        x = x + self.ffn(self.norm_ffn(x))
        return x


class _CrossAttentionBlock(nn.Module):
    def __init__(self, dim: int, heads: int, ffn_mult: int, dropout: float) -> None:
        super().__init__()
        self.norm_q = _RMSNorm(dim)
        self.norm_kv = _RMSNorm(dim)
        self.attn = nn.MultiheadAttention(
            dim,
            max(1, int(heads)),
            dropout=float(dropout),
            batch_first=True,
        )
        self.dropout = nn.Dropout(float(dropout))
        self.norm_ffn = _RMSNorm(dim)
        self.ffn = _FeedForward(dim, max(dim, int(dim * ffn_mult)), dropout)

    def forward(
        self,
        query: torch.Tensor,
        key_value: torch.Tensor,
        key_padding_mask: torch.Tensor | None = None,
    ) -> torch.Tensor:
        q = self.norm_q(query)
        kv = self.norm_kv(key_value)
        attn, _ = self.attn(q, kv, kv, key_padding_mask=key_padding_mask, need_weights=False)
        query = query + self.dropout(attn)
        query = query + self.ffn(self.norm_ffn(query))
        return query


def _make_mlp(in_dim: int, hidden_dim: int, layers: int, dropout: float, out_dim: int) -> nn.Sequential:
    modules: list[nn.Module] = []
    width = int(in_dim)
    for _ in range(max(0, int(layers) - 1)):
        modules.append(nn.Linear(width, int(hidden_dim)))
        modules.append(nn.SiLU())
        modules.append(nn.Dropout(float(dropout)))
        width = int(hidden_dim)
    modules.append(nn.Linear(width, int(out_dim)))
    return nn.Sequential(*modules)


class ResidualTransformerStage(nn.Module):
    """Boosting-aware Transformer stage that predicts one raw logit delta."""

    def __init__(
        self,
        *,
        lookback: int,
        num_features: int,
        num_symbols: int,
        d_model: int,
        temporal_layers: int,
        temporal_heads: int,
        temporal_ffn_mult: int,
        market_layers: int,
        market_heads: int,
        market_ffn_mult: int,
        num_market_tokens: int,
        head_hidden_dim: int,
        head_layers: int,
        dropout: float,
        input_dropout: float,
        use_time_pos: bool,
        use_symbol_pos: bool,
        dynamic_market_tokens: bool,
        dynamic_token_gate_init: float,
        condition_on_previous: bool,
        zero_init_output: bool,
        runtime_shape_check: bool,
        allow_dynamic_symbols: bool,
    ) -> None:
        super().__init__()
        self.lookback = int(lookback)
        self.num_features = int(num_features)
        self.num_symbols = int(num_symbols)
        self.d_model = int(d_model)
        self.condition_on_previous = bool(condition_on_previous)
        self.runtime_shape_check = bool(runtime_shape_check)
        self.allow_dynamic_symbols = bool(allow_dynamic_symbols)
        self.use_time_pos = bool(use_time_pos)
        self.use_symbol_pos = bool(use_symbol_pos)
        self.dynamic_market_tokens = bool(dynamic_market_tokens)

        self.feature_proj = nn.Linear(self.num_features, self.d_model)
        self.input_dropout = nn.Dropout(float(input_dropout))
        if self.use_time_pos:
            self.time_pos = nn.Parameter(torch.zeros(1, self.lookback, 1, self.d_model))
            nn.init.normal_(self.time_pos, std=0.02)
        else:
            self.register_parameter("time_pos", None)
        if self.use_symbol_pos:
            self.symbol_pos = nn.Parameter(torch.zeros(1, 1, self.num_symbols, self.d_model))
            nn.init.normal_(self.symbol_pos, std=0.02)
        else:
            self.register_parameter("symbol_pos", None)

        self.temporal_blocks = nn.ModuleList(
            _SelfAttentionBlock(self.d_model, temporal_heads, temporal_ffn_mult, dropout)
            for _ in range(max(0, int(temporal_layers)))
        )
        self.temporal_pool = nn.Linear(self.d_model, 1)

        if self.condition_on_previous:
            self.condition_proj = nn.Sequential(
                nn.Linear(2, self.d_model),
                nn.SiLU(),
                nn.Linear(self.d_model, self.d_model),
            )
        else:
            self.condition_proj = None

        self.num_market_tokens = max(1, int(num_market_tokens))
        self.market_queries = nn.Parameter(torch.empty(1, self.num_market_tokens, self.d_model))
        nn.init.normal_(self.market_queries, std=0.02)
        if self.dynamic_market_tokens:
            self.market_summary_proj = nn.Sequential(
                nn.Linear(self.d_model * 3, self.d_model * self.num_market_tokens),
                nn.SiLU(),
                nn.Linear(self.d_model * self.num_market_tokens, self.d_model * self.num_market_tokens),
            )
            gate = torch.full((), float(dynamic_token_gate_init)).clamp(1e-4, 1.0 - 1e-4)
            self.dynamic_market_gate = nn.Parameter(torch.logit(gate))
        else:
            self.market_summary_proj = None
            self.register_parameter("dynamic_market_gate", None)

        self.market_read_blocks = nn.ModuleList(
            _CrossAttentionBlock(self.d_model, market_heads, market_ffn_mult, dropout)
            for _ in range(max(0, int(market_layers)))
        )
        self.stock_read_blocks = nn.ModuleList(
            _CrossAttentionBlock(self.d_model, market_heads, market_ffn_mult, dropout)
            for _ in range(max(0, int(market_layers)))
        )
        self.market_gate = nn.Linear(self.d_model * 2, self.d_model)
        self.output_norm = _RMSNorm(self.d_model)
        self.head = _make_mlp(self.d_model, head_hidden_dim, head_layers, dropout, 1)
        if zero_init_output:
            final_linear = next(module for module in reversed(self.head) if isinstance(module, nn.Linear))
            nn.init.zeros_(final_linear.weight)
            nn.init.zeros_(final_linear.bias)

    def _check_shapes(self, x: torch.Tensor, mask: torch.Tensor) -> None:
        if x.dim() != 4:
            raise ValueError(f"Expected x shape [B,L,S,F], got ndim={x.dim()}")
        if int(x.size(1)) != self.lookback:
            raise ValueError(f"Expected lookback={self.lookback}, got {int(x.size(1))}")
        if (not self.allow_dynamic_symbols) and int(x.size(2)) != self.num_symbols:
            raise ValueError(f"Expected num_symbols={self.num_symbols}, got {int(x.size(2))}")
        if int(x.size(3)) != self.num_features:
            raise ValueError(f"Expected num_features={self.num_features}, got {int(x.size(3))}")
        if tuple(mask.shape) != (int(x.size(0)), int(x.size(2))):
            raise ValueError(f"Expected mask shape {(int(x.size(0)), int(x.size(2)))}, got {tuple(mask.shape)}")

    def _market_tokens(self, z: torch.Tensor, mask: torch.Tensor) -> torch.Tensor:
        tokens = self.market_queries.expand(z.size(0), -1, -1)
        if self.market_summary_proj is None:
            return tokens
        mean = _masked_mean(z, mask)
        centered = (z - mean).masked_fill(~mask.unsqueeze(-1), 0.0)
        mask_f = mask.to(dtype=z.dtype).unsqueeze(-1)
        denom = mask_f.sum(dim=1, keepdim=True).clamp_min(1.0)
        std = torch.sqrt((centered.float().pow(2) * mask_f.float()).sum(dim=1, keepdim=True) / denom.float()).to(
            dtype=z.dtype
        )
        disp = (centered.abs() * mask_f).sum(dim=1, keepdim=True) / denom
        summary = torch.cat([mean.squeeze(1), std.squeeze(1), disp.squeeze(1)], dim=-1)
        delta = self.market_summary_proj(summary).view(z.size(0), self.num_market_tokens, self.d_model)
        gate = torch.sigmoid(self.dynamic_market_gate).to(dtype=z.dtype)
        return tokens + gate * delta

    def forward(
        self,
        x: torch.Tensor,
        mask: torch.Tensor,
        previous_logits: torch.Tensor | None = None,
        previous_weights: torch.Tensor | None = None,
        return_aux: bool = False,
    ) -> torch.Tensor | tuple[torch.Tensor, dict[str, torch.Tensor]]:
        mask_bool = mask.to(device=x.device, dtype=torch.bool)
        if self.runtime_shape_check:
            self._check_shapes(x, mask_bool)

        clean_x = torch.nan_to_num(x, nan=0.0, posinf=20.0, neginf=-20.0).clamp(min=-20.0, max=20.0)
        h = self.feature_proj(clean_x)
        if self.time_pos is not None:
            h = h + self.time_pos.to(dtype=h.dtype)
        if self.symbol_pos is not None and int(h.size(2)) == int(self.symbol_pos.size(2)):
            h = h + self.symbol_pos.to(dtype=h.dtype)
        h = self.input_dropout(h)

        bsz, lookback, symbols, dim = h.shape
        temporal = h.permute(0, 2, 1, 3).reshape(bsz * symbols, lookback, dim)
        for block in self.temporal_blocks:
            temporal = block(temporal)
        pool_logits = self.temporal_pool(temporal).squeeze(-1)
        pool_weights = torch.softmax(pool_logits.float(), dim=1).to(dtype=temporal.dtype)
        z = (temporal * pool_weights.unsqueeze(-1)).sum(dim=1).view(bsz, symbols, dim)
        z = z.masked_fill(~mask_bool.unsqueeze(-1), 0.0)

        if self.condition_proj is not None:
            if previous_logits is None:
                previous_logits = z.new_zeros(bsz, symbols)
            if previous_weights is None:
                previous_weights = z.new_zeros(bsz, symbols)
            cond = torch.stack(
                [
                    previous_logits.to(device=z.device, dtype=z.dtype),
                    previous_weights.to(device=z.device, dtype=z.dtype),
                ],
                dim=-1,
            )
            cond = torch.nan_to_num(cond, nan=0.0, posinf=20.0, neginf=-20.0).clamp(min=-20.0, max=20.0)
            z = z + self.condition_proj(cond).masked_fill(~mask_bool.unsqueeze(-1), 0.0)

        safe_market_mask = _safe_attention_mask(mask_bool)
        market = self._market_tokens(z, safe_market_mask)
        key_padding_mask = ~safe_market_mask
        z_before_market = z
        for read_block, stock_block in zip(self.market_read_blocks, self.stock_read_blocks):
            market = read_block(market, z, key_padding_mask=key_padding_mask)
            z = stock_block(z, market, key_padding_mask=None)
            z = z.masked_fill(~mask_bool.unsqueeze(-1), 0.0)
        market_delta = z - z_before_market
        gate = torch.sigmoid(self.market_gate(torch.cat([z_before_market, z], dim=-1)))
        z = self.output_norm(z_before_market + gate * market_delta)
        z = z.masked_fill(~mask_bool.unsqueeze(-1), 0.0)

        logits = self.head(z).squeeze(-1)
        logits = torch.nan_to_num(logits, nan=0.0, posinf=20.0, neginf=-20.0).clamp(min=-20.0, max=20.0)
        logits = logits.masked_fill(~mask_bool, 0.0)
        if not return_aux:
            return logits
        aux = {
            "stock_embedding": z,
            "market_tokens": market,
            "temporal_pool_weights": pool_weights.view(bsz, symbols, lookback),
            "market_gate": gate,
        }
        return logits, aux


class GradientBoostedPortfolioTransformer(nn.Module):
    """End-to-end boosted Transformer for portfolio weights.

    The base stage predicts initial raw holdings.  Each residual stage receives
    the same market window plus the current aggregate logits and normalized
    partial portfolio, then emits a zero-initialized correction.  The final
    aggregate is normalized once into the configured legal portfolio.
    """

    def __init__(
        self,
        *,
        lookback: int,
        num_features: int,
        num_symbols: int,
        d_model: int = 64,
        temporal_layers: int = 2,
        temporal_heads: int = 4,
        temporal_ffn_mult: int = 2,
        market_layers: int = 1,
        market_heads: int = 4,
        market_ffn_mult: int = 2,
        num_market_tokens: int = 4,
        head_hidden_dim: int = 64,
        head_layers: int = 1,
        dropout: float = 0.1,
        input_dropout: float = 0.0,
        use_time_pos: bool = True,
        use_symbol_pos: bool = False,
        dynamic_market_tokens: bool = True,
        dynamic_token_gate_init: float = 0.1,
        num_residual_stages: int = 2,
        stage_eta: Sequence[float] = (0.5, 0.25),
        trainable_eta: bool = True,
        eta_max: float = 1.0,
        detach_stage_condition: bool = True,
        default_temperature: float = 1.0,
        portfolio_mode: str = "long_short",
        portfolio_activation: str = "identity",
        portfolio_output_mode: str = "projection_l1",
        center_final_logits: bool = True,
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
        self.num_residual_stages = max(0, int(num_residual_stages))
        self.detach_stage_condition = bool(detach_stage_condition)
        self.default_temperature = float(default_temperature)
        self.portfolio_mode = normalize_portfolio_mode(portfolio_mode)
        self.portfolio_activation = normalize_portfolio_activation(portfolio_activation)
        self.portfolio_output_mode = normalize_portfolio_output_mode(portfolio_output_mode)
        self.center_final_logits = bool(center_final_logits)
        self.return_aux = bool(return_aux)
        self.return_aux_details = bool(return_aux_details)
        self.runtime_shape_check = bool(runtime_shape_check)
        self.allow_dynamic_symbols = bool(allow_dynamic_symbols)
        self.eta_max = float(eta_max)
        if self.eta_max <= 0.0:
            raise ValueError("eta_max must be positive")
        self.eta_logit_limit = 12.0
        self.parameter_abs_limit = 20.0

        stage_kwargs = {
            "lookback": self.lookback,
            "num_features": self.num_features,
            "num_symbols": self.num_symbols,
            "d_model": self.d_model,
            "temporal_layers": temporal_layers,
            "temporal_heads": temporal_heads,
            "temporal_ffn_mult": temporal_ffn_mult,
            "market_layers": market_layers,
            "market_heads": market_heads,
            "market_ffn_mult": market_ffn_mult,
            "num_market_tokens": num_market_tokens,
            "head_hidden_dim": head_hidden_dim,
            "head_layers": head_layers,
            "dropout": dropout,
            "input_dropout": input_dropout,
            "use_time_pos": use_time_pos,
            "use_symbol_pos": use_symbol_pos,
            "dynamic_market_tokens": dynamic_market_tokens,
            "dynamic_token_gate_init": dynamic_token_gate_init,
            "runtime_shape_check": runtime_shape_check,
            "allow_dynamic_symbols": allow_dynamic_symbols,
        }
        self.base_stage = ResidualTransformerStage(
            **stage_kwargs,
            condition_on_previous=False,
            zero_init_output=False,
        )
        self.residual_stages = nn.ModuleList(
            ResidualTransformerStage(
                **stage_kwargs,
                condition_on_previous=True,
                zero_init_output=True,
            )
            for _ in range(self.num_residual_stages)
        )

        eta_values = torch.tensor(_repeat_or_trim_eta(stage_eta, self.num_residual_stages), dtype=torch.float32)
        self.trainable_eta = bool(trainable_eta)
        if self.trainable_eta:
            clipped = eta_values.clamp(min=1e-6, max=self.eta_max - 1e-6)
            probs = (clipped / self.eta_max).clamp(min=1e-6, max=1.0 - 1e-6)
            self.eta_logits = nn.Parameter(torch.logit(probs).clamp(min=-self.eta_logit_limit, max=self.eta_logit_limit))
            self.register_buffer("eta_values", torch.empty(0), persistent=False)
        else:
            self.register_buffer("eta_values", eta_values.clamp(min=0.0, max=self.eta_max), persistent=True)
            self.eta_logits = None

    @torch.no_grad()
    def stabilize_parameters_after_step_(self) -> None:
        for param in self.parameters():
            if param is None:
                continue
            if not torch.isfinite(param.data).all():
                continue
            param.data.clamp_(min=-self.parameter_abs_limit, max=self.parameter_abs_limit)
        if self.trainable_eta and self.eta_logits is not None:
            if torch.isfinite(self.eta_logits.data).all():
                self.eta_logits.data.clamp_(min=-self.eta_logit_limit, max=self.eta_logit_limit)

    def _eta(self, *, device: torch.device, dtype: torch.dtype) -> torch.Tensor:
        if self.num_residual_stages == 0:
            return torch.empty(0, device=device, dtype=dtype)
        if self.trainable_eta:
            assert self.eta_logits is not None
            eta_logits = torch.nan_to_num(
                self.eta_logits,
                nan=0.0,
                posinf=self.eta_logit_limit,
                neginf=-self.eta_logit_limit,
            ).clamp(min=-self.eta_logit_limit, max=self.eta_logit_limit)
            eta = self.eta_max * torch.sigmoid(eta_logits)
        else:
            eta = self.eta_values
        return eta.to(device=device, dtype=dtype)

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

    def _mask_bool(self, x: torch.Tensor, mask: torch.Tensor | None) -> torch.Tensor:
        if mask is None:
            return torch.ones(x.size(0), x.size(2), dtype=torch.bool, device=x.device)
        return mask.to(device=x.device, dtype=torch.bool)

    def _materialize_windows_from_panel(self, features: torch.Tensor, date_indices: torch.Tensor) -> torch.Tensor:
        date_indices_source = date_indices.to(device=features.device, dtype=torch.long)
        offsets = torch.arange(
            self.lookback - 1,
            -1,
            -1,
            device=features.device,
            dtype=torch.long,
        )
        window_idx = date_indices_source[:, None] - offsets[None, :]
        return features[window_idx]

    def _materialize_windows_from_slab(self, feature_slab: torch.Tensor) -> torch.Tensor:
        windowed = feature_slab.unfold(0, self.lookback, 1)
        return windowed.permute(0, 3, 1, 2).contiguous()

    def _normalize_logits(
        self,
        logits: torch.Tensor,
        mask_bool: torch.Tensor,
        temperature: float | torch.Tensor | None,
        *,
        include_aux: bool = False,
    ) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor, dict[str, torch.Tensor]]:
        scores = torch.nan_to_num(logits, nan=0.0, posinf=20.0, neginf=-20.0).clamp(min=-20.0, max=20.0)
        scores = scores.masked_fill(~mask_bool, 0.0)
        masked_scores = scores.masked_fill(~mask_bool, finite_mask_fill_value(scores))
        if temperature is None:
            temp = masked_scores.new_tensor(self.default_temperature)
        elif isinstance(temperature, torch.Tensor):
            temp = temperature.to(device=masked_scores.device, dtype=masked_scores.dtype)
        else:
            temp = masked_scores.new_tensor(float(temperature))
        temp = torch.clamp(temp, min=0.05)

        if self.portfolio_mode == "long_only":
            centered = scores
            long_only = True
        else:
            centered = scores - masked_cross_sectional_mean(scores, mask_bool) if self.center_final_logits else scores
            long_only = False
        target_logits = (centered / temp).masked_fill(~mask_bool, 0.0)

        output_aux: dict[str, torch.Tensor] = {}
        if self.portfolio_output_mode == "logits":
            weights = target_logits
        elif self.portfolio_output_mode == "signed_softmax":
            action_output = masked_signed_action_weights(
                target_logits,
                mask_bool,
                transform="softmax",
                long_only=long_only,
                return_parts=include_aux,
            )
            if include_aux:
                weights, output_aux = action_output
            else:
                weights = action_output
        elif self.portfolio_output_mode == "signed_sparsemax":
            action_output = masked_signed_action_weights(
                target_logits,
                mask_bool,
                transform="sparsemax",
                long_only=long_only,
                return_parts=include_aux,
            )
            if include_aux:
                weights, output_aux = action_output
            else:
                weights = action_output
        elif self.portfolio_output_mode == "signed_entmax15":
            action_output = masked_signed_action_weights(
                target_logits,
                mask_bool,
                transform="entmax15",
                long_only=long_only,
                return_parts=include_aux,
            )
            if include_aux:
                weights, output_aux = action_output
            else:
                weights = action_output
        elif self.portfolio_output_mode == "projection_l1":
            weights = masked_l1_projection_weights(target_logits, mask_bool, long_only=long_only)
            if include_aux:
                output_aux = {
                    "projection_gross_exposure": weights.abs().sum(dim=1),
                    "implicit_cash_weight": (1.0 - weights.abs().sum(dim=1)).clamp_min(0.0),
                }
        else:
            activation = "identity" if self.portfolio_output_mode == "l1" else self.portfolio_activation
            weights = masked_activation_l1_weights(
                target_logits,
                mask_bool,
                long_only=long_only,
                activation=activation,
            )
        weights = weights.masked_fill(~mask_bool, 0.0)
        return weights, masked_scores, centered, output_aux

    def _partial_weights(
        self,
        logits: torch.Tensor,
        mask_bool: torch.Tensor,
        temperature: float | torch.Tensor | None,
    ) -> torch.Tensor:
        weights, _, _, _ = self._normalize_logits(logits, mask_bool, temperature, include_aux=False)
        return weights

    def _forward_window(
        self,
        x: torch.Tensor,
        mask_bool: torch.Tensor,
        temperature: float | torch.Tensor | None,
        return_aux: bool | None,
    ):
        base_output = self.base_stage(x, mask_bool, return_aux=bool(return_aux is True and self.return_aux_details))
        if isinstance(base_output, tuple):
            aggregate, base_aux = base_output
        else:
            aggregate, base_aux = base_output, None
        base_logits = aggregate

        eta = self._eta(device=aggregate.device, dtype=aggregate.dtype)
        residual_logits: list[torch.Tensor] = []
        residual_aux: list[dict[str, torch.Tensor] | None] = []
        for idx, stage in enumerate(self.residual_stages):
            partial_weights = self._partial_weights(aggregate, mask_bool, temperature)
            condition_logits = aggregate.detach() if self.detach_stage_condition else aggregate
            condition_weights = partial_weights.detach() if self.detach_stage_condition else partial_weights
            stage_output = stage(
                x,
                mask_bool,
                previous_logits=condition_logits,
                previous_weights=condition_weights,
                return_aux=bool(return_aux is True and self.return_aux_details),
            )
            if isinstance(stage_output, tuple):
                delta, aux = stage_output
            else:
                delta, aux = stage_output, None
            residual_logits.append(delta)
            residual_aux.append(aux)
            aggregate = aggregate + eta[idx] * delta

        include_aux = bool(return_aux is True or (return_aux is None and self.return_aux and self.return_aux_details))
        weights, masked_scores, centered, output_aux = self._normalize_logits(
            aggregate,
            mask_bool,
            temperature,
            include_aux=include_aux,
        )

        if return_aux is True:
            aux = {
                "base_logits": base_logits,
                "aggregate_logits": aggregate,
                "score_logits": aggregate,
                "rank_logits": aggregate,
                "centered_score_logits": centered,
                "boost_eta": eta,
            }
            for idx, delta in enumerate(residual_logits, start=1):
                aux[f"delta_logits_{idx}"] = delta
            if self.return_aux_details:
                if base_aux is not None:
                    aux.update({f"base_{key}": value for key, value in base_aux.items()})
                for idx, stage_aux in enumerate(residual_aux, start=1):
                    if stage_aux is not None:
                        aux.update({f"stage_{idx}_{key}": value for key, value in stage_aux.items()})
            aux.update(output_aux)
            return weights, masked_scores, aux
        if return_aux is None and self.return_aux:
            output = {
                "weights": weights,
                "scores": masked_scores,
                "score_logits": aggregate,
                "rank_logits": aggregate,
                "centered_score_logits": centered,
            }
            if self.return_aux_details:
                aux = {
                    "aggregate_logits": aggregate,
                    "boost_eta": eta,
                }
                for idx, delta in enumerate(residual_logits, start=1):
                    aux[f"delta_logits_{idx}"] = delta
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
    ):
        if self.runtime_shape_check:
            self._check_shapes(x, mask)
        mask_bool = self._mask_bool(x, mask)
        return self._forward_window(x, mask_bool, temperature, return_aux)

    def forward_from_panel(
        self,
        features: torch.Tensor,
        date_indices: torch.Tensor,
        mask: torch.Tensor | None = None,
        temperature: float | torch.Tensor | None = None,
        return_aux: bool | None = None,
    ):
        x = self._materialize_windows_from_panel(features, date_indices)
        if mask is None:
            mask_bool = torch.ones(x.size(0), x.size(2), dtype=torch.bool, device=x.device)
        else:
            mask_bool = mask.to(device=x.device, dtype=torch.bool)
        return self._forward_window(x, mask_bool, temperature, return_aux)

    def forward_from_panel_slab(
        self,
        feature_slab: torch.Tensor,
        mask: torch.Tensor | None = None,
        temperature: float | torch.Tensor | None = None,
        return_aux: bool | None = None,
    ):
        x = self._materialize_windows_from_slab(feature_slab)
        if mask is None:
            mask_bool = torch.ones(x.size(0), x.size(2), dtype=torch.bool, device=x.device)
        else:
            mask_bool = mask.to(device=x.device, dtype=torch.bool)
        return self._forward_window(x, mask_bool, temperature, return_aux)
