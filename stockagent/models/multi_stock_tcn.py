from __future__ import annotations

import torch
from torch import nn

from stockagent.models.normalization import dual_branch_softmax, masked_softmax


class _CausalTCNBlock(nn.Module):
    def __init__(
        self,
        channels: int,
        kernel_size: int,
        dilation: int,
        dropout: float,
        conv_mode: str = "separable",
        conv_layers: int = 1,
        norm_type: str = "none",
    ) -> None:
        super().__init__()
        self.kernel_size = max(2, int(kernel_size))
        self.dilation = max(1, int(dilation))
        self.pad_left = (self.kernel_size - 1) * self.dilation
        self.convs = nn.ModuleList(
            [
                _CausalTemporalConv(
                    channels=channels,
                    kernel_size=self.kernel_size,
                    dilation=self.dilation,
                    mode=conv_mode,
                )
                for _ in range(max(1, int(conv_layers)))
            ]
        )
        self.norms = nn.ModuleList([_make_temporal_norm(norm_type, channels) for _ in range(len(self.convs))])
        self.act = nn.GELU()
        self.dropout = nn.Dropout(float(dropout))

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        residual = x
        for conv, norm in zip(self.convs, self.norms, strict=True):
            x = conv(x)
            x = norm(x)
            x = self.act(x)
            x = self.dropout(x)
        return residual + x


class _CausalTemporalConv(nn.Module):
    def __init__(self, channels: int, kernel_size: int, dilation: int, mode: str) -> None:
        super().__init__()
        mode_norm = mode.strip().lower().replace("-", "_")
        self.pad_left = (int(kernel_size) - 1) * int(dilation)
        if mode_norm in {"full", "standard", "dense"}:
            self.conv = nn.Conv1d(channels, channels, kernel_size=kernel_size, dilation=dilation)
        elif mode_norm in {"separable", "depthwise_separable", "dw_sep"}:
            self.conv = nn.Sequential(
                nn.Conv1d(
                    channels,
                    channels,
                    kernel_size=kernel_size,
                    dilation=dilation,
                    groups=channels,
                    bias=False,
                ),
                nn.Conv1d(channels, channels, kernel_size=1),
            )
        else:
            raise ValueError(f"Unsupported multi_stock_tcn.tcn_conv_mode={mode!r}")

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return self.conv(nn.functional.pad(x, (self.pad_left, 0)))


def _make_temporal_norm(norm_type: str, channels: int) -> nn.Module:
    norm = norm_type.strip().lower()
    if norm in {"none", "identity", "off", "false", "0"}:
        return nn.Identity()
    if norm in {"batch", "batchnorm", "batch_norm"}:
        return nn.BatchNorm1d(channels)
    if norm in {"group", "groupnorm", "group_norm"}:
        return nn.GroupNorm(1, channels)
    raise ValueError(f"Unsupported multi_stock_tcn.norm_type={norm_type!r}")


class CrossSectionalMultiStockTCN(nn.Module):
    """Shared per-stock TCN, mean stock pooling, and portfolio-weight MLP.

    The trainer still provides x as [B, lookback, symbols, features], while the
    model also accepts the requested [B, symbols, lookback, features] layout.
    Internally it follows:

    [B,N,T,F] -> [B*N,T,F] -> [B*N,F,T] -> TCN -> latest -> [B,N,d]
    -> mean pooling -> [B,d] -> MLP -> [B,N] portfolio weights.
    """

    def __init__(
        self,
        lookback: int,
        num_features: int,
        num_symbols: int,
        hidden_channels: int,
        embedding_dim: int,
        tcn_blocks: int,
        tcn_kernel_size: int,
        head_hidden_dim: int,
        head_layers: int,
        dropout: float,
        tcn_conv_mode: str = "separable",
        conv_layers_per_block: int = 1,
        norm_type: str = "none",
        sanitize_inputs: bool = False,
        long_only: bool = True,
        runtime_shape_check: bool = False,
        allow_dynamic_symbols: bool = True,
    ) -> None:
        super().__init__()
        self.lookback = int(lookback)
        self.num_features = int(num_features)
        self.num_symbols = int(num_symbols)
        self.long_only = bool(long_only)
        self.runtime_shape_check = bool(runtime_shape_check)
        self.allow_dynamic_symbols = bool(allow_dynamic_symbols)
        self.sanitize_inputs = bool(sanitize_inputs)

        channels = int(hidden_channels)
        embed_dim = int(embedding_dim)
        self.input_projection = nn.Conv1d(self.num_features, channels, kernel_size=1)
        self.temporal_tcn = nn.Sequential(
            *[
                _CausalTCNBlock(
                    channels=channels,
                    kernel_size=tcn_kernel_size,
                    dilation=2**idx,
                    dropout=dropout,
                    conv_mode=tcn_conv_mode,
                    conv_layers=conv_layers_per_block,
                    norm_type=norm_type,
                )
                for idx in range(max(1, int(tcn_blocks)))
            ]
        )
        self.stock_embedding = nn.Sequential(
            nn.LayerNorm(channels),
            nn.Linear(channels, embed_dim),
            nn.GELU(),
            nn.Dropout(float(dropout)),
        )

        head: list[nn.Module] = []
        in_dim = embed_dim
        for _ in range(max(0, int(head_layers))):
            head.extend(
                [
                    nn.Linear(in_dim, int(head_hidden_dim)),
                    nn.GELU(),
                    nn.Dropout(float(dropout)),
                ]
            )
            in_dim = int(head_hidden_dim)
        head.append(nn.Linear(in_dim, self.num_symbols))
        self.portfolio_head = nn.Sequential(*head)

    def _to_symbol_first(self, x: torch.Tensor) -> tuple[torch.Tensor, int, int]:
        if x.dim() != 4:
            raise ValueError(f"Expected x.ndim=4, got {x.dim()}")
        if int(x.size(3)) != self.num_features:
            raise ValueError(f"Expected num_features={self.num_features}, got {int(x.size(3))}")

        project_layout = int(x.size(1)) == self.lookback
        symbol_first_layout = int(x.size(2)) == self.lookback
        if symbol_first_layout and not project_layout:
            n_symbols = int(x.size(1))
            steps = int(x.size(2))
            symbol_first = x
        elif project_layout:
            n_symbols = int(x.size(2))
            steps = int(x.size(1))
            symbol_first = x.permute(0, 2, 1, 3)
        else:
            raise ValueError(
                "Expected x shape [B, lookback, symbols, features] or "
                f"[B, symbols, lookback, features] with lookback={self.lookback}; "
                f"got {tuple(x.shape)}"
            )

        if (not self.allow_dynamic_symbols) and n_symbols != self.num_symbols:
            raise ValueError(f"Expected num_symbols={self.num_symbols}, got {n_symbols}")
        if n_symbols > self.num_symbols:
            raise ValueError(
                f"Model output head was built for at most {self.num_symbols} symbols, got {n_symbols}"
            )
        return symbol_first, n_symbols, steps

    def forward(self, x: torch.Tensor, tradable_mask: torch.Tensor | None = None) -> torch.Tensor:
        bsz = int(x.size(0))
        symbol_first, n_symbols, steps = self._to_symbol_first(x)

        if self.sanitize_inputs:
            symbol_first = torch.nan_to_num(symbol_first, nan=0.0, posinf=0.0, neginf=0.0)
        seq = symbol_first.permute(0, 1, 3, 2).reshape(bsz * n_symbols, self.num_features, steps).contiguous()

        z = self.input_projection(seq)
        z = self.temporal_tcn(z)
        latest = z[:, :, -1]
        stock_embeddings = self.stock_embedding(latest).reshape(bsz, n_symbols, -1)
        stock_embeddings = torch.nan_to_num(stock_embeddings, nan=0.0, posinf=0.0, neginf=0.0)

        if tradable_mask is None:
            portfolio_embedding = stock_embeddings.mean(dim=1)
        else:
            mask_f = tradable_mask.to(device=stock_embeddings.device, dtype=stock_embeddings.dtype).unsqueeze(-1)
            portfolio_embedding = (stock_embeddings * mask_f).sum(dim=1) / mask_f.sum(dim=1).clamp_min(1.0)

        logits = self.portfolio_head(portfolio_embedding)
        logits = logits[:, :n_symbols]
        logits = torch.nan_to_num(logits, nan=0.0, posinf=20.0, neginf=-20.0).clamp(min=-20.0, max=20.0)

        if self.long_only:
            return masked_softmax(logits, tradable_mask)
        return dual_branch_softmax(logits, tradable_mask)
