from __future__ import annotations

import torch
from torch import nn

from stockagent.models.normalization import dual_branch_softmax, masked_softmax, normalize_portfolio_activation


class _FeatureTokenizer(nn.Module):
    """Tokenize continuous tabular features into per-feature embeddings."""

    def __init__(self, num_input_features: int, d_token: int) -> None:
        super().__init__()
        self.weight = nn.Parameter(torch.empty(num_input_features, d_token))
        self.bias = nn.Parameter(torch.empty(num_input_features, d_token))
        nn.init.xavier_uniform_(self.weight)
        nn.init.zeros_(self.bias)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        # x: [N, D] -> tokens: [N, D, d_token]
        return x.unsqueeze(-1) * self.weight.unsqueeze(0) + self.bias.unsqueeze(0)


class CrossSectionalFTTransformer(nn.Module):
    """Per-symbol FT-Transformer for cross-sectional portfolio weights."""

    def __init__(
        self,
        lookback: int,
        num_features: int,
        num_symbols: int,
        d_token: int,
        n_heads: int,
        n_layers: int,
        ffn_dim: int,
        dropout: float,
        long_only: bool = True,
        portfolio_activation: str = "identity",
        use_cls_token: bool = True,
        max_encoder_batch_rows: int = 60000,
    ) -> None:
        super().__init__()
        input_dim = int(lookback) * int(num_features)
        if input_dim <= 0:
            raise ValueError(f"Invalid tokenizer input_dim={input_dim}")
        if d_token <= 0 or n_heads <= 0 or n_layers <= 0 or ffn_dim <= 0:
            raise ValueError("d_token, n_heads, n_layers, and ffn_dim must all be positive")
        if d_token % n_heads != 0:
            raise ValueError(f"d_token ({d_token}) must be divisible by n_heads ({n_heads})")

        self.lookback = int(lookback)
        self.num_symbols = int(num_symbols)
        self.long_only = bool(long_only)
        self.portfolio_activation = normalize_portfolio_activation(portfolio_activation)
        self.use_cls_token = bool(use_cls_token)
        self.max_encoder_batch_rows = max(1, int(max_encoder_batch_rows))
        self.tokenizer = _FeatureTokenizer(num_input_features=input_dim, d_token=d_token)

        if self.use_cls_token:
            self.cls_token = nn.Parameter(torch.zeros(1, 1, d_token))
            nn.init.normal_(self.cls_token, mean=0.0, std=0.02)
        else:
            self.register_parameter("cls_token", None)

        encoder_layer = nn.TransformerEncoderLayer(
            d_model=d_token,
            nhead=n_heads,
            dim_feedforward=ffn_dim,
            dropout=dropout,
            activation="gelu",
            batch_first=True,
            norm_first=True,
        )
        self.encoder = nn.TransformerEncoder(
            encoder_layer,
            num_layers=n_layers,
            norm=nn.LayerNorm(d_token),
        )
        self.head = nn.Sequential(
            nn.LayerNorm(d_token),
            nn.Linear(d_token, 1),
        )

    def _encode_tokens_chunked(self, tokens: torch.Tensor) -> torch.Tensor:
        """Run transformer encoder in chunks to avoid CUDA SDP batch-index limits."""
        row_count = int(tokens.size(0))
        if row_count <= self.max_encoder_batch_rows:
            return self.encoder(tokens)

        encoded_chunks: list[torch.Tensor] = []
        for start in range(0, row_count, self.max_encoder_batch_rows):
            end = min(start + self.max_encoder_batch_rows, row_count)
            encoded_chunks.append(self.encoder(tokens[start:end]))
        return torch.cat(encoded_chunks, dim=0)

    def forward(self, x: torch.Tensor, tradable_mask: torch.Tensor | None = None) -> torch.Tensor:
        # x: [B, lookback, S, F]
        bsz, lookback, num_symbols, num_features = x.shape
        if lookback != self.lookback:
            raise ValueError(f"Expected lookback={self.lookback}, got {lookback}")
        if num_symbols != self.num_symbols:
            raise ValueError(f"Expected num_symbols={self.num_symbols}, got {num_symbols}")

        # Flatten temporal-feature axis per symbol, then run FT-Transformer independently per symbol.
        x_flat = x.permute(0, 2, 1, 3).reshape(bsz * num_symbols, lookback * num_features)
        tokens = self.tokenizer(x_flat)

        if self.use_cls_token and self.cls_token is not None:
            cls = self.cls_token.expand(tokens.size(0), -1, -1)
            tokens = torch.cat((cls, tokens), dim=1)

        encoded = self._encode_tokens_chunked(tokens)
        if self.use_cls_token:
            symbol_repr = encoded[:, 0, :]
        else:
            symbol_repr = encoded.mean(dim=1)

        logits = self.head(symbol_repr).reshape(bsz, num_symbols)

        if self.long_only:
            return masked_softmax(logits, tradable_mask, activation=self.portfolio_activation)
        return dual_branch_softmax(logits, tradable_mask, activation=self.portfolio_activation)
