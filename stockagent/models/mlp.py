from __future__ import annotations

import torch
from torch import nn
from transformers import AutoConfig, AutoModel


def _masked_softmax(logits: torch.Tensor, mask: torch.Tensor | None) -> torch.Tensor:
    if mask is None:
        return torch.softmax(logits, dim=1)

    mask_bool = mask.bool()
    mask_f = mask.to(dtype=logits.dtype)
    masked_logits = logits.masked_fill(~mask_bool, torch.finfo(logits.dtype).min)
    weights = torch.softmax(masked_logits, dim=1) * mask_f
    normalizer = weights.sum(dim=1, keepdim=True).clamp_min(1e-8)
    return weights / normalizer


class CrossSectionalMLP(nn.Module):
    """Optimized MLP with feature embedding + HuggingFace transformers (flash-attn)."""
    def __init__(self, lookback: int, num_features: int, num_symbols: int, hidden_dim: int, dropout: float, embedding_dim: int = 64) -> None:
        super().__init__()
        self.num_symbols = num_symbols
        self.embedding_dim = embedding_dim
        
        # Feature embedding: compress F features to embedding_dim
        self.feature_embedding = nn.Linear(num_features, embedding_dim)
        
        # Use transformers with flash-attn (with fallback to eager if flash-attn unavailable)
        config = AutoConfig.from_pretrained(
            "bert-base-uncased",
            hidden_size=embedding_dim,
            num_hidden_layers=2,
            num_attention_heads=8,
            intermediate_size=256,
            hidden_dropout_prob=dropout,
            attention_probs_dropout_prob=dropout,
            max_position_embeddings=lookback + 10,
        )
        
        # Try to use flash_attention_2, fallback to eager
        try:
            config.attn_implementation = "flash_attention_2"
            self.transformer = AutoModel.from_config(config, add_pooling_layer=False, trust_remote_code=True)
        except Exception:
            config.attn_implementation = "eager"
            self.transformer = AutoModel.from_config(config, add_pooling_layer=False, trust_remote_code=True)
        
        # Portfolio scoring head
        self.portfolio_head = nn.Sequential(
            nn.Linear(embedding_dim, hidden_dim),
            nn.GELU(),
            nn.Dropout(dropout),
            nn.Linear(hidden_dim, 1),
        )

    def forward(self, x: torch.Tensor, tradable_mask: torch.Tensor | None = None) -> torch.Tensor:
        """Return per-symbol portfolio weights.
        
        Args:
            x: [B, lookback, S, F]
            tradable_mask: [B, S] bool (optional)
        
        Returns:
            weights: [B, S]
        """
        B, lookback, S, F = x.shape
        x = x.permute(0, 2, 1, 3)  # [B, S, lookback, F]
        x = x.reshape(B * S, lookback, F)  # [B*S, lookback, F]
        
        # Feature embedding
        x = self.feature_embedding(x)  # [B*S, lookback, embedding_dim]
        
        # Transformer (flash-attn v2 for speed and memory efficiency)
        output = self.transformer(inputs_embeds=x, return_dict=True)
        x = output.last_hidden_state  # [B*S, lookback, embedding_dim]
        
        # Pool: take last timestep
        x = x[:, -1, :]  # [B*S, embedding_dim]
        
        # Portfolio scoring
        logits = self.portfolio_head(x).squeeze(-1)  # [B*S]
        logits = logits.reshape(B, S)
        
        # Apply softmax with mask
        if tradable_mask is not None:
            weights = _masked_softmax(logits, tradable_mask)
        else:
            weights = torch.softmax(logits, dim=1)
        
        return weights
