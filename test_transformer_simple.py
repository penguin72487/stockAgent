#!/usr/bin/env python3
"""Quick debug: test Transformer on GPU."""

import torch
from torch import nn

device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
print(f"Device: {device}")
print(f"CUDA available: {torch.cuda.is_available()}")

# Test simple Transformer
embedding_dim = 16
nhead = 4
lookback = 1

encoder_layer = nn.TransformerEncoderLayer(
    d_model=embedding_dim,
    nhead=nhead,
    dim_feedforward=embedding_dim // 2,
    batch_first=True,
    norm_first=True,
    dropout=0.1,
    activation='gelu',
)
transformer = nn.TransformerEncoder(encoder_layer, num_layers=2, norm=nn.LayerNorm(embedding_dim))

# Move to device
transformer = transformer.to(device)

# Create test input
x = torch.randn(8, lookback, embedding_dim, device=device)  # [B, T, D]
print(f"Input shape: {x.shape}")

try:
    with torch.no_grad():
        y = transformer(x)
    print(f"Output shape: {y.shape}")
    print("SUCCESS")
except Exception as e:
    print(f"ERROR: {e}")
    import traceback
    traceback.print_exc()
