#!/usr/bin/env python3
"""Test FT-Transformer model forward pass."""

import torch

from stockagent.models.ft_transformer import CrossSectionalFTTransformer


device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
print(f"Device: {device}")

model = CrossSectionalFTTransformer(
    lookback=5,
    num_features=12,
    num_symbols=128,
    d_token=32,
    n_heads=4,
    n_layers=2,
    ffn_dim=128,
    dropout=0.1,
    long_only=True,
    use_cls_token=True,
).to(device)
print("Model created and moved to device")

B, lookback, S, F = 4, 5, 128, 12
x = torch.randn(B, lookback, S, F, device=device)
tradable_mask = torch.randint(0, 2, (B, S), dtype=torch.bool, device=device)

print(f"Input shape: {x.shape}")
print(f"Mask shape: {tradable_mask.shape}")

try:
    with torch.no_grad():
        weights = model(x, tradable_mask)
    print(f"Output shape: {weights.shape}")
    print(f"Expected shape: ({B}, {S})")
    print(f"Min weight: {weights.min().item():.6f}")
    print(f"Max weight: {weights.max().item():.6f}")
    print(f"Row sum (first sample): {weights[0].sum().item():.6f}")
    print("SUCCESS")
except Exception as e:
    print(f"ERROR: {e}")
    import traceback

    traceback.print_exc()
