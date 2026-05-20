#!/usr/bin/env python3
"""Test MLP model."""

import torch
from stockagent.models.mlp import CrossSectionalMLP

device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
print(f"Device: {device}")

# Create model
model = CrossSectionalMLP(
    lookback=1,
    num_features=12,
    num_symbols=724,
    hidden_dim=1024,
    dropout=0.1,
    embedding_dim=16,
)
model = model.to(device)
print("Model created and moved to device")

# Create test batch
B, lookback, S, F = 4, 1, 724, 12
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
    print("SUCCESS")
except Exception as e:
    print(f"ERROR: {e}")
    import traceback
    traceback.print_exc()
