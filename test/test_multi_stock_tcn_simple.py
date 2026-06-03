#!/usr/bin/env python3
"""Test simple multi-stock TCN forward pass and Sharpe loss."""

import torch

from stockagent.models.multi_stock_tcn import CrossSectionalMultiStockTCN
from stockagent.training.loss import risk_aware_loss


device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
print(f"Device: {device}")

B, T, N, F = 4, 16, 12, 8
model = CrossSectionalMultiStockTCN(
    lookback=T,
    num_features=F,
    num_symbols=N,
    hidden_channels=16,
    embedding_dim=16,
    tcn_blocks=3,
    tcn_kernel_size=3,
    head_hidden_dim=16,
    head_layers=1,
    dropout=0.1,
    long_only=False,
    runtime_shape_check=True,
).to(device)

x = torch.randn(B, T, N, F, device=device)
tradable_mask = torch.randint(0, 2, (B, N), dtype=torch.bool, device=device)
tradable_mask[:, 0] = True
future_log_returns = torch.randn(B, N, device=device) * 0.01

print(f"Input shape: {x.shape}")
print(f"Mask shape: {tradable_mask.shape}")

try:
    weights = model(x, tradable_mask)
    weights_symbol_first = model(x.permute(0, 2, 1, 3).contiguous(), tradable_mask)
    loss = risk_aware_loss(
        weights,
        future_log_returns,
        tradable_mask,
        objective="sharpe",
        long_only=False,
    )
    loss.backward()

    print(f"Output shape: {weights.shape}")
    print(f"Symbol-first output shape: {weights_symbol_first.shape}")
    print(f"Expected shape: ({B}, {N})")
    print(f"Gross exposure first row: {weights[0].abs().sum().item():.6f}")
    print(f"Sharpe loss: {loss.detach().item():.6f}")
    print("SUCCESS")
except Exception as e:
    print(f"ERROR: {e}")
    import traceback

    traceback.print_exc()
    raise
