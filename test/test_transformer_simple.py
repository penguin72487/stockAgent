"""Small device smoke test; collection must not execute a model."""

import torch
from torch import nn


def test_prenorm_transformer_forward() -> None:
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    layer = nn.TransformerEncoderLayer(
        d_model=16, nhead=4, dim_feedforward=8, batch_first=True,
        norm_first=True, dropout=0.1, activation="gelu",
    )
    model = nn.TransformerEncoder(
        layer, num_layers=2, norm=nn.LayerNorm(16), enable_nested_tensor=False,
    ).to(device).eval()
    x = torch.randn(8, 1, 16, device=device)
    with torch.no_grad():
        y = model(x)
    assert y.shape == x.shape
    assert torch.isfinite(y).all()
