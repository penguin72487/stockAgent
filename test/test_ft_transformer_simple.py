"""FT-Transformer smoke test with observable portfolio invariants."""

import torch

from stockagent.models.ft_transformer import CrossSectionalFTTransformer


def test_ft_transformer_forward_preserves_tradability() -> None:
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model = CrossSectionalFTTransformer(
        lookback=5, num_features=12, num_symbols=128, d_token=32,
        n_heads=4, n_layers=2, ffn_dim=128, dropout=0.1,
        long_only=True, use_cls_token=True,
    ).to(device).eval()
    x = torch.randn(4, 5, 128, 12, device=device)
    mask = torch.ones(4, 128, dtype=torch.bool, device=device)
    mask[:, ::2] = False
    mask[-1] = False
    with torch.no_grad():
        weights = model(x, mask)
    assert weights.shape == (4, 128)
    assert torch.isfinite(weights).all()
    assert (weights >= 0).all()
    assert (weights[~mask] == 0).all()
    torch.testing.assert_close(weights[:-1].sum(dim=1), torch.ones(3, device=device))
