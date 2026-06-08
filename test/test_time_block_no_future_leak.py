import torch

from stockagent.models.time_block_transformer_base_portfolio import TimeBlockTransformerBasePortfolioModel


def test_time_block_output_day_does_not_depend_on_future_context() -> None:
    torch.manual_seed(11)
    model = TimeBlockTransformerBasePortfolioModel(
        lookback=4,
        num_features=3,
        num_symbols=4,
        d_model=16,
        attention_mode="temporal_only",
        use_flash_attention=True,
        use_time_pos=False,
        use_symbol_pos=True,
        temporal_layers=2,
        temporal_heads=4,
        temporal_ffn_mult=1,
        cross_heads=4,
        cross_ffn_mult=1,
        num_latent_factors=4,
        num_market_tokens=2,
        market_layers=1,
        dynamic_latent_tokens=False,
        dynamic_market_tokens=False,
        head_hidden_dim=16,
        head_layers=1,
        dropout=0.0,
        portfolio_mode="long_short",
        temporal_causal=True,
        temporal_local_window=4,
        return_aux=True,
        return_aux_details=False,
    )
    model.eval()
    x_context = torch.randn(8, 4, 3)
    mask = torch.ones(3, 4, dtype=torch.bool)

    baseline = model.forward_time_block(x_context, mask, target_offset=4, target_len=3)["weights"]
    perturbed = x_context.clone()
    perturbed[5:] = torch.randn_like(perturbed[5:]) * 100.0
    changed = model.forward_time_block(perturbed, mask, target_offset=4, target_len=3)["weights"]

    assert torch.allclose(baseline[0], changed[0], atol=1e-6, rtol=1e-6)
