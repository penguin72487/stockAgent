import torch

from stockagent.models.time_block_transformer_base_portfolio import TimeBlockTransformerBasePortfolioModel


def test_time_block_single_day_matches_forward_wrapper() -> None:
    torch.manual_seed(17)
    model = TimeBlockTransformerBasePortfolioModel(
        lookback=5,
        num_features=2,
        num_symbols=3,
        d_model=12,
        attention_mode="market_token",
        use_flash_attention=True,
        use_time_pos=False,
        use_symbol_pos=True,
        temporal_layers=1,
        temporal_heads=3,
        temporal_ffn_mult=1,
        cross_heads=3,
        cross_ffn_mult=1,
        num_latent_factors=3,
        num_market_tokens=2,
        dynamic_latent_tokens=False,
        dynamic_market_tokens=False,
        head_hidden_dim=12,
        head_layers=1,
        dropout=0.0,
        portfolio_mode="long_short",
        temporal_causal=True,
        temporal_local_window=5,
        return_aux=True,
        return_aux_details=False,
    )
    model.eval()
    x_context = torch.randn(5, 3, 2)
    mask = torch.tensor([[True, True, False]])

    block_output = model.forward_time_block(x_context, mask, target_offset=4, target_len=1)
    wrapped_output = model(x_context.unsqueeze(0), mask)

    assert torch.allclose(block_output["weights"], wrapped_output["weights"], atol=1e-6, rtol=1e-6)
    assert torch.allclose(block_output["scores"], wrapped_output["scores"], atol=1e-6, rtol=1e-6)
