import torch

from stockagent.models.time_block_transformer_base_portfolio import TimeBlockTransformerBasePortfolioModel


def _make_model(**overrides) -> TimeBlockTransformerBasePortfolioModel:
    kwargs = dict(
        lookback=4,
        num_features=3,
        num_symbols=5,
        d_model=16,
        attention_mode="market_token",
        use_flash_attention=True,
        use_time_pos=False,
        use_symbol_pos=True,
        sdpa_batch_limit=128,
        temporal_layers=1,
        temporal_heads=4,
        temporal_ffn_mult=1,
        cross_layers=0,
        cross_heads=4,
        cross_ffn_mult=1,
        latent_layers=1,
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
        return_aux_details=True,
    )
    kwargs.update(overrides)
    model = TimeBlockTransformerBasePortfolioModel(**kwargs)
    model.eval()
    return model


def test_time_block_transformer_forward_shapes_and_masking() -> None:
    torch.manual_seed(7)
    model = _make_model()
    x_context = torch.randn(7, 5, 3)
    mask = torch.ones(3, 5, dtype=torch.bool)
    mask[1, 2] = False

    output = model.forward_time_block(x_context, mask, target_offset=4, target_len=3)

    assert output["weights"].shape == (3, 5)
    assert output["scores"].shape == (3, 5)
    assert output["score_logits"].shape == (3, 5)
    assert output["rank_logits"].shape == (3, 5)
    assert output["weights"][1, 2].item() == 0.0
    assert torch.isfinite(output["weights"]).all()
    assert torch.allclose(output["weights"].abs().sum(dim=1), torch.ones(3), atol=1e-5)


def test_time_block_transformer_forward_rejects_learned_time_position() -> None:
    try:
        _make_model(use_time_pos=True)
    except ValueError as exc:
        assert "use_time_pos=false" in str(exc)
    else:
        raise AssertionError("expected use_time_pos=True to be rejected")
