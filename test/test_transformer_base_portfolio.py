#!/usr/bin/env python3
"""Smoke tests for the scalable Transformer-base portfolio model."""

import copy
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest
import torch

import stockagent.models.transformer_base_portfolio as transformer_module
import stockagent.training.trainer as trainer_module
from stockagent.config import load_config
from stockagent.models.factory import build_model, model_hidden_dim_hint
from stockagent.models.transformer_base_portfolio import (
    FlashSDPAAttention,
    LegacyDynamicTokenGenerator,
    ONLINE_SAFE_TEMPORAL_BASIS_FAMILIES,
    PortfolioRMSNorm,
    SwiGLUFeedForward,
    TemporalBasisFeatureEncoder,
    TransformerBasePortfolioModel,
    _compiled_cross_attention_requires_blackwell_workaround,
    _sanitize_scores_to_dtype,
    _temporal_basis_matrix,
)
from stockagent.models.normalization import (
    masked_activation_l1_weights,
    masked_cash_asset_l1_weights,
    masked_cash_entmax15_weights,
    masked_l1_projection_weights,
    masked_signed_action_weights,
)
from stockagent.training.loss import risk_aware_loss
from stockagent.training.trainer import (
    _DynamicSymbolPanelSlabWrapper,
    _extract_weights_and_aux,
    _active_model_config,
    _load_state_dict,
    _maybe_compact_train_windowed_symbols,
    _PanelSlabForwardWrapper,
    _panel_slab_dynamic_symbol_bounds,
    _train_symbol_compaction_bounds,
    _train_symbol_compaction_upper_bound,
)
from stockagent.training.windowed import WindowedSplitTensors


def _device() -> torch.device:
    return torch.device("cuda" if torch.cuda.is_available() else "cpu")


@pytest.mark.parametrize(
    ("rank0", "isatty", "expected"),
    [
        (True, True, True),
        (True, False, False),
        (False, True, False),
    ],
)
def test_interactive_progress_requires_rank0_real_terminal(
    monkeypatch: pytest.MonkeyPatch,
    rank0: bool,
    isatty: bool,
    expected: bool,
) -> None:
    stream = SimpleNamespace(isatty=lambda: isatty)
    monkeypatch.setattr(trainer_module, "_distributed_is_rank0", lambda: rank0)
    monkeypatch.setattr(trainer_module.sys, "stderr", stream)

    assert trainer_module._interactive_progress_enabled() is expected


def _make_model(**overrides) -> TransformerBasePortfolioModel:
    torch.manual_seed(23)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(23)
    params = {
        "lookback": 6,
        "num_features": 11,
        "num_symbols": 13,
        "d_model": 24,
        "attention_mode": "latent",
        "use_flash_attention": True,
        "use_time_pos": True,
        "use_symbol_pos": True,
        "input_dropout": 0.0,
        "sdpa_batch_limit": 4096,
        "norm_type": "rmsnorm",
        "ffn_type": "swiglu",
        "qk_norm": True,
        "rope_temporal": True,
        "rope_base": 10000.0,
        "temporal_layers": 1,
        "temporal_heads": 2,
        "temporal_ffn_mult": 1,
        "temporal_pooling": "attention",
        "temporal_query_mode": "full_then_last",
        "cross_layers": 1,
        "cross_heads": 2,
        "cross_ffn_mult": 1,
        "joint_layers": 1,
        "joint_heads": 2,
        "joint_ffn_mult": 1,
        "latent_layers": 1,
        "num_latent_factors": 4,
        "num_market_tokens": 2,
        "market_layers": 1,
        "head_hidden_dim": 24,
        "head_layers": 1,
        "dropout": 0.0,
        "default_temperature": 1.0,
        "portfolio_mode": "long_short",
        "max_full_tokens": 512,
        "checkpoint_blocks": False,
        "return_aux": True,
        "return_aux_details": True,
        "runtime_shape_check": True,
        "allow_dynamic_symbols": True,
    }
    params.update(overrides)
    return TransformerBasePortfolioModel(**params).to(_device())


def test_single_key_self_attention_fast_path_preserves_output_and_optimizer_gradients() -> None:
    """Singleton attention is value-only, including explicit zero Q/K grads."""

    torch.manual_seed(29)
    generic = FlashSDPAAttention(
        dim=16,
        num_heads=4,
        dropout=0.0,
        use_flash_attention=True,
        qk_norm=True,
        max_rope_steps=1,
    ).train()
    generic.single_key_value_fast_path = False
    optimized = copy.deepcopy(generic)
    optimized.single_key_value_fast_path = True
    generic_input = torch.randn(7, 1, 16, requires_grad=True)
    optimized_input = generic_input.detach().clone().requires_grad_(True)
    rope_positions = torch.zeros(1)

    generic_output = generic(generic_input, rope_positions=rope_positions)
    optimized_output = optimized(optimized_input, rope_positions=rope_positions)
    upstream = torch.randn_like(generic_output)
    (generic_output * upstream).sum().backward()
    (optimized_output * upstream).sum().backward()

    torch.testing.assert_close(optimized_output, generic_output, atol=2e-7, rtol=1e-6)
    torch.testing.assert_close(
        optimized_input.grad, generic_input.grad, atol=2e-7, rtol=1e-6
    )
    for generic_parameter, optimized_parameter in zip(
        generic.parameters(), optimized.parameters(), strict=True
    ):
        assert generic_parameter.grad is not None
        assert optimized_parameter.grad is not None
        torch.testing.assert_close(
            optimized_parameter.grad,
            generic_parameter.grad,
            atol=1e-6,
            rtol=1e-6,
        )
    assert optimized.in_proj.weight.grad is not None
    assert torch.count_nonzero(optimized.in_proj.weight.grad[: 2 * optimized.dim]) == 0


def test_daily_checkpoint_override_survives_minute_checkpoint_suppression(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    model = _make_model(checkpoint_blocks=True).train()
    model.minute_checkpoint_blocks = False
    calls: list[bool] = []

    def fake_checkpoint(function, *args, **kwargs):
        calls.append(bool(kwargs.get("use_reentrant") is False))
        return function(*args)

    monkeypatch.setattr(transformer_module, "activation_checkpoint", fake_checkpoint)
    block = model.temporal_blocks[0]
    value = torch.randn(2, 3, model.d_model, device=_device(), requires_grad=True)

    model._run_block(block, value)
    assert calls == []
    model._run_block(block, value, checkpoint_blocks_override=True)
    assert calls == [True]


def test_legacy_dynamic_market_checkpoint_is_reconstructed_strictly() -> None:
    device = _device()
    source = _make_model(
        attention_mode="market_token",
        return_aux=False,
        return_aux_details=False,
    ).eval()
    assert source.market_queries is not None
    source.dynamic_market_generator = LegacyDynamicTokenGenerator(
        dim=source.d_model,
        num_tokens=int(source.market_queries.size(1)),
        hidden_dim=source.d_model * 2,
        norm_type=source.norm_type,
        ffn_type=source.ffn_type,
    ).to(device=device)
    with torch.no_grad():
        source.dynamic_market_generator.gate_logit.fill_(2.0)

    target = _make_model(
        attention_mode="market_token",
        return_aux=False,
        return_aux_details=False,
    ).eval()
    _load_state_dict(target, source.state_dict())

    assert target.dynamic_market_generator is not None
    x = torch.randn(2, 6, 13, 11, device=device)
    mask = torch.ones(2, 13, dtype=torch.bool, device=device)
    with torch.no_grad():
        expected = source(x, mask)
        actual = target(x, mask)
    assert torch.equal(actual, expected)


def test_incomplete_legacy_dynamic_market_checkpoint_is_rejected() -> None:
    source = _make_model(attention_mode="market_token").eval()
    assert source.market_queries is not None
    source.dynamic_market_generator = LegacyDynamicTokenGenerator(
        dim=source.d_model,
        num_tokens=int(source.market_queries.size(1)),
        hidden_dim=source.d_model * 2,
        norm_type=source.norm_type,
        ffn_type=source.ffn_type,
    ).to(device=_device())
    state = source.state_dict()
    del state["dynamic_market_generator.out_proj.bias"]

    with pytest.raises(RuntimeError, match="Incomplete legacy dynamic-token checkpoint schema"):
        _load_state_dict(_make_model(attention_mode="market_token"), state)


@pytest.mark.parametrize("mode", ["full", "axial", "latent", "market_token", "temporal_only"])
def test_attention_modes_forward(mode: str) -> None:
    device = _device()
    model = _make_model(attention_mode=mode).eval()
    x = torch.randn(2, 6, 13, 11, device=device)
    mask = torch.ones(2, 13, dtype=torch.bool, device=device)
    mask[1, 10:] = False

    with torch.no_grad():
        out = model(x, mask)

    weights, aux = _extract_weights_and_aux(out)
    assert weights.shape == (2, 13)
    assert aux is not None
    assert aux["score_logits"].shape == (2, 13)
    assert torch.isfinite(weights).all()
    assert weights[1, 10:].abs().max().item() < 1e-6
    assert torch.all(weights.abs().sum(dim=1) <= 1.0 + 1e-5)
    assert bool((weights > 0).any().item())
    assert bool((weights < 0).any().item())


@pytest.mark.parametrize(
    "family",
    ONLINE_SAFE_TEMPORAL_BASIS_FAMILIES,
)
def test_temporal_basis_banks_are_orthonormal_and_non_dc(family: str) -> None:
    basis = _temporal_basis_matrix(family, steps=32, components=8).double()

    torch.testing.assert_close(
        basis @ basis.transpose(0, 1),
        torch.eye(8, dtype=torch.float64),
        rtol=1e-6,
        atol=1e-6,
    )
    torch.testing.assert_close(
        basis.sum(dim=1),
        torch.zeros(8, dtype=torch.float64),
        rtol=0.0,
        atol=1e-6,
    )


def test_temporal_multi_basis_forward_aux_mask_and_gradients() -> None:
    device = _device()
    model = _make_model(
        lookback=8,
        attention_mode="market_token",
        temporal_basis_families=("wavelet", "fourier", "dct"),
        temporal_basis_components=4,
        return_aux=True,
        return_aux_details=True,
    ).train()
    x = torch.randn(2, 8, 13, 11, device=device, requires_grad=True)
    mask = torch.ones(2, 13, dtype=torch.bool, device=device)
    mask[1, -2:] = False

    weights, _scores, aux = model(x, mask, return_aux=True)
    aux["score_logits"].square().mean().backward()

    assert model.temporal_basis_families == ("haar", "fourier", "dct")
    assert weights.shape == (2, 13)
    assert weights[1, -2:].abs().max().item() == 0.0
    assert aux["temporal_basis_input_features"].shape == (2, 13, 312)
    assert aux["temporal_basis_output"].shape == (2, 13, 24)
    for family in model.temporal_basis_families:
        assert aux[f"temporal_basis_{family}_coefficients"].shape == (
            2,
            4,
            13,
            24,
        )
    assert x.grad is not None
    assert torch.isfinite(x.grad).all()
    assert model.temporal_basis_feature_encoder is not None
    encoder = model.temporal_basis_feature_encoder
    assert set(dict(encoder.named_parameters())) == {
        "feature_projection.weight",
        "feature_projection.bias",
    }
    projection_grad = encoder.feature_projection.weight.grad
    assert projection_grad is not None
    assert torch.isfinite(projection_grad).all()


def test_temporal_basis_coefficients_are_plain_input_features() -> None:
    torch.manual_seed(41)
    encoder = TemporalBasisFeatureEncoder(
        lookback=8,
        dim=3,
        families=("dct",),
        components=2,
    )
    temporal = torch.randn(2, 8, 4, 3)
    z_base = torch.randn(2, 4, 3)
    mask = torch.ones(2, 4, dtype=torch.bool)

    output, aux = encoder(temporal, z_base, mask, collect_aux=True)
    basis = encoder.dct_basis
    coefficients = torch.einsum("kl,blsd->bksd", basis, temporal)
    expected_features = torch.cat(
        (z_base, coefficients.permute(0, 2, 1, 3).flatten(start_dim=2)),
        dim=-1,
    )

    torch.testing.assert_close(
        aux["temporal_basis_input_features"],
        expected_features,
    )
    torch.testing.assert_close(
        output,
        encoder.feature_projection(expected_features),
    )
    assert not hasattr(encoder, "gate")
    assert not hasattr(encoder, "fusion")
    assert not hasattr(encoder, "energy_mix_logits")


def test_raw_feature_basis_fused_projection_matches_explicit_coefficients() -> None:
    torch.manual_seed(57)
    encoder = TemporalBasisFeatureEncoder(
        lookback=8,
        dim=3,
        source_dim=5,
        families=("haar", "dct", "learned"),
        components=2,
        fuse_projection=True,
    )
    temporal = torch.randn(2, 8, 4, 5, requires_grad=True)
    z_base = torch.randn(2, 4, 3, requires_grad=True)
    mask = torch.ones(2, 4, dtype=torch.bool)
    mask[1, -1] = False

    with_aux, aux = encoder(temporal, z_base, mask, collect_aux=True)
    fused, _ = encoder(temporal, z_base, mask, collect_aux=False)

    explicit = encoder.feature_projection(aux["temporal_basis_input_features"])
    explicit = explicit.masked_fill(~mask.unsqueeze(-1), 0.0)
    torch.testing.assert_close(fused, with_aux, rtol=0.0, atol=0.0)
    torch.testing.assert_close(fused, explicit, rtol=1e-5, atol=1e-6)
    assert aux["temporal_basis_input_features"].shape == (2, 4, 33)
    assert aux["temporal_basis_original_path"].shape == (2, 4, 3)
    for family in ("haar", "dct", "learned"):
        assert aux[f"temporal_basis_{family}_coefficients"].shape == (
            2,
            2,
            4,
            5,
        )

    fused.square().mean().backward()
    assert temporal.grad is not None
    assert torch.isfinite(temporal.grad).all()
    assert encoder.learned_basis.grad is not None
    assert torch.isfinite(encoder.learned_basis.grad).all()


def test_raw_feature_basis_explainability_decomposition_is_exact() -> None:
    device = _device()
    model = _make_model(
        lookback=8,
        attention_mode="market_token",
        temporal_basis_families=("haar", "dct", "learned"),
        temporal_basis_components=2,
        temporal_basis_input="raw_features",
        portfolio_output_mode="logits",
        return_aux=False,
        return_aux_details=False,
    ).eval()
    x = torch.randn(2, 8, 7, 11, device=device)
    mask = torch.ones(2, 7, dtype=torch.bool, device=device)
    mask[1, -2:] = False

    with torch.no_grad():
        decomposition = model.temporal_basis_decomposition_for_explainability(
            x,
            mask,
        )
        reconstructed = decomposition["reconstructed"]
        fused = decomposition["fused"]
        assert torch.is_tensor(reconstructed)
        assert torch.is_tensor(fused)
        torch.testing.assert_close(reconstructed, fused, rtol=1e-5, atol=2e-6)

        original = decomposition["original_contribution"]
        bias = decomposition["bias_contribution"]
        family_contributions = decomposition["family_contributions"]
        assert torch.is_tensor(original)
        assert torch.is_tensor(bias)
        assert isinstance(family_contributions, dict)
        explicit = original + bias
        for contribution in family_contributions.values():
            explicit = explicit + contribution
        torch.testing.assert_close(explicit, fused, rtol=1e-5, atol=2e-6)

        reused_weights, reused_scores = model.forward_from_stock_embeddings_explainability(
            reconstructed,
            mask,
            return_aux=False,
            return_scores=True,
        )
        direct_weights, direct_scores, _aux = model(x, mask, return_aux=True)
        torch.testing.assert_close(reused_weights, direct_weights, rtol=1e-5, atol=2e-6)
        torch.testing.assert_close(reused_scores, direct_scores, rtol=1e-5, atol=2e-6)
        assert reused_weights[1, -2:].abs().max().item() == 0.0


def test_online_complete_temporal_basis_forward_and_learned_dictionary_grad() -> None:
    device = _device()
    model = _make_model(
        lookback=32,
        attention_mode="temporal_only",
        temporal_pooling="last",
        temporal_query_mode="last_only",
        temporal_basis_families=ONLINE_SAFE_TEMPORAL_BASIS_FAMILIES,
        temporal_basis_components=4,
        return_aux=True,
        return_aux_details=True,
    ).train()
    x = torch.randn(1, 32, 5, 11, device=device, requires_grad=True)
    mask = torch.ones(1, 5, dtype=torch.bool, device=device)

    _weights, _scores, aux = model(x, mask, return_aux=True)
    aux["score_logits"].square().mean().backward()

    encoder = model.temporal_basis_feature_encoder
    assert encoder is not None
    assert isinstance(encoder.learned_basis, torch.nn.Parameter)
    assert set(dict(encoder.named_parameters())) == {
        "learned_basis",
        "feature_projection.weight",
        "feature_projection.bias",
    }
    assert encoder.learned_basis.grad is not None
    assert torch.isfinite(encoder.learned_basis.grad).all()
    assert "temporal_basis_learned_coefficients" in aux
    assert "learned_basis" in encoder.state_dict()
    assert "haar_basis" not in encoder.state_dict()


def test_temporal_basis_rejects_unknown_family_and_non_dyadic_haar() -> None:
    with pytest.raises(ValueError, match="Unsupported temporal basis family"):
        _make_model(temporal_basis_families=("emd",))
    with pytest.raises(ValueError, match="power of two"):
        _make_model(lookback=6, temporal_basis_families=("haar",))
    with pytest.raises(ValueError, match="power of two"):
        _make_model(lookback=6, temporal_basis_families=("walsh",))
    with pytest.raises(ValueError, match="requires the Financial Transformer"):
        _make_model(
            temporal_basis_families=("dct",),
            temporal_basis_input="input_features",
        )


def test_disabled_temporal_basis_keeps_legacy_model_fingerprint_projection() -> None:
    config = load_config("configs/experiment_baseline.yaml")
    config.training.model_name = "transformer_base_portfolio"

    disabled = _active_model_config(config)["values"]
    assert not any(name.startswith("temporal_basis_") for name in disabled)

    config.training.transformer_base_portfolio.temporal_basis_families = ["dct"]
    enabled = _active_model_config(config)["values"]
    assert enabled["temporal_basis_families"] == ["dct"]
    assert enabled["temporal_basis_components"] == 8
    assert "temporal_basis_input" not in enabled

    config.training.transformer_base_portfolio.temporal_basis_input = "raw_features"
    raw_enabled = _active_model_config(config)["values"]
    assert raw_enabled["temporal_basis_input"] == "raw_features"

    config.training.transformer_base_portfolio.temporal_basis_input = "input_features"
    input_enabled = _active_model_config(config)["values"]
    assert input_enabled["temporal_basis_input"] == "input_features"


@pytest.mark.parametrize("feature_idx", [1, 2])
def test_preprojected_explainability_forward_matches_raw_counterfactual(feature_idx: int) -> None:
    device = _device()
    model = _make_model(
        categorical_feature_indices=(2,),
        categorical_embedding_dim=3,
        categorical_embedding_cardinality=16,
    ).eval()
    x = torch.randn(2, 6, 13, 11, device=device)
    x[..., 2] = torch.randint(0, 8, x[..., 2].shape, device=device).float()
    mask = torch.ones(2, 13, dtype=torch.bool, device=device)
    mask[1, -2:] = False
    time_idx = 3
    raw_changed = x.clone()
    raw_changed[:, time_idx, :, feature_idx] = 0.0

    with torch.no_grad():
        raw_output = model(raw_changed, mask)
        base_projected = model.project_features_for_explainability(x)
        base_embedded = model.embed_projected_for_explainability(base_projected)
        changed_slice = x[:, time_idx].clone()
        changed_slice[..., feature_idx] = 0.0
        changed_projected = model.project_features_for_explainability(changed_slice)
        embedded_changed = base_embedded.clone()
        embedded_changed[:, time_idx] += changed_projected - base_projected[:, time_idx]
        embedded_output = model.forward_from_embedded_explainability(embedded_changed, mask)

    raw_weights, raw_aux = _extract_weights_and_aux(raw_output)
    embedded_weights, embedded_aux = _extract_weights_and_aux(embedded_output)
    torch.testing.assert_close(embedded_weights, raw_weights, rtol=2e-5, atol=2e-6)
    assert raw_aux is not None and embedded_aux is not None
    torch.testing.assert_close(
        embedded_aux["score_logits"],
        raw_aux["score_logits"],
        rtol=2e-5,
        atol=2e-6,
    )


@pytest.mark.parametrize("mode", ["latent", "latent_only", "market_token", "temporal_only"])
def test_temporal_stock_embedding_cache_matches_embedded_forward(mode: str) -> None:
    device = _device()
    model = _make_model(attention_mode=mode).eval()
    x = torch.randn(3, 6, 13, 11, device=device)
    mask = torch.ones(3, 13, dtype=torch.bool, device=device)
    mask[1, -4:] = False
    mask[2] = False
    mask[2, 0] = True

    with torch.no_grad():
        projected = model.project_features_for_explainability(x)
        embedded = model.embed_projected_for_explainability(projected)
        expected_weights, expected_scores, expected_aux = (
            model.forward_from_embedded_explainability(
                embedded,
                mask,
                return_aux=True,
            )
        )
        stock_embeddings = model.temporal_stock_embeddings_for_explainability(
            embedded,
            mask,
        )
        actual_weights, actual_scores, actual_aux = (
            model.forward_from_stock_embeddings_explainability(
                stock_embeddings,
                mask,
                return_aux=True,
            )
        )

    assert stock_embeddings.shape == (3, 13, model.d_model)
    torch.testing.assert_close(actual_weights, expected_weights, rtol=2e-5, atol=2e-6)
    torch.testing.assert_close(actual_scores, expected_scores, rtol=2e-5, atol=2e-6)
    torch.testing.assert_close(
        actual_aux["score_logits"],
        expected_aux["score_logits"],
        rtol=2e-5,
        atol=2e-6,
    )
    assert actual_weights[1, -4:].abs().max().item() == 0.0
    assert actual_weights[2, 1:].abs().max().item() == 0.0


@pytest.mark.parametrize("mode", ["latent", "market_token"])
def test_temporal_stock_embedding_cache_exactly_patches_one_changed_source(mode: str) -> None:
    device = _device()
    model = _make_model(
        attention_mode=mode,
        categorical_feature_indices=(2,),
        categorical_embedding_dim=3,
        categorical_embedding_cardinality=16,
    ).eval()
    x = torch.randn(2, 6, 13, 11, device=device)
    x[..., 2] = torch.randint(0, 8, x[..., 2].shape, device=device).float()
    mask = torch.ones(2, 13, dtype=torch.bool, device=device)
    mask[1, -3:] = False
    source = 3
    changed_x = x.clone()
    changed_x[:, :, source, 1] = 0.0

    with torch.no_grad():
        expected_weights, expected_scores, _expected_aux = model(
            changed_x,
            mask,
            return_aux=True,
        )
        base_projected = model.project_features_for_explainability(x)
        base_embedded = model.embed_projected_for_explainability(base_projected)
        cached_stocks = model.temporal_stock_embeddings_for_explainability(
            base_embedded,
            mask,
        ).clone()

        changed_projected = model.project_features_for_explainability(
            changed_x[:, :, source : source + 1]
        )
        changed_source_embedded = base_embedded[:, :, source : source + 1] + (
            changed_projected - base_projected[:, :, source : source + 1]
        )
        changed_stock = model.temporal_stock_embeddings_for_explainability(
            changed_source_embedded,
        )
        cached_stocks[:, source] = changed_stock[:, 0]
        actual_weights, actual_scores, _actual_aux = (
            model.forward_from_stock_embeddings_explainability(
                cached_stocks,
                mask,
                return_aux=True,
            )
        )

    torch.testing.assert_close(actual_weights, expected_weights, rtol=2e-5, atol=2e-6)
    torch.testing.assert_close(actual_scores, expected_scores, rtol=2e-5, atol=2e-6)


@pytest.mark.parametrize("mode", ["full", "axial"])
def test_temporal_stock_embedding_cache_rejects_noncompact_modes(mode: str) -> None:
    device = _device()
    model = _make_model(attention_mode=mode).eval()
    embedded = torch.randn(2, 6, 13, model.d_model, device=device)
    stock_embeddings = torch.randn(2, 13, model.d_model, device=device)
    mask = torch.ones(2, 13, dtype=torch.bool, device=device)

    with pytest.raises(RuntimeError, match="only supported for compact attention modes"):
        model.temporal_stock_embeddings_for_explainability(embedded, mask)
    with pytest.raises(RuntimeError, match="only supported for compact attention modes"):
        model.forward_from_stock_embeddings_explainability(stock_embeddings, mask)


def test_compiled_stock_embedding_explainability_returns_weights_and_scores(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    device = _device()
    model = _make_model(attention_mode="market_token").eval()
    x = torch.randn(2, 6, 13, 11, device=device)
    mask = torch.ones(2, 13, dtype=torch.bool, device=device)
    mask[1, -3:] = False
    compile_kwargs: list[dict[str, object]] = []

    def fake_compile(fn, **kwargs):
        compile_kwargs.append(kwargs)
        return fn

    monkeypatch.setattr(torch, "compile", fake_compile)

    with torch.no_grad():
        embedded = model.embed_projected_for_explainability(
            model.project_features_for_explainability(x)
        )
        stock_embeddings = model.temporal_stock_embeddings_for_explainability(
            embedded,
            mask,
        )
        expected_weights, expected_scores, _expected_aux = (
            model.forward_from_stock_embeddings_explainability(
                stock_embeddings,
                mask,
                return_aux=True,
            )
        )
        actual_weights, actual_scores = (
            model.forward_from_stock_embeddings_explainability_compiled(
                stock_embeddings,
                mask,
            )
        )

    torch.testing.assert_close(actual_weights, expected_weights, rtol=2e-5, atol=2e-6)
    torch.testing.assert_close(actual_scores, expected_scores, rtol=2e-5, atol=2e-6)
    assert compile_kwargs == [
        {
            "dynamic": False,
            "fullgraph": False,
            "options": {"triton.cudagraphs": False},
        }
    ]


@pytest.mark.parametrize(
    ("use_latent_factors", "use_market_tokens", "expected_mode"),
    [
        (True, True, "latent"),
        (True, False, "latent_only"),
        (False, True, "market_token"),
        (False, False, "temporal_only"),
    ],
)
def test_bottleneck_switches_select_all_four_compact_paths(
    use_latent_factors: bool,
    use_market_tokens: bool,
    expected_mode: str,
) -> None:
    device = _device()
    model = _make_model(
        attention_mode="market_token",
        use_latent_factors=use_latent_factors,
        use_market_tokens=use_market_tokens,
    ).eval()
    assert model.requested_attention_mode == "market_token"
    assert model.attention_mode == expected_mode
    assert model.use_latent_factors is use_latent_factors
    assert model.use_market_tokens is use_market_tokens
    assert (model.latent_queries is not None) is use_latent_factors
    assert (model.market_queries is not None) is use_market_tokens
    assert bool(model.latent_blocks) is use_latent_factors
    assert bool(model.stock_read_latent_blocks) is use_latent_factors
    assert bool(model.market_blocks) is use_market_tokens
    assert bool(model.stock_read_market_blocks) is use_market_tokens

    x = torch.randn(2, 6, 13, 11, device=device)
    mask = torch.ones(2, 13, dtype=torch.bool, device=device)
    mask[1, 9:] = False
    with torch.no_grad():
        weights, _, aux = model(x, mask, return_aux=True)

    assert weights.shape == (2, 13)
    assert torch.isfinite(weights).all()
    assert weights[1, 9:].abs().max().item() < 1e-6
    assert ("latent_factors" in aux) is use_latent_factors
    assert ("factor_tokens" in aux) is use_latent_factors
    assert ("z_factor_context" in aux) is use_latent_factors
    assert ("market_tokens" in aux) is use_market_tokens
    assert ("z_market_context" in aux) is use_market_tokens
    assert ("stock_market_gate" in aux) is use_market_tokens
    assert ("z_market_delta" in aux) is use_market_tokens


def test_compact_switches_reject_enabling_bottleneck_in_full_attention() -> None:
    with pytest.raises(ValueError, match="cannot enable compact bottlenecks"):
        _make_model(
            attention_mode="full",
            use_latent_factors=True,
            use_market_tokens=False,
        )


@pytest.mark.parametrize(
    ("mode", "use_latent_factors", "use_market_tokens"),
    [
        ("latent", True, True),
        ("market_token", False, True),
        ("temporal_only", False, False),
    ],
)
def test_explicit_switches_preserve_legacy_preset_state_and_output(
    mode: str,
    use_latent_factors: bool,
    use_market_tokens: bool,
) -> None:
    device = _device()
    legacy = _make_model(attention_mode=mode).eval()
    explicit = _make_model(
        attention_mode=mode,
        use_latent_factors=use_latent_factors,
        use_market_tokens=use_market_tokens,
    ).eval()
    legacy_state = legacy.state_dict()
    explicit_state = explicit.state_dict()
    assert legacy_state.keys() == explicit_state.keys()
    for name in legacy_state:
        assert torch.equal(legacy_state[name], explicit_state[name]), name

    x = torch.randn(2, 6, 13, 11, device=device)
    mask = torch.ones(2, 13, dtype=torch.bool, device=device)
    with torch.no_grad():
        legacy_weights = legacy(x, mask, return_aux=False)
        explicit_weights = explicit(x, mask, return_aux=False)
    assert torch.allclose(legacy_weights, explicit_weights, atol=0.0, rtol=0.0)


def test_latent_only_backward_updates_factors_without_market_parameters() -> None:
    device = _device()
    model = _make_model(
        attention_mode="market_token",
        use_latent_factors=True,
        use_market_tokens=False,
    ).train()
    x = torch.randn(2, 6, 13, 11, device=device)
    mask = torch.ones(2, 13, dtype=torch.bool, device=device)

    _, _, aux = model(x, mask, return_aux=True)
    aux["score_logits"].square().mean().backward()

    assert model.latent_queries is not None
    assert model.latent_queries.grad is not None
    assert torch.isfinite(model.latent_queries.grad).all()
    assert model.market_queries is None
    assert all(not parameter.requires_grad for parameter in model.stock_market_gate.parameters())


def test_full_mode_token_guard() -> None:
    device = _device()
    model = _make_model(attention_mode="full", max_full_tokens=8).eval()
    x = torch.randn(1, 6, 13, 11, device=device)
    mask = torch.ones(1, 13, dtype=torch.bool, device=device)

    with pytest.raises(ValueError, match="attention_mode=full"):
        model(x, mask)


def test_categorical_feature_embedding_forward_path() -> None:
    device = _device()
    model = _make_model(
        num_features=5,
        categorical_feature_indices=[2, 4],
        categorical_embedding_dim=3,
        categorical_embedding_cardinality=16,
    ).eval()
    x = torch.randn(2, 6, 13, 5, device=device)
    x[..., 2] = torch.randint(0, 10, x[..., 2].shape, device=device).float()
    x[..., 4] = torch.randint(0, 2, x[..., 4].shape, device=device).float()
    mask = torch.ones(2, 13, dtype=torch.bool, device=device)

    with torch.no_grad():
        out = model(x, mask)

    weights, _ = _extract_weights_and_aux(out)
    assert model.categorical_feature_indices == (2, 4)
    assert len(model.categorical_embeddings) == 2
    assert weights.shape == (2, 13)
    assert torch.isfinite(weights).all()


def test_sdpa_batch_chunking_matches_unchunked_eval() -> None:
    device = _device()
    unchunked = _make_model(sdpa_batch_limit=0).eval()
    chunked = _make_model(sdpa_batch_limit=3).eval()
    chunked.load_state_dict(unchunked.state_dict())
    x = torch.randn(2, 6, 13, 11, device=device)
    mask = torch.ones(2, 13, dtype=torch.bool, device=device)
    mask[1, 9:] = False

    with torch.no_grad():
        out_a = unchunked(x, mask)
        out_b = chunked(x, mask)

    assert torch.allclose(out_a["weights"], out_b["weights"], atol=1e-5, rtol=1e-5)
    assert torch.allclose(out_a["score_logits"], out_b["score_logits"], atol=1e-5, rtol=1e-5)


def test_modern_components_and_learned_token_aux() -> None:
    device = _device()
    model = _make_model(attention_mode="latent").eval()
    assert isinstance(model.temporal_blocks[0].norm_query, PortfolioRMSNorm)
    assert isinstance(model.temporal_blocks[0].ffn, SwiGLUFeedForward)
    assert model.temporal_blocks[0].attn.qk_norm is True
    assert model.rope_temporal is True
    assert model.latent_queries is not None
    assert tuple(model.latent_queries.shape) == (1, 4, 24)
    assert model.market_queries is not None
    assert tuple(model.market_queries.shape) == (1, 2, 24)
    assert hasattr(model, "score_head")
    assert not hasattr(model, "mu_head")
    assert not hasattr(model, "sigma_head")
    assert not hasattr(model, "confidence_head")

    x = torch.randn(2, 6, 13, 11, device=device)
    mask = torch.ones(2, 13, dtype=torch.bool, device=device)
    with torch.no_grad():
        out = model(x, mask)

    aux = out["aux"]
    assert aux["stock_embedding"].shape == (2, 13, 24)
    assert aux["factor_tokens"].shape == (2, 4, 24)
    assert aux["latent_factors"].shape == (2, 4, 24)
    assert torch.allclose(aux["factor_tokens"], aux["latent_factors"], atol=0.0, rtol=0.0)
    assert aux["market_tokens"].shape == (2, 2, 24)
    assert aux["z_factor_context"].shape == (2, 13, 24)
    assert aux["z_market_context"].shape == (2, 13, 24)
    assert aux["stock_market_gate"].shape == (2, 13, 1)
    assert aux["z_market_delta"].shape == (2, 13, 24)
    assert aux["score_logits"].shape == (2, 13)
    assert aux["rank_logits"].shape == (2, 13)
    assert "dynamic_latent_delta" not in aux
    assert "dynamic_market_delta" not in aux


@pytest.mark.parametrize("mode", ["latent", "market_token"])
def test_learned_token_attention_aux_contract(mode: str) -> None:
    device = _device()
    model = _make_model(attention_mode=mode).eval()
    x = torch.randn(2, 6, 13, 11, device=device)
    mask = torch.ones(2, 13, dtype=torch.bool, device=device)
    mask[1, 9:] = False

    with torch.no_grad():
        weights, _, aux = model(x, mask, return_aux=True)

    assert weights.shape == (2, 13)
    assert weights[1, 9:].abs().max().item() < 1e-6
    assert aux["stock_embedding"].shape == (2, 13, 24)
    assert aux["market_tokens"].shape == (2, 2, 24)
    assert aux["z_market_context"].shape == (2, 13, 24)
    assert aux["stock_market_gate"].shape == (2, 13, 1)
    if mode == "latent":
        assert aux["factor_tokens"].shape == (2, 4, 24)
        assert aux["latent_factors"].shape == (2, 4, 24)
        assert aux["z_factor_context"].shape == (2, 13, 24)
    else:
        assert "factor_tokens" not in aux
        assert "latent_factors" not in aux
        assert "z_factor_context" not in aux
    assert "dynamic_latent_delta" not in aux
    assert "dynamic_market_delta" not in aux


@pytest.mark.parametrize(
    "deprecated_kwarg",
    [
        "dynamic_latent_tokens",
        "dynamic_market_tokens",
        "dynamic_token_hidden_mult",
        "dynamic_token_gate_init",
        "dynamic_token_dropout",
    ],
)
def test_dynamic_token_kwargs_are_not_accepted(deprecated_kwarg: str) -> None:
    with pytest.raises(TypeError):
        _make_model(**{deprecated_kwarg: True})


def test_aux_details_false_keeps_training_output_light() -> None:
    device = _device()
    model = _make_model(attention_mode="latent", return_aux=True, return_aux_details=False).eval()
    x = torch.randn(2, 6, 13, 11, device=device)
    mask = torch.ones(2, 13, dtype=torch.bool, device=device)

    with torch.no_grad():
        light_out = model(x, mask)
        weights, scores, aux = model(x, mask, return_aux=True)

    assert set(light_out) == {
        "weights",
        "scores",
        "score_logits",
        "rank_logits",
        "centered_score_logits",
    }
    assert "aux" not in light_out
    assert torch.allclose(light_out["weights"], weights, atol=1e-6, rtol=1e-6)
    assert torch.allclose(light_out["scores"], scores, atol=1e-6, rtol=1e-6)
    assert aux["token_embedding"].shape == (2, 6, 13, 24)
    assert aux["factor_tokens"].shape == (2, 4, 24)
    assert aux["latent_factors"].shape == (2, 4, 24)
    assert aux["market_tokens"].shape == (2, 2, 24)
    assert aux["stock_market_gate"].shape == (2, 13, 1)


def _windows_from_panel(features: torch.Tensor, date_indices: torch.Tensor, lookback: int) -> torch.Tensor:
    return torch.stack(
        [
            features[int(idx.item()) - lookback + 1 : int(idx.item()) + 1]
            for idx in date_indices.detach().cpu()
        ],
        dim=0,
    )


@pytest.mark.parametrize("date_values", [[5, 6, 7], [5, 8, 10]])
def test_forward_from_panel_equivalence(date_values: list[int]) -> None:
    device = _device()
    model = _make_model(
        attention_mode="market_token",
        temporal_pooling="last",
        temporal_layers=2,
        return_aux=True,
        return_aux_details=False,
    ).eval()
    features = torch.randn(14, 13, 11)
    date_indices = torch.tensor(date_values, dtype=torch.long, device=device)
    x = _windows_from_panel(features, date_indices, model.lookback).to(device=device)
    mask = torch.ones(len(date_values), 13, dtype=torch.bool, device=device)
    mask[-1, 10:] = False

    with torch.no_grad():
        weights_a, scores_a, aux_a = model(x, mask, return_aux=True)
        weights_b, scores_b, aux_b = model.forward_from_panel(features, date_indices, mask, return_aux=True)

    assert torch.allclose(weights_a, weights_b, atol=1e-5, rtol=1e-5)
    assert torch.allclose(scores_a, scores_b, atol=1e-5, rtol=1e-5)
    assert torch.allclose(aux_a["score_logits"], aux_b["score_logits"], atol=1e-5, rtol=1e-5)


def test_forward_from_panel_slab_equivalence_for_contiguous_rows() -> None:
    device = _device()
    model = _make_model(
        attention_mode="market_token",
        temporal_pooling="last",
        temporal_layers=2,
        return_aux=True,
        return_aux_details=False,
    ).eval()
    features = torch.randn(14, 13, 11)
    date_indices = torch.tensor([5, 6, 7], dtype=torch.long, device=device)
    feature_slab = features.narrow(0, 0, 8)
    x = _windows_from_panel(features, date_indices, model.lookback).to(device=device)
    mask = torch.ones(3, 13, dtype=torch.bool, device=device)
    mask[-1, 10:] = False

    with torch.no_grad():
        weights_x, scores_x, aux_x = model(x, mask, return_aux=True)
        weights_panel, scores_panel, aux_panel = model.forward_from_panel(features, date_indices, mask, return_aux=True)
        weights_slab, scores_slab, aux_slab = model.forward_from_panel_slab(feature_slab, mask, return_aux=True)

    assert torch.allclose(weights_x, weights_panel, atol=1e-5, rtol=1e-5)
    assert torch.allclose(scores_x, scores_panel, atol=1e-5, rtol=1e-5)
    assert torch.allclose(aux_x["score_logits"], aux_panel["score_logits"], atol=1e-5, rtol=1e-5)
    assert torch.allclose(weights_x, weights_slab, atol=1e-5, rtol=1e-5)
    assert torch.allclose(scores_x, scores_slab, atol=1e-5, rtol=1e-5)
    assert torch.allclose(aux_x["score_logits"], aux_slab["score_logits"], atol=1e-5, rtol=1e-5)


def test_temporal_multi_basis_panel_paths_and_embedding_reuse_match() -> None:
    device = _device()
    model = _make_model(
        lookback=8,
        attention_mode="market_token",
        temporal_pooling="last",
        temporal_query_mode="last_only",
        temporal_basis_families=("haar", "fourier", "dct"),
        temporal_basis_components=4,
        return_aux=True,
        return_aux_details=True,
    ).eval()
    feature_slab = torch.randn(11, 13, 11, device=device)
    date_indices = torch.arange(7, 11, dtype=torch.long, device=device)
    windows = feature_slab.unfold(0, 8, 1).permute(0, 3, 1, 2).contiguous()
    mask = torch.ones(4, 13, dtype=torch.bool, device=device)
    mask[-1, -2:] = False

    with torch.no_grad():
        materialized = model(windows, mask, return_aux=True)
        panel = model.forward_from_panel(
            feature_slab,
            date_indices,
            mask,
            return_aux=True,
        )
        slab = model.forward_from_panel_slab(
            feature_slab,
            mask,
            return_aux=True,
        )
        projected = model.project_features_for_explainability(windows)
        embedded = model.embed_projected_for_explainability(projected)
        temporal_embeddings = model.temporal_stock_embeddings_for_explainability(
            embedded,
            mask,
        )

    for expected, actual in ((materialized, panel), (materialized, slab)):
        torch.testing.assert_close(actual[0], expected[0], rtol=1e-5, atol=1e-6)
        torch.testing.assert_close(actual[1], expected[1], rtol=1e-5, atol=1e-6)
        torch.testing.assert_close(
            actual[2]["temporal_basis_output"],
            expected[2]["temporal_basis_output"],
            rtol=1e-5,
            atol=1e-6,
        )
    torch.testing.assert_close(
        temporal_embeddings,
        materialized[2]["stock_embedding"],
        rtol=1e-5,
        atol=1e-6,
    )


def test_panel_slab_compiles_fullgraph_on_cpu_with_compact_symbols_and_backward() -> None:
    if not hasattr(torch, "compile"):
        pytest.skip("torch.compile is unavailable")

    model = _make_model(
        attention_mode="market_token",
        portfolio_mode="long_short",
        portfolio_output_mode="projection_l1",
        temporal_pooling="attention",
        temporal_query_mode="full_then_last",
        allow_dynamic_symbols=False,
        return_aux=False,
        return_aux_details=False,
    ).cpu().train()
    wrapper = _PanelSlabForwardWrapper(model)
    compiled = torch.compile(
        wrapper,
        backend="eager",
        fullgraph=True,
        dynamic=False,
    )

    batch_rows = 3
    compact_symbols = torch.tensor([0, 2, 5, 8, 12], dtype=torch.long)
    feature_slab = torch.randn(
        batch_rows + model.lookback - 1,
        int(compact_symbols.numel()),
        model.num_features,
        requires_grad=True,
    )
    mask = torch.ones(batch_rows, int(compact_symbols.numel()), dtype=torch.bool)
    mask[-1, -1] = False

    with torch.no_grad():
        expected = wrapper(feature_slab, mask, compact_symbols)
    actual = compiled(feature_slab, mask, compact_symbols)
    actual.square().sum().backward()

    assert torch.allclose(actual, expected, atol=1e-6, rtol=1e-6)
    assert torch.all(actual.abs().sum(dim=1) <= 1.0 + 1e-6)
    assert actual[-1, -1].item() == 0.0
    assert feature_slab.grad is not None
    assert torch.isfinite(feature_slab.grad).all()


def test_panel_slab_symbolic_stock_axis_reuses_one_graph_and_matches_eager() -> None:
    if not hasattr(torch, "compile"):
        pytest.skip("torch.compile is unavailable")

    model = _make_model(
        attention_mode="market_token",
        portfolio_mode="long_short",
        portfolio_output_mode="projection_l1",
        temporal_pooling="attention",
        temporal_query_mode="full_then_last",
        allow_dynamic_symbols=False,
        return_aux=False,
        return_aux_details=False,
    ).cpu().train()
    eager = _PanelSlabForwardWrapper(model)
    graph_count = 0

    def counting_backend(graph_module, _example_inputs):
        nonlocal graph_count
        graph_count += 1
        return graph_module.forward

    torch._dynamo.reset()
    try:
        compiled = torch.compile(
            eager,
            backend=counting_backend,
            fullgraph=True,
            dynamic=None,
        )
        symbolic = _DynamicSymbolPanelSlabWrapper(
            compiled,
            min_symbols=4,
            max_symbols=8,
        )

        for symbols in (5, 7, 6):
            feature_slab = torch.randn(
                3 + model.lookback - 1,
                symbols,
                model.num_features,
                requires_grad=True,
            )
            mask = torch.ones(3, symbols, dtype=torch.bool)
            mask[-1, -1] = False
            symbol_indices = torch.arange(symbols, dtype=torch.long)

            with torch.no_grad():
                expected = eager(feature_slab, mask, symbol_indices)
            actual = symbolic(feature_slab, mask, symbol_indices)
            actual.square().sum().backward()

            assert torch.allclose(actual, expected, atol=1e-6, rtol=1e-6)
            assert feature_slab.grad is not None
            assert not hasattr(feature_slab, "_dynamo_dynamic_indices")
            assert not hasattr(mask, "_dynamo_dynamic_indices")
            assert not hasattr(symbol_indices, "_dynamo_dynamic_indices")
            model.zero_grad(set_to_none=True)

        assert graph_count == 1
    finally:
        torch._dynamo.reset()


def test_symbol_indices_length_and_range_checks_remain_strict_in_eager_mode() -> None:
    model = _make_model(allow_dynamic_symbols=False).cpu()

    with pytest.raises(ValueError, match="Expected symbol_indices length"):
        model._check_symbol_indices(torch.tensor([0, 1]), n_symbols=3)
    with pytest.raises(ValueError, match="symbol_indices must be in"):
        model._check_symbol_indices(
            torch.tensor([0, model.num_symbols]),
            n_symbols=2,
        )


def test_panel_slab_dynamic_bounds_do_not_cross_sdpa_loop_count() -> None:
    model = _make_model(sdpa_batch_limit=65_535).cpu()

    assert _panel_slab_dynamic_symbol_bounds(
        model,
        observed_symbols=2_582,
        max_symbols=2_735,
        local_batch_rows=128,
    ) == (2_560, 2_735)
    assert _panel_slab_dynamic_symbol_bounds(
        model,
        observed_symbols=2_653,
        max_symbols=2_735,
        local_batch_rows=128,
    ) == (2_560, 2_735)
    # Lookback-256 uses only 16 local rows under two-rank DDP, so every real
    # train width fits one SDPA interval. Preserve the causal run minimum
    # instead of widening the symbolic domain to impossible tiny universes.
    assert _panel_slab_dynamic_symbol_bounds(
        model,
        observed_symbols=1_244,
        min_symbols=1_244,
        max_symbols=2_598,
        local_batch_rows=16,
    ) == (1_244, 2_598)


def test_symbol_indices_preserve_full_universe_symbol_positions() -> None:
    device = _device()
    symbol_indices_cpu = torch.tensor([0, 2, 5, 8], dtype=torch.long)
    symbol_indices = symbol_indices_cpu.to(device=device)
    full = _make_model(
        attention_mode="market_token",
        num_symbols=13,
        allow_dynamic_symbols=False,
        temporal_pooling="attention",
        temporal_query_mode="full_then_last",
        return_aux=True,
        return_aux_details=False,
    ).eval()
    compact = _make_model(
        attention_mode="market_token",
        num_symbols=int(symbol_indices_cpu.numel()),
        allow_dynamic_symbols=False,
        temporal_pooling="attention",
        temporal_query_mode="full_then_last",
        return_aux=True,
        return_aux_details=False,
    ).eval()
    compact_state = compact.state_dict()
    for name, value in full.state_dict().items():
        if name == "symbol_position":
            compact_state[name].copy_(value.index_select(2, symbol_indices_cpu.to(device=value.device)))
        elif name in compact_state and tuple(compact_state[name].shape) == tuple(value.shape):
            compact_state[name].copy_(value)
    compact.load_state_dict(compact_state)

    x = torch.randn(2, 6, 4, 11, device=device)
    mask = torch.ones(2, 4, dtype=torch.bool, device=device)
    mask[1, 3:] = False
    feature_slab = torch.randn(7, 4, 11, device=device)

    with pytest.raises(ValueError, match="Expected num_symbols=13"):
        full(x, mask)

    with torch.no_grad():
        weights_full, scores_full, aux_full = full(x, mask, return_aux=True, symbol_indices=symbol_indices)
        weights_compact, scores_compact, aux_compact = compact(x, mask, return_aux=True)
        slab_full = full.forward_from_panel_slab(
            feature_slab,
            mask,
            return_aux=False,
            symbol_indices=symbol_indices,
        )
        slab_compact = compact.forward_from_panel_slab(feature_slab, mask, return_aux=False)

    assert torch.allclose(weights_full, weights_compact, atol=1e-5, rtol=1e-5)
    assert torch.allclose(scores_full, scores_compact, atol=1e-5, rtol=1e-5)
    assert torch.allclose(aux_full["score_logits"], aux_compact["score_logits"], atol=1e-5, rtol=1e-5)
    assert torch.allclose(slab_full, slab_compact, atol=1e-5, rtol=1e-5)


def test_fixed_symbol_position_capacity_preserves_parameter_count_for_larger_universe() -> None:
    baseline = _make_model(num_symbols=13, symbol_position_capacity=13).cpu()
    expanded = _make_model(num_symbols=19, symbol_position_capacity=13).cpu()

    assert baseline.symbol_position.shape == (1, 1, 13, baseline.d_model)
    assert expanded.symbol_position.shape == baseline.symbol_position.shape
    assert sum(parameter.numel() for parameter in expanded.parameters()) == sum(
        parameter.numel() for parameter in baseline.parameters()
    )
    expanded.load_state_dict(baseline.state_dict(), strict=True)

    positions = expanded._symbol_position(
        4,
        torch.tensor([0, 12, 13, 18], dtype=torch.long),
    )
    assert torch.equal(positions[:, :, :2], expanded.symbol_position[:, :, [0, 12]])
    assert torch.count_nonzero(positions[:, :, 2:]).item() == 0


def test_fixed_symbol_position_capacity_supports_full_runtime_universe() -> None:
    device = _device()
    model = _make_model(
        num_symbols=19,
        symbol_position_capacity=13,
        attention_mode="market_token",
        allow_dynamic_symbols=False,
        return_aux=False,
        return_aux_details=False,
    ).eval()
    x = torch.randn(2, model.lookback, model.num_symbols, model.num_features, device=device)
    mask = torch.ones(2, model.num_symbols, dtype=torch.bool, device=device)

    with torch.no_grad():
        weights = model(x, mask)

    assert weights.shape == (2, model.num_symbols)
    assert torch.isfinite(weights).all()


def test_fixed_symbol_position_capacity_preserves_ddp_gradient_layout() -> None:
    model = _make_model(
        num_symbols=19,
        symbol_position_capacity=13,
        attention_mode="temporal_only",
        allow_dynamic_symbols=False,
        return_aux=False,
        return_aux_details=False,
    ).cpu()
    expanded = model._symbol_position(model.num_symbols)
    expected = torch.cat(
        (
            model.symbol_position,
            model.symbol_position.new_zeros(
                1,
                1,
                model.num_symbols - model.symbol_position_capacity,
                model.d_model,
            ),
        ),
        dim=2,
    )
    torch.testing.assert_close(expanded, expected)
    expanded.square().sum().backward()

    gradient = model.symbol_position.grad
    assert gradient is not None
    assert gradient.stride() == model.symbol_position.stride()
    assert "symbol_position_prefix_indices" not in model.state_dict()


def test_dynamic_symbol_upper_bound_uses_train_groups_not_future_panel_symbols() -> None:
    dates = 10
    symbols = 8
    tradable = np.zeros((dates, symbols), dtype=bool)
    tradable[:5, :2] = True
    tradable[5:8, :5] = True
    # These newly listed symbols exist only in validation/test dates and must
    # not widen the compiled training ABI.
    tradable[8:, :] = True
    panel = SimpleNamespace(
        num_symbols=symbols,
        tradable_mask=tradable,
        returns_1d=np.zeros((dates, symbols), dtype=np.float32),
        alive_mask=tradable.copy(),
        day_trade_eligible_mask=None,
    )
    grouped_folds = {
        (2001,): [SimpleNamespace(train_indices=np.arange(0, 5))],
        (2001, 2002): [SimpleNamespace(train_indices=np.arange(0, 8))],
    }
    config = SimpleNamespace(
        trading=SimpleNamespace(execution_mode="naive"),
        training=SimpleNamespace(
            lookback=2,
            train_symbol_compaction="train_union",
            train_symbol_compaction_bucket_size=0,
        ),
    )

    assert _train_symbol_compaction_bounds(panel, grouped_folds, config) == (2, 5)
    assert _train_symbol_compaction_upper_bound(panel, grouped_folds, config) == 5

    # Universe changes are re-derived from the current run's data, not retained
    # from the previous shape or hard-coded to a historical market size.
    tradable[7, 5] = True
    assert _train_symbol_compaction_bounds(panel, grouped_folds, config) == (2, 6)
    assert _train_symbol_compaction_upper_bound(panel, grouped_folds, config) == 6


def test_dynamic_symbol_upper_bound_matches_panel_history_compaction() -> None:
    dates = 10
    symbols = 5
    alive = np.zeros((dates, symbols), dtype=bool)
    alive[4:7, :3] = True
    alive[7, 0] = True
    prior_alive = np.zeros_like(alive)
    prior_alive[1:] = alive[:-1]
    panel = SimpleNamespace(
        num_symbols=symbols,
        tradable_mask=alive.copy(),
        returns_1d=np.zeros((dates, symbols), dtype=np.float32),
        alive_mask=alive,
        day_trade_eligible_mask=None,
    )
    train_indices = np.arange(5, 9, dtype=np.int64)
    grouped_folds = {
        (2001,): [SimpleNamespace(train_indices=train_indices)],
    }
    config = SimpleNamespace(
        trading=SimpleNamespace(execution_mode="tw_overnight"),
        training=SimpleNamespace(
            lookback=3,
            train_symbol_compaction="train_union",
            train_symbol_compaction_bucket_size=0,
        ),
        walk_forward=SimpleNamespace(lookback_context="panel_history"),
    )
    split = WindowedSplitTensors(
        features=torch.zeros((dates, symbols, 1)),
        valid_indices=torch.as_tensor(train_indices),
        future_log_returns=torch.zeros((dates, symbols)),
        tradable_mask=torch.from_numpy(prior_alive),
        can_buy_mask=torch.from_numpy(prior_alive.copy()),
        can_sell_mask=torch.from_numpy(prior_alive.copy()),
        benchmark=torch.zeros(dates),
        lookback=3,
        execution_mode="tw_overnight",
        unresolved_corporate_action_mask=torch.zeros(
            (dates, symbols),
            dtype=torch.bool,
        ),
    )

    compacted = _maybe_compact_train_windowed_symbols(
        split,
        config,
        label="panel-history-upper-bound-test",
    )
    upper_bound = _train_symbol_compaction_upper_bound(
        panel,
        grouped_folds,
        config,
    )

    assert compacted.num_symbols == 3
    assert upper_bound == 3
    assert upper_bound >= compacted.num_symbols


def test_train_union_bucket_padding_preserves_output_loss_grad_and_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Compaction is only a representation change, including recurrent state."""
    monkeypatch.setattr(torch.cuda, "is_available", lambda: False)
    torch.manual_seed(101)
    total_symbols = 13
    active_indices = torch.tensor([0, 2, 5, 8, 12], dtype=torch.long)
    valid_indices = torch.tensor([5, 6, 7], dtype=torch.long)
    features = torch.randn(8, total_symbols, 11)
    future_returns = torch.randn(8, total_symbols) * 0.01
    tradable = torch.zeros(8, total_symbols, dtype=torch.bool)
    tradable[valid_indices[:, None], active_indices[None, :]] = True
    split = WindowedSplitTensors(
        features=features,
        valid_indices=valid_indices,
        future_log_returns=future_returns,
        tradable_mask=tradable,
        can_buy_mask=tradable.clone(),
        can_sell_mask=tradable.clone(),
        benchmark=torch.randn(8) * 0.001,
        lookback=6,
        volume_notional=None,
    )
    config = SimpleNamespace(
        training=SimpleNamespace(
            train_symbol_compaction="train_union",
            train_symbol_compaction_bucket_size=4,
        )
    )

    compact = _maybe_compact_train_windowed_symbols(split, config, label="test")

    assert compact.num_symbols == 8
    assert compact.symbol_indices is not None
    assert compact.symbol_indices[: active_indices.numel()].tolist() == active_indices.tolist()
    assert compact.symbol_indices[active_indices.numel() :].tolist() == [0, 0, 0]
    assert not compact.tradable_mask[:, active_indices.numel() :].any()
    assert compact.volume_notional is None

    full_batch = split.panel_slab_batch_by_rows(0, len(split), torch.device("cpu"), non_blocking=False)
    compact_batch = compact.panel_slab_batch_by_rows(
        0,
        len(compact),
        torch.device("cpu"),
        non_blocking=False,
    )
    assert full_batch is not None
    assert compact_batch is not None
    full_slab = full_batch["feature_slab"].detach().clone().requires_grad_(True)
    compact_slab = compact_batch["feature_slab"].detach().clone().requires_grad_(True)

    full_model = _make_model(
        attention_mode="market_token",
        num_symbols=total_symbols,
        allow_dynamic_symbols=False,
        temporal_pooling="last",
        temporal_query_mode="last_only",
        portfolio_output_mode="logits",
        return_aux=False,
        return_aux_details=False,
    ).cpu().train()
    compact_model = copy.deepcopy(full_model)
    full_weights = full_model.forward_from_panel_slab(
        full_slab,
        full_batch["tradable_mask"],
        return_aux=False,
    )
    compact_weights = compact_model.forward_from_panel_slab(
        compact_slab,
        compact_batch["tradable_mask"],
        return_aux=False,
        symbol_indices=compact_batch["symbol_indices"],
    )
    active_count = int(active_indices.numel())

    assert torch.allclose(
        full_weights.index_select(1, active_indices),
        compact_weights[:, :active_count],
        atol=1e-5,
        rtol=1e-5,
    )
    assert torch.count_nonzero(full_weights[:, ~tradable[valid_indices].any(dim=0)]) == 0
    assert torch.count_nonzero(compact_weights[:, active_count:]) == 0

    initial_full = torch.zeros(total_symbols)
    initial_full[active_indices] = torch.tensor([0.08, -0.06, 0.04, -0.02, 0.01])
    initial_compact = torch.cat((initial_full[active_indices], torch.zeros(3)))
    full_state = {"initial_weights": initial_full, "initial_alive": torch.tensor(True)}
    compact_state = {"initial_weights": initial_compact, "initial_alive": torch.tensor(True)}
    loss_kwargs = {
        "long_only": False,
        "buy_fee_rate": 0.001,
        "sell_fee_rate": 0.002,
        "max_turnover_ratio": 0.6,
        "gross_leverage": 1.0,
        "min_trade_weight": 0.0,
        "portfolio_activation": "identity",
        "gamma_sharpe": 1.0,
        "gamma_turnover": 0.03,
        "objective": "log_utility",
        "concentration_weight": 0.0,
        "net_exposure_weight": 0.0,
    }
    full_loss = risk_aware_loss(
        full_weights,
        full_batch["future_log_returns"],
        full_batch["tradable_mask"],
        benchmark_returns=full_batch["benchmark"],
        can_buy_mask=full_batch["can_buy_mask"],
        can_sell_mask=full_batch["can_sell_mask"],
        can_short_open_mask=full_batch["can_short_open_mask"],
        force_short_cover_mask=full_batch["force_short_cover_mask"],
        force_exit_mask=full_batch["force_exit_mask"],
        sample_mask=full_batch["sample_mask"],
        aux_outputs=full_state,
        **loss_kwargs,
    )
    compact_loss = risk_aware_loss(
        compact_weights,
        compact_batch["future_log_returns"],
        compact_batch["tradable_mask"],
        benchmark_returns=compact_batch["benchmark"],
        can_buy_mask=compact_batch["can_buy_mask"],
        can_sell_mask=compact_batch["can_sell_mask"],
        can_short_open_mask=compact_batch["can_short_open_mask"],
        force_short_cover_mask=compact_batch["force_short_cover_mask"],
        force_exit_mask=compact_batch["force_exit_mask"],
        sample_mask=compact_batch["sample_mask"],
        aux_outputs=compact_state,
        **loss_kwargs,
    )

    assert torch.allclose(compact_loss, full_loss, atol=2e-6, rtol=2e-6)
    assert torch.allclose(
        compact_state["_final_weights"][:active_count],
        full_state["_final_weights"].index_select(0, active_indices),
        atol=2e-6,
        rtol=2e-6,
    )
    assert torch.count_nonzero(compact_state["_final_weights"][active_count:]) == 0
    assert torch.count_nonzero(full_state["_final_weights"][~tradable[valid_indices].any(dim=0)]) == 0
    assert torch.equal(compact_state["_final_alive"], full_state["_final_alive"])

    full_loss.backward()
    compact_loss.backward()
    assert full_slab.grad is not None
    assert compact_slab.grad is not None
    assert torch.allclose(
        compact_slab.grad[:, :active_count],
        full_slab.grad.index_select(1, active_indices),
        atol=2e-5,
        rtol=2e-5,
    )
    assert torch.count_nonzero(compact_slab.grad[:, active_count:]) == 0
    assert torch.count_nonzero(full_slab.grad[:, ~tradable[valid_indices].any(dim=0)]) == 0
    for (full_name, full_param), (compact_name, compact_param) in zip(
        full_model.named_parameters(),
        compact_model.named_parameters(),
        strict=True,
    ):
        assert compact_name == full_name
        assert (compact_param.grad is None) == (full_param.grad is None), full_name
        if full_param.grad is not None:
            assert torch.allclose(compact_param.grad, full_param.grad, atol=2e-5, rtol=2e-5), full_name


@pytest.mark.parametrize("mode", ["axial", "latent", "market_token", "temporal_only"])
def test_last_pooling_fast_path_matches_full_temporal_path(mode: str) -> None:
    device = _device()
    model = _make_model(
        attention_mode=mode,
        temporal_pooling="last",
        temporal_layers=2,
        temporal_self_attention_fast_path=True,
        return_aux=True,
        return_aux_details=False,
    ).eval()
    with torch.no_grad():
        model.temporal_blocks[-1].norm_context.weight.fill_(2.0)
    x = torch.randn(2, 6, 13, 11, device=device)
    mask = torch.ones(2, 13, dtype=torch.bool, device=device)
    mask[1, 11:] = False

    with torch.no_grad():
        fast_out = model(x, mask)
        full_weights, full_scores, full_aux = model(x, mask, return_aux=True)

    assert "aux" not in fast_out
    assert full_aux["token_embedding"].shape == (2, 6, 13, 24)
    assert torch.allclose(fast_out["weights"], full_weights, atol=1e-5, rtol=1e-5)
    assert torch.allclose(fast_out["scores"], full_scores, atol=1e-5, rtol=1e-5)


def test_amp_native_position_add_keeps_bfloat16_residual() -> None:
    device = _device()
    model = _make_model(
        amp_native_position_add=True,
        sanitize_inputs=False,
        return_aux=False,
    ).to(device).eval()
    x = torch.randn(2, 6, 13, 11, device=device)
    device_type = "cuda" if device.type == "cuda" else "cpu"

    with torch.autocast(device_type=device_type, dtype=torch.bfloat16):
        embedded = model._embed_inputs(x)

    assert embedded.dtype == torch.bfloat16


@pytest.mark.skipif(not torch.cuda.is_available(), reason="CUDA BF16 cache equivalence")
def test_bfloat16_feature_cache_path_matches_amp_output_and_gradients() -> None:
    reference = _make_model(
        attention_mode="market_token",
        temporal_pooling="last",
        temporal_query_mode="last_only",
        temporal_layers=2,
        sanitize_inputs=True,
        amp_native_position_add=False,
        return_aux=False,
        return_aux_details=False,
    ).train()
    optimized = copy.deepcopy(reference)
    optimized.sanitize_inputs = False
    optimized.amp_native_position_add = False

    feature_slab = torch.randn(9, 13, 11, device="cuda", dtype=torch.float32)
    mask = torch.ones(4, 13, device="cuda", dtype=torch.bool)
    mask[1, 11:] = False
    upstream = torch.randn(4, 13, device="cuda", dtype=torch.float32)

    with torch.autocast(device_type="cuda", dtype=torch.bfloat16):
        reference_output = reference.forward_from_panel_slab(feature_slab, mask, return_aux=False)
        optimized_output = optimized.forward_from_panel_slab(
            feature_slab.to(dtype=torch.bfloat16),
            mask,
            return_aux=False,
        )
    (reference_output.float() * upstream).sum().backward()
    (optimized_output.float() * upstream).sum().backward()

    torch.testing.assert_close(optimized_output.float(), reference_output.float(), atol=1e-6, rtol=1e-6)
    reference_parameters = dict(reference.named_parameters())
    optimized_parameters = dict(optimized.named_parameters())
    assert reference_parameters.keys() == optimized_parameters.keys()
    assert sum(parameter.numel() for parameter in reference_parameters.values()) == sum(
        parameter.numel() for parameter in optimized_parameters.values()
    )
    for name, reference_parameter in reference_parameters.items():
        optimized_parameter = optimized_parameters[name]
        assert (reference_parameter.grad is None) == (optimized_parameter.grad is None), name
        if reference_parameter.grad is not None:
            torch.testing.assert_close(
                optimized_parameter.grad,
                reference_parameter.grad,
                atol=1e-6,
                rtol=1e-6,
                msg=lambda message, name=name: f"{name}: {message}",
            )


def test_compiled_cross_attention_workaround_is_blackwell_only(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(torch.cuda, "is_available", lambda: True)
    monkeypatch.setattr(torch.cuda, "get_device_capability", lambda: (8, 9))
    assert not _compiled_cross_attention_requires_blackwell_workaround()
    monkeypatch.setattr(torch.cuda, "get_device_capability", lambda: (12, 0))
    assert _compiled_cross_attention_requires_blackwell_workaround()


@pytest.mark.skipif(not torch.cuda.is_available(), reason="CUDA module migration contract")
def test_cross_attention_apply_recomputes_blackwell_workaround_for_parameter_device(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen_devices: list[torch.device | str | int | None] = []

    def fake_requires_workaround(device=None) -> bool:
        seen_devices.append(device)
        return device is not None and torch.device(device).type == "cuda"

    monkeypatch.setattr(
        transformer_module,
        "_compiled_cross_attention_requires_blackwell_workaround",
        fake_requires_workaround,
    )
    attention = FlashSDPAAttention(dim=16, num_heads=4, dropout=0.0)
    assert attention.compiled_cross_attention_blackwell_workaround is False
    assert seen_devices == [None]

    attention = attention.cuda()
    assert torch.device(seen_devices[-1]) == attention.in_proj.weight.device
    assert attention.compiled_cross_attention_blackwell_workaround is True

    attention = attention.cpu()
    assert torch.device(seen_devices[-1]) == attention.in_proj.weight.device
    assert attention.compiled_cross_attention_blackwell_workaround is False


@pytest.mark.skipif(not torch.cuda.is_available(), reason="CUDA BF16 cross-attention equivalence")
@pytest.mark.parametrize("query_steps", [1, 3])
def test_dynamo_manual_cross_attention_matches_eager_sdpa_output_and_grad(query_steps: int) -> None:
    """The optimized compiled branch must preserve eager SDPA numerics."""
    torch.manual_seed(103)
    eager = FlashSDPAAttention(
        dim=16,
        num_heads=4,
        dropout=0.0,
        use_flash_attention=True,
        sdpa_batch_limit=4096,
        qk_norm=True,
    ).cuda().train()
    eager.compiled_cross_attention_backend = "manual"
    compiled_module = copy.deepcopy(eager)
    compiled = torch.compile(
        compiled_module,
        backend="eager",
        fullgraph=True,
        dynamic=False,
    )
    query_eager = torch.randn(2, query_steps, 16, device="cuda", requires_grad=True)
    context_eager = torch.randn(2, 7, 16, device="cuda", requires_grad=True)
    query_compiled = query_eager.detach().clone().requires_grad_(True)
    context_compiled = context_eager.detach().clone().requires_grad_(True)
    key_mask = torch.tensor(
        [[True, True, True, True, True, True, True], [True, True, True, True, False, False, False]],
        device="cuda",
    )

    with torch.autocast(device_type="cuda", dtype=torch.bfloat16):
        eager_output = eager(query_eager, context_eager, key_mask=key_mask)
    upstream = torch.randn_like(eager_output, dtype=torch.float32)
    (eager_output.float() * upstream).sum().backward()
    with torch.autocast(device_type="cuda", dtype=torch.bfloat16):
        compiled_output = compiled(query_compiled, context_compiled, key_mask=key_mask)
    (compiled_output.float() * upstream).sum().backward()

    assert torch.allclose(compiled_output.float(), eager_output.float(), atol=8e-3, rtol=8e-3)
    assert query_eager.grad is not None and query_compiled.grad is not None
    assert context_eager.grad is not None and context_compiled.grad is not None
    assert torch.allclose(query_compiled.grad, query_eager.grad, atol=1e-2, rtol=1e-2)
    assert torch.allclose(context_compiled.grad, context_eager.grad, atol=1e-2, rtol=1e-2)
    for eager_param, compiled_param in zip(eager.parameters(), compiled_module.parameters(), strict=True):
        assert eager_param.grad is not None and compiled_param.grad is not None
        assert torch.allclose(compiled_param.grad, eager_param.grad, atol=3e-2, rtol=2e-2)


def test_last_only_temporal_shapes_and_finite() -> None:
    device = _device()
    model = _make_model(
        attention_mode="market_token",
        temporal_pooling="last",
        temporal_layers=2,
        temporal_query_mode="last_only",
        return_aux=False,
        return_aux_details=False,
    ).eval()
    x = torch.randn(3, 6, 13, 11, device=device)
    mask = torch.ones(3, 13, dtype=torch.bool, device=device)
    mask[0, 9:] = False

    with torch.no_grad():
        weights = model(x, mask, return_aux=False)

    assert weights.shape == (3, 13)
    assert torch.isfinite(weights).all()
    assert weights[0, 9:].abs().max().item() < 1e-6


def test_last_only_main_output_is_invariant_to_aux_collection() -> None:
    device = _device()
    model = _make_model(
        attention_mode="market_token",
        temporal_pooling="last",
        temporal_layers=2,
        temporal_query_mode="last_only",
        return_aux=True,
        return_aux_details=True,
    ).eval()
    x = torch.randn(2, 6, 13, 11, device=device)
    mask = torch.ones(2, 13, dtype=torch.bool, device=device)
    mask[1, 10:] = False

    with torch.no_grad():
        weights_without_aux = model(x, mask, return_aux=False)
        weights_with_aux, _, aux = model(x, mask, return_aux=True)
        default_output = model(x, mask)

    assert aux["token_embedding"].shape == (2, 1, 13, 24)
    assert torch.allclose(weights_without_aux, weights_with_aux, atol=1e-6, rtol=1e-6)
    assert torch.allclose(
        weights_without_aux,
        default_output["weights"],
        atol=1e-6,
        rtol=1e-6,
    )


def test_market_token_fast_path_matches_generic_branch() -> None:
    device = _device()
    model = _make_model(
        attention_mode="market_token",
        temporal_pooling="last",
        temporal_layers=2,
        return_aux=True,
        return_aux_details=True,
    ).eval()
    x = torch.randn(2, 6, 13, 11, device=device)
    mask = torch.ones(2, 13, dtype=torch.bool, device=device)
    mask[1, 11:] = False
    safe_mask = mask.clone()
    h = model._embed_inputs(x)

    with torch.no_grad():
        z_fast, aux_fast = model._forward_market_token_fast(h, safe_mask, collect_aux=True)
        z_generic, aux_generic = model._forward_latent_or_market(
            h,
            safe_mask,
            use_latent=False,
            collect_aux=True,
        )

    assert torch.allclose(z_fast, z_generic, atol=1e-5, rtol=1e-5)
    assert torch.allclose(aux_fast["market_tokens"], aux_generic["market_tokens"], atol=1e-5, rtol=1e-5)
    assert "latent_factors" not in aux_fast


def test_windowed_metadata_batch_has_no_x() -> None:
    features = torch.randn(8, 4, 3)
    valid_indices = torch.tensor([2, 3, 4, 6], dtype=torch.long)
    split = WindowedSplitTensors(
        features=features,
        valid_indices=valid_indices,
        future_log_returns=torch.randn(8, 4),
        tradable_mask=torch.ones(8, 4, dtype=torch.bool),
        can_buy_mask=torch.ones(8, 4, dtype=torch.bool),
        can_sell_mask=torch.ones(8, 4, dtype=torch.bool),
        benchmark=torch.randn(8),
        lookback=3,
        sample_mask=torch.tensor([True, False, True, True]),
    )

    batch = split.batch_metadata_by_rows(1, 3, torch.device("cpu"), non_blocking=False)
    indexed = split.batch_metadata_by_batch_indices(torch.tensor([0, 3]), torch.device("cpu"), non_blocking=False)

    assert "x" not in batch
    assert batch["date_indices"].tolist() == [3, 4]
    assert batch["date_start"].tolist() == [3]
    assert bool(batch["rows_are_contiguous"].item()) is True
    assert batch["sample_mask"].tolist() == [False, True]
    assert "x" not in indexed
    assert indexed["date_indices"].tolist() == [2, 6]
    assert indexed["date_start"].tolist() == [2]
    assert bool(indexed["rows_are_contiguous"].item()) is False

    compact = split.subset_symbols(torch.tensor([1, 3], dtype=torch.long))
    compact_batch = compact.batch_metadata_by_rows(1, 3, torch.device("cpu"), non_blocking=False)
    assert compact.features.shape == (8, 2, 3)
    assert compact.symbol_indices.tolist() == [1, 3]
    assert compact_batch["symbol_indices"].tolist() == [1, 3]
    assert compact_batch["tradable_mask"].shape == (2, 2)


def test_legacy_norm_and_ffn_can_be_configured() -> None:
    device = _device()
    model = _make_model(
        norm_type="layernorm",
        ffn_type="gelu",
        qk_norm=False,
        rope_temporal=False,
    ).eval()
    assert isinstance(model.temporal_blocks[0].norm_query, torch.nn.LayerNorm)
    assert not isinstance(model.temporal_blocks[0].ffn, SwiGLUFeedForward)
    assert model.temporal_blocks[0].attn.qk_norm is False

    x = torch.randn(1, 6, 13, 11, device=device)
    mask = torch.ones(1, 13, dtype=torch.bool, device=device)
    with torch.no_grad():
        out = model(x, mask)

    aux = out["aux"]
    assert "dynamic_latent_delta" not in aux
    assert "dynamic_market_delta" not in aux


@pytest.mark.skipif(not torch.cuda.is_available(), reason="CUDA-only SDPA batch-limit smoke")
def test_large_temporal_batch_uses_chunked_sdpa_without_cuda_invalid_argument() -> None:
    device = _device()
    model = _make_model(
        lookback=32,
        num_symbols=128,
        d_model=32,
        temporal_heads=4,
        cross_heads=4,
        joint_heads=4,
        head_hidden_dim=32,
        sdpa_batch_limit=1024,
        return_aux_details=False,
    ).eval()
    x = torch.randn(8, 32, 128, 11, device=device)
    mask = torch.ones(8, 128, dtype=torch.bool, device=device)

    with torch.no_grad():
        out = model(x, mask)
        torch.cuda.synchronize(device)

    assert out["weights"].shape == (8, 128)
    assert torch.isfinite(out["weights"]).all()


def test_long_only_mode_empty_row_is_finite_and_zero() -> None:
    device = _device()
    model = _make_model(portfolio_mode="long_only").eval()
    x = torch.randn(2, 6, 13, 11, device=device)
    mask = torch.ones(2, 13, dtype=torch.bool, device=device)
    mask[0, :] = False

    with torch.no_grad():
        weights, scores, aux = model(x, mask, return_aux=True)

    assert torch.isfinite(weights).all()
    assert torch.isfinite(scores).all()
    assert torch.isfinite(aux["market_tokens"]).all()
    torch.testing.assert_close(weights[0], torch.zeros_like(weights[0]))


def test_portfolio_output_mode_l1_uses_identity_l1_weights() -> None:
    device = _device()
    model = _make_model(
        attention_mode="market_token",
        portfolio_mode="long_short",
        portfolio_activation="tanh",
        portfolio_output_mode="l1",
    ).eval()
    x = torch.randn(2, 6, 13, 11, device=device)
    mask = torch.ones(2, 13, dtype=torch.bool, device=device)
    mask[1, 10:] = False

    with torch.no_grad():
        weights, _, aux = model(x, mask, return_aux=True)

    expected = masked_activation_l1_weights(
        aux["centered_score_logits"],
        mask,
        long_only=False,
        activation="identity",
    )
    assert model.portfolio_output_mode == "l1"
    assert torch.allclose(weights, expected, atol=1e-6, rtol=1e-6)
    long_gross = weights.clamp_min(0.0).sum(dim=1)
    short_gross = (-weights.clamp_max(0.0)).sum(dim=1)
    expected_gross = torch.ones_like(long_gross)
    assert torch.allclose(weights.abs().sum(dim=1), expected_gross, atol=1e-6, rtol=1e-6)
    assert torch.allclose(long_gross + short_gross, expected_gross, atol=1e-6, rtol=1e-6)


def test_cash_l1_jointly_normalizes_stock_and_model_cash_scores() -> None:
    stock_logits = torch.tensor(
        [[2.0, -1.0, 9.0], [0.0, 0.0, 0.0]], requires_grad=True
    )
    cash_logits = torch.tensor([0.25, -0.5], requires_grad=True)
    mask = torch.tensor([[True, True, False], [False, False, False]])

    weights, cash_weight, positive_cash_score = masked_cash_asset_l1_weights(
        stock_logits,
        cash_logits,
        mask,
        long_only=False,
    )

    assert weights[0, 2].item() == 0.0
    assert torch.equal(weights[1], torch.zeros_like(weights[1]))
    torch.testing.assert_close(
        weights.abs().sum(dim=1) + cash_weight,
        torch.ones_like(cash_weight),
    )
    assert cash_weight[1].item() == pytest.approx(1.0)
    assert bool(torch.all(positive_cash_score > 0.0))
    (weights.square().sum() + cash_weight.square().sum()).backward()
    assert cash_logits.grad is not None
    assert bool(torch.isfinite(cash_logits.grad).all())


def test_transformer_cash_l1_scores_cash_as_contextual_extra_asset() -> None:
    device = _device()
    model = _make_model(
        portfolio_output_mode="cash_l1",
        center_long_short_logits=False,
        return_aux=True,
        return_aux_details=True,
    ).train()
    assert model.cash_asset_token is not None
    assert model.cash_asset_norm is not None
    assert not hasattr(model, "cash_score_head")
    x = torch.randn(2, 6, 13, 11, device=device)
    mask = torch.ones(2, 13, dtype=torch.bool, device=device)
    mask[0, 10:] = False
    mask[1] = False

    weights, _, aux = model(x, mask, return_aux=True)

    assert tuple(aux["cash_score_logits"].shape) == (2,)
    assert tuple(aux["cash_weight"].shape) == (2,)
    assert tuple(aux["score_logits_with_cash"].shape) == (2, 14)
    assert tuple(aux["allocation_scores_with_cash"].shape) == (2, 14)
    assert tuple(aux["weights_with_cash"].shape) == (2, 14)
    torch.testing.assert_close(aux["weights_with_cash"][:, :-1], weights)
    torch.testing.assert_close(
        aux["weights_with_cash"][:, -1], aux["cash_weight"]
    )
    expected_weights, expected_cash, _ = masked_cash_asset_l1_weights(
        aux["centered_score_logits"],
        aux["cash_target_logits"],
        mask,
        long_only=False,
    )
    torch.testing.assert_close(weights, expected_weights)
    torch.testing.assert_close(aux["cash_weight"], expected_cash)
    assert torch.equal(weights[1], torch.zeros_like(weights[1]))
    torch.testing.assert_close(
        weights.abs().sum(dim=1) + aux["cash_weight"],
        torch.ones(2, device=device),
        atol=1e-5,
        rtol=1e-5,
    )
    assert aux["cash_weight"][1].item() == pytest.approx(1.0)
    loss = weights[0].square().sum() + aux["cash_weight"][0].square()
    loss.backward()
    assert model.cash_asset_token.grad is not None
    assert bool(torch.isfinite(model.cash_asset_token.grad).all())


def test_portfolio_output_mode_logits_returns_masked_centered_scores() -> None:
    device = _device()
    model = _make_model(
        attention_mode="market_token",
        portfolio_mode="long_short",
        portfolio_output_mode="logits",
    ).eval()
    x = torch.randn(2, 6, 13, 11, device=device)
    mask = torch.ones(2, 13, dtype=torch.bool, device=device)
    mask[1, 10:] = False

    with torch.no_grad():
        weights, _, aux = model(x, mask, return_aux=True)

    expected = aux["centered_score_logits"].masked_fill(~mask, 0.0)
    assert model.portfolio_output_mode == "logits"
    assert torch.allclose(weights, expected, atol=1e-6, rtol=1e-6)
    assert weights[1, 10:].abs().max().item() < 1e-6


def test_score_sanitization_preserves_finite_values_outside_legacy_clip() -> None:
    scores = torch.tensor([[-100.0, 100.0, float("nan"), float("inf"), -float("inf")]], dtype=torch.float32)

    sanitized = _sanitize_scores_to_dtype(scores)

    assert sanitized[0, 0].item() == -100.0
    assert sanitized[0, 1].item() == 100.0
    assert sanitized[0, 2].item() == 0.0
    assert sanitized[0, 3].item() == torch.finfo(torch.float32).max
    assert sanitized[0, 4].item() == torch.finfo(torch.float32).min


def test_l1_projection_preserves_large_finite_values_inside_radius() -> None:
    logits = torch.tensor([[100.0, -100.0]], dtype=torch.float32)
    mask = torch.ones_like(logits, dtype=torch.bool)

    projected = masked_l1_projection_weights(logits, mask, radius=250.0)

    assert torch.equal(projected, logits)


def test_long_short_logit_centering_can_be_disabled() -> None:
    device = _device()
    model = _make_model(
        attention_mode="market_token",
        portfolio_mode="long_short",
        portfolio_output_mode="logits",
        center_long_short_logits=False,
    ).eval()
    x = torch.randn(2, 6, 13, 11, device=device)
    mask = torch.ones(2, 13, dtype=torch.bool, device=device)
    mask[1, 10:] = False

    with torch.no_grad():
        weights, _, aux = model(x, mask, return_aux=True)

    expected = aux["score_logits"].masked_fill(~mask, 0.0)
    assert not model.center_long_short_logits
    assert torch.allclose(weights, expected, atol=1e-6, rtol=1e-6)
    assert torch.allclose(aux["centered_score_logits"], aux["score_logits"], atol=1e-6, rtol=1e-6)


def test_portfolio_output_mode_signed_softmax_matches_action_helper() -> None:
    device = _device()
    model = _make_model(
        attention_mode="market_token",
        portfolio_mode="long_short",
        portfolio_output_mode="signed_action_softmax",
    ).eval()
    x = torch.randn(2, 6, 13, 11, device=device)
    mask = torch.ones(2, 13, dtype=torch.bool, device=device)
    mask[1, 10:] = False

    with torch.no_grad():
        weights, _, aux = model(x, mask, return_aux=True)

    expected, expected_parts = masked_signed_action_weights(
        aux["centered_score_logits"],
        mask,
        transform="softmax",
        long_only=False,
        return_parts=True,
    )
    assert model.portfolio_output_mode == "signed_softmax"
    assert torch.allclose(weights, expected, atol=1e-6, rtol=1e-6)
    assert torch.allclose(aux["action_long_alloc"], expected_parts["action_long_alloc"], atol=1e-6, rtol=1e-6)
    assert torch.allclose(aux["action_short_alloc"], expected_parts["action_short_alloc"], atol=1e-6, rtol=1e-6)
    assert torch.allclose(aux["action_cash_alloc"], expected_parts["action_cash_alloc"], atol=1e-6, rtol=1e-6)
    assert torch.all(weights.abs().sum(dim=1) <= 1.0 + 1e-6)
    assert weights[1, 10:].abs().max().item() < 1e-6


def test_portfolio_output_mode_signed_entmax_matches_sparse_action_helper() -> None:
    device = _device()
    direct_logits = torch.tensor([[8.0, 1.0, -8.0, 0.0]], dtype=torch.float32, device=device)
    direct_mask = torch.ones_like(direct_logits, dtype=torch.bool)
    _, direct_parts = masked_signed_action_weights(
        direct_logits,
        direct_mask,
        transform="entmax15",
        long_only=False,
        return_parts=True,
    )
    direct_actions = torch.cat(
        [
            direct_parts["action_long_alloc"],
            direct_parts["action_short_alloc"],
            direct_parts["action_cash_alloc"].view(1, 1),
        ],
        dim=1,
    )
    assert int((direct_actions <= 1e-7).sum().item()) >= 1

    model = _make_model(
        attention_mode="market_token",
        portfolio_mode="long_short",
        portfolio_output_mode="signed_entmax",
    ).eval()
    x = torch.randn(2, 6, 13, 11, device=device)
    mask = torch.ones(2, 13, dtype=torch.bool, device=device)
    mask[1, 10:] = False

    with torch.no_grad():
        weights, _, aux = model(x, mask, return_aux=True)

    expected, _ = masked_signed_action_weights(
        aux["centered_score_logits"],
        mask,
        transform="entmax15",
        long_only=False,
        return_parts=True,
    )
    assert model.portfolio_output_mode == "signed_entmax15"
    assert torch.allclose(weights, expected, atol=1e-5, rtol=1e-5)
    assert torch.all(weights.abs().sum(dim=1) <= 1.0 + 1e-6)
    assert weights[1, 10:].abs().max().item() < 1e-6


def test_portfolio_output_mode_signed_sparsemax_matches_sparse_action_helper() -> None:
    device = _device()
    direct_logits = torch.tensor([[4.0, 1.0, -4.0, 0.0]], dtype=torch.float32, device=device)
    direct_mask = torch.ones_like(direct_logits, dtype=torch.bool)
    _, direct_parts = masked_signed_action_weights(
        direct_logits,
        direct_mask,
        transform="sparsemax",
        long_only=False,
        return_parts=True,
    )
    direct_actions = torch.cat(
        [
            direct_parts["action_long_alloc"],
            direct_parts["action_short_alloc"],
            direct_parts["action_cash_alloc"].view(1, 1),
        ],
        dim=1,
    )
    assert int((direct_actions <= 1e-7).sum().item()) >= 1

    model = _make_model(
        attention_mode="market_token",
        portfolio_mode="long_short",
        portfolio_output_mode="signed_sparsemax",
    ).eval()
    x = torch.randn(2, 6, 13, 11, device=device)
    mask = torch.ones(2, 13, dtype=torch.bool, device=device)
    mask[1, 10:] = False

    with torch.no_grad():
        weights, _, aux = model(x, mask, return_aux=True)

    expected, _ = masked_signed_action_weights(
        aux["centered_score_logits"],
        mask,
        transform="sparsemax",
        long_only=False,
        return_parts=True,
    )
    assert model.portfolio_output_mode == "signed_sparsemax"
    assert torch.allclose(weights, expected, atol=1e-6, rtol=1e-6)
    assert torch.all(weights.abs().sum(dim=1) <= 1.0 + 1e-6)
    assert weights[1, 10:].abs().max().item() < 1e-6


def test_portfolio_output_mode_projection_l1_matches_projection_helper() -> None:
    device = _device()
    model = _make_model(
        attention_mode="market_token",
        portfolio_mode="long_short",
        portfolio_output_mode="differentiable_projection",
    ).eval()
    x = torch.randn(2, 6, 13, 11, device=device)
    mask = torch.ones(2, 13, dtype=torch.bool, device=device)
    mask[1, 10:] = False

    with torch.no_grad():
        weights, _, aux = model(x, mask, return_aux=True)

    expected = masked_l1_projection_weights(aux["centered_score_logits"], mask, long_only=False)
    assert model.portfolio_output_mode == "projection_l1"
    assert torch.allclose(weights, expected, atol=1e-6, rtol=1e-6)
    assert torch.all(weights.abs().sum(dim=1) <= 1.0 + 1e-6)
    assert torch.allclose(aux["projection_gross_exposure"], weights.abs().sum(dim=1), atol=1e-6, rtol=1e-6)
    assert weights[1, 10:].abs().max().item() < 1e-6


def test_portfolio_output_mode_cash_entmax_matches_cash_helper() -> None:
    device = _device()
    model = _make_model(
        attention_mode="market_token",
        portfolio_mode="long_short",
        portfolio_output_mode="cash_entmax15",
    ).eval()
    x = torch.randn(2, 6, 13, 11, device=device)
    mask = torch.ones(2, 13, dtype=torch.bool, device=device)
    mask[1, 10:] = False

    with torch.no_grad():
        weights, _, aux = model(x, mask, return_aux=True)

    expected, expected_parts = masked_cash_entmax15_weights(
        aux["centered_score_logits"],
        mask,
        short_mask=mask,
        return_parts=True,
    )
    assert model.portfolio_output_mode == "cash_entmax15"
    torch.testing.assert_close(weights, expected)
    torch.testing.assert_close(
        aux["cash_entmax_risk_fraction"],
        expected_parts["cash_entmax_risk_fraction"],
    )
    assert torch.all(weights.abs().sum(dim=1) < 1.0)
    assert weights[1, 10:].abs().max().item() == 0.0


def test_factory_builds_transformer_base_portfolio_model() -> None:
    cfg = load_config(Path("configs/experiment_baseline.yaml"))
    cfg.training.model_name = "transformer_base_portfolio"
    model = build_model(config=cfg, lookback=8, num_features=21, num_symbols=37)

    assert isinstance(model, TransformerBasePortfolioModel)
    assert model_hidden_dim_hint(cfg) == cfg.training.transformer_base_portfolio.d_model
    assert model.attention_mode == cfg.training.transformer_base_portfolio.attention_mode
    assert model.portfolio_mode == "long_short"
    assert model.sdpa_batch_limit == cfg.training.transformer_base_portfolio.sdpa_batch_limit
    assert model.norm_type == cfg.training.transformer_base_portfolio.norm_type
    assert model.ffn_type == cfg.training.transformer_base_portfolio.ffn_type
    assert model.qk_norm == cfg.training.transformer_base_portfolio.qk_norm
    assert model.rope_temporal == cfg.training.transformer_base_portfolio.rope_temporal
    assert model.temporal_query_mode == cfg.training.transformer_base_portfolio.temporal_query_mode
    assert model.portfolio_output_mode == cfg.training.transformer_base_portfolio.portfolio_output_mode
    if model.attention_mode == "market_token":
        assert len(model.latent_blocks) == 0


def test_factory_passes_independent_latent_and_market_switches() -> None:
    cfg = load_config(Path("configs/experiment_baseline.yaml"))
    cfg.training.model_name = "transformer_base_portfolio"
    cfg.training.transformer_base_portfolio.use_latent_factors = True
    cfg.training.transformer_base_portfolio.use_market_tokens = False

    model = build_model(config=cfg, lookback=8, num_features=21, num_symbols=37)

    assert isinstance(model, TransformerBasePortfolioModel)
    assert model.attention_mode == "latent_only"
    assert model.use_latent_factors is True
    assert model.use_market_tokens is False
    assert model.latent_queries is not None
    assert model.market_queries is None
