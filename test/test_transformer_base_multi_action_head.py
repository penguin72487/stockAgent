"""Focused contracts for the shared-backbone multi-action Transformer head."""

from __future__ import annotations

from pathlib import Path

import pytest
import torch

from stockagent.config import load_config
from stockagent.models.factory import build_model
from stockagent.models.financial_transformer import FinancialTransformerModel
from stockagent.models.transformer_base_portfolio import (
    TransformerBasePortfolioModel,
)


def _devices() -> list[str]:
    devices = ["cpu"]
    if torch.cuda.is_available():
        devices.append("cuda")
    return devices


def _make_model(
    *,
    execution_mode: str = "naive",
    portfolio_output_mode: str = "activation_l1",
    device: str = "cpu",
) -> TransformerBasePortfolioModel:
    torch.manual_seed(71)
    if device == "cuda":
        torch.cuda.manual_seed_all(71)
    return TransformerBasePortfolioModel(
        lookback=3,
        num_features=4,
        num_symbols=5,
        d_model=8,
        attention_mode="temporal_only",
        use_flash_attention=True,
        use_time_pos=True,
        use_symbol_pos=True,
        input_dropout=0.0,
        sdpa_batch_limit=128,
        norm_type="rmsnorm",
        ffn_type="swiglu",
        qk_norm=False,
        rope_temporal=True,
        temporal_layers=1,
        temporal_heads=2,
        temporal_ffn_mult=1,
        temporal_pooling="last",
        temporal_query_mode="last_only",
        head_hidden_dim=8,
        head_layers=1,
        dropout=0.0,
        portfolio_mode="long_short",
        portfolio_activation="identity",
        portfolio_output_mode=portfolio_output_mode,
        return_aux=False,
        return_aux_details=False,
        runtime_shape_check=True,
        allow_dynamic_symbols=False,
        execution_mode=execution_mode,
    ).to(device=device)


@pytest.mark.parametrize("device", _devices())
def test_legacy_single_target_head_keeps_strict_state_and_output_contract(
    device: str,
) -> None:
    historical = _make_model(device=device).eval()
    explicit = _make_model(execution_mode="naive", device=device).eval()

    incompatible = explicit.load_state_dict(historical.state_dict(), strict=True)
    assert incompatible.missing_keys == []
    assert incompatible.unexpected_keys == []
    assert explicit.action_schema == "single_target_v1"
    assert explicit.action_channel_names == ("target",)
    assert explicit.num_action_channels == 1
    assert explicit.score_head[-1].out_features == 1
    assert not any(
        key.startswith(("action_head.", "phase_score_head."))
        for key in historical.state_dict()
    )

    x = torch.randn(2, 3, 5, 4, device=device)
    mask = torch.ones(2, 5, dtype=torch.bool, device=device)
    mask[1, -1] = False
    with torch.no_grad():
        expected = historical(x, mask, return_aux=False)
        actual = explicit(x, mask, return_aux=False)

    assert expected.shape == (2, 5)
    torch.testing.assert_close(actual, expected, rtol=0.0, atol=0.0)


@pytest.mark.parametrize("device", _devices())
def test_tw_cash_logits_postprocess_each_phase_independently(device: str) -> None:
    logits_model = _make_model(
        execution_mode="tw_cash",
        portfolio_output_mode="logits",
        device=device,
    ).eval()
    target_model = _make_model(
        execution_mode="tw_cash",
        portfolio_output_mode="activation_l1",
        device=device,
    ).eval()
    target_model.load_state_dict(logits_model.state_dict(), strict=True)

    x = torch.randn(2, 3, 5, 4, device=device)
    mask = torch.ones(2, 5, dtype=torch.bool, device=device)
    mask[1, -2:] = False
    with torch.no_grad():
        raw_logits = logits_model(x, mask, return_aux=False)
        expected_targets = target_model(x, mask, return_aux=False)
        actual_targets = logits_model.postprocess_action_logits(
            raw_logits,
            mask,
        )

    assert logits_model.action_schema == "open_close_targets_v1"
    assert logits_model.action_channel_names == (
        "open_target",
        "close_target",
    )
    assert raw_logits.shape == (2, 2, 5)
    torch.testing.assert_close(
        actual_targets,
        expected_targets,
        rtol=1e-6,
        atol=1e-7,
    )
    torch.testing.assert_close(
        actual_targets.abs().sum(dim=2),
        torch.ones(2, 2, device=device),
        rtol=1e-6,
        atol=1e-6,
    )
    assert torch.count_nonzero(actual_targets[1, :, -2:]).item() == 0


@pytest.mark.parametrize("device", _devices())
def test_tw_overnight_postprocess_bounds_due_exit_and_shares_entry_budget(
    device: str,
) -> None:
    logits_model = _make_model(
        execution_mode="tw_overnight",
        portfolio_output_mode="logits",
        device=device,
    ).eval()
    target_model = _make_model(
        execution_mode="tw_overnight",
        portfolio_output_mode="activation_l1",
        device=device,
    ).eval()
    target_model.load_state_dict(logits_model.state_dict(), strict=True)

    mask = torch.tensor(
        [[True, True, True, True, False], [True, True, True, False, False]],
        dtype=torch.bool,
        device=device,
    )
    raw_logits = torch.tensor(
        [
            [
                [-3.0, -1.0, 0.0, 2.0, 8.0],
                [3.0, -2.0, 1.0, -4.0, 9.0],
                [-1.0, 4.0, -3.0, 2.0, 9.0],
            ],
            [
                [5.0, -5.0, 0.0, 7.0, 7.0],
                [2.0, -1.0, 3.0, 8.0, 8.0],
                [-4.0, 1.0, -2.0, 8.0, 8.0],
            ],
        ],
        device=device,
    )
    with torch.no_grad():
        processed, parts = logits_model.postprocess_action_logits(
            raw_logits,
            mask,
            return_parts=True,
        )

        x = torch.randn(2, 3, 5, 4, device=device)
        model_logits = logits_model(x, mask, return_aux=False)
        expected_targets = logits_model.postprocess_action_logits(
            model_logits,
            mask,
        )
        actual_targets = target_model(x, mask, return_aux=False)

    assert logits_model.action_schema == "due_exit_open_close_entries_v1"
    assert logits_model.action_channel_names == (
        "due_exit_fraction",
        "open_entry_target",
        "close_entry_target",
    )
    assert model_logits.shape == (2, 3, 5)
    assert torch.all((processed[:, 0] >= 0.0) & (processed[:, 0] <= 1.0))
    assert torch.count_nonzero(processed.masked_select(~mask[:, None, :])).item() == 0
    torch.testing.assert_close(
        processed[:, 1:].abs().sum(dim=(1, 2)),
        torch.ones(2, device=device),
        rtol=1e-6,
        atol=1e-6,
    )
    torch.testing.assert_close(
        parts["entry_gross_exposure"],
        torch.ones(2, device=device),
        rtol=1e-6,
        atol=1e-6,
    )
    assert torch.all(processed[:, 1:].abs().sum(dim=2) < 1.0)
    torch.testing.assert_close(
        actual_targets,
        expected_targets,
        rtol=1e-6,
        atol=1e-7,
    )


@pytest.mark.parametrize("execution_mode", ["tw_cash", "tw_overnight"])
@pytest.mark.parametrize("portfolio_output_mode", ["logits", "activation_l1"])
@pytest.mark.parametrize("device", _devices())
def test_multi_action_three_forward_apis_are_equivalent(
    execution_mode: str,
    portfolio_output_mode: str,
    device: str,
) -> None:
    model = _make_model(
        execution_mode=execution_mode,
        portfolio_output_mode=portfolio_output_mode,
        device=device,
    ).eval()
    features = torch.randn(6, 5, 4)
    date_indices = torch.tensor([2, 3], dtype=torch.long, device=device)
    x = torch.stack([features[0:3], features[1:4]], dim=0).to(device=device)
    feature_slab = features[:4]
    mask = torch.ones(2, 5, dtype=torch.bool, device=device)
    mask[1, -2:] = False

    with torch.no_grad():
        materialized = model(x, mask, return_aux=False)
        panel = model.forward_from_panel(
            features,
            date_indices,
            mask,
            return_aux=False,
        )
        slab = model.forward_from_panel_slab(
            feature_slab,
            mask,
            return_aux=False,
        )

    expected_phases = 2 if execution_mode == "tw_cash" else 3
    assert materialized.shape == (2, expected_phases, 5)
    torch.testing.assert_close(panel, materialized, rtol=1e-5, atol=1e-6)
    torch.testing.assert_close(slab, materialized, rtol=1e-5, atol=1e-6)


@pytest.mark.parametrize(
    ("execution_mode", "expected_channels"),
    [("naive", 1), ("tw_day_trade", 1), ("tw_cash", 2), ("tw_overnight", 3)],
)
def test_factory_derives_action_schema_from_execution_mode(
    execution_mode: str,
    expected_channels: int,
) -> None:
    config = load_config(Path("configs/experiment_baseline.yaml"))
    config.training.model_name = "transformer_base_portfolio"
    config.trading.execution_mode = execution_mode

    model = build_model(
        config=config,
        lookback=3,
        num_features=4,
        num_symbols=5,
    )

    assert isinstance(model, TransformerBasePortfolioModel)
    assert model.execution_mode == execution_mode
    assert model.num_action_channels == expected_channels
    assert model.score_head[-1].out_features == expected_channels


@pytest.mark.parametrize(
    ("execution_mode", "expected_channels"),
    [("tw_cash", 2), ("tw_overnight", 3)],
)
@pytest.mark.parametrize("device", _devices())
def test_factory_financial_transformer_inherits_multi_action_shape(
    execution_mode: str,
    expected_channels: int,
    device: str,
) -> None:
    config = load_config(Path("configs/experiment_baseline.yaml"))
    config.training.model_name = "financial_transformer"
    config.trading.execution_mode = execution_mode
    config.training.financial_transformer.d_model = 8
    config.training.financial_transformer.attention_mode = "temporal_only"
    config.training.financial_transformer.use_latent_factors = False
    config.training.financial_transformer.use_market_tokens = False
    config.training.financial_transformer.temporal_layers = 1
    config.training.financial_transformer.temporal_heads = 2
    config.training.financial_transformer.temporal_ffn_mult = 1
    config.training.financial_transformer.temporal_pooling = "last"
    config.training.financial_transformer.temporal_query_mode = "last_only"
    config.training.financial_transformer.head_hidden_dim = 8
    config.training.financial_transformer.head_layers = 1
    config.training.financial_transformer.dropout = 0.0
    config.training.financial_transformer.candle_dropout = 0.0
    config.training.financial_transformer.return_aux = False
    config.training.financial_transformer.return_aux_details = False

    model = build_model(
        config=config,
        lookback=3,
        num_features=4,
        num_symbols=5,
    ).to(device=device).eval()
    x = torch.randn(2, 3, 5, 4, device=device)
    mask = torch.ones(2, 5, dtype=torch.bool, device=device)
    mask[1, -2:] = False

    with torch.no_grad():
        output = model(x, mask, return_aux=False)

    assert isinstance(model, FinancialTransformerModel)
    assert model.execution_mode == execution_mode
    assert model.num_action_channels == expected_channels
    assert output.shape == (2, expected_channels, 5)
    assert torch.count_nonzero(output[1, :, -2:]).item() == 0
