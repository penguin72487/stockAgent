from __future__ import annotations

from pathlib import Path

import pytest
import torch
from torch import nn

from stockagent.config import load_config
from stockagent.models.financial_transformer import CandleEncoder
from stockagent.training.trainer import (
    _PretrainedInitialization,
    _pretrained_temporal_basis_matches_target,
    _reset_pretrained_futures_action_head_to_flat_,
    _transfer_pretrained_feature_identity,
    _transfer_pretrained_transformer_feature_projection,
    _validate_pretrained_epoch_zero_account_segment,
)


class _TinyFinancialStem(nn.Module):
    def __init__(
        self,
        *,
        num_features: int,
        feature_bottleneck_dim: int,
        causal_rms: bool,
    ) -> None:
        super().__init__()
        self.candle_encoder = CandleEncoder(
            num_features=num_features,
            d_model=4,
            dropout=0.0,
            norm_type="rmsnorm",
            ffn_type="gelu",
            sanitize_inputs=True,
            feature_bottleneck_dim=feature_bottleneck_dim,
            causal_feature_rms_normalization=causal_rms,
        )

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return self.candle_encoder(x)[0]


def test_epoch_zero_guard_rejects_finite_ruin_clamped_account() -> None:
    defaults = torch.tensor([False, True, False], dtype=torch.bool)
    reasons = torch.tensor([0, 1, 0], dtype=torch.int64)
    equity = torch.tensor([1.0, 0.0, 0.0], dtype=torch.float32)
    with pytest.raises(RuntimeError, match=r"defaulted.*row=1 reason=1"):
        _validate_pretrained_epoch_zero_account_segment(
            fold_id=7,
            row_start=0,
            row_end=3,
            defaults=defaults,
            default_reasons=reasons,
            equity_scale=equity,
        )


def test_epoch_zero_guard_accepts_only_alive_exact_account() -> None:
    assert _validate_pretrained_epoch_zero_account_segment(
        fold_id=3,
        row_start=1,
        row_end=3,
        defaults=torch.zeros(4, dtype=torch.bool),
        default_reasons=torch.zeros(4, dtype=torch.int64),
        equity_scale=torch.tensor([1.0, 0.9, 1.1, 1.2]),
    ) == (0, pytest.approx(1.1))


def test_rejected_pretrained_account_resets_only_trainable_action_head() -> None:
    model = _TinyTransformerFuturesStem(
        num_features=3,
        with_execution_residuals=True,
    )
    backbone_before = {
        name: value.detach().clone()
        for name, value in model.state_dict().items()
        if not name.startswith("futures_action_head.")
    }

    receipt = _reset_pretrained_futures_action_head_to_flat_(model)

    assert receipt["method"] == "zero_futures_action_head_flat_portfolio_v1"
    assert receipt["reset_parameter_count"] == 5
    assert receipt["trainable_parameter_count"] == 5
    assert torch.count_nonzero(model.futures_action_head.weight).item() == 0
    assert torch.count_nonzero(model.futures_action_head.bias).item() == 0
    for name, expected in backbone_before.items():
        assert torch.equal(model.state_dict()[name], expected), name

    features = torch.randn(4, 3)
    output = model.shared_output(features)
    assert torch.count_nonzero(output).item() == 0
    target = torch.linspace(-1.0, 1.0, steps=4).unsqueeze(-1)
    (output * target).sum().backward()
    assert model.futures_action_head.weight.grad is not None
    assert torch.count_nonzero(model.futures_action_head.weight.grad).item() > 0


def test_feature_name_adapter_preserves_source_output_with_causal_rms() -> None:
    torch.manual_seed(7)
    source = _TinyFinancialStem(
        num_features=2,
        feature_bottleneck_dim=0,
        causal_rms=False,
    )
    target = _TinyFinancialStem(
        num_features=4,
        feature_bottleneck_dim=2,
        causal_rms=True,
    )
    target.candle_encoder.set_causal_feature_rms_normalizer(
        torch.tensor([2.0, 3.0, 5.0, 7.0]),
        torch.ones(4, dtype=torch.bool),
    )
    source_features = ["old_a", "old_b"]
    target_features = ["new_x", "old_a", "new_y", "old_b"]
    initialization = _PretrainedInitialization(
        checkpoint_path=Path("source.pt"),
        checkpoint={"model_state_dict": source.state_dict()},
        source_feature_names=source_features,
        provenance={
            "source_checkpoint": "source.pt",
            "source_checkpoint_sha256": "unit-test",
        },
    )

    report = _transfer_pretrained_feature_identity(
        target,
        initialization,
        target_feature_names=target_features,
        require_exact_backbone=True,
        trainable_parameter_prefixes=(
            "candle_encoder.continuous_feature_bottleneck.",
        ),
    )

    expanded = torch.randn(3, 4)
    old_input = expanded[:, [1, 3]]
    source.eval()
    target.eval()
    with torch.inference_mode():
        expected = source(old_input)
        actual = target(expanded)
    assert torch.allclose(actual, expected, atol=1.0e-6, rtol=1.0e-6)

    adapter = target.candle_encoder.continuous_feature_bottleneck.weight
    assert torch.count_nonzero(adapter).item() == 2
    assert adapter[0, 1].item() == 3.0
    assert adapter[1, 3].item() == 7.0
    assert report["incompatible_source_tensor_count"] == 0
    assert report["causal_rms_identity_compensation"] is True
    assert all(
        parameter.requires_grad
        == name.startswith("candle_encoder.continuous_feature_bottleneck.")
        for name, parameter in target.named_parameters()
    )


def test_same_feature_abi_preserves_learned_bottleneck_checkpoint_exactly() -> None:
    torch.manual_seed(17)
    source = _TinyFinancialStem(
        num_features=4,
        feature_bottleneck_dim=2,
        causal_rms=True,
    )
    source.candle_encoder.set_causal_feature_rms_normalizer(
        torch.tensor([2.0, 3.0, 5.0, 7.0]),
        torch.ones(4, dtype=torch.bool),
    )
    target = _TinyFinancialStem(
        num_features=4,
        feature_bottleneck_dim=2,
        causal_rms=True,
    )
    target.candle_encoder.set_causal_feature_rms_normalizer(
        torch.ones(4),
        torch.ones(4, dtype=torch.bool),
    )
    feature_names = ["a", "b", "c", "d"]
    initialization = _PretrainedInitialization(
        checkpoint_path=Path("source.pt"),
        checkpoint={"model_state_dict": source.state_dict()},
        source_feature_names=feature_names,
        provenance={
            "source_checkpoint": "source.pt",
            "source_checkpoint_sha256": "unit-test",
        },
    )

    report = _transfer_pretrained_feature_identity(
        target,
        initialization,
        target_feature_names=feature_names,
        require_exact_backbone=True,
        trainable_parameter_prefixes=(
            "candle_encoder.continuous_feature_bottleneck.",
        ),
    )

    for key, source_value in source.state_dict().items():
        assert torch.equal(target.state_dict()[key], source_value), key
    inputs = torch.randn(3, 4)
    source.eval()
    target.eval()
    with torch.inference_mode():
        assert torch.equal(target(inputs), source(inputs))
    assert report["feature_adapter"] == "exact_state_by_feature_name"
    assert report["adapter_output_features"] == 2
    assert report["target_feature_count"] == 4
    assert report["incompatible_source_tensor_count"] == 0
    assert all(
        parameter.requires_grad
        == name.startswith("candle_encoder.continuous_feature_bottleneck.")
        for name, parameter in target.named_parameters()
    )


class _TinyTransformerFuturesStem(nn.Module):
    def __init__(self, *, num_features: int, with_execution_residuals: bool) -> None:
        super().__init__()
        self.feature_proj = nn.Linear(num_features, 4)
        self.shared_backbone = nn.Linear(4, 4)
        self.futures_action_head = nn.Linear(4, 1)
        if with_execution_residuals:
            self.futures_underlying_norm = nn.LayerNorm(4)
            self.futures_underlying_projection = nn.Linear(4, 4, bias=False)
            self.futures_underlying_gate = nn.Linear(8, 1)
            self.futures_denomination_encoder = nn.Sequential(
                nn.Linear(2, 4), nn.SiLU(), nn.LayerNorm(4)
            )
            self.futures_current_open_encoder = nn.Sequential(
                nn.Linear(1, 4), nn.SiLU(), nn.LayerNorm(4)
            )

    def shared_output(self, features: torch.Tensor) -> torch.Tensor:
        return self.futures_action_head(self.shared_backbone(self.feature_proj(features)))


class _TinyBasisProjection(nn.Module):
    def __init__(self, input_width: int) -> None:
        super().__init__()
        self.feature_projection = nn.Linear(input_width, 4)


class _TinyTransformerFuturesBasisStem(_TinyTransformerFuturesStem):
    def __init__(self, *, num_features: int, basis_input_width: int) -> None:
        super().__init__(
            num_features=num_features,
            with_execution_residuals=False,
        )
        self.temporal_basis_feature_encoder = _TinyBasisProjection(
            basis_input_width
        )


def test_transformer_projection_adapter_preserves_old_features_and_zeroes_new_paths() -> None:
    torch.manual_seed(19)
    source = _TinyTransformerFuturesStem(
        num_features=2,
        with_execution_residuals=False,
    )
    target = _TinyTransformerFuturesStem(
        num_features=3,
        with_execution_residuals=True,
    )
    initialization = _PretrainedInitialization(
        checkpoint_path=Path("source.pt"),
        checkpoint={"model_state_dict": source.state_dict()},
        source_feature_names=["old_a", "old_b"],
        provenance={
            "source_checkpoint": "source.pt",
            "source_checkpoint_sha256": "unit-test",
        },
    )

    report = _transfer_pretrained_transformer_feature_projection(
        target,
        initialization,
        target_feature_names=["new_x", "old_b", "old_a"],
        require_exact_backbone=True,
        trainable_parameter_prefixes=(
            "feature_proj.",
            "futures_underlying_",
            "futures_denomination_encoder.",
            "futures_current_open_encoder.",
            "futures_action_head.",
        ),
    )

    expanded = torch.randn(5, 3)
    old_input = expanded[:, [2, 1]]
    source.eval()
    target.eval()
    with torch.inference_mode():
        expected = source.shared_output(old_input)
        actual = target.shared_output(expanded)
    assert torch.allclose(actual, expected, atol=1.0e-6, rtol=1.0e-6)
    assert torch.count_nonzero(target.feature_proj.weight[:, 0]).item() == 0
    assert torch.count_nonzero(target.futures_underlying_projection.weight).item() == 0
    assert torch.count_nonzero(target.futures_denomination_encoder[0].weight).item() == 0
    assert torch.count_nonzero(target.futures_current_open_encoder[0].weight).item() == 0
    assert report["adapter_new_feature_columns_initialized_zero"] == 1
    assert report["incompatible_source_tensor_count"] == 0
    assert report["zero_initialized_residual_tensor_count"] == 5
    assert target.shared_backbone.weight.requires_grad is False
    assert target.futures_action_head.weight.requires_grad is True


def test_transformer_projection_adapter_fails_closed_on_symbol_axis_drift() -> None:
    source = _TinyTransformerFuturesStem(
        num_features=2,
        with_execution_residuals=False,
    )
    target = _TinyTransformerFuturesStem(
        num_features=3,
        with_execution_residuals=True,
    )
    initialization = _PretrainedInitialization(
        checkpoint_path=Path("source.pt"),
        checkpoint={"model_state_dict": source.state_dict()},
        source_feature_names=["old_a", "old_b"],
        provenance={
            "source_checkpoint": "source.pt",
            "source_checkpoint_sha256": "unit-test",
        },
        source_symbol_names=["2330", "2317"],
    )

    with pytest.raises(RuntimeError, match="symbol axes differ"):
        _transfer_pretrained_transformer_feature_projection(
            target,
            initialization,
            target_feature_names=["old_a", "old_b", "new_x"],
            target_symbol_names=["2317", "2330"],
            require_exact_backbone=True,
            trainable_parameter_prefixes=("feature_proj.",),
        )


def test_transformer_projection_adapter_zeroes_only_expanded_basis_residual() -> None:
    torch.manual_seed(29)
    source = _TinyTransformerFuturesBasisStem(
        num_features=2,
        basis_input_width=12,
    )
    target = _TinyTransformerFuturesBasisStem(
        num_features=3,
        basis_input_width=28,
    )
    initialization = _PretrainedInitialization(
        checkpoint_path=Path("source.pt"),
        checkpoint={"model_state_dict": source.state_dict()},
        source_feature_names=["old_a", "old_b"],
        provenance={
            "source_checkpoint": "source.pt",
            "source_checkpoint_sha256": "unit-test",
        },
    )

    report = _transfer_pretrained_transformer_feature_projection(
        target,
        initialization,
        target_feature_names=["old_a", "old_b", "new_x"],
        require_exact_backbone=False,
        trainable_parameter_prefixes=(
            "temporal_basis_feature_encoder.",
        ),
    )

    source_projection = source.temporal_basis_feature_encoder.feature_projection
    target_projection = target.temporal_basis_feature_encoder.feature_projection
    torch.testing.assert_close(
        target_projection.weight[:, :4],
        source_projection.weight[:, :4],
    )
    assert torch.count_nonzero(target_projection.weight[:, 4:]).item() == 0
    torch.testing.assert_close(target_projection.bias, source_projection.bias)
    assert target_projection.weight.requires_grad is True
    assert target.feature_proj.weight.requires_grad is False
    assert (
        "temporal_basis_feature_encoder.feature_projection.weight"
        in report["zero_initialized_residual_tensors"]
    )
    assert report["incompatible_source_tensor_count"] >= 1


def test_pretrained_basis_reuse_fails_closed_for_expanded_target_abi() -> None:
    config = load_config(
        "configs/markets/"
        "tw_stock_context_all_futures_carry_to_expiry_0845_integer_22_"
        "effective_rank_pretrained_guard_full_features_multi_basis_"
        "projection_l1_cash_capital10m.yaml"
    )
    initialization = _PretrainedInitialization(
        checkpoint_path=Path("source.pt"),
        checkpoint={
            "model_state_dict": {
                "temporal_basis_feature_encoder.feature_projection.weight": (
                    torch.zeros(32, 2336)
                )
            },
            "temporal_basis_selection": {
                "lookback": 32,
                "families": ["haar", "learned"],
                "selected_counts": {"haar": 4, "learned": 4},
            },
        },
        source_feature_names=["old_a", "old_b"],
        provenance={
            "source_checkpoint": "source.pt",
            "source_checkpoint_sha256": "unit-test",
        },
    )
    assert not _pretrained_temporal_basis_matches_target(
        initialization,
        config=config,
        target_feature_count=99,
    )
