from __future__ import annotations

from pathlib import Path

import pytest
import torch
from torch import nn

from stockagent.models.financial_transformer import CandleEncoder
from stockagent.training.trainer import (
    _PretrainedInitialization,
    _transfer_pretrained_feature_identity,
    _transfer_pretrained_transformer_feature_projection,
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
