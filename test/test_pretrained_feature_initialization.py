from __future__ import annotations

from pathlib import Path

import pytest
import torch
from torch import nn

from stockagent.config import load_config
from stockagent.models.financial_transformer import CandleEncoder
from stockagent.training.trainer import (
    _PretrainedEpochZeroUnderperformsFlatCash,
    _PretrainedInitialization,
    _pretrained_temporal_basis_matches_target,
    _reset_pretrained_exact_account_action_head_to_flat_,
    _reset_pretrained_futures_action_head_to_flat_,
    _temporary_pretrained_exact_account_flat_checkpoint,
    _transfer_pretrained_feature_identity,
    _transfer_pretrained_transformer_feature_projection,
    _validate_pretrained_epoch_zero_account_segment,
    _validate_pretrained_epoch_zero_improves_flat_cash,
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


def test_epoch_zero_guard_rejects_solvent_policy_worse_than_flat_cash() -> None:
    with pytest.raises(
        _PretrainedEpochZeroUnderperformsFlatCash,
        match=r"does not improve on flat cash",
    ) as rejected:
        _validate_pretrained_epoch_zero_improves_flat_cash(
            fold_id=2,
            validation_loss=0.14884938299655914,
            min_delta=1.0e-4,
        )
    assert rejected.value.fold_id == 2
    assert rejected.value.validation_loss == pytest.approx(0.14884938299655914)
    assert rejected.value.required_loss_below == pytest.approx(-1.0e-4)
    with pytest.raises(RuntimeError, match=r"does not improve on flat cash"):
        _validate_pretrained_epoch_zero_improves_flat_cash(
            fold_id=2,
            validation_loss=-1.0e-4,
            min_delta=1.0e-4,
        )


def test_epoch_zero_guard_accepts_policy_strictly_better_than_flat_cash() -> None:
    assert _validate_pretrained_epoch_zero_improves_flat_cash(
        fold_id=1,
        validation_loss=-0.018756341189146042,
        min_delta=1.0e-4,
    ) == pytest.approx(0.018756341189146042)


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


class _TinyProjectionL1StockPolicy(nn.Module):
    portfolio_output_mode = "projection_l1"

    def __init__(self) -> None:
        super().__init__()
        self.score_head = nn.Sequential(
            nn.Linear(3, 4),
            nn.GELU(),
            nn.Linear(4, 1),
        )

    def forward(self, features: torch.Tensor) -> torch.Tensor:
        scores = self.score_head(features).squeeze(-1)
        return scores - scores.mean()


def test_rejected_stock_policy_resets_only_final_scalar_score_layer() -> None:
    torch.manual_seed(19)
    model = _TinyProjectionL1StockPolicy()
    stem_before = {
        name: value.detach().clone()
        for name, value in model.state_dict().items()
        if not name.startswith("score_head.2.")
    }

    receipt = _reset_pretrained_exact_account_action_head_to_flat_(model)

    assert receipt["method"] == "zero_score_head_final_linear_flat_projection_l1_v1"
    assert receipt["reset_parameter_names"] == [
        "score_head.2.weight",
        "score_head.2.bias",
    ]
    assert torch.count_nonzero(model.score_head[-1].weight).item() == 0
    assert torch.count_nonzero(model.score_head[-1].bias).item() == 0
    for name, expected in stem_before.items():
        assert torch.equal(model.state_dict()[name], expected), name

    features = torch.randn(7, 3)
    output = model(features)
    assert torch.count_nonzero(output).item() == 0
    target = torch.linspace(-1.0, 1.0, steps=7)
    (output * target).sum().backward()
    assert model.score_head[-1].weight.grad is not None
    assert torch.count_nonzero(model.score_head[-1].weight.grad).item() > 0


def test_learned_cash_stock_policy_has_an_exact_flat_checkpoint_floor() -> None:
    model = _TinyProjectionL1StockPolicy()
    model.portfolio_output_mode = "learned_cash"

    receipt = _reset_pretrained_exact_account_action_head_to_flat_(model)

    assert receipt["schema_version"] == 2
    assert receipt["method"] == (
        "zero_score_head_final_linear_flat_learned_cash_v2"
    )
    assert receipt["portfolio_output_mode"] == "learned_cash"
    assert torch.count_nonzero(model.score_head[-1].weight).item() == 0
    assert torch.count_nonzero(model.score_head[-1].bias).item() == 0


def test_flat_stock_checkpoint_restores_transferred_training_initialization() -> None:
    torch.manual_seed(23)
    model = _TinyProjectionL1StockPolicy()
    original_state = {
        name: value.detach().clone()
        for name, value in model.state_dict().items()
    }
    features = torch.randn(7, 3)
    expected = model(features).detach().clone()
    assert torch.count_nonzero(expected).item() > 0

    with _temporary_pretrained_exact_account_flat_checkpoint(model) as receipt:
        assert receipt["checkpoint_only"] is True
        assert receipt["training_initialization_preserved"] is True
        assert receipt["restore_scope"] == "complete_model_state_dict"
        assert torch.count_nonzero(model(features)).item() == 0

    assert torch.equal(model(features), expected)
    for name, expected_value in original_state.items():
        assert torch.equal(model.state_dict()[name], expected_value), name


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


def test_non_strict_transfer_preserves_new_zero_initialized_cash_gate() -> None:
    source = _TinyFinancialStem(
        num_features=4,
        feature_bottleneck_dim=2,
        causal_rms=False,
    )
    target = _TinyFinancialStem(
        num_features=4,
        feature_bottleneck_dim=2,
        causal_rms=False,
    )
    target.learned_cash_score_head = nn.Linear(4, 1)
    nn.init.zeros_(target.learned_cash_score_head.weight)
    nn.init.zeros_(target.learned_cash_score_head.bias)
    feature_names = ["a", "b", "c", "d"]
    initialization = _PretrainedInitialization(
        checkpoint_path=Path("projection-source.pt"),
        checkpoint={"model_state_dict": source.state_dict()},
        source_feature_names=feature_names,
        provenance={"source_checkpoint_sha256": "unit-test"},
    )

    report = _transfer_pretrained_feature_identity(
        target,
        initialization,
        target_feature_names=feature_names,
        require_exact_backbone=False,
        trainable_parameter_prefixes=(),
    )

    assert torch.count_nonzero(target.learned_cash_score_head.weight).item() == 0
    assert torch.count_nonzero(target.learned_cash_score_head.bias).item() == 0
    assert target.learned_cash_score_head.weight.requires_grad
    assert target.learned_cash_score_head.bias.requires_grad
    assert "learned_cash_score_head.weight:missing" in report[
        "incompatible_source_tensors"
    ]
    assert "learned_cash_score_head.bias:missing" in report[
        "incompatible_source_tensors"
    ]


def test_financial_transfer_allows_explicit_position_free_symbol_superset() -> None:
    source = _TinyFinancialStem(
        num_features=4,
        feature_bottleneck_dim=2,
        causal_rms=False,
    )
    target = _TinyFinancialStem(
        num_features=4,
        feature_bottleneck_dim=2,
        causal_rms=False,
    )
    target.use_symbol_pos = False
    initialization = _PretrainedInitialization(
        checkpoint_path=Path("source.pt"),
        checkpoint={"model_state_dict": source.state_dict()},
        source_feature_names=["a", "b", "c", "d"],
        provenance={"source_checkpoint_sha256": "unit-test"},
        source_symbol_names=["2317", "2330"],
        source_uses_symbol_position=False,
    )

    report = _transfer_pretrained_feature_identity(
        target,
        initialization,
        target_feature_names=["a", "b", "c", "d"],
        target_symbol_names=["0050", "2317", "2330"],
        symbol_axis_adapter="permutation_invariant_superset",
        require_exact_backbone=True,
        trainable_parameter_prefixes=(),
    )

    assert report["symbol_axis_adapter"] == "permutation_invariant_superset"
    assert report["common_symbol_count"] == 2
    assert report["target_only_symbols"] == ["0050"]
    assert report["epoch_zero_revaluation_required_for_expanded_cross_section"]


@pytest.mark.parametrize("source_uses,target_uses", [(True, False), (None, False), (False, True)])
def test_symbol_superset_transfer_requires_both_position_free_proofs(
    source_uses: bool | None,
    target_uses: bool,
) -> None:
    source = _TinyFinancialStem(
        num_features=4,
        feature_bottleneck_dim=2,
        causal_rms=False,
    )
    target = _TinyFinancialStem(
        num_features=4,
        feature_bottleneck_dim=2,
        causal_rms=False,
    )
    target.use_symbol_pos = target_uses
    initialization = _PretrainedInitialization(
        checkpoint_path=Path("source.pt"),
        checkpoint={"model_state_dict": source.state_dict()},
        source_feature_names=["a", "b", "c", "d"],
        provenance={"source_checkpoint_sha256": "unit-test"},
        source_symbol_names=["2330"],
        source_uses_symbol_position=source_uses,
    )
    with pytest.raises(RuntimeError, match="symbol positions|use_symbol_pos"):
        _transfer_pretrained_feature_identity(
            target,
            initialization,
            target_feature_names=["a", "b", "c", "d"],
            target_symbol_names=["0050", "2330"],
            symbol_axis_adapter="permutation_invariant_superset",
            require_exact_backbone=True,
            trainable_parameter_prefixes=(),
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
