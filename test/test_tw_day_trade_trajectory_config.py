from __future__ import annotations

from copy import deepcopy
from dataclasses import asdict
from types import SimpleNamespace

import numpy as np
import torch

from stockagent.config import load_config
from stockagent.training.checkpoint_contract import (
    _trading_checkpoint_contract,
    _training_checkpoint_contract,
)
from stockagent.training.checkpoint_contract import (
    _active_model_config as _checkpoint_active_model_config,
    _checkpoint_model_values,
)
from stockagent.training.trainer import _requested_vs_executed_allocation_summary
from stockagent.training.loss import _canonical_target_weights_for_loss


FIXED_CONFIG = (
    "configs/deployments/"
    "tw_day_trade_last_last_only_training_vastai1t.yaml"
)
TRAJECTORY_V5_CONFIG = (
    "configs/deployments/"
    "tw_day_trade_last_last_only_training_vastai1t_v5.yaml"
)
LEARNED_CASH_V6_CONFIG = (
    "configs/deployments/"
    "tw_day_trade_last_last_only_training_vastai1t_v6.yaml"
)
SCORE_ENTMAX_V8_CONFIG = (
    "configs/deployments/"
    "tw_day_trade_last_last_only_training_vastai1t_v8_score_entmax_cash.yaml"
)
HISTORICAL_CONFIG = (
    "configs/deployments/"
    "tw_day_trade_last_last_only_training_vastai1t_v4.yaml"
)


def test_fixed_day_trade_recipe_is_self_contained_under_main_workspace() -> None:
    config = load_config(FIXED_CONFIG)

    assert str(config.runner.output_dir).startswith("/root/stockAgent/artifacts/")
    assert str(config.data.panel_cache_root).startswith("/root/stockAgent/artifacts/")
    assert str(config.data.day_trade_minute_execution_cache_dir).startswith(
        "/root/stockAgent/artifacts/"
    )
    assert "stockAgent-daytrade-training-20260910" not in str(config)
    assert config.environment.amp_dtype == "bf16"
    assert config.training.epochs == 1000
    assert config.training.batch_size_train == 32
    assert config.training.batch_size_eval == 16
    assert len(config.data.feature_include) == 99


def test_trajectory_v5_preserves_the_v4_model_and_transfer_abi() -> None:
    trajectory = load_config(TRAJECTORY_V5_CONFIG)
    historical = load_config(HISTORICAL_CONFIG)

    assert asdict(trajectory.training.financial_transformer) == asdict(
        historical.training.financial_transformer
    )
    assert trajectory.training.day_trade_optimizer_step_per_trajectory is True
    assert historical.training.day_trade_optimizer_step_per_trajectory is False


def test_fixed_day_trade_recipe_changes_only_the_policy_owned_output_contract() -> None:
    fixed = load_config(FIXED_CONFIG)
    trajectory = load_config(TRAJECTORY_V5_CONFIG)

    fixed_model = asdict(fixed.training.financial_transformer)
    trajectory_model = asdict(trajectory.training.financial_transformer)
    for changed in ("portfolio_output_mode", "center_long_short_logits"):
        fixed_model.pop(changed)
        trajectory_model.pop(changed)
    assert fixed_model == trajectory_model
    assert fixed.training.pretrained_initialization_root == (
        trajectory.training.pretrained_initialization_root
    )
    assert fixed.training.pretrained_initialization_require_exact_backbone == (
        trajectory.training.pretrained_initialization_require_exact_backbone
    )
    assert fixed.training.financial_transformer.portfolio_output_mode == (
        "learned_cash"
    )
    assert fixed.training.financial_transformer.center_long_short_logits is False
    # The exact loss/executor may clip only an accidental gross > 1 numerical
    # overshoot; it must not renormalize a deliberate sub-unit gross back to 1.
    assert fixed.trading.portfolio_activation == "pre_normalized"
    assert fixed.training.loss_portfolio_activation == "pre_normalized"
    assert fixed.training.loss_min_trade_weight is None
    assert trajectory.training.financial_transformer.portfolio_output_mode == (
        "projection_l1"
    )
    assert trajectory.training.financial_transformer.center_long_short_logits is True
    assert fixed.training.financial_transformer.temporal_pooling == "last"
    assert fixed.training.financial_transformer.temporal_query_mode == "last_only"
    assert fixed.training.financial_transformer.norm_type == "layernorm"
    assert fixed.training.financial_transformer.temporal_basis_input == "raw_features"


def test_fixed_day_trade_recipe_uses_one_optimizer_step_per_fifo_trajectory() -> None:
    fixed = load_config(FIXED_CONFIG)
    historical = load_config(HISTORICAL_CONFIG)

    assert fixed.training.day_trade_optimizer_step_per_trajectory is True
    assert fixed.training.lr_scheduler_warmup_steps == 32
    assert fixed.training.early_stopping_no_improve_ratio == 0.1
    assert (
        fixed.training.pretrained_initialization_require_improvement_over_flat_cash
        is True
    )
    assert historical.training.day_trade_optimizer_step_per_trajectory is False
    assert _training_checkpoint_contract(fixed)["optimizer"]["step_cadence"] == (
        "full_chronological_trajectory"
    )
    assert "step_cadence" not in _training_checkpoint_contract(historical)["optimizer"]


def test_fixed_day_trade_recipe_keeps_user_assumptions_and_old_basis_rank_map() -> None:
    fixed = load_config(FIXED_CONFIG)
    old_basis = load_config(
        "configs/markets/"
        "tw_day_trade_daily_multi_basis_22_effective_rank_projection_l1_"
        "tplus2_close_capital10m.yaml"
    )
    fixed_model = fixed.training.financial_transformer
    old_model = old_basis.training.financial_transformer

    assert fixed.training.lookback == 32
    assert fixed.training.tw_continuous_gradient_horizon_rows == 32
    assert fixed.training.epoch_test_curve is True
    assert fixed.training.curve_test_interval == 1
    assert fixed.training.early_stopping_no_improve_ratio == 0.1
    assert fixed.trading.volume_participation_equity == 10_000_000.0
    assert fixed.trading.max_volume_participation == 0.5
    assert fixed.trading.tw_commission_discount == 0.2
    assert fixed_model.temporal_basis_families == old_model.temporal_basis_families
    assert (
        fixed_model.temporal_basis_components_by_family
        == old_model.temporal_basis_components_by_family
    )
    assert (
        fixed_model.temporal_basis_novelty_threshold
        == old_model.temporal_basis_novelty_threshold
        == 1.0e-4
    )
    assert sum(fixed_model.temporal_basis_components_by_family.values()) == 524


def test_active_v7_changes_only_terminal_capacity_and_owns_a_fresh_contract() -> None:
    fixed = load_config(FIXED_CONFIG)
    learned_cash_v6 = load_config(LEARNED_CASH_V6_CONFIG)

    assert (
        fixed.trading.tw_day_trade_terminal_liquidation_unlimited_capacity
        is True
    )
    assert (
        learned_cash_v6.trading.tw_day_trade_terminal_liquidation_unlimited_capacity
        is False
    )
    assert fixed.runner.output_dir != learned_cash_v6.runner.output_dir
    assert "terminal_unlimited" in str(fixed.runner.output_dir)
    assert asdict(fixed.training.financial_transformer) == asdict(
        learned_cash_v6.training.financial_transformer
    )
    assert fixed.training.early_stopping_no_improve_ratio == 0.1
    assert learned_cash_v6.training.early_stopping_no_improve_ratio == 0.1
    fixed_contract = _trading_checkpoint_contract(fixed)
    v6_contract = _trading_checkpoint_contract(learned_cash_v6)
    assert fixed_contract != v6_contract
    assert fixed_contract["taiwan_execution"][
        "day_trade_terminal_liquidation_unlimited_capacity"
    ] is True
    assert (
        "day_trade_terminal_liquidation_unlimited_capacity"
        not in v6_contract["taiwan_execution"]
    )


def test_learned_cash_output_owns_a_fresh_checkpoint_and_artifact_contract() -> None:
    fixed = load_config(FIXED_CONFIG)
    trajectory = load_config(TRAJECTORY_V5_CONFIG)

    assert fixed.runner.output_dir != trajectory.runner.output_dir
    assert "learned_cash" in str(fixed.runner.output_dir)
    fixed_active = _checkpoint_active_model_config(fixed)
    trajectory_active = _checkpoint_active_model_config(trajectory)
    fixed_model_contract = _checkpoint_model_values(
        fixed,
        fixed_active,
        fixed.data.feature_include,
    )
    trajectory_model_contract = _checkpoint_model_values(
        trajectory,
        trajectory_active,
        trajectory.data.feature_include,
    )
    assert fixed_model_contract != trajectory_model_contract
    assert fixed_model_contract["portfolio_output_mode"] == "learned_cash"
    assert fixed_model_contract["portfolio_output_contract"] == (
        "contextual_cash_gate_signed_direction_v1"
    )
    assert trajectory_model_contract["portfolio_output_mode"] == "projection_l1"


def test_score_entmax_v8_removes_the_independent_cash_gate() -> None:
    from stockagent.models.factory import build_model
    from stockagent.models.normalization import masked_cash_entmax15_weights

    v7 = load_config(FIXED_CONFIG)
    v8 = load_config(SCORE_ENTMAX_V8_CONFIG)
    assert v8.runner.output_dir != v7.runner.output_dir
    assert v8.training.financial_transformer.portfolio_output_mode == "score_entmax_cash"
    assert v8.training.transformer_base_portfolio.portfolio_output_mode == "score_entmax_cash"
    assert v8.training.early_stopping_no_improve_ratio == 0.1
    assert _trading_checkpoint_contract(v8) == _trading_checkpoint_contract(v7)
    assert v8.training.pretrained_initialization_root == v7.training.pretrained_initialization_root
    assert v8.training.loss_portfolio_activation == "pre_normalized"
    assert v8.trading.portfolio_activation == "pre_normalized"
    assert v8.data.feature_include == v7.data.feature_include
    v7_model = asdict(v7.training.financial_transformer)
    v8_model = asdict(v8.training.financial_transformer)
    assert v7_model.pop("portfolio_output_mode") == "learned_cash"
    assert v8_model.pop("portfolio_output_mode") == "score_entmax_cash"
    assert v8_model == v7_model

    old_contract = _checkpoint_model_values(
        v7, _checkpoint_active_model_config(v7), v7.data.feature_include
    )
    new_contract = _checkpoint_model_values(
        v8, _checkpoint_active_model_config(v8), v8.data.feature_include
    )
    assert new_contract != old_contract
    assert "portfolio_output_contract" not in new_contract

    # Model construction needs a training-fold PCA/KLT override.  Omit basis
    # families only in this local head-ABI probe; the loaded recipe above is
    # unchanged and formal training still fits the full 22-family map.
    head_probe = deepcopy(v8)
    head_probe.training.financial_transformer.temporal_basis_families = []
    head_probe.training.financial_transformer.temporal_basis_components_by_family = {}
    model = build_model(
        config=head_probe,
        lookback=v8.training.lookback,
        num_features=len(v8.data.feature_include),
        num_symbols=16,
        feature_names=v8.data.feature_include,
    )
    assert model.cash_asset_token is None
    assert model.cash_asset_norm is None
    assert model.learned_cash_score_head is None
    assert not any("cash_asset" in name or "learned_cash_score_head" in name
                   for name in model.state_dict())
    model.eval()
    with torch.no_grad():
        actual_weights, _, actual_aux = model(
            torch.randn(2, v8.training.lookback, 16, len(v8.data.feature_include)),
            torch.ones(2, 16, dtype=torch.bool),
            return_aux=True,
        )
    assert actual_weights.shape == (2, 16)
    assert bool((actual_weights.abs().sum(dim=1) < 1.0).all())
    assert "cash_gate_risky_gross" not in actual_aux
    assert "cash_entmax_cash_fraction" in actual_aux
    with torch.no_grad(), torch.autocast("cpu", dtype=torch.bfloat16):
        amp_weights, _, _ = model(
            torch.randn(1, v8.training.lookback, 16, len(v8.data.feature_include)),
            torch.ones(1, 16, dtype=torch.bool),
            return_aux=True,
        )
    assert amp_weights.dtype == torch.float32

    scores = torch.tensor([[4.0, -2.0, 0.0, 0.0]], requires_grad=True)
    weights, parts = masked_cash_entmax15_weights(
        scores, torch.ones_like(scores, dtype=torch.bool), return_parts=True
    )
    gross = weights.abs().sum(dim=1)
    assert 0.0 < float(gross[0].detach()) < 1.0
    torch.testing.assert_close(parts["implicit_cash_weight"], 1.0 - gross)
    gross.sum().backward()
    assert scores.grad is not None
    assert bool(torch.isfinite(scores.grad).all())
    assert float(scores.grad.detach().abs().sum()) > 0.0
    flat = masked_cash_entmax15_weights(
        torch.zeros(1, 4), torch.ones(1, 4, dtype=torch.bool)
    )
    strong = masked_cash_entmax15_weights(
        torch.tensor([[100.0, 0.0, 0.0, 0.0]]),
        torch.ones(1, 4, dtype=torch.bool),
    )
    assert float(flat.abs().sum()) == 0.0
    assert float(strong.abs().sum()) > 0.98
    long_short = masked_cash_entmax15_weights(
        torch.tensor([[2.0, -2.0]]), torch.ones(1, 2, dtype=torch.bool)
    )
    assert float(long_short[0, 0]) > 0.0
    assert float(long_short[0, 1]) < 0.0
    assert float(long_short.abs().sum()) < 1.0

    bf16_scores = torch.tensor([[3.0, -1.0]], dtype=torch.bfloat16)
    bf16_mask = torch.ones_like(bf16_scores, dtype=torch.bool)
    legacy = masked_cash_entmax15_weights(bf16_scores, bf16_mask)
    exact = masked_cash_entmax15_weights(
        bf16_scores, bf16_mask, preserve_fp32_output=True
    )
    assert legacy.dtype == torch.bfloat16
    assert exact.dtype == torch.float32


def test_exact_loss_preserves_model_chosen_subunit_gross() -> None:
    fixed = load_config(FIXED_CONFIG)
    requested = torch.tensor(
        [[0.20, -0.10, 0.05], [0.0, 0.0, 0.0]],
        dtype=torch.float32,
        requires_grad=True,
    )

    canonical = _canonical_target_weights_for_loss(
        requested,
        long_only=fixed.trading.long_only,
        gross_leverage=1.0,
        min_trade_weight=0.0,
        portfolio_activation=fixed.training.loss_portfolio_activation,
    )

    torch.testing.assert_close(canonical, requested)
    torch.testing.assert_close(
        canonical.abs().sum(dim=1),
        torch.tensor([0.35, 0.0]),
    )
    canonical[0].sum().backward()
    assert requested.grad is not None
    assert torch.equal(requested.grad[0], torch.ones(3))


def test_allocation_audit_separates_requested_cash_from_execution_shortfall() -> None:
    result = SimpleNamespace(
        requested_weights_history=np.asarray(
            [[0.20, -0.10], [0.40, 0.10]], dtype=np.float32
        ),
        weights_history=np.asarray(
            [[0.05, -0.02], [0.20, 0.00]], dtype=np.float32
        ),
    )

    summary = _requested_vs_executed_allocation_summary(result)

    assert np.isclose(summary["model_requested_gross_mean"], 0.4)
    assert np.isclose(summary["model_requested_cash_mean"], 0.6)
    assert summary["model_requested_full_gross_fraction"] == 0.0
    assert np.isclose(summary["executor_realized_gross_mean"], 0.135)
