from __future__ import annotations

from pathlib import Path

import pytest
import torch

from stockagent.backtest.simulator import CANONICAL_BACKTEST_CONTRACT_VERSION
from stockagent.config import load_config
from stockagent.models.executable_portfolio_transformer import (
    ExecutablePortfolioTransformerModel,
)
from stockagent.models.factory import build_model, model_hidden_dim_hint
from stockagent.training.checkpoint_contract import _active_model_config
from stockagent.training.trainer import (
    _ExecutionRuntime,
    _execution_context_from_batch,
    _tw_settlement_compile_backend,
)


CONFIG_PATH = Path(
    "configs/markets/"
    "tw_day_trade_daily_executable_portfolio_transformer_22_basis_"
    "tplus2_close_capital10m.yaml"
)
BENCHMARK_CONFIG_PATH = Path(
    "configs/benchmarks/"
    "tw_day_trade_executable_portfolio_transformer_22_basis_throughput.yaml"
)
THROUGHPUT_CONFIG_PATH = Path(
    "configs/markets/"
    "tw_day_trade_daily_executable_portfolio_transformer_22_basis_"
    "tplus2_close_capital10m_v5_throughput.yaml"
)
SYMBOL_SHARDED_CONFIG_PATH = Path(
    "configs/markets/"
    "tw_day_trade_daily_executable_portfolio_transformer_22_basis_"
    "tplus2_close_capital10m_v6_symbol_sharded.yaml"
)


def _assert_epoch_observability_enabled(config: object) -> None:
    training = config.training
    assert training.record_epoch_curve is True
    assert training.curve_plot_interval == 1
    assert training.curve_test_interval == 1
    assert training.curve_plot_async is True
    assert training.epoch_test_curve is True
    assert training.defer_epoch_curve_plot_until_end is False


def _make_model(**overrides: object) -> ExecutablePortfolioTransformerModel:
    torch.manual_seed(71)
    params: dict[str, object] = {
        "lookback": 4,
        "num_features": 6,
        "num_symbols": 5,
        "d_model": 16,
        "attention_mode": "market_token",
        "use_flash_attention": True,
        "use_time_pos": False,
        "use_symbol_pos": True,
        "input_dropout": 0.0,
        "sdpa_batch_limit": 4096,
        "norm_type": "rmsnorm",
        "ffn_type": "swiglu",
        "qk_norm": False,
        "rope_temporal": True,
        "temporal_layers": 1,
        "temporal_heads": 2,
        "temporal_ffn_mult": 1,
        "temporal_pooling": "attention",
        "temporal_query_mode": "full_then_last",
        "cross_heads": 2,
        "cross_ffn_mult": 1,
        "latent_layers": 1,
        "num_latent_factors": 2,
        "num_market_tokens": 2,
        "market_layers": 1,
        "head_hidden_dim": 16,
        "head_layers": 1,
        "dropout": 0.0,
        "portfolio_mode": "long_short",
        "portfolio_output_mode": "projection_l1",
        "center_long_short_logits": False,
        "projection_l1_scale_by_active_count": True,
        "return_aux": False,
        "return_aux_details": False,
        "runtime_shape_check": True,
        "allow_dynamic_symbols": True,
        "candle_dropout": 0.0,
        "execution_context_hidden_dim": 8,
        "max_volume_participation": 0.5,
        "volume_participation_equity": 10_000_000.0,
        "short_capacity_limit_enabled": True,
        "execution_mode": "tw_day_trade",
    }
    params.update(overrides)
    return ExecutablePortfolioTransformerModel(**params)


def _raw_context(rows: int = 3, symbols: int = 5) -> torch.Tensor:
    context = torch.ones(rows, symbols, 6)
    context[..., 4] = 2_000_000.0
    context[..., 5] = 1_000_000.0
    return context


def test_formal_config_keeps_rule_only_1000_epoch_contract() -> None:
    config = load_config(CONFIG_PATH)
    model_cfg = config.training.executable_portfolio_transformer

    assert config.training.model_name == "executable_portfolio_transformer"
    assert config.training.epochs == 1000
    assert config.training.batch_size_train == 32
    assert config.training.batch_size_eval == 16
    assert config.training.auto_batch_size is False
    assert config.training.loss_type == "log_utility"
    assert model_cfg.portfolio_output_mode == "projection_l1"
    assert model_cfg.center_long_short_logits is False
    assert model_cfg.projection_l1_scale_by_active_count is True
    assert model_cfg.temporal_basis_algebraic_contraction is True
    assert model_cfg.use_execution_context_features is True
    assert model_cfg.execution_context_schema_version == 2
    assert len(model_cfg.temporal_basis_families) == 22
    assert sum(model_cfg.temporal_basis_components_by_family.values()) == 524
    assert config.trading.min_trade_weight == 0.0
    assert config.trading.max_turnover_ratio == 0.0
    assert config.trading.tw_day_trade_unlimited_margin_conversion is True
    assert config.trading.tw_short_capacity_limit_enabled is False
    assert "causal_stateful_carry" in str(config.runner.output_dir)
    assert str(config.runner.output_dir).endswith("capital10m_v4")
    assert CANONICAL_BACKTEST_CONTRACT_VERSION == 21
    _assert_epoch_observability_enabled(config)
    assert all(
        getattr(config.training.multitask_loss, name) == 0.0
        for name in (
            "rank_ic_weight",
            "return_rank_ic_weight",
            "direction_weight",
            "volatility_regime_weight",
            "concentration_weight",
            "net_exposure_weight",
        )
    )
    assert "executable_portfolio_transformer" in str(config.runner.output_dir)
    active = _active_model_config(config)
    assert active["config_name"] == "executable_portfolio_transformer"
    assert "use_execution_context_features" not in active["values"]
    assert active["values"]["temporal_basis_algebraic_contraction"] is True

    historical = load_config("configs/experiment_baseline.yaml")
    assert (
        "temporal_basis_algebraic_contraction"
        not in _active_model_config(historical)["values"]
    )


def test_v5_throughput_config_uses_fresh_batch128_root_and_keeps_plots() -> None:
    config = load_config(THROUGHPUT_CONFIG_PATH)

    assert config.training.model_name == "executable_portfolio_transformer"
    assert config.training.epochs == 1000
    assert config.training.batch_size_train == 128
    assert config.training.batch_size_eval == 16
    assert config.training.auto_batch_size is False
    assert config.runner.resume is True
    assert str(config.runner.output_dir).endswith("v5_batch128")
    assert config.trading.tw_day_trade_unlimited_margin_conversion is True
    assert config.trading.tw_short_capacity_limit_enabled is False
    _assert_epoch_observability_enabled(config)


def test_v6_symbol_sharded_config_uses_power_of_two_batches_and_keeps_plots() -> None:
    config = load_config(SYMBOL_SHARDED_CONFIG_PATH)

    assert config.training.model_name == "executable_portfolio_transformer"
    assert config.training.distributed_symbol_sharded_ledger is True
    assert config.training.distributed_symbol_sharded_pack_metadata is True
    assert config.training.distributed_symbol_sharded_pack_scalars is True
    assert config.training.distributed_symbol_sharded_skip_noop_collectives is True
    assert config.training.batch_size_train == 128
    assert config.training.batch_size_eval == 16
    assert config.training.batch_size_train.bit_count() == 1
    assert config.training.batch_size_eval.bit_count() == 1
    assert config.training.auto_batch_size is False
    assert config.runner.resume is False
    assert str(config.runner.output_dir).endswith("v6_symbol_sharded")
    assert config.trading.tw_day_trade_unlimited_margin_conversion is True
    assert config.trading.tw_short_capacity_limit_enabled is False
    _assert_epoch_observability_enabled(config)


def test_throughput_benchmark_keeps_epoch_observability_enabled() -> None:
    config = load_config(BENCHMARK_CONFIG_PATH)

    _assert_epoch_observability_enabled(config)


def test_stateful_day_trade_preflight_monitors_dual_session_executor() -> None:
    runtime = _ExecutionRuntime(
        mode="tw_day_trade",
        buy_fee_rates=None,
        sell_fee_rates=None,
        lot_sizes=None,
        settlement_lag_sessions=2,
        day_trade_unlimited_margin_conversion=True,
    )

    assert (
        _tw_settlement_compile_backend(
            runtime,
            day_trade_minute_compile=False,
        )
        == "dual_session"
    )


def test_factory_builds_independent_executable_model() -> None:
    config = load_config(CONFIG_PATH)
    model_cfg = config.training.executable_portfolio_transformer
    model_cfg.temporal_basis_families = []
    model_cfg.temporal_basis_components_by_family = {}
    model_cfg.d_model = 16
    model_cfg.temporal_layers = 1
    model_cfg.temporal_heads = 2
    model_cfg.cross_heads = 2
    model_cfg.num_market_tokens = 2
    model_cfg.market_layers = 1
    model_cfg.head_hidden_dim = 16

    model = build_model(
        config=config,
        lookback=4,
        num_features=6,
        num_symbols=5,
        feature_names=[f"f{idx}" for idx in range(6)],
    )

    assert isinstance(model, ExecutablePortfolioTransformerModel)
    assert model_hidden_dim_hint(config) == 16


def test_forward_requires_context_and_panel_paths_are_equivalent() -> None:
    model = _make_model().eval()
    feature_slab = torch.randn(6, 5, 6)
    date_indices = torch.arange(3, 6, dtype=torch.long)
    windows = feature_slab.unfold(0, 4, 1).permute(0, 3, 1, 2).contiguous()
    mask = torch.ones(3, 5, dtype=torch.bool)
    context = {"execution_context": _raw_context()}

    with pytest.raises(ValueError, match="requires causal execution_context"):
        model(windows, mask)

    with torch.no_grad():
        materialized = model(windows, mask, portfolio_context=context)
        panel = model.forward_from_panel(
            feature_slab,
            date_indices,
            mask,
            portfolio_context=context,
        )
        slab = model.forward_from_panel_slab(
            feature_slab,
            mask,
            portfolio_context=context,
        )

    torch.testing.assert_close(panel, materialized, rtol=1e-5, atol=1e-6)
    torch.testing.assert_close(slab, materialized, rtol=1e-5, atol=1e-6)
    assert bool((materialized.abs().sum(dim=1) <= 1.0 + 1.0e-6).all())


def test_rule_gate_never_redistributes_blocked_weight() -> None:
    requested = torch.tensor([[0.4, -0.3, 0.2, -0.1]])
    long_allowed = torch.tensor([[True, True, False, True]])
    short_allowed = torch.tensor([[True, False, True, True]])

    executed_request = ExecutablePortfolioTransformerModel._apply_direction_permissions(
        requested,
        long_allowed,
        short_allowed,
    )

    torch.testing.assert_close(
        executed_request,
        torch.tensor([[0.4, 0.0, 0.0, -0.1]]),
    )
    assert executed_request.abs().sum() < requested.abs().sum()


def test_rule_gate_keeps_boundary_gradient_only_when_a_direction_exists() -> None:
    requested = torch.tensor([[-0.4, 0.3]], requires_grad=True)
    long_allowed = torch.tensor([[True, False]])
    short_allowed = torch.tensor([[False, False]])

    executable = ExecutablePortfolioTransformerModel._apply_direction_permissions(
        requested,
        long_allowed,
        short_allowed,
    )
    executable.sum().backward()

    torch.testing.assert_close(executable, torch.zeros_like(executable))
    torch.testing.assert_close(requested.grad, torch.tensor([[1.0, 0.0]]))


def test_context_feature_ablation_keeps_causal_hard_direction_gate() -> None:
    model = _make_model(use_execution_context_features=False).eval()
    assert model.execution_context_encoder is None
    assert model.execution_context_gate is None

    x = torch.randn(2, 4, 5, 6)
    mask = torch.ones(2, 5, dtype=torch.bool)
    context = _raw_context(rows=2)
    context[:, 0, 1] = 0.0  # Completely ineligible.
    context[:, 1, 0] = 0.0  # Long direction only.
    context[:, 2, 2] = 0.0  # Short direction only.

    with pytest.raises(ValueError, match="requires causal execution_context"):
        model(x, mask)
    with torch.no_grad():
        weights = model(
            x,
            mask,
            portfolio_context={"execution_context": context},
        )

    torch.testing.assert_close(weights[:, 0], torch.zeros_like(weights[:, 0]))
    assert bool((weights[:, 1] >= 0.0).all())
    assert bool((weights[:, 2] <= 0.0).all())


def test_explainability_binding_repeats_fixed_context_without_changing_policy() -> None:
    model = _make_model().eval()
    x = torch.randn(2, 4, 5, 6)
    mask = torch.ones(2, 5, dtype=torch.bool)
    context = _raw_context(rows=2)
    repeated_x = x.repeat(3, 1, 1, 1)
    repeated_mask = mask.repeat(3, 1)
    repeated_context = context.repeat(3, 1, 1)

    model.bind_execution_context_for_explainability(context)
    with torch.no_grad():
        bound = model(repeated_x, repeated_mask)
        explicit = model(
            repeated_x,
            repeated_mask,
            portfolio_context={"execution_context": repeated_context},
        )
    model.clear_execution_context_for_explainability()

    torch.testing.assert_close(bound, explicit, rtol=1e-5, atol=1e-6)
    with pytest.raises(ValueError, match="requires causal execution_context"):
        model(x, mask)


def test_trainer_builds_fixed_context_schema_and_zero_pads() -> None:
    rows, symbols = 2, 3
    batch = {
        "tradable_mask": torch.ones(rows, symbols, dtype=torch.bool),
        "can_buy_mask": torch.ones(rows, symbols, dtype=torch.bool),
        "can_sell_mask": torch.ones(rows, symbols, dtype=torch.bool),
        "can_short_open_mask": torch.ones(rows, symbols, dtype=torch.bool),
        "can_short_open_open_mask": torch.ones(
            rows, symbols, dtype=torch.bool
        ),
        "day_trade_eligible_mask": torch.ones(rows, symbols, dtype=torch.bool),
        "day_trade_can_buy_open_mask": torch.ones(
            rows, symbols, dtype=torch.bool
        ),
        "day_trade_can_sell_open_mask": torch.ones(
            rows, symbols, dtype=torch.bool
        ),
        "volume_notional": torch.full((rows, symbols), 123.0),
        "short_capacity_notional": torch.full((rows, symbols), 456.0),
    }

    context = _execution_context_from_batch(batch, target_rows=4)

    assert context.shape == (4, symbols, 6)
    torch.testing.assert_close(context[:rows, :, 4], batch["volume_notional"])
    torch.testing.assert_close(
        context[:rows, :, 5], batch["short_capacity_notional"]
    )
    assert torch.count_nonzero(context[rows:]) == 0


def test_close_outcomes_cannot_enter_policy_context() -> None:
    rows, symbols = 2, 3
    base = {
        "tradable_mask": torch.ones(rows, symbols, dtype=torch.bool),
        "can_buy_mask": torch.zeros(rows, symbols, dtype=torch.bool),
        "can_sell_mask": torch.zeros(rows, symbols, dtype=torch.bool),
        "can_short_open_mask": torch.zeros(rows, symbols, dtype=torch.bool),
        "can_short_open_open_mask": torch.ones(
            rows, symbols, dtype=torch.bool
        ),
        "day_trade_eligible_mask": torch.ones(rows, symbols, dtype=torch.bool),
        "day_trade_can_buy_open_mask": torch.ones(
            rows, symbols, dtype=torch.bool
        ),
        "day_trade_can_sell_open_mask": torch.ones(
            rows, symbols, dtype=torch.bool
        ),
        "volume_notional": torch.full((rows, symbols), 123.0),
        "short_capacity_notional": torch.full((rows, symbols), 456.0),
    }
    perturbed = dict(base)
    perturbed["can_buy_mask"] = torch.ones(rows, symbols, dtype=torch.bool)
    perturbed["can_sell_mask"] = torch.ones(rows, symbols, dtype=torch.bool)
    perturbed["can_short_open_mask"] = torch.ones(
        rows, symbols, dtype=torch.bool
    )

    torch.testing.assert_close(
        _execution_context_from_batch(base),
        _execution_context_from_batch(perturbed),
    )
