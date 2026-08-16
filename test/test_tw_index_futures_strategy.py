from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest
import torch
from torch.utils.data import default_collate

from stockagent.config import DAY_TRADE_OPEN_GAP_FEATURE, load_config
from stockagent.backtest.simulator import BacktestResult
from stockagent.backtest.tw_index_futures import (
    build_tw_index_futures_day_execution_tensor,
)
from stockagent.data.panel import PanelData
from stockagent.data.tw_index_futures import (
    TAIFEX_INDEX_FUTURES_ACTION_COUNT,
    TaiwanIndexFuturesDaySession,
    build_causal_taifex_futures_model_context,
)
from stockagent.models.cross_sectional_index_futures import (
    CrossSectionalIndexFuturesModel,
)
from stockagent.models.factory import build_model
from stockagent.training.dataset import CrossSectionalDataset
from stockagent.training.loss import risk_aware_loss
from stockagent.training.trainer import (
    _format_fold_evaluation_lines,
    _reporting_weight_table_payload,
)


def _panel() -> PanelData:
    rows = 8
    symbols = 2
    alive = np.ones((rows, symbols), dtype=bool)
    alive[6] = False
    panel = PanelData(
        dates=np.arange(
            np.datetime64("2025-01-01"),
            np.datetime64("2025-01-09"),
            dtype="datetime64[D]",
        ),
        symbols=["A", "B"],
        feature_names=["feature"],
        features=np.arange(rows * symbols, dtype=np.float32).reshape(
            rows, symbols, 1
        ),
        returns_1d=np.zeros((rows, symbols), dtype=np.float32),
        tradable_mask=np.ones((rows, symbols), dtype=bool),
        alive_mask=alive,
        benchmark_returns=np.zeros(rows, dtype=np.float32),
        close_prices=np.ones((rows, symbols), dtype=np.float32),
        can_buy_mask=np.ones((rows, symbols), dtype=bool),
        can_sell_mask=np.ones((rows, symbols), dtype=bool),
    )
    tradable = np.ones((rows, 3), dtype=bool)
    tradable[6, 0] = False
    prices = np.full((rows, 3), 20000.0)
    log_returns = np.arange(rows * 3, dtype=np.float64).reshape(rows, 3) / 10000
    rolling_returns = log_returns + 0.01
    tenor_prices = np.repeat(prices[:, None, :], 6, axis=1)
    tenor_returns = np.repeat(log_returns[:, None, :], 6, axis=1)
    tenor_tradable = np.repeat(tradable[:, None, :], 6, axis=1)
    panel.index_futures_day_session = TaiwanIndexFuturesDaySession(
        dates=panel.dates,
        products=("TX", "MTX", "TMF"),
        contract_months=np.full((rows, 3), "202501", dtype="U16"),
        open_prices=prices,
        high_prices=prices,
        low_prices=prices,
        close_prices=prices,
        volumes=np.ones((rows, 3), dtype=np.int64),
        log_returns=log_returns,
        tradable_mask=tradable,
        multipliers=np.asarray([200, 50, 10], dtype=np.int64),
        rolling_buy_hold_log_returns=rolling_returns,
        rolling_buy_hold_tradable_mask=np.ones((rows, 3), dtype=bool),
        front_month_roll_mask=np.zeros((rows, 3), dtype=bool),
        tenor_contract_months=np.full((rows, 6), "202501", dtype="U6"),
        tenor_open_prices=tenor_prices,
        tenor_high_prices=tenor_prices,
        tenor_low_prices=tenor_prices,
        tenor_close_prices=tenor_prices * np.exp(tenor_returns),
        tenor_volumes=np.ones((rows, 6, 3), dtype=np.int64),
        tenor_log_returns=tenor_returns,
        tenor_tradable_mask=tenor_tradable,
    )
    panel.index_futures_reference_product = "TX"
    (
        panel.index_futures_candidate_features,
        panel.index_futures_candidate_mask,
    ) = build_causal_taifex_futures_model_context(
        panel.index_futures_day_session
    )
    panel.index_futures_execution_returns = (
        build_tw_index_futures_day_execution_tensor(
            panel.index_futures_day_session
        )
    )
    return panel


def test_canonical_dataset_ends_features_at_previous_stock_session() -> None:
    panel = _panel()
    dataset = CrossSectionalDataset(
        panel,
        np.arange(panel.num_dates),
        lookback=3,
        lookback_context="panel_history",
        execution_mode="tw_index_futures_day",
        include_volume_notional=False,
    )

    # Row 6 lacks TX execution and row 7 has no stock alive at t-1.
    np.testing.assert_array_equal(dataset.valid_indices, [3, 4, 5])
    sample = dataset[0]
    np.testing.assert_array_equal(sample["x"].numpy(), panel.features[0:3])
    expected_return = panel.index_futures_day_session.log_returns[3, 0]
    np.testing.assert_allclose(
        sample["future_log_returns"].numpy(), expected_return
    )
    np.testing.assert_array_equal(
        sample["tradable_mask"].numpy(), panel.alive_mask[2]
    )
    assert sample["benchmark"].item() == np.float32(
        panel.index_futures_day_session.rolling_buy_hold_log_returns[3, 0]
    )


def test_dataset_allows_more_futures_input_tokens_than_action_slots() -> None:
    panel = _panel()
    extra_tokens = 13
    panel.index_futures_candidate_features = np.concatenate(
        (
            np.zeros(
                (panel.num_dates, extra_tokens, 13),
                dtype=np.float32,
            ),
            panel.index_futures_candidate_features,
        ),
        axis=1,
    )
    panel.index_futures_candidate_mask = np.concatenate(
        (
            np.ones((panel.num_dates, extra_tokens), dtype=bool),
            panel.index_futures_candidate_mask,
        ),
        axis=1,
    )

    dataset = CrossSectionalDataset(
        panel,
        np.arange(panel.num_dates),
        lookback=3,
        lookback_context="panel_history",
        execution_mode="tw_index_futures_day",
        include_volume_notional=False,
    )

    assert dataset.derivative_candidate_features_t.shape == (
        panel.num_dates,
        31,
        13,
    )
    assert dataset.overnight_log_returns_t.shape == (panel.num_dates, 18, 3)


def test_joint_18_action_model_and_exact_forward_loss_share_one_contract() -> None:
    dataset = CrossSectionalDataset(
        _panel(),
        np.arange(8),
        lookback=3,
        lookback_context="panel_history",
        execution_mode="tw_index_futures_day",
        include_volume_notional=False,
    )
    batch = default_collate([dataset[index] for index in range(len(dataset))])
    model = CrossSectionalIndexFuturesModel(
        lookback=3,
        num_features=1,
        num_symbols=2,
        d_model=8,
        attention_mode="market_token",
        use_latent_factors=False,
        use_market_tokens=True,
        temporal_layers=1,
        temporal_heads=2,
        temporal_ffn_mult=2,
        temporal_pooling="last",
        temporal_query_mode="last_only",
        cross_layers=1,
        cross_heads=2,
        cross_ffn_mult=2,
        joint_layers=1,
        joint_heads=2,
        joint_ffn_mult=2,
        latent_layers=1,
        num_latent_factors=2,
        num_market_tokens=2,
        market_layers=1,
        head_hidden_dim=8,
        head_layers=1,
        dropout=0.0,
        use_flash_attention=False,
    )
    actions = model(
        batch["x"],
        batch["tradable_mask"],
        portfolio_context={
            "candidate_features": batch["derivative_candidate_features"],
            "candidate_mask": batch["derivative_candidate_mask"],
        },
    )
    loss = risk_aware_loss(
        actions,
        batch["future_log_returns"],
        batch["tradable_mask"],
        benchmark_returns=batch["benchmark"],
        overnight_log_returns=batch["overnight_log_returns"],
        state_advance_mask=batch["session_advance_mask"],
        long_only=False,
        execution_mode="tw_index_futures_day",
        objective="log_utility",
        concentration_weight=0.0,
        gamma_turnover=0.0,
        day_trade_execution_initial_capital=100_000_000.0,
    )

    assert actions.shape == (len(dataset), 18)
    assert torch.isfinite(loss)
    loss.backward()
    assert any(
        parameter.grad is not None and torch.isfinite(parameter.grad).all()
        for parameter in model.parameters()
    )


def test_config_inherits_canonical_candles_baseline() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    baseline = load_config(
        repo_root / "configs/markets/tw_public_lanten_market_candles.yaml"
    )
    config = load_config(repo_root / "configs/markets/tw_index_futures_day.yaml")

    assert config.trading.execution_mode == "tw_index_futures_day"
    assert config.training.model_name == "cross_sectional_index_futures"
    assert config.data.day_trade_open_feature is False
    assert DAY_TRADE_OPEN_GAP_FEATURE not in config.data.feature_include
    assert config.training.transformer_base_portfolio.temporal_pooling == "last"
    assert (
        config.training.transformer_base_portfolio.temporal_query_mode
        == "last_only"
    )
    assert config.training.transformer_base_portfolio.sdpa_batch_limit == 16384
    assert config.training.batch_size_train == baseline.training.batch_size_train
    assert config.training.batch_size_eval == baseline.training.batch_size_eval
    assert config.training.epochs == baseline.training.epochs
    assert config.training.lookback == baseline.training.lookback
    assert config.training.learning_rate == baseline.training.learning_rate
    assert config.training.weight_decay == baseline.training.weight_decay
    assert config.trading.tw_index_futures_total_fee_per_side_twd == [
        60.0,
        24.0,
        16.0,
    ]
    assert config.trading.tw_index_futures_sell_transaction_tax_rate == pytest.approx(
        0.00002
    )
    assert config.trading.tw_index_futures_initial_capital == 100_000_000.0
    assert config.trading.tw_index_futures_data_path.endswith(
        "day_session_contracts.parquet"
    )

    model = build_model(
        config=config,
        lookback=config.training.lookback,
        num_features=1,
        num_symbols=2,
        feature_names=["feature"],
    )
    assert isinstance(model, CrossSectionalIndexFuturesModel)
    assert model.temporal_pooling == "last"
    assert model.temporal_query_mode == "last_only"


def test_futures_console_distinguishes_exact_and_surrogate_metrics() -> None:
    surrogate = {
        "cagr": 0.1,
        "cumulative_return": 0.2,
        "excess_return_vs_benchmark": -0.3,
    }
    exact = {
        "cagr": 0.05,
        "cumulative_return": 0.08,
        "excess_return_vs_benchmark": -0.42,
    }

    lines = _format_fold_evaluation_lines(
        execution_mode="tw_index_futures_day",
        loss_objective="log_utility",
        val_ic={},
        val_metrics=surrogate,
        test_ic={},
        test_metrics=surrogate,
        test_integer_metrics=exact,
    )

    assert [line.split("]", 1)[0] + "]" for line in lines] == [
        "  [val surrogate]",
        "  [test exact]",
        "  [test surrogate]",
    ]
    assert all("cagr=" in line for line in lines)
    assert all("log_utility=" not in line for line in lines)
    assert "cum_ret=+0.0800" in lines[1]
    assert "cum_ret=+0.2000" in lines[2]


def test_dual_5090_config_preserves_strategy_and_uses_power_of_two_batch() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    canonical = load_config(
        repo_root / "configs/markets/tw_index_futures_day.yaml"
    )
    tuned = load_config(
        repo_root / "configs/markets/tw_index_futures_day_dual_5090.yaml"
    )

    assert tuned.trading.execution_mode == canonical.trading.execution_mode
    assert tuned.training.model_name == canonical.training.model_name
    assert tuned.training.loss_type == canonical.training.loss_type
    assert tuned.training.lookback == canonical.training.lookback
    assert tuned.training.multi_gpu_strategy == "distributed_data_parallel"
    assert tuned.training.batch_size_train == 512
    assert (tuned.training.batch_size_train // 2) == 256
    model = tuned.training.transformer_base_portfolio
    hardware_shape_values = {
        "batch_size_train": tuned.training.batch_size_train,
        "batch_size_eval": tuned.training.batch_size_eval,
        "lookback": tuned.training.lookback,
        "eval_model_chunk_rows": tuned.training.eval_model_chunk_rows,
        "eval_backtest_chunk_rows": tuned.training.eval_backtest_chunk_rows,
        "eval_auto_chunk_rows_cap": tuned.training.eval_auto_chunk_rows_cap,
        "tw_continuous_compile_chunk_rows": (
            tuned.training.tw_continuous_compile_chunk_rows
        ),
        "tw_continuous_gradient_horizon_rows": (
            tuned.training.tw_continuous_gradient_horizon_rows
        ),
        "ddp_bucket_cap_mb": tuned.training.ddp_bucket_cap_mb,
        "d_model": model.d_model,
        "sdpa_batch_limit": model.sdpa_batch_limit,
        "temporal_layers": model.temporal_layers,
        "temporal_heads": model.temporal_heads,
        "temporal_ffn_mult": model.temporal_ffn_mult,
        "cross_layers": model.cross_layers,
        "cross_heads": model.cross_heads,
        "cross_ffn_mult": model.cross_ffn_mult,
        "joint_layers": model.joint_layers,
        "joint_heads": model.joint_heads,
        "joint_ffn_mult": model.joint_ffn_mult,
        "num_latent_factors": model.num_latent_factors,
        "num_market_tokens": model.num_market_tokens,
        "head_hidden_dim": model.head_hidden_dim,
    }
    for name, value in hardware_shape_values.items():
        assert isinstance(value, int), name
        assert value > 0 and value & (value - 1) == 0, (name, value)
    # These are semantic budgets/cardinalities, not tensor tiling knobs.
    assert tuned.training.epochs == 1000
    assert TAIFEX_INDEX_FUTURES_ACTION_COUNT == 18
    assert tuned.runner.isolate_train_folds is True
    assert tuned.runner.output_dir == (
        "artifacts/markets/"
        "tw_index_futures_day_joint_18_dual_5090_100m_exact_power2_v8"
    )


def test_v11_uses_non_saturating_power_of_two_policy_contract() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    config = load_config(
        repo_root
        / "configs/markets/tw_index_futures_day_directional_night_dual_5090_v11.yaml"
    )
    model = config.training.transformer_base_portfolio

    assert config.training.epochs == 1000
    assert config.training.batch_size_train == 512
    assert config.training.val_interval_epochs == 8
    assert config.training.curve_test_interval == 8
    assert config.training.learning_rate == pytest.approx(2.0**-13)
    assert config.training.weight_decay == pytest.approx(2.0**-5)
    assert config.training.lr_scheduler_warmup_steps == 64
    assert model.d_model == 16
    assert model.use_latent_factors is False
    assert model.use_market_tokens is True
    assert model.futures_action_mode == "directional_allocation"
    assert model.futures_exposure_activation == "softsign"
    assert model.futures_allocation_logit_scale == 2.0
    assert model.futures_allocation_temperature == 2.0
    assert model.futures_require_joint_context is True
    assert config.runner.output_dir.endswith("power2_v11")


def test_futures_daily_weight_table_uses_direct_action_axis() -> None:
    requested = np.arange(36, dtype=np.float32).reshape(2, 18) / 100.0
    result = BacktestResult(
        strategy_returns=np.zeros(2, dtype=np.float32),
        benchmark_returns=np.zeros(2, dtype=np.float32),
        turnovers=np.zeros(2, dtype=np.float32),
        weights_history=np.zeros((2, 3), dtype=np.float32),
        execution_mode="tw_index_futures_day",
        requested_weights_history=requested,
    )

    symbols, weights = _reporting_weight_table_payload(
        result,
        ["stock_a", "stock_b", "stock_c"],
    )

    assert symbols == [
        *(f"TX_E{tenor}" for tenor in range(1, 7)),
        *(f"MTX_E{tenor}" for tenor in range(1, 7)),
        *(f"TMF_E{tenor}" for tenor in range(1, 7)),
    ]
    np.testing.assert_array_equal(weights, requested)
