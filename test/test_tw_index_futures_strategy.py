from __future__ import annotations

from pathlib import Path

import numpy as np

from stockagent.config import DAY_TRADE_OPEN_GAP_FEATURE, load_config
from stockagent.data.panel import PanelData
from stockagent.data.tw_index_futures import TaiwanIndexFuturesDaySession
from stockagent.models.cross_sectional_index_futures import (
    CrossSectionalIndexFuturesModel,
)
from stockagent.models.factory import build_model
from stockagent.training.dataset import CrossSectionalDataset


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
    )
    panel.index_futures_reference_product = "TX"
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
