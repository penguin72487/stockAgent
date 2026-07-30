from __future__ import annotations

import numpy as np

from stockagent.data.panel import PanelData
from stockagent.data.tw_index_futures import TaiwanIndexFuturesDaySession
from stockagent.strategies.tw_index_futures_day import (
    decision_indices_for_futures_day_session,
    decision_stock_mask,
    model_feature_end_indices,
)


def _panel() -> PanelData:
    rows = 8
    symbols = 2
    alive = np.ones((rows, symbols), dtype=bool)
    alive[6] = False
    return PanelData(
        dates=np.arange(
            np.datetime64("2025-01-01"),
            np.datetime64("2025-01-09"),
            dtype="datetime64[D]",
        ),
        symbols=["A", "B"],
        feature_names=["feature"],
        features=np.zeros((rows, symbols, 1), dtype=np.float32),
        returns_1d=np.zeros((rows, symbols), dtype=np.float32),
        tradable_mask=np.ones((rows, symbols), dtype=bool),
        alive_mask=alive,
        benchmark_returns=np.zeros(rows, dtype=np.float32),
        close_prices=np.ones((rows, symbols), dtype=np.float32),
    )


def _market(panel: PanelData) -> TaiwanIndexFuturesDaySession:
    rows = panel.num_dates
    tradable = np.ones((rows, 3), dtype=bool)
    tradable[6, 0] = False
    prices = np.full((rows, 3), 20000.0)
    return TaiwanIndexFuturesDaySession(
        dates=panel.dates,
        products=("TX", "MTX", "TMF"),
        contract_months=np.full((rows, 3), "202501", dtype="U16"),
        open_prices=prices,
        high_prices=prices,
        low_prices=prices,
        close_prices=prices,
        volumes=np.ones((rows, 3), dtype=np.int64),
        log_returns=np.zeros((rows, 3), dtype=np.float64),
        tradable_mask=tradable,
        multipliers=np.asarray([200, 50, 10], dtype=np.int64),
    )


def test_decision_window_ends_at_previous_stock_session() -> None:
    panel = _panel()
    market = _market(panel)
    decisions = decision_indices_for_futures_day_session(
        panel,
        market,
        np.arange(panel.num_dates),
        lookback=3,
        lookback_context="panel_history",
    )

    # Row 6 is rejected because TX is unavailable and row 7 because the prior
    # stock row has no alive symbol.
    np.testing.assert_array_equal(decisions, [3, 4, 5])
    np.testing.assert_array_equal(model_feature_end_indices(decisions), [2, 3, 4])
    np.testing.assert_array_equal(
        decision_stock_mask(panel, decisions),
        panel.alive_mask[[2, 3, 4]],
    )
