from __future__ import annotations

import numpy as np

from stockagent.live.portfolio_state import build_rebalance_rows, estimate_drifted_weights
from stockagent.live.report_formatter import format_signal_message


def test_estimate_drifted_weights_marks_signed_portfolio_to_market() -> None:
    previous = np.array([0.5, -0.25, 0.0], dtype=np.float64)
    base = np.array([100.0, 50.0, 10.0], dtype=np.float64)
    current = np.array([110.0, 40.0, 10.0], dtype=np.float64)

    result = estimate_drifted_weights(previous, base, current)

    # Long leg gains 5%, short leg gains another 5%, total NAV +10%.
    assert np.isclose(result.simple_return, 0.10)
    assert np.isclose(result.nav_ratio, 1.10)
    assert np.allclose(result.weights[:2], [0.5, -0.1818181818])


def test_build_rebalance_rows_sorts_by_absolute_delta() -> None:
    rows = build_rebalance_rows(
        ["A", "B", "C"],
        np.array([0.1, 0.0, -0.2]),
        np.array([0.2, -0.3, -0.1]),
        np.array([10.0, 20.0, 30.0]),
        np.array([10.0, 10.0, 30.0]),
        symbol_names={"B": "Bravo"},
        min_abs_delta=0.05,
    )

    assert [row["symbol"] for row in rows] == ["B", "A", "C"]
    assert rows[0]["name"] == "Bravo"
    assert rows[0]["delta_weight"] == -0.3
    assert rows[0]["trade_price"] == 20.0
    assert rows[0]["price_return"] == 1.0


def test_format_signal_message_stays_discord_sized() -> None:
    summary = {
        "asof_date": "2026-06-19",
        "panel_date": "2026-06-18",
        "fold_id": 25,
        "price_source": "panel_close",
        "portfolio_simple_return": 0.0123,
        "benchmark_simple_return": -0.004,
        "turnover": 0.52,
        "estimated_trade_cost": 0.001,
        "top_positions": [{"symbol": "2330", "name": "TSMC", "weight": 0.2, "current_price": 1000.0}],
        "rebalance": [
            {
                "symbol": "2330",
                "name": "TSMC",
                "delta_weight": 0.05,
                "trade_price": 1000.0,
                "current_weight": 0.15,
                "target_weight": 0.2,
            }
        ],
    }

    message = format_signal_message(summary, max_rows=10)

    assert "stockAgent live signal" in message
    assert "2330" in message
    assert "TSMC" in message
    assert "px=1000.00" in message
    assert len(message) < 1900
