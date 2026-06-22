from __future__ import annotations

import numpy as np
import polars as pl

from stockagent.backtest.simulator import HoldingsRecord, holding_record_abs_sort_key
from stockagent.live.portfolio_state import build_rebalance_rows, classify_rebalance_action, estimate_drifted_weights
from stockagent.live.report_formatter import INVESTMENT_WARNING, format_signal_message
from stockagent.live.signal_engine import _build_decision_rows, _date_string, _load_previous_weights, write_live_weights_history
from stockagent.live.portfolio_history import load_portfolio_history
from stockagent.live.stock_history import load_stock_history


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


def test_holdings_record_sort_key_uses_absolute_holding_ratio() -> None:
    rows = [
        HoldingsRecord("2026-01-02", "LONG", 1, 10.0, 10.0, 0.20, False),
        HoldingsRecord("2026-01-02", "SHORT", -1, 10.0, -10.0, -0.40, False),
        HoldingsRecord("2026-01-02", "CASH", 0, 1.0, 30.0, 0.30, True),
    ]

    rows.sort(key=holding_record_abs_sort_key)

    assert [row.symbol for row in rows] == ["SHORT", "CASH", "LONG"]


def test_classify_rebalance_action_handles_hold_reduce_exit_and_direction() -> None:
    assert classify_rebalance_action(0.1, 0.1000000001) == "HOLD"
    assert classify_rebalance_action(0.1, 0.0) == "EXIT"
    assert classify_rebalance_action(0.2, 0.1) == "REDUCE"
    assert classify_rebalance_action(0.0, 0.1) == "BUY"
    assert classify_rebalance_action(0.0, -0.1) == "SELL"


def test_build_decision_rows_records_scores_constraints_and_reasons() -> None:
    rows = _build_decision_rows(
        symbols=["A", "B", "C"],
        symbol_names={"B": "Bravo"},
        asof_date="2026-06-19",
        panel_date="2026-06-18",
        model_weights=np.array([0.2, -0.3, 0.0]),
        current_weights=np.array([0.1, 0.0, -0.1]),
        target_weights=np.array([0.2, -0.3, 0.0]),
        scores=np.array([0.5, -1.2, 0.0]),
        current_prices=np.array([10.0, 20.0, 30.0]),
        base_prices=np.array([10.0, 10.0, 30.0]),
        price_returns=np.array([0.0, 1.0, 0.0]),
        tradable_mask=np.array([True, True, False]),
        can_buy_mask=np.array([True, True, False]),
        can_sell_mask=np.array([True, True, False]),
        aux=None,
    )

    assert [row["symbol"] for row in rows] == ["B", "A", "C"]
    assert rows[0]["name"] == "Bravo"
    assert rows[0]["action"] == "SELL"
    assert rows[0]["abs_score_rank"] == 1
    assert rows[0]["trade_price"] == 20.0
    assert "negative_score" in rows[0]["decision_reason"]
    assert rows[-1]["constraint"] == "not_tradable"


def test_live_signal_dates_preserve_intraday_time_and_live_weights_take_precedence(tmp_path) -> None:
    assert _date_string(np.datetime64("2026-06-22T00:15:00")) == "2026-06-22 00:15:00"
    assert _date_string(np.datetime64("2026-06-22")) == "2026-06-22"

    fold_dir = tmp_path / "fold_06"
    fold_dir.mkdir()
    pl.DataFrame({"date": ["2026-06-19"], "AAA": [0.90]}).write_parquet(fold_dir / "daily_weights.parquet")

    first_summary = {"asof_date": "2026-06-22 00:00:00"}
    first_rows = [{"symbol": "AAA", "target_weight": 0.10}]
    path = write_live_weights_history(fold_dir, first_summary, first_rows)
    second_summary = {"asof_date": "2026-06-22 00:15:00"}
    second_rows = [{"symbol": "AAA", "target_weight": 0.20}]
    write_live_weights_history(fold_dir, second_summary, second_rows)

    weights, date_text, weights_path = _load_previous_weights(
        ["AAA"],
        output_dir=tmp_path,
        fold_id=6,
        weights_path=None,
        asof_date="2026-06-22 00:10:00",
    )
    assert weights_path == path
    assert date_text == "2026-06-22 00:00:00"
    assert np.isclose(weights[0], 0.10)

    weights, date_text, weights_path = _load_previous_weights(
        ["AAA"],
        output_dir=tmp_path,
        fold_id=6,
        weights_path=None,
        asof_date="2026-06-22 00:20:00",
    )
    assert weights_path == path
    assert date_text == "2026-06-22 00:15:00"
    assert np.isclose(weights[0], 0.20)


def test_load_stock_history_combines_model_integer_and_holding_tables(tmp_path) -> None:
    fold_dir = tmp_path / "fold_25"
    fold_dir.mkdir()
    pl.DataFrame(
        {
            "date": ["2026-01-02", "2026-01-05", "2026-01-06"],
            "AAA": [0.10, 0.15, -0.20],
        }
    ).write_parquet(fold_dir / "daily_weights.parquet")
    pl.DataFrame(
        {
            "date": ["2026-01-02", "2026-01-05", "2026-01-06"],
            "AAA": [0.00, 0.12, -0.18],
        }
    ).write_parquet(fold_dir / "integer_share_daily_weights.parquet")
    pl.DataFrame(
        {
            "date": ["2026-01-05", "2026-01-06"],
            "symbol": ["AAA", "AAA"],
            "shares": [10, -5],
            "price": [20.0, 22.0],
            "market_value": [200.0, -110.0],
            "holding_ratio": [0.12, -0.18],
            "is_cash": [False, False],
        }
    ).write_parquet(fold_dir / "holdings.parquet")
    pl.DataFrame(
        {
            "date": ["2026-01-02", "2026-01-05", "2026-01-06"],
            "portfolio_return": [0.01, -0.02, 0.03],
            "benchmark_return": [0.00, 0.01, -0.01],
            "turnover": [0.1, 0.2, 0.3],
        }
    ).write_parquet(fold_dir / "integer_share_daily_portfolio_returns.parquet")

    result = load_stock_history(fold_dir, "aaa", limit=2, symbol_names={"AAA": "Alpha"})

    assert result.symbol == "AAA"
    assert result.name == "Alpha"
    assert [row["date"] for row in result.rows] == ["2026-01-06", "2026-01-05"]
    assert [row["action"] for row in result.rows] == ["FLIP_TO_SHORT", "OPEN_LONG"]
    assert result.rows[0]["shares"] == -5
    assert np.isclose(result.rows[0]["model_weight_delta"], -0.35)
    assert np.isclose(result.rows[1]["holding_ratio"], 0.12)


def test_load_stock_history_collapses_intraday_snapshots_to_daily(tmp_path) -> None:
    fold_dir = tmp_path / "fold_06"
    fold_dir.mkdir()
    pl.DataFrame(
        {
            "date": ["2026-01-02 00:00:00", "2026-01-02 00:15:00", "2026-01-05 00:00:00"],
            "AAA": [0.10, 0.20, 0.30],
        }
    ).write_parquet(fold_dir / "daily_weights.parquet")
    pl.DataFrame(
        {
            "date": ["2026-01-02 00:00:00", "2026-01-02 00:15:00", "2026-01-05 00:00:00"],
            "AAA": [0.08, 0.18, 0.28],
        }
    ).write_parquet(fold_dir / "integer_share_daily_weights.parquet")
    pl.DataFrame(
        {
            "date": ["2026-01-02", "2026-01-02", "2026-01-05"],
            "symbol": ["AAA", "AAA", "AAA"],
            "shares": [10, 20, 30],
            "price": [10.0, 11.0, 12.0],
            "market_value": [100.0, 220.0, 360.0],
            "holding_ratio": [0.08, 0.18, 0.28],
            "is_cash": [False, False, False],
        }
    ).write_parquet(fold_dir / "holdings.parquet")
    pl.DataFrame(
        {
            "date": ["2026-01-02 00:00:00", "2026-01-02 00:15:00", "2026-01-05 00:00:00"],
            "portfolio_return": [0.01, 0.02, 0.03],
            "benchmark_return": [0.00, 0.01, -0.01],
            "turnover": [0.10, 0.20, 0.30],
        }
    ).write_parquet(fold_dir / "integer_share_daily_portfolio_returns.parquet")

    result = load_stock_history(fold_dir, "AAA", limit=0, changes_only=False)

    assert [row["date"] for row in result.rows] == ["2026-01-05", "2026-01-02"]
    assert np.isclose(result.rows[1]["model_weight"], 0.20)
    assert np.isclose(result.rows[1]["actual_weight"], 0.18)
    assert result.rows[1]["shares"] == 20
    assert np.isclose(result.rows[1]["market_value"], 220.0)
    assert np.isclose(result.rows[1]["portfolio_return"], 1.01 * 1.02 - 1.0)
    assert np.isclose(result.rows[1]["turnover"], 0.30)
    assert result.rows[0]["prev_shares"] == 20
    assert result.rows[0]["share_delta"] == 10


def test_load_stock_history_can_preserve_intraday_bar_rows(tmp_path) -> None:
    fold_dir = tmp_path / "fold_06"
    fold_dir.mkdir()
    pl.DataFrame(
        {
            "date": ["2026-01-02 00:00:00", "2026-01-02 00:15:00", "2026-01-05 00:00:00"],
            "AAA": [0.10, 0.20, 0.30],
        }
    ).write_parquet(fold_dir / "daily_weights.parquet")
    pl.DataFrame(
        {
            "date": ["2026-01-02 00:00:00", "2026-01-02 00:15:00", "2026-01-05 00:00:00"],
            "AAA": [0.08, 0.18, 0.28],
        }
    ).write_parquet(fold_dir / "integer_share_daily_weights.parquet")
    pl.DataFrame(
        {
            "date": ["2026-01-02", "2026-01-02", "2026-01-05"],
            "symbol": ["AAA", "AAA", "AAA"],
            "shares": [10, 20, 30],
            "price": [10.0, 11.0, 12.0],
            "market_value": [100.0, 220.0, 360.0],
            "holding_ratio": [0.08, 0.18, 0.28],
            "is_cash": [False, False, False],
        }
    ).write_parquet(fold_dir / "holdings.parquet")
    pl.DataFrame(
        {
            "date": ["2026-01-02 00:00:00", "2026-01-02 00:15:00", "2026-01-05 00:00:00"],
            "portfolio_return": [0.01, 0.02, 0.03],
            "benchmark_return": [0.00, 0.01, -0.01],
            "turnover": [0.10, 0.20, 0.30],
        }
    ).write_parquet(fold_dir / "integer_share_daily_portfolio_returns.parquet")

    result = load_stock_history(fold_dir, "AAA", limit=0, changes_only=False, frequency="bar")

    assert [row["date"] for row in result.rows] == [
        "2026-01-05 00:00:00",
        "2026-01-02 00:15:00",
        "2026-01-02 00:00:00",
    ]
    assert np.isclose(result.rows[1]["portfolio_return"], 0.02)
    assert np.isclose(result.rows[1]["turnover"], 0.20)
    assert result.rows[1]["shares"] == 20
    assert np.isclose(result.rows[1]["market_value"], 220.0)
    assert result.rows[1]["prev_shares"] == 10


def test_load_portfolio_history_summarizes_pnl_and_holding_changes(tmp_path) -> None:
    fold_dir = tmp_path / "fold_25"
    fold_dir.mkdir()
    pl.DataFrame(
        {
            "date": ["2026-01-02", "2026-01-02", "2026-01-05", "2026-01-05", "2026-01-06"],
            "symbol": ["CASH", "AAA", "CASH", "AAA", "CASH"],
            "shares": [900, 10, 800, 20, 1000],
            "price": [1.0, 10.0, 1.0, 10.0, 1.0],
            "market_value": [900.0, 100.0, 800.0, 200.0, 1000.0],
            "holding_ratio": [0.9, 0.1, 0.8, 0.2, 1.0],
            "is_cash": [True, False, True, False, True],
        }
    ).write_parquet(fold_dir / "holdings.parquet")
    pl.DataFrame(
        {
            "date": ["2026-01-02", "2026-01-05", "2026-01-06"],
            "portfolio_return": [0.01, 0.05, -0.02],
            "benchmark_return": [0.00, 0.02, 0.01],
            "turnover": [0.10, 0.20, 0.30],
        }
    ).write_parquet(fold_dir / "integer_share_daily_portfolio_returns.parquet")

    result = load_portfolio_history(fold_dir, days=2, top_changes=2, symbol_names={"AAA": "Alpha"})

    assert result.start_date == "2026-01-05"
    assert result.end_date == "2026-01-06"
    assert np.isclose(result.period_return, 1.05 * 0.98 - 1.0)
    assert np.isclose(result.profit_value, 30.0)
    assert [row["date"] for row in result.rows] == ["2026-01-06", "2026-01-05"]
    assert result.rows[0]["changes"][0]["action"] == "EXIT_LONG"
    assert result.rows[1]["changes"][0]["action"] == "ADD_LONG"
    assert result.rows[1]["changes"][0]["name"] == "Alpha"


def test_load_portfolio_history_scales_values_from_current_capital(tmp_path) -> None:
    fold_dir = tmp_path / "fold_25"
    fold_dir.mkdir()
    pl.DataFrame(
        {
            "date": ["2026-01-02", "2026-01-02", "2026-01-05", "2026-01-05"],
            "symbol": ["CASH", "AAA", "CASH", "AAA"],
            "shares": [900, 10, 800, 20],
            "price": [1.0, 10.0, 1.0, 10.0],
            "market_value": [900.0, 100.0, 800.0, 200.0],
            "holding_ratio": [0.9, 0.1, 0.8, 0.2],
            "is_cash": [True, False, True, False],
        }
    ).write_parquet(fold_dir / "holdings.parquet")
    pl.DataFrame(
        {
            "date": ["2026-01-02", "2026-01-05"],
            "portfolio_return": [0.01, 0.05],
            "benchmark_return": [0.00, 0.02],
            "turnover": [0.10, 0.20],
        }
    ).write_parquet(fold_dir / "integer_share_daily_portfolio_returns.parquet")

    result = load_portfolio_history(fold_dir, days=1, top_changes=1, current_capital=2000.0)

    assert result.capital.mode == "current_capital"
    assert np.isclose(result.capital.scale, 2.0)
    assert np.isclose(result.rows[0]["nav"], 2000.0)
    assert np.isclose(result.rows[0]["profit_value"], 100.0)
    assert np.isclose(result.rows[0]["changes"][0]["market_value"], 400.0)
    assert np.isclose(result.rows[0]["changes"][0]["market_value_delta"], 200.0)


def test_load_portfolio_history_collapses_intraday_snapshots_to_daily(tmp_path) -> None:
    fold_dir = tmp_path / "fold_06"
    fold_dir.mkdir()
    pl.DataFrame(
        {
            "date": ["2026-01-02", "2026-01-02", "2026-01-02", "2026-01-02", "2026-01-05", "2026-01-05"],
            "symbol": ["CASH", "AAA", "CASH", "AAA", "CASH", "AAA"],
            "shares": [900, 10, 800, 20, 700, 30],
            "price": [1.0, 10.0, 1.0, 11.0, 1.0, 12.0],
            "market_value": [900.0, 100.0, 800.0, 220.0, 700.0, 360.0],
            "holding_ratio": [0.90, 0.10, 0.80, 0.22, 0.70, 0.36],
            "is_cash": [True, False, True, False, True, False],
        }
    ).write_parquet(fold_dir / "holdings.parquet")
    pl.DataFrame(
        {
            "date": ["2026-01-02 00:00:00", "2026-01-02 00:15:00", "2026-01-05 00:00:00"],
            "portfolio_return": [0.01, 0.02, 0.03],
            "benchmark_return": [0.00, 0.01, -0.01],
            "turnover": [0.10, 0.20, 0.30],
        }
    ).write_parquet(fold_dir / "integer_share_daily_portfolio_returns.parquet")

    result = load_portfolio_history(fold_dir, days=2, top_changes=2, symbol_names={"AAA": "Alpha"})

    assert result.days == 2
    assert result.start_date == "2026-01-02"
    assert result.end_date == "2026-01-05"
    assert [row["date"] for row in result.rows] == ["2026-01-05", "2026-01-02"]
    assert np.isclose(result.period_return, (1.01 * 1.02) * 1.03 - 1.0)
    assert np.isclose(result.rows[1]["portfolio_return"], 1.01 * 1.02 - 1.0)
    assert np.isclose(result.rows[1]["turnover"], 0.30)
    assert np.isclose(result.rows[1]["nav"], 1020.0)
    assert np.isclose(result.rows[1]["gross_exposure"], 220.0)
    assert result.rows[1]["position_count"] == 1
    assert result.rows[1]["changes"][0]["action"] == "OPEN_LONG"
    assert np.isclose(result.rows[1]["changes"][0]["holding_ratio"], 0.22)
    assert result.rows[0]["changes"][0]["action"] == "ADD_LONG"
    assert np.isclose(result.rows[0]["changes"][0]["market_value_delta"], 140.0)


def test_load_portfolio_history_can_preserve_intraday_bar_rows(tmp_path) -> None:
    fold_dir = tmp_path / "fold_06"
    fold_dir.mkdir()
    pl.DataFrame(
        {
            "date": [
                "2026-01-02",
                "2026-01-02",
                "2026-01-02",
                "2026-01-02",
                "2026-01-05",
                "2026-01-05",
            ],
            "symbol": ["CASH", "AAA", "CASH", "AAA", "CASH", "AAA"],
            "shares": [900, 10, 800, 20, 700, 30],
            "price": [1.0, 10.0, 1.0, 11.0, 1.0, 12.0],
            "market_value": [900.0, 100.0, 800.0, 220.0, 700.0, 360.0],
            "holding_ratio": [0.90, 0.10, 0.80, 0.22, 0.70, 0.36],
            "is_cash": [True, False, True, False, True, False],
        }
    ).write_parquet(fold_dir / "holdings.parquet")
    pl.DataFrame(
        {
            "date": ["2026-01-02 00:00:00", "2026-01-02 00:15:00", "2026-01-05 00:00:00"],
            "portfolio_return": [0.01, 0.02, 0.03],
            "benchmark_return": [0.00, 0.01, -0.01],
            "turnover": [0.10, 0.20, 0.30],
        }
    ).write_parquet(fold_dir / "integer_share_daily_portfolio_returns.parquet")

    result = load_portfolio_history(fold_dir, days=3, top_changes=2, frequency="bar")

    assert result.frequency == "bar"
    assert result.days == 3
    assert result.start_date == "2026-01-02 00:00:00"
    assert result.end_date == "2026-01-05 00:00:00"
    assert [row["date"] for row in result.rows] == [
        "2026-01-05 00:00:00",
        "2026-01-02 00:15:00",
        "2026-01-02 00:00:00",
    ]
    assert np.isclose(result.period_return, 1.01 * 1.02 * 1.03 - 1.0)
    assert np.isclose(result.rows[1]["portfolio_return"], 0.02)
    assert np.isclose(result.rows[1]["turnover"], 0.20)
    assert np.isclose(result.rows[1]["nav"], 1020.0)
    assert np.isclose(result.rows[1]["profit_value"], 20.0)
    assert result.rows[1]["changes"][0]["action"] == "ADD_LONG"
    assert np.isclose(result.rows[1]["changes"][0]["market_value_delta"], 120.0)


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
        "market_notice": "今天沒有開盤，使用最後可用資料 `2026-06-19` 產生訊號。",
        "decision_explanation_path": "artifacts/live_signals/tw/2026-06-19/signal/decision_explanations.parquet",
        "top_positions": [
            {"symbol": f"S{i:02d}", "name": f"Name{i:02d}", "weight": 0.2 - i * 0.01, "current_price": 1000.0 + i}
            for i in range(10)
        ],
        "rebalance": [
            {
                "symbol": f"S{i:02d}",
                "name": f"Name{i:02d}",
                "delta_weight": 0.05 - i * 0.001,
                "trade_price": 1000.0 + i,
                "current_weight": 0.15,
                "target_weight": 0.2,
            }
            for i in range(10)
        ],
    }

    message = format_signal_message(summary, max_rows=10)

    assert "stockAgent live signal" in message
    assert "S00" in message
    assert "Name00" in message
    assert "10. `S09` Name09" in message
    assert "px=1000.00" in message
    assert "今天沒有開盤" in message
    assert "explain:" in message
    assert INVESTMENT_WARNING in message
    assert len(message) < 1900


def test_format_signal_message_shows_period_and_recent_baseline_pnl() -> None:
    summary = {
        "asof_date": "2026-06-22 00:15:00",
        "panel_date": "2026-06-22 00:15:00",
        "previous_weights_date": "2026-06-22 00:00:00",
        "portfolio_simple_return": 0.01,
        "benchmark_simple_return": 0.002,
        "display_capital": 100_000.0,
        "portfolio_pnl_value": 1_000.0,
        "benchmark_pnl_value": 200.0,
        "excess_pnl_value": 800.0,
        "recent_performance": {
            "window_days": 32,
            "window_label": "過去32天",
            "strategy_return": 0.08,
            "benchmark_return": 0.03,
            "excess_return": 0.05,
            "strategy_pnl_value": 8_000.0,
            "benchmark_pnl_value": 3_000.0,
            "excess_pnl_value": 5_000.0,
        },
    }

    message = format_signal_message(summary, max_rows=0)

    assert "上個訊號到現在" in message
    assert "`portfolio=+1.00%`" in message
    assert "`baseline=+0.20%`" in message
    assert "`excess=+0.80%`" in message
    assert "`capital=100,000`" in message
    assert "`pnl=+1,000`" in message
    assert "過去32天" in message
    assert "`strategy=+8.00%`" in message
    assert "`baseline=+3.00%`" in message
    assert "`excess_pnl=+5,000`" in message


def test_format_signal_message_displays_crypto_times_in_taipei_timezone() -> None:
    summary = {
        "market_label": "加密貨幣",
        "asof_date": "2026-06-22 03:45:00",
        "panel_date": "2026-06-22 03:45:00",
        "previous_weights_date": "2026-06-22 03:30:00",
        "data_timezone": "UTC",
        "display_timezone": "Asia/Taipei",
        "display_timezone_label": "UTC+8 台北",
        "portfolio_simple_return": 0.01,
        "benchmark_simple_return": 0.002,
    }

    message = format_signal_message(summary, max_rows=0)

    assert "`2026-06-22 11:45:00`  `tz=UTC+8 台北`" in message
    assert "`panel=2026-06-22 11:45:00`" in message
    assert "`2026-06-22 11:30:00`..`2026-06-22 11:45:00`" in message
