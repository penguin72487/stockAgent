from __future__ import annotations

from types import SimpleNamespace

import numpy as np
import polars as pl

from services.discord_bot.bot import (
    _add_user_watch_symbol,
    _decision_overview_page,
    _daily_summary_message,
    _filter_watchlist_rows,
    _latest_changes_pages,
    _latest_signal_message,
    _performance_message,
    _enrich_signal_performance_for_discord,
    _position_line,
    _portfolio_change_line,
    _portfolio_history_header_lines,
    _portfolio_history_block,
    _prepend_latest_signal_row_to_portfolio_history,
    _rebalance_line,
    _remove_user_watch_symbol,
    _risk_message,
    _scheduled_detail_page_groups,
    _signal_sanity_issues,
    _signal_sanity_level,
    _stock_history_header_lines,
    _user_watchlist,
)


def test_scheduled_detail_pages_include_positions_and_rebalances() -> None:
    cfg = SimpleNamespace(
        market="unit",
        min_abs_delta=0.001,
        current_capital=1_000_000.0,
        initial_capital=None,
    )
    result = SimpleNamespace(
        summary={
            "market": "unit",
            "signal_id": "sig-test",
            "panel_date": "2026-06-21",
            "price_source": "panel_close",
            "output_dir": "artifacts/live_signals/unit/2026-06-21/sig-test",
            "positions_markdown_path": "artifacts/live_signals/unit/target_positions.md",
            "rebalance_markdown_path": "artifacts/live_signals/unit/rebalance.md",
        },
        output_dir=None,
        weights_rows=[
            {
                "symbol": "AAA",
                "name": "Alpha",
                "action": "BUY",
                "current_weight": 0.01,
                "target_weight": 0.05,
                "delta_weight": 0.04,
                "score": 1.2,
                "current_price": 10.0,
                "price_return": 0.02,
            },
            {
                "symbol": "BBB",
                "name": "Beta",
                "action": "HOLD",
                "current_weight": 0.0,
                "target_weight": 0.0,
                "delta_weight": 0.0,
                "score": 0.0,
                "current_price": 20.0,
                "price_return": 0.0,
            },
            {
                "symbol": "CCC",
                "name": "Gamma",
                "action": "SELL",
                "current_weight": -0.04,
                "target_weight": -0.01,
                "delta_weight": 0.03,
                "score": -0.5,
                "current_price": 30.0,
                "price_return": -0.01,
            },
        ],
        rebalance_rows=[
            {
                "symbol": "AAA",
                "name": "Alpha",
                "action": "BUY",
                "current_weight": 0.01,
                "target_weight": 0.05,
                "delta_weight": 0.04,
                "trade_price": 10.0,
                "price_return": 0.02,
            },
            {
                "symbol": "CCC",
                "name": "Gamma",
                "action": "SELL",
                "current_weight": -0.04,
                "target_weight": -0.01,
                "delta_weight": 0.03,
                "trade_price": 30.0,
                "price_return": -0.01,
            },
        ],
    )

    position_pages, rebalance_pages = _scheduled_detail_page_groups(cfg, result)
    debug_position_pages, debug_rebalance_pages = _scheduled_detail_page_groups(cfg, result, debug=True)

    assert len(position_pages) == 1
    assert len(rebalance_pages) == 1
    assert "scheduled current / target positions" in position_pages[0]
    assert "scheduled rebalance" in rebalance_pages[0]
    assert "`AAA` Alpha **BUY**" in position_pages[0]
    assert "`CCC` Gamma **SELL**" in position_pages[0]
    assert "`BBB`" not in position_pages[0]
    assert "`now=1.00%`" in position_pages[0]
    assert "`target=5.00%`" in position_pages[0]
    assert "`delta_value=+40,000`" in rebalance_pages[0]
    assert "display_tz=" not in position_pages[0]
    assert "output:" not in position_pages[0]
    assert "full:" not in position_pages[0]
    assert "display_tz=" in debug_position_pages[0]
    assert "output:" in debug_position_pages[0]
    assert "full:" in debug_position_pages[0]
    assert "full:" in debug_rebalance_pages[0]


def test_signal_now_detail_pages_include_actionable_decisions() -> None:
    cfg = SimpleNamespace(
        market="unit",
        min_abs_delta=0.001,
        current_capital=None,
        initial_capital=None,
    )
    result = SimpleNamespace(
        summary={
            "market": "unit",
            "signal_id": "sig-test",
            "panel_date": "2026-06-21",
            "price_source": "panel_close",
            "output_dir": "artifacts/live_signals/unit/2026-06-21/sig-test",
            "positions_markdown_path": "artifacts/live_signals/unit/target_positions.md",
            "rebalance_markdown_path": "artifacts/live_signals/unit/rebalance.md",
            "decision_report_path": "artifacts/live_signals/unit/decision_report.md",
        },
        output_dir=None,
        weights_rows=[
            {
                "symbol": "AAA",
                "name": "Alpha",
                "action": "BUY",
                "current_weight": 0.01,
                "target_weight": 0.05,
                "delta_weight": 0.04,
                "score": 1.2,
                "current_price": 10.0,
                "price_return": 0.02,
            },
        ],
        rebalance_rows=[
            {
                "symbol": "AAA",
                "name": "Alpha",
                "action": "BUY",
                "current_weight": 0.01,
                "target_weight": 0.05,
                "delta_weight": 0.04,
                "trade_price": 10.0,
                "price_return": 0.02,
            },
        ],
        decision_rows=[
            {
                "symbol": "AAA",
                "name": "Alpha",
                "action": "BUY",
                "current_weight": 0.01,
                "model_weight": 0.06,
                "target_weight": 0.05,
                "delta_weight": 0.04,
                "trade_price": 10.0,
                "price_return": 0.02,
                "score": 1.2,
                "abs_score_rank": 1,
                "abs_target_rank": 1,
                "tradable": True,
                "can_buy": True,
                "can_sell": True,
                "decision_reason": "positive_score, target_increase",
            },
            {
                "symbol": "BBB",
                "name": "Beta",
                "action": "HOLD",
                "current_weight": 0.0,
                "model_weight": 0.0,
                "target_weight": 0.0,
                "delta_weight": 0.0,
                "score": 0.0,
                "decision_reason": "no_change",
            },
        ],
    )

    position_pages, rebalance_pages, decision_pages = _scheduled_detail_page_groups(
        cfg,
        result,
        title_prefix="signal_now",
        include_decisions=True,
    )
    debug_pages = _scheduled_detail_page_groups(
        cfg,
        result,
        title_prefix="signal_now",
        include_decisions=True,
        debug=True,
    )

    assert "signal_now current / target positions" in position_pages[0]
    assert "signal_now rebalance" in rebalance_pages[0]
    assert "signal_now decision explanations" in decision_pages[0]
    assert "`rows=1`" in decision_pages[0]
    assert "`AAA` Alpha **BUY**" in decision_pages[0]
    assert "`BBB`" not in decision_pages[0]
    assert "full:" not in decision_pages[0]
    assert "full:" in debug_pages[2][0]


def test_signal_sanity_blocks_implausible_latest_return() -> None:
    cfg = SimpleNamespace(
        market="unit",
        label="Unit",
        config_path="missing.yaml",
    )
    summary = {
        "market": "unit",
        "asof_date": "2026-06-24 13:30:00",
        "panel_date": "2026-06-24 13:30:00",
        "portfolio_simple_return": 0.80,
        "benchmark_simple_return": 0.01,
    }

    issues = _signal_sanity_issues(cfg, summary)

    assert _signal_sanity_level(issues) == "BLOCK"
    assert any("portfolio return" in text for _, text in issues)


def test_latest_signal_message_uses_saved_summary_without_debug_paths(tmp_path) -> None:
    cfg = SimpleNamespace(
        market="unit",
        label="Unit",
        current_capital=100_000.0,
        initial_capital=None,
        benchmark_window_days=32,
        history_frequency="daily",
        config_path="missing.yaml",
    )
    summary = {
        "market": "unit",
        "market_label": "Unit",
        "signal_id": "sig-test",
        "fold_id": 25,
        "asof_date": "2026-06-24 13:30:00",
        "panel_date": "2026-06-24 13:30:00",
        "price_source": "panel_close",
        "portfolio_simple_return": 0.01,
        "benchmark_simple_return": 0.002,
        "turnover": 0.1,
        "weights_path": str(tmp_path / "weights.parquet"),
        "rebalance_path": str(tmp_path / "rebalance.parquet"),
        "top_positions": [{"symbol": "AAA", "name": "Alpha", "weight": 0.1, "current_price": 10.0}],
        "rebalance": [{"symbol": "AAA", "name": "Alpha", "action": "BUY", "delta_weight": 0.02, "current_weight": 0.0, "target_weight": 0.02, "trade_price": 10.0}],
    }

    normal = _latest_signal_message(cfg, tmp_path / "summary.json", summary, top_n=1, debug=False)
    debug = _latest_signal_message(cfg, tmp_path / "summary.json", summary, top_n=1, debug=True)

    assert "stockAgent live signal" in normal
    assert "`AAA` Alpha" in normal
    assert "fold=" not in normal
    assert "**files**" not in normal
    assert "`fold=25`" in debug
    assert "**files**" in debug


def test_latest_changes_pages_can_filter_to_watchlist(tmp_path) -> None:
    cfg = SimpleNamespace(
        market="unit",
        label="Unit",
        current_capital=100_000.0,
        initial_capital=None,
        min_abs_delta=0.001,
        config_path="missing.yaml",
    )
    summary = {
        "market": "unit",
        "signal_id": "sig-test",
        "asof_date": "2026-06-24 13:30:00",
        "panel_date": "2026-06-24 13:30:00",
        "rebalance": [
            {"symbol": "AAA", "name": "Alpha", "action": "BUY", "delta_weight": 0.04, "current_weight": 0.0, "target_weight": 0.04, "trade_price": 10.0},
            {"symbol": "BBB", "name": "Beta", "action": "SELL", "delta_weight": -0.03, "current_weight": 0.03, "target_weight": 0.0, "trade_price": 20.0},
        ],
    }

    pages = _latest_changes_pages(
        cfg,
        tmp_path / "summary.json",
        summary,
        watchlist=["AAA"],
        current_capital=100_000.0,
        page_size=10,
    )

    assert "`AAA` Alpha" in pages[0]
    assert "`BBB` Beta" not in pages[0]
    assert "`watch=AAA`" in pages[0]
    assert "`delta_value=+4,000`" in pages[0]


def test_performance_and_risk_messages_are_investor_facing(tmp_path) -> None:
    cfg = SimpleNamespace(
        market="unit",
        label="Unit",
        current_capital=100_000.0,
        initial_capital=None,
        benchmark_window_days=32,
        history_frequency="daily",
        config_path="missing.yaml",
    )
    summary = {
        "market": "unit",
        "asof_date": "2026-06-24 13:30:00",
        "panel_date": "2026-06-24 13:30:00",
        "portfolio_simple_return": 0.01,
        "benchmark_simple_return": 0.002,
        "turnover": 0.1,
        "estimated_trade_cost": 0.001,
        "recent_performance": {
            "window_days": 32,
            "strategy_return": 0.08,
            "benchmark_return": 0.03,
            "excess_return": 0.05,
        },
        "target_risk": {
            "gross": 0.95,
            "long_gross": 0.60,
            "short_gross": 0.35,
            "net": 0.25,
            "top_abs_weight": 0.12,
            "hhi": 0.08,
        },
        "top_positions": [{"symbol": "AAA", "name": "Alpha", "weight": 0.12, "current_price": 10.0}],
    }

    performance = _performance_message(cfg, tmp_path / "summary.json", summary, days=0)
    risk = _risk_message(cfg, tmp_path / "summary.json", summary, top_n=3)

    assert "**上個訊號到現在**" in performance
    assert "**過去32天**" in performance
    assert "`excess=+0.80%`" in performance
    assert "**largest positions**" in risk
    assert "`AAA` Alpha" in risk
    assert "`sanity=OK`" in risk


def test_user_watchlist_state_add_remove_and_filter(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr("services.discord_bot.bot.STATE_PATH", tmp_path / "state.json")

    items = _add_user_watch_symbol(123, "tw", "2330.TW")
    items = _add_user_watch_symbol(123, "tw", "6669")
    other_user_items = _user_watchlist(456, "tw")

    assert items == ["2330", "6669"]
    assert other_user_items == []
    rows = [
        {"symbol": "2330", "name": "台積電"},
        {"symbol": "9999", "name": "Other"},
    ]
    assert _filter_watchlist_rows(rows, _user_watchlist(123, "tw")) == [rows[0]]

    items = _remove_user_watch_symbol(123, "tw", "2330")

    assert items == ["6669"]


def test_portfolio_history_can_prepend_latest_signal_day(tmp_path) -> None:
    weights_path = tmp_path / "target_weights.parquet"
    rebalance_path = tmp_path / "rebalance.parquet"
    pl.DataFrame(
        {
            "symbol": ["AAA", "BBB"],
            "name": ["Alpha", "Beta"],
            "target_weight": [0.10, -0.20],
        }
    ).write_parquet(weights_path)
    pl.DataFrame(
        {
            "symbol": ["BBB", "AAA"],
            "name": ["Beta", "Alpha"],
            "action": ["SELL", "BUY"],
            "current_weight": [0.0, 0.05],
            "target_weight": [-0.20, 0.10],
            "delta_weight": [-0.20, 0.05],
            "trade_price": [20.0, 10.0],
            "price_return": [-0.10, 0.10],
        }
    ).write_parquet(rebalance_path)
    summary_path = tmp_path / "summary.json"
    summary_path.write_text("{}", encoding="utf-8")
    result = SimpleNamespace(
        rows=[
            {
                "date": "2026-01-05",
                "portfolio_return": 0.05,
                "benchmark_return": 0.02,
                "profit_value": 50.0,
                "changes": [],
                "change_counts": {},
            },
            {
                "date": "2026-01-02",
                "portfolio_return": 0.01,
                "benchmark_return": 0.00,
                "profit_value": 10.0,
                "changes": [],
                "change_counts": {},
            },
        ],
        source_paths=(),
        days=2,
        top_changes=1,
        start_date="2026-01-02",
        end_date="2026-01-05",
        period_return=1.01 * 1.05 - 1.0,
        benchmark_return=0.02,
        profit_value=60.0,
        capital=SimpleNamespace(capital=1_000.0),
    )
    summary = {
        "asof_date": "2026-01-06",
        "portfolio_simple_return": 0.03,
        "benchmark_simple_return": 0.01,
        "turnover": 0.20,
        "display_capital": 1_000.0,
        "target_risk": {"gross": 0.30, "net": -0.10, "long_gross": 0.10, "short_gross": 0.20},
        "weights_path": str(weights_path),
        "rebalance_path": str(rebalance_path),
    }

    inserted = _prepend_latest_signal_row_to_portfolio_history(
        result,
        summary_path=summary_path,
        summary=summary,
        max_rows=2,
    )

    assert inserted is True
    assert [row["date"] for row in result.rows] == ["2026-01-06", "2026-01-05"]
    assert result.start_date == "2026-01-05"
    assert result.end_date == "2026-01-06"
    assert result.days == 2
    assert np.isclose(result.period_return, 1.05 * 1.03 - 1.0)
    assert np.isclose(result.benchmark_return, 1.02 * 1.01 - 1.0)
    assert np.isclose(result.profit_value, 80.0)
    assert result.rows[0]["position_count"] == 2
    assert result.rows[0]["long_count"] == 1
    assert result.rows[0]["short_count"] == 1
    assert result.rows[0]["change_count"] == 2
    assert result.rows[0]["changes"][0]["symbol"] == "BBB"
    assert result.rows[0]["changes"][0]["current_weight"] == 0.0
    assert result.rows[0]["changes"][0]["target_weight"] == -0.20
    assert result.rows[0]["changes"][0]["stock_return"] == 0.0
    assert result.rows[0]["changes"][0]["portfolio_contribution"] == 0.0
    assert "shares" not in result.rows[0]["changes"][0]
    assert summary_path in result.source_paths


def test_portfolio_history_prepend_uses_panel_data_date_before_asof(tmp_path) -> None:
    result = SimpleNamespace(
        rows=[{"date": "2026-06-23", "changes": [], "change_counts": {}}],
        end_date="2026-06-23",
    )
    summary = {
        "asof_date": "2026-06-23 11:42:00",
        "panel_data_date": "2026-06-23",
    }

    inserted = _prepend_latest_signal_row_to_portfolio_history(
        result,
        summary_path=tmp_path / "summary.json",
        summary=summary,
        max_rows=2,
    )

    assert inserted is False
    assert [row["date"] for row in result.rows] == ["2026-06-23"]


def test_portfolio_history_prepend_keeps_panel_display_time(tmp_path) -> None:
    result = SimpleNamespace(
        rows=[{"date": "2026-06-23", "portfolio_return": 0.0, "benchmark_return": 0.0, "profit_value": 0.0}],
        source_paths=(),
        days=1,
        top_changes=1,
        start_date="2026-06-23",
        end_date="2026-06-23",
        period_return=0.0,
        benchmark_return=0.0,
        profit_value=0.0,
        capital=SimpleNamespace(capital=None),
    )
    summary = {
        "asof_date": "2026-06-24 10:53:08",
        "panel_date": "2026-06-24 13:30:00",
        "panel_data_date": "2026-06-24 00:00:00",
        "data_timezone": "Asia/Taipei",
        "display_timezone": "Asia/Taipei",
        "target_risk": {"gross": 0.0, "net": 0.0, "long_gross": 0.0, "short_gross": 0.0},
    }

    inserted = _prepend_latest_signal_row_to_portfolio_history(
        result,
        summary_path=tmp_path / "summary.json",
        summary=summary,
        max_rows=2,
    )

    assert inserted is True
    assert result.rows[0]["date"] == "2026-06-24 00:00:00"
    assert result.rows[0]["display_date"] == "2026-06-24 13:30:00"


def test_portfolio_change_line_omits_missing_live_signal_shares() -> None:
    line = _portfolio_change_line(
        {
            "symbol": "AAA",
            "name": "Alpha",
            "action": "BUY",
            "holding_ratio_delta": 0.05,
            "holding_ratio": 0.10,
            "market_value": 100.0,
            "market_value_delta": 50.0,
            "price": 10.0,
        }
    )

    assert "shares=" not in line
    assert "Δsh=" not in line
    assert "Δhold=+5.00%" in line


def test_position_adjusted_returns_show_stock_pnl_and_portfolio_contribution() -> None:
    long_line = _portfolio_change_line(
        {
            "symbol": "LONG",
            "action": "HOLD",
            "holding_ratio": 0.50,
            "holding_ratio_delta": 0.0,
            "price_return": 0.10,
        }
    )
    short_line = _portfolio_change_line(
        {
            "symbol": "SHORT",
            "action": "HOLD",
            "holding_ratio": -0.50,
            "holding_ratio_delta": 0.0,
            "price_return": -0.10,
        }
    )
    flat_line = _portfolio_change_line(
        {
            "symbol": "FLAT",
            "action": "HOLD",
            "holding_ratio": 0.0,
            "holding_ratio_delta": 0.0,
            "price_return": 0.10,
        }
    )

    assert "stock_ret=+10.00%" in long_line
    assert "pnl_contrib=+5.00%" in long_line
    assert "stock_ret=+10.00%" in short_line
    assert "pnl_contrib=+5.00%" in short_line
    assert "stock_ret=0.00%" in flat_line
    assert "pnl_contrib=0.00%" in flat_line


def test_portfolio_change_line_uses_previous_position_for_exit_short_pnl() -> None:
    line = _portfolio_change_line(
        {
            "symbol": "SHORT",
            "action": "EXIT_SHORT",
            "holding_ratio": 0.0,
            "prev_holding_ratio": -0.50,
            "holding_ratio_delta": 0.50,
            "market_value": 0.0,
            "market_value_delta": 500.0,
            "shares": 0,
            "share_delta": 5,
            "price": 90.0,
            "price_return": -0.10,
        }
    )

    assert "EXIT_SHORT" in line
    assert "hold=0.00%" in line
    assert "stock_ret=+10.00%" in line
    assert "pnl_contrib=+5.00%" in line


def test_portfolio_history_block_wraps_change_rows_for_readability() -> None:
    block = _portfolio_history_block(
        {
            "date": "2026-06-24",
            "display_date": "2026-06-24 13:30:00",
            "portfolio_return": 0.0515,
            "benchmark_return": -0.0339,
            "profit_value": None,
            "cumulative_return": 0.0753,
            "turnover": 1.3671,
            "nav": None,
            "gross_ratio": 0.465,
            "net_ratio": 0.3586,
            "cash_ratio": 0.535,
            "position_count": 3,
            "long_count": 2,
            "short_count": 1,
            "change_count": 1,
            "change_counts": {"EXIT": 1},
            "changes": [
                {
                    "symbol": "6669",
                    "name": "緯穎",
                    "action": "EXIT",
                    "holding_ratio_delta": 0.9021,
                    "holding_ratio": 0.0,
                    "prev_holding_ratio": -0.9021,
                    "price_return": -0.0515,
                    "stock_return": 0.0515,
                    "portfolio_contribution": 0.0464,
                    "price": 4605.0,
                }
            ],
        }
    )

    assert "`2026-06-24 13:30:00`" in block
    assert "1. `6669` 緯穎 **EXIT**" in block
    assert "\n       `Δhold=+90.21%`" in block
    assert max(len(line) for line in block.splitlines()) < 120


def test_history_headers_hide_internal_details_until_debug(tmp_path) -> None:
    cfg = SimpleNamespace(
        market="tw",
        history_frequency="daily",
        timezone="Asia/Taipei",
        display_timezone="Asia/Taipei",
    )
    capital = SimpleNamespace(mode="artifact", capital=None, reference_date="2026-06-24")
    stock_result = SimpleNamespace(
        symbol="6924",
        name="榮惠-KY創",
        requested_symbol="6924",
        rows=[{}],
        changes_only=True,
        fell_back_to_all_rows=False,
        fold_dir=tmp_path / "fold_25",
        source_paths=(tmp_path / "holdings.parquet",),
        capital=capital,
    )
    portfolio_result = SimpleNamespace(
        rows=[
            {"date": "2026-06-24 00:00:00", "display_date": "2026-06-24 13:30:00"},
            {"date": "2026-06-22", "display_date": "2026-06-22"},
        ],
        days=2,
        frequency="daily",
        top_changes=5,
        start_date="2026-06-22",
        end_date="2026-06-24",
        period_return=0.02,
        benchmark_return=0.01,
        profit_value=100.0,
        fold_dir=tmp_path / "fold_25",
        source_paths=(tmp_path / "holdings.parquet",),
        capital=capital,
    )

    stock_normal = "\n".join(_stock_history_header_lines(cfg, stock_result, debug=False))
    portfolio_normal = "\n".join(_portfolio_history_header_lines(cfg, portfolio_result, debug=False))
    stock_debug = "\n".join(_stock_history_header_lines(cfg, stock_result, debug=True))
    portfolio_debug = "\n".join(_portfolio_history_header_lines(cfg, portfolio_result, debug=True))

    for text in (stock_normal, portfolio_normal):
        assert "sources:" not in text
        assert "fold=" not in text
        assert "display_tz=" not in text
        assert "capital_mode=" not in text
        assert "capital=artifact" not in text
    assert "period=2026-06-22..2026-06-24 13:30:00" in portfolio_normal
    assert "2026-06-24 00:00:00" not in portfolio_normal
    assert "sources:" in stock_debug
    assert "fold=" in stock_debug
    assert "display_tz=" in stock_debug
    assert "capital_mode=" in stock_debug
    assert "sources:" in portfolio_debug
    assert "fold=" in portfolio_debug
    assert "display_tz=" in portfolio_debug
    assert "capital_mode=" in portfolio_debug


def test_decision_overview_hides_artifact_details_until_debug(tmp_path) -> None:
    summary_path = tmp_path / "summary.json"
    explain_path = tmp_path / "decision_explanations.parquet"
    summary = {
        "signal_id": "sig-test",
        "market": "tw",
        "asof_date": "2026-06-24 13:30:00",
        "panel_date": "2026-06-24 13:30:00",
        "fold_id": 25,
        "display_timezone": "Asia/Taipei",
        "display_timezone_label": "UTC+8 台北",
        "decision_report_path": str(tmp_path / "decision_report.md"),
        "model_explanation": {
            "confidence_proxy_score_std": 0.1234,
            "source": "internal score/weight decision table",
            "top_score_drivers": [{"symbol": "AAA", "name": "Alpha", "score": 1.0, "target_weight": 0.1}],
            "top_feature_drivers": [{"feature": "close_logret_1d", "weighted_abs_value": 0.5}],
        },
    }
    rows_all = [
        {"symbol": "AAA", "action": "BUY"},
        {"symbol": "BBB", "action": "HOLD"},
    ]
    rows_filtered = [{"symbol": "AAA", "action": "BUY"}]

    normal = _decision_overview_page(
        summary=summary,
        summary_path=summary_path,
        explain_path=explain_path,
        rows_all=rows_all,
        rows_filtered=rows_filtered,
        symbol="",
        action="actionable",
        sort_by="delta",
        debug=False,
    )
    debug = _decision_overview_page(
        summary=summary,
        summary_path=summary_path,
        explain_path=explain_path,
        rows_all=rows_all,
        rows_filtered=rows_filtered,
        symbol="",
        action="actionable",
        sort_by="delta",
        debug=True,
    )

    assert "score drivers:" in normal
    assert "feature drivers:" in normal
    assert "fold=" not in normal
    assert "display_tz=" not in normal
    assert "source=" not in normal
    assert "**files**" not in normal
    assert "fold=25" in debug
    assert "display_tz=UTC+8 台北" in debug
    assert "source=internal score/weight decision table" in debug
    assert "**files**" in debug


def test_daily_summary_hides_artifact_details_until_debug(monkeypatch, tmp_path) -> None:
    cfg = SimpleNamespace(
        market="tw",
        label="台股",
        timezone="Asia/Taipei",
        display_timezone="Asia/Taipei",
    )
    status = SimpleNamespace(
        status="ok",
        market_open=True,
        market_open_reason="open",
        cfg=cfg,
        data=SimpleNamespace(
            fresh=True,
            last_data_date="2026-06-24 13:30:00",
            panel_date="2026-06-24 13:30:00",
            benchmark_date="2026-06-24 13:30:00",
        ),
    )
    summary_path = tmp_path / "summary.json"
    summary = {
        "signal_id": "sig-test",
        "asof_date": "2026-06-24 13:30:00",
        "panel_date": "2026-06-24 13:30:00",
        "fold_id": 25,
        "portfolio_simple_return": 0.01,
        "benchmark_simple_return": 0.002,
        "turnover": 0.10,
    }

    monkeypatch.setattr("services.discord_bot.bot._runtime_status", lambda cfg: status)
    monkeypatch.setattr("services.discord_bot.bot._latest_market_signal", lambda cfg: (summary_path, summary))

    normal = _daily_summary_message(cfg, debug=False)
    debug = _daily_summary_message(cfg, debug=True)

    assert "latest signal" in normal
    assert "display_tz=" not in normal
    assert "signal=sig-test" not in normal
    assert "fold=25" not in normal
    assert "artifact:" not in normal
    assert "display_tz=UTC+8 台北" in debug
    assert "signal=sig-test" in debug
    assert "fold=25" in debug
    assert "artifact:" in debug


def test_live_signal_lines_use_current_weight_for_pnl_direction() -> None:
    row = {
        "symbol": "SHORT",
        "action": "SELL",
        "current_weight": -0.25,
        "target_weight": -0.50,
        "delta_weight": -0.25,
        "current_price": 98.0,
        "trade_price": 98.0,
        "price_return": -0.02,
    }

    position = _position_line(row)
    rebalance = _rebalance_line(row)

    assert "`stock_ret=+2.00%`" in position
    assert "`pnl_contrib=+0.50%`" in position
    assert "`stock_ret=+2.00%`" in rebalance
    assert "`pnl_contrib=+0.50%`" in rebalance


def test_signal_enrichment_adds_capital_pnl_and_crypto_window_label() -> None:
    cfg = SimpleNamespace(
        market="crypto",
        current_capital=500_000.0,
        initial_capital=None,
        benchmark_window_days=32,
        history_frequency="bar",
        config_path="configs/markets/crypto.yaml",
    )
    result = SimpleNamespace(
        summary={
            "market": "crypto",
            "market_label": "加密貨幣",
            "signal_id": "sig-test",
            "asof_date": "2026-06-22 00:15:00",
            "panel_date": "2026-06-22 00:15:00",
            "previous_weights_date": "2026-06-22 00:00:00",
            "portfolio_simple_return": 0.012,
            "benchmark_simple_return": 0.004,
            "turnover": 0.1,
            "estimated_trade_cost": 0.0005,
            "recent_performance": {
                "window_days": 32,
                "strategy_return": 0.10,
                "benchmark_return": 0.02,
                "excess_return": 0.08,
            },
        },
        message="",
        output_dir=None,
    )

    enriched = _enrich_signal_performance_for_discord(cfg, result, max_rows=0)

    assert enriched.summary["display_capital"] == 500_000.0
    assert enriched.summary["portfolio_pnl_value"] == 6_000.0
    assert enriched.summary["benchmark_pnl_value"] == 2_000.0
    assert enriched.summary["excess_pnl_value"] == 4_000.0
    assert enriched.summary["recent_performance"]["window_label"] == "過去32根15m"
    assert enriched.summary["recent_performance"]["strategy_pnl_value"] == 50_000.0
    assert "上個訊號到現在" in enriched.message
    assert "`baseline=+0.40%`" in enriched.message
    assert "`capital=500,000`" in enriched.message
    assert "`pnl=+6,000`" in enriched.message
    assert "過去32根15m" in enriched.message
    assert "`baseline_pnl=+10,000`" in enriched.message
