from __future__ import annotations

from types import SimpleNamespace

import numpy as np
import polars as pl

from services.discord_bot.bot import (
    _enrich_signal_performance_for_discord,
    _portfolio_change_line,
    _prepend_latest_signal_row_to_portfolio_history,
    _scheduled_detail_page_groups,
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

    assert "signal_now current / target positions" in position_pages[0]
    assert "signal_now rebalance" in rebalance_pages[0]
    assert "signal_now decision explanations" in decision_pages[0]
    assert "`rows=1`" in decision_pages[0]
    assert "`AAA` Alpha **BUY**" in decision_pages[0]
    assert "`BBB`" not in decision_pages[0]


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
    assert "shares" not in result.rows[0]["changes"][0]
    assert summary_path in result.source_paths


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
