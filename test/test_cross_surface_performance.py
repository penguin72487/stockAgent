from datetime import date, datetime, timedelta
import json
import math
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import polars as pl
import pytest

from stockagent.live.performance_contract import (
    PERFORMANCE_SCHEMA_VERSION, log_to_simple_return, paper_account_performance, simple_return_frame,
)
from stockagent.live.portfolio_history import _period_total_return, _with_day_trade_close_nav
from stockagent.live.stock_history import _read_returns
from stockagent.live.tw_day_trade_dashboard import build_dashboard_snapshot
from stockagent.live.tw_day_trade_simulation import TwDayTradeSimulationEngine
from stockagent.live.report_formatter import format_signal_message
from stockagent.live.market_status import cumulative_recent_returns
from services.discord_bot import bot


def test_log_boundary_is_explicit_idempotent_and_ruin_safe():
    raw = pl.DataFrame({"portfolio_return": [math.log(1.1), -math.inf, None], "benchmark_return": [0.0, 0.0, None]})
    decoded = simple_return_frame(raw)
    assert decoded["portfolio_return"].to_list() == pytest.approx([0.1, -1.0, None])
    assert decoded.equals(simple_return_frame(decoded))
    assert log_to_simple_return(-math.inf) == -1.0
    assert log_to_simple_return(1e-18) == pytest.approx(1e-18, abs=1e-30)
    for value in [math.nan, math.inf]:
        with pytest.raises(ValueError):
            simple_return_frame(pl.DataFrame({"portfolio_return": [value]}))
    with pytest.raises(ValueError):
        simple_return_frame(raw.with_columns(pl.lit("percent").alias("return_type")))


def test_discord_stock_history_and_settlement_share_log_contract(tmp_path, monkeypatch):
    logs = [math.log(1.1), math.log(0.8)]
    pl.DataFrame({"date": ["2026-09-08", "2026-09-09"], "portfolio_return": logs,
                  "benchmark_return": [0.0, 0.0]}).write_parquet(tmp_path / "daily_portfolio_returns.parquet")
    monkeypatch.setattr(bot, "_formal_returns_artifact_path", lambda cfg: tmp_path / "daily_portfolio_returns.parquet")
    monkeypatch.setattr(bot, "_recent_market_signal_metrics", lambda *a, **kw: pytest.fail("day trade must not scan preview returns"))
    recent = bot._recent_performance_from_returns(SimpleNamespace(day_trade_simulation_enabled=True), 32)
    rows, _ = _read_returns(tmp_path)
    assert recent["strategy_return"] == pytest.approx(-0.12)
    assert cumulative_recent_returns(tmp_path, window_days=32)["strategy_return"] == pytest.approx(recent["strategy_return"])
    assert recent["strategy_return"] == pytest.approx(_period_total_return(rows.to_dicts(), "portfolio_return"))
    assert recent["return_type"] == "simple"
    assert recent["source"] == "model_backtest_returns"
    daily = rows.with_columns(pl.Series("nav", [1000.0, 1100.0]))
    ledger = pl.DataFrame({"date": ["2026-09-08", "2026-09-09"], "close_nav": [1100.0, 880.0]})
    valued = _with_day_trade_close_nav(daily, ledger)
    assert valued["close_nav"].to_list() == pytest.approx(valued["ledger_close_nav"].to_list())
    expected_risk = bot._risk_adjusted_metrics_from_simple_returns([0.1, -0.2])
    for key in ["sharpe", "sortino", "max_drawdown"]:
        assert recent[key] == pytest.approx(expected_risk[key])


def test_old_semantics_cache_is_rejected(monkeypatch):
    monkeypatch.setattr(bot, "_discord_performance_revision", lambda cfg: {})
    summary = {"discord_presentation_schema_version": 1, "recent_performance": {}, "discord_performance_revision": {}}
    assert not bot._has_current_discord_performance_snapshot(SimpleNamespace(), summary)
    summary["discord_presentation_schema_version"] = PERFORMANCE_SCHEMA_VERSION
    assert bot._has_current_discord_performance_snapshot(SimpleNamespace(), summary)


def test_failed_refresh_does_not_certify_old_wrong_numbers(monkeypatch):
    cfg = SimpleNamespace(market="unit", initial_capital=None, current_capital=None, benchmark_window_days=32)
    monkeypatch.setattr(bot, "_refresh_summary_recent_performance_from_history", lambda *_a, **_k: False)
    monkeypatch.setattr(bot, "_discord_performance_revision", lambda *_: {})
    enriched = bot._summary_with_capital_context(cfg, {"recent_performance": {"strategy_return": 9.99}})
    assert enriched["recent_performance"]["status"] == "unavailable"
    assert "strategy_return" not in enriched["recent_performance"]
    assert not bot._has_current_discord_performance_snapshot(cfg, enriched)


def test_producer_and_discord_select_the_same_execution_return_source(tmp_path, monkeypatch):
    for stem, value in [("daily_portfolio_returns", 0.05), ("integer_share_daily_portfolio_returns", 0.1)]:
        pl.DataFrame({"date": ["2026-09-09"], "portfolio_return": [math.log1p(value)], "benchmark_return": [0.0]}).write_parquet(tmp_path / (stem + ".parquet"))
    monkeypatch.setattr(bot, "_market_fold_dir", lambda _: tmp_path)
    cfg = SimpleNamespace(market="unit", config_path="unused", day_trade_simulation_enabled=True)
    for conversion, expected in [(False, 0.1), (True, 0.05)]:
        monkeypatch.setattr(bot, "_load_experiment_config_cached", lambda _: SimpleNamespace(trading=SimpleNamespace(execution_mode="tw_day_trade", tw_day_trade_unlimited_margin_conversion=conversion)))
        result = bot._recent_performance_from_returns(cfg, 32)
        producer = cumulative_recent_returns(tmp_path, window_days=32, prefer_integer=not conversion)
        assert result["strategy_return"] == pytest.approx(expected)
        assert result["strategy_return"] == pytest.approx(producer["strategy_return"])


def test_risk_does_not_hide_total_loss():
    risk = bot._risk_adjusted_metrics_from_simple_returns([-1.0, 0.1])
    assert risk["max_drawdown"] == -1.0
    assert risk["ruined"]
    assert risk["sharpe"] is None


def test_discord_account_matches_web_and_refreshes_without_return_scan(tmp_path, monkeypatch):
    market = "tw_day_trade_unit"
    mode = {"market": market, "session_date": "2026-09-09", "initial_capital_twd": 1000.0,
            "total_equity_twd": 1250.0, "last_mark_at": "2026-09-09T13:30:02+08:00", "positions": {}}
    state = {"enabled_markets": [market], "modes": {market: mode}, "state_revision": 10,
             "updated_at": "2026-09-09T13:30:02+08:00"}
    (tmp_path / "state.json").write_text(json.dumps(state))
    (tmp_path / "status.json").write_text(json.dumps({"updated_at": state["updated_at"]}))
    for name in ["signals", "orders", "fills", "marks", "benchmark_marks", "events"]:
        (tmp_path / (name + ".jsonl")).write_text("")
    cfg = SimpleNamespace(market=market, current_capital=None, initial_capital=1000.0,
                          day_trade_simulation_enabled=True, day_trade_simulation_state_dir=str(tmp_path))
    monkeypatch.setattr(bot, "_has_current_discord_performance_snapshot", lambda *_: True)
    monkeypatch.setattr(bot, "_discord_performance_revision", lambda *_: {})
    monkeypatch.setattr(bot, "_refresh_summary_recent_performance_from_history", lambda *_a, **_k: pytest.fail("hot path history scan"))
    summary = {"asof_date": "2026-09-09", "recent_performance": {"window_days": 32, "end_date": "2026-08-19"}}
    enriched = bot._summary_with_capital_context(cfg, summary, reuse_current_performance_snapshot=True)
    web = build_dashboard_snapshot(state_dir=tmp_path, now=datetime.fromisoformat("2026-09-09T13:31:00+08:00"), include_ledger_session_dates=False)
    assert enriched["account_performance"] == web["modes"][0]["account_performance"]
    assert enriched["account_performance"]["return_fraction"] == pytest.approx(0.25)
    assert not enriched["recent_performance"]["through_signal_date"]
    state["modes"][market]["total_equity_twd"] = 1300.0
    state["state_revision"] = 11
    (tmp_path / "state.json").write_text(json.dumps(state))
    updated = bot._summary_with_capital_context(cfg, enriched, reuse_current_performance_snapshot=True)
    assert updated["account_performance"]["return_fraction"] == pytest.approx(0.30)
    assert updated["display_capital"] == 1000.0  # Explicitly different sizing reference.
    historical_mark = {"market": market, "session_date": "2026-09-08", "minute": "2026-09-08T13:30+08:00",
                       "recorded_at": "2026-09-08T13:30:02+08:00", "initial_capital_twd": 1000.0,
                       "total_equity_twd": 1200.0, "stale_position_count": 0}
    (tmp_path / "marks.jsonl").write_text(json.dumps(historical_mark) + "\n")
    historical = build_dashboard_snapshot(state_dir=tmp_path, session_date="2026-09-08",
        now=datetime.fromisoformat("2026-09-09T13:31:00+08:00"), include_ledger_session_dates=True)
    account = historical["modes"][0]["account_performance"]
    assert account["asof"] == "2026-09-08T13:30+08:00"
    assert account["state_revision"] is None
    assert account["return_fraction"] == pytest.approx(0.2)


@pytest.mark.parametrize("clock", ["13:30:02", "13:30:59", "14:35:00"])
def test_terminal_minute_is_catchup_safe_and_idempotent(tmp_path, clock):
    engine = TwDayTradeSimulationEngine(tmp_path)
    engine.state["modes"] = {"unit": {"session_date": "2026-09-09", "initial_capital_twd": 1000.0,
        "cumulative_realized_net_pnl_twd": 5.0, "positions": {}, "closing_auction_settled_at": "2026-09-09T13:30:02+08:00"}}
    now = datetime.fromisoformat("2026-09-09T" + clock + "+08:00")
    engine._mark_mode("unit", now, {})
    engine._mark_mode("unit", now, {})
    marks = [json.loads(line) for line in engine.marks_path.read_text().splitlines()]
    assert len(marks) == 1
    assert marks[0]["minute"] == "2026-09-09T13:30+08:00"
    assert marks[0]["recorded_at"] == now.isoformat()
    assert marks[0]["total_equity_twd"] == 1005.0
    engine._persist(now)
    restarted = TwDayTradeSimulationEngine(tmp_path)
    restarted._mark_mode("unit", now, {})
    assert len(engine.marks_path.read_text().splitlines()) == 1


def test_terminal_catchup_cannot_relabel_other_session_or_unsettled_account(tmp_path):
    engine = TwDayTradeSimulationEngine(tmp_path)
    engine.state["modes"] = {"unit": {"session_date": "2026-09-09", "initial_capital_twd": 1000.0, "positions": {}}}
    engine._mark_mode("unit", datetime.fromisoformat("2026-09-09T14:00:00+08:00"), {})
    engine.state["modes"]["unit"]["closing_auction_settled_at"] = "2026-09-09T13:30:02+08:00"
    engine._mark_mode("unit", datetime.fromisoformat("2026-09-10T13:30:02+08:00"), {})
    assert not engine.marks_path.exists()


def test_formatter_separates_account_model_history_and_signal_product():
    account = paper_account_performance({"initial_capital_twd": 1000, "total_equity_twd": 1250,
        "session_date": "2026-09-09", "last_mark_at": "2026-09-09T13:30:02+08:00"}, revision=10)
    text = format_signal_message({"market": "tw_day_trade_unit", "signal_id": "example",
        "signal_price_contract": {"model_observation": "intraday_latest_quote"}, "account_performance": account,
        "recent_performance": {"strategy_return": 0.01, "start_date": "2026-07-06", "end_date": "2026-08-19", "through_signal_date": False}}, max_rows=0)
    assert "盤中即時重估" in text
    assert "模擬帳戶累積績效" in text
    assert "模型歷史回測" in text
    assert "2026-08-19" in text
    assert "+25.00%" in text


def test_flat_accounts_have_an_independent_minute_clock(tmp_path):
    from scripts.run_tw_day_trade_simulation import _flat_mark_markets_due
    engine = TwDayTradeSimulationEngine(tmp_path)
    engine.state["modes"] = {"unit": {"session_date": "2026-09-09", "initial_capital_twd": 1000.0, "positions": {}}}
    observed = datetime.fromisoformat("2026-09-09T09:05:02+08:00")
    assert _flat_mark_markets_due(engine, observed=observed, last_mark_minute=None) == ["unit"]
    assert _flat_mark_markets_due(engine, observed=observed, last_mark_minute="2026-09-09T09:05+08:00") == []
    closing = datetime.fromisoformat("2026-09-09T14:00:00+08:00")
    assert _flat_mark_markets_due(engine, observed=closing, last_mark_minute=None) == ["unit"]
    engine.process_quotes(quotes={}, now=closing)
    assert _flat_mark_markets_due(engine, observed=closing, last_mark_minute=None) == []


def _terminal_recovery_fixture(tmp_path):
    day = "2026-09-07"
    def stamp(clock):
        return f"{day}T{clock}+08:00"
    position = {"position_id": "p", "signed_shares": 0, "filled_shares": 1, "realized_net_pnl_twd": 3.0}
    state = {"modes": {"unit": {"session_date": day, "positions": {"p": position}, "total_equity_twd": 108.0}}}
    (tmp_path / "state.json").write_text(json.dumps(state))
    fills = [{"market": "unit", "session_date": day, "position_id": "p", "quantity": 1, "purpose": "entry",
              "recorded_at": stamp("09:00:02"), "fill_at": stamp("09:00:02")},
             {"market": "unit", "session_date": day, "position_id": "p", "quantity": 1, "purpose": "exit",
              "recorded_at": stamp("13:30:02"), "fill_at": stamp("13:30:02"), "net_pnl_twd": 3.0}]
    (tmp_path / "fills.jsonl").write_text("\n".join(json.dumps(x) for x in fills))
    (tmp_path / "events.jsonl").write_text(json.dumps({"market": "unit", "event": "closing_auction_settled", "recorded_at": stamp("13:30:02")}))
    rows = [{"session_date": day, "market": "unit", "minute": (datetime.fromisoformat(stamp("09:01:00")) + timedelta(minutes=i)).isoformat(timespec="minutes"),
             "recorded_at": (datetime.fromisoformat(stamp("09:01:00")) + timedelta(minutes=i)).isoformat(),
             "initial_capital_twd": 100.0, "cumulative_realized_net_pnl_twd": 5.0, "total_equity_twd": 105.0,
             "open_position_count": 1, "open_net_liquidation_pnl_twd": 0.0} for i in range(269)]
    return rows, fills


def test_terminal_repair_uses_fills_and_rejects_invented_prices(tmp_path):
    from scripts.rebuild_tw_day_trade_minute_curves import recover_terminal_marks
    rows, _ = _terminal_recovery_fixture(tmp_path)
    rows.append({**rows[-1], "minute": "2026-09-08T09:01+08:00", "open_position_count": 0, "total_equity_twd": 108.0})
    repaired, receipt = recover_terminal_marks(tmp_path, rows, start=date(2026, 9, 7), end=date(2026, 9, 7))
    assert len(repaired) == 270
    assert repaired[-1]["total_equity_twd"] == 108.0
    assert repaired[-1]["valuation_source"] == "accepted_closed_fill_ledger"
    assert len(receipt["removed_cross_session_duplicates"]) == 1
    again, receipt = recover_terminal_marks(tmp_path, repaired, start=date(2026, 9, 7), end=date(2026, 9, 7))
    assert again == repaired
    assert not receipt["recovered_endpoints"]


@pytest.mark.parametrize("defect", ["late_fill", "wrong_pnl", "missing_settlement", "wrong_anchor", "unfilled"])
def test_terminal_repair_fails_closed_on_inconsistent_evidence(tmp_path, defect):
    from scripts.rebuild_tw_day_trade_minute_curves import recover_terminal_marks
    rows, fills = _terminal_recovery_fixture(tmp_path)
    if defect == "late_fill":
        fills[-1]["fill_at"] = "2026-09-07T13:31:00+08:00"
    elif defect == "wrong_pnl":
        fills[-1]["net_pnl_twd"] = 99.0
    elif defect == "missing_settlement":
        (tmp_path / "events.jsonl").write_text("")
    elif defect == "wrong_anchor":
        rows[-1]["cumulative_realized_net_pnl_twd"] = 99.0
    else:
        fills[-1]["quantity"] = 0
    (tmp_path / "fills.jsonl").write_text("\n".join(json.dumps(x) for x in fills))
    with pytest.raises(RuntimeError):
        recover_terminal_marks(tmp_path, rows, start=date(2026, 9, 7), end=date(2026, 9, 7))
