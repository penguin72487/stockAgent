from __future__ import annotations

import sys
from types import SimpleNamespace
from datetime import datetime
from zoneinfo import ZoneInfo

import numpy as np
import polars as pl

from services.discord_bot.bot import (
    _add_user_watch_symbol,
    _auto_signal_price_source,
    _artifact_backfill_key,
    _can_reuse_latest_signal_now,
    _ConsoleProgress,
    _decision_overview_page,
    _daily_summary_message,
    _ensure_signal_ready,
    _filter_watchlist_rows,
    _guide_message,
    _latest_changes_pages,
    _latest_signal_message,
    _market_notice,
    _market_artifact_backfill_time,
    _market_has_live_signal_for_date,
    _validate_pre_signal_download_artifacts,
    BotUserError,
    _performance_message,
    _enrich_signal_performance_for_discord,
    _position_line,
    _portfolio_change_line,
    _portfolio_history_header_lines,
    _portfolio_history_block,
    _prepare_realtime_signal_sync,
    _include_live_signals_in_portfolio_history,
    _prepend_latest_signal_row_to_portfolio_history,
    _public_broadcasts_enabled,
    _rebalance_line,
    _remove_user_watch_symbol,
    _resolve_pre_signal_command,
    _remove_user_subscription,
    _replace_user_watch_symbol,
    _risk_message,
    _scheduled_detail_page_groups,
    _scheduled_markets,
    _scheduled_retry_allowed,
    _mark_scheduled_retry,
    _clear_scheduled_retry,
    _set_user_subscription,
    _signal_now_should_refresh_data,
    _summary_age_seconds,
    _signal_kwargs,
    _signal_sanity_issues,
    _signal_sanity_level,
    _subscription_alert_pages,
    _subscription_summary_lines,
    _subscribed_users_for_market,
    _stock_history_header_lines,
    _summary_with_capital_context,
    _user_subscriptions,
    _user_watchlist,
    _watch_crash_delay_seconds,
    _watch_delay_seconds,
    _watch_poll_seconds,
)


def test_pre_signal_python_sentinel_uses_running_interpreter() -> None:
    command = _resolve_pre_signal_command(("{python}", "downloader/example.py", "--mode", "incremental"))

    assert command == [sys.executable, "downloader/example.py", "--mode", "incremental"]


def test_scheduled_markets_defaults_to_all_configured_markets(monkeypatch) -> None:
    monkeypatch.delenv("STOCKAGENT_SCHEDULED_MARKETS", raising=False)
    monkeypatch.setattr(
        "services.discord_bot.bot._market_configs",
        lambda: {"tw": object(), "crypto": object(), "us": object()},
    )

    assert _scheduled_markets() == ["crypto", "tw", "us"]


def test_scheduled_markets_respects_explicit_env(monkeypatch) -> None:
    monkeypatch.setenv("STOCKAGENT_SCHEDULED_MARKETS", "tw,crypto")
    monkeypatch.setattr(
        "services.discord_bot.bot._market_configs",
        lambda: {"tw": object(), "crypto": object(), "us": object()},
    )

    assert _scheduled_markets() == ["tw", "crypto"]


def test_public_broadcasts_default_to_disabled(monkeypatch) -> None:
    monkeypatch.delenv("STOCKAGENT_PUBLIC_BROADCASTS", raising=False)

    assert not _public_broadcasts_enabled()


def test_public_broadcasts_can_be_enabled(monkeypatch) -> None:
    monkeypatch.setenv("STOCKAGENT_PUBLIC_BROADCASTS", "1")

    assert _public_broadcasts_enabled()


def test_replace_user_watch_symbol_updates_or_adds(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr("services.discord_bot.bot.STATE_PATH", tmp_path / "state.json")
    items = _add_user_watch_symbol(123, "tw", "2330")

    assert items == ["2330"]

    items = _replace_user_watch_symbol(123, "tw", "2330", "2317")

    assert items == ["2317"]

    items = _replace_user_watch_symbol(123, "tw", "9999", "0050")

    assert items == ["2317", "0050"]


def test_artifact_backfill_key_uses_backfill_time_and_skips_interval_markets(monkeypatch) -> None:
    monkeypatch.setattr("services.discord_bot.bot._market_state", lambda market: {})
    daily_cfg = SimpleNamespace(
        market="tw",
        data_ready_time="18:00",
        close_time="13:30",
        summary_time="14:00",
        schedule_time="13:15",
        schedule_interval_minutes=None,
    )
    interval_cfg = SimpleNamespace(
        market="crypto",
        data_ready_time="00:00",
        close_time=None,
        summary_time=None,
        schedule_time=None,
        schedule_interval_minutes=15,
        schedule_delay_seconds=45,
    )
    now = datetime(2026, 7, 6, 13, 30, tzinfo=ZoneInfo("Asia/Taipei"))

    assert _market_artifact_backfill_time(daily_cfg) == "13:30"
    assert _artifact_backfill_key(daily_cfg, now) == "2026-07-06:tw:artifact_backfill"
    assert _artifact_backfill_key(daily_cfg, now.replace(hour=13, minute=29)) is None
    assert _artifact_backfill_key(interval_cfg, now) is None


def test_market_has_live_signal_for_date_uses_summary_data_fields(monkeypatch) -> None:
    cfg = SimpleNamespace(market="tw")
    monkeypatch.setattr(
        "services.discord_bot.bot._recent_market_signal_metrics",
        lambda cfg, max_summaries: [
            (None, {"panel_data_date": "2026-07-05 13:30:00"}),
            (None, {"weights_date": "2026-07-06 13:30:00"}),
        ],
    )

    assert _market_has_live_signal_for_date(cfg, "2026-07-06")
    assert not _market_has_live_signal_for_date(cfg, "2026-07-07")


def test_validate_pre_signal_download_artifacts_rejects_all_failed_download(tmp_path) -> None:
    output_dir = tmp_path / "tw_stocks"
    output_dir.mkdir()
    (output_dir / "download_summary.json").write_text(
        '{"asset_class":"tw_stocks","symbol_count":2307,"row_count":0,"status_counts":{"failed":2307}}',
        encoding="utf-8",
    )
    cfg = SimpleNamespace(market="tw", market_type="tw")
    command = ["python", "downloader/download_yahoo_ohlcv.py", "--output-dir", str(output_dir), "--mode", "daily-update"]

    try:
        _validate_pre_signal_download_artifacts(cfg, command, tmp_path / "pre_signal.log")
    except BotUserError as exc:
        assert "did not produce usable data" in str(exc)
    else:
        raise AssertionError("expected BotUserError")


def test_scheduled_retry_helpers_apply_and_clear_cooldown(monkeypatch) -> None:
    clock = {"now": 100.0}
    retry_after = {}

    monkeypatch.setattr("services.discord_bot.bot.time.monotonic", lambda: clock["now"])
    monkeypatch.setenv("STOCKAGENT_SCHEDULED_RETRY_DELAY_SECONDS", "30")

    assert _scheduled_retry_allowed(retry_after, "2026-07-06:tw")

    _mark_scheduled_retry(retry_after, "2026-07-06:tw")

    assert not _scheduled_retry_allowed(retry_after, "2026-07-06:tw")
    clock["now"] = 130.0
    assert _scheduled_retry_allowed(retry_after, "2026-07-06:tw")

    _clear_scheduled_retry(retry_after, "2026-07-06:tw")
    assert retry_after == {}


def test_prepare_realtime_signal_does_not_refresh_disabled_market(monkeypatch) -> None:
    cfg = SimpleNamespace()
    calls = []

    def fake_ensure_signal_ready(cfg):
        raise RuntimeError("disabled")

    monkeypatch.setattr("services.discord_bot.bot._ensure_signal_ready", fake_ensure_signal_ready)
    monkeypatch.setattr("services.discord_bot.bot._run_pre_signal_command", lambda cfg: calls.append(cfg))

    try:
        _prepare_realtime_signal_sync(cfg, force_refresh=True)
    except RuntimeError:
        pass

    assert calls == []


def test_prepare_realtime_signal_does_not_run_daily_updater_just_because_market_is_open(monkeypatch) -> None:
    cfg = SimpleNamespace(market="tw", market_type="tw", history_frequency="daily", schedule_interval_minutes=None)
    status = SimpleNamespace(market_open=True, data=SimpleNamespace(fresh=True))
    calls = []

    monkeypatch.setattr("services.discord_bot.bot._ensure_signal_ready", lambda cfg: status)
    monkeypatch.setattr("services.discord_bot.bot._run_pre_signal_command", lambda cfg: calls.append(cfg))
    monkeypatch.setattr("services.discord_bot.bot._runtime_status", lambda cfg: status)

    source, resolved_status, refreshed = _prepare_realtime_signal_sync(cfg, requested_price_source="auto", force_refresh=False)

    assert source == "tw"
    assert resolved_status is status
    assert not refreshed
    assert calls == []


def test_prepare_realtime_signal_refreshes_interval_market(monkeypatch) -> None:
    cfg = SimpleNamespace(market="crypto", market_type="crypto", history_frequency="bar", schedule_interval_minutes=15)
    status = SimpleNamespace(market_open=True, data=SimpleNamespace(fresh=True))
    calls = []

    monkeypatch.setattr("services.discord_bot.bot._ensure_signal_ready", lambda cfg: status)
    monkeypatch.setattr("services.discord_bot.bot._run_pre_signal_command", lambda cfg: calls.append(cfg))
    monkeypatch.setattr("services.discord_bot.bot._runtime_status", lambda cfg: status)

    source, resolved_status, refreshed = _prepare_realtime_signal_sync(cfg, requested_price_source="auto", force_refresh=False)

    assert source == "panel"
    assert resolved_status is status
    assert refreshed
    assert calls == [cfg]


def test_can_reuse_latest_signal_now_for_closed_fresh_panel_close() -> None:
    cfg = SimpleNamespace(market="tw")
    status = SimpleNamespace(
        market_open=False,
        data=SimpleNamespace(fresh=True, last_data_date="2026-07-06", panel_date="2026-07-06"),
    )
    summary = {"panel_date": "2026-07-06 13:30:00", "price_source": "panel_close"}

    reusable, reason = _can_reuse_latest_signal_now(cfg, status, summary, requested_price_source="auto")

    assert reusable
    assert reason == "cached_latest_close"


def test_can_reuse_latest_signal_now_rejects_stale_closed_panel() -> None:
    cfg = SimpleNamespace(market="tw")
    status = SimpleNamespace(
        market_open=False,
        data=SimpleNamespace(fresh=False, last_data_date="2026-07-02", panel_date="2026-07-02"),
    )
    summary = {"panel_date": "2026-07-02 13:30:00", "price_source": "panel_close"}

    reusable, _ = _can_reuse_latest_signal_now(cfg, status, summary, requested_price_source="auto")

    assert not reusable


def test_can_reuse_latest_signal_now_rejects_closed_tw_panel_when_today_panel_missing(monkeypatch) -> None:
    cfg = SimpleNamespace(
        market="tw",
        market_type="tw",
        history_frequency="daily",
        display_timezone="Asia/Taipei",
        timezone="Asia/Taipei",
        open_time="00:00",
    )
    status = SimpleNamespace(
        market_open=False,
        data=SimpleNamespace(fresh=True, last_data_date="2000-01-01", panel_date="2000-01-01"),
    )

    panel_summary = {"asof_date": "2026-07-09 14:35:00", "panel_date": "2000-01-01", "price_source": "panel_close"}
    reusable, _ = _can_reuse_latest_signal_now(cfg, status, panel_summary, requested_price_source="auto")
    assert not reusable

    mis_summary = {
        "asof_date": "2026-07-09 14:35:00",
        "panel_date": "2000-01-01",
        "price_source": "twse_tpex:mis",
        "price_available_count": 2000,
    }
    monkeypatch.setattr("services.discord_bot.bot._summary_age_seconds", lambda summary, cfg: 30.0)
    monkeypatch.setenv("STOCKAGENT_SIGNAL_NOW_OPEN_CACHE_SECONDS", "60")

    reusable, reason = _can_reuse_latest_signal_now(cfg, status, mis_summary, requested_price_source="auto")

    assert reusable
    assert reason == "cached_tw_after_close_age=30s"


def test_can_reuse_latest_signal_now_for_recent_open_panel_market(monkeypatch) -> None:
    cfg = SimpleNamespace(
        market="crypto",
        market_type="crypto",
        history_frequency="bar",
        display_timezone="Asia/Taipei",
        timezone="UTC",
    )
    status = SimpleNamespace(
        market_open=True,
        data=SimpleNamespace(fresh=True, last_data_date="2026-07-08 14:00:00", panel_date="2026-07-08 14:00:00"),
    )
    summary = {
        "asof_date": "2026-07-08 22:00:30",
        "panel_date": "2026-07-08 14:00:00",
        "price_source": "panel_close",
    }

    monkeypatch.setattr("services.discord_bot.bot._signal_now_open_cache_seconds", lambda: 120.0)
    monkeypatch.setattr("services.discord_bot.bot._summary_age_seconds", lambda summary, cfg: 30.0)

    reusable, reason = _can_reuse_latest_signal_now(cfg, status, summary, requested_price_source="auto")

    assert reusable
    assert reason == "cached_open_panel_age=30s"


def test_summary_age_seconds_handles_timezone_aware_generated_at(monkeypatch) -> None:
    cfg = SimpleNamespace(display_timezone="Asia/Taipei", timezone="UTC")
    summary = {"generated_at": "2026-07-08T22:29:00+08:00"}

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            if tz is not None:
                return cls(2026, 7, 8, 22, 29, 30, tzinfo=tz)
            return cls(2026, 7, 8, 22, 29, 30)

        @classmethod
        def fromisoformat(cls, date_string):
            return datetime.fromisoformat(date_string)

    monkeypatch.setattr("services.discord_bot.bot.datetime", FixedDateTime)

    assert _summary_age_seconds(summary, cfg) == 30.0


def test_can_reuse_latest_signal_now_for_recent_open_yahoo(monkeypatch) -> None:
    cfg = SimpleNamespace(market="tw", display_timezone="Asia/Taipei", timezone="Asia/Taipei")
    status = SimpleNamespace(market_open=True, data=SimpleNamespace(fresh=True))
    summary = {"asof_date": "2026-07-07 09:30:00", "price_source": "yahoo:quote"}

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 7, 7, 9, 30, 30, tzinfo=tz)

    monkeypatch.setattr("services.discord_bot.bot.datetime", FixedDateTime)
    monkeypatch.setenv("STOCKAGENT_SIGNAL_NOW_OPEN_CACHE_SECONDS", "60")

    reusable, reason = _can_reuse_latest_signal_now(cfg, status, summary, requested_price_source="auto")

    assert reusable
    assert reason == "cached_open_yahoo_age=30s"


def test_signal_now_refreshes_automatically_when_data_is_stale() -> None:
    fresh = SimpleNamespace(data=SimpleNamespace(fresh=True))
    stale = SimpleNamespace(data=SimpleNamespace(fresh=False))

    assert not _signal_now_should_refresh_data(fresh, refresh_data=False)
    assert _signal_now_should_refresh_data(fresh, refresh_data=True)
    assert _signal_now_should_refresh_data(stale, refresh_data=False)


def test_auto_signal_price_source_uses_tw_mis_for_open_taiwan_market() -> None:
    status = SimpleNamespace(market_open=True)
    cfg = SimpleNamespace(market_type="tw", history_frequency="daily", pre_signal_command=["download"])

    assert _auto_signal_price_source(cfg, status, "auto") == "tw"
    assert _auto_signal_price_source(cfg, status, None) == "tw"


def test_auto_signal_price_source_uses_yahoo_for_other_open_stock_markets() -> None:
    status = SimpleNamespace(market_open=True)
    cfg = SimpleNamespace(market_type="us", history_frequency="daily", pre_signal_command=["download"])

    assert _auto_signal_price_source(cfg, status, "auto") == "yahoo"


def test_auto_signal_price_source_keeps_bar_markets_on_latest_panel_bar() -> None:
    status = SimpleNamespace(market_open=True)
    cfg = SimpleNamespace(market_type="crypto", history_frequency="bar", pre_signal_command=["download"])

    assert _auto_signal_price_source(cfg, status, "auto") == "panel"


def test_auto_signal_price_source_respects_explicit_and_closed_market_defaults() -> None:
    open_status = SimpleNamespace(market_open=True)
    today = datetime.now().date().isoformat()
    closed_fresh_status = SimpleNamespace(
        market_open=False,
        data=SimpleNamespace(last_data_date=today, panel_date=today),
    )
    closed_lagging_after_open_status = SimpleNamespace(
        market_open=False,
        data=SimpleNamespace(last_data_date="2000-01-01", panel_date="2000-01-01"),
    )
    cfg = SimpleNamespace(
        market_type="tw",
        history_frequency="daily",
        pre_signal_command=["download"],
        timezone="Asia/Taipei",
        open_time="00:00",
    )

    assert _auto_signal_price_source(cfg, open_status, "panel") == "panel"
    assert _auto_signal_price_source(cfg, open_status, "yahoo") == "yahoo"
    assert _auto_signal_price_source(cfg, closed_fresh_status, "auto") is None
    assert _auto_signal_price_source(cfg, closed_lagging_after_open_status, "auto") == "tw"


def test_console_progress_prints_backend_progress_bar(capsys) -> None:
    progress = _ConsoleProgress(prefix="unit:tw")
    progress({"label": "unit:tw", "step": 3, "total": 6, "message": "halfway"})

    output = capsys.readouterr().out
    assert "[signal-progress] unit:tw" in output
    assert "03/06" in output
    assert "50.00%" in output
    assert "halfway" in output


def test_signal_kwargs_forwards_progress_fields(monkeypatch) -> None:
    captured = {}

    def fake_signal_kwargs(**overrides):
        captured.update(overrides)
        return dict(overrides)

    cfg = SimpleNamespace(market="unit", signal_kwargs=fake_signal_kwargs)
    status = SimpleNamespace()
    monkeypatch.setattr("services.discord_bot.bot._resolve_market", lambda market: cfg)
    monkeypatch.setattr("services.discord_bot.bot._ensure_signal_ready", lambda cfg, scheduled=False: status)
    monkeypatch.setattr("services.discord_bot.bot._market_notice", lambda status: "notice")

    callback = object()
    result = _signal_kwargs(market="unit", progress_callback=callback, progress_label="unit-progress")

    assert result["progress_callback"] is callback
    assert result["progress_label"] == "unit-progress"
    assert captured["market_notice"] == "notice"


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


def test_stale_data_still_allows_signal_with_notice(monkeypatch) -> None:
    cfg = SimpleNamespace(
        market="tw",
        label="台股",
        timezone="Asia/Taipei",
        display_timezone="Asia/Taipei",
    )
    status = SimpleNamespace(
        enabled=True,
        checkpoint=object(),
        market_open=True,
        market_open_reason="open",
        cfg=cfg,
        data=SimpleNamespace(
            fresh=False,
            reason="latest data 2026-06-22 older than expected 2026-06-23",
            last_data_date="2026-06-22 13:30:00",
            panel_date="2026-06-22 13:30:00",
            expected_latest_date="2026-06-23 13:30:00",
        ),
    )

    monkeypatch.setattr("services.discord_bot.bot._runtime_status", lambda cfg: status)

    assert _ensure_signal_ready(cfg) is status
    notice = _market_notice(status)
    assert notice is not None
    assert "資料提醒" in notice
    assert "仍會產生訊號" in notice


def test_bot_reloader_defaults_restart_immediately_on_file_updates(monkeypatch) -> None:
    monkeypatch.delenv("STOCKAGENT_BOT_RESTART_DELAY_SECONDS", raising=False)
    monkeypatch.delenv("STOCKAGENT_BOT_RELOAD_POLL_SECONDS", raising=False)
    monkeypatch.delenv("STOCKAGENT_BOT_CRASH_RESTART_DELAY_SECONDS", raising=False)

    assert _watch_delay_seconds() == 0.0
    assert _watch_poll_seconds() == 0.2
    assert _watch_crash_delay_seconds() == 10.0


def test_sanity_block_still_includes_full_latest_signal(tmp_path) -> None:
    cfg = SimpleNamespace(
        market="unit",
        label="Unit",
        current_capital=None,
        initial_capital=None,
        benchmark_window_days=32,
        history_frequency="daily",
        config_path="missing.yaml",
    )
    summary = {
        "market": "unit",
        "market_label": "Unit",
        "signal_id": "sig-test",
        "asof_date": "2026-06-24 13:30:00",
        "panel_date": "2026-06-24 13:30:00",
        "portfolio_simple_return": 0.01,
        "benchmark_simple_return": 0.00,
        "target_risk": {"gross": 1.0, "top_abs_weight": 0.9851},
        "top_positions": [{"symbol": "AAA", "name": "Alpha", "weight": 0.9851, "current_price": 10.0}],
    }

    message = _latest_signal_message(cfg, tmp_path / "summary.json", summary, top_n=1)

    assert "**sanity BLOCK**" in message
    assert "訊號可能異常，但以下仍提供完整訊號" in message
    assert "已暫停自動公開播報" not in message
    assert "**stockAgent live signal** Unit" in message
    assert "`AAA` Alpha" in message


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


def test_summary_recent_performance_uses_live_portfolio_history(monkeypatch) -> None:
    cfg = SimpleNamespace(market="tw", benchmark_window_days=32, current_capital=None, initial_capital=None)
    history = SimpleNamespace(
        days=4,
        period_return=0.12,
        benchmark_return=0.05,
        start_date="2026-06-26",
        end_date="2026-07-01",
    )
    monkeypatch.setattr(
        "services.discord_bot.bot._load_portfolio_history_for_market",
        lambda cfg, days, top_changes, min_abs_change, initial_capital, current_capital: history,
    )

    summary = _summary_with_capital_context(
        cfg,
        {
            "portfolio_simple_return": 0.01,
            "benchmark_simple_return": 0.0,
            "recent_performance": {
                "window_days": 32,
                "strategy_return": -0.99,
                "benchmark_return": -0.99,
            },
        },
    )

    recent = summary["recent_performance"]
    assert recent["source"] == "portfolio_history_with_live_signals"
    assert recent["window_days"] == 4
    assert np.isclose(recent["strategy_return"], 0.12)
    assert np.isclose(recent["benchmark_return"], 0.05)
    assert np.isclose(recent["excess_return"], 0.07)


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


def test_guide_mentions_daily_investor_commands() -> None:
    text = _guide_message()

    assert "/latest" in text
    assert "/changes" in text
    assert "/performance" in text
    assert "/risk" in text
    assert "/subscribe" in text
    assert "/watch" in text


def test_user_subscriptions_are_per_user_and_per_market(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr("services.discord_bot.bot.STATE_PATH", tmp_path / "state.json")

    subscriptions = _set_user_subscription(123, "tw", watchlist_only=True)
    _set_user_subscription(123, "crypto", watchlist_only=False)
    _set_user_subscription(456, "tw", watchlist_only=False)

    assert subscriptions == {"tw": {"enabled": True, "watchlist_only": True}}
    assert _user_subscriptions(123)["crypto"]["watchlist_only"] is False
    assert sorted(user_id for user_id, _ in _subscribed_users_for_market("tw")) == ["123", "456"]
    lines = "\n".join(_subscription_summary_lines(123))
    assert "`tw` mode=`watchlist_only`" in lines
    assert "`crypto` mode=`all_changes`" in lines

    _remove_user_subscription(123, "tw")

    assert "tw" not in _user_subscriptions(123)
    assert sorted(user_id for user_id, _ in _subscribed_users_for_market("tw")) == ["456"]


def test_subscription_alert_pages_only_include_watchlist_matches(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr("services.discord_bot.bot.STATE_PATH", tmp_path / "state.json")
    _add_user_watch_symbol(123, "tw", "2330")
    cfg = SimpleNamespace(market="tw", label="台股", config_path="missing.yaml")
    summary = {
        "market": "tw",
        "asof_date": "2026-06-24 13:30:00",
        "panel_date": "2026-06-24 13:30:00",
        "portfolio_simple_return": 0.01,
        "benchmark_simple_return": 0.0,
    }
    rows = [
        {"symbol": "2330", "name": "台積電", "action": "BUY", "delta_weight": 0.05, "current_weight": 0.0, "target_weight": 0.05, "trade_price": 1000.0},
        {"symbol": "6669", "name": "緯穎", "action": "SELL", "delta_weight": -0.04, "current_weight": 0.04, "target_weight": 0.0, "trade_price": 4500.0},
    ]

    pages = _subscription_alert_pages(
        cfg,
        summary,
        rows,
        user_id=123,
        settings={"enabled": True, "watchlist_only": True},
    )
    all_pages = _subscription_alert_pages(
        cfg,
        summary,
        rows,
        user_id=123,
        settings={"enabled": True, "watchlist_only": False},
    )

    assert "`2330` 台積電" in pages[0]
    assert "`6669` 緯穎" not in pages[0]
    assert "`mode=watchlist_only`" in pages[0]
    assert "`2330` 台積電" in all_pages[0]
    assert "`6669` 緯穎" in all_pages[0]
    assert "`mode=all_changes`" in all_pages[0]


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


def test_portfolio_history_includes_all_newer_live_signals(monkeypatch, tmp_path) -> None:
    result = SimpleNamespace(
        rows=[{"date": "2026-06-25", "portfolio_return": 0.01, "benchmark_return": 0.0, "profit_value": 10.0}],
        source_paths=(),
        days=3,
        top_changes=1,
        start_date="2026-06-25",
        end_date="2026-06-25",
        period_return=0.01,
        benchmark_return=0.0,
        profit_value=10.0,
        capital=SimpleNamespace(capital=1_000.0),
    )
    cfg = SimpleNamespace(market="tw", live_output_dir=str(tmp_path))
    summaries = []
    for date_text, ret in (("2026-06-26", 0.02), ("2026-06-30", -0.01)):
        signal_dir = tmp_path / date_text
        signal_dir.mkdir()
        summary_path = signal_dir / "summary.json"
        weights_path = signal_dir / "target_weights.parquet"
        rebalance_path = signal_dir / "rebalance.parquet"
        pl.DataFrame({"symbol": ["AAA"], "target_weight": [0.1]}).write_parquet(weights_path)
        pl.DataFrame(
            {
                "symbol": ["AAA"],
                "action": ["BUY"],
                "current_weight": [0.0],
                "target_weight": [0.1],
                "delta_weight": [0.1],
            }
        ).write_parquet(rebalance_path)
        summary = {
            "asof_date": f"{date_text} 13:15:00",
            "panel_date": f"{date_text} 13:30:00",
            "panel_data_date": date_text,
            "portfolio_simple_return": ret,
            "benchmark_simple_return": 0.0,
            "display_capital": 1_000.0,
            "target_risk": {"gross": 0.1, "net": 0.1, "long_gross": 0.1, "short_gross": 0.0},
            "weights_path": str(weights_path),
            "rebalance_path": str(rebalance_path),
        }
        summary_path.write_text("{}", encoding="utf-8")
        summaries.append((summary_path, summary))

    monkeypatch.setattr("services.discord_bot.bot._market_signals", lambda cfg: summaries)

    _include_live_signals_in_portfolio_history(cfg, result, max_rows=3)

    assert [row["date"] for row in result.rows] == ["2026-06-30", "2026-06-26", "2026-06-25"]
    assert np.isclose(result.period_return, 1.01 * 1.02 * 0.99 - 1.0)
    assert result.rows[0]["source"] == "latest_live_signal"
    assert result.rows[1]["source"] == "latest_live_signal"


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
