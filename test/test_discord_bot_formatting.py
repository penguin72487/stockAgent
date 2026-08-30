from __future__ import annotations

import asyncio
import json
import sys
from types import SimpleNamespace
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import numpy as np
import polars as pl
import discord

from services.discord_bot.bot import (
    _BOT_RUN_ID,
    bot as stockagent_bot,
    _add_user_watch_symbol,
    _annotate_history_rows_with_display_time,
    _auto_signal_price_source,
    _artifact_backfill_is_current,
    _artifact_backfill_key,
    _artifact_backfill_retry_allowed,
    _artifact_backfill_health_summary,
    _begin_artifact_backfill,
    _can_reuse_latest_signal_now,
    _config_trading_limits,
    _completed_session_receipt_ready,
    _ConsoleProgress,
    _decision_overview_page,
    _daily_summary_message,
    _discord_page_kwargs,
    _send_channel_pages,
    _send_paginated_response,
    _ensure_signal_ready,
    _filter_watchlist_rows,
    _formal_history_latest_date,
    _formal_history_timeout_seconds,
    _finish_artifact_backfill,
    _guide_message,
    _handle_signal_now_command,
    _latest_changes_pages,
    _latest_signal_message,
    _market_notice,
    _market_artifact_backfill_time,
    _opening_critical_work_pending,
    _market_has_live_signal_for_date,
    _market_has_generated_signal_for_session,
    _day_trade_schedule_state,
    _validate_pre_signal_download_artifacts,
    BotUserError,
    _performance_message,
    _enrich_signal_performance_for_discord,
    _position_line,
    _portfolio_change_line,
    _portfolio_history_header_lines,
    _portfolio_history_block,
    _portfolio_history_pages,
    _preopen_prepare_key,
    _preopen_market_final_armed_for_session,
    _preopen_market_ready_for_session,
    _rotate_error_log_if_needed,
    _write_preopen_final_arm,
    _write_preopen_readiness,
    _validate_day_trade_portfolio_history_result,
    _prepare_realtime_signal_sync,
    _include_live_signals_in_portfolio_history,
    _interactive_signal_work_pending,
    _prepend_latest_signal_row_to_portfolio_history,
    _public_broadcasts_enabled,
    _raw_score_pages,
    _rebalance_line,
    _remove_user_watch_symbol,
    _resolve_pre_signal_command,
    _recent_pre_signal_failure,
    _remember_pre_signal_failure,
    _remember_pre_signal_success,
    _register_signal_now_job,
    _remove_user_subscription,
    _replace_user_watch_symbol,
    _recent_performance_from_returns,
    _risk_adjusted_metrics_from_simple_returns,
    _resume_signal_now_jobs_once,
    _run_artifact_backfill_sync,
    _run_day_trade_settlement_backfill,
    _risk_message,
    _scheduled_detail_page_groups,
    _scheduled_signal_requires_preopen_catch_up,
    _scheduled_signal_key,
    _scheduled_market_session_day,
    _scheduled_markets,
    _scheduled_retry_allowed,
    _signal_now_background_key,
    _signal_now_job_health_summary,
    _signal_now_resumable_jobs,
    _run_signal_now_background_refresh,
    _mark_scheduled_retry,
    _clear_scheduled_retry,
    _set_user_subscription,
    _signal_now_should_refresh_data,
    _normalize_signal_now_mode,
    _summary_age_seconds,
    _summary_has_raw_score_contract,
    _signal_kwargs,
    _signal_now_detail_page_groups,
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
    _wait_for_existing_tw_data_update,
    _tw_data_layer_lock_path,
)


def test_portfolio_history_command_has_no_multi_period_page_size_option() -> None:
    command = stockagent_bot.tree.get_command("portfolio_history")
    assert command is not None
    parameters = {parameter.name: parameter for parameter in command.parameters}
    assert "page_size" not in parameters
    assert parameters["top_changes"].min_value == 0
    assert parameters["top_changes"].max_value == 20


def test_paginated_senders_omit_none_view_for_single_page() -> None:
    class Recorder:
        def __init__(self) -> None:
            self.calls = []

        async def send(self, **kwargs) -> None:
            self.calls.append(kwargs)

    async def exercise() -> tuple[Recorder, Recorder]:
        followup = Recorder()
        channel = Recorder()
        interaction = SimpleNamespace(followup=followup)
        await _send_paginated_response(interaction, ["single interaction page"])
        await _send_channel_pages(channel, ["single channel page"])
        return followup, channel

    followup, channel = asyncio.run(exercise())

    assert followup.calls == [{"content": "single interaction page", "embed": None}]
    assert channel.calls == [{"content": "single channel page", "embed": None}]


def test_paginated_senders_attach_view_for_multiple_pages() -> None:
    class Recorder:
        def __init__(self) -> None:
            self.calls = []

        async def send(self, **kwargs) -> None:
            self.calls.append(kwargs)

    async def exercise() -> Recorder:
        followup = Recorder()
        interaction = SimpleNamespace(followup=followup)
        await _send_paginated_response(interaction, ["first", "second"])
        return followup

    followup = asyncio.run(exercise())

    assert followup.calls[0]["content"] == "first"
    assert followup.calls[0]["embed"] is None
    assert isinstance(followup.calls[0]["view"], discord.ui.View)


def test_portfolio_history_renders_exactly_one_day_per_page(
    monkeypatch, tmp_path
) -> None:
    monkeypatch.setattr(
        "services.discord_bot.bot._market_execution_mode",
        lambda cfg: "tw_day_trade",
    )
    cfg = SimpleNamespace(
        market="tw_day_trade",
        open_time="09:00",
        display_timezone="Asia/Taipei",
    )
    changes = [
        {
            "symbol": f"LONG_SYMBOL_{index}",
            "name": "很長的測試股票名稱",
            "action": "BUY",
            "holding_ratio_delta": 0.01,
            "holding_ratio": 0.01,
            "market_value": 10_000.0,
            "market_value_delta": 10_000.0,
            "shares": 1000,
            "share_delta": 1000,
            "entry_price": 10.0,
            "exit_price": 10.2,
            "price_contract": "session_open_to_close",
            "intraday_price_included": False,
            "execution_mode": "tw_day_trade",
        }
        for index in range(20)
    ]
    rows = []
    for day, daily_return in (("2026-08-07", 0.01), ("2026-08-06", -0.02)):
        open_nav = 1_000_000.0
        rows.append(
            {
                "date": day,
                "display_date": f"{day} 09:00:00",
                "position_source": "executed_history",
                "source": "integer_share_backtest",
                "price_contract": "session_open_to_close",
                "intraday_price_included": False,
                "portfolio_return": daily_return,
                "benchmark_return": 0.0,
                "profit_value": open_nav * daily_return,
                "open_nav": open_nav,
                "close_nav": open_nav * (1.0 + daily_return),
                "turnover": 0.5,
                "gross_ratio": 0.25,
                "net_ratio": 0.05,
                "cash_ratio": 0.95,
                "position_count": 20,
                "long_count": 20,
                "short_count": 0,
                "change_count": 100,
                "change_counts": {"OPEN_LONG": 100},
                "changes": changes,
                "execution_mode": "tw_day_trade",
            }
        )
    result = SimpleNamespace(
        rows=rows,
        frequency="daily",
        fold_dir=tmp_path / "fold_11",
        source_paths=(),
    )

    _validate_day_trade_portfolio_history_result(cfg, result)
    pages = _portfolio_history_pages(cfg, result)

    assert len(pages) == len(rows) == 2
    assert all(1850 < len(page) <= 4000 for page in pages)
    assert "2026-08-07 09:00:00" in pages[0]
    assert "2026-08-06 09:00:00" not in pages[0]
    assert "2026-08-06 09:00:00" in pages[1]
    assert "2026-08-07 09:00:00" not in pages[1]
    assert "page=1/2" in pages[0]
    assert "page=2/2" in pages[1]
    assert "top changes: `20/100`" in pages[0]
    assert all(f"LONG_SYMBOL_{index}" in pages[0] for index in range(20))
    assert "`Δhold=" not in pages[0]
    assert "`Δvalue=" not in pages[0]
    assert "`Δsh=" not in pages[0]
    assert "`hold=" in pages[0]
    assert "`value=" in pages[0]
    assert "`shares=" in pages[0]
    assert "`open=" in pages[0]
    assert "`close=" in pages[0]

    payload = _discord_page_kwargs(pages[0])
    assert payload["content"] is None
    assert payload["embed"].description == pages[0]


def test_pre_signal_python_sentinel_uses_running_interpreter() -> None:
    command = _resolve_pre_signal_command(
        ("{python}", "downloader/example.py", "--mode", "incremental")
    )

    assert command == [sys.executable, "downloader/example.py", "--mode", "incremental"]


def test_scheduled_markets_defaults_to_all_configured_markets(monkeypatch) -> None:
    monkeypatch.delenv("STOCKAGENT_SCHEDULED_MARKETS", raising=False)
    monkeypatch.setattr(
        "services.discord_bot.bot._market_configs",
        lambda: {"tw": object(), "crypto": object(), "us": object()},
    )
    monkeypatch.setattr("services.discord_bot.bot._market_enabled", lambda cfg: True)

    assert _scheduled_markets() == ["crypto", "tw", "us"]


def test_scheduled_markets_respects_explicit_env(monkeypatch) -> None:
    monkeypatch.setenv("STOCKAGENT_SCHEDULED_MARKETS", "tw,crypto")
    monkeypatch.setattr(
        "services.discord_bot.bot._market_configs",
        lambda: {"tw": object(), "crypto": object(), "us": object()},
    )
    monkeypatch.setattr("services.discord_bot.bot._market_enabled", lambda cfg: True)

    assert _scheduled_markets() == ["tw", "crypto"]


def test_scheduled_markets_excludes_disabled_market_even_when_explicit(
    monkeypatch,
) -> None:
    configs = {"tw": object(), "crypto": object(), "us": object()}
    monkeypatch.setenv("STOCKAGENT_SCHEDULED_MARKETS", "all")
    monkeypatch.setattr("services.discord_bot.bot._market_configs", lambda: configs)
    monkeypatch.setattr(
        "services.discord_bot.bot._market_enabled",
        lambda cfg: cfg is not configs["crypto"],
    )

    assert _scheduled_markets() == ["tw", "us"]


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


def test_artifact_backfill_key_uses_backfill_time_and_skips_interval_markets(
    monkeypatch,
) -> None:
    monkeypatch.setattr("services.discord_bot.bot._market_state", lambda market: {})
    monkeypatch.setattr(
        "services.discord_bot.bot._scheduled_market_session_day",
        lambda _cfg, _now: (True, "fixture session"),
    )
    monkeypatch.setattr(
        "services.discord_bot.bot._runtime_status_for_display",
        lambda cfg: SimpleNamespace(
            data=SimpleNamespace(
                expected_latest_date="2026-07-06",
                last_data_date="2026-07-06",
                fresh=True,
            )
        ),
    )
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
    now = datetime(2026, 7, 6, 18, 0, tzinfo=ZoneInfo("Asia/Taipei"))

    assert _market_artifact_backfill_time(daily_cfg) == "18:00"
    assert _artifact_backfill_key(daily_cfg, now) == "2026-07-06:tw:artifact_backfill"
    assert _artifact_backfill_key(daily_cfg, now.replace(hour=23, minute=59)) == (
        "2026-07-06:tw:artifact_backfill"
    )
    assert _artifact_backfill_key(daily_cfg, now.replace(hour=17, minute=59)) is None
    assert _artifact_backfill_key(interval_cfg, now) is None


def test_artifact_backfill_uses_weekly_session_and_freshness_gates(monkeypatch) -> None:
    cfg = SimpleNamespace(
        market="tw_day_trade",
        data_ready_time="13:40",
        close_time="13:30",
        summary_time="14:00",
        schedule_time=None,
        schedule_interval_minutes=None,
    )
    saturday = datetime(2026, 8, 22, 14, 0, tzinfo=ZoneInfo("Asia/Taipei"))
    monkeypatch.setattr(
        "services.discord_bot.bot._scheduled_market_session_day",
        lambda _cfg, _now: (False, "weekend"),
    )
    assert _artifact_backfill_key(cfg, saturday) is None

    monday = saturday.replace(day=24)
    monkeypatch.setattr(
        "services.discord_bot.bot._scheduled_market_session_day",
        lambda _cfg, _now: (True, "fixture session"),
    )
    monkeypatch.setattr(
        "services.discord_bot.bot._runtime_status_for_display",
        lambda _cfg: SimpleNamespace(
            data=SimpleNamespace(
                expected_latest_date="2026-08-21",
                last_data_date="2026-08-20",
                fresh=False,
            )
        ),
    )
    assert _artifact_backfill_key(cfg, monday) is None

    monkeypatch.setattr(
        "services.discord_bot.bot._runtime_status_for_display",
        lambda _cfg: SimpleNamespace(
            data=SimpleNamespace(
                expected_latest_date="2026-08-21",
                last_data_date="2026-08-21",
                fresh=True,
            )
        ),
    )
    assert _artifact_backfill_key(cfg, monday) == (
        "2026-08-21:tw_day_trade:artifact_backfill"
    )


def test_artifact_backfill_defers_for_opening_critical_day_trade(monkeypatch) -> None:
    cfg = SimpleNamespace(
        market="tw_day_trade",
        timezone="Asia/Taipei",
        preopen_prepare_time="08:15",
        open_time="09:00",
        day_trade_simulation_enabled=True,
    )
    monkeypatch.setattr(
        "services.discord_bot.bot._market_configs", lambda: {cfg.market: cfg}
    )
    monkeypatch.setattr(
        "services.discord_bot.bot._scheduled_market_session_day",
        lambda _cfg, _now: (True, "fixture session"),
    )
    monkeypatch.setattr(
        "services.discord_bot.bot._day_trade_schedule_state",
        lambda _cfg, _date: "retry",
    )

    assert _opening_critical_work_pending(
        datetime(2026, 7, 6, 8, 15, tzinfo=ZoneInfo("Asia/Taipei"))
    )
    assert _opening_critical_work_pending(
        datetime(2026, 7, 6, 9, 20, tzinfo=ZoneInfo("Asia/Taipei"))
    )

    monkeypatch.setattr(
        "services.discord_bot.bot._day_trade_schedule_state",
        lambda _cfg, _date: "completed",
    )
    assert not _opening_critical_work_pending(
        datetime(2026, 7, 6, 9, 20, tzinfo=ZoneInfo("Asia/Taipei"))
    )


def test_preopen_prepare_key_catches_up_missing_day_trade_readiness(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "services.discord_bot.bot._scheduled_market_session_day",
        lambda _cfg, current: (current.weekday() < 5, "fixture calendar"),
    )
    now = datetime(2026, 7, 6, 8, 30, tzinfo=ZoneInfo("Asia/Taipei"))
    configured = SimpleNamespace(
        market="tw_day_trade",
        preopen_prepare_time="08:30",
        open_time="09:00",
        schedule_interval_minutes=None,
        day_trade_simulation_enabled=True,
        holidays=(),
    )
    generic = SimpleNamespace(
        market="tw",
        preopen_prepare_time=None,
        open_time="09:00",
        schedule_interval_minutes=None,
        day_trade_simulation_enabled=False,
        holidays=(),
    )

    assert _preopen_prepare_key(configured, now) == ("2026-07-06:tw_day_trade:preopen")
    monkeypatch.setattr(
        "services.discord_bot.bot._preopen_market_ready_for_session",
        lambda cfg, session_date: True,
    )
    monkeypatch.setattr(
        "services.discord_bot.bot._preopen_market_final_armed_for_session",
        lambda cfg, session_date: False,
    )
    assert _preopen_prepare_key(configured, now.replace(minute=55)) == (
        "2026-07-06:tw_day_trade:preopen-final-arm"
    )
    monkeypatch.setattr(
        "services.discord_bot.bot._preopen_market_final_armed_for_session",
        lambda cfg, session_date: True,
    )
    assert _preopen_prepare_key(configured, now.replace(minute=55)) == (
        "2026-07-06:tw_day_trade:preopen"
    )
    monkeypatch.setattr(
        "services.discord_bot.bot._preopen_market_ready_for_session",
        lambda cfg, session_date: False,
    )
    monkeypatch.setattr(
        "services.discord_bot.bot._day_trade_schedule_state",
        lambda cfg, session_date: "retry",
    )
    assert _preopen_prepare_key(configured, now.replace(hour=9)) == (
        "2026-07-06:tw_day_trade:preopen-catch-up"
    )
    monkeypatch.setattr(
        "services.discord_bot.bot._preopen_market_ready_for_session",
        lambda cfg, session_date: True,
    )
    assert _preopen_prepare_key(configured, now.replace(hour=9)) is None
    assert _preopen_prepare_key(generic, now) is None


def test_preopen_readiness_preserves_same_day_ready_rows_across_restart(
    monkeypatch, tmp_path
) -> None:
    path = tmp_path / "preopen.json"
    monkeypatch.setenv("STOCKAGENT_PREOPEN_READINESS_PATH", str(path))
    today = datetime.now(ZoneInfo("Asia/Taipei")).date().isoformat()
    path.write_text(
        json.dumps(
            {
                "run_id": "previous-run",
                "markets": {
                    "tw_day_trade": {
                        "status": "ready",
                        "completed_at": f"{today}T08:40:00+08:00",
                        "panel_date": "2026-08-13 13:30:00",
                        "checkpoint_fingerprint": "abc",
                        "symbol_count": 2744,
                        "preopen_price_limits": {
                            "trading_date": today,
                            "prepared_count": 2000,
                        },
                        "same_session_eligibility": {
                            "target_date": today,
                            "venues": {"twse": {"covered": True}},
                        },
                    },
                    "stale_mode": {
                        "status": "ready",
                        "completed_at": "2026-08-12T08:40:00+08:00",
                    },
                },
            }
        ),
        encoding="utf-8",
    )
    cfg = SimpleNamespace(market="tw_day_trade_1m", timezone="Asia/Taipei")
    _write_preopen_readiness(
        cfg,
        status="running",
        started_at=f"{today}T10:00:00+08:00",
        elapsed_seconds=1.0,
        step=1,
        total=23,
        message="starting",
    )
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert set(payload["markets"]) == {"tw_day_trade", "tw_day_trade_1m"}
    ready_cfg = SimpleNamespace(market="tw_day_trade")
    assert _preopen_market_ready_for_session(ready_cfg, today) is True
    armed_cfg = SimpleNamespace(market="tw_day_trade", timezone="Asia/Taipei")
    _write_preopen_final_arm(
        armed_cfg,
        status="ready",
        started_at=f"{today}T08:55:00+08:00",
        elapsed_seconds=0.2,
        summary={
            "live_latency": {
                "panel_cache_hit": True,
                "checkpoint_cache_hit": True,
                "model_cache_hit": True,
            },
            "quote_prewarm": {
                "ready": True,
                "run_id": _BOT_RUN_ID,
                "connection_scope": "process",
                "requested_count": 2303,
                "primed_count": 2303,
                "resolved_count": 2303,
                "missing_count": 0,
            },
        },
    )
    assert _preopen_market_final_armed_for_session(armed_cfg, today) is True


def test_day_trade_schedule_catches_up_after_service_restart(
    monkeypatch, tmp_path
) -> None:
    monkeypatch.setattr("services.discord_bot.bot._market_state", lambda market: {})
    public_root = tmp_path / "data_tw_public"
    public_root.mkdir()
    pl.DataFrame(
        {
            "Name": ["中華民國開國紀念日"],
            "Date": ["1150101"],
            "_dataset": ["twse_api_holidayschedule_holidayschedule"],
            "_source": ["TWSE OpenAPI"],
            "_as_of_date": ["2026-08-14"],
        }
    ).write_parquet(public_root / "twse_api_holidayschedule_holidayschedule.parquet")
    cfg = SimpleNamespace(
        market="tw_day_trade_1m",
        market_type="tw",
        schedule_time="09:00",
        schedule_interval_minutes=None,
        day_trade_simulation_enabled=True,
        day_trade_rule_data_dir=str(public_root),
        config_path="",
        holidays=(),
    )
    morning = datetime(2026, 8, 14, 9, 12, tzinfo=ZoneInfo("Asia/Taipei"))

    assert _scheduled_signal_key(cfg, morning) == "2026-08-14:tw_day_trade_1m"
    assert _scheduled_signal_key(cfg, morning.replace(hour=13, minute=19)) == (
        "2026-08-14:tw_day_trade_1m"
    )
    assert _scheduled_signal_key(cfg, morning.replace(hour=13, minute=20)) is None
    assert _scheduled_signal_key(cfg, morning.replace(day=15)) is None
    assert _scheduled_market_session_day(cfg, morning.replace(day=15))[0] is False
    assert _scheduled_signal_requires_preopen_catch_up(cfg, morning) is True
    assert (
        _scheduled_signal_requires_preopen_catch_up(cfg, morning.replace(minute=0))
        is False
    )


def test_non_day_trade_daily_schedule_still_requires_exact_minute(monkeypatch) -> None:
    monkeypatch.setattr("services.discord_bot.bot._market_state", lambda market: {})
    monkeypatch.setattr(
        "services.discord_bot.bot._scheduled_market_session_day",
        lambda _cfg, current: (current.weekday() < 5, "fixture calendar"),
    )
    cfg = SimpleNamespace(
        market="tw",
        schedule_time="09:00",
        schedule_interval_minutes=None,
        day_trade_simulation_enabled=False,
        holidays=(),
    )
    now = datetime(2026, 8, 14, 9, 12, tzinfo=ZoneInfo("Asia/Taipei"))

    assert _scheduled_signal_key(cfg, now) is None
    assert _scheduled_signal_key(cfg, now.replace(minute=0)) == "2026-08-14:tw"
    assert _scheduled_signal_requires_preopen_catch_up(cfg, now) is False


def test_day_trade_restart_deduplicates_from_latest_generated_signal(
    monkeypatch, tmp_path
) -> None:
    cfg = SimpleNamespace(market="tw_day_trade")
    monkeypatch.setattr(
        "services.discord_bot.bot._latest_market_signal",
        lambda _cfg: (
            tmp_path / "summary.json",
            {"generated_at": "2026-08-14T09:22:55+08:00"},
        ),
    )

    assert _market_has_generated_signal_for_session(cfg, "2026-08-14") is True
    assert _market_has_generated_signal_for_session(cfg, "2026-08-15") is False


def test_day_trade_scheduler_waits_for_engine_execution_record(
    monkeypatch, tmp_path
) -> None:
    cfg = SimpleNamespace(market="tw_day_trade")
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    signal_id = "today-signal"
    monkeypatch.setenv("TW_DAY_TRADE_STATE_DIR", str(state_dir))
    monkeypatch.setattr(
        "services.discord_bot.bot._latest_market_signal",
        lambda _cfg: (
            tmp_path / "summary.json",
            {
                "generated_at": "2026-08-14T09:22:55+08:00",
                "signal_id": signal_id,
            },
        ),
    )

    state = {
        "modes": {
            "tw_day_trade": {
                "session_date": "2026-08-13",
                "signal_id": "old",
                "positions": {},
            }
        }
    }
    (state_dir / "state.json").write_text(json.dumps(state), encoding="utf-8")
    assert _day_trade_schedule_state(cfg, "2026-08-14") == "retry"

    state["modes"]["tw_day_trade"].update(
        {
            "session_date": "2026-08-14",
            "signal_id": signal_id,
            "entry_completed_at": "2026-08-14T09:23:01+08:00",
        }
    )
    (state_dir / "state.json").write_text(json.dumps(state), encoding="utf-8")
    assert _day_trade_schedule_state(cfg, "2026-08-14") == "completed"

    state["modes"]["tw_day_trade"]["positions"] = {"legacy": {"signed_shares": -1_000}}
    (state_dir / "state.json").write_text(json.dumps(state), encoding="utf-8")
    assert _day_trade_schedule_state(cfg, "2026-08-14") == "blocked_open_position"


def test_artifact_backfill_key_catches_up_previous_session_after_midnight(
    monkeypatch,
) -> None:
    monkeypatch.setattr("services.discord_bot.bot._market_state", lambda market: {})
    monkeypatch.setattr(
        "services.discord_bot.bot._scheduled_market_session_day",
        lambda _cfg, _now: (True, "fixture session"),
    )
    monkeypatch.setattr(
        "services.discord_bot.bot._runtime_status_for_display",
        lambda cfg: SimpleNamespace(
            data=SimpleNamespace(
                expected_latest_date="2026-07-27",
                last_data_date="2026-07-27",
                fresh=True,
            )
        ),
    )
    cfg = SimpleNamespace(
        market="tw_day_trade",
        data_ready_time="13:40",
        close_time="13:30",
        summary_time="14:00",
        schedule_time=None,
        schedule_interval_minutes=None,
    )
    after_midnight = datetime(
        2026,
        7,
        28,
        0,
        5,
        tzinfo=ZoneInfo("Asia/Taipei"),
    )

    assert _artifact_backfill_key(cfg, after_midnight) == (
        "2026-07-27:tw_day_trade:artifact_backfill"
    )


def test_artifact_backfill_failure_uses_durable_bounded_retry(
    monkeypatch,
    tmp_path,
) -> None:
    status_path = tmp_path / "artifact_backfill_status.json"
    monkeypatch.setenv(
        "STOCKAGENT_ARTIFACT_BACKFILL_STATUS_PATH",
        str(status_path),
    )
    monkeypatch.setenv("STOCKAGENT_ARTIFACT_BACKFILL_RETRY_BASE_SECONDS", "60")
    monkeypatch.setenv("STOCKAGENT_ARTIFACT_BACKFILL_RETRY_MAX_SECONDS", "120")
    key = "2026-07-27:tw_day_trade:artifact_backfill"

    started = _begin_artifact_backfill(key, "tw_day_trade")
    assert started["attempt"] == 1
    assert not _artifact_backfill_retry_allowed(key)

    failed = _finish_artifact_backfill(
        key,
        "tw_day_trade",
        status="failed",
        exc=BotUserError("fixture timeout"),
    )
    assert failed["retry_delay_seconds"] == 60.0
    assert not _artifact_backfill_retry_allowed(key)
    assert _artifact_backfill_health_summary()["status"] == "degraded"

    payload = json.loads(status_path.read_text(encoding="utf-8"))
    payload["jobs"][key]["next_retry_at"] = (
        datetime.now().astimezone() - timedelta(seconds=1)
    ).isoformat(timespec="seconds")
    status_path.write_text(json.dumps(payload), encoding="utf-8")
    assert _artifact_backfill_retry_allowed(key)

    retried = _begin_artifact_backfill(key, "tw_day_trade")
    assert retried["attempt"] == 2
    ready = _finish_artifact_backfill(
        key,
        "tw_day_trade",
        status="ready",
    )
    assert ready["next_retry_at"] is None
    assert "retry_delay_seconds" not in ready
    assert not _artifact_backfill_retry_allowed(key)
    assert _artifact_backfill_health_summary()["status"] == "ready"


def test_error_log_rotates_at_startup_and_leaves_fresh_current_path(
    monkeypatch,
    tmp_path,
) -> None:
    error_log = tmp_path / "errors.log"
    error_log.write_bytes(b"x" * (1024 * 1024))
    monkeypatch.setattr("services.discord_bot.bot.ERROR_LOG_PATH", error_log)
    monkeypatch.setenv("STOCKAGENT_DISCORD_ERROR_LOG_MAX_BYTES", str(1024 * 1024))
    monkeypatch.setenv("STOCKAGENT_DISCORD_ERROR_LOG_GENERATIONS", "3")

    assert _rotate_error_log_if_needed()
    assert error_log.read_bytes() == b""
    assert error_log.with_name("errors.log.1").stat().st_size == 1024 * 1024
    assert not _rotate_error_log_if_needed()


def test_signal_now_stale_job_is_durable_and_waits_for_source(
    monkeypatch,
    tmp_path,
) -> None:
    status_path = tmp_path / "artifact_backfill_status.json"
    monkeypatch.setenv(
        "STOCKAGENT_ARTIFACT_BACKFILL_STATUS_PATH",
        str(status_path),
    )
    cfg = SimpleNamespace(market="tw_day_trade_multi_basis")
    stale = SimpleNamespace(
        data=SimpleNamespace(
            fresh=False,
            expected_latest_date="2026-08-26",
            last_data_date="2026-08-25",
            panel_date="2026-08-25",
            reason="latest data 2026-08-25 older than expected 2026-08-26",
        )
    )
    key = _signal_now_background_key(
        cfg,
        target_date="2026-08-26",
        requested_price_source="auto",
        top_n=20,
        min_abs_delta=0.001,
        debug=False,
        force_refresh=False,
        mode="signal",
    )
    first = _register_signal_now_job(
        key,
        user_id=101,
        cfg=cfg,
        runtime_status=stale,
        requested_price_source="auto",
        top_n=20,
        min_abs_delta=0.001,
        debug=False,
        force_refresh=False,
        mode="signal",
    )
    second = _register_signal_now_job(
        key,
        user_id=202,
        cfg=cfg,
        runtime_status=stale,
        requested_price_source="auto",
        top_n=20,
        min_abs_delta=0.001,
        debug=False,
        force_refresh=False,
        mode="signal",
    )

    assert first["status"] == "waiting_source"
    assert second["user_ids"] == [101, 202]
    assert second["actual_data_date"] == "2026-08-25"
    assert second["target_date"] == "2026-08-26"
    assert _signal_now_resumable_jobs()[0]["key"] == key
    health = _signal_now_job_health_summary()
    assert health["status"] == "waiting_source"
    assert health["waiting_source_count"] == 1
    payload = json.loads(status_path.read_text(encoding="utf-8"))
    assert payload["schema_version"] == 2


def test_interactive_signal_work_preempts_formal_history(monkeypatch) -> None:
    monkeypatch.setattr(
        "services.discord_bot.bot._signal_now_resumable_jobs",
        lambda: [{"status": "waiting_source"}],
    )
    stockagent_bot._signal_now_background_tasks.clear()

    assert _interactive_signal_work_pending()

    monkeypatch.setattr(
        "services.discord_bot.bot._signal_now_resumable_jobs",
        lambda: [],
    )
    assert not _interactive_signal_work_pending()


def test_signal_now_stale_job_does_not_run_preview_or_activation(
    monkeypatch,
    tmp_path,
) -> None:
    status_path = tmp_path / "artifact_backfill_status.json"
    monkeypatch.setenv(
        "STOCKAGENT_ARTIFACT_BACKFILL_STATUS_PATH",
        str(status_path),
    )
    cfg = SimpleNamespace(
        market="tw_day_trade_multi_basis",
        timezone="Asia/Taipei",
    )
    stale = SimpleNamespace(
        data=SimpleNamespace(
            fresh=False,
            expected_latest_date="2026-08-26",
            last_data_date="2026-08-25",
            panel_date="2026-08-25",
            reason="source not accepted",
        )
    )
    key = _signal_now_background_key(
        cfg,
        target_date="2026-08-26",
        requested_price_source="auto",
        top_n=20,
        min_abs_delta=0.001,
        debug=False,
        force_refresh=False,
        mode="signal",
    )
    _register_signal_now_job(
        key,
        user_id=101,
        cfg=cfg,
        runtime_status=stale,
        requested_price_source="auto",
        top_n=20,
        min_abs_delta=0.001,
        debug=False,
        force_refresh=False,
        mode="signal",
    )
    calls = {"signal": 0, "activate": 0}

    async def fail_signal(**kwargs):
        del kwargs
        calls["signal"] += 1
        raise AssertionError("stale job must not infer")

    def fail_activation(*args, **kwargs):
        del args, kwargs
        calls["activate"] += 1
        raise AssertionError("stale job must not activate")

    monkeypatch.setattr("services.discord_bot.bot._resolve_market", lambda market: cfg)
    monkeypatch.setattr(
        "services.discord_bot.bot._ensure_signal_ready_cached",
        lambda resolved: stale,
    )
    monkeypatch.setattr("services.discord_bot.bot._run_market_signal", fail_signal)
    monkeypatch.setattr(
        "services.discord_bot.bot._prepare_realtime_signal_sync",
        fail_activation,
    )
    stockagent_bot._signal_now_background_waiters[key] = {101}

    asyncio.run(
        _run_signal_now_background_refresh(
            key,
            market=cfg.market,
            requested_price_source="auto",
            top_n=20,
            min_abs_delta=0.001,
            debug=False,
            force_refresh=False,
            mode="signal",
        )
    )

    assert calls == {"signal": 0, "activate": 0}
    assert _signal_now_resumable_jobs()[0]["status"] == "waiting_source"


def test_signal_now_waiting_job_resumes_after_source_becomes_fresh(
    monkeypatch,
    tmp_path,
) -> None:
    status_path = tmp_path / "artifact_backfill_status.json"
    monkeypatch.setenv(
        "STOCKAGENT_ARTIFACT_BACKFILL_STATUS_PATH",
        str(status_path),
    )
    cfg = SimpleNamespace(market="tw_day_trade_multi_basis")
    stale = SimpleNamespace(
        data=SimpleNamespace(
            fresh=False,
            expected_latest_date="2026-08-26",
            last_data_date="2026-08-25",
            panel_date="2026-08-25",
            reason="source not accepted",
        )
    )
    fresh = SimpleNamespace(
        data=SimpleNamespace(
            fresh=True,
            expected_latest_date="2026-08-26",
            last_data_date="2026-08-26",
            panel_date="2026-08-26",
            reason=None,
        )
    )
    key = _signal_now_background_key(
        cfg,
        target_date="2026-08-26",
        requested_price_source="auto",
        top_n=20,
        min_abs_delta=0.001,
        debug=False,
        force_refresh=False,
        mode="signal",
    )
    _register_signal_now_job(
        key,
        user_id=101,
        cfg=cfg,
        runtime_status=stale,
        requested_price_source="auto",
        top_n=20,
        min_abs_delta=0.001,
        debug=False,
        force_refresh=False,
        mode="signal",
    )
    resumed: list[str] = []

    async def fake_run(job_key, **kwargs):
        del kwargs
        resumed.append(job_key)

    monkeypatch.setattr("services.discord_bot.bot._opening_critical_work_pending", lambda: False)
    monkeypatch.setattr("services.discord_bot.bot._resolve_market", lambda market: cfg)
    monkeypatch.setattr(
        "services.discord_bot.bot._ensure_signal_ready_cached",
        lambda resolved: fresh,
    )
    monkeypatch.setattr(
        "services.discord_bot.bot._run_signal_now_background_refresh",
        fake_run,
    )
    stockagent_bot._signal_now_background_tasks.clear()
    stockagent_bot._signal_now_background_waiters.clear()

    async def exercise() -> None:
        await _resume_signal_now_jobs_once()
        await stockagent_bot._signal_now_background_tasks[key]

    asyncio.run(exercise())

    assert resumed == [key]
    stockagent_bot._signal_now_background_tasks.clear()
    stockagent_bot._signal_now_background_waiters.clear()


def test_signal_now_stale_closed_market_rebuilds_completed_close(
    monkeypatch,
    tmp_path,
) -> None:
    status_path = tmp_path / "artifact_backfill_status.json"
    monkeypatch.setenv(
        "STOCKAGENT_ARTIFACT_BACKFILL_STATUS_PATH",
        str(status_path),
    )
    cfg = SimpleNamespace(
        market="tw_day_trade_multi_basis",
        timezone="Asia/Taipei",
        day_trade_simulation_enabled=True,
        completed_session_command=("finalize",),
        schedule_interval_minutes=None,
    )
    stale = SimpleNamespace(
        market_open=False,
        data=SimpleNamespace(
            fresh=False,
            expected_latest_date="2026-08-26",
            last_data_date="2026-08-25",
            panel_date="2026-08-25",
            reason="model close layer is stale",
        ),
    )
    fresh = SimpleNamespace(
        market_open=False,
        data=SimpleNamespace(
            fresh=True,
            expected_latest_date="2026-08-26",
            last_data_date="2026-08-26",
            panel_date="2026-08-26",
            reason=None,
        ),
    )
    key = _signal_now_background_key(
        cfg,
        target_date="2026-08-26",
        requested_price_source="auto",
        top_n=20,
        min_abs_delta=0.001,
        debug=False,
        force_refresh=False,
        mode="signal",
    )
    _register_signal_now_job(
        key,
        user_id=0,
        cfg=cfg,
        runtime_status=stale,
        requested_price_source="auto",
        top_n=20,
        min_abs_delta=0.001,
        debug=False,
        force_refresh=False,
        mode="signal",
    )
    prepared: list[dict[str, object]] = []

    def prepare(_cfg, **kwargs):
        prepared.append(dict(kwargs))
        return None, fresh, True

    async def run_signal(**kwargs):
        del kwargs
        return SimpleNamespace(summary={"signal_id": "close-signal"}, message="ok")

    monkeypatch.setattr("services.discord_bot.bot._resolve_market", lambda market: cfg)
    monkeypatch.setattr(
        "services.discord_bot.bot._ensure_signal_ready_cached",
        lambda resolved: stale,
    )
    monkeypatch.setattr(
        "services.discord_bot.bot._scheduled_market_session_day",
        lambda *_args: (False, "market closed"),
    )
    monkeypatch.setattr(
        "services.discord_bot.bot._signal_now_cached_result",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        "services.discord_bot.bot._prepare_realtime_signal_sync",
        prepare,
    )
    monkeypatch.setattr(
        "services.discord_bot.bot._sync_latest_live_weights_to_market_artifact",
        lambda _cfg: None,
    )
    monkeypatch.setattr("services.discord_bot.bot._run_market_signal", run_signal)
    monkeypatch.setattr(
        "services.discord_bot.bot._enrich_signal_performance_for_discord",
        lambda _cfg, result, **kwargs: result,
    )
    monkeypatch.setattr(
        "services.discord_bot.bot._signal_sanity_issues",
        lambda *_args: [],
    )

    asyncio.run(
        _run_signal_now_background_refresh(
            key,
            market=cfg.market,
            requested_price_source="auto",
            top_n=20,
            min_abs_delta=0.001,
            debug=False,
            force_refresh=False,
            mode="signal",
        )
    )

    assert prepared == [
        {
            "requested_price_source": "auto",
            "force_refresh": True,
            "completed_session": True,
        }
    ]
    assert _signal_now_resumable_jobs() == []


def test_formal_history_latest_date_reads_settled_returns(
    monkeypatch, tmp_path
) -> None:
    fold_dir = tmp_path / "fold_11"
    fold_dir.mkdir()
    pl.DataFrame(
        {
            "date": ["2026-07-20", "2026-07-21"],
            "portfolio_return": [0.01, -0.02],
        }
    ).write_parquet(fold_dir / "daily_portfolio_returns.parquet")
    monkeypatch.setattr(
        "services.discord_bot.bot._market_fold_dir", lambda cfg: fold_dir
    )

    assert _formal_history_latest_date(SimpleNamespace()) == "2026-07-21"


def test_day_trade_settlement_backfill_skips_current_history(monkeypatch) -> None:
    cfg = SimpleNamespace(market="tw_day_trade", fold_id=11)
    status = SimpleNamespace(
        data=SimpleNamespace(
            expected_latest_date="2026-07-21",
            last_data_date="2026-07-21",
            panel_date=None,
        )
    )
    monkeypatch.setattr(
        "services.discord_bot.bot._formal_history_latest_date",
        lambda cfg: "2026-07-21",
    )

    def fail_run(*args, **kwargs):
        raise AssertionError("current formal history must not rerun inference")

    monkeypatch.setattr("services.discord_bot.bot.subprocess.run", fail_run)

    assert not _run_day_trade_settlement_backfill(cfg, status)


def test_day_trade_settlement_backfill_runs_formal_fold_inference(
    monkeypatch, tmp_path
) -> None:
    cfg = SimpleNamespace(
        market="tw_day_trade",
        fold_id=11,
        config_path=tmp_path / "tw_day_trade.yaml",
        output_dir=tmp_path / "artifacts",
        pre_signal_timeout_seconds=123,
        formal_history_timeout_seconds=456,
    )
    status = SimpleNamespace(
        data=SimpleNamespace(
            expected_latest_date="2026-07-22",
            last_data_date="2026-07-21",
            panel_date=None,
        )
    )
    dates = iter(["2026-07-17", "2026-07-22"])
    monkeypatch.setattr(
        "services.discord_bot.bot._formal_history_latest_date",
        lambda cfg: next(dates),
    )
    calls = []

    def fake_run(command, **kwargs):
        calls.append((command, kwargs))
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr("services.discord_bot.bot.subprocess.run", fake_run)

    assert _run_day_trade_settlement_backfill(cfg, status)
    command, kwargs = calls[0]
    assert command[1].endswith("train.py")
    assert command[command.index("--mode") + 1] == "infer"
    assert command[command.index("--start-fold") + 1] == "11"
    assert command[command.index("--multi-gpu-strategy") + 1] == "none"
    assert kwargs["timeout"] == 456
    assert _formal_history_timeout_seconds(cfg) == 456


def test_formal_history_backfill_discovers_fold_from_checkpoint(
    monkeypatch, tmp_path
) -> None:
    cfg = SimpleNamespace(
        market="forex",
        fold_id=None,
        config_path=tmp_path / "forex.yaml",
        output_dir=tmp_path / "artifacts",
        pre_signal_timeout_seconds=123,
        formal_history_timeout_seconds=456,
    )
    status = SimpleNamespace(
        data=SimpleNamespace(
            expected_latest_date="2026-07-27",
            last_data_date="2026-07-27",
            panel_date=None,
        )
    )
    checkpoint = tmp_path / "artifacts" / "fold_07" / "checkpoint_best.pt"
    dates = iter(["2026-07-24", "2026-07-27"])
    calls = []
    monkeypatch.setattr(
        "services.discord_bot.bot._formal_history_latest_date",
        lambda cfg: next(dates),
    )
    monkeypatch.setattr(
        "services.discord_bot.bot._market_model_checkpoint",
        lambda cfg: checkpoint,
    )
    monkeypatch.setattr(
        "services.discord_bot.bot.subprocess.run",
        lambda command, **kwargs: (
            calls.append((command, kwargs)) or SimpleNamespace(returncode=0)
        ),
    )

    assert _run_day_trade_settlement_backfill(cfg, status)
    command, _ = calls[0]
    assert command[command.index("--start-fold") + 1] == "7"


def test_naive_artifact_backfill_runs_formal_history_inference(monkeypatch) -> None:
    cfg = SimpleNamespace(
        market="tw",
        signal_kwargs=lambda **kwargs: kwargs,
    )
    status = SimpleNamespace(
        data=SimpleNamespace(
            fresh=True,
            expected_latest_date="2026-07-23",
            last_data_date="2026-07-23",
            panel_date="2026-07-23",
        )
    )
    calls = []
    monkeypatch.setattr(
        "services.discord_bot.bot._effective_market_config", lambda value: value
    )
    monkeypatch.setattr(
        "services.discord_bot.bot._ensure_signal_ready", lambda value: status
    )
    monkeypatch.setattr(
        "services.discord_bot.bot._market_execution_mode", lambda value: "naive"
    )
    monkeypatch.setattr(
        "services.discord_bot.bot._run_pre_signal_command",
        lambda value: calls.append("download"),
    )
    monkeypatch.setattr(
        "services.discord_bot.bot._clear_runtime_status_cache", lambda: None
    )
    monkeypatch.setattr(
        "services.discord_bot.bot._runtime_status", lambda value: status
    )
    monkeypatch.setattr("services.discord_bot.bot._market_notice", lambda runtime: None)
    monkeypatch.setattr(
        "services.discord_bot.bot._require_fresh_data_for_artifact_generation",
        lambda value, runtime: calls.append("fresh"),
    )
    monkeypatch.setattr(
        "services.discord_bot.bot._run_formal_history_backfill",
        lambda value, runtime: calls.append("infer"),
    )
    monkeypatch.setattr(
        "services.discord_bot.bot._market_has_panel_close_signal_for_date",
        lambda value, date_text: False,
    )
    monkeypatch.setattr(
        "services.discord_bot.bot.generate_live_signal",
        lambda **kwargs: calls.append(("signal", kwargs["price_source"])) or object(),
    )
    monkeypatch.setattr(
        "services.discord_bot.bot._sync_latest_live_weights_to_market_artifact",
        lambda value: calls.append("sync"),
    )

    assert _run_artifact_backfill_sync(cfg) is not None
    assert calls == [
        "fresh",
        "infer",
        ("signal", "panel"),
        "sync",
    ]


def test_naive_artifact_current_requires_contiguous_formal_history_and_close_signal(
    monkeypatch,
) -> None:
    cfg = SimpleNamespace(market="tw")
    status = SimpleNamespace(
        data=SimpleNamespace(
            fresh=True,
            expected_latest_date="2026-07-27",
            last_data_date="2026-07-27",
            panel_date="2026-07-27",
        )
    )
    monkeypatch.setattr(
        "services.discord_bot.bot._previous_source_session_date",
        lambda cfg, target: "2026-07-24",
    )
    monkeypatch.setattr(
        "services.discord_bot.bot._market_has_panel_close_signal_for_date",
        lambda cfg, target: True,
    )
    monkeypatch.setattr(
        "services.discord_bot.bot._formal_history_latest_date",
        lambda cfg: "2026-07-23",
    )

    assert not _artifact_backfill_is_current(cfg, status, "naive")

    monkeypatch.setattr(
        "services.discord_bot.bot._formal_history_latest_date",
        lambda cfg: "2026-07-24",
    )
    assert _artifact_backfill_is_current(cfg, status, "naive")


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


def test_validate_pre_signal_download_artifacts_rejects_all_failed_download(
    tmp_path,
) -> None:
    output_dir = tmp_path / "tw_stocks"
    output_dir.mkdir()
    (output_dir / "download_summary.json").write_text(
        '{"asset_class":"tw_stocks","symbol_count":2307,"row_count":0,"status_counts":{"failed":2307}}',
        encoding="utf-8",
    )
    cfg = SimpleNamespace(market="tw", market_type="tw")
    command = [
        "python",
        "downloader/download_yahoo_ohlcv.py",
        "--output-dir",
        str(output_dir),
        "--mode",
        "daily-update",
    ]

    try:
        _validate_pre_signal_download_artifacts(
            cfg, command, tmp_path / "pre_signal.log"
        )
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

    monkeypatch.setattr(
        "services.discord_bot.bot._ensure_signal_ready", fake_ensure_signal_ready
    )
    monkeypatch.setattr(
        "services.discord_bot.bot._run_pre_signal_command",
        lambda cfg: calls.append(cfg),
    )

    try:
        _prepare_realtime_signal_sync(cfg, force_refresh=True)
    except RuntimeError:
        pass

    assert calls == []


def test_prepare_realtime_signal_does_not_run_daily_updater_just_because_market_is_open(
    monkeypatch,
) -> None:
    cfg = SimpleNamespace(
        market="tw",
        market_type="tw",
        history_frequency="daily",
        schedule_interval_minutes=None,
    )
    status = SimpleNamespace(market_open=True, data=SimpleNamespace(fresh=True))
    calls = []

    monkeypatch.setattr(
        "services.discord_bot.bot._ensure_signal_ready", lambda cfg: status
    )
    monkeypatch.setattr(
        "services.discord_bot.bot._run_pre_signal_command",
        lambda cfg: calls.append(cfg),
    )
    monkeypatch.setattr("services.discord_bot.bot._runtime_status", lambda cfg: status)

    source, resolved_status, refreshed = _prepare_realtime_signal_sync(
        cfg, requested_price_source="auto", force_refresh=False
    )

    assert source == "shioaji"
    assert resolved_status is status
    assert not refreshed
    assert calls == []


def test_prepare_realtime_signal_refreshes_interval_market(monkeypatch) -> None:
    cfg = SimpleNamespace(
        market="crypto",
        market_type="crypto",
        history_frequency="bar",
        schedule_interval_minutes=15,
    )
    status = SimpleNamespace(market_open=True, data=SimpleNamespace(fresh=True))
    calls = []

    monkeypatch.setattr(
        "services.discord_bot.bot._ensure_signal_ready", lambda cfg: status
    )
    monkeypatch.setattr(
        "services.discord_bot.bot._run_pre_signal_command",
        lambda cfg: calls.append(cfg),
    )
    monkeypatch.setattr("services.discord_bot.bot._runtime_status", lambda cfg: status)

    source, resolved_status, refreshed = _prepare_realtime_signal_sync(
        cfg, requested_price_source="auto", force_refresh=False
    )

    assert source == "panel"
    assert resolved_status is status
    assert refreshed
    assert calls == [cfg]


def test_tw_pre_signal_waits_for_existing_data_update(monkeypatch, tmp_path) -> None:
    import fcntl

    lock_path = tmp_path / "tw.lock"
    holder = lock_path.open("a+")
    fcntl.flock(holder.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    command = [
        "python",
        "downloader/download_tw_official_data.py",
        "--lock-file",
        str(lock_path),
    ]
    sleeps = []

    def release_lock(_seconds):
        sleeps.append(_seconds)
        fcntl.flock(holder.fileno(), fcntl.LOCK_UN)

    monkeypatch.setattr("services.discord_bot.bot.time.sleep", release_lock)
    try:
        assert _wait_for_existing_tw_data_update(command, timeout_seconds=5)
    finally:
        holder.close()
    assert sleeps == [1.0]


def test_tw_snapshot_refresh_uses_the_outer_shared_lock(tmp_path) -> None:
    live_root = tmp_path / "live" / "data_tw_public"
    command = [
        "python",
        "scripts/refresh_tw_public_live_snapshot.py",
        "--live-root",
        str(live_root),
    ]

    assert _tw_data_layer_lock_path(command) == (
        live_root.parent / ".locks" / "tw-public-refresh.lock"
    )


def test_pre_signal_failure_cache_is_shared_and_success_clears_it(monkeypatch) -> None:
    from services.discord_bot import bot as discord_bot

    clock = {"now": 100.0}
    command = ["python", "scripts/refresh_tw_public_live_snapshot.py"]
    monkeypatch.setattr(discord_bot.time, "monotonic", lambda: clock["now"])
    monkeypatch.setenv("STOCKAGENT_PRE_SIGNAL_FAILURE_TTL_SECONDS", "900")
    discord_bot._PRE_SIGNAL_FAILURE_AT.clear()
    discord_bot._PRE_SIGNAL_SUCCESS_AT.clear()

    _remember_pre_signal_failure(command, "shared publication lag")
    assert _recent_pre_signal_failure(command) == "shared publication lag"

    clock["now"] = 1001.0
    assert _recent_pre_signal_failure(command) is None

    clock["now"] = 1100.0
    _remember_pre_signal_failure(command, "stale failure")
    _remember_pre_signal_success(command)
    assert _recent_pre_signal_failure(command) is None


def test_pre_signal_command_is_single_flight_per_shared_command(monkeypatch) -> None:
    from concurrent.futures import ThreadPoolExecutor
    import threading
    import time

    from services.discord_bot import bot as discord_bot

    state = {"active": 0, "maximum": 0, "calls": 0}
    state_lock = threading.Lock()

    def fake_serialized(_cfg, *, bypass_cache=False):
        assert not bypass_cache
        with state_lock:
            state["active"] += 1
            state["maximum"] = max(state["maximum"], state["active"])
            state["calls"] += 1
        time.sleep(0.03)
        with state_lock:
            state["active"] -= 1

    monkeypatch.setattr(
        discord_bot, "_run_pre_signal_command_serialized", fake_serialized
    )
    cfg = SimpleNamespace(pre_signal_command=("python", "shared.py"))
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(discord_bot._run_pre_signal_command, cfg)
            for _ in range(2)
        ]
        for future in futures:
            future.result()

    assert state == {"active": 0, "maximum": 1, "calls": 2}


def test_prewarm_is_single_flight_per_market_session(monkeypatch) -> None:
    from concurrent.futures import ThreadPoolExecutor
    import threading
    import time

    from services.discord_bot import bot as discord_bot

    calls = {"count": 0}
    calls_lock = threading.Lock()
    result = SimpleNamespace(summary={"panel_date": "2026-08-27 13:30:00"})

    def fake_serialized(_cfg):
        with calls_lock:
            calls["count"] += 1
        time.sleep(0.03)
        return result

    monkeypatch.setattr(
        discord_bot, "_prewarm_market_signal_serialized", fake_serialized
    )
    discord_bot._PREWARM_RUN_LOCKS.clear()
    discord_bot._PREWARM_RESULTS.clear()
    cfg = SimpleNamespace(market="tw_day_trade_test", timezone="Asia/Taipei")
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(discord_bot._prewarm_market_signal_sync, cfg)
            for _ in range(2)
        ]
        observed = [future.result() for future in futures]

    assert observed == [result, result]
    assert calls["count"] == 1


def test_can_reuse_latest_signal_now_for_closed_fresh_panel_close() -> None:
    cfg = SimpleNamespace(market="tw")
    status = SimpleNamespace(
        market_open=False,
        data=SimpleNamespace(
            fresh=True, last_data_date="2026-07-06", panel_date="2026-07-06"
        ),
    )
    summary = {"panel_date": "2026-07-06 13:30:00", "price_source": "panel_close"}

    reusable, reason = _can_reuse_latest_signal_now(
        cfg, status, summary, requested_price_source="auto"
    )

    assert reusable
    assert reason == "cached_latest_close"


def test_signal_now_cache_requires_raw_score_contract() -> None:
    assert not _summary_has_raw_score_contract({})
    assert not _summary_has_raw_score_contract(
        {"score_contract": {"schema_version": 0}}
    )
    assert _summary_has_raw_score_contract(
        {
            "score_contract": {
                "schema_version": 1,
                "raw_score": "score_logits",
                "score": "centered_score_logits",
            }
        }
    )
    assert not _summary_has_raw_score_contract(
        {
            "score_contract": {
                "schema_version": 1,
                "raw_score": "score_logits",
            }
        },
        require_unconstrained=True,
    )
    assert _summary_has_raw_score_contract(
        {
            "score_contract": {
                "schema_version": 2,
                "raw_score": "score_logits",
                "raw_score_scope": "all_checkpoint_symbols_unmasked",
            }
        },
        require_unconstrained=True,
    )


def test_signal_now_mode_normalization() -> None:
    assert _normalize_signal_now_mode("signal") == "signal"
    assert _normalize_signal_now_mode("raw_scores") == "raw_scores"
    assert _normalize_signal_now_mode("原始分數") == "raw_scores"


def test_raw_score_now_is_a_separate_slash_command() -> None:
    signal_command = stockagent_bot.tree.get_command("signal_now")
    raw_command = stockagent_bot.tree.get_command("raw_score_now")

    assert signal_command is not None
    assert raw_command is not None
    assert "mode" not in {parameter.name for parameter in signal_command.parameters}
    assert {parameter.name for parameter in raw_command.parameters} == {
        "market",
        "price_source",
        "refresh_data",
        "debug",
    }


def test_can_reuse_latest_signal_now_rejects_stale_closed_panel() -> None:
    cfg = SimpleNamespace(market="tw")
    status = SimpleNamespace(
        market_open=False,
        data=SimpleNamespace(
            fresh=False, last_data_date="2026-07-02", panel_date="2026-07-02"
        ),
    )
    summary = {"panel_date": "2026-07-02 13:30:00", "price_source": "panel_close"}

    reusable, _ = _can_reuse_latest_signal_now(
        cfg, status, summary, requested_price_source="auto"
    )

    assert not reusable


def test_can_reuse_latest_signal_now_rejects_another_model_deployment() -> None:
    cfg = SimpleNamespace(market="tw", fold_id=20)
    status = SimpleNamespace(
        market_open=False,
        checkpoint=SimpleNamespace(fingerprint="new-checkpoint"),
        config_fingerprint="new-config",
        data=SimpleNamespace(
            fresh=True, last_data_date="2026-07-14", panel_date="2026-07-14"
        ),
    )
    summary = {
        "fold_id": 25,
        "checkpoint_fingerprint": "old-checkpoint",
        "config_fingerprint": "old-config",
        "panel_date": "2026-07-14 13:30:00",
        "price_source": "panel_close",
    }

    reusable, reason = _can_reuse_latest_signal_now(
        cfg, status, summary, requested_price_source="auto"
    )

    assert not reusable
    assert reason == "deployment_fold_changed"


def test_can_reuse_latest_signal_now_rejects_closed_tw_panel_when_today_panel_missing(
    monkeypatch,
) -> None:
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
        data=SimpleNamespace(
            fresh=True, last_data_date="2000-01-01", panel_date="2000-01-01"
        ),
    )

    panel_summary = {
        "asof_date": "2026-07-09 14:35:00",
        "panel_date": "2000-01-01",
        "price_source": "panel_close",
    }
    reusable, _ = _can_reuse_latest_signal_now(
        cfg, status, panel_summary, requested_price_source="auto"
    )
    assert not reusable

    shioaji_summary = {
        "asof_date": "2026-07-09 14:35:00",
        "panel_date": "2000-01-01",
        "price_source": "shioaji:stock_snapshot",
        "price_available_count": 2000,
    }
    monkeypatch.setattr(
        "services.discord_bot.bot._summary_age_seconds", lambda summary, cfg: 30.0
    )
    monkeypatch.setenv("STOCKAGENT_SIGNAL_NOW_OPEN_CACHE_SECONDS", "60")

    reusable, reason = _can_reuse_latest_signal_now(
        cfg, status, shioaji_summary, requested_price_source="auto"
    )

    assert reusable
    assert reason == "cached_shioaji_after_close_age=30s"


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
        data=SimpleNamespace(
            fresh=True,
            last_data_date="2026-07-08 14:00:00",
            panel_date="2026-07-08 14:00:00",
        ),
    )
    summary = {
        "asof_date": "2026-07-08 22:00:30",
        "panel_date": "2026-07-08 14:00:00",
        "price_source": "panel_close",
    }

    monkeypatch.setattr(
        "services.discord_bot.bot._signal_now_open_cache_seconds", lambda: 120.0
    )
    monkeypatch.setattr(
        "services.discord_bot.bot._summary_age_seconds", lambda summary, cfg: 30.0
    )

    reusable, reason = _can_reuse_latest_signal_now(
        cfg, status, summary, requested_price_source="auto"
    )

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
    cfg = SimpleNamespace(
        market="tw", display_timezone="Asia/Taipei", timezone="Asia/Taipei"
    )
    status = SimpleNamespace(market_open=True, data=SimpleNamespace(fresh=True))
    summary = {"asof_date": "2026-07-07 09:30:00", "price_source": "yahoo:quote"}

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 7, 7, 9, 30, 30, tzinfo=tz)

    monkeypatch.setattr("services.discord_bot.bot.datetime", FixedDateTime)
    monkeypatch.setenv("STOCKAGENT_SIGNAL_NOW_OPEN_CACHE_SECONDS", "60")

    reusable, reason = _can_reuse_latest_signal_now(
        cfg, status, summary, requested_price_source="auto"
    )

    assert reusable
    assert reason == "cached_open_yahoo_age=30s"


def test_open_day_trade_cache_requires_current_decision_panel(monkeypatch) -> None:
    cfg = SimpleNamespace(
        market="tw_day_trade",
        market_type="tw",
        history_frequency="daily",
        display_timezone="Asia/Taipei",
        timezone="Asia/Taipei",
    )
    status = SimpleNamespace(market_open=True, data=SimpleNamespace(fresh=True))
    summary = {
        "asof_date": "2026-07-21 12:10:46",
        "panel_date": "2026-07-17 13:30:00",
        "price_source": "twse_tpex:mis",
        "price_timestamp": "2026-07-21T04:10:43+00:00",
        "price_available_count": 2000,
        "execution_mode": "tw_day_trade",
        "live_session_open_feature_applied": False,
    }
    monkeypatch.setattr(
        "services.discord_bot.bot._summary_age_seconds", lambda summary, cfg: 10.0
    )

    reusable, reason = _can_reuse_latest_signal_now(
        cfg,
        status,
        summary,
        requested_price_source="auto",
    )

    assert not reusable
    assert reason == "day_trade_live_open_feature_missing"

    summary.update(
        panel_date="2026-07-21 12:10:43",
        live_session_open_feature_applied=True,
    )
    reusable, reason = _can_reuse_latest_signal_now(
        cfg,
        status,
        summary,
        requested_price_source="auto",
    )
    assert reusable
    assert reason == "cached_open_tw_mis_age=10s"


def test_signal_now_refreshes_automatically_when_data_is_stale() -> None:
    fresh = SimpleNamespace(data=SimpleNamespace(fresh=True))
    stale = SimpleNamespace(data=SimpleNamespace(fresh=False))

    assert not _signal_now_should_refresh_data(fresh, refresh_data=False)
    assert _signal_now_should_refresh_data(fresh, refresh_data=True)
    assert _signal_now_should_refresh_data(stale, refresh_data=False)


def test_completed_session_receipt_must_ack_latest_close_phase(
    monkeypatch,
    tmp_path,
) -> None:
    receipt_path = tmp_path / "completed.json"
    publication_root = tmp_path / "publications"
    phase_root = publication_root / "close_final"
    phase_root.mkdir(parents=True)
    publication = {
        "status": "ok",
        "phase": "close_final",
        "started_at_taipei": "2026-08-26T17:30:00+08:00",
        "completed_at_taipei": "2026-08-26T17:31:00+08:00",
        "download_summary": {
            "end_date": "2026-08-26",
            "daily_close_ready": True,
            "blocking_failed_count": 0,
        },
    }
    (phase_root / "latest.json").write_text(
        json.dumps(publication),
        encoding="utf-8",
    )
    receipt_path.write_text(
        json.dumps(
            {
                "status": "ok",
                "expected_date": "2026-08-26",
                "source_publication_phase": "close_final",
                "source_publication_completed_at_taipei": (
                    "2026-08-26T17:31:00+08:00"
                ),
                "after": {
                    "current": True,
                    "dates": {
                        "stock_panel": "2026-08-26",
                        "public_features": "2026-08-26",
                    },
                },
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv(
        "STOCKAGENT_TW_COMPLETED_SESSION_RECEIPT",
        str(receipt_path),
    )
    monkeypatch.setenv(
        "STOCKAGENT_TW_PUBLICATION_RECEIPT_ROOT",
        str(publication_root),
    )
    status = SimpleNamespace(
        data=SimpleNamespace(
            expected_latest_date="2026-08-26",
            last_data_date="2026-08-26",
        )
    )

    assert _completed_session_receipt_ready(status)

    publication["completed_at_taipei"] = "2026-08-26T17:32:00+08:00"
    (phase_root / "latest.json").write_text(
        json.dumps(publication),
        encoding="utf-8",
    )
    assert not _completed_session_receipt_ready(status)


def test_signal_now_stale_response_says_waiting_source_not_background_update(
    monkeypatch,
) -> None:
    messages: list[str] = []

    class Response:
        async def defer(self, **kwargs):
            del kwargs

    class Followup:
        async def send(self, content, **kwargs):
            del kwargs
            messages.append(str(content))

    interaction = SimpleNamespace(
        response=Response(),
        followup=Followup(),
        user=SimpleNamespace(id=101),
    )
    cfg = SimpleNamespace(market="tw_day_trade_multi_basis")
    stale = SimpleNamespace(
        market_open=False,
        data=SimpleNamespace(
            fresh=False,
            expected_latest_date="2026-08-26",
            last_data_date="2026-08-25",
            panel_date="2026-08-25",
            reason="source not accepted",
        ),
    )
    monkeypatch.setattr("services.discord_bot.bot._resolve_market", lambda market: cfg)
    monkeypatch.setattr(
        "services.discord_bot.bot._ensure_signal_ready_cached",
        lambda resolved: stale,
    )
    monkeypatch.setattr(
        "services.discord_bot.bot._signal_now_cached_result",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        "services.discord_bot.bot._enqueue_signal_now_background_refresh",
        lambda **kwargs: ("2026-08-26:tw_day_trade_multi_basis:auto", True),
    )

    asyncio.run(
        _handle_signal_now_command(
            interaction,
            market=cfg.market,
            mode="signal",
            price_source="auto",
            top_n=20,
            min_abs_delta=0.001,
            refresh_data=False,
            debug=False,
        )
    )

    assert len(messages) == 1
    assert "status=`waiting_source`" in messages[0]
    assert "不會重算舊資料" in messages[0]
    assert "背景更新與推論" not in messages[0]


def test_auto_signal_price_source_uses_shioaji_for_open_taiwan_market() -> None:
    status = SimpleNamespace(market_open=True)
    cfg = SimpleNamespace(
        market_type="tw", history_frequency="daily", pre_signal_command=["download"]
    )

    assert _auto_signal_price_source(cfg, status, "auto") == "shioaji"
    assert _auto_signal_price_source(cfg, status, None) == "shioaji"


def test_auto_day_trade_signal_uses_reserved_shioaji_when_mis_is_down() -> None:
    status = SimpleNamespace(market_open=True)
    cfg = SimpleNamespace(
        market_type="tw",
        history_frequency="daily",
        day_trade_simulation_enabled=True,
    )

    assert _auto_signal_price_source(cfg, status, "auto") == "shioaji"


def test_auto_signal_price_source_uses_yahoo_for_other_open_stock_markets() -> None:
    status = SimpleNamespace(market_open=True)
    cfg = SimpleNamespace(
        market_type="us", history_frequency="daily", pre_signal_command=["download"]
    )

    assert _auto_signal_price_source(cfg, status, "auto") == "yahoo"


def test_auto_signal_price_source_keeps_bar_markets_on_latest_panel_bar() -> None:
    status = SimpleNamespace(market_open=True)
    cfg = SimpleNamespace(
        market_type="crypto", history_frequency="bar", pre_signal_command=["download"]
    )

    assert _auto_signal_price_source(cfg, status, "auto") == "panel"


def test_auto_signal_price_source_respects_explicit_and_closed_market_defaults() -> (
    None
):
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
    assert (
        _auto_signal_price_source(cfg, closed_lagging_after_open_status, "auto")
        == "shioaji"
    )


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
    monkeypatch.setattr(
        "services.discord_bot.bot._ensure_signal_ready",
        lambda cfg, scheduled=False: status,
    )
    monkeypatch.setattr(
        "services.discord_bot.bot._market_notice", lambda status: "notice"
    )

    callback = object()
    result = _signal_kwargs(
        market="unit", progress_callback=callback, progress_label="unit-progress"
    )

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
                "raw_score": 2.5,
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
                "raw_score": 2.5,
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
    limited_position_pages, limited_rebalance_pages = _scheduled_detail_page_groups(
        cfg,
        result,
        max_rows=1,
    )
    debug_position_pages, debug_rebalance_pages = _scheduled_detail_page_groups(
        cfg, result, debug=True
    )

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
    assert "`raw_score=2.5000`" in position_pages[0]
    assert "`raw_score=2.5000`" in rebalance_pages[0]
    assert "`rows=1`" in limited_position_pages[0]
    assert "`rows=1`" in limited_rebalance_pages[0]
    assert "`CCC`" not in limited_position_pages[0]
    assert "`CCC`" not in limited_rebalance_pages[0]
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
                "raw_score": 2.5,
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
                "raw_score": 2.5,
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
                "raw_score": 2.5,
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
    assert "`raw_score`=模型未置中原始分數" in decision_pages[0]
    assert "`rows=1`" in decision_pages[0]
    assert "`AAA` Alpha **BUY**" in decision_pages[0]
    assert "`raw_score=2.5000`" in position_pages[0]
    assert "`raw_score=2.5000`" in rebalance_pages[0]
    assert "`raw_score=2.5000`" in decision_pages[0]
    assert "`BBB`" not in decision_pages[0]
    assert "full:" not in decision_pages[0]
    assert "full:" in debug_pages[2][0]


def test_signal_now_day_trade_only_includes_current_target_positions() -> None:
    cfg = SimpleNamespace(
        market="tw_day_trade_multi_basis",
        min_abs_delta=0.001,
        current_capital=10_000_000.0,
        initial_capital=None,
    )
    row = {
        "symbol": "AAA",
        "name": "Alpha",
        "action": "BUY",
        "current_weight": 0.0,
        "model_weight": 0.05,
        "target_weight": 0.05,
        "delta_weight": 0.05,
        "score": 1.2,
        "raw_score": 2.5,
        "current_price": 10.0,
        "trade_price": 10.0,
        "price_return": 0.0,
        "decision_reason": "positive_score, target_increase",
    }
    result = SimpleNamespace(
        summary={
            "market": cfg.market,
            "execution_mode": "tw_day_trade",
            "signal_id": "sig-test",
            "panel_date": "2026-08-11 13:30:00",
            "price_source": "panel_close",
        },
        output_dir=None,
        weights_rows=[row],
        rebalance_rows=[row],
        decision_rows=[row],
    )

    groups = _signal_now_detail_page_groups(cfg, result, mode="signal", top_n=10)

    assert len(groups) == 1
    rendered = "\n".join(groups[0])
    assert "signal_now current / target positions" in rendered
    assert "signal_now rebalance" not in rendered
    assert "signal_now decision explanations" not in rendered
    assert "`AAA` Alpha **BUY**" in rendered


def test_signal_now_day_trade_positions_keep_all_rows_and_paginate() -> None:
    cfg = SimpleNamespace(
        market="tw_day_trade",
        min_abs_delta=0.001,
        current_capital=10_000_000.0,
        initial_capital=None,
    )
    rows = [
        {
            "symbol": f"S{index:02d}",
            "name": f"Stock {index:02d}",
            "action": "BUY",
            "current_weight": 0.0,
            "target_weight": 0.001 + index / 100_000,
            "delta_weight": 0.001 + index / 100_000,
            "score": float(index),
            "raw_score": float(index),
            "current_price": 10.0 + index,
            "price_return": 0.0,
        }
        for index in range(25)
    ]
    result = SimpleNamespace(
        summary={
            "market": cfg.market,
            "execution_mode": "tw_day_trade",
            "signal_id": "sig-paged",
            "panel_date": "2026-08-11 13:30:00",
            "price_source": "panel_close",
        },
        output_dir=None,
        weights_rows=rows,
        rebalance_rows=rows,
        decision_rows=rows,
    )

    groups = _signal_now_detail_page_groups(cfg, result, mode="signal", top_n=10)

    assert len(groups) == 1
    pages = groups[0]
    assert len(pages) > 1
    rendered = "\n".join(pages)
    assert "rows 1-" in pages[0]
    assert "/25`" in pages[0]
    assert "`S00` Stock 00 **BUY**" in rendered
    assert "`S24` Stock 24 **BUY**" in rendered
    assert "signal_now rebalance" not in rendered
    assert "signal_now decision explanations" not in rendered


def test_raw_score_pages_show_complete_universe_without_trade_filtering() -> None:
    cfg = SimpleNamespace(market="unit")
    result = SimpleNamespace(
        summary={
            "market": "unit",
            "signal_id": "sig-raw",
            "asof_date": "2026-08-10",
            "panel_date": "2026-08-08",
            "score_contract": {
                "schema_version": 2,
                "raw_score": "score_logits",
                "raw_score_scope": "all_checkpoint_symbols_unmasked",
            },
        },
        weights_rows=[
            {
                "symbol": "BLOCKED",
                "raw_score": -3.0,
                "current_price": 12.0,
                "alive": True,
                "tradable": False,
                "can_buy": False,
                "can_sell": False,
            },
            {
                "symbol": "OPEN",
                "raw_score": 1.0,
                "current_price": 20.0,
                "alive": True,
                "tradable": True,
                "can_buy": True,
                "can_sell": True,
            },
            {
                "symbol": "DEAD",
                "raw_score": 0.5,
                "alive": False,
                "tradable": False,
                "can_buy": False,
                "can_sell": False,
            },
        ],
    )

    pages = _raw_score_pages(cfg, result)

    content = "\n".join(pages)
    assert "rows=3" in content
    assert "scope=all_checkpoint_symbols_unmasked" in content
    assert "filter=none" in content
    assert "`BLOCKED`" in content
    assert "`OPEN`" in content
    assert "`DEAD`" in content
    assert "raw_score=-3.000000" in content
    assert "tradable=False" in content
    assert "abs_rank=1" in content


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


def test_zero_turnover_cap_is_disabled_in_signal_sanity() -> None:
    cfg = SimpleNamespace(
        market="tw_day_trade_100m",
        label="台股當沖 100m",
        config_path="configs/deployments/tw_day_trade_100m_fold11.yaml",
    )
    gross_limit, turnover_limit = _config_trading_limits(cfg)
    summary = {
        "asof_date": "2026-08-10 10:30:00",
        "panel_date": "2026-08-07 13:30:00",
        "turnover": 0.2712,
        "target_risk": {"gross": 0.2712, "top_abs_weight": 0.0025},
    }

    issues = _signal_sanity_issues(cfg, summary)

    assert gross_limit == 1.0
    assert turnover_limit is None
    assert not any("turnover" in text for _, text in issues)


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
        "top_positions": [
            {"symbol": "AAA", "name": "Alpha", "weight": 0.9851, "current_price": 10.0}
        ],
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
        "top_positions": [
            {"symbol": "AAA", "name": "Alpha", "weight": 0.1, "current_price": 10.0}
        ],
        "rebalance": [
            {
                "symbol": "AAA",
                "name": "Alpha",
                "action": "BUY",
                "delta_weight": 0.02,
                "current_weight": 0.0,
                "target_weight": 0.02,
                "trade_price": 10.0,
            }
        ],
    }

    normal = _latest_signal_message(
        cfg, tmp_path / "summary.json", summary, top_n=1, debug=False
    )
    debug = _latest_signal_message(
        cfg, tmp_path / "summary.json", summary, top_n=1, debug=True
    )

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
            {
                "symbol": "AAA",
                "name": "Alpha",
                "action": "BUY",
                "delta_weight": 0.04,
                "current_weight": 0.0,
                "target_weight": 0.04,
                "trade_price": 10.0,
            },
            {
                "symbol": "BBB",
                "name": "Beta",
                "action": "SELL",
                "delta_weight": -0.03,
                "current_weight": 0.03,
                "target_weight": 0.0,
                "trade_price": 20.0,
            },
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
        "top_positions": [
            {"symbol": "AAA", "name": "Alpha", "weight": 0.12, "current_price": 10.0}
        ],
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
    cfg = SimpleNamespace(
        market="tw",
        benchmark_window_days=32,
        current_capital=None,
        initial_capital=None,
    )
    history = SimpleNamespace(
        days=4,
        period_return=0.12,
        benchmark_return=0.05,
        start_date="2026-06-26",
        end_date="2026-07-01",
    )
    monkeypatch.setattr(
        "services.discord_bot.bot._load_portfolio_history_for_market",
        lambda cfg, days, top_changes, min_abs_change, initial_capital, current_capital: (
            history
        ),
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


def test_recent_performance_uses_settled_history_then_contiguous_live_signal(
    monkeypatch,
    tmp_path,
) -> None:
    fold_dir = tmp_path / "fold_25"
    fold_dir.mkdir()
    pl.DataFrame(
        {
            "date": ["2026-07-28", "2026-07-29"],
            "portfolio_return": [0.01, 0.02],
            "benchmark_return": [0.001, 0.002],
        }
    ).write_parquet(fold_dir / "integer_share_daily_portfolio_returns.parquet")
    stale_path = tmp_path / "stale.json"
    latest_path = tmp_path / "latest.json"
    stale_path.write_text("{}", encoding="utf-8")
    latest_path.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(
        "services.discord_bot.bot._market_fold_dir",
        lambda cfg: fold_dir,
    )
    monkeypatch.setattr(
        "services.discord_bot.bot._recent_market_signal_metrics",
        lambda cfg, max_summaries: [
            (
                stale_path,
                {
                    "panel_data_date": "2026-07-29 13:30:00",
                    "previous_weights_data_date": "2026-07-28",
                    "portfolio_simple_return": 0.9,
                    "benchmark_simple_return": 0.9,
                },
            ),
            (
                latest_path,
                {
                    "panel_data_date": "2026-07-30 00:00:00",
                    "previous_weights_data_date": "2026-07-29 00:00:00",
                    "portfolio_simple_return": 0.03,
                    "benchmark_simple_return": 0.003,
                },
            ),
        ],
    )

    result = _recent_performance_from_returns(
        SimpleNamespace(market="tw"),
        3,
    )

    assert result is not None
    assert result["start_date"] == "2026-07-28"
    assert result["end_date"] == "2026-07-30"
    assert np.isclose(result["strategy_return"], 1.01 * 1.02 * 1.03 - 1.0)
    assert np.isclose(result["benchmark_return"], 1.001 * 1.002 * 1.003 - 1.0)
    expected = _risk_adjusted_metrics_from_simple_returns([0.01, 0.02, 0.03])
    for key in ("sharpe", "sortino", "max_drawdown", "calmar", "annualized_return"):
        assert np.isclose(result[key], expected[key])
    assert result["risk_observations"] == 3
    assert result["risk_annualization_periods"] == 252


def test_risk_adjusted_metrics_include_initial_nav_in_drawdown() -> None:
    metrics = _risk_adjusted_metrics_from_simple_returns([-0.10, 0.05])

    assert np.isclose(metrics["max_drawdown"], -0.10)
    assert metrics["risk_observations"] == 2
    assert metrics["risk_return_basis"] == "net_log_return"


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
    assert sorted(user_id for user_id, _ in _subscribed_users_for_market("tw")) == [
        "123",
        "456",
    ]
    lines = "\n".join(_subscription_summary_lines(123))
    assert "`tw` mode=`watchlist_only`" in lines
    assert "`crypto` mode=`all_changes`" in lines

    _remove_user_subscription(123, "tw")

    assert "tw" not in _user_subscriptions(123)
    assert sorted(user_id for user_id, _ in _subscribed_users_for_market("tw")) == [
        "456"
    ]


def test_subscription_alert_pages_only_include_watchlist_matches(
    monkeypatch, tmp_path
) -> None:
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
        {
            "symbol": "2330",
            "name": "台積電",
            "action": "BUY",
            "delta_weight": 0.05,
            "current_weight": 0.0,
            "target_weight": 0.05,
            "trade_price": 1000.0,
        },
        {
            "symbol": "6669",
            "name": "緯穎",
            "action": "SELL",
            "delta_weight": -0.04,
            "current_weight": 0.04,
            "target_weight": 0.0,
            "trade_price": 4500.0,
        },
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
        "target_risk": {
            "gross": 0.30,
            "net": -0.10,
            "long_gross": 0.10,
            "short_gross": 0.20,
        },
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


def test_portfolio_history_uses_complete_weights_when_rebalance_is_empty(
    tmp_path,
) -> None:
    weights_path = tmp_path / "target_weights.parquet"
    rebalance_path = tmp_path / "rebalance.parquet"
    pl.DataFrame(
        {
            "symbol": ["AAA", "BBB", "CCC"],
            "name": ["Alpha", "Beta", "Gamma"],
            "action": ["SELL", "EXIT", "BUY"],
            "current_weight": [0.0001, 0.0004, 0.0],
            "target_weight": [-0.0004, 0.0, 0.00001],
            "delta_weight": [-0.0005, -0.0004, 0.00001],
            "abs_delta_weight": [0.0005, 0.0004, 0.00001],
            "current_price": [10.0, 20.0, 30.0],
        }
    ).write_parquet(weights_path)
    pl.DataFrame().write_parquet(rebalance_path)
    summary_path = tmp_path / "summary.json"
    summary_path.write_text("{}", encoding="utf-8")
    result = SimpleNamespace(
        rows=[
            {
                "date": "2026-07-28",
                "portfolio_return": 0.0,
                "benchmark_return": 0.0,
                "profit_value": 0.0,
            }
        ],
        source_paths=(),
        days=1,
        top_changes=5,
        min_abs_change=0.0001,
        start_date="2026-07-28",
        end_date="2026-07-28",
        period_return=0.0,
        benchmark_return=0.0,
        profit_value=0.0,
        capital=SimpleNamespace(capital=1_000.0),
    )
    summary = {
        "weights_date": "2026-07-29 13:30:00",
        "panel_date": "2026-07-29 13:30:00",
        "portfolio_simple_return": 0.01,
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
    assert result.rows[0]["change_count"] == 2
    assert result.rows[0]["change_counts"] == {"SELL": 1, "EXIT": 1}
    assert [row["symbol"] for row in result.rows[0]["changes"]] == ["AAA", "BBB"]


def test_portfolio_history_prepend_uses_weights_date_before_panel_date(
    tmp_path,
) -> None:
    result = SimpleNamespace(
        rows=[
            {
                "date": "2026-06-23",
                "portfolio_return": 0.0,
                "benchmark_return": 0.0,
                "profit_value": 0.0,
                "changes": [],
                "change_counts": {},
            }
        ],
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
        "asof_date": "2026-06-24 11:42:00",
        "panel_data_date": "2026-06-23",
        "weights_date": "2026-06-24 11:41:55",
    }

    inserted = _prepend_latest_signal_row_to_portfolio_history(
        result,
        summary_path=tmp_path / "summary.json",
        summary=summary,
        max_rows=2,
    )

    assert inserted is True
    assert [row["date"] for row in result.rows] == [
        "2026-06-24 11:41:55",
        "2026-06-23",
    ]


def test_portfolio_history_excludes_day_trade_preview_without_observed_open(
    tmp_path,
) -> None:
    result = SimpleNamespace(rows=[{"date": "2026-07-17"}], end_date="2026-07-17")
    summary = {
        "asof_date": "2026-07-22 10:30:00",
        "panel_data_date": "2026-07-22",
        "execution_mode": "tw_day_trade",
        "execution_preview_only": True,
    }

    inserted = _prepend_latest_signal_row_to_portfolio_history(
        result,
        summary_path=tmp_path / "summary.json",
        summary=summary,
        max_rows=32,
    )

    assert inserted is False
    assert result.rows == [{"date": "2026-07-17"}]


def test_portfolio_history_excludes_old_nonpreview_day_trade_without_open_contract(
    tmp_path,
) -> None:
    result = SimpleNamespace(rows=[{"date": "2026-07-17"}], end_date="2026-07-17")
    summary = {
        "asof_date": "2026-07-22 10:30:00",
        "panel_data_date": "2026-07-22",
        "execution_mode": "tw_day_trade",
        "execution_preview_only": False,
        "portfolio_simple_return": 0.50,
    }

    inserted = _prepend_latest_signal_row_to_portfolio_history(
        result,
        summary_path=tmp_path / "summary.json",
        summary=summary,
        max_rows=32,
    )

    assert inserted is False
    assert result.rows == [{"date": "2026-07-17"}]


def test_portfolio_history_includes_observed_open_day_trade_target(tmp_path) -> None:
    weights_path = tmp_path / "target_weights.parquet"
    pl.DataFrame(
        {
            "symbol": ["AAA", "BBB"],
            "name": ["Alpha", "Beta"],
            "action": ["BUY", "SELL"],
            "current_weight": [0.0, 0.0],
            "target_weight": [0.4, -0.6],
            "delta_weight": [0.4, -0.6],
            "abs_delta_weight": [0.4, 0.6],
            "current_price": [10.0, 20.0],
            "open_price": [9.5, 19.0],
        }
    ).write_parquet(weights_path)
    summary_path = tmp_path / "summary.json"
    summary_path.write_text("{}", encoding="utf-8")
    result = SimpleNamespace(
        rows=[
            {
                "date": "2026-08-07",
                "portfolio_return": 0.01,
                "benchmark_return": 0.01,
                "profit_value": 10.0,
            }
        ],
        source_paths=(),
        days=2,
        top_changes=2,
        min_abs_change=0.001,
        start_date="2026-08-07",
        end_date="2026-08-07",
        period_return=0.01,
        benchmark_return=0.01,
        profit_value=10.0,
        capital=SimpleNamespace(capital=1_000.0),
        execution_mode="tw_day_trade",
    )
    summary = {
        "weights_date": "2026-08-10 09:57:07",
        "panel_date": "2026-08-10 09:57:08",
        "execution_mode": "tw_day_trade",
        "execution_preview_only": True,
        "live_session_open_feature_applied": True,
        "opening_price_available_count": 2,
        "signal_price_contract": {
            "schema_version": 1,
            "model_observation": "session_open",
            "history_effective_price": "session_open",
            "intraday_prices_allowed_in_portfolio_history": False,
        },
        "execution_constraints_complete": False,
        "execution_constraints_notice": "同日官方當沖資格尚未完整套用",
        "portfolio_simple_return": None,
        "benchmark_simple_return": 0.012,
        "turnover": 1.0,
        "target_risk": {
            "gross": 1.0,
            "net": -0.2,
            "long_gross": 0.4,
            "short_gross": 0.6,
        },
        "weights_path": str(weights_path),
        "price_source": "twse_tpex:mis",
        "price_timestamp": "2026-08-10T01:57:08+00:00",
    }

    inserted = _prepend_latest_signal_row_to_portfolio_history(
        result,
        summary_path=summary_path,
        summary=summary,
        max_rows=2,
    )

    assert inserted is True
    assert result.rows[0]["date"] == "2026-08-10 09:57:07"
    assert result.rows[0]["position_source"] == "signal_target"
    assert result.rows[0]["source"] == "live_signal_open_target"
    assert result.rows[0]["price_contract"] == "session_open_target"
    assert result.rows[0]["intraday_price_included"] is False
    assert result.rows[0]["portfolio_return"] is None
    assert result.rows[0]["change_count"] == 2
    assert result.rows[0]["changes"][0]["symbol"] == "BBB"
    assert result.rows[0]["changes"][0]["price"] == 19.0
    assert result.rows[0]["changes"][0]["entry_price_source"] == "saved_session_open"
    assert result.rows[0]["changes"][0]["price_contract"] == "session_open_target"
    assert result.rows[0]["changes"][0]["intraday_price_included"] is False
    assert result.rows[0]["changes"][0]["price_return"] is None
    block = _portfolio_history_block(result.rows[0])
    assert "source=signal_target" in block
    assert "target_gross=100.00%" in block
    assert "open=19.00" in block
    assert "同日官方當沖資格尚未完整套用" in block


def test_portfolio_history_keeps_one_open_price_signal_not_later_intraday_snapshot(
    monkeypatch,
    tmp_path,
) -> None:
    result = SimpleNamespace(
        rows=[{"date": "2026-08-07", "portfolio_return": 0.01, "profit_value": 10.0}],
        source_paths=(),
        days=2,
        top_changes=1,
        min_abs_change=0.001,
        start_date="2026-08-07",
        end_date="2026-08-07",
        period_return=0.01,
        benchmark_return=0.01,
        profit_value=10.0,
        capital=SimpleNamespace(capital=1_000.0),
        execution_mode="tw_day_trade",
    )
    contract = {
        "schema_version": 1,
        "model_observation": "session_open",
        "history_effective_price": "session_open",
        "intraday_prices_allowed_in_portfolio_history": False,
    }
    signals = []
    for label, asof, target, current_price in (
        ("open", "2026-08-10 09:05:00", 0.1, 12.0),
        ("intraday", "2026-08-10 11:30:00", 0.9, 18.0),
    ):
        signal_dir = tmp_path / label
        signal_dir.mkdir()
        weights_path = signal_dir / "target_weights.parquet"
        pl.DataFrame(
            {
                "symbol": ["AAA"],
                "action": ["BUY"],
                "current_weight": [0.0],
                "target_weight": [target],
                "delta_weight": [target],
                "abs_delta_weight": [target],
                "open_price": [10.0],
                "current_price": [current_price],
            }
        ).write_parquet(weights_path)
        summary_path = signal_dir / "summary.json"
        summary_path.write_text("{}", encoding="utf-8")
        signals.append(
            (
                summary_path,
                {
                    "asof_date": asof,
                    "weights_date": asof,
                    "previous_weights_data_date": "2026-08-07",
                    "execution_mode": "tw_day_trade",
                    "execution_preview_only": True,
                    "live_session_open_feature_applied": True,
                    "opening_price_available_count": 100,
                    "signal_price_contract": contract,
                    "weights_path": str(weights_path),
                    "turnover": target,
                    "target_risk": {
                        "gross": target,
                        "net": target,
                        "long_gross": target,
                        "short_gross": 0.0,
                    },
                },
            )
        )
    monkeypatch.setattr(
        "services.discord_bot.bot._recent_market_signals",
        lambda cfg, max_summaries: signals,
    )
    monkeypatch.setattr(
        "services.discord_bot.bot._market_fold_dir",
        lambda cfg: tmp_path / "fold",
    )
    cfg = SimpleNamespace(market="tw_day_trade", open_time="09:00")

    _include_live_signals_in_portfolio_history(cfg, result, max_rows=2)

    assert result.rows[0]["date"] == "2026-08-10 09:00:00"
    assert result.rows[0]["gross_ratio"] == 0.1
    assert result.rows[0]["changes"][0]["price"] == 10.0
    assert signals[0][0] in result.source_paths
    assert signals[1][0] not in result.source_paths


def test_portfolio_history_prepend_keeps_panel_display_time(tmp_path) -> None:
    result = SimpleNamespace(
        rows=[
            {
                "date": "2026-06-23",
                "portfolio_return": 0.0,
                "benchmark_return": 0.0,
                "profit_value": 0.0,
            }
        ],
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
        "target_risk": {
            "gross": 0.0,
            "net": 0.0,
            "long_gross": 0.0,
            "short_gross": 0.0,
        },
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


def test_portfolio_history_includes_all_newer_live_signals(
    monkeypatch, tmp_path
) -> None:
    result = SimpleNamespace(
        rows=[
            {
                "date": "2026-06-25",
                "portfolio_return": 0.01,
                "benchmark_return": 0.0,
                "profit_value": 10.0,
            }
        ],
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
    previous_date = "2026-06-25"
    for date_text, ret in (("2026-06-26", 0.02), ("2026-06-30", -0.01)):
        signal_dir = tmp_path / date_text
        signal_dir.mkdir()
        summary_path = signal_dir / "summary.json"
        weights_path = signal_dir / "target_weights.parquet"
        rebalance_path = signal_dir / "rebalance.parquet"
        pl.DataFrame({"symbol": ["AAA"], "target_weight": [0.1]}).write_parquet(
            weights_path
        )
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
            "weights_date": f"{date_text} 13:30:00",
            "previous_weights_data_date": previous_date,
            "portfolio_simple_return": ret,
            "benchmark_simple_return": 0.0,
            "display_capital": 1_000.0,
            "target_risk": {
                "gross": 0.1,
                "net": 0.1,
                "long_gross": 0.1,
                "short_gross": 0.0,
            },
            "weights_path": str(weights_path),
            "rebalance_path": str(rebalance_path),
        }
        summary_path.write_text("{}", encoding="utf-8")
        summaries.append((summary_path, summary))
        previous_date = date_text

    monkeypatch.setattr(
        "services.discord_bot.bot._market_signals", lambda cfg: summaries
    )

    _include_live_signals_in_portfolio_history(cfg, result, max_rows=3)

    assert [row["date"] for row in result.rows] == [
        "2026-06-30 13:30:00",
        "2026-06-26 13:30:00",
        "2026-06-25",
    ]
    assert np.isclose(result.period_return, 1.01 * 1.02 * 0.99 - 1.0)
    assert result.rows[0]["source"] == "latest_live_signal"
    assert result.rows[1]["source"] == "latest_live_signal"


def test_portfolio_history_does_not_bridge_missing_live_signal_dates(
    monkeypatch,
    tmp_path,
) -> None:
    result = SimpleNamespace(
        rows=[{"date": "2026-06-25", "portfolio_return": 0.01}],
        source_paths=(),
        days=2,
        top_changes=0,
        start_date="2026-06-25",
        end_date="2026-06-25",
        period_return=0.01,
        benchmark_return=None,
        profit_value=0.0,
        capital=SimpleNamespace(capital=None),
    )
    summary_path = tmp_path / "summary.json"
    summary_path.write_text("{}", encoding="utf-8")
    summary = {
        "weights_date": "2026-07-14 13:30:00",
        "panel_data_date": "2026-07-14",
        "previous_weights_data_date": "2026-07-13",
        "portfolio_simple_return": 0.50,
    }
    monkeypatch.setattr(
        "services.discord_bot.bot._market_signals",
        lambda cfg: [(summary_path, summary)],
    )

    _include_live_signals_in_portfolio_history(
        SimpleNamespace(market="tw", live_output_dir=str(tmp_path)),
        result,
        max_rows=2,
    )

    assert result.rows == [{"date": "2026-06-25", "portfolio_return": 0.01}]


def test_portfolio_history_excludes_signal_after_canonical_live_weights(
    monkeypatch,
    tmp_path,
) -> None:
    fold_dir = tmp_path / "fold_20"
    fold_dir.mkdir()
    pl.DataFrame({"date": ["2026-07-15 00:00:00"], "AAA": [0.5]}).write_parquet(
        fold_dir / "live_signal_weights.parquet"
    )
    result = SimpleNamespace(
        rows=[{"date": "2026-07-14", "portfolio_return": 0.0}],
        source_paths=(),
        days=2,
        top_changes=0,
        start_date="2026-07-14",
        end_date="2026-07-14",
        period_return=0.0,
        benchmark_return=0.0,
        profit_value=0.0,
        capital=SimpleNamespace(capital=1_000.0),
    )
    summary_path = tmp_path / "bad" / "summary.json"
    summary_path.parent.mkdir()
    summary_path.write_text("{}", encoding="utf-8")
    bad_summary = {
        "panel_data_date": "2026-07-16 00:00:00",
        "weights_date": "2026-07-16 00:00:00",
        "previous_weights_data_date": "2026-07-15 00:00:00",
        "portfolio_simple_return": -0.18,
    }
    monkeypatch.setattr(
        "services.discord_bot.bot._market_fold_dir",
        lambda cfg: fold_dir,
    )
    monkeypatch.setattr(
        "services.discord_bot.bot._market_signals",
        lambda cfg: [(summary_path, bad_summary)],
    )

    _include_live_signals_in_portfolio_history(
        SimpleNamespace(market="tw", live_output_dir=str(tmp_path)),
        result,
        max_rows=2,
    )

    assert [row["date"] for row in result.rows] == ["2026-07-14"]


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


def test_day_trade_portfolio_history_block_labels_open_execution_separately() -> None:
    block = _portfolio_history_block(
        {
            "date": "2026-07-28",
            "execution_mode": "tw_day_trade",
            "portfolio_return": 0.0,
            "benchmark_return": -0.03,
            "profit_value": 0.0,
            "cumulative_return": 0.02,
            "turnover": 0.0,
            "nav": 1_200_000.0,
            "open_nav": 1_200_000.0,
            "close_nav": 1_188_000.0,
            "gross_ratio": 0.0,
            "net_ratio": 0.0,
            "cash_ratio": 1.0,
            "requested_gross_ratio": 1.0,
            "execution_fill_ratio": 0.0,
            "requested_position_count": 100,
            "position_count": 0,
            "long_count": 0,
            "short_count": 0,
            "change_count": 0,
            "change_counts": {},
            "changes": [],
        }
    )

    assert "`open_nav=1,200,000`" in block
    assert "`close_nav=1,188,000`" in block
    assert "`open_gross=0.00%`" in block
    assert "`model_gross=100.00%`" in block
    assert "`open_executed=0.00%`" in block
    assert "`fill=0.00%`" in block
    assert "未成交: 開盤目標受到前一交易日成交量" in block
    assert "`gross=" not in block


def test_day_trade_history_rows_are_recorded_at_market_open(monkeypatch) -> None:
    cfg = SimpleNamespace(
        history_frequency="daily",
        open_time="09:00",
        timezone="Asia/Taipei",
        display_timezone="Asia/Taipei",
    )
    monkeypatch.setattr(
        "services.discord_bot.bot._market_execution_mode",
        lambda _cfg: "tw_day_trade",
    )
    rows = [{"date": "2026-08-05"}]

    _annotate_history_rows_with_display_time(cfg, rows)

    assert rows[0]["display_date"] == "2026-08-05 09:00:00"


def test_history_headers_hide_internal_details_until_debug(tmp_path) -> None:
    cfg = SimpleNamespace(
        market="tw",
        history_frequency="daily",
        timezone="Asia/Taipei",
        display_timezone="Asia/Taipei",
    )
    capital = SimpleNamespace(
        mode="artifact", capital=None, reference_date="2026-06-24"
    )
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

    stock_normal = "\n".join(
        _stock_history_header_lines(cfg, stock_result, debug=False)
    )
    portfolio_normal = "\n".join(
        _portfolio_history_header_lines(cfg, portfolio_result, debug=False)
    )
    stock_debug = "\n".join(_stock_history_header_lines(cfg, stock_result, debug=True))
    portfolio_debug = "\n".join(
        _portfolio_history_header_lines(cfg, portfolio_result, debug=True)
    )

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
            "top_score_drivers": [
                {"symbol": "AAA", "name": "Alpha", "score": 1.0, "target_weight": 0.1}
            ],
            "top_feature_drivers": [
                {"feature": "close_logret_1d", "weighted_abs_value": 0.5}
            ],
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


def test_daily_summary_hides_artifact_details_until_debug(
    monkeypatch, tmp_path
) -> None:
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
    monkeypatch.setattr(
        "services.discord_bot.bot._latest_market_signal",
        lambda cfg: (summary_path, summary),
    )

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
    assert enriched.summary["recent_performance"]["window_label"] == "過去32根1m"
    assert enriched.summary["recent_performance"]["strategy_pnl_value"] == 50_000.0
    assert "上個訊號到現在" in enriched.message
    assert "`baseline=+0.40%`" in enriched.message
    assert "`capital=500,000`" in enriched.message
    assert "`pnl=+6,000`" in enriched.message
    assert "過去32根1m" in enriched.message
    assert "`baseline_pnl=+10,000`" in enriched.message
