from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
import json
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

pytest.importorskip("discord")
from services.discord_bot import bot as module


def config(**kwargs):
    return SimpleNamespace(market="test", timezone="Asia/Taipei", close_time="13:30",
                           day_trade_simulation_enabled=True,
                           overnight_simulation_enabled=False, **kwargs)


def clock(monkeypatch, value):
    instant = datetime.fromisoformat(value)

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return instant.astimezone(tz) if tz else instant.replace(tzinfo=None)

    monkeypatch.setattr(module, "datetime", Clock)


@pytest.fixture(autouse=True)
def scheduling(monkeypatch):
    monkeypatch.setattr(module, "_market_schedule_time", lambda cfg: "09:00")
    monkeypatch.setattr(module, "_market_schedule_interval_minutes", lambda cfg: None)
    monkeypatch.setattr(module, "_market_schedule_delay_seconds", lambda cfg: 0)
    yield
    module.scheduled_signal.change_interval(seconds=0.1)
    module.postclose_fast_cache.change_interval(seconds=0.1)


@pytest.mark.parametrize("at,expected", [
    ("08:59:29.999999", 5), ("08:59:30", .1),
    ("08:59:59.999999", .1), ("09:00:00", .1),
    ("09:00:00.000001", 5), ("19:00:00", 5),
])
def test_daily_guard_is_not_an_eligibility_window(monkeypatch, at, expected):
    clock(monkeypatch, f"2026-10-14T{at}+08:00")
    assert module._scheduler_poll_seconds([config()]) == expected


@pytest.mark.parametrize("interval,delay,at,expected", [
    (15, 10, "12:14:39.999999", 5), (15, 10, "12:14:40", .1),
    (15, 10, "12:15:09.999999", .1), (15, 10, "12:15:10", 5),
    (1, 0, "12:00:29.999999", 5), (1, 0, "12:00:30", .1),
    (100, 0, "23:59:30", .1), (2880, 0, "23:59:30", .1),
    (2880, 90010, "00:59:40", .1),
])
def test_interval_delay_and_date_rollover(monkeypatch, interval, delay, at, expected):
    clock(monkeypatch, f"2026-10-14T{at}+08:00")
    monkeypatch.setattr(module, "_market_schedule_interval_minutes", lambda cfg: interval)
    monkeypatch.setattr(module, "_market_schedule_delay_seconds", lambda cfg: delay)
    assert module._scheduler_poll_seconds([config()]) == expected


def test_pending_confirmations_and_jobs_do_not_use_idle_cadence(monkeypatch):
    clock(monkeypatch, "2026-10-14T19:00:00+08:00")
    assert module._scheduler_poll_seconds([config()], pending=True) == .1
    assert module._scheduler_poll_seconds([], pending=True) == .1
    assert module._scheduler_poll_seconds([]) == 5


def test_any_market_deadline_uses_its_own_timezone(monkeypatch):
    clock(monkeypatch, "2026-10-14T21:29:40+08:00")
    cfgs = [config(), SimpleNamespace(market="us", timezone="America/New_York")]
    monkeypatch.setattr(module, "_market_schedule_time", lambda cfg: "09:00" if cfg.market == "test" else "09:30")
    assert module._scheduler_poll_seconds(cfgs) == .1


@pytest.mark.parametrize("fold", [0, 1])
def test_dst_fold_keeps_original_wall_clock_deadline(monkeypatch, fold):
    local = datetime(2026, 11, 1, 1, 29, 40, tzinfo=ZoneInfo("America/New_York"), fold=fold)
    clock(monkeypatch, local.isoformat())
    monkeypatch.setattr(module, "_market_schedule_time", lambda cfg: "01:30")
    assert module._scheduler_poll_seconds([SimpleNamespace(market="us", timezone="America/New_York")]) == .1


@pytest.mark.parametrize("at,completed,expected", [
    ("13:29:29.999999", False, 5), ("13:29:30", False, .1),
    ("13:30:00", False, .1), ("13:40:00", False, .1),
    ("13:40:00", True, 5), ("15:00:00", False, .1),
    ("15:00:00.000001", False, 5), ("19:00:00", False, 5),
])
def test_close_completion_is_idle_but_pending_retry_stays_fast(monkeypatch, at, completed, expected):
    cfg = config()
    clock(monkeypatch, f"2026-10-14T{at}+08:00")
    monkeypatch.setattr(module, "_resolve_market", lambda _: cfg)
    monkeypatch.setattr(module, "_postclose_fast_window_minutes", lambda: 90)
    monkeypatch.setattr(module.bot, "_postclose_fast_completed_keys", set())
    key = module._postclose_fast_clock_key(cfg, module.datetime.now(ZoneInfo(cfg.timezone)), phase="cache")
    if completed:
        module.bot._postclose_fast_completed_keys.add(key)
    assert module._postclose_scheduler_poll_seconds([cfg.market]) == expected


def test_interval_changes_only_when_needed():
    calls = []
    loop = SimpleNamespace(seconds=.1, change_interval=lambda **kw: calls.append(kw))
    module._set_scheduler_poll_seconds(loop, .1)
    module._set_scheduler_poll_seconds(loop, 5)
    assert calls == [{"seconds": 5}]


def test_sort_uses_one_current_readiness_snapshot_without_ttl(monkeypatch, tmp_path):
    path = tmp_path / "ready.json"
    payload = {"markets": {"test": {"symbol_count": 2757}, "other": {"symbol_count": 30}}}
    path.write_text(json.dumps(payload))
    monkeypatch.setattr(module, "_preopen_readiness_path", lambda: path)
    calls = []
    original = Path.read_text

    def counted(path, *args, **kwargs):
        calls.append(path)
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", counted)
    current = module._preopen_readiness_payload()
    counts = [module._preopen_market_symbol_count(config(), readiness=current) for _ in range(5)]
    assert counts == [2757] * 5 and calls == [path]
    path.write_text('{"markets":{"test":{"symbol_count":42}}}')
    assert module._preopen_market_symbol_count(config()) == 42
    assert calls == [path, path]


@pytest.mark.parametrize("content", [None, "not-json", "{}", '{"markets":{}}'])
def test_readiness_failure_retains_original_zero_count(monkeypatch, tmp_path, content):
    path = tmp_path / "ready.json"
    if content is not None:
        path.write_text(content)
    monkeypatch.setattr(module, "_preopen_readiness_path", lambda: path)
    assert module._preopen_market_symbol_count(config()) == 0


def forbid(*args, **kwargs):
    raise AssertionError("Idle callback reached compute/provider/trading")


def test_closed_calendar_remains_fail_closed_with_fast_clock(monkeypatch):
    cfg = config()
    clock(monkeypatch, "2026-10-14T08:59:50+08:00")
    monkeypatch.setattr(module, "_scheduled_markets", lambda: [cfg.market])
    monkeypatch.setattr(module, "_resolve_market", lambda _: cfg)
    monkeypatch.setattr(module, "_preopen_readiness_payload", lambda: {})
    monkeypatch.setattr(module, "_scheduled_market_session_day", lambda *_: (False, "unknown"))
    monkeypatch.setattr(module, "_prepare_scheduled_attempt_inputs", forbid)
    monkeypatch.setattr(module, "_ensure_signal_ready", forbid)
    monkeypatch.setattr(module, "_enqueue_scheduled_results", forbid)
    monkeypatch.setattr(module.bot, "_scheduled_input_jobs", {})
    asyncio.run(module.scheduled_signal())
    assert module.scheduled_signal.seconds == .1


@pytest.mark.parametrize("state", ["pending_confirmation", "blocked_open_position"])
def test_pending_broker_state_retains_fast_original_no_inference(monkeypatch, state):
    cfg = config()
    clock(monkeypatch, "2026-10-14T09:10:00+08:00")
    monkeypatch.setattr(module, "_scheduled_markets", lambda: [cfg.market])
    monkeypatch.setattr(module, "_resolve_market", lambda _: cfg)
    monkeypatch.setattr(module, "_preopen_readiness_payload", lambda: {})
    monkeypatch.setattr(module, "_scheduled_market_session_day", lambda *_: (True, "open"))
    monkeypatch.setattr(module, "_day_trade_schedule_state", lambda *_: state)
    monkeypatch.setattr(module, "_prepare_scheduled_attempt_inputs", forbid)
    monkeypatch.setattr(module, "_ensure_signal_ready", forbid)
    monkeypatch.setattr(module.bot, "_last_scheduled_keys", set())
    monkeypatch.setattr(module.bot, "_scheduled_input_jobs", {})
    asyncio.run(module.scheduled_signal())
    assert module.scheduled_signal.seconds == .1


def test_completed_close_callback_is_noop_and_sleeps_without_compute(monkeypatch):
    cfg = config()
    clock(monkeypatch, "2026-10-14T13:40:00+08:00")
    monkeypatch.setattr(module, "_postclose_fast_markets", lambda: [cfg.market])
    monkeypatch.setattr(module, "_resolve_market", lambda _: cfg)
    key = module._postclose_fast_clock_key(cfg, module.datetime.now(ZoneInfo(cfg.timezone)), phase="cache")
    monkeypatch.setattr(module.bot, "_postclose_fast_completed_keys", {key})
    monkeypatch.setattr(module, "_ensure_signal_ready", forbid)
    monkeypatch.setattr(module, "_write_postclose_fast_cache_status_best_effort", forbid)
    asyncio.run(module.postclose_fast_cache())
    assert module.postclose_fast_cache.seconds == 5


def test_real_discord_task_changes_interval_before_clock_deadline(monkeypatch):
    from discord.ext import tasks

    cfg = config()
    instants = iter(["08:59:29", "08:59:30", "09:00:00"])
    observed = []

    @tasks.loop(seconds=.1)
    async def loop():
        clock(monkeypatch, f"2026-10-14T{next(instants)}+08:00")
        module._set_scheduler_poll_seconds(loop, module._scheduler_poll_seconds([cfg]))
        observed.append(loop.seconds)
        if len(observed) == 3:
            loop.stop()

    async def execute():
        await asyncio.wait_for(loop.start(), timeout=8)

    asyncio.run(execute())
    assert observed == [5, .1, .1]
