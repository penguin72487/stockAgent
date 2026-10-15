from datetime import datetime
from types import SimpleNamespace

import pytest

from services.discord_bot import bot as discord_bot


@pytest.mark.parametrize("clock,release_allowed,warm_deferred", [
    ("06:59:59", True, True), ("07:00:00", True, True), ("07:30:00", True, True),
    ("08:04:59", True, True), ("08:05:00", False, False), ("08:14:59", False, False),
    ("08:15:00", False, True), ("09:02:00", False, True), ("09:20:00", False, False),
    ("13:19:59", False, False), ("13:20:00", False, True), ("13:30:00", True, True),
    ("17:00:00", True, True),
])
def test_shared_warm_and_release_window_preserves_opening_and_close_boundaries(
    monkeypatch, clock, release_allowed, warm_deferred,
):
    cfg = SimpleNamespace(market="cash", timezone="Asia/Taipei", day_trade_simulation_enabled=True,
                          preopen_prepare_time="08:15", open_time="09:00", close_time="13:30")
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime.fromisoformat(f"2026-10-14T{clock}+08:00")
    monkeypatch.setattr(discord_bot, "datetime", Clock)
    monkeypatch.setattr(discord_bot, "_env_float", lambda _key, default: default)
    monkeypatch.setattr(discord_bot, "_deployment_warmup_window_open", lambda: False)
    monkeypatch.setattr(discord_bot, "_scheduled_markets", lambda: [cfg.market])
    monkeypatch.setattr(discord_bot, "_resolve_market", lambda _: cfg)
    monkeypatch.setattr(discord_bot, "_scheduled_market_session_day", lambda *_: (True, "verified"))
    monkeypatch.setattr(discord_bot, "_day_trade_schedule_state", lambda *_: "retry")
    monkeypatch.setattr(discord_bot, "_opening_critical_work_pending", lambda: False)
    monkeypatch.setattr(discord_bot.bot, "_opening_attempt_started_monotonic", None)
    assert discord_bot._offhours_gpu_cache_release_allowed() is release_allowed
    assert discord_bot._startup_inference_warmup_must_defer() is warm_deferred


@pytest.mark.parametrize("lead,expected", [
    (600, 485), (120, 493), (1, 494), (9000, 420),
    (float("nan"), 485), (float("inf"), 485), ("invalid", 485),
])
def test_lead_override_is_bounded_and_invalid_values_use_measured_default(monkeypatch, lead, expected):
    cfg = SimpleNamespace(preopen_prepare_time="08:15")
    monkeypatch.setattr(discord_bot, "_env_float", lambda *_args: float(lead))
    assert discord_bot._startup_gpu_protection_start_minutes(cfg) == expected


@pytest.mark.parametrize("prepare", ["invalid", "25:00", "08:75", "-1:00"])
def test_unknown_prepare_clock_retains_original_fail_closed_protection(monkeypatch, prepare):
    assert discord_bot._startup_gpu_prepare_minutes(SimpleNamespace(preopen_prepare_time=prepare)) is None
    assert discord_bot._startup_gpu_protection_start_minutes(SimpleNamespace(preopen_prepare_time=prepare)) == 420


@pytest.mark.parametrize("released", [False, True])
def test_released_models_cannot_leave_a_sticky_process_hot_flag(monkeypatch, released):
    monkeypatch.setattr(discord_bot, "release_idle_live_cuda_cache", lambda: released)
    monkeypatch.setattr(discord_bot.bot, "_startup_inference_warmup_complete", True)
    monkeypatch.setattr(discord_bot.bot, "_startup_inference_warmup_failure_count", 3)
    monkeypatch.setattr(discord_bot.bot, "_startup_inference_warmup_retry_after", 100.0)
    monkeypatch.setattr(discord_bot.bot, "_startup_inference_warmup_terminal", True)
    with discord_bot._MODEL_INFERENCE_LOCK:
        assert discord_bot._release_live_cuda_cache_owned() is released
    assert discord_bot.bot._startup_inference_warmup_complete is (not released)
    assert discord_bot.bot._startup_inference_warmup_terminal  # No indefinite retry of permanent failures.
    assert discord_bot.bot._startup_inference_warmup_failure_count == (0 if released else 3)
    assert discord_bot.bot._startup_inference_warmup_retry_after == (0.0 if released else 100.0)
