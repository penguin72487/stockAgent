import asyncio
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


@pytest.fixture
def idle_model_cache(monkeypatch):
    """No network, filesystem publication, model load or CUDA in these probes."""
    releases = []
    monkeypatch.setattr(discord_bot, "_deployment_warmup_window_open", lambda: False)
    monkeypatch.setattr(discord_bot, "_scheduled_markets", lambda: [])
    monkeypatch.setattr(discord_bot, "_opening_critical_work_pending", lambda: False)
    monkeypatch.setattr(discord_bot, "release_idle_live_cuda_cache", lambda: releases.append("release") or True)
    monkeypatch.setattr(discord_bot.bot, "_postclose_fast_cache_batches", 0, raising=False)
    monkeypatch.setattr(discord_bot.bot, "_postclose_fast_cache_inflight", {})
    monkeypatch.setattr(discord_bot.bot, "_startup_inference_warmup_complete", False)
    monkeypatch.setattr(discord_bot.bot, "_startup_inference_warmup_failure_count", 0)
    monkeypatch.setattr(discord_bot.bot, "_startup_inference_warmup_retry_after", 0.0)
    return releases


@pytest.mark.parametrize("outcome", ["success", "failure", "cancel"])
def test_close_batch_retains_shared_models_between_modes_and_releases_after_exit(
    monkeypatch, idle_model_cache, outcome,
):
    configs = {name: SimpleNamespace(market=name, timezone="Asia/Taipei", min_abs_delta=0.001)
               for name in ("first", "second")}
    monkeypatch.setattr(discord_bot, "_postclose_fast_markets", lambda: list(configs))
    monkeypatch.setattr(discord_bot, "_resolve_market", configs.__getitem__)
    monkeypatch.setattr(discord_bot, "_set_scheduler_poll_seconds", lambda *_: None)
    monkeypatch.setattr(discord_bot, "_postclose_scheduler_poll_seconds", lambda _: 0.1)
    monkeypatch.setattr(discord_bot, "_postclose_fast_clock_key", lambda cfg, *_args, **_kw: cfg.market)
    monkeypatch.setattr(discord_bot, "_scheduled_retry_allowed", lambda *_: True)
    monkeypatch.setattr(discord_bot, "_write_postclose_fast_cache_status_best_effort", lambda *_args, **_kw: None)
    monkeypatch.setattr(discord_bot, "_ensure_signal_ready", lambda _: SimpleNamespace())
    monkeypatch.setattr(discord_bot, "_postclose_fast_signal_context", lambda *_args, **_kw: {})
    monkeypatch.setattr(discord_bot, "_log_exception", lambda *_: None)
    monkeypatch.setattr(discord_bot.bot, "_postclose_fast_completed_keys", set())
    monkeypatch.setattr(discord_bot.bot, "_postclose_fast_retry_after", {})
    seen = []

    async def generate(cfg, *_args, **_kwargs):
        # The per-inference mutex is free here, as it is between real modes.
        assert not discord_bot._MODEL_INFERENCE_LOCK.locked()
        assert await asyncio.to_thread(discord_bot._release_offhours_gpu_cache_sync) is False
        seen.append(cfg.market)
        if cfg.market == "first":
            if outcome == "failure":
                raise RuntimeError("one mode failed")
            if outcome == "cancel":
                raise asyncio.CancelledError()
        await asyncio.sleep(0)
        return SimpleNamespace(summary={"signal_id": cfg.market}), "generated"

    monkeypatch.setattr(discord_bot, "_postclose_fast_result", generate)
    if outcome == "cancel":
        with pytest.raises(asyncio.CancelledError):
            asyncio.run(discord_bot.postclose_fast_cache.coro())
    else:
        asyncio.run(discord_bot.postclose_fast_cache.coro())
    assert seen == (["first"] if outcome == "cancel" else ["first", "second"])
    assert discord_bot.bot._postclose_fast_cache_batches == 0
    assert idle_model_cache == []
    assert discord_bot._release_offhours_gpu_cache_sync() is True
    assert idle_model_cache == ["release"]
    if outcome == "failure":
        assert set(discord_bot.bot._postclose_fast_retry_after) == {"first"}
        assert discord_bot.bot._postclose_fast_completed_keys == {"second"}


def test_timed_out_preview_finishes_once_and_remains_cpu_cached(monkeypatch, idle_model_cache):
    monkeypatch.setattr(discord_bot.bot, "_postclose_fast_state_date", "2026-10-15")
    monkeypatch.setattr(discord_bot.bot, "_postclose_fast_cache", {})
    monkeypatch.setattr(discord_bot, "_postclose_fast_timeout_seconds", lambda: 0.01)
    monkeypatch.setattr(discord_bot, "_mark_postclose_fast_preview", lambda _cfg, result, *_a, **_kw: result)
    monkeypatch.setattr(discord_bot, "_enrich_signal_performance_for_discord", lambda _cfg, result, **_kw: result)
    generated = []

    async def exercise():
        finish = asyncio.Event()

        async def generate(*_args, **_kwargs):
            generated.append("inference")
            await finish.wait()
            return SimpleNamespace(summary={"signal_id": "unchanged"}, weights_rows=[{"symbol": "2330"}],
                                   rebalance_rows=[], decision_rows=[], message="raw", output_dir=None)

        monkeypatch.setattr(discord_bot, "_generate_postclose_fast_signal", generate)
        args = (SimpleNamespace(market="cash"), SimpleNamespace(), {"session_date": "2026-10-15"})
        kwargs = dict(price_source="tw", top_n=20, min_abs_delta=0.001,
                      include_unconstrained_raw_scores=False, debug=False)
        with pytest.raises(asyncio.TimeoutError):
            await discord_bot._postclose_fast_result(*args, **kwargs)
        pending = list(discord_bot.bot._postclose_fast_cache_inflight.values())
        assert len(pending) == 1 and not pending[0].done()
        # Caller timeout/cancellation must not evict a shielded live producer.
        assert await asyncio.to_thread(discord_bot._release_offhours_gpu_cache_sync) is False
        finish.set()
        await pending[0]
        await asyncio.sleep(0)
        assert not discord_bot.bot._postclose_fast_cache_inflight
        assert len(discord_bot.bot._postclose_fast_cache) == 1
        result, source = await discord_bot._postclose_fast_result(*args, **kwargs)
        assert source == "memory" and result.summary["signal_id"] == "unchanged"
        assert result.weights_rows == [{"symbol": "2330"}]
        result.weights_rows.clear()
        result2, source2 = await discord_bot._postclose_fast_result(*args, **kwargs)
        assert source2 == "memory" and result2.weights_rows == [{"symbol": "2330"}]

    asyncio.run(exercise())
    assert generated == ["inference"]
    assert discord_bot._release_offhours_gpu_cache_sync() is True
    assert idle_model_cache == ["release"]


def test_offhours_release_rechecks_eligibility_after_acquiring_model_mutex(monkeypatch, idle_model_cache):
    eligibility = iter((True, False))
    monkeypatch.setattr(discord_bot, "_offhours_gpu_cache_release_allowed", lambda: next(eligibility))
    assert discord_bot._release_offhours_gpu_cache_sync() is False
    assert not discord_bot._MODEL_INFERENCE_LOCK.locked()
    assert idle_model_cache == []
