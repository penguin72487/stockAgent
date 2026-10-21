from __future__ import annotations

import asyncio
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import threading
import time
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import numpy as np
import pytest

from services.discord_bot import bot as discord_bot
from stockagent.live import quote_provider, signal_engine
from stockagent.live.completed_inference_cache import LoadedImplementationChanged


def test_changed_loaded_code_rejects_before_uncancellable_prewarm_source_builder(monkeypatch):
    def changed():
        raise LoadedImplementationChanged()

    monkeypatch.setattr(discord_bot, "loaded_implementation_identity", changed)
    monkeypatch.setattr(discord_bot, "_prewarm_market_signal_serialized",
        lambda *_: pytest.fail("inadmissible process must not start a source/model build"))
    # No config, cache, lock or source access is needed to reject code drift.
    with pytest.raises(LoadedImplementationChanged):
        discord_bot._prewarm_market_signal_sync(object())


@pytest.mark.parametrize("completed_session", [False, True])
def test_changed_loaded_code_rejects_before_realtime_freshness_or_source_refresh(monkeypatch, completed_session):
    def changed():
        raise LoadedImplementationChanged()

    monkeypatch.setattr(discord_bot, "loaded_implementation_identity", changed)
    monkeypatch.setattr(discord_bot, "_ensure_signal_ready",
        lambda *_: pytest.fail("code drift must be checked before source scans"))
    monkeypatch.setattr(discord_bot, "_run_pre_signal_command",
        lambda *_: pytest.fail("inadmissible process must not run a source hook"))
    monkeypatch.setattr(discord_bot, "_run_completed_session_command",
        lambda *_: pytest.fail("inadmissible process must not run a completed-session hook"))
    with pytest.raises(LoadedImplementationChanged):
        discord_bot._prepare_realtime_signal_sync(object(), force_refresh=True, completed_session=completed_session)


def test_restart_recovers_original_observed_clock_not_new_process_wake(monkeypatch, tmp_path):
    now = datetime.fromisoformat("2026-10-13T09:20:00+08:00")

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return now

    path = tmp_path / "opening_signal_latency.jsonl"
    base = {"session_date": "2026-10-13", "market": "test", "schedule_key": "2026-10-13:test", "gate_at": "2026-10-13T09:00:00+08:00"}
    rows = [
        {**base, "first_scheduler_observed_at": "2026-10-13T09:17:00+08:00"},
        {**base, "first_scheduler_observed_at": "2026-10-13T09:00:00.003+08:00"},
        {**base, "first_scheduler_observed_at": "2026-10-12T09:00:00+08:00"},
        {**base, "first_scheduler_observed_at": "2026-10-13T09:30:00+08:00"},
        {**base, "first_scheduler_observed_at": "2026-10-13T09:00:00.001"},
    ]
    path.write_text("\n".join(json.dumps(row) for row in rows) + '\n{"session_date":"2026-10-13",broken\n')
    monkeypatch.setattr(discord_bot, "datetime", Clock)
    monkeypatch.setattr(discord_bot, "_opening_signal_latency_path", lambda: path)
    restored = discord_bot._recover_opening_first_observations("2026-10-13")
    assert restored == {"2026-10-13:test": datetime.fromisoformat("2026-10-13T09:00:00.003+08:00")}
    assert discord_bot._recover_opening_first_observations("2026-10-12") == {}


@pytest.mark.parametrize("at,active,session_open,state,defer", [
    ("07:30", None, True, "retry", True),
    ("08:10", None, True, "retry", False),
    ("09:02", None, True, "retry", True),
    ("09:20", None, True, "retry", False),
    ("09:20", 1., True, "retry", True),
    ("09:20", None, True, "pending_confirmation", False),
    ("09:20", None, True, "completed", False),
    ("09:20", None, True, "blocked_open_position", False),
    ("09:20", 1., True, "completed", True),
    ("13:20", None, True, "retry", True),
    ("09:20", None, False, "retry", True),
])
def test_cold_owner_can_warm_while_waiting_after_protected_opening(monkeypatch, at, active, session_open, state, defer):
    cfg = SimpleNamespace(market="test", timezone="Asia/Taipei", day_trade_simulation_enabled=True,
                          preopen_prepare_time="08:15", open_time="09:00")
    now = datetime.fromisoformat(f"2026-10-13T{at}:00+08:00")

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return now

    monkeypatch.setattr(discord_bot, "datetime", Clock)
    monkeypatch.setattr(discord_bot, "_scheduled_markets", lambda: [cfg.market])
    monkeypatch.setattr(discord_bot, "_resolve_market", lambda market: cfg)
    monkeypatch.setattr(discord_bot, "_scheduled_market_session_day", lambda *args: (session_open, "fixture"))
    monkeypatch.setattr(discord_bot, "_day_trade_schedule_state", lambda *args: state)
    monkeypatch.setattr(discord_bot.bot, "_opening_attempt_started_monotonic", active)
    assert discord_bot._startup_inference_warmup_must_defer() is defer


def test_background_warmup_does_not_steal_an_interactive_job(monkeypatch):
    monkeypatch.setattr(discord_bot, "_postclose_fast_priority_window_active", lambda: False)
    monkeypatch.setattr(discord_bot, "_interactive_signal_work_pending", lambda: True)
    monkeypatch.setattr(discord_bot, "_warm_startup_inference_sync", lambda: pytest.fail("interactive job has priority"))
    asyncio.run(discord_bot.startup_inference_warmup())


def test_opening_input_wait_records_structured_counts_without_result():
    cfg = SimpleNamespace(market="test", timezone="Asia/Taipei", open_time="09:00")
    with pytest.raises(signal_engine.TWOpeningInputCoveragePending) as caught:
        signal_engine._require_day_trade_opening_coverage(
            live_session=True, active=2290, opens=1889, no_prints=0,
            responses=2290, required=2061, model_observation="session_open",
        )
    record = discord_bot._opening_signal_latency_record(
        cfg=cfg, session_date="2026-10-12", schedule_key="fixture", timing={},
        result=None, error=caught.value,
    )
    assert record["status"] == "failed"
    assert record["error_code"] == "tw_opening_input_coverage_pending"
    assert record["opening_input_coverage"]["required"] == 2061
    assert record["signal_id"] is None
    assert record["ready_from_open_ms"] is None


def test_opening_repair_keeps_failed_attempt_and_retries_without_inference(monkeypatch, tmp_path):
    monkeypatch.setattr(discord_bot, "_day_trade_state_dir", lambda: tmp_path)

    def fail(**kwargs):
        raise ConnectionError("source unavailable")

    monkeypatch.setattr(discord_bot, "repair_tw_opening_snapshot_receipt", fail)
    first = discord_bot._opening_source_repair_sync(str(tmp_path), "2026-10-07", 1)
    assert first["status"] == "failed"
    assert first["error_type"] == "ConnectionError"
    monkeypatch.setattr(discord_bot, "repair_tw_opening_snapshot_receipt", lambda **kwargs: {
        "status": "complete", "valid_open_count": 2294, "missing_open_count": 0,
    })
    second = discord_bot._opening_source_repair_sync(str(tmp_path), "2026-10-07", 2)
    assert second["status"] == "complete"
    attempts = [__import__("json").loads(line) for line in (tmp_path / "opening_source_repair_latency.jsonl").read_text().splitlines()]
    assert [row["status"] for row in attempts] == ["failed", "complete"]
    assert all(not row["inference_performed"] and not row["orders_submitted"] for row in attempts)
    assert all(row["observer_policy"] == "shared_provider_backoff_no_source_wait_starvation_v1" for row in attempts)
    assert second["minimum_observer_retry_delay_seconds"] == 60.0


@pytest.mark.parametrize("busy", [True, False])
@pytest.mark.parametrize("started,stage,expected_calls", [
    (None, None, 1), (1.0, "preopen_catch_up", 1),
    (1.0, "realtime_prepare", 1), (1.0, "inference", 0),
    (1.0, "started", 0), (1.0, None, 0),
])
def test_background_opening_repair_runs_between_retries_without_circular_wait(
    monkeypatch, tmp_path, busy, started, stage, expected_calls,
):
    now = datetime.fromisoformat("2026-10-07T09:00:10+08:00")

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return now

    cfg_path = tmp_path / "config.yaml"
    cfg_path.write_text("fixture")
    cfg = SimpleNamespace(day_trade_simulation_enabled=True, config_path=str(cfg_path))
    calls = []
    monkeypatch.setattr(discord_bot, "datetime", Clock)
    monkeypatch.setattr(discord_bot, "_market_configs", lambda: {"test": cfg})
    monkeypatch.setattr(discord_bot, "_market_enabled", lambda cfg: True)
    monkeypatch.setattr(discord_bot, "_scheduled_market_session_day", lambda *a: (True, "fixture"))
    monkeypatch.setattr(discord_bot, "_day_trade_schedule_state", lambda *a: "retry" if busy else "completed")
    monkeypatch.setattr(discord_bot.bot, "_opening_attempt_started_monotonic", started)
    monkeypatch.setattr(discord_bot.bot, "_opening_attempt_stage", stage)
    monkeypatch.setattr(discord_bot.bot, "_opening_source_repair_retry_at", {})
    monkeypatch.setattr(discord_bot.bot, "_opening_source_repair_attempts", {})
    monkeypatch.setattr(discord_bot, "_opening_source_repair_root", lambda *a: str(tmp_path))

    def repair(*args):
        calls.append(args)
        return {"status": "waiting_source", "recovered_count": 0}

    monkeypatch.setattr(discord_bot, "_opening_source_repair_sync", repair)
    asyncio.run(discord_bot.opening_source_repair.coro())
    asyncio.run(discord_bot.opening_source_repair.coro())
    assert len(calls) == expected_calls


@pytest.mark.parametrize("at,status,progress,attempt,expected", [
    ("09:00:00", "not_started", 0, 1, 1.0),
    ("09:05:00", "not_started", 0, 20, 1.0),
    ("09:00:59", "waiting_source", 0, 20, 0.5),
    ("09:00:59", "failed", 0, 20, 0.5),
    ("09:01:00", "waiting_source", 0, 20, 30.0),
    ("09:01:00", "awaiting_first_print", 1, 20, 0.5),
    ("09:00:10", "complete", 0, 1, 60.0),
])
def test_opening_source_observer_delay_does_not_compound_provider_backoff(
    at, status, progress, attempt, expected,
):
    result = {"status": status, "recovered_count": 0, "recovered_input_count": progress}
    now = datetime.fromisoformat(f"2026-10-14T{at}+08:00")
    assert discord_bot._opening_source_repair_delay(result, attempt, now) == expected


@pytest.mark.parametrize("providers,expected", [
    ({"healthy": {"retry_after_monotonic": 108.}, "failed": {"retry_after_monotonic": 160.}}, 8.),
    ({"inflight": {"state": "running"}, "failed": {"retry_after_monotonic": 160.}}, .5),
    ({"ready": {"retry_after_monotonic": 99.}}, .5),
    ({"failed": {"retry_after_monotonic": 160.}}, 30.),
    ({"bad": {"retry_after_monotonic": float("nan")}, "good": {"retry_after_monotonic": 102.}}, 2.),
    ({"bad": {"retry_after_monotonic": float("inf")}}, 30.),
    ({"bad": {"retry_after_monotonic": True}}, 30.),
    ({"bad": None, "other": "invalid"}, 30.),
])
def test_opening_source_observer_aligns_with_earliest_provider_not_failed_peer(monkeypatch, providers, expected):
    monkeypatch.setattr(discord_bot.time, "monotonic", lambda: 100.)
    result = {"status": "waiting_source", "opening_acquisition": {"opening_providers": providers}}
    now = datetime.fromisoformat("2026-10-19T09:12:00+08:00")
    assert discord_bot._opening_source_repair_delay(result, 60, now) == expected


def test_opening_observer_does_not_add_receipt_io_to_provider_retry(monkeypatch, tmp_path):
    now = datetime.fromisoformat("2026-10-19T09:12:00+08:00")

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return now

    cfg_path = tmp_path / "config.yaml"
    cfg_path.write_text("fixture")
    cfg = SimpleNamespace(day_trade_simulation_enabled=True, config_path=str(cfg_path))
    monkeypatch.setattr(discord_bot, "datetime", Clock)
    monkeypatch.setattr(discord_bot.time, "monotonic", lambda: 100.)
    monkeypatch.setattr(discord_bot, "_market_configs", lambda: {"test": cfg})
    monkeypatch.setattr(discord_bot, "_market_enabled", lambda cfg: True)
    monkeypatch.setattr(discord_bot, "_scheduled_market_session_day", lambda *a: (True, "fixture"))
    monkeypatch.setattr(discord_bot.bot, "_opening_attempt_started_monotonic", None)
    monkeypatch.setattr(discord_bot.bot, "_opening_source_repair_retry_at", {})
    monkeypatch.setattr(discord_bot.bot, "_opening_source_repair_attempts", {})
    monkeypatch.setattr(discord_bot, "_opening_source_repair_root", lambda *a: str(tmp_path))
    monkeypatch.setattr(discord_bot, "_opening_source_repair_sync", lambda *a: {
        "status": "waiting_source", "minimum_observer_retry_delay_seconds": 9.,
        "opening_acquisition": {"opening_providers": {"finmind": {"retry_after_monotonic": 101.}}},
    })
    asyncio.run(discord_bot.opening_source_repair.coro())
    assert discord_bot.bot._opening_source_repair_retry_at[now.date().isoformat() + ":" + str(tmp_path)] == 101.


@pytest.mark.parametrize("at,interval", [("08:59:29", 1.), ("08:59:30", .1),
    ("09:00:00", .1), ("09:00:10", .1), ("09:00:11", 1.), ("10:00:00", 1.)])
def test_source_owner_enters_fast_wake_before_gate_independently_of_models(monkeypatch, at, interval):
    now = datetime.fromisoformat("2026-10-21T" + at + "+08:00")
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return now.astimezone(tz or now.tzinfo)
    calls = []
    monkeypatch.setattr(discord_bot, "datetime", Clock)
    monkeypatch.setattr(discord_bot, "_market_configs", lambda: {})
    monkeypatch.setattr(discord_bot, "_opening_source_repair_must_defer", lambda: False)
    monkeypatch.setattr(discord_bot, "_set_scheduler_poll_seconds", lambda loop, seconds: calls.append(seconds))
    asyncio.run(discord_bot.opening_source_repair.coro())
    assert calls == [interval]


@pytest.mark.parametrize("at,expected", [("08:59:29", 0), ("08:59:30", 1),
    ("08:59:49", 1), ("08:59:50", 0), ("08:59:59", 0)])
def test_source_transport_renews_once_before_gate_not_as_a_late_opening_dependency(monkeypatch, tmp_path, at, expected):
    now = datetime.fromisoformat("2026-10-21T" + at + "+08:00")
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return now.astimezone(tz or now.tzinfo)
    cfg_path = tmp_path / "config.yaml"
    cfg_path.write_text("fixture")
    cfg = SimpleNamespace(day_trade_simulation_enabled=True, config_path=str(cfg_path))
    calls = []
    monkeypatch.setattr(discord_bot, "datetime", Clock)
    monkeypatch.setattr(discord_bot, "_market_configs", lambda: {"test": cfg})
    monkeypatch.setattr(discord_bot, "_market_enabled", lambda *_: True)
    monkeypatch.setattr(discord_bot, "_scheduled_market_session_day", lambda *a: (True, "fixture"))
    monkeypatch.setattr(discord_bot, "_opening_source_repair_must_defer", lambda: False)
    monkeypatch.setattr(discord_bot, "_opening_source_repair_root", lambda *a: str(tmp_path))
    monkeypatch.setattr(discord_bot, "_set_scheduler_poll_seconds", lambda *a: None)
    monkeypatch.setattr(discord_bot.bot, "_opening_source_repair_retry_at", {})
    monkeypatch.setattr(discord_bot, "_opening_source_transport_prewarm_sync", lambda *a: calls.append(a))
    monkeypatch.setattr(discord_bot, "_opening_source_repair_sync", lambda *a: pytest.fail("no source acquisition before 09:00"))
    asyncio.run(discord_bot.opening_source_repair.coro())
    asyncio.run(discord_bot.opening_source_repair.coro())
    assert len(calls) == expected


def test_overlapping_commands_share_work_cancel_independently_and_next_call_is_fresh(monkeypatch):
    calls = []
    release = asyncio.Event()

    async def generate(**kwargs):
        calls.append(kwargs)
        await release.wait()
        return SimpleNamespace(summary={"signal_id": str(len(calls))}, message="ready", output_dir="proof")

    monkeypatch.setattr(discord_bot, "_run_market_signal", generate)
    monkeypatch.setattr(discord_bot, "_intraday_waits_for_opening", lambda: False)
    monkeypatch.setattr(discord_bot, "_enrich_signal_performance_for_discord", lambda _cfg, result, **_kw: result)
    monkeypatch.setattr(discord_bot.bot, "_intraday_signal_inflight", {})
    cfg = SimpleNamespace(market="tw_day_trade_test")
    status = SimpleNamespace(data=SimpleNamespace(fresh=True))
    kwargs = dict(price_source="tw", top_n=20, min_abs_delta=0.001,
                  include_unconstrained_raw_scores=False, debug=False)

    async def exercise():
        first = asyncio.create_task(discord_bot._intraday_signal_now_result(cfg, status, **kwargs))
        second = asyncio.create_task(discord_bot._intraday_signal_now_result(cfg, status, **kwargs))
        while not calls:
            await asyncio.sleep(0.001)
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        release.set()
        shared = await second
        fresh = await discord_bot._intraday_signal_now_result(cfg, status, **kwargs)
        return shared, fresh

    shared, fresh = asyncio.run(exercise())
    assert shared.summary["signal_now_singleflight_joined"] is True
    assert shared.summary["signal_id"] == "1"
    assert fresh.summary["signal_id"] == "2"
    assert len(calls) == 2
    assert calls[0]["publish_latest"] is False
    assert calls[0]["prepared_status"] is status
    assert calls[0]["_prefetch_prices"] is True
    assert not discord_bot.bot._intraday_signal_inflight


def test_quote_io_can_overlap_but_model_runtime_stays_serialized(monkeypatch):
    quote_barrier = threading.Barrier(2)
    running = 0
    peak = 0
    mutex = threading.Lock()

    def prefetch(**kwargs):
        # This barrier cannot complete if either worker holds the model lock
        # during network I/O. Use events, not latency thresholds, for proof.
        quote_barrier.wait(timeout=2)
        return kwargs["market"]

    def infer(**kwargs):
        nonlocal running, peak
        with mutex:
            running += 1
            peak = max(peak, running)
        time.sleep(0.01)
        with mutex:
            running -= 1
        return kwargs["_prefetched_quote"]

    monkeypatch.setenv("STOCKAGENT_BOT_PROGRESS", "0")
    monkeypatch.setattr(discord_bot, "_signal_kwargs", lambda **kwargs: kwargs)
    monkeypatch.setattr(discord_bot, "prefetch_live_signal_prices", prefetch)
    monkeypatch.setattr(discord_bot, "generate_live_signal", infer)
    with ThreadPoolExecutor(max_workers=2) as executor:
        jobs = [executor.submit(discord_bot._run_market_signal_sync, market=name, _prefetch_prices=True)
                for name in ("a", "b")]
        assert [job.result(timeout=3) for job in jobs] == ["a", "b"]
    assert peak == 1


def test_scheduled_prefetch_overlaps_and_ready_mode_is_not_held_by_slow_mode(monkeypatch):
    started = {name: asyncio.Event() for name in ("slow", "fast")}
    release = asyncio.Event()
    source_calls = []

    async def prepare(cfg):
        source_calls.append(cfg.market)
        started[cfg.market].set()
        if cfg.market == "slow":
            await release.wait()
        return ("tw", cfg.market, False), None, 1.0, cfg.market, {}

    monkeypatch.setattr(discord_bot, "_prepare_scheduled_inputs", prepare)

    async def exercise():
        due = [(SimpleNamespace(market=name), None, name, False) for name in ("slow", "fast")]
        tasks = {row[0].market: asyncio.create_task(discord_bot._prepare_scheduled_inputs(row[0])) for row in due}
        iterator = discord_bot._scheduled_inputs_ready_order(due, tasks)
        row, inputs = await anext(iterator)
        assert all(event.is_set() for event in started.values())
        assert row[0].market == "fast" and inputs[3] == "fast"
        assert not tasks["slow"].done()
        release.set()
        row, inputs = await anext(iterator)
        assert row[0].market == "slow" and inputs[3] == "slow"
        with pytest.raises(StopAsyncIteration):
            await anext(iterator)

    asyncio.run(exercise())
    assert source_calls == ["slow", "fast"]


def test_parallel_input_watchdog_tracks_oldest_and_clears_cancelled_tasks(monkeypatch):
    monkeypatch.setattr(discord_bot.bot, "_opening_input_stages", {
        "slow": {"market": "slow", "started_monotonic": 10.0, "hot": True},
        "fast": {"market": "fast", "started_monotonic": 11.0, "hot": True},
    })
    assert discord_bot._oldest_opening_input_stage()["market"] == "slow"

    async def exercise():
        blocked = asyncio.Event()
        slow, fast = (SimpleNamespace(market=name) for name in ("slow", "fast"))
        tasks = {"slow": asyncio.create_task(blocked.wait()),
                 "fast": asyncio.create_task(asyncio.sleep(0, result="ready"))}
        iterator = discord_bot._scheduled_inputs_ready_order(
            [(slow, None, None, False), (fast, None, None, False)], tasks)
        row, result = await anext(iterator)
        assert row[0].market == "fast" and result == "ready"
        assert set(discord_bot.bot._opening_input_stages) == {"slow"}
        await iterator.aclose()
        assert tasks["slow"].cancelled()
        assert discord_bot._oldest_opening_input_stage() is None

    asyncio.run(exercise())


def test_input_watchdog_uses_real_progress_not_builder_start_or_poll(monkeypatch):
    building = {"market": "building", "started_monotonic": 1., "hot": True,
                "last_progress_monotonic": 190.}
    hung = {"market": "hung", "started_monotonic": 150., "hot": True}
    monkeypatch.setattr(discord_bot.bot, "_opening_input_stages", {"building": building, "hung": hung})
    monkeypatch.setattr(discord_bot, "_opening_source_operation", lambda *_args, **_kw: None)
    assert discord_bot._oldest_opening_input_stage(now_monotonic=220.) is hung
    monkeypatch.setattr(discord_bot.time, "monotonic", lambda: 221.)
    discord_bot._record_opening_input_progress(building, {"message": "load", "step": 250, "total": 2760})
    assert building["last_progress_monotonic"] == 221.
    monkeypatch.setattr(discord_bot.time, "monotonic", lambda: 500.)
    discord_bot._record_opening_input_progress(building, {"message": "load", "step": 250, "total": 2760})
    assert building["last_progress_monotonic"] == 221.  # No timer-only liveness.
    discord_bot._record_opening_input_progress(building, {"message": "load", "step": 500, "total": 2760})
    assert building["last_progress_monotonic"] == 500.


def test_persistent_input_poll_retries_fast_mode_without_waiting_or_restarting_slow_job(monkeypatch):
    monkeypatch.setattr(discord_bot.bot, "_scheduled_input_jobs", {})
    monkeypatch.setattr(discord_bot.bot, "_opening_input_stages", {})
    monkeypatch.setattr(discord_bot.bot, "_opening_first_observed_at", {})
    monkeypatch.setattr(discord_bot, "_preopen_market_final_armed_for_session", lambda *_: True)
    now = datetime.fromisoformat("2026-10-14T09:00:00.004+08:00")
    cfgs = [SimpleNamespace(market=m, day_trade_simulation_enabled=True,
                            overnight_simulation_enabled=False) for m in ("slow", "fast")]
    calls = {"slow": 0, "fast": 0}

    async def exercise():
        release = asyncio.Event()

        async def prepare(row, stage):
            market = row[0].market
            calls[market] += 1
            if market == "slow":
                await release.wait()
            return market, None, 0., None, {}

        monkeypatch.setattr(discord_bot, "_prepare_scheduled_attempt_inputs", prepare)
        due = [(cfg, now, cfg.market + ":session", False) for cfg in cfgs]
        slow_task = None
        for attempt in range(3):
            poll_due, jobs = discord_bot._scheduled_input_poll_jobs(due)
            if slow_task is None:
                slow_task = jobs["slow"]
            assert jobs["slow"] is slow_task
            ready = []
            async for row, result in discord_bot._scheduled_inputs_ready_order(
                    poll_due, jobs, poll_timeout=.001, cancel_pending=False):
                ready.append(row[0].market)
                assert result[0] == "fast"
                discord_bot.bot._scheduled_input_jobs.pop(row[0].market)
            assert ready == ["fast"]
            assert not slow_task.done()
            assert "slow" in discord_bot.bot._opening_input_stages
        assert calls == {"slow": 1, "fast": 3}
        assert discord_bot.bot._opening_first_observed_at["fast:session"] == now
        release.set()
        await slow_task
        rows, jobs = discord_bot._scheduled_input_poll_jobs([due[0]])
        ready = [row[0].market async for row, _ in discord_bot._scheduled_inputs_ready_order(
            rows, jobs, poll_timeout=0, cancel_pending=False)]
        assert ready == ["slow"]
        assert not discord_bot.bot._opening_input_stages

    asyncio.run(exercise())


def test_stale_session_or_config_job_is_drained_before_replacement_not_cancelled(monkeypatch):
    monkeypatch.setattr(discord_bot.bot, "_scheduled_input_jobs", {})
    monkeypatch.setattr(discord_bot.bot, "_opening_input_stages", {})
    monkeypatch.setattr(discord_bot.bot, "_opening_first_observed_at", {})
    monkeypatch.setattr(discord_bot, "_preopen_market_final_armed_for_session", lambda *_: False)
    old_cfg = SimpleNamespace(market="mode", day_trade_simulation_enabled=True,
                              overnight_simulation_enabled=False, checkpoint="old")
    new_cfg = SimpleNamespace(**{**vars(old_cfg), "checkpoint": "new"})
    now = datetime.fromisoformat("2026-10-14T09:00:00+08:00")

    async def exercise():
        release = asyncio.Event()
        calls = []

        async def prepare(row, stage):
            calls.append(row[0].checkpoint)
            await release.wait()
            return row[0].checkpoint

        monkeypatch.setattr(discord_bot, "_prepare_scheduled_attempt_inputs", prepare)
        _, jobs = discord_bot._scheduled_input_poll_jobs([(old_cfg, now, "old-session", True)])
        original = jobs["mode"]
        await asyncio.sleep(0)
        assert discord_bot._scheduled_input_poll_jobs([(new_cfg, now, "new-session", True)]) == ([], {})
        assert calls == ["old"] and not original.cancelled()
        release.set()
        await original
        rows, jobs = discord_bot._scheduled_input_poll_jobs([(new_cfg, now, "new-session", True)])
        assert rows[0][0] == new_cfg and jobs["mode"] is not original
        assert await jobs["mode"] == "new"
        assert calls == ["old", "new"]
        discord_bot._scheduled_input_poll_jobs([])
        assert not discord_bot.bot._scheduled_input_jobs

    asyncio.run(exercise())


def test_catchup_error_keeps_original_error_timing_and_does_not_prepare_model(monkeypatch):
    def fail(_cfg):
        raise OSError("source-owner-failure")
    monkeypatch.setattr(discord_bot, "_prewarm_market_signal_sync", fail)
    monkeypatch.setattr(discord_bot, "_prepare_scheduled_inputs", lambda *_: pytest.fail("source failed"))
    stage = {"started_monotonic": 1.}
    result = asyncio.run(discord_bot._prepare_scheduled_attempt_inputs(
        (SimpleNamespace(market="failed"), None, "session", True), stage))
    assert isinstance(result[1], OSError)
    assert result[4]["signal_stage"] == "preopen_catch_up"
    assert result[4]["preopen_catch_up_ms"] >= 0
    assert stage["phase"] == "preopen_catch_up"


def test_scheduled_report_and_chat_queue_preserves_order_without_blocking_retries(monkeypatch):
    monkeypatch.setattr(discord_bot.bot, "_scheduled_delivery_task", None)
    events = []

    async def exercise():
        release = asyncio.Event()

        async def send(deliveries, errors):
            name = deliveries[0]
            events.append(("started", name))
            if name == "first":
                await release.wait()
            events.append(("finished", name))

        monkeypatch.setattr(discord_bot, "_deliver_scheduled_results", send)
        discord_bot._enqueue_scheduled_results(["first"], [])
        await asyncio.sleep(0)
        original = discord_bot.bot._scheduled_delivery_task
        discord_bot._enqueue_scheduled_results(["second"], [])
        latest = discord_bot.bot._scheduled_delivery_task
        await asyncio.sleep(0)
        assert events == [("started", "first")]
        assert not original.done() and not latest.done()
        # More source/scheduler work can run while the observer is stuck.
        events.append(("retry", "model"))
        release.set()
        await latest
        await asyncio.sleep(0)
        assert events == [("started", "first"), ("retry", "model"), ("finished", "first"),
                          ("started", "second"), ("finished", "second")]
        assert discord_bot.bot._scheduled_delivery_task is None

    asyncio.run(exercise())


def test_scheduled_source_failure_is_a_value_and_does_not_run_model(monkeypatch):
    cfg = SimpleNamespace(market="failed", day_trade_simulation_enabled=True)

    async def prepare(_cfg):
        return ("tw", "verified-status", False), None, 2.0

    def fail(**kwargs):
        assert kwargs["prepared_status"] == "verified-status"
        assert discord_bot._MODEL_INFERENCE_LOCK.acquire(blocking=False)
        discord_bot._MODEL_INFERENCE_LOCK.release()
        raise ConnectionError("credential=never-persist-this")

    monkeypatch.setattr(discord_bot, "_prepare_scheduled_signal", prepare)
    monkeypatch.setattr(discord_bot, "_signal_kwargs", lambda **kwargs: kwargs)
    monkeypatch.setattr(discord_bot, "prefetch_live_signal_prices", fail)
    monkeypatch.setattr(discord_bot, "generate_live_signal", lambda **kw: pytest.fail("preparation must not infer"))
    prepared, error, prepare_ms, quote, timing = asyncio.run(discord_bot._prepare_scheduled_inputs(cfg))
    assert prepared[0] == "tw" and prepare_ms == 2.0
    assert isinstance(error, ConnectionError) and quote is None
    assert timing["signal_stage"] == "quote_prefetch_outside_model_lock"
    assert timing["parallel_quote_prefetch_ms"] >= 0
    assert "never-persist-this" not in str(timing)
    assert set(timing["input_preparation_stages_ms"]) == {"realtime_freshness"}


@pytest.mark.parametrize("fails", [False, True])
def test_input_stage_timings_persist_real_work_even_on_failure(monkeypatch, fails):
    cfg = SimpleNamespace(market="measured", day_trade_simulation_enabled=True,
                          timezone="Asia/Taipei", open_time="09:00")

    async def prepare(_cfg):
        return ("tw", "verified", False), None, 1.0

    def prefetch(**kwargs):
        report = kwargs["progress_callback"]
        report({"message": "panel_fingerprint", "step": 0, "total": 1})
        report({"message": "load", "step": 250, "total": 2760})
        report({"message": "load", "step": 500, "total": 2760})
        report({"message": "quote_transport", "step": 0, "total": 1})
        if fails:
            raise ConnectionError("do not persist secret")
        return SimpleNamespace(snapshot=SimpleNamespace(transport_timing={}))

    monkeypatch.setattr(discord_bot, "_prepare_scheduled_signal", prepare)
    monkeypatch.setattr(discord_bot, "_signal_kwargs", lambda **kwargs: kwargs)
    monkeypatch.setattr(discord_bot, "prefetch_live_signal_prices", prefetch)
    prepared, error, _ms, quote, timing = asyncio.run(discord_bot._prepare_scheduled_inputs(cfg))
    spans = timing["input_preparation_stages_ms"]
    assert set(spans) == {"realtime_freshness", "panel_fingerprint", "load", "quote_transport"}
    assert all(ms >= 0 for ms in spans.values())
    assert bool(error) == fails and (quote is None) == fails
    assert "secret" not in str(timing)
    record = discord_bot._opening_signal_latency_record(cfg=cfg, session_date="2026-10-15",
        schedule_key="test:2026-10-15", timing=timing, result=None, error=error)
    assert record["input_preparation_stages_ms"] == spans


def test_scheduled_prefetched_quote_is_revalidated_by_canonical_model_owner(monkeypatch):
    quote = object()
    monkeypatch.setenv("STOCKAGENT_BOT_PROGRESS", "0")
    monkeypatch.setattr(discord_bot, "_signal_kwargs", lambda **kwargs: kwargs)
    monkeypatch.setattr(discord_bot, "prefetch_live_signal_prices", lambda **kw: pytest.fail("no duplicate source fetch"))

    def infer(**kwargs):
        assert kwargs["_prefetched_quote"] is quote
        return "same-canonical-validation"

    monkeypatch.setattr(discord_bot, "generate_live_signal", infer)
    assert discord_bot._run_market_signal_sync(
        market="test", _prefetch_prices=True, _prefetched_quote=quote,
    ) == "same-canonical-validation"


def test_session_open_prefetch_uses_the_same_validated_request(monkeypatch, tmp_path):
    config = SimpleNamespace(data=SimpleNamespace(), trading=SimpleNamespace(execution_mode="tw_day_trade"), runner=SimpleNamespace(output_dir=str(tmp_path)), training=SimpleNamespace(lookback=1))
    panel = SimpleNamespace(features=np.zeros((1, 1, 1)))
    request = dict(source="tw", symbols=["2330"], fallback_prices=np.array([99.0]), require_official_tw_session_open=True)
    calls = []
    monkeypatch.setattr(signal_engine, "load_config", lambda *args: config)
    monkeypatch.setattr(signal_engine, "_resolve_checkpoint", lambda *args: (11, tmp_path / "checkpoint.pt"))
    monkeypatch.setattr(signal_engine, "_cached_checkpoint", lambda *args: ({"model_state_dict": {}}, "original-checkpoint", True))
    monkeypatch.setattr(signal_engine, "_build_panel", lambda *args, **kwargs: (panel, True, "memory"))
    monkeypatch.setattr(signal_engine, "_cached_aligned_panel", lambda *args, **kwargs: (panel, True))
    monkeypatch.setattr(signal_engine, "_resolve_usable_panel_index", lambda *args: (0, None))

    def quote_request(**kwargs):
        assert kwargs["observation"] == "session_open"
        return request

    def source(**kwargs):
        calls.append(kwargs)
        return quote_provider.PriceSnapshot(prices=np.array([100.0]), source="fixture:opening")

    monkeypatch.setattr(signal_engine, "_live_quote_request", quote_request)
    monkeypatch.setattr(signal_engine, "_price_snapshot", source)
    result = signal_engine.prefetch_live_signal_prices(config_path=tmp_path / "model.yaml", day_trade_model_observation="session_open", price_source="tw")
    assert result.request_key == signal_engine._live_quote_request_key(request)
    assert result.snapshot.source == "fixture:opening"
    assert len(calls) == 1 and calls[0]["require_official_tw_session_open"] is True


def test_opening_prefetch_rejects_pending_quorum_with_exact_mode_mask(monkeypatch):
    now = datetime.fromisoformat("2026-10-13T09:00:02+08:00")
    sample = quote_provider.PriceSnapshot(
        prices=np.array([100., 99., 101.]), open_prices=np.array([100., np.nan, 101.]),
        source="fixture:opening", available_mask=np.ones(3, dtype=bool),
        available_count=3, requested_count=3,
        transport_timing={"opening_quorum_ready": False},
    )
    monkeypatch.setenv("STOCKAGENT_TW_OPENING_MIN_COVERAGE", "0.90")
    request = {"require_official_tw_session_open": True, "request_mask": np.array([True, True, False])}
    with pytest.raises(signal_engine.TWOpeningInputCoveragePending) as caught:
        signal_engine._reject_unready_opening_prefetch(
            request, sample, workflow_started_utc=now, requested_at_utc=now,
            received_at_utc=now, pre_quote_prepare_ms=2., quote_fetch_ms=3.,
        )
    assert caught.value.coverage == {"active": 2, "opens": 1, "responses": 2, "required": 2, "no_prints": 0, "basis": "session_open"}
    summary = caught.value.opening_prefetch_summary
    assert summary["price_source"] == "fixture:opening"
    assert summary["live_latency"]["quote_fetch_ms"] == 3.
    # Never use the early guard as a successful validation or weaken the
    # model owner's original checks for ready or uninstrumented sources.
    for marker in [True, None]:
        sample.transport_timing["opening_quorum_ready"] = marker
        signal_engine._reject_unready_opening_prefetch(
            request, sample, workflow_started_utc=now, requested_at_utc=now,
            received_at_utc=now, pre_quote_prepare_ms=2., quote_fetch_ms=3.,
        )


def test_pending_prefetch_records_source_wait_before_any_model_execution(monkeypatch):
    cfg = SimpleNamespace(market="test", day_trade_simulation_enabled=True)
    monkeypatch.setattr(discord_bot, "_signal_kwargs", lambda **kwargs: kwargs)

    async def prepare(_cfg):
        return ("tw", SimpleNamespace(), None), None, 1.0

    def pending(**kwargs):
        assert discord_bot._MODEL_INFERENCE_LOCK.acquire(blocking=False)
        discord_bot._MODEL_INFERENCE_LOCK.release()
        error = signal_engine.TWOpeningInputCoveragePending("pending", {"active": 2, "opens": 1, "required": 2})
        error.opening_prefetch_summary = {"price_source": "fixture", "live_latency": {"quote_fetch_ms": 5.0}}
        raise error

    monkeypatch.setattr(discord_bot, "_prepare_scheduled_signal", prepare)
    monkeypatch.setattr(discord_bot, "prefetch_live_signal_prices", pending)
    monkeypatch.setattr(discord_bot, "generate_live_signal", lambda **kwargs: pytest.fail("pending inputs must not run the model"))
    _prepared, error, _ms, prefetched, timing = asyncio.run(discord_bot._prepare_scheduled_inputs(cfg))
    assert isinstance(error, signal_engine.TWOpeningInputCoveragePending)
    assert prefetched is None
    assert timing["signal_stage"] == "opening_input_coverage"
    assert timing["signal_summary"]["price_source"] == "fixture"


def test_failed_opening_prefetch_preserves_its_stage_and_does_not_hold_model_lock(monkeypatch):
    monkeypatch.setenv("STOCKAGENT_BOT_PROGRESS", "0")
    monkeypatch.setattr(discord_bot, "_signal_kwargs", lambda **kwargs: kwargs)

    def failed(**kwargs):
        acquired = discord_bot._MODEL_INFERENCE_LOCK.acquire(blocking=False)
        assert acquired
        discord_bot._MODEL_INFERENCE_LOCK.release()
        raise TimeoutError("full-market quote source did not reach quorum")

    monkeypatch.setattr(discord_bot, "prefetch_live_signal_prices", failed)
    monkeypatch.setattr(discord_bot, "generate_live_signal", lambda **kwargs: pytest.fail("source failure cannot run model"))
    timing = {}
    error = TimeoutError("source did not reach quorum")
    with pytest.raises(TimeoutError):
        discord_bot._run_market_signal_sync(market="test", _prefetch_prices=True, _timing_sink=timing)
    cfg = SimpleNamespace(market="test", timezone="Asia/Taipei", open_time="09:00")
    record = discord_bot._opening_signal_latency_record(cfg=cfg, session_date="2026-10-08", schedule_key="test:2026-10-08", timing=timing, result=None, error=error)
    assert record["failure_stage"] == "quote_prefetch_outside_model_lock"
    assert record["stages"]["quote_prefetch_outside_model_lock_ms"] >= 0
    assert record["status"] == "failed"


def test_alignment_cache_requires_same_panel_and_checkpoint_and_skips_legacy(monkeypatch):
    monkeypatch.setattr(signal_engine, "_LIVE_ALIGNED_PANEL_CACHE", signal_engine.OrderedDict())
    calls = []
    monkeypatch.setattr(signal_engine, "checkpoint_manifest_symbols", lambda payload: payload.get("symbols"))

    def align(panel, *_args, **_kwargs):
        calls.append(panel)
        return SimpleNamespace(source=panel)

    monkeypatch.setattr(signal_engine, "align_panel_to_checkpoint_universe", align)
    panel = SimpleNamespace(features=np.zeros((2, 2, 1)))
    kwargs = dict(checkpoint_key="v1", checkpoint_payload={"symbols": ["B", "A"]},
                  state_dict={}, fold_dir=signal_engine.Path("unused"), context="test")
    first, hit = signal_engine._cached_aligned_panel(panel, **kwargs)
    assert not hit
    repeated, hit = signal_engine._cached_aligned_panel(panel, **kwargs)
    assert hit and repeated is first
    signal_engine._cached_aligned_panel(panel, **{**kwargs, "checkpoint_key": "v2"})
    signal_engine._cached_aligned_panel(SimpleNamespace(features=panel.features.copy()), **kwargs)
    for _ in range(2):
        signal_engine._cached_aligned_panel(panel, **{**kwargs, "checkpoint_payload": {}})
    assert len(calls) == 5


@pytest.mark.parametrize("change", [
    {"symbols": ["B", "A"]}, {"request_mask": np.array([True, False])},
    {"fallback_prices": np.array([101., 200.])}, {"source": "shioaji"},
    {"require_official_tw_session_open": True},
])
def test_prefetch_identity_invalidates_changed_price_contract(change):
    request = dict(symbols=["A", "B"], request_mask=np.array([True, True]),
                   fallback_prices=np.array([100., 200.]), source="tw",
                   require_official_tw_session_open=False)
    assert signal_engine._live_quote_request_key(request) != signal_engine._live_quote_request_key({**request, **change})


def test_mis_coverage_quorum_reuses_connections_but_fetches_new_prices(monkeypatch, tmp_path):
    missing = {"1009"}
    calls = []
    sessions = []
    price = 101.0
    symbols = [str(1000 + index) for index in range(10)]

    class Response:
        def __init__(self, rows):
            self.rows = rows

        def raise_for_status(self):
            pass

        def json(self):
            return {"msgArray": self.rows}

    class Session:
        def __init__(self):
            self.headers = {}
            sessions.append(self)

        def get(self, _url, *, params, **_kwargs):
            code = params["ex_ch"].split("_", 1)[1].split(".", 1)[0]
            calls.append(code)
            rows = [] if code in missing else [{"c": code, "z": str(price), "o": "100.0"}]
            return Response(rows)

    monkeypatch.setattr(quote_provider.requests, "Session", Session)
    monkeypatch.setattr(quote_provider, "warm_tw_mis_quote_client", lambda: {})
    monkeypatch.setattr(quote_provider, "load_symbol_yahoo_map", lambda _: {s: f"{s}.TW" for s in symbols})
    monkeypatch.setattr(quote_provider, "_TW_MIS_BOOTSTRAP_COOKIES", {})
    monkeypatch.setattr(quote_provider, "_TW_MIS_HTTP_LOCAL", threading.local())
    kwargs = dict(parquet_root=tmp_path, chunk_size=1, max_parallel_requests=1,
                  empty_chunk_retry_attempts=1, empty_chunk_retry_delay_seconds=0,
                  minimum_response_coverage=0.9)
    with ThreadPoolExecutor(max_workers=1) as pool:
        monkeypatch.setattr(quote_provider, "_TW_MIS_HTTP_POOL", pool)
        first = quote_provider.fetch_tw_mis_last_prices(symbols, np.ones(10), **kwargs)
        assert len(calls) == 10 and first.available_count == 9
        assert first.transport_timing["mis_retry_rounds"] == 0
        price = 102.0
        second = quote_provider.fetch_tw_mis_last_prices(symbols, np.ones(10), **kwargs)
        assert second.prices[0] == 102.0 and first.prices[0] == 101.0
        assert len(sessions) == 1
        missing.add("1008")
        third = quote_provider.fetch_tw_mis_last_prices(symbols, np.ones(10), **kwargs)
        assert third.available_count == 8
        assert third.transport_timing["mis_retry_rounds"] == 1
        assert len(calls) == 32


def test_opening_priority_releases_when_scheduled_batch_is_published(monkeypatch):
    cfg = SimpleNamespace(day_trade_simulation_enabled=True)
    state = "retry"
    monkeypatch.setattr(discord_bot, "_scheduled_markets", lambda: ["mode"])
    monkeypatch.setattr(discord_bot, "_resolve_market", lambda _: cfg)
    monkeypatch.setattr(discord_bot, "_scheduled_market_session_day", lambda *_: (True, "session"))
    monkeypatch.setattr(discord_bot, "_day_trade_schedule_state", lambda *_: state)
    monkeypatch.setattr(discord_bot.bot, "_opening_attempt_started_monotonic", None)
    at_open = datetime(2026, 9, 9, 9, 0, 0, tzinfo=ZoneInfo("Asia/Taipei"))
    assert discord_bot._intraday_waits_for_opening(at_open)
    state = "pending_confirmation"
    assert not discord_bot._intraday_waits_for_opening(at_open)
    state = "retry"
    assert not discord_bot._intraday_waits_for_opening(at_open.replace(second=15))


def test_different_model_universes_share_pending_quotes_and_keep_own_missing_fallback(monkeypatch, tmp_path):
    started = threading.Event()
    release = threading.Event()
    calls = []

    def fetch(symbols, fallback_prices, **_kwargs):
        calls.append(list(symbols))
        if symbols == ["A", "B"]:
            started.set()
            assert release.wait(timeout=2)
        available = np.array([symbol != "B" for symbol in symbols])
        return quote_provider.PriceSnapshot(
            prices=np.where(available, 105.0, fallback_prices), source="twse_tpex:mis",
            available_count=int(available.sum()), requested_count=len(symbols), available_mask=available,
            timestamps_ms=np.ones(len(symbols), dtype=np.int64),
        )

    monkeypatch.setattr(quote_provider, "_fetch_tw_mis_last_prices", fetch)
    monkeypatch.setattr(quote_provider, "_TW_MIS_INFLIGHT", {})
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(quote_provider.fetch_tw_mis_last_prices, ["A", "B"], np.array([1., 2.]), parquet_root=tmp_path)
        assert started.wait(timeout=2)
        second = pool.submit(quote_provider.fetch_tw_mis_last_prices, ["C", "B", "A"], np.array([3., 4., 5.]), parquet_root=tmp_path)
        deadline = time.monotonic() + 2
        while ["C"] not in calls and time.monotonic() < deadline:
            time.sleep(0.001)
        release.set()
        a, b = first.result(timeout=2), second.result(timeout=2)
    assert calls == [["A", "B"], ["C"]]
    np.testing.assert_equal(a.prices, [105., 2.])
    np.testing.assert_equal(b.prices, [105., 4., 105.])
    assert b.available_mask.tolist() == [True, False, True]
    assert b.transport_timing["mis_inflight_shared_symbols"] == 2
    assert not quote_provider._TW_MIS_INFLIGHT
