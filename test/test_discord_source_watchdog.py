from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
import fcntl
import io
import subprocess
import sys
import threading
from types import SimpleNamespace

import pytest

from services.discord_bot.source_watchdog import SourceOperationRegistry
from services.discord_bot import bot as discord_bot


COMMAND = ("{python}", "shared_source.py")


@pytest.fixture
def clock(monkeypatch):
    value = {"now": 100.0}
    monkeypatch.setattr(discord_bot.time, "monotonic", lambda: value["now"])
    return value


def fake_process(*, pid=123, returncode=None):
    return SimpleNamespace(pid=pid, poll=lambda: returncode)


@pytest.mark.parametrize("timeout", [0, -1, float("inf"), float("nan")])
def test_registry_rejects_unbounded_source_wait(timeout):
    registry = SourceOperationRegistry()
    with pytest.raises(ValueError, match="positive finite"):
        with registry.watch_process(COMMAND, fake_process(), timeout_seconds=timeout):
            pytest.fail("must reject before registering")
    assert registry.snapshot(COMMAND) is None


def test_process_deadline_does_not_slide_and_does_not_prove_readiness(clock):
    registry = SourceOperationRegistry()
    with registry.watch_process(COMMAND, fake_process(), timeout_seconds=1200):
        first = registry.snapshot(COMMAND)
        assert first["deadline_monotonic"] == 1300.0
        assert first["source_ready"] is False
        clock["now"] = 1000.0
        second = registry.snapshot(COMMAND)
        assert second["operation_id"] == first["operation_id"]
        assert second["deadline_monotonic"] == 1300.0
        assert second["remaining_seconds"] == 300.0
        assert registry.snapshot(("unrelated_source.py",)) is None
        for at in (1300.0, 1300.1, 99.0, float("nan")):
            assert registry.snapshot(COMMAND, now=at) is None
    assert registry.snapshot(COMMAND) is None


def test_completed_process_and_expired_context_cannot_excuse_stall(clock):
    registry = SourceOperationRegistry()
    process = fake_process()
    with pytest.raises(OSError):
        with registry.watch_process(COMMAND, process, timeout_seconds=1200):
            assert registry.snapshot(COMMAND) is not None
            process.poll = lambda: 0
            assert registry.snapshot(COMMAND) is None
            raise OSError("original failure")
    assert registry.snapshot(COMMAND) is None


def test_duplicate_operation_does_not_replace_original_owner(clock):
    registry = SourceOperationRegistry()
    with registry.watch_process(COMMAND, fake_process(), timeout_seconds=1200):
        original = registry.snapshot(COMMAND)
        with pytest.raises(RuntimeError, match="single-flight"):
            with registry.watch_process(COMMAND, fake_process(pid=456), timeout_seconds=1200):
                pass
        assert registry.snapshot(COMMAND) == original


def test_writer_wait_requires_recent_actual_blocked_probe_and_open_handle(clock):
    registry = SourceOperationRegistry()
    handle = io.StringIO()
    with registry.watch_writer_lock(COMMAND, handle, timeout_seconds=1200) as operation:
        assert registry.snapshot(COMMAND)["kind"] == "verified_writer_lock_wait"
        clock["now"] = 103.0
        assert registry.snapshot(COMMAND) is None
        registry.observe_blocked(operation)
        proof = registry.snapshot(COMMAND)
        assert proof["pid"] is None
        assert proof["deadline_monotonic"] == 1300.0
        handle.close()
        assert registry.snapshot(COMMAND) is None
    assert registry.snapshot(COMMAND) is None


def test_actual_owned_child_is_live_then_reaped_without_a_pid_only_guess():
    registry = SourceOperationRegistry()
    command = [sys.executable, "-c", "import sys; sys.stdin.buffer.read(1)"]
    with subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.DEVNULL,
                          stderr=subprocess.DEVNULL) as process:
        try:
            with registry.watch_process(command, process, timeout_seconds=5):
                proof = registry.snapshot(command)
                assert proof["pid"] == process.pid
                assert proof["source_ready"] is False
                process.stdin.write(b"x")
                process.stdin.flush()
                assert process.wait(timeout=5) == 0
                assert registry.snapshot(command) is None
        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=5)
    assert registry.snapshot(command) is None


def test_existing_writer_wait_is_bound_to_the_real_lock_and_cleans_up(
    tmp_path, monkeypatch,
):
    registry = SourceOperationRegistry()
    lock_path = tmp_path / "writer.lock"
    blocked, release = threading.Event(), threading.Event()
    monkeypatch.setattr(discord_bot, "_PRE_SIGNAL_SOURCE_OPERATIONS", registry)
    monkeypatch.setattr(discord_bot, "_tw_data_layer_lock_path", lambda command: lock_path)

    def wait_one_probe(_seconds):
        blocked.set()
        assert release.wait(timeout=5)

    monkeypatch.setattr(discord_bot.time, "sleep", wait_one_probe)
    with lock_path.open("a+") as writer:
        fcntl.flock(writer, fcntl.LOCK_EX)
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(discord_bot._wait_for_existing_tw_data_update,
                                 list(COMMAND), timeout_seconds=10)
            try:
                assert blocked.wait(timeout=5)
                proof = registry.snapshot(COMMAND)
                assert proof["kind"] == "verified_writer_lock_wait"
                assert proof["source_ready"] is False
                fcntl.flock(writer, fcntl.LOCK_UN)
                release.set()
                assert future.result(timeout=5) is True
            finally:
                fcntl.flock(writer, fcntl.LOCK_UN)
                release.set()
    assert registry.snapshot(COMMAND) is None
    # An unlocked file is not an ongoing source operation or a readiness proof.
    assert discord_bot._wait_for_existing_tw_data_update(list(COMMAND), timeout_seconds=10) is False
    assert registry.snapshot(COMMAND) is None


def test_writer_wait_exception_does_not_leave_liveness_evidence(tmp_path, monkeypatch):
    registry = SourceOperationRegistry()
    lock_path = tmp_path / "writer.lock"
    monkeypatch.setattr(discord_bot, "_PRE_SIGNAL_SOURCE_OPERATIONS", registry)
    monkeypatch.setattr(discord_bot, "_tw_data_layer_lock_path", lambda command: lock_path)

    def interrupted(_seconds):
        assert registry.snapshot(COMMAND)["kind"] == "verified_writer_lock_wait"
        raise RuntimeError("original interruption")

    monkeypatch.setattr(discord_bot.time, "sleep", interrupted)
    with lock_path.open("a+") as writer:
        fcntl.flock(writer, fcntl.LOCK_EX)
        with pytest.raises(RuntimeError, match="original interruption"):
            discord_bot._wait_for_existing_tw_data_update(list(COMMAND), timeout_seconds=10)
        fcntl.flock(writer, fcntl.LOCK_UN)
    assert registry.snapshot(COMMAND) is None


@pytest.fixture
def opening(monkeypatch, clock):
    cfg = SimpleNamespace(market="test", pre_signal_command=COMMAND,
                          pre_signal_timeout_seconds=1200)
    registry = SourceOperationRegistry()
    monkeypatch.setattr(discord_bot, "_PRE_SIGNAL_SOURCE_OPERATIONS", registry)
    monkeypatch.setattr(discord_bot, "_resolve_market", lambda market: cfg)
    monkeypatch.setattr(discord_bot, "_write_discord_service_status", lambda: {})
    notifications = []
    monkeypatch.setattr(discord_bot, "notify_systemd", notifications.append)
    for key, value in {
        "_opening_attempt_started_monotonic": 100.0,
        "_opening_attempt_last_progress_monotonic": 100.0,
        "_opening_attempt_progress_message": "refresh public snapshot",
        "_opening_attempt_market": "test",
        "_opening_attempt_hot": False,
        "_opening_attempt_stage": "preopen_catch_up",
        "_opening_attempt_source_operation_id": None,
        "_opening_input_stages": {},
    }.items():
        monkeypatch.setattr(discord_bot.bot, key, value)
    monkeypatch.delenv("STOCKAGENT_OPENING_COLD_ATTEMPT_TIMEOUT_SECONDS", raising=False)
    monkeypatch.delenv("STOCKAGENT_OPENING_HOT_ATTEMPT_TIMEOUT_SECONDS", raising=False)

    def exit_process(code):
        raise SystemExit(code)

    monkeypatch.setattr(discord_bot.os, "_exit", exit_process)
    return cfg, registry, notifications


def test_heartbeat_waits_only_for_its_actual_bounded_source_owner(
    opening, clock, capsys,
):
    cfg, registry, notifications = opening
    command = discord_bot._resolve_pre_signal_command(cfg.pre_signal_command)
    with registry.watch_process(command, fake_process(), timeout_seconds=1200):
        clock["now"] = 300.0
        asyncio.run(discord_bot.service_heartbeat.coro())
        assert "action=wait_bounded_source" in capsys.readouterr().out
        assert notifications[0] == "WATCHDOG=1"
        assert "not source-ready" in notifications[1]
        assert discord_bot.bot._opening_attempt_last_progress_monotonic == 100.0
        # Repeated observations cannot fake progress or move its 1200s deadline.
        clock["now"] = 1200.0
        asyncio.run(discord_bot.service_heartbeat.coro())
        assert capsys.readouterr().out == ""
        clock["now"] = 1300.0
        with pytest.raises(SystemExit) as error:
            asyncio.run(discord_bot.service_heartbeat.coro())
        assert error.value.code == 70
        assert "action=exit_for_systemd_restart" in capsys.readouterr().out


def test_background_catchup_retains_fixed_source_deadline_but_cannot_hide_stalled_quote(
    opening, clock, monkeypatch, capsys,
):
    cfg, registry, _ = opening
    monkeypatch.setattr(discord_bot.bot, "_opening_attempt_started_monotonic", None)
    monkeypatch.setattr(discord_bot.bot, "_opening_input_stages", {
        "test": {"market": "test", "phase": "preopen_catch_up", "started_monotonic": 100., "hot": False}})
    command = discord_bot._resolve_pre_signal_command(cfg.pre_signal_command)
    with registry.watch_process(command, fake_process(), timeout_seconds=1200):
        clock["now"] = 350.
        asyncio.run(discord_bot.service_heartbeat.coro())
        assert "action=wait_bounded_source" in capsys.readouterr().out
        discord_bot.bot._opening_input_stages["quotes"] = {
            "market": "quotes", "phase": "realtime_prepare", "started_monotonic": 110., "hot": False}
        with pytest.raises(SystemExit):
            asyncio.run(discord_bot.service_heartbeat.coro())
        assert "market=quotes" in capsys.readouterr().out
        del discord_bot.bot._opening_input_stages["quotes"]
        clock["now"] = 1300.
        with pytest.raises(SystemExit):
            asyncio.run(discord_bot.service_heartbeat.coro())
        assert "action=exit_for_systemd_restart" in capsys.readouterr().out


@pytest.mark.parametrize("stage", [None, "started", "realtime_prepare", "inference"])
def test_source_wait_cannot_relax_quote_or_model_watchdog(opening, clock, monkeypatch, stage):
    cfg, registry, _ = opening
    command = discord_bot._resolve_pre_signal_command(cfg.pre_signal_command)
    monkeypatch.setattr(discord_bot.bot, "_opening_attempt_stage", stage)
    with registry.watch_process(command, fake_process(), timeout_seconds=1200):
        clock["now"] = 300.0
        assert discord_bot._opening_source_operation(300.0) is None
        with pytest.raises(SystemExit):
            asyncio.run(discord_bot.service_heartbeat.coro())


@pytest.mark.parametrize("kind", ["absent", "unrelated", "terminated"])
def test_unproved_source_wait_retains_original_watchdog(opening, clock, kind):
    cfg, registry, _ = opening
    command = discord_bot._resolve_pre_signal_command(cfg.pre_signal_command)
    watched = ("unrelated.py",) if kind != "terminated" else command
    process = fake_process(returncode=0 if kind == "terminated" else None)
    with registry.watch_process(watched, process, timeout_seconds=1200):
        clock["now"] = 300.0
        with pytest.raises(SystemExit):
            asyncio.run(discord_bot.service_heartbeat.coro())


def test_parallel_input_stage_never_uses_an_unrelated_source_budget(opening, clock, monkeypatch):
    cfg, registry, _ = opening
    command = discord_bot._resolve_pre_signal_command(cfg.pre_signal_command)
    monkeypatch.setattr(discord_bot.bot, "_opening_attempt_started_monotonic", None)
    monkeypatch.setattr(discord_bot.bot, "_opening_input_stages", {
        "inputs": {"market": "test", "started_monotonic": 100.0, "hot": True},
    })
    with registry.watch_process(command, fake_process(), timeout_seconds=1200):
        clock["now"] = 161.0
        with pytest.raises(SystemExit):
            asyncio.run(discord_bot.service_heartbeat.coro())


@pytest.mark.parametrize("returncode", [0, 2, "timeout"])
def test_existing_source_command_registers_its_actual_wait_and_always_cleans_up(
    tmp_path, opening, clock, monkeypatch, returncode,
):
    cfg, registry, _ = opening
    command = discord_bot._resolve_pre_signal_command(cfg.pre_signal_command)
    monkeypatch.setattr(discord_bot, "ROOT", tmp_path)
    monkeypatch.setattr(discord_bot, "_wait_for_existing_tw_data_update", lambda *a, **k: False)
    monkeypatch.setattr(discord_bot, "_recent_pre_signal_artifact_failure", lambda *a: None)
    monkeypatch.setattr(discord_bot, "_validate_pre_signal_download_artifacts", lambda *a: None)
    monkeypatch.setattr(discord_bot, "_remember_pre_signal_failure", lambda *a: None)
    monkeypatch.setattr(discord_bot, "_remember_pre_signal_success", lambda *a: None)
    monkeypatch.setattr(discord_bot, "clear_live_panel_memory_cache", lambda: None)
    process = fake_process()
    process.stdout = io.BytesIO(b"")
    killed = []
    process.kill = lambda: killed.append(True)

    def wait(*, timeout):
        assert timeout == 1200
        proof = registry.snapshot(command)
        assert proof["pid"] == process.pid
        assert proof["remaining_seconds"] == 1200.0
        if returncode == "timeout":
            raise subprocess.TimeoutExpired(command, timeout)
        return returncode

    process.wait = wait
    monkeypatch.setattr(discord_bot.subprocess, "Popen", lambda *a, **k: process)
    if returncode == 0:
        discord_bot._run_pre_signal_command_serialized(cfg, bypass_cache=True)
    else:
        with pytest.raises(discord_bot.BotUserError):
            discord_bot._run_pre_signal_command_serialized(cfg, bypass_cache=True)
    assert registry.snapshot(command) is None
    assert killed == ([True] if returncode == "timeout" else [])
