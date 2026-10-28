"""Opening input recovery must not accidentally dispatch full-history work."""
import fcntl
import json
import os
import signal
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from scripts import run_discord_artifact_maintenance as runner
from stockagent.live import market_status


TAIPEI = ZoneInfo("Asia/Taipei")


@pytest.mark.parametrize("clock,session,expected", [
    ("00:00", True, True), ("05:59", True, True),
    ("08:55", True, True), ("09:00", True, True),
    ("13:19", True, True), ("13:20", True, False),
    ("08:30", False, False),
])
def test_source_admission_requires_real_session_and_precedes_exit_window(monkeypatch, clock, session, expected):
    monkeypatch.setattr(market_status, "tw_stock_day_decision",
                        lambda *a, **k: SimpleNamespace(is_session=session))
    observed = datetime.fromisoformat(f"2026-10-28T{clock}:00").replace(tzinfo=TAIPEI)
    assert runner._preopen_source_priority_window(observed) is expected


def test_priority_dispatch_is_only_for_explicit_source_requests(monkeypatch):
    monkeypatch.setenv("STOCKAGENT_PREOPEN_PRIORITY_SOURCE", "1")
    monkeypatch.setattr(runner, "_preopen_source_priority_window", lambda _now: True)
    now = datetime.now(TAIPEI)
    assert runner._priority_source_requested({"due_keys": ["source_projection_refresh:factorized"]}, now)
    assert runner._priority_source_requested({"due_keys": ["source_cache_warming:factorized"]}, now)
    assert not runner._priority_source_requested({"due_keys": ["2026-10-27:factorized:artifact_backfill"]}, now)
    monkeypatch.delenv("STOCKAGENT_PREOPEN_PRIORITY_SOURCE")
    assert not runner._priority_source_requested({"due_keys": ["source_projection_refresh:factorized"]}, now)


def test_protected_runway_dispatches_only_source_owner_and_keeps_normal_history_deferred(tmp_path, monkeypatch):
    from scripts import check_outside_tw_opening_resource_window as window

    monkeypatch.setenv("STOCKAGENT_PREOPEN_PRIORITY_SOURCE", "1")
    monkeypatch.setattr(runner, "_preopen_source_priority_window", lambda _now: True)
    monkeypatch.setattr(window, "evaluate", lambda *a, **k: {"allowed": False})
    monkeypatch.setattr(runner, "_retry_dispatch_state_path", lambda: tmp_path / "state.json")
    commands = []
    monkeypatch.setattr(runner.subprocess, "run", lambda cmd, **kwargs: commands.append(cmd))
    probe = {"due": True, "due_keys": ["source_projection_refresh:factorized", "history"],
             "wakeup_fingerprint": "one"}
    monkeypatch.setattr(runner, "retry_probe", lambda: probe)
    assert runner.request_due_retry() == 0
    assert commands == [["systemctl", "start", "--no-block", runner.PRIORITY_SOURCE_UNIT]]
    assert json.loads((tmp_path / "state.json").read_text())["owner_unit"] == runner.PRIORITY_SOURCE_UNIT
    probe.update(due_keys=["history"], wakeup_fingerprint="two")
    assert runner.request_due_retry() == 0
    assert len(commands) == 1


def test_priority_source_retry_does_not_wait_for_history_coalescing_interval(tmp_path, monkeypatch):
    from scripts import check_outside_tw_opening_resource_window as window

    monkeypatch.setenv("STOCKAGENT_PREOPEN_PRIORITY_SOURCE", "1")
    monkeypatch.setattr(runner, "_preopen_source_priority_window", lambda _now: True)
    monkeypatch.setattr(window, "evaluate", lambda *a, **k: {"allowed": False})
    state = tmp_path / "state.json"
    monkeypatch.setattr(runner, "_retry_dispatch_state_path", lambda: state)
    state.write_text(json.dumps({"owner_unit": runner.PRIORITY_SOURCE_UNIT,
        "wakeup_fingerprint": "unchanged",
        "dispatch_accepted_at": (datetime.now(timezone.utc) - timedelta(seconds=61)).isoformat()}))
    monkeypatch.setattr(runner, "retry_probe", lambda: {"due": True,
        "due_keys": ["source_projection_refresh:factorized"], "wakeup_fingerprint": "unchanged"})
    commands = []
    monkeypatch.setattr(runner.subprocess, "run", lambda cmd, **_kwargs: commands.append(cmd))
    assert runner.request_due_retry() == 0
    assert commands == [["systemctl", "start", "--no-block", runner.PRIORITY_SOURCE_UNIT]]


def test_source_child_reuses_worker_lock_and_never_runs_history(tmp_path, monkeypatch):
    status = tmp_path / "status.json"
    cfg = SimpleNamespace(market="factorized")
    bot = SimpleNamespace(
        _artifact_backfill_status_path=lambda: status,
        _interactive_signal_work_pending=lambda: False,
        _artifact_maintenance_markets=lambda: ["factorized"],
        _resolve_market=lambda _: cfg,
    )
    monkeypatch.setattr(runner, "_preopen_source_priority_window", lambda _now: True)
    monkeypatch.setattr(runner, "_load_discord_bot", lambda: bot)
    monkeypatch.setattr(runner, "_wait_for_tw_public_refresh", lambda **_: True)
    monkeypatch.setattr(runner, "_owns_factorized_source_updater", lambda _: True)
    calls = []
    monkeypatch.setattr(runner, "_recover_deferred_factorized_source",
                        lambda obj, **kwargs: calls.append((obj, kwargs)) or True)
    with (tmp_path / "status.json.worker.lock").open("a+") as held:
        fcntl.flock(held, fcntl.LOCK_EX)
        assert runner.run_preopen_source_child() == 75
        assert calls == []
    assert runner.run_preopen_source_child() == 0
    assert calls == [(cfg, {"allow_market_open": True})]


def test_source_wait_does_not_claim_full_maintenance_is_running(monkeypatch):
    checks = iter([True, False])
    status_writes = []
    bot = SimpleNamespace(_env_float=lambda _name, default: default,
                          _tw_public_refresh_in_progress=lambda: next(checks),
                          _record_artifact_maintenance_run=lambda *a, **k: status_writes.append(a))
    monkeypatch.setattr(runner, "_load_discord_bot", lambda: bot)
    monkeypatch.setattr(runner.time, "sleep", lambda _: None)
    assert runner._wait_for_tw_public_refresh(record_maintenance_run=False)
    assert status_writes == []


def test_supervisor_kills_entire_producer_group_on_deadline(monkeypatch):
    events = []
    class Proc:
        pid = 12345
        def __init__(self):
            self.first = True
        def wait(self, timeout=None):
            if self.first:
                self.first = False
                raise subprocess.TimeoutExpired("source", timeout)
            return 0
    proc = Proc()
    monkeypatch.setattr(runner, "_preopen_source_priority_window", lambda _now: True)
    monkeypatch.setattr(runner.subprocess, "Popen", lambda *a, **kw: proc)
    monkeypatch.setattr(runner.os, "killpg", lambda pid, sig: events.append((pid, sig)))
    assert runner.run_preopen_source_supervised() == 75
    assert events == [(12345, signal.SIGTERM), (12345, signal.SIGKILL)]


def test_real_source_process_group_is_stopped_including_descendants():
    source = ("import subprocess,sys,time; "
              "p=subprocess.Popen([sys.executable,'-c','import time; time.sleep(120)']); "
              "print(p.pid,flush=True); time.sleep(120)")
    proc = subprocess.Popen([sys.executable, "-c", source], stdout=subprocess.PIPE,
                            text=True, start_new_session=True)
    try:
        child_pid = int(proc.stdout.readline())
        runner._stop_preopen_source_group(proc)
        assert proc.poll() is not None
        try:
            child_state = Path(f"/proc/{child_pid}/stat").read_text().split()[2]
        except FileNotFoundError:
            child_state = None
        assert child_state in (None, "Z")
    finally:
        if proc.poll() is None:
            os.killpg(proc.pid, signal.SIGKILL)
            proc.wait()
        if proc.stdout is not None:
            proc.stdout.close()


def test_priority_unit_uses_same_source_owner_and_bounded_supervisor():
    root = Path(runner.__file__).resolve().parents[1]
    unit = (root / "deploy/systemd/stockagent-tw-day-trade-priority-source.service.in").read_text()
    assert "--preopen-source-only" in unit
    assert "KillMode=control-group" in unit
    assert "CPUWeight=10000" in unit
