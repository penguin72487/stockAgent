from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import verify_discord_source_watchdog as verifier


@pytest.fixture
def clocks(monkeypatch):
    now = datetime(2026, 10, 14, 1, 30, tzinfo=timezone.utc)
    event = now - timedelta(seconds=60)
    boot = int(event.timestamp()) - 500
    monkeypatch.setattr(verifier, "Path", lambda path: SimpleNamespace(
        read_text=lambda: f"cpu 1 2 3 4\nbtime {boot}\n"))
    monkeypatch.setattr(verifier.os, "sysconf", lambda name: 100)
    row = {"__REALTIME_TIMESTAMP": str(int(event.timestamp() * 1_000_000))}
    child = {"pid": 123, "start_ticks": "20000", "boot_id": "current"}
    return now, row, child


def test_wait_evidence_is_current_bounded_and_not_readiness(clocks):
    now, row, child = clocks
    value = verifier.wait_clock_proof(row, child=child, idle=180.5,
                                     remaining=1019.5, now=now)
    assert value["event_age_seconds"] == 60.0
    assert value["remaining_now_seconds"] == 959.5


@pytest.mark.parametrize("kind", ["expired", "future_event", "pid_reused", "hot_only", "nan_idle",
                                  "zero_remaining", "unbounded_remaining", "nan_remaining"])
def test_native_wait_cannot_reuse_stale_clock_pid_or_unbounded_deadline(clocks, kind):
    now, row, child = clocks
    idle, remaining = 180.5, 1019.5
    if kind == "expired":
        remaining = 60.0
    elif kind == "future_event":
        row["__REALTIME_TIMESTAMP"] = str(int((now.timestamp() + 1) * 1_000_000))
    elif kind == "pid_reused":
        child["start_ticks"] = "50200"
    elif kind == "hot_only":
        idle = 60.5
    elif kind == "nan_idle":
        idle = float("nan")
    elif kind == "zero_remaining":
        remaining = 0
    elif kind == "unbounded_remaining":
        remaining = 1201
    elif kind == "nan_remaining":
        remaining = float("nan")
    with pytest.raises(ValueError, match="expired_or_pid_reused"):
        verifier.wait_clock_proof(row, child=child, idle=idle, remaining=remaining, now=now)


@pytest.mark.parametrize("invocation", ["old", ""])
def test_readback_rejects_changed_owner_before_reading_any_status(monkeypatch, invocation):
    monkeypatch.setattr(verifier, "identity", lambda unit: {
        "ActiveState": "active", "MainPID": "123", "InvocationID": invocation})
    with pytest.raises(ValueError, match="owner_changed_before"):
        verifier.verify("current")


def test_readback_rejects_active_without_real_process(monkeypatch):
    monkeypatch.setattr(verifier, "identity", lambda unit: {
        "ActiveState": "active", "MainPID": "0", "InvocationID": "current"})
    with pytest.raises(ValueError, match="owner_changed_before"):
        verifier.verify("current")


@pytest.fixture
def completed(tmp_path, monkeypatch):
    from scripts import verify_discord_gpu_idle_runtime as native

    monkeypatch.setattr(verifier, "ROOT", tmp_path)
    monkeypatch.setattr(verifier, "sha", lambda path: "unchanged")
    owner = {"ActiveState": "active", "MainPID": "100", "InvocationID": "current"}
    monkeypatch.setattr(verifier, "identity", lambda unit: dict(owner))
    control = {
        "state": "accepted_native_bounded_source_wait", "owner": dict(owner),
        "protected": {unit: dict(owner) for unit in verifier.PROTECTED},
        "code_sha256": {str(path): "unchanged" for path in verifier.CODE[:2]},
        "run_id": "105-current", "source_process": {"pid": 123},
        "wait_event": {"__REALTIME_TIMESTAMP": str(int(
            datetime(2026, 10, 14, 1, 25, tzinfo=timezone.utc).timestamp() * 1_000_000))},
    }
    status = {"run_id": "105-current", "scheduled_day_trade_markets": list("abcde")}
    monkeypatch.setattr(native, "status_for_owner", lambda: (status, SimpleNamespace(name="105")))
    monkeypatch.setattr(verifier, "process_identity", lambda pid: None if pid == 123 else {"pid": pid})
    monkeypatch.setattr(native, "engine_acknowledgements", lambda status, markets: [{"market": x} for x in markets])
    directory = tmp_path / "artifacts/data_preparation/tw_factorized_scale_cash_live_v1"
    target = directory / "generations/complete"
    target.mkdir(parents=True)
    manifest = target / "factorized_manifest.json"
    manifest.write_text(json.dumps({"status": "complete"}))
    (directory / "current").symlink_to(target)
    receipt = {"status": "verified", "phase": "preopen", "attempt": 1,
               "decision_session": "2026-10-14", "wall_seconds": 485.8,
               "manifest": str(manifest)}
    receipt_path = directory / "updater_status.json"
    receipt_path.write_text(json.dumps(receipt))
    market = "tw_day_trade_factorized_scale_separated_cash"
    journal = SimpleNamespace(stdout=("action=wait_bounded_source\n"
        f"[pre-signal:{market}] done returncode=0\n"
        f"[scheduled] market={market} status=awaiting_engine_confirmation session=2026-10-14\n"))
    monkeypatch.setattr(verifier.subprocess, "run", lambda *a, **kw: journal)
    return control, status, receipt, receipt_path, journal, native


def test_natural_terminal_readback_is_not_a_full_rehash_slo_or_broker_fill(completed):
    control, *_ = completed
    value = verifier.verify_completion(control)
    assert value["state"] == "accepted_native_source_preparation_terminal"
    assert value["source_ready"] is True
    assert len(value["engine_acknowledgements"]) == 5
    for key in ("opening_slo_verified", "broker_fill_verified", "full_projection_independently_rehashed",
                "services_changed_by_verifier", "trading_writes_by_verifier"):
        assert value[key] is False


@pytest.mark.parametrize("kind", ["unaccepted_control", "owner", "gateway_run", "live_child", "code",
                                  "old_receipt", "wrong_phase", "bad_wall", "restarted",
                                  "missing_terminal_log", "missing_ack", "manifest_incomplete"])
def test_terminal_acceptance_cannot_relabel_stale_restarted_or_partial_completion(
    completed, monkeypatch, kind,
):
    control, status, receipt, receipt_path, journal, native = completed
    if kind == "unaccepted_control":
        control["state"] = "waiting_source"
    elif kind == "owner":
        control["owner"]["InvocationID"] = "old"
    elif kind == "gateway_run":
        status["run_id"] = "old"
    elif kind == "live_child":
        monkeypatch.setattr(verifier, "process_identity", lambda pid: {"pid": pid})
    elif kind == "code":
        control["code_sha256"] = {}
    elif kind == "old_receipt":
        receipt["decision_session"] = "2026-10-13"
    elif kind == "wrong_phase":
        receipt["phase"] = "completed"
    elif kind == "bad_wall":
        receipt["wall_seconds"] = 1201
    elif kind == "restarted":
        journal.stdout += "action=exit_for_systemd_restart\n"
    elif kind == "missing_terminal_log":
        journal.stdout = "action=wait_bounded_source\n"
    elif kind == "missing_ack":
        monkeypatch.setattr(native, "engine_acknowledgements", lambda *a: [])
    elif kind == "manifest_incomplete":
        Path(receipt["manifest"]).write_text(json.dumps({"status": "running"}))
    receipt_path.write_text(json.dumps(receipt))
    with pytest.raises(ValueError):
        verifier.verify_completion(control)
