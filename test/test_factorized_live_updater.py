import json
from datetime import date
import os
from pathlib import Path
import subprocess
import pytest

from scripts import update_tw_day_trade_factorized_live as updater
from stockagent.live.tw_public_source_guard import TWPublicSourceUpdatePending
from stockagent.live.market_status import TwStockDayDecision
from scripts import prepare_tw_day_trade_factorized_live as builder


@pytest.mark.skipif(os.name != "posix", reason="native WSL direct executable entrypoint")
def test_native_updater_is_executable_and_help_does_not_launch_the_data_owner():
    path = Path(updater.__file__).resolve()
    assert path.stat().st_mode & 0o111, "configured direct updater requires tracked executable mode"
    result = subprocess.run([str(path), "--help"], capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stderr
    assert "--phase {preopen,completed}" in result.stdout


def _clock(monkeypatch):
    clock = [0.0]
    monkeypatch.setattr(updater.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(updater.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + seconds))
    return clock


def test_writer_race_waits_without_consuming_build_attempts(tmp_path, monkeypatch):
    clock = _clock(monkeypatch)
    calls = []

    def operation():
        calls.append(clock[0])
        if len(calls) <= 3:
            raise TWPublicSourceUpdatePending(tmp_path, tmp_path / "writer.lock")
        return {"status": "already_current", "decision_session": "2026-10-14"}

    receipt = tmp_path / "status.json"
    result = updater._prepare_with_retry(operation, receipt_path=receipt, phase="completed",
                                        attempts=1, source_wait_seconds=10)
    assert result["status"] == "already_current"
    assert calls == [0, 2, 4, 6]
    assert json.loads(receipt.read_text())["attempt"] == 1


def test_writer_wait_expiry_fails_visibly_and_retains_last_good(tmp_path, monkeypatch):
    _clock(monkeypatch)
    receipt = tmp_path / "status.json"

    def operation():
        raise TWPublicSourceUpdatePending(tmp_path, tmp_path / "writer.lock")

    with pytest.raises(TWPublicSourceUpdatePending):
        updater._prepare_with_retry(operation, receipt_path=receipt, phase="preopen",
                                    attempts=2, source_wait_seconds=3)
    status = json.loads(receipt.read_text())
    assert status["status"] == "failed_retryable"
    assert status["source_wait_expired"] is True
    assert status["model_executed"] is False
    assert status["last_good_generation_retained"] is True


def test_corruption_is_not_treated_as_a_busy_writer(tmp_path, monkeypatch):
    _clock(monkeypatch)
    calls = []

    def operation():
        calls.append(True)
        raise ValueError("immutable source member changed")

    receipt = tmp_path / "status.json"
    with pytest.raises(ValueError, match="member changed"):
        updater._prepare_with_retry(operation, receipt_path=receipt, phase="completed",
                                    attempts=2, source_wait_seconds=600)
    assert len(calls) == 2
    status = json.loads(receipt.read_text())
    assert status["status"] == "failed_retryable"
    assert status["attempt"] == 2
    assert status["error_type"] == "ValueError"
    assert "source_wait_expired" not in status


def test_projection_next_clock_reuses_official_calendar_not_future_eligibility(tmp_path, monkeypatch):
    decisions = {date(2026, 10, 9): TwStockDayDecision("closed", "official holiday"),
                 date(2026, 10, 10): TwStockDayDecision("closed", "official weekend"),
                 date(2026, 10, 11): TwStockDayDecision("closed", "official weekend"),
                 date(2026, 10, 12): TwStockDayDecision("scheduled_open", "official schedule")}
    checked = []

    def classify(day, *, parquet_root):
        assert parquet_root == tmp_path / "stocks"
        checked.append(day)
        return decisions[day]

    monkeypatch.setattr(builder, "tw_stock_day_decision", classify)
    day, proof = builder._next_projection_session(tmp_path, date(2026, 10, 8))
    assert day == date(2026, 10, 12)
    assert checked == list(decisions)
    assert proof == {"status": "scheduled_open", "reason": "official schedule",
                     "execution_or_eligibility_proof": False}


def test_projection_unknown_calendar_cannot_skip_to_a_weekday(tmp_path, monkeypatch):
    monkeypatch.setattr(builder, "tw_stock_day_decision", lambda *_args, **_kwargs:
                        TwStockDayDecision("unknown", "official schedule missing"))
    with pytest.raises(ValueError, match="calendar is unknown"):
        builder._next_projection_session(tmp_path, date(2026, 10, 13))
