import json
from datetime import date
import os
from pathlib import Path
from types import SimpleNamespace
import subprocess
import sys
import pytest

from scripts import update_tw_day_trade_factorized_live as updater
from stockagent.live.tw_public_source_guard import TWPublicSourceUpdatePending
from stockagent.live.market_status import TwStockDayDecision
from scripts import prepare_tw_day_trade_factorized_live as builder
from stockagent.data.annual_cache_admission import AnnualCacheBuildDeferred, CONTRACT


@pytest.mark.skipif(os.name != "posix", reason="native WSL direct executable entrypoint")
def test_native_updater_is_executable_and_help_does_not_launch_the_data_owner():
    path = Path(updater.__file__).resolve()
    assert path.stat().st_mode & 0o111, "configured direct updater requires tracked executable mode"
    result = subprocess.run([str(path), "--help"], capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stderr
    assert "--phase {preopen,completed}" in result.stdout


@pytest.mark.parametrize("threads", [2, 3])
def test_fresh_source_entrypoint_configures_both_actual_pools_without_cuda(threads):
    script = """
import json, os, runpy, sys
sys.argv = [sys.argv[1], '--help']
try:
    runpy.run_path(sys.argv[0], run_name='__main__')
except SystemExit as exc:
    assert exc.code == 0
else:
    raise AssertionError('help did not exit before invoking the data owner')
import polars as pl, pyarrow as pa, torch
visible = len(os.sched_getaffinity(0)) if hasattr(os, 'sched_getaffinity') else (os.cpu_count() or 1)
expected = min(int(os.environ['STOCKAGENT_FACTORIZED_PREP_THREADS']), visible)
assert pl.thread_pool_size() == pa.cpu_count() == expected
assert os.environ['CUDA_VISIBLE_DEVICES'] == '' and not torch.cuda.is_initialized()
print(json.dumps({'polars': pl.thread_pool_size(), 'arrow': pa.cpu_count(), 'expected': expected}))
"""
    result = subprocess.run([sys.executable, "-c", script, str(Path(updater.__file__).resolve())],
        env={**os.environ, "STOCKAGENT_FACTORIZED_PREP_THREADS": str(threads),
             "POLARS_MAX_THREADS": "64", "OMP_NUM_THREADS": "64"},
        capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr
    observed = json.loads(result.stdout.splitlines()[-1])
    assert observed["polars"] == observed["arrow"] == observed["expected"]


@pytest.mark.parametrize("phase", ["preopen", "completed"])
def test_scheduled_entrypoint_uses_the_canonical_cache_and_records_actual_pools(tmp_path, monkeypatch, phase):
    monkeypatch.setattr(updater, "ROOT", tmp_path)
    monkeypatch.setattr(sys, "argv", [str(updater.__file__), "--phase", phase])
    monkeypatch.setattr(updater, "_projection_start_date", lambda *_: "2014-01-06")
    pools = {"polars_observed_threads": 2, "arrow_observed_threads": 2,
             "process_cpu_quota": False}
    commands, preparation = [], []

    def source_command(command, *, cwd, check):
        assert cwd == tmp_path and check is True
        commands.append(command)

    def prepare(**kwargs):
        preparation.append(kwargs)
        return {"status": "verified", "decision_session": "2026-10-14"}

    monkeypatch.setattr(updater.subprocess, "run", source_command)
    monkeypatch.setattr(updater, "prepare", prepare)
    monkeypatch.setattr(updater, "_processing_pools", lambda: pools)
    updater.main()
    assert len(commands) == len(preparation) == 1
    expected_script = ("activate_tw_public_opening_data.py" if phase == "preopen"
                       else "finalize_tw_public_completed_session.py")
    assert commands[0][:2] == [sys.executable, str(tmp_path / "scripts" / expected_script)]
    assert preparation[0]["annual_cache_root"] == tmp_path / "artifacts/cache/tw_factorized_live/annual"
    assert preparation[0]["start_date"] == "2014-01-06"
    receipt = tmp_path / "artifacts/data_preparation/tw_factorized_scale_cash_live_v1/updater_status.json"
    status = json.loads(receipt.read_text())
    assert status["status"] == "verified" and status["phase"] == phase
    assert status["attempt"] == 1 and status["processing_pools"] == pools
    assert status["requested_start_date"] == "2014-01-06"


@pytest.mark.parametrize("configured,expected", [("2014-01-06", "2014-01-06"),
                                                  ("2026-01-01", "2026-01-01"),
                                                  (None, "2014-01-06")])
def test_projection_scope_follows_selected_full_horizon(tmp_path, monkeypatch, configured, expected):
    parent = tmp_path / "parent.json"
    parent.write_text(json.dumps({"dates": ["2014-01-06", "2026-10-16"]}))
    monkeypatch.setattr(updater, "load_config", lambda _: SimpleNamespace(
        data=SimpleNamespace(panel_start_date=configured)))
    assert updater._projection_start_date(tmp_path / "config.yaml", parent) == expected


def test_projection_rejects_an_uncovered_earlier_horizon(tmp_path, monkeypatch):
    parent = tmp_path / "parent.json"
    parent.write_text(json.dumps({"dates": ["2014-01-06", "2026-10-16"]}))
    monkeypatch.setattr(updater, "load_config", lambda _: SimpleNamespace(
        data=SimpleNamespace(panel_start_date="2013-01-01")))
    with pytest.raises(ValueError, match="precedes"):
        updater._projection_start_date(tmp_path / "config.yaml", parent)


@pytest.mark.parametrize("dates", [[], ["bad-date"], ["2026-10-16", "2014-01-06"],
                                    ["2014-01-06", "2014-01-06"], [None]])
def test_projection_rejects_invalid_parent_calendar(tmp_path, monkeypatch, dates):
    parent = tmp_path / "parent.json"
    parent.write_text(json.dumps({"dates": dates}))
    monkeypatch.setattr(updater, "load_config", lambda _: SimpleNamespace(
        data=SimpleNamespace(panel_start_date=None)))
    with pytest.raises(ValueError):
        updater._projection_start_date(tmp_path / "config.yaml", parent)


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


@pytest.mark.parametrize("phase", ["preopen", "completed"])
def test_denied_cache_build_records_the_existing_owner_without_repeating(tmp_path, monkeypatch, phase):
    clock = _clock(monkeypatch)
    calls = []
    details = {"contract": CONTRACT, "reason": "insufficient_memory_headroom",
               "source": str(tmp_path / "2025.parquet"), "immutable_seed": True,
               "required_headroom_bytes": 26 << 30, "observed_headroom_bytes": 23 << 30}
    failure = AnnualCacheBuildDeferred(details)

    def operation():
        calls.append(clock[0])
        raise failure

    receipt = tmp_path / "status.json"
    with pytest.raises(AnnualCacheBuildDeferred) as error:
        updater._prepare_with_retry(operation, receipt_path=receipt, phase=phase,
                                    attempts=4, source_wait_seconds=600)
    assert error.value is failure
    assert calls == [0.0] and clock[0] == 0.0
    status = json.loads(receipt.read_text())
    assert status["status"] == "cache_warming_required"
    assert status["phase"] == phase and status["attempt"] == 1
    assert status["admission"] == details
    assert status["retry_owner"] == "stockagent-discord-artifact-maintenance.service"
    assert status["model_executed"] is False
    assert status["last_good_generation_retained"] is True
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
