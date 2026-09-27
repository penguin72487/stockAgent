from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime, timedelta
import json
import subprocess
from types import SimpleNamespace

import pytest

from scripts import service_stage_journal as stages


@pytest.fixture
def evidence(monkeypatch):
    start = datetime(2026, 9, 25, 9, tzinfo=UTC)
    identity, boot = "a" * 32, "b" * 32
    attempt = {
        "last_attempt_invocation_id": identity, "last_attempt_boot_id": boot,
        "last_attempt_started_monotonic_us": 100_000_000,
        "last_attempt_completed_monotonic_us": 369_200_000,
        "last_attempt_started_at_utc": start.isoformat(),
        "last_attempt_completed_at_utc": (start + timedelta(seconds=269.2)).isoformat(),
        "last_attempt_outcome": "process_exited_zero", "last_attempt_exit_status": None,
    }
    events = []
    for stage, elapsed, offset in (("options_daily", 266, 266.1), ("recent_ticks", 2, 268.1),
                                    ("final_settlement", 1, 269.1)):
        events.append({
            "MESSAGE": f"[taifex-auxiliary] stage={stage} exit_status=0 elapsed_seconds={elapsed}",
            "_SYSTEMD_UNIT": stages.TAIFEX_AUXILIARY_UNIT, "_SYSTEMD_INVOCATION_ID": identity,
            "_BOOT_ID": boot, "__MONOTONIC_TIMESTAMP": str(int((100 + offset) * 1e6)),
            "__REALTIME_TIMESTAMP": str(int((start.timestamp() + offset) * 1e6)),
        })
    props = {"ActiveState": "inactive"}
    snapshot = {"observed_at": (start + timedelta(seconds=300)).isoformat(), "monotonic": 400.0}
    calls = []

    def query(args, **kwargs):
        calls.append((args, kwargs))
        return SimpleNamespace(returncode=0, stdout="\n".join(json.dumps(event) for event in reversed(events)))

    monkeypatch.setattr(stages.subprocess, "run", query)
    return props, snapshot, attempt, events, calls


def inspect(evidence):
    return stages.taifex_auxiliary_stage_receipt(*evidence[:3])


def test_exact_terminal_run_exposes_only_fixed_stage_timings(evidence):
    result = inspect(evidence)
    assert result["state"] == "completed"
    assert result["matches_last_attempt"] is True
    assert result["source_completeness"] == "not_checked"
    assert result["completion_claim"] == "recorded_wrapper_steps_only"
    assert result["timing_resolution_seconds"] == 1
    assert result["elapsed_seconds"] == 269.2
    assert [row["elapsed_seconds"] for row in result["step_receipts"]] == [266, 2, 1]
    args, kwargs = evidence[4][0]
    assert "_BOOT_ID=" + "b" * 32 in args
    assert "_SYSTEMD_INVOCATION_ID=" + "a" * 32 in args
    assert args[args.index("-n") + 1] == "16"
    assert kwargs["timeout"] == 2


@pytest.mark.parametrize("state", ["active", "activating", "deactivating", "reloading", None])
def test_nonterminal_never_reads_old_journal(evidence, state):
    evidence[0]["ActiveState"] = state
    assert inspect(evidence)["matches_last_attempt"] is False
    assert evidence[4] == []


@pytest.mark.parametrize("field,value", [
    ("last_attempt_outcome", "exit_status_unknown"),
    ("last_attempt_outcome", "failed"),
    ("last_attempt_exit_status", 1),
    ("last_attempt_exit_status", False),
    ("last_attempt_exit_status", 0.0),
    ("last_attempt_exit_code", "killed"),
    ("last_attempt_invocation_id", "invalid"),
    ("last_attempt_boot_id", "invalid"),
    ("last_attempt_started_monotonic_us", None),
    ("last_attempt_started_monotonic_us", True),
    ("last_attempt_completed_monotonic_us", 50_000_000),
    ("last_attempt_completed_at_utc", "2026-09-25T09:04:40+00:00"),
    ("last_attempt_completed_at_utc", "2026-09-25T09:04:29.2"),
])
def test_invalid_process_evidence_is_not_promoted(evidence, field, value):
    evidence[2][field] = value
    result = inspect(evidence)
    assert result["matches_last_attempt"] is False
    assert "step_receipts" not in result


@pytest.mark.parametrize("field,value", [
    ("_SYSTEMD_UNIT", "other.service"), ("_SYSTEMD_INVOCATION_ID", "c" * 32),
    ("_BOOT_ID", "c" * 32), ("MESSAGE", None), ("MESSAGE", ["bad"]),
    ("MESSAGE", "[taifex-auxiliary] stage=unknown exit_status=0 elapsed_seconds=266"),
    ("MESSAGE", "[taifex-auxiliary] stage=options_daily exit_status=256 elapsed_seconds=266"),
    ("MESSAGE", "[taifex-auxiliary] stage=options_daily exit_status=0 elapsed_seconds=1"),
    ("MESSAGE", "[taifex-auxiliary] stage=options_daily exit_status=0 elapsed_seconds=-266"),
    ("__MONOTONIC_TIMESTAMP", "99999999"), ("__MONOTONIC_TIMESTAMP", True),
    ("__MONOTONIC_TIMESTAMP", "99999999999999999999999"),
    ("__REALTIME_TIMESTAMP", "1"),
])
def test_invalid_or_cross_invocation_event_rejects_entire_group(evidence, field, value):
    evidence[3][0][field] = value
    assert inspect(evidence)["matches_last_attempt"] is False


@pytest.mark.parametrize("change", ["missing", "duplicate", "extra", "reversed_clocks", "same_mono", "rollback"])
def test_incomplete_duplicate_or_disordered_sequence_is_unknown(evidence, change):
    events = evidence[3]
    if change == "missing":
        events.pop()
    elif change == "duplicate":
        events.append(deepcopy(events[0]))
    elif change == "extra":
        events.append({})
    elif change == "reversed_clocks":
        events[0]["MESSAGE"], events[1]["MESSAGE"] = events[1]["MESSAGE"], events[0]["MESSAGE"]
    elif change == "same_mono":
        events[1]["__MONOTONIC_TIMESTAMP"] = events[0]["__MONOTONIC_TIMESTAMP"]
    else:
        events[1]["__REALTIME_TIMESTAMP"] = str(int(events[0]["__REALTIME_TIMESTAMP"]) - 1)
    assert inspect(evidence)["matches_last_attempt"] is False


def test_failed_step_must_agree_with_wrapper_exit(evidence):
    evidence[3][1]["MESSAGE"] = "[taifex-auxiliary] stage=recent_ticks exit_status=2 elapsed_seconds=2"
    assert inspect(evidence)["matches_last_attempt"] is False
    evidence[2].update(last_attempt_outcome="failed", last_attempt_exit_code="exited", last_attempt_exit_status=1)
    result = inspect(evidence)
    assert result["matches_last_attempt"] is True
    assert result["state"] == "completed_with_failures"
    assert result["failed_steps"] == ["recent_ticks"]
    evidence[2]["last_attempt_exit_code"] = "killed"
    assert inspect(evidence)["matches_last_attempt"] is False


def test_lock_skip_is_deferred_without_timings(evidence):
    evidence[3][:] = evidence[3][:1]
    evidence[3][0]["MESSAGE"] = stages._LOCK_SKIP
    result = inspect(evidence)
    assert result["state"] == "deferred"
    assert result["reason"] == "lock_busy"
    assert "step_receipts" not in result


@pytest.mark.parametrize("output", ["{", "null", "[1]", "x" * 65537, "\n" * 17])
def test_malformed_bounded_output_is_unknown(evidence, monkeypatch, output):
    monkeypatch.setattr(stages.subprocess, "run", lambda *a, **kw: SimpleNamespace(returncode=0, stdout=output))
    assert inspect(evidence)["matches_last_attempt"] is False


@pytest.mark.parametrize("error", [OSError("unavailable"), subprocess.TimeoutExpired("journalctl", 2)])
def test_query_failure_is_nonfatal(evidence, monkeypatch, error):
    def fail(*args, **kwargs):
        raise error
    monkeypatch.setattr(stages.subprocess, "run", fail)
    assert inspect(evidence)["matches_last_attempt"] is False


def test_current_process_uses_current_identity_not_old_journal(evidence, monkeypatch):
    monkeypatch.setattr(stages.Path, "read_text", lambda *_a, **_kw: "b" * 32)
    evidence[0].update(ExecMainStartTimestampMonotonic=100_000_000,
                       ExecMainExitTimestampMonotonic=369_200_000,
                       InvocationID="a" * 32, Result="success", ExecMainStatus=0)
    assert inspect(evidence)["matches_last_attempt"] is True
    evidence[0]["InvocationID"] = "c" * 32
    assert inspect(evidence)["matches_last_attempt"] is False


@pytest.mark.parametrize("sampled", [True, float("nan"), float("inf"), -1, 369.0, None])
def test_current_snapshot_clock_must_be_valid(evidence, monkeypatch, sampled):
    evidence[0].update(ExecMainStartTimestampMonotonic=100_000_000,
                       ExecMainExitTimestampMonotonic=369_200_000,
                       InvocationID="a" * 32, Result="success", ExecMainStatus=0)
    evidence[1]["monotonic"] = sampled
    assert inspect(evidence)["matches_last_attempt"] is False
