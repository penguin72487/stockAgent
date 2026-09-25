from __future__ import annotations

import json

import pytest

from scripts import track_service_runtime_trends as tracker
from scripts import benchmark_dashboard_latency as benchmark
from scripts import audit_service_latency_coverage as audit


@pytest.fixture(autouse=True)
def _stub_schedule_snapshot(monkeypatch):
    monkeypatch.setattr(audit, "schedule_snapshot", lambda: {"timers": {
        "stockagent-healthy.timer": {
            "UnitFileState": "enabled", "ActiveState": "active",
            "SubState": "waiting",
        }
    }})


def _snapshot(
    when: float, *, invocation: str = "one", cpu_ns: int = 1_000_000_000,
    active: bool = False,
):
    return {
        "monotonic": when,
        "query_ms": 12.5,
        "units": {
            "stockagent-job.service": {
                "ActiveState": "active" if active else "inactive",
                "Result": "success",
                "ExecMainStatus": 0,
                "last_attempt_seconds": 2.0,
                "InvocationID": invocation,
                "MainPID": "42",
                "NRestarts": "0",
                "CPUUsageNSec": cpu_ns,
                "IOReadBytes": None,
                "IOWriteBytes": None,
                "CgroupIOReadBytes": 100,
                "CgroupIOWriteBytes": 500,
                "MemoryCurrent": None,
            }
        },
    }


def test_tracker_samples_all_units_at_bounded_cadence(tmp_path, monkeypatch):
    path = tmp_path / "baseline.json"
    monkeypatch.setattr(tracker, "_boot_id", lambda: "same-boot")
    snapshots = iter([
        _snapshot(100, active=True),
        _snapshot(401, cpu_ns=3_000_000_000, active=True),
    ])
    monkeypatch.setattr(benchmark, "service_snapshot", lambda: next(snapshots))

    baseline = tracker.sample_service_runtime(path, now_monotonic=100)
    assert baseline["service_count"] == 1
    assert baseline["interval_seconds"] is None
    assert baseline["services"][0]["cpu_cores_average"] is None
    assert baseline["recurring_timers"] == {
        "state": "observed_clear", "timer_count": 1, "findings": [],
    }
    assert tracker.sample_service_runtime(path, now_monotonic=399) is None

    measured = tracker.sample_service_runtime(path, now_monotonic=401)
    assert measured["interval_seconds"] == 301
    assert measured["services"][0]["cpu_cores_average"] == round(2 / 301, 4)
    assert measured["services"][0]["last_completed_process_wall_seconds"] is None
    assert measured["services"][0]["write_bytes_delta"] == 0


def test_io_trend_uses_cgroup_counters_not_unavailable_systemd_accounting(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(tracker, "_boot_id", lambda: "same-boot")
    first = _snapshot(100, active=True)
    second = _snapshot(401, active=True)
    first["units"]["stockagent-job.service"]["IOWriteBytes"] = 900
    second["units"]["stockagent-job.service"]["IOWriteBytes"] = 1_000
    second["units"]["stockagent-job.service"]["CgroupIOReadBytes"] = 1_100
    second["units"]["stockagent-job.service"]["CgroupIOWriteBytes"] = 2_500
    snapshots = iter([first, second])
    monkeypatch.setattr(benchmark, "service_snapshot", lambda: next(snapshots))
    path = tmp_path / "baseline.json"
    tracker.sample_service_runtime(path, now_monotonic=100)
    row = tracker.sample_service_runtime(path, now_monotonic=401)["services"][0]
    assert row["read_bytes_delta"] == 1_000
    assert row["write_bytes_delta"] == 2_000


def test_tracker_records_unarmed_recurring_timer_without_treating_observation_failure_as_clear(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(tracker, "_boot_id", lambda: "same-boot")
    snapshot = _snapshot(100)
    snapshot["units"]["stockagent-job.service"]["ActiveState"] = "inactive"
    monkeypatch.setattr(benchmark, "service_snapshot", lambda: snapshot)
    monkeypatch.setattr(audit, "schedule_snapshot", lambda: {"timers": {
        "stockagent-job.timer": {
            "UnitFileState": "enabled", "ActiveState": "active",
            "SubState": "elapsed", "TimersMonotonic": "OnUnitInactiveUSec=1min",
            "Unit": "stockagent-job.service", "NextElapseUSecRealtime": "",
            "NextElapseUSecMonotonic": "infinity",
        }
    }})
    result = tracker.sample_service_runtime(tmp_path / "baseline.json", now_monotonic=100)
    assert result["recurring_timers"]["state"] == "attention"
    assert result["recurring_timers"]["findings"][0]["unit"] == "stockagent-job.timer"

    def unavailable():
        raise OSError("systemd unreachable")

    monkeypatch.setattr(audit, "schedule_snapshot", unavailable)
    failure = tracker._recurring_timer_health(snapshot)
    assert failure == {
        "state": "unavailable", "timer_count": None, "findings": [],
        "error_type": "OSError",
    }
    monkeypatch.setattr(audit, "schedule_snapshot", lambda: {"timers": {}})
    empty = tracker._recurring_timer_health(snapshot)
    assert empty == {
        "state": "unavailable", "timer_count": 0, "findings": [],
        "error_type": "NoTimersObserved",
    }


def test_filesystem_space_trend_does_not_attribute_volume_change_to_service(
    tmp_path, monkeypatch
):
    path = tmp_path / "baseline.json"
    monkeypatch.setattr(tracker, "_boot_id", lambda: "same-boot")
    snapshots = iter([_snapshot(100), _snapshot(401), _snapshot(702)])
    monkeypatch.setattr(benchmark, "service_snapshot", lambda: next(snapshots))
    spaces = iter([
        {"device_id": 7, "total_bytes": 1_000, "available_bytes": 900},
        {"device_id": 7, "total_bytes": 1_000, "available_bytes": 700},
        {"device_id": 8, "total_bytes": 1_000, "available_bytes": 600},
    ])
    monkeypatch.setattr(tracker, "_filesystem_space", lambda: next(spaces))

    first = tracker.sample_service_runtime(path, now_monotonic=100)
    assert first["filesystem"]["available_delta_bytes"] is None
    second = tracker.sample_service_runtime(path, now_monotonic=401)
    assert second["filesystem"]["available_bytes"] == 700
    assert second["filesystem"]["available_delta_bytes"] == -200
    assert "not attributable to one service" in second["boundary"]
    third = tracker.sample_service_runtime(path, now_monotonic=702)
    assert third["filesystem"]["available_delta_bytes"] is None


def test_filesystem_space_unavailable_is_explicit(tmp_path):
    assert tracker._filesystem_space(tmp_path / "missing") is None


def test_restart_never_subtracts_unrelated_counters(tmp_path, monkeypatch):
    path = tmp_path / "baseline.json"
    monkeypatch.setattr(tracker, "_boot_id", lambda: "same-boot")
    snapshots = iter([_snapshot(100), _snapshot(401, invocation="two", cpu_ns=100)])
    monkeypatch.setattr(benchmark, "service_snapshot", lambda: next(snapshots))

    tracker.sample_service_runtime(path, now_monotonic=100)
    measured = tracker.sample_service_runtime(path, now_monotonic=401)
    row = measured["services"][0]
    assert row["sample_continuous"] is False
    assert row["cpu_cores_average"] is None
    assert row["read_bytes_delta"] is None


def test_inactive_oneshot_is_not_reported_as_zero_cpu(tmp_path, monkeypatch):
    path = tmp_path / "baseline.json"
    monkeypatch.setattr(tracker, "_boot_id", lambda: "same-boot")
    snapshots = iter([_snapshot(100), _snapshot(401)])
    monkeypatch.setattr(benchmark, "service_snapshot", lambda: next(snapshots))

    tracker.sample_service_runtime(path, now_monotonic=100)
    row = tracker.sample_service_runtime(path, now_monotonic=401)["services"][0]
    assert row["last_completed_process_wall_seconds"] == 2.0
    assert row["last_attempt_invocation_id"] == "one"
    assert row["cpu_cores_average"] is None
    assert row["write_bytes_delta"] is None
    assert row["sample_continuous"] is False


def test_unloaded_oneshot_keeps_journal_wall_and_outcome(tmp_path, monkeypatch):
    path = tmp_path / "baseline.json"
    monkeypatch.setattr(tracker, "_boot_id", lambda: "same-boot")
    snapshot = _snapshot(100)
    props = snapshot["units"]["stockagent-job.service"]
    props["InvocationID"] = ""
    props["last_attempt_seconds"] = None
    props["last_attempt_outcome"] = "not_observed"
    monkeypatch.setattr(benchmark, "service_snapshot", lambda: snapshot)
    monkeypatch.setattr(audit, "_journal_last_completed_process", lambda unit: {
        "last_attempt_wall_seconds": 12.5,
        "last_attempt_outcome": "failed",
        "last_attempt_exit_status": 1,
        "last_attempt_exit_code": "exited",
        "last_attempt_invocation_id": "completed-invocation",
        "last_attempt_boot_id": "previous-boot",
        "last_attempt_started_at_utc": "2026-09-23T23:59:47+00:00",
        "last_attempt_completed_at_utc": "2026-09-24T00:00:00+00:00",
    } if unit == "stockagent-job.service" else None)

    row = tracker.sample_service_runtime(path, now_monotonic=100)["services"][0]
    assert row["last_completed_process_wall_seconds"] == 12.5
    assert row["last_completed_process_source"] == "systemd_journal"
    assert row["last_attempt_outcome"] == "failed"
    assert row["exit_status"] == 1
    assert row["exit_code"] == "exited"
    assert row["last_attempt_invocation_id"] == "completed-invocation"
    assert row["last_attempt_boot_id"] == "previous-boot"
    assert row["last_completed_at_utc"] == "2026-09-24T00:00:00+00:00"
    assert row["cpu_cores_average"] is None


def test_boot_change_disables_cross_boot_delta(tmp_path, monkeypatch):
    path = tmp_path / "baseline.json"
    boot = ["first"]
    monkeypatch.setattr(tracker, "_boot_id", lambda: boot[0])
    snapshots = iter([_snapshot(100), _snapshot(10, cpu_ns=2_000_000_000)])
    monkeypatch.setattr(benchmark, "service_snapshot", lambda: next(snapshots))

    tracker.sample_service_runtime(path, now_monotonic=100)
    boot[0] = "second"
    measured = tracker.sample_service_runtime(path, now_monotonic=10)
    assert measured["interval_seconds"] is None
    assert measured["services"][0]["cpu_cores_average"] is None


def test_hardened_service_without_boot_id_still_throttles(tmp_path, monkeypatch):
    path = tmp_path / "baseline.json"
    monkeypatch.setattr(tracker, "_boot_id", lambda: None)
    snapshots = iter([
        _snapshot(100, active=True),
        _snapshot(401, cpu_ns=3_000_000_000, active=True),
    ])
    monkeypatch.setattr(benchmark, "service_snapshot", lambda: next(snapshots))

    tracker.sample_service_runtime(path, now_monotonic=100)
    assert tracker.sample_service_runtime(path, now_monotonic=399) is None
    measured = tracker.sample_service_runtime(path, now_monotonic=401)
    assert measured["interval_seconds"] == 301
    assert measured["services"][0]["cpu_cores_average"] == round(2 / 301, 4)


def test_tracker_keeps_deferred_business_state_distinct_from_process_success(
    tmp_path, monkeypatch
):
    receipt = tmp_path / "refresh_receipt.json"
    receipt.write_text(json.dumps({
        "started_at_utc": "2026-09-23T19:59:00+00:00",
        "state": "deferred_source_changed",
    }))
    monkeypatch.setattr(audit, "BUSINESS_RECEIPTS", {"stockagent-job.service": receipt})
    monkeypatch.setattr(tracker, "_boot_id", lambda: "same-boot")
    snapshot = _snapshot(500)
    snapshot["observed_at"] = "2026-09-23T20:00:00+00:00"
    snapshot["units"]["stockagent-job.service"]["ExecMainStartTimestampMonotonic"] = 440_000_000
    monkeypatch.setattr(benchmark, "service_snapshot", lambda: snapshot)

    row = tracker.sample_service_runtime(
        tmp_path / "baseline.json", now_monotonic=500
    )["services"][0]
    assert row["result"] == "success"
    assert row["business_receipt"]["matches_last_attempt"] is True
    assert row["business_receipt"]["state"] == "deferred_source_changed"
