from __future__ import annotations

import json
import hashlib
from types import SimpleNamespace

import pytest

from scripts import track_service_runtime_trends as tracker
from scripts import benchmark_dashboard_latency as benchmark
from scripts import audit_service_latency_coverage as audit


def test_small_journal_sample_preserves_original_event():
    sample = {"event": "all_service_runtime_sample", "services": [], "service_count": 0}
    assert tracker.runtime_sample_journal_lines(sample) == [
        json.dumps(sample, separators=(",", ":"))
    ]


@pytest.mark.parametrize("value", ["plain", '"\\\n\t', "台灣資料🧪"])
def test_large_journal_sample_frames_are_bounded_and_lossless(value):
    sample = {
        "event": "all_service_runtime_sample", "service_count": 57,
        "services": [{"unit": "test", "business_receipt": value * 40_000}],
    }
    original = json.dumps(sample, separators=(",", ":"))
    lines = tracker.runtime_sample_journal_lines(sample)
    assert len(lines) > 1
    assert all(len(line.encode("utf-8")) <= 32 * 1024 for line in lines)
    frames = [json.loads(line) for line in lines]
    assert {f["event"] for f in frames} == {"all_service_runtime_sample_chunk"}
    assert {f["part_count"] for f in frames} == {len(frames)}
    assert [f["part_index"] for f in frames] == list(range(len(frames)))
    assert len({f["sample_id"] for f in frames}) == 1
    reconstructed = "".join(f["payload_fragment"] for f in frames)
    assert reconstructed == original
    assert json.loads(reconstructed) == sample
    assert {f["payload_sha256"] for f in frames} == {
        hashlib.sha256(reconstructed.encode("ascii")).hexdigest()
    }
    # Lost/changed frames cannot silently pass the recorded payload checksum.
    incomplete = "".join(f["payload_fragment"] for f in frames[1:])
    assert hashlib.sha256(incomplete.encode("ascii")).hexdigest() != frames[0]["payload_sha256"]
    repeated = [json.loads(line) for line in tracker.runtime_sample_journal_lines(sample)]
    assert repeated[0]["sample_id"] != frames[0]["sample_id"]
    assert repeated[0]["payload_sha256"] == frames[0]["payload_sha256"]


def test_journal_sample_frame_boundary_and_nonfinite_values():
    prefix = {"value": ""}
    overhead = len(json.dumps(prefix, separators=(",", ":")))
    exact = {"value": "x" * (tracker.JOURNAL_LINE_MAX_BYTES - overhead)}
    assert len(tracker.runtime_sample_journal_lines(exact)) == 1
    above = {"value": exact["value"] + "x"}
    assert len(tracker.runtime_sample_journal_lines(above)) > 1
    for invalid in [float("nan"), float("inf"), -float("inf")]:
        with pytest.raises(ValueError):
            tracker.runtime_sample_journal_lines({"value": invalid})


@pytest.mark.parametrize("serialization_fails", [False, True])
def test_snapshot_main_isolates_journal_framing_from_status_publication(
    tmp_path, monkeypatch, capsys, serialization_fails,
):
    from scripts import snapshot_data_refresh_services as snapshot

    monkeypatch.setattr(snapshot, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(snapshot, "parse_args", lambda: SimpleNamespace(
        output=tmp_path / "services.json", public_status_output=tmp_path / "public.json",
        feature_inventory_output=tmp_path / "features.json",
    ))
    monkeypatch.setattr(snapshot, "_refresh_service_states", lambda **_: {
        "fixture": {"evidence_source": "systemd_live"},
    })
    monkeypatch.setattr(snapshot, "InventorySnapshot", lambda *_: object())
    inventory = {
        "datasets": {}, "cached_files": 0, "refreshed_files": 0,
        "identity_rechecked_files": 0, "identity_unbound_files": 0,
    }
    monkeypatch.setattr(snapshot, "build_record_inventory", lambda *_, **__: inventory)
    monkeypatch.setattr(snapshot, "build_shioaji_public_status", lambda *_, **__: {
        "read_only": True, "simulation_only": True, "production_order_possible": False,
    })
    monkeypatch.setattr(snapshot, "build_data_monitor_public_status", lambda *_, **__: {"sources": []})
    monkeypatch.setattr(snapshot, "feature_source_metadata_sha256", lambda *_: "a" * 64)
    monkeypatch.setattr(snapshot, "_current_feature_snapshot", lambda *_, **__: 0)
    monkeypatch.setattr(snapshot, "_write_public_summary_snapshot", lambda *_: None)
    writes = []
    monkeypatch.setattr(snapshot, "_atomic_json", lambda path, *_, **__: writes.append(path.name))
    sample = {"event": "all_service_runtime_sample", "services": [], "padding": "x" * 40_000}
    monkeypatch.setattr(tracker, "sample_service_runtime", lambda *_: sample)
    if serialization_fails:
        def fail(_sample):
            raise ValueError("fixture encoder failure must not stop dashboard projection")
        monkeypatch.setattr(tracker, "runtime_sample_journal_lines", fail)
    assert snapshot.main() == 0
    assert {"services.json", "public.json", "shioaji_status.json"} <= set(writes)
    events = [json.loads(line) for line in capsys.readouterr().out.splitlines() if line.startswith("{")]
    assert any(e["event"] == "data_monitor_timing" for e in events)
    failures = [e for e in events if e["event"] == "all_service_runtime_sample_failed"]
    chunks = [e for e in events if e["event"] == "all_service_runtime_sample_chunk"]
    if serialization_fails:
        assert failures == [{"event": "all_service_runtime_sample_failed", "error_type": "ValueError"}]
        assert not chunks
    else:
        assert not failures
        assert json.loads("".join(e["payload_fragment"] for e in chunks)) == sample


@pytest.fixture(autouse=True)
def _stub_schedule_snapshot(monkeypatch):
    monkeypatch.setattr(audit, "schedule_snapshot", lambda: {"timers": {
        "stockagent-healthy.timer": {
            "UnitFileState": "enabled", "ActiveState": "active",
            "SubState": "waiting",
        }
    }})
    monkeypatch.setattr(audit, "repository_process_snapshot", lambda: {
        "inspection_state": "observed", "monotonic": 0.0, "processes": [],
    })


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


def _repository_snapshot(when, *, write=10, start=700):
    return {"inspection_state": "observed", "monotonic": when, "processes": [{
        "pid": 42, "ppid": 1, "comm": "python", "stockagent_unit": None,
        "start_time_ticks": start,
        "io_counters": {"read_bytes": 0, "write_bytes": write, "cancelled_write_bytes": 0},
    }]}


@pytest.mark.parametrize("hardened", [False, True])
def test_tracker_reuses_periodic_sampler_for_unmanaged_process_io(tmp_path, monkeypatch, hardened):
    monkeypatch.setattr(tracker, "_boot_id", lambda: None if hardened else "boot-one")
    snapshots = iter([
        _snapshot(100, invocation="a" * 32, active=True),
        _snapshot(401, invocation="a" * 32, active=True),
    ])
    processes = iter([_repository_snapshot(100), _repository_snapshot(401, write=2010)])
    monkeypatch.setattr(benchmark, "service_snapshot", lambda: next(snapshots))
    calls = []
    def process_snapshot():
        calls.append("capture")
        return next(processes)
    monkeypatch.setattr(audit, "repository_process_snapshot", process_snapshot)
    path = tmp_path / "baseline.json"
    first = tracker.sample_service_runtime(path, now_monotonic=100)
    assert first["repository_process_io"]["processes"][0]["write_bytes_delta"] is None
    assert tracker.sample_service_runtime(path, now_monotonic=200) is None
    assert calls == ["capture"]  # no new proc walk on an ordinary status refresh
    second = tracker.sample_service_runtime(path, now_monotonic=401)
    measurement = second["repository_process_io"]
    assert measurement["sample_seconds"] == 301
    assert measurement["processes"][0]["write_bytes_delta"] == 2000
    assert measurement["processes"][0]["stockagent_unit"] is None
    assert measurement["epoch_continuity_source"] == (
        "systemd_invocation_anchor" if hardened else "kernel_boot_id"
    )
    assert measurement["epoch_continuity_anchor_unit"] == (
        "stockagent-job.service" if hardened else None
    )
    assert json.loads(path.read_bytes())["repository_processes"]["processes"][0]["io_counters"]["write_bytes"] == 2010


@pytest.mark.parametrize("anchor_changed", ["invocation", "pid", "inactive", "invalid_uuid"])
def test_unknown_boot_requires_real_unchanged_invocation_for_process_and_volume_deltas(
    tmp_path, monkeypatch, anchor_changed,
):
    monkeypatch.setattr(tracker, "_boot_id", lambda: None)
    first = _snapshot(100, invocation="a" * 32, active=True)
    second = _snapshot(401, invocation="a" * 32, active=True)
    props = second["units"]["stockagent-job.service"]
    if anchor_changed == "invocation":
        props["InvocationID"] = "b" * 32
    elif anchor_changed == "pid":
        props["MainPID"] = "43"
    elif anchor_changed == "inactive":
        props["ActiveState"] = "inactive"
    else:
        props["InvocationID"] = "a-non-uuid"
    units = iter([first, second])
    processes = iter([_repository_snapshot(100), _repository_snapshot(401, write=2010)])
    spaces = iter([
        {"device_id": 1, "total_bytes": 1000, "available_bytes": 900},
        {"device_id": 1, "total_bytes": 1000, "available_bytes": 700},
    ])
    monkeypatch.setattr(benchmark, "service_snapshot", lambda: next(units))
    monkeypatch.setattr(audit, "repository_process_snapshot", lambda: next(processes))
    monkeypatch.setattr(tracker, "_filesystem_space", lambda: next(spaces))
    path = tmp_path / "baseline.json"
    tracker.sample_service_runtime(path, now_monotonic=100)
    result = tracker.sample_service_runtime(path, now_monotonic=401)
    assert result["repository_process_io"]["epoch_continuity_source"] == "unproven"
    assert result["repository_process_io"]["processes"][0]["write_bytes_delta"] is None
    assert result["filesystem"]["available_delta_bytes"] is None


def test_optional_process_inspection_failure_cannot_erase_service_baseline(tmp_path, monkeypatch):
    def denied():
        raise PermissionError("fixture private details must not appear")
    monkeypatch.setattr(audit, "repository_process_snapshot", denied)
    monkeypatch.setattr(benchmark, "service_snapshot", lambda: _snapshot(100))
    monkeypatch.setattr(tracker, "_boot_id", lambda: "same-boot")
    path = tmp_path / "baseline.json"
    result = tracker.sample_service_runtime(path, now_monotonic=100)
    assert result["service_count"] == 1
    assert result["repository_process_io"]["snapshots_available"] is False
    baseline = json.loads(path.read_bytes())
    assert baseline["repository_processes"]["error_type"] == "PermissionError"
    assert "private details" not in json.dumps(baseline)
    assert baseline["sample"]["units"]["stockagent-job.service"]["InvocationID"] == "one"


def test_optional_process_baseline_huge_clock_recovers_without_poisoning_services(tmp_path, monkeypatch):
    monkeypatch.setattr(tracker, "_boot_id", lambda: "same-boot")
    units = iter([_snapshot(100), _snapshot(401)])
    processes = iter([_repository_snapshot(100), _repository_snapshot(401, write=2010)])
    monkeypatch.setattr(benchmark, "service_snapshot", lambda: next(units))
    monkeypatch.setattr(audit, "repository_process_snapshot", lambda: next(processes))
    path = tmp_path / "baseline.json"
    tracker.sample_service_runtime(path, now_monotonic=100)
    payload = json.loads(path.read_bytes())
    payload["repository_processes"]["monotonic"] = 10**1000
    path.write_text(json.dumps(payload))
    result = tracker.sample_service_runtime(path, now_monotonic=401)
    assert result["service_count"] == 1
    assert result["repository_process_io"]["sample_seconds"] is None
    assert result["repository_process_io"]["processes"][0]["measurement_state"] == "sampling_clock_invalid"
    assert json.loads(path.read_bytes())["repository_processes"]["monotonic"] == 401


def test_filesystem_resize_does_not_become_a_writer_growth_delta(tmp_path, monkeypatch):
    monkeypatch.setattr(tracker, "_boot_id", lambda: "same-boot")
    units = iter([_snapshot(100), _snapshot(401)])
    spaces = iter([
        {"device_id": 7, "total_bytes": 1000, "available_bytes": 700},
        {"device_id": 7, "total_bytes": 2000, "available_bytes": 1700},
    ])
    monkeypatch.setattr(benchmark, "service_snapshot", lambda: next(units))
    monkeypatch.setattr(tracker, "_filesystem_space", lambda: next(spaces))
    path = tmp_path / "baseline.json"
    tracker.sample_service_runtime(path, now_monotonic=100)
    assert tracker.sample_service_runtime(path, now_monotonic=401)["filesystem"]["available_delta_bytes"] is None


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


@pytest.mark.parametrize("bad", [
    "{", "[]", "null", "[" * 2000 + "]" * 2000,
    '{"schema_version":1,"sample":{"monotonic":NaN,"units":{}}}',
    '{"schema_version":1,"sample":{"monotonic":Infinity,"units":{}}}',
    '{"schema_version":1,"sample":{"monotonic":1e999,"units":{}}}',
    '{"schema_version":1,"sample":{"monotonic":-1,"units":{}}}',
    '{"schema_version":true,"sample":{"monotonic":1,"units":{}}}',
    '{"schema_version":2,"sample":{"monotonic":1,"units":{}}}',
    '{"schema_version":1,"sample":{"monotonic":true,"units":{}}}',
    '{"schema_version":1,"sample":{"monotonic":100,"units":[]}}',
    '{"schema_version":1,"sample":{"monotonic":100,"units":{"stockagent-job.service":[]}}}',
    '{"schema_version":1,"sample":{"monotonic":100,"units":{"stockagent-job.service":null}}}',
    '{"schema_version":1,"boot_id":[],"sample":{"monotonic":100,"units":{}}}',
])
def test_corrupt_baseline_recovers_once_without_crossing_invalid_interval(tmp_path, monkeypatch, bad):
    path = tmp_path / "baseline.json"
    path.write_text(bad)
    calls = []
    monkeypatch.setattr(tracker, "_boot_id", lambda: "same-boot")

    def current():
        calls.append(True)
        return _snapshot(401, active=True)

    monkeypatch.setattr(benchmark, "service_snapshot", current)
    first = tracker.sample_service_runtime(path, now_monotonic=401)
    assert first["baseline_state"].startswith("reset_")
    assert first["interval_seconds"] is None
    row = first["services"][0]
    assert row["sample_continuous"] is False
    assert row["cpu_cores_average"] is None
    assert row["read_bytes_delta"] is None
    assert row["write_bytes_delta"] is None
    assert json.loads(path.read_text())["sample"]["monotonic"] == 401
    assert tracker.sample_service_runtime(path, now_monotonic=402) is None
    assert len(calls) == 1  # Recovery restores the ordinary cheap cadence gate.
    assert tracker.runtime_sample_journal_lines(first)


def test_oversized_baseline_is_bounded_and_replaced_with_full_current_inventory(tmp_path, monkeypatch):
    path = tmp_path / "baseline.json"
    path.write_bytes(b" " * (tracker.BASELINE_MAX_BYTES + 1))
    monkeypatch.setattr(benchmark, "service_snapshot", lambda: _snapshot(401, active=True))
    measured = tracker.sample_service_runtime(path, now_monotonic=401)
    assert measured["baseline_state"] == "reset_oversized"
    assert measured["service_count"] == 1
    assert set(json.loads(path.read_text())["sample"]["units"]) == {"stockagent-job.service"}


@pytest.mark.parametrize("boot", [None, "same-boot"])
def test_monotonic_rollback_discards_all_counter_deltas_even_with_matching_boot(tmp_path, monkeypatch, boot):
    path = tmp_path / "baseline.json"
    monkeypatch.setattr(tracker, "_boot_id", lambda: boot)
    snapshots = iter([_snapshot(401, active=True), _snapshot(100, active=True)])
    monkeypatch.setattr(benchmark, "service_snapshot", lambda: next(snapshots))
    tracker.sample_service_runtime(path, now_monotonic=401)
    measured = tracker.sample_service_runtime(path, now_monotonic=100)
    assert measured["baseline_state"] == "reset_clock_rollback"
    assert measured["interval_seconds"] is None
    assert measured["services"][0]["read_bytes_delta"] is None
    assert measured["services"][0]["write_bytes_delta"] is None


@pytest.mark.parametrize("stamp", [True, -1, float("nan"), float("inf")])
def test_invalid_sampling_clock_leaves_existing_evidence_unchanged(tmp_path, monkeypatch, stamp):
    path = tmp_path / "baseline.json"
    path.write_text("old evidence")
    monkeypatch.setattr(benchmark, "service_snapshot", lambda: pytest.fail("no systemd query"))
    with pytest.raises(ValueError, match="sampling clock"):
        tracker.sample_service_runtime(path, now_monotonic=stamp)
    assert path.read_text() == "old evidence"


@pytest.mark.parametrize("bad", [
    None, [], {"monotonic": float("nan"), "units": {}},
    {"monotonic": 500, "units": {"service": []}},
    {"monotonic": 500, "units": {}, "query_ms": "bad"},
    {"monotonic": 500, "units": {}, "query_ms": float("inf")},
    {"monotonic": 500, "units": {}, "query_ms": -1},
    {"monotonic": 500, "units": {}, "query_ms": True},
])
def test_invalid_current_snapshot_does_not_replace_baseline(tmp_path, monkeypatch, bad):
    path = tmp_path / "baseline.json"
    path.write_text("previous evidence")
    monkeypatch.setattr(benchmark, "service_snapshot", lambda: bad)
    with pytest.raises(ValueError, match="current service snapshot"):
        tracker.sample_service_runtime(path, now_monotonic=500)
    assert path.read_text() == "previous evidence"


def test_current_baseline_serialization_failure_is_atomic_without_inventory_truncation(tmp_path, monkeypatch):
    path = tmp_path / "baseline.json"
    path.write_text("previous evidence")
    current = _snapshot(500, active=True)
    current["oversized"] = "x" * tracker.BASELINE_MAX_BYTES
    monkeypatch.setattr(benchmark, "service_snapshot", lambda: current)
    with pytest.raises(ValueError, match="no services truncated"):
        tracker.sample_service_runtime(path, now_monotonic=500)
    assert path.read_text() == "previous evidence"
    assert list(tmp_path.iterdir()) == [path]


def test_negative_baseline_counter_is_not_a_positive_usage_measurement():
    assert tracker._counter_delta({"cpu": -1}, {"cpu": 10}, "cpu", same_process=True) is None


def test_deactivating_service_is_not_terminal(tmp_path, monkeypatch):
    current = _snapshot(500)
    current["units"]["stockagent-job.service"]["ActiveState"] = "deactivating"
    monkeypatch.setattr(benchmark, "service_snapshot", lambda: current)
    measured = tracker.sample_service_runtime(tmp_path / "baseline.json", now_monotonic=500)
    row = measured["services"][0]
    assert row["last_completed_process_wall_seconds"] is None
    assert row["exit_status"] is None


def test_snapshot_clock_rollback_after_admission_drops_volume_and_unit_deltas(tmp_path, monkeypatch):
    path = tmp_path / "baseline.json"
    monkeypatch.setattr(tracker, "_boot_id", lambda: "same-boot")
    snapshots = iter([_snapshot(100, active=True), _snapshot(99, active=True)])
    monkeypatch.setattr(benchmark, "service_snapshot", lambda: next(snapshots))
    spaces = iter([
        {"device_id": 7, "total_bytes": 1000, "available_bytes": 900},
        {"device_id": 7, "total_bytes": 1000, "available_bytes": 100},
    ])
    monkeypatch.setattr(tracker, "_filesystem_space", lambda: next(spaces))
    tracker.sample_service_runtime(path, now_monotonic=100)
    measured = tracker.sample_service_runtime(path, now_monotonic=401)
    assert measured["baseline_state"] == "reset_clock_rollback"
    assert measured["interval_seconds"] is None
    assert measured["filesystem"]["available_delta_bytes"] is None
    assert measured["services"][0]["read_bytes_delta"] is None


def test_negative_baseline_free_space_is_not_a_volume_gain(tmp_path, monkeypatch):
    path = tmp_path / "baseline.json"
    monkeypatch.setattr(tracker, "_boot_id", lambda: "same-boot")
    snapshots = iter([_snapshot(100), _snapshot(401)])
    monkeypatch.setattr(benchmark, "service_snapshot", lambda: next(snapshots))
    monkeypatch.setattr(tracker, "_filesystem_space", lambda: {
        "device_id": 7, "total_bytes": 1000, "available_bytes": 500,
    })
    tracker.sample_service_runtime(path, now_monotonic=100)
    baseline = json.loads(path.read_text())
    baseline["filesystem"]["available_bytes"] = -1
    path.write_text(json.dumps(baseline))
    measured = tracker.sample_service_runtime(path, now_monotonic=401)
    assert measured["filesystem"]["available_delta_bytes"] is None


def test_reserved_space_consumption_remains_visible_when_available_is_zero(tmp_path, monkeypatch):
    path = tmp_path / "baseline.json"
    monkeypatch.setattr(tracker, "_boot_id", lambda: "same-boot")
    snapshots = iter([_snapshot(100), _snapshot(401)])
    monkeypatch.setattr(benchmark, "service_snapshot", lambda: next(snapshots))
    spaces = iter([
        {"device_id": 7, "total_bytes": 1000, "available_bytes": 0, "free_bytes_including_reserved": 100, "used_bytes": 900, "reserved_or_unavailable_free_bytes": 100},
        {"device_id": 7, "total_bytes": 1000, "available_bytes": 0, "free_bytes_including_reserved": 40, "used_bytes": 960, "reserved_or_unavailable_free_bytes": 40},
    ])
    monkeypatch.setattr(tracker, "_filesystem_space", lambda: next(spaces))
    first = tracker.sample_service_runtime(path, now_monotonic=100)
    assert first["filesystem"]["free_delta_bytes_including_reserved"] is None
    measured = tracker.sample_service_runtime(path, now_monotonic=401)
    assert measured["filesystem"]["available_delta_bytes"] == 0
    assert measured["filesystem"]["free_delta_bytes_including_reserved"] == -60
    assert measured["filesystem"]["used_delta_bytes"] == 60
    assert measured["filesystem"]["available_bytes"] == 0
    assert "do not change admission floors" in measured["boundary"]


def test_existing_baseline_without_reserved_space_counter_remains_unknown(tmp_path, monkeypatch):
    path = tmp_path / "baseline.json"
    monkeypatch.setattr(tracker, "_boot_id", lambda: "same-boot")
    snapshots = iter([_snapshot(100), _snapshot(401)])
    monkeypatch.setattr(benchmark, "service_snapshot", lambda: next(snapshots))
    spaces = iter([
        {"device_id": 7, "total_bytes": 1000, "available_bytes": 900},
        {"device_id": 7, "total_bytes": 1000, "available_bytes": 600, "free_bytes_including_reserved": 700},
    ])
    monkeypatch.setattr(tracker, "_filesystem_space", lambda: next(spaces))
    tracker.sample_service_runtime(path, now_monotonic=100)
    measured = tracker.sample_service_runtime(path, now_monotonic=401)
    assert measured["filesystem"]["available_delta_bytes"] == -300
    assert measured["filesystem"]["free_delta_bytes_including_reserved"] is None
    assert measured["filesystem"]["used_delta_bytes"] is None


@pytest.mark.parametrize("change", [
    {"device_id": 8}, {"device_id": None}, {"device_id": True}, {"total_bytes": 2000},
    {"total_bytes": None}, {"free_bytes_including_reserved": -1},
    {"free_bytes_including_reserved": 1001}, {"free_bytes_including_reserved": True},
    {"free_bytes_including_reserved": 100.0}, {"free_bytes_including_reserved": None},
])
def test_reserved_space_counter_rejects_invalid_geometry_or_optional_value(change):
    before = {"device_id": 7, "total_bytes": 1000, "free_bytes_including_reserved": 100}
    after = {**before, **change}
    assert tracker._filesystem_counter_delta(before, after, "free_bytes_including_reserved") is None
    assert tracker._filesystem_counter_delta(after, before, "free_bytes_including_reserved") is None


def test_filesystem_snapshot_reports_reserve_without_an_extra_walk_or_statvfs(tmp_path, monkeypatch):
    from types import SimpleNamespace
    calls = []

    def statvfs(path):
        calls.append(path)
        return SimpleNamespace(f_frsize=4096, f_bsize=4096, f_blocks=1000, f_bfree=100, f_bavail=0)

    monkeypatch.setattr(tracker.os, "statvfs", statvfs)
    measured = tracker._filesystem_space(tmp_path)
    assert calls == [tmp_path]
    assert measured["available_bytes"] == 0
    assert measured["free_bytes_including_reserved"] == 409600
    assert measured["used_bytes"] == 3686400
    assert measured["reserved_or_unavailable_free_bytes"] == 409600


def test_missing_query_timing_is_unknown_not_zero(tmp_path, monkeypatch):
    current = _snapshot(100)
    current.pop("query_ms")
    monkeypatch.setattr(benchmark, "service_snapshot", lambda: current)
    measured = tracker.sample_service_runtime(tmp_path / "baseline.json", now_monotonic=100)
    assert measured["query_ms"] is None
