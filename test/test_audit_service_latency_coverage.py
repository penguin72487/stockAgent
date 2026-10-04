from __future__ import annotations

from datetime import UTC, datetime, timedelta
import io
from pathlib import Path
import json
import subprocess

import pytest

from scripts import audit_service_latency_coverage as audit


def test_public_ipv6_external_evidence_never_promotes_stale_or_reused_probe(
    tmp_path: Path,
) -> None:
    receipt_path = tmp_path / "ipv6.json"
    completed = datetime(2026, 9, 26, 0, tzinfo=UTC)
    receipt = {
        "hostname": audit.PUBLIC_IPV6_HOSTNAME,
        "completed_at_utc": completed.isoformat(),
        "state": "passed",
        "passed": True,
        "dns": {"aaaa": ["2001:db8::10"]},
        "external_probe": {
            "probe_request_issued": True,
            "ipv6": {"done": True, "success": True},
        },
    }

    def inspect(*, minutes: int = 1) -> dict[str, object]:
        receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
        return audit._public_ipv6_external_evidence(
            receipt_path,
            now=completed + timedelta(minutes=minutes),
            resolve_ipv6=lambda _host: ["2001:db8::10"],
        )

    assert inspect()["state"] == "external_pass_observed"
    assert inspect(minutes=61)["state"] == "stale_receipt"
    receipt["external_probe"]["probe_request_issued"] = False
    assert inspect()["state"] == "probe_age_unproven"
    receipt["external_probe"]["probe_request_issued"] = True
    receipt["dns"]["aaaa"] = ["2001:db8::20"]
    assert inspect()["state"] == "dns_changed_since_probe"
    receipt["state"] = "inconclusive_timeout"
    assert inspect()["state"] == "external_probe_inconclusive"
    receipt["state"] = "failed"
    receipt["dns"]["aaaa"] = ["2001:db8::10"]
    receipt["external_probe"]["ipv6"]["done"] = False
    assert inspect()["state"] == "external_probe_inconclusive"


def test_public_ipv6_external_evidence_missing_and_malformed_are_not_green(
    tmp_path: Path,
) -> None:
    receipt_path = tmp_path / "missing.json"
    assert audit._public_ipv6_external_evidence(receipt_path)["state"] == (
        "missing_receipt"
    )
    receipt_path.write_text("{", encoding="utf-8")
    assert audit._public_ipv6_external_evidence(receipt_path)["state"] == (
        "invalid_receipt"
    )


def test_gateway_recovery_is_measured_without_claiming_cold_boot(tmp_path: Path) -> None:
    log = tmp_path / "startup.log"
    log.write_text(
        "2026-09-24T08:49:30+08:00 gateway backend healthy=False uri=/healthz\n"
        "2026-09-24T08:49:31+08:00 WSL gateway start dispatched pid=1 reason=backend_unhealthy\n"
        "2026-09-24T08:50:00+08:00 WSL gateway start dispatched pid=2 reason=backend_unhealthy\n"
        "2026-09-24T08:54:04+08:00 gateway backend healthy=True uri=/healthz\n"
        "invalid line\n"
        "2026-09-24T11:43:00+08:00 gateway backend healthy=False uri=/healthz\n"
        "2026-09-24T11:43:06+08:00 gateway backend healthy=True uri=/healthz\n"
        "2026-09-24T12:00:00+08:00 gateway backend healthy=False uri=/healthz\n"
    )

    result = audit._gateway_recovery_episodes(
        log, now=datetime(2026, 9, 24, 12, tzinfo=UTC),
        current_userspace_at_utc=datetime(2026, 9, 24, 0, 53, 51, tzinfo=UTC),
        current_vm_evidence={
            "available": True,
            "vm_network_driver_loaded_at_utc": "2026-09-24T00:53:49+00:00",
            "wsl_host_first_at_utc": "2026-09-24T00:53:51.480481+00:00",
        },
    )

    assert result["available"] is True
    assert result["completed_7d_count"] == 2
    assert result["last_episode"]["recovery_seconds"] == 6.0
    assert result["last_episode"]["wsl_dispatch_count"] == 0
    assert result["slowest_7d_episode"]["recovery_seconds"] == 274.0
    assert result["slowest_7d_episode"]["dispatch_to_healthy_seconds"] == 273.0
    assert result["slowest_7d_episode"]["dispatch_to_userspace_seconds"] == 260.0
    assert result["slowest_7d_episode"]["userspace_to_healthy_seconds"] == 13.0
    assert result["slowest_7d_episode"]["dispatch_to_vm_driver_seconds"] == 258.0
    assert result["slowest_7d_episode"]["vm_driver_to_userspace_seconds"] == 2.0
    assert "dispatch_to_userspace_seconds" not in result["last_episode"]
    assert "dispatch_to_vm_driver_seconds" not in result["last_episode"]
    assert result["slowest_7d_episode"]["wsl_dispatch_count"] == 2
    assert result["ongoing_since"] == "2026-09-24T04:00:00+00:00"

    invalid = audit._gateway_recovery_episodes(
        log, now=datetime(2026, 9, 24, 12, tzinfo=UTC),
        current_userspace_at_utc=datetime(2026, 9, 24, 0, 53, 51, tzinfo=UTC),
        current_vm_evidence={
            "available": True,
            "vm_network_driver_loaded_at_utc": "2026-09-24T00:54:30+00:00",
            "wsl_host_first_at_utc": "2026-09-24T00:54:31+00:00",
        },
    )
    assert "dispatch_to_vm_driver_seconds" not in invalid["slowest_7d_episode"]


def test_gateway_recovery_missing_log_is_unknown(tmp_path: Path) -> None:
    result = audit._gateway_recovery_episodes(tmp_path / "missing.log")
    assert result["available"] is False
    assert result["last_episode"] is None


def test_gateway_audit_keeps_wsl_command_completion_separate_from_readiness(
    tmp_path: Path,
) -> None:
    log = tmp_path / "startup.log"
    log.write_text(
        "2026-09-25T02:45:54+08:00 WSL gateway start dispatched pid=1 reason=supervisor_start\n"
        "2026-09-25T02:45:55+08:00 gateway backend healthy=True uri=/healthz\n"
        "2026-09-25T02:46:00+08:00 WSL gateway dispatch completed pid=1 "
        "verb=start reason=supervisor_start exit_code=0 "
        "elapsed_seconds=0.186 observed_lag_seconds=5.532\n",
        encoding="utf-8",
    )
    result = audit._gateway_recovery_episodes(
        log, now=datetime(2026, 9, 25, 0, tzinfo=UTC)
    )
    assert result["wsl_dispatch_completions_7d"] == 1
    assert result["latest_wsl_dispatch_completion"] == {
        "observed_at_utc": "2026-09-24T18:46:00+00:00",
        "exit_code": 0,
        "elapsed_seconds": 0.186,
        "verb": "start",
        "reason": "supervisor_start",
        "boundary": "wsl.exe command duration, not WSL VM boot or gateway readiness",
    }
    assert result["last_episode"] is None


def test_wsl_userspace_marker_is_utc_and_missing_is_unknown(monkeypatch) -> None:
    def good(command, **kwargs):
        assert command[:3] == ["systemctl", "show", "--property=UserspaceTimestamp"]
        assert kwargs["env"]["TZ"] == "UTC"
        return subprocess.CompletedProcess(
            command, 0, stdout="Thu 2026-09-24 00:53:51 UTC\n"
        )

    monkeypatch.setattr(audit.subprocess, "run", good)
    assert audit._wsl_userspace_at_utc() == datetime(2026, 9, 24, 0, 53, 51, tzinfo=UTC)

    monkeypatch.setattr(
        audit.subprocess, "run",
        lambda command, **kwargs: subprocess.CompletedProcess(command, 0, stdout="n/a"),
    )
    assert audit._wsl_userspace_at_utc() is None


def test_windows_caddy_process_evidence_has_no_commandline(monkeypatch) -> None:
    def run(command, **_kwargs):
        assert command[-1] == audit.WINDOWS_CADDY_PROCESS_QUERY
        return subprocess.CompletedProcess(command, 0, stdout=(
            '[{"pid":5060,"parent_pid":12128,"parent_name":"powershell.exe",'
            '"started":"2026-09-15 00:00:00"}]'
        ))

    monkeypatch.setattr(audit.subprocess, "run", run)
    rows = audit._windows_caddy_processes()
    assert rows == [{
        "pid": 5060, "parent_pid": 12128,
        "parent_name": "powershell.exe", "started": "2026-09-15 00:00:00",
    }]
    assert "CommandLine" not in audit.WINDOWS_CADDY_PROCESS_QUERY


def test_windows_wsl_vm_evidence_is_bounded_and_omits_process_commandline(monkeypatch) -> None:
    def run(command, **_kwargs):
        assert command[-1] == audit.WINDOWS_WSL_VM_QUERY
        return subprocess.CompletedProcess(command, 0, stdout=json.dumps({
            "available": True,
            "wsl_host_first_at_utc": "2026-09-24T00:53:51Z",
            "vm_network_driver_loaded_at_utc": "2026-09-24T00:53:49Z",
            "vm_driver_event_count": 1,
            "process_count": 2,
        }))

    monkeypatch.setattr(audit.subprocess, "run", run)
    result = audit._windows_wsl_vm_evidence()
    assert result is not None
    assert result["vm_driver_event_count"] == 1
    assert "commandline" not in json.dumps(result).lower()
    assert "vm_id" not in result


def test_repository_process_inventory_finds_unmanaged_without_exposing_argv(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "stockAgent"
    repo.mkdir()
    proc = tmp_path / "proc"
    proc.mkdir()

    def process(pid: int, *, cwd: Path, command: bytes, cgroup: str) -> None:
        directory = proc / str(pid)
        directory.mkdir()
        (directory / "cwd").symlink_to(cwd, target_is_directory=True)
        (directory / "cmdline").write_bytes(command)
        (directory / "status").write_text("Name:\ttest\nPPid:\t1\n")
        (directory / "comm").write_text("python\n")
        (directory / "cgroup").write_text(cgroup)
        (directory / "statm").write_text("100 5 0 0 0 0 0\n")

    process(
        101, cwd=repo, command=b"python\0--token=private\0",
        cgroup="0::/system.slice/stockagent-public-dashboards.service\n",
    )
    process(
        102, cwd=tmp_path, command=f"python\0{repo}/scripts/job.py\0secret\0".encode(),
        cgroup="0::/init.scope\n",
    )
    process(103, cwd=tmp_path, command=b"python\0other.py\0", cgroup="0::/init.scope\n")
    process(104, cwd=repo, command=b"python\0audit.py\0", cgroup="0::/init.scope\n")
    result = audit.repository_process_snapshot(repo, proc, exclude_pids={104})
    assert result["inspected_proc_count"] == 4
    assert result["matched_count"] == 2
    assert result["managed_unit_process_count"] == 1
    assert result["unmanaged_candidate_count"] == 1
    assert [row["pid"] for row in result["processes"]] == [101, 102]
    assert result["processes"][0]["stockagent_unit"] == "stockagent-public-dashboards.service"
    assert result["processes"][1]["match_basis"] == "argv_path"
    assert "private" not in json.dumps(result)
    assert "secret" not in json.dumps(result)


def test_repository_process_snapshot_fences_identity_and_io(tmp_path, monkeypatch):
    repo, proc = tmp_path / "stockAgent", tmp_path / "proc"
    repo.mkdir()
    directory = proc / "101"
    directory.mkdir(parents=True)
    (directory / "cwd").symlink_to(repo, target_is_directory=True)
    (directory / "status").write_text("PPid:\t1\n")
    (directory / "comm").write_text("python\n")
    (directory / "cgroup").write_text("0::/init.scope\n")
    (directory / "statm").write_text("100 5\n")
    # Field 22 follows 19 earlier fields after comm, whose name has nested ')'.
    (directory / "stat").write_text("101 (a (b) c)) S " + "0 " * 18 + "700 0\n")
    (directory / "io").write_text(
        "read_bytes: 10\nwrite_bytes: 30\ncancelled_write_bytes: 2\nwchar: 500\n"
    )
    row = audit.repository_process_snapshot(repo, proc, exclude_pids=set())["processes"][0]
    assert row["start_time_ticks"] == 700
    assert row["io_counters"] == {"read_bytes": 10, "write_bytes": 30, "cancelled_write_bytes": 2}
    assert "wchar" not in row["io_counters"]
    identity_reads = iter([700, 701])
    monkeypatch.setattr(audit, "_proc_start_ticks", lambda _: next(identity_reads))
    raced = audit.repository_process_snapshot(repo, proc, exclude_pids=set())["processes"][0]
    assert raced["start_time_ticks"] is None
    assert set(raced["io_counters"].values()) == {None}


def test_repository_process_visibility_counts_do_not_imply_full_host_inventory(tmp_path):
    repo, proc = tmp_path / "stockAgent", tmp_path / "proc"
    repo.mkdir()
    unreadable = proc / "101"
    unreadable.mkdir(parents=True)
    # Missing context can mean exit or permissions; do not invent an owner.
    result = audit.repository_process_snapshot(repo, proc, exclude_pids=set())
    assert result["inspected_proc_count"] == 1
    assert result["matched_count"] == 0
    assert result["visibility"]["cwd_read_failures"] == 1
    assert result["visibility"]["argv_read_failures"] == 1
    assert "not a fully readable" in result["scope"]


def test_process_counter_permissions_and_malformed_values_do_not_fake_zero(tmp_path):
    entry = tmp_path / "42"
    entry.mkdir()
    assert audit._proc_start_ticks(entry) is None
    assert set(audit._proc_io_counters(entry).values()) == {None}
    for content in ["42 missing-parens " + "1 " * 25, "42 (test) S 1\n"]:
        (entry / "stat").write_text(content)
        assert audit._proc_start_ticks(entry) is None
    (entry / "io").write_text("read_bytes: -1\nwrite_bytes: bad\ncancelled_write_bytes: 7\n")
    assert audit._proc_io_counters(entry) == {
        "read_bytes": None, "write_bytes": None, "cancelled_write_bytes": 7,
    }
    assert audit.repository_process_snapshot(tmp_path, tmp_path / "absent", exclude_pids=set())["inspection_state"] == "unavailable"


def _process_io_row(pid=42, *, start=700, write=10, unit=None):
    return {"pid": pid, "ppid": 1, "comm": "python", "start_time_ticks": start,
            "stockagent_unit": unit,
            "io_counters": {"read_bytes": 0, "write_bytes": write, "cancelled_write_bytes": 0}}


def test_process_io_deltas_keep_unmanaged_and_reaped_parent_counters_non_additive():
    before = {"monotonic": 100, "processes": [_process_io_row(), _process_io_row(43)]}
    after = {"monotonic": 102, "processes": [_process_io_row(write=110), _process_io_row(43, write=210)]}
    result = audit.repository_process_io_deltas(before, after, same_host_epoch=True)
    assert result["sample_seconds"] == 2
    assert [row["write_bytes_delta"] for row in result["processes"]] == [100, 200]
    assert all(row["measurement_state"] == "measured" for row in result["processes"])
    assert "not additive" in result["scope"]
    assert "total_write_bytes" not in result


@pytest.mark.parametrize("change,state", [
    ({"start_time_ticks": 701}, "pid_reused"),
    ({"start_time_ticks": None}, "process_identity_unavailable"),
    ({"start_time_ticks": True}, "process_identity_unavailable"),
    ({"start_time_ticks": -1}, "process_identity_unavailable"),
    ({"stockagent_unit": "stockagent-other.service"}, "unit_context_changed"),
    ({"io_counters": {"write_bytes": 9}}, "counter_decreased"),
    ({"io_counters": None}, "counters_unavailable"),
])
def test_process_io_deltas_reject_identity_counter_and_context_breaks(change, state):
    before = {"monotonic": 100, "processes": [_process_io_row()]}
    after = {"monotonic": 102, "processes": [{**_process_io_row(write=110), **change}]}
    row = audit.repository_process_io_deltas(before, after, same_host_epoch=True)["processes"][0]
    assert row["measurement_state"] == state
    assert row["write_bytes_delta"] is None


@pytest.mark.parametrize("clock", [100, 99, float("nan"), True, None, 10**1000])
def test_process_io_invalid_clock_has_no_counter_delta(clock):
    before = {"monotonic": 100, "processes": [_process_io_row()]}
    after = {"monotonic": clock, "processes": [_process_io_row(write=110)]}
    result = audit.repository_process_io_deltas(before, after, same_host_epoch=True)
    assert result["sample_seconds"] is None
    assert result["processes"][0]["measurement_state"] == "sampling_clock_invalid"


def test_process_io_epochs_missing_processes_and_unavailable_snapshots_are_explicit():
    before = {"monotonic": 100, "processes": [_process_io_row(), _process_io_row(43)]}
    after = {"monotonic": 102, "processes": [_process_io_row(write=110), _process_io_row(44)]}
    result = audit.repository_process_io_deltas(before, after, same_host_epoch=False)
    assert [row["measurement_state"] for row in result["processes"]] == [
        "host_epoch_unproven", "left_observed_context", "entered_observed_context",
    ]
    assert all(row["write_bytes_delta"] is None for row in result["processes"])
    unavailable = {"monotonic": 102, "processes": [], "inspection_state": "unavailable"}
    result = audit.repository_process_io_deltas(before, unavailable, same_host_epoch=True)
    assert result["snapshots_available"] is False
    assert all(row["measurement_state"] == "snapshot_unavailable" for row in result["processes"])
    assert audit.repository_process_io_deltas(
        {"processes": None}, {"processes": "malformed"}, same_host_epoch=False,
    )["processes"] == []


def test_auxiliary_schedules_find_cron_and_user_units_without_commands(
    tmp_path: Path,
) -> None:
    cron = tmp_path / "cron.d"
    cron.mkdir()
    (cron / "project").write_text(
        "# 0 0 * * * root /root/stockAgent/secret=comment-only\n"
        "0 4 * * * root /root/stockAgent/scripts/job.sh --secret=private\n"
    )
    (cron / "other").write_text("0 5 * * * root /usr/bin/true\n")

    def run(command, **_kwargs):
        if command == ["crontab", "-l"]:
            return subprocess.CompletedProcess(
                command, 0, stdout="@reboot /root/stockAgent/start.sh token=hidden\n"
            )
        assert command[:3] == ["systemctl", "--user", "list-unit-files"]
        return subprocess.CompletedProcess(
            command, 0, stdout="stockagent-helper.service enabled -\nother.timer enabled -\n"
        )

    result = audit.auxiliary_schedule_snapshot((cron,), runner=run)
    assert result["cron"]["files_scanned"] == 2
    assert result["cron"]["matching_files"] == [{
        "path": str(cron / "project"),
        "active_reference_lines": 1,
        "name_match": False,
    }]
    assert result["cron"]["root_crontab"] == {
        "state": "read", "active_reference_lines": 1,
    }
    assert result["root_user_systemd"]["matching_units"] == [
        "stockagent-helper.service"
    ]
    serialized = json.dumps(result)
    assert "private" not in serialized
    assert "hidden" not in serialized
    assert "comment-only" not in serialized


def test_windows_task_discovery_includes_action_reference_without_arguments() -> None:
    assert "$_.Arguments -match '(?i)stockagent'" in audit.WINDOWS_TASK_QUERY
    assert "match_basis" in audit.WINDOWS_TASK_QUERY
    assert "action_arguments" not in audit.WINDOWS_TASK_QUERY
    assert "multiple_instances_policy" in audit.WINDOWS_TASK_QUERY
    assert "repetition_intervals" in audit.WINDOWS_TASK_QUERY


def test_running_windows_supervisor_nonzero_trigger_is_not_called_process_failure() -> None:
    task = {
        "state": "Running", "last_task_result": 0x800710E0,
        "multiple_instances_policy": "IgnoreNew",
        "repetition_intervals": ["PT1M"],
    }
    assert audit._windows_task_result_context(task) == (
        "ambiguous_nonzero_with_ignore_new_repetition"
    )
    assert audit._windows_task_result_context({**task, "repetition_intervals": []}) == (
        "unresolved_nonzero_while_running"
    )
    assert audit._windows_task_result_context({**task, "state": "Ready"}) is None


def test_unit_blocks_keep_only_installed_stockagent_unit_kind() -> None:
    payload = (
        "Id=stockagent-a.timer\nActiveState=active\n"
        "TimersMonotonic={ OnUnitInactiveUSec=1min ; next_elapse=0 }\n"
        "TimersMonotonic={ OnBootUSec=2min ; next_elapse=0 }\n\n"
        "Id=stockagent-b.path\nActiveState=active\n\n"
        "Id=other.timer\nActiveState=active\n"
    )
    assert list(audit._unit_blocks(payload, suffix=".timer")) == ["stockagent-a.timer"]
    assert list(audit._unit_blocks(payload, suffix=".path")) == ["stockagent-b.path"]
    assert "OnUnitInactiveUSec" in audit._unit_blocks(payload, suffix=".timer")[
        "stockagent-a.timer"
    ]["TimersMonotonic"]


def test_recurring_timer_without_next_trigger_is_visible_not_green() -> None:
    base = {
        "UnitFileState": "enabled",
        "ActiveState": "active",
        "SubState": "elapsed",
        "TimersMonotonic": "{ OnUnitInactiveUSec=1min ; next_elapse=0 }",
        "NextElapseUSecRealtime": "",
        "NextElapseUSecMonotonic": "infinity",
    }
    timers = {
        "stockagent-stalled.timer": {**base, "Unit": "stockagent-stalled.service"},
        "stockagent-working.timer": {
            **base,
            "Unit": "stockagent-working.service",
            "NextElapseUSecMonotonic": "Fri 2026-09-25 12:00:00 CST",
        },
        "stockagent-running.timer": {**base, "Unit": "stockagent-running.service"},
        "stockagent-live-cycle.timer": {
            **base,
            "Unit": "stockagent-live-cycle.service",
            "SubState": "running",
        },
        "stockagent-one-shot.timer": {
            **base,
            "Unit": "stockagent-one-shot.service",
            "TimersMonotonic": "{ OnBootUSec=2min ; next_elapse=0 }",
        },
    }
    services = [
        {"unit": "stockagent-stalled.service", "active_state": "inactive"},
        {"unit": "stockagent-working.service", "active_state": "inactive"},
        {"unit": "stockagent-running.service", "active_state": "active"},
        {"unit": "stockagent-live-cycle.service", "active_state": "inactive"},
        {"unit": "stockagent-one-shot.service", "active_state": "inactive"},
    ]
    assert audit.timer_schedule_findings(timers, services) == [
        {
            "unit": "stockagent-stalled.timer",
            "target": "stockagent-stalled.service",
            "target_state": "inactive",
            "finding": "recurring_timer_without_next_trigger",
        }
    ]


def test_pressure_snapshot_keeps_host_contention_separate_from_unit_cpu(
    tmp_path: Path,
) -> None:
    (tmp_path / "cpu").write_text(
        "some avg10=12.50 avg60=2.00 avg300=1.00 total=123\n", encoding="utf-8"
    )
    (tmp_path / "memory").write_text(
        "some avg10=8.00 avg60=4.00 avg300=2.00 total=456\n"
        "full avg10=7.00 avg60=3.00 avg300=1.00 total=123\n",
        encoding="utf-8",
    )
    snapshot = audit.pressure_snapshot(tmp_path)
    assert snapshot["cpu"]["some"]["avg10"] == 12.5
    assert snapshot["memory"]["full"]["avg60"] == 3.0
    assert snapshot["io"] is None


def test_business_receipt_requires_matching_systemd_attempt(
    tmp_path: Path, monkeypatch
) -> None:
    receipt = tmp_path / "refresh_receipt.json"
    monkeypatch.setattr(
        audit,
        "BUSINESS_RECEIPTS",
        {"stockagent-crypto-training-refresh.service": receipt},
    )
    snapshot = {"observed_at": "2026-09-23T20:00:00+00:00", "monotonic": 500.0}
    props = {"ExecMainStartTimestampMonotonic": 440_000_000}
    receipt.write_text(json.dumps({
        "started_at_utc": "2026-09-23T19:59:00+00:00",
        "finished_at_utc": "2026-09-23T19:59:45+00:00",
        "state": "deferred_source_changed",
    }))

    current = audit._business_receipt(
        "stockagent-crypto-training-refresh.service", props, snapshot
    )
    assert current is not None
    assert current["matches_last_attempt"] is True
    assert current["state"] == "deferred_source_changed"

    receipt.write_text(json.dumps({
        "started_at_utc": "2026-09-23T18:00:00+00:00",
        "state": "completed",
    }))
    stale = audit._business_receipt(
        "stockagent-crypto-training-refresh.service", props, snapshot
    )
    assert stale is not None
    assert stale["matches_last_attempt"] is False
    assert "state" not in stale


@pytest.fixture
def crypto_refresh_receipt() -> dict[str, object]:
    commands = [
        ["downloader/download_bybit_funding_history.py"],
        ["downloader/materialize_bybit_perpetual_daily.py"],
        ["scripts/build_bybit_venue_daily_features.py"],
        ["scripts/report_crypto_training_features.py"],
        ["scripts/publish_data_releases.py", "publish", "bybit"],
        ["scripts/audit_crypto_historical_coverage.py"],
        ["scripts/report_crypto_venue_1m_features.py", "okx"],
        ["scripts/report_crypto_venue_1m_features.py", "binance"],
    ]
    return {
        "refresh_contract_version": 4,
        "started_at_utc": "2026-09-23T19:59:00+00:00",
        "finished_at_utc": "2026-09-23T20:02:00+00:00",
        "state": "completed_with_report_deferrals",
        "core_state": "published",
        "publication_state": "published",
        "reports": [
            {"name": "coverage", "state": "deferred_source_changed", "command": "SECRET"},
            {"name": "okx", "state": "completed", "credential": "SECRET"},
            {"name": "binance", "state": "completed"},
        ],
        "step_attempts": [
            {
                "command": ["/private/python", *command, "--credential", "SECRET"],
                "status": "process_completed",
                "started_at_utc": "2026-09-23T19:59:01+00:00",
                "elapsed_seconds": 107.5 if index == 4 else index,
                "credential": "SECRET",
            }
            for index, command in enumerate(commands)
        ],
    }


def test_crypto_v4_business_receipt_separates_core_reports_and_process_timing(
    tmp_path: Path, monkeypatch, crypto_refresh_receipt,
) -> None:
    receipt = tmp_path / "refresh.json"
    unit = "stockagent-crypto-training-refresh.service"
    monkeypatch.setattr(audit, "BUSINESS_RECEIPTS", {unit: receipt})
    receipt.write_text(json.dumps(crypto_refresh_receipt))
    snapshot = {"observed_at": "2026-09-23T20:03:00+00:00", "monotonic": 500.0}
    props = {"ExecMainStartTimestampMonotonic": 260_000_000}

    current = audit._business_receipt(unit, props, snapshot)
    assert current["core_state"] == current["publication_state"] == "published"
    assert current["state"] == "completed_with_report_deferrals"
    assert current["reports"] == [
        {"name": "coverage", "state": "deferred_source_changed"},
        {"name": "okx", "state": "completed"},
        {"name": "binance", "state": "completed"},
    ]
    assert [step["step"] for step in current["step_receipts"]] == [
        "bybit_funding_history", "bybit_daily_materialization", "bybit_venue_features",
        "bybit_training_report", "bybit_cold_publish", "crypto_coverage_report",
        "okx_feature_report", "binance_feature_report",
    ]
    assert audit._matched_step_timing_summary(current) == {
        "scope": "recorded_steps_only", "recorded_step_count": 8,
        "slowest_step": "bybit_cold_publish", "slowest_step_seconds": 107.5,
    }
    assert audit.business_receipt_findings([{"unit": unit, "business_receipt": current}])[0]["receipt_state"] == (
        "completed_with_report_deferrals"
    )
    for secret in ("SECRET", "command", "credential", "/private/python"):
        assert secret not in json.dumps(current)

    # Same completed receipt cannot supply current health or timing to a later run.
    props["ExecMainStartTimestampMonotonic"] += 60_000_000
    stale = audit._business_receipt(unit, props, snapshot)
    assert stale["matches_last_attempt"] is False
    assert "core_state" not in stale and "reports" not in stale
    assert audit._matched_step_timing_summary(stale) is None


@pytest.mark.parametrize("version", [3, "4", True, None])
def test_crypto_projection_requires_typed_v4_contract(version, crypto_refresh_receipt) -> None:
    crypto_refresh_receipt["refresh_contract_version"] = version
    assert audit._crypto_refresh_details(
        crypto_refresh_receipt, datetime(2026, 9, 23, 19, 59, tzinfo=UTC),
    ) == {}


@pytest.mark.parametrize("elapsed", [True, -1, float("nan"), float("inf"), "2", None])
def test_crypto_bad_elapsed_does_not_create_partial_timing(crypto_refresh_receipt, elapsed) -> None:
    crypto_refresh_receipt["step_attempts"][3]["elapsed_seconds"] = elapsed
    details = audit._crypto_refresh_details(
        crypto_refresh_receipt, datetime(2026, 9, 23, 19, 59, tzinfo=UTC),
    )
    assert "step_receipts" not in details and "step_receipts_scope" not in details


@pytest.mark.parametrize("field,value", [
    ("command", []), ("command", "SECRET"), ("command", ["python", None]),
    ("command", ["python", "unknown.py"]),
    ("command", ["python", "publish_data_releases.py", "publish", "tw-public"]),
    ("command", ["python", "report_crypto_venue_1m_features.py", "unknown"]),
    ("command", ["python", "report_crypto_venue_1m_features.py"]),
    ("status", "running"), ("status", "unknown"), ("status", {}),
    ("started_at_utc", "2026-09-23T19:58:00+00:00"),
    ("started_at_utc", "2026-09-23T20:03:00+00:00"),
    ("started_at_utc", "2026-09-23T19:59:00"),
    ("started_at_utc", None), ("elapsed_seconds", 999),
])
def test_crypto_invalid_attempt_does_not_create_timing(crypto_refresh_receipt, field, value) -> None:
    crypto_refresh_receipt["step_attempts"][0][field] = value
    details = audit._crypto_refresh_details(
        crypto_refresh_receipt, datetime(2026, 9, 23, 19, 59, tzinfo=UTC),
    )
    assert "step_receipts_scope" not in details


@pytest.mark.parametrize("field,value", [
    ("step_attempts", []), ("step_attempts", None), ("step_attempts", [{}]),
    ("reused_previous_started_at_utc", "2026-09-23T10:00:00Z"),
    ("finished_at_utc", None), ("finished_at_utc", "2026-09-23T19:58:00Z"),
])
def test_crypto_empty_noop_unfinished_runs_do_not_create_timing(crypto_refresh_receipt, field, value) -> None:
    crypto_refresh_receipt[field] = value
    details = audit._crypto_refresh_details(
        crypto_refresh_receipt, datetime(2026, 9, 23, 19, 59, tzinfo=UTC),
    )
    assert "step_receipts_scope" not in details


@pytest.mark.parametrize("status", ["failed", "timed_out"])
def test_crypto_failed_attempt_time_is_not_success(crypto_refresh_receipt, status) -> None:
    crypto_refresh_receipt["step_attempts"][4]["status"] = status
    details = audit._crypto_refresh_details(
        crypto_refresh_receipt, datetime(2026, 9, 23, 19, 59, tzinfo=UTC),
    )
    assert details["step_receipts"][4]["state"] == status
    assert details["step_receipts"][4]["elapsed_seconds"] == 107.5


def test_crypto_unknown_state_is_never_copied_as_safe_state(crypto_refresh_receipt) -> None:
    crypto_refresh_receipt.update(core_state={"SECRET": 1}, publication_state="SECRET")
    crypto_refresh_receipt["reports"][0]["state"] = "SECRET"
    details = audit._crypto_refresh_details(
        crypto_refresh_receipt, datetime(2026, 9, 23, 19, 59, tzinfo=UTC),
    )
    assert details["core_state"] == details["publication_state"] == "unknown"
    assert details["reports"][0]["state"] == "unknown"
    assert "SECRET" not in json.dumps(details)


@pytest.fixture
def crypto_v5_receipt(crypto_refresh_receipt) -> dict[str, object]:
    crypto_refresh_receipt.update(
        refresh_contract_version=5, transport_state="pending",
        state="completed_with_transport_pending", source_lease_elapsed_seconds=102.427,
    )
    crypto_refresh_receipt["step_attempts"].append({
        "command": ["python", "scripts/retry_packed_syncthing_scans.py",
                    "--dataset", "bybit", "--receipt", "/private/transport-unique.json"],
        "status": "timed_out", "elapsed_seconds": 20.0,
        "started_at_utc": "2026-09-23T20:01:00Z",
    })
    return crypto_refresh_receipt


def test_crypto_v5_transport_failure_preserves_core_and_finding(crypto_v5_receipt) -> None:
    details = audit._crypto_refresh_details(
        crypto_v5_receipt, datetime(2026, 9, 23, 19, 59, tzinfo=UTC),
    )
    assert details["core_state"] == details["publication_state"] == "published"
    assert details["transport_state"] == "pending"
    assert details["source_lease_elapsed_seconds"] == 102.427
    assert details["step_receipts"][-1] == {
        "step": "bybit_transport_scan", "state": "timed_out", "elapsed_seconds": 20.0,
    }
    receipt = {**details, "matches_last_attempt": True, "state": crypto_v5_receipt["state"]}
    assert audit._matched_step_timing_summary(receipt)["recorded_step_count"] == 9
    finding = audit.business_receipt_findings([{"unit": "crypto", "business_receipt": receipt}])[0]
    assert finding["receipt_state"] == "completed_with_transport_pending"
    assert "non_complete_state" in finding["reasons"]
    assert "/private" not in json.dumps(details)


@pytest.mark.parametrize("state", [
    "not_attempted", "not_required", "scanning", "pending", "request_acknowledged", "no_pending",
])
def test_crypto_v5_transport_states_are_allowlisted(crypto_v5_receipt, state) -> None:
    crypto_v5_receipt["transport_state"] = state
    details = audit._crypto_refresh_details(crypto_v5_receipt, datetime(2026, 9, 23, 19, 59, tzinfo=UTC))
    assert details["transport_state"] == state


@pytest.mark.parametrize("state", [None, {}, "SECRET"])
def test_crypto_v5_unknown_transport_is_not_leaked(crypto_v5_receipt, state) -> None:
    crypto_v5_receipt["transport_state"] = state
    details = audit._crypto_refresh_details(crypto_v5_receipt, datetime(2026, 9, 23, 19, 59, tzinfo=UTC))
    assert details["transport_state"] == "unknown"
    assert "SECRET" not in json.dumps(details)


@pytest.mark.parametrize("value", [None, True, -1, float("nan"), float("inf"), "2"])
def test_crypto_v5_invalid_lease_measurement_is_omitted(crypto_v5_receipt, value) -> None:
    crypto_v5_receipt["source_lease_elapsed_seconds"] = value
    details = audit._crypto_refresh_details(crypto_v5_receipt, datetime(2026, 9, 23, 19, 59, tzinfo=UTC))
    assert "source_lease_elapsed_seconds" not in details


def test_crypto_v5_noop_accepts_only_fresh_transport_timing(crypto_v5_receipt) -> None:
    crypto_v5_receipt.update(no_op=True, reused_previous_started_at_utc="2026-09-22T10:00:00Z")
    started = datetime(2026, 9, 23, 19, 59, tzinfo=UTC)
    assert "step_receipts" not in audit._crypto_refresh_details(crypto_v5_receipt, started)
    crypto_v5_receipt["step_attempts"] = crypto_v5_receipt["step_attempts"][-1:]
    details = audit._crypto_refresh_details(crypto_v5_receipt, started)
    assert details["step_receipts_scope"] == "exact_matched_run"
    assert len(details["step_receipts"]) == 1
    assert details["step_receipts"][0]["step"] == "bybit_transport_scan"
    crypto_v5_receipt["step_attempts"][0]["started_at_utc"] = "2026-09-22T10:00:00Z"
    assert "step_receipts" not in audit._crypto_refresh_details(crypto_v5_receipt, started)


@pytest.mark.parametrize("command", [
    ["python", "retry_packed_syncthing_scans.py"],
    ["python", "retry_packed_syncthing_scans.py", "--dataset", "okx", "--receipt", "/new.json"],
    ["python", "retry_packed_syncthing_scans.py", "--dataset", "bybit", "--receipt", "--help"],
    ["python", "retry_packed_syncthing_scans.py", "--dataset", "bybit", "--receipt", "/new.json", "--dataset", "okx"],
])
def test_crypto_v5_transport_command_requires_exact_bybit_scope(crypto_v5_receipt, command) -> None:
    crypto_v5_receipt["step_attempts"][-1]["command"] = command
    details = audit._crypto_refresh_details(crypto_v5_receipt, datetime(2026, 9, 23, 19, 59, tzinfo=UTC))
    assert "step_receipts" not in details


def test_crypto_v4_does_not_adopt_transport_extension(crypto_v5_receipt) -> None:
    crypto_v5_receipt["refresh_contract_version"] = 4
    details = audit._crypto_refresh_details(crypto_v5_receipt, datetime(2026, 9, 23, 19, 59, tzinfo=UTC))
    assert "transport_state" not in details
    assert "source_lease_elapsed_seconds" not in details
    assert "step_receipts" not in details


@pytest.mark.parametrize("no_op", [False, True])
def test_crypto_v5_transport_allows_unique_tail_batch_flag(crypto_v5_receipt, no_op) -> None:
    crypto_v5_receipt["step_attempts"][-1]["command"].append("--batch-object-paths")
    if no_op:
        crypto_v5_receipt.update(no_op=True, reused_previous_started_at_utc="2026-09-22T10:00:00Z")
        crypto_v5_receipt["step_attempts"] = crypto_v5_receipt["step_attempts"][-1:]
    details = audit._crypto_refresh_details(crypto_v5_receipt, datetime(2026, 9, 23, 19, 59, tzinfo=UTC))
    assert details["step_receipts_scope"] == "exact_matched_run"
    assert details["step_receipts"][-1]["step"] == "bybit_transport_scan"
    assert "/private" not in json.dumps(details)


@pytest.mark.parametrize("arguments", [
    ["--batch-object-paths", "--dataset", "bybit", "--receipt", "/new.json"],
    ["--dataset", "bybit", "--receipt", "/new.json", "--batch-object-paths", "--batch-object-paths"],
    ["--dataset", "bybit", "--receipt", "/new.json", "--unknown"],
    ["--dataset", "okx", "--receipt", "/new.json", "--batch-object-paths"],
])
def test_crypto_v5_batch_flag_does_not_relax_transport_scope(crypto_v5_receipt, arguments) -> None:
    crypto_v5_receipt["step_attempts"][-1]["command"] = ["python", "retry_packed_syncthing_scans.py", *arguments]
    details = audit._crypto_refresh_details(crypto_v5_receipt, datetime(2026, 9, 23, 19, 59, tzinfo=UTC))
    assert "step_receipts" not in details


@pytest.fixture
def openbb_receipt() -> dict[str, object]:
    return {
        "schema_version": 1, "event": "openbb_l1_compaction_attempt", "attempt_id": "c" * 32,
        "started_at_utc": "2026-09-23T19:59:00Z",
        "finished_at_utc": "2026-09-23T20:01:00Z",
        "elapsed_seconds": 120.0, "exit_code": 0, "reason": None,
        "mode": "compact", "publication_stage": "status_published",
        "systemd_invocation_id": "a" * 32,
        "state": "completed",
        "failed_segments": 0, "deferred_failed_segments": 0,
        "new_segments": 256, "stale_segments": 0,
        "pending_files": 900_000, "pending_rows": 8_000_000,
        "compacted_files": 100_000, "compacted_rows": 4_000_000,
        "stage_seconds": {
            "source_journal_prepare": 0.1, "stale_contract_audit": 10.0,
            "source_journal_checkpoint": 0.2, "unassigned_source_load": 2.0,
            "batch_planning": 0.5, "segment_build": 20.0, "query_view_publish": 30.0,
            "nested_source_scan": 500.0, "status_projection_write": 0.3,
        },
        "query_view_policy": {
            "schema_grouped_sec_requested": True, "schema_grouped_policy_revision": 1,
            "physical_identity_revision": 2, "command": "SECRET",
        },
        "view_endpoint_actions": {
            "regulators.sec.filing_headers": "rebuilt_schema_groups", "SECRET": "SECRET",
        },
        "command": "SECRET", "query_database": "/private/SECRET.duckdb",
        "view_endpoint_seconds": {"regulators.sec.filing_headers": 29.0},
    }


@pytest.fixture
def inspect_openbb_receipt(tmp_path: Path, monkeypatch, openbb_receipt):
    unit = "stockagent-openbb-l1-compaction.service"
    path = tmp_path / "l1_compaction_attempt_latest.json"
    monkeypatch.setattr(audit, "BUSINESS_RECEIPTS", {unit: path})
    monkeypatch.setattr(audit, "_openbb_attempt_journal", lambda *_args: None)

    def inspect(*, props=None, journal=None):
        path.write_text(json.dumps(openbb_receipt))
        return audit._business_receipt(
            unit,
            {"InvocationID": "a" * 32, "ExecMainStartTimestampMonotonic": 260_000_000,
             "ExecMainExitTimestampMonotonic": 380_000_000, "ActiveState": "inactive"}
            if props is None else props,
            {"observed_at": "2026-09-23T20:03:00+00:00", "monotonic": 500.0},
            journal,
        )
    return inspect


def test_openbb_receipt_has_exact_identity_safe_counts_policy_and_serial_stages(inspect_openbb_receipt) -> None:
    current = inspect_openbb_receipt()
    assert current["matches_last_attempt"] is True
    assert current["systemd_invocation_id"] == "a" * 32
    assert current["pending_files"] == 900_000
    assert current["failed_segments"] == current["deferred_failed_segments"] == 0
    assert current["query_view_policy"] == {
        "schema_grouped_sec_requested": True, "schema_grouped_policy_revision": 1,
        "physical_identity_revision": 2,
    }
    assert current["view_endpoint_actions"] == {"regulators.sec.filing_headers": "rebuilt_schema_groups"}
    assert [row["step"] for row in current["step_receipts"]] == [
        "source_journal_prepare", "stale_contract_audit", "source_journal_checkpoint",
        "unassigned_source_load", "batch_planning", "segment_build", "query_view_publish",
    ]
    assert audit._matched_step_timing_summary(current) == {
        "scope": "recorded_steps_only", "recorded_step_count": 7,
        "slowest_step": "query_view_publish", "slowest_step_seconds": 30.0,
    }
    for unsafe in ("SECRET", "query_database", "view_endpoint_seconds", "nested_source_scan"):
        assert unsafe not in json.dumps(current)
    assert "wall_seconds" not in current  # Never rename a stage sum as total wall.


@pytest.mark.parametrize("identity", [None, "", "b" * 32, "a" * 31, "A" * 32, True])
def test_openbb_receipt_missing_old_or_invalid_identity_never_matches(
    openbb_receipt, inspect_openbb_receipt, identity,
) -> None:
    openbb_receipt["systemd_invocation_id"] = identity
    current = inspect_openbb_receipt()
    assert current["matches_last_attempt"] is False
    assert "state" not in current and "step_receipts" not in current


def test_openbb_current_and_journal_identity_follow_same_start_authority(inspect_openbb_receipt) -> None:
    journal = {"last_attempt_started_at_utc": "2026-09-23T19:59:00+00:00",
               "last_attempt_invocation_id": "a" * 32}
    assert inspect_openbb_receipt(props={}, journal=journal)["matches_last_attempt"] is True
    assert inspect_openbb_receipt(props={}, journal={**journal, "last_attempt_invocation_id": "b" * 32})[
        "matches_last_attempt"
    ] is False
    # Even a time-near old journal cannot authorize a current different run.
    props = {"InvocationID": "b" * 32, "ExecMainStartTimestampMonotonic": 260_000_000}
    assert inspect_openbb_receipt(props=props, journal=journal)["matches_last_attempt"] is False
    props["InvocationID"] = ""
    assert inspect_openbb_receipt(props=props, journal=journal)["matches_last_attempt"] is False


def test_openbb_same_identity_still_requires_time_window(openbb_receipt, inspect_openbb_receipt) -> None:
    openbb_receipt["started_at_utc"] = "2026-09-23T19:58:00Z"
    assert inspect_openbb_receipt()["matches_last_attempt"] is False


@pytest.fixture
def openbb_resources(openbb_receipt):
    resources = {
        stage: {
            "cgroup_memory_high_events": 17130 if index == 4 else 0,
            "cgroup_memory_max_events": 0,
            "cgroup_memory_oom_events": 0,
            "cgroup_memory_oom_kill_events": 0,
            "cgroup_memory_bytes": (index + 1) * 100,
            "cgroup_memory_peak_bytes": (index + 1) * 200,
            "cgroup_memory_anon_bytes": (index + 1) * 80,
            "cgroup_memory_file_bytes": (index + 1) * 10,
            "cgroup_swap_bytes": [0, 4096, 8192, 0, 0][index],
            "process_swap_bytes": 0,
            "process_cpu_ns": index * 5_000_000_000,
            "monotonic_ns": 1_000_000_000_000 + index * 10_000_000_000,
            "SECRET": "SECRET",
        }
        for index, stage in enumerate(audit.OPENBB_RESOURCE_STAGES)
    }
    resources["nested_SECRET"] = {"cgroup_memory_high_events": 10**12}
    openbb_receipt["stage_resources"] = resources
    return resources


def test_openbb_completed_resource_pressure_is_separate_from_business_health(
    openbb_resources, inspect_openbb_receipt,
) -> None:
    current = inspect_openbb_receipt()
    assert current["state"] == "completed"
    assert list(current["stage_resources"]) == list(audit.OPENBB_RESOURCE_STAGES)
    summary = current["resource_pressure_summary"]
    assert summary["scope"] == "exact_matched_run_stage_samples"
    assert summary["observed_sample_maxima"]["cgroup_memory_high_events"] == 17130
    assert summary["observed_sample_maxima"]["cgroup_swap_bytes"] == 8192
    assert summary["first_to_last_counter_deltas"] == {
        "cgroup_memory_high_events": 17130, "cgroup_memory_max_events": 0,
        "cgroup_memory_oom_events": 0, "cgroup_memory_oom_kill_events": 0,
        "process_cpu_ns": 20_000_000_000, "monotonic_ns": 40_000_000_000,
    }
    services = [{"unit": "openbb", "business_receipt": current,
                 "memory_high_events_total": None, "memory_current_bytes": None}]
    assert audit.business_receipt_findings(services) == []
    finding, = audit.resource_pressure_findings(services)
    assert finding["systemd_invocation_id"] == "a" * 32
    assert finding["reasons"] == ["memory_high_events_observed", "swap_observed"]
    assert "SECRET" not in json.dumps(current)


@pytest.mark.parametrize("field", audit.OPENBB_RESOURCE_COUNTERS + audit.OPENBB_RESOURCE_GAUGES)
@pytest.mark.parametrize("value", [None, True, "0", -1, 0.0, float("nan"), float("inf"), 2**63])
def test_openbb_resource_invalid_values_are_unknown_not_zero(
    openbb_resources, inspect_openbb_receipt, field, value,
) -> None:
    for stage in audit.OPENBB_RESOURCE_STAGES:
        openbb_resources[stage][field] = value
    current = inspect_openbb_receipt()
    assert all(sample[field] is None for sample in current["stage_resources"].values())
    summary = current["resource_pressure_summary"]
    assert summary["observed_sample_maxima"][field] is None
    if field in audit.OPENBB_RESOURCE_COUNTERS:
        assert summary["first_to_last_counter_deltas"][field] is None


@pytest.mark.parametrize("damage", ("missing_stage", "missing_field", "reset"))
def test_openbb_resource_deltas_do_not_bridge_missing_or_reset_counters(
    openbb_resources, inspect_openbb_receipt, damage,
) -> None:
    stage = audit.OPENBB_RESOURCE_STAGES[2]
    if damage == "missing_stage":
        del openbb_resources[stage]
    elif damage == "missing_field":
        del openbb_resources[stage]["cgroup_memory_high_events"]
    else:
        openbb_resources[audit.OPENBB_RESOURCE_STAGES[1]]["cgroup_memory_high_events"] = 5
    current = inspect_openbb_receipt()
    summary = current["resource_pressure_summary"]
    assert summary["first_to_last_counter_deltas"]["cgroup_memory_high_events"] is None
    assert summary["observed_sample_maxima"]["cgroup_memory_high_events"] == 17130


def test_openbb_resource_monotonic_span_cannot_exceed_receipt_run(
    openbb_resources, inspect_openbb_receipt,
) -> None:
    openbb_resources[audit.OPENBB_RESOURCE_STAGES[-1]]["monotonic_ns"] += 500_000_000_000
    current = inspect_openbb_receipt()
    assert current["resource_pressure_summary"]["first_to_last_counter_deltas"]["monotonic_ns"] is None


@pytest.mark.parametrize("timings", [None, {}, {"query_view_publish": float("nan")}])
def test_openbb_resource_pressure_survives_invalid_optional_timing(
    openbb_receipt, openbb_resources, inspect_openbb_receipt, timings,
) -> None:
    openbb_receipt["stage_seconds"] = timings
    current = inspect_openbb_receipt()
    assert "step_receipts" not in current
    assert current["resource_pressure_summary"]["observed_sample_maxima"]["cgroup_memory_high_events"] == 17130


@pytest.mark.parametrize("resources", [None, {}, {"SECRET": {"cgroup_memory_high_events": 1}}])
def test_openbb_absent_resources_are_not_a_zero_pressure_proof(
    openbb_receipt, inspect_openbb_receipt, resources,
) -> None:
    openbb_receipt["stage_resources"] = resources
    current = inspect_openbb_receipt()
    assert "stage_resources" not in current
    assert "resource_pressure_summary" not in current
    assert audit.resource_pressure_findings([{"unit": "openbb", "business_receipt": current}]) == []


def test_openbb_valid_zero_resources_remain_zero_not_unknown(openbb_resources, inspect_openbb_receipt) -> None:
    for stage in audit.OPENBB_RESOURCE_STAGES:
        for field in audit.OPENBB_PRESSURE_SIGNALS:
            openbb_resources[stage][field] = 0
    current = inspect_openbb_receipt()
    summary = current["resource_pressure_summary"]
    assert summary["observed_sample_maxima"]["cgroup_memory_high_events"] == 0
    assert summary["first_to_last_counter_deltas"]["cgroup_memory_high_events"] == 0
    assert audit.resource_pressure_findings([{"unit": "openbb", "business_receipt": current}]) == []


@pytest.mark.parametrize("damage", ("active", "missing_exit", "exit_before_start", "exit_before_receipt", "future_receipt", "stale_identity"))
def test_openbb_resource_pressure_requires_same_completed_attempt(
    openbb_receipt, openbb_resources, inspect_openbb_receipt, damage,
) -> None:
    props = {"InvocationID": "a" * 32, "ExecMainStartTimestampMonotonic": 260_000_000,
             "ExecMainExitTimestampMonotonic": 380_000_000, "ActiveState": "inactive"}
    if damage == "active":
        props["ActiveState"] = "active"
    elif damage == "missing_exit":
        del props["ExecMainExitTimestampMonotonic"]
    elif damage == "exit_before_start":
        props["ExecMainExitTimestampMonotonic"] = 100_000_000
    elif damage == "exit_before_receipt":
        props["ExecMainExitTimestampMonotonic"] = 300_000_000
    elif damage == "future_receipt":
        openbb_receipt["finished_at_utc"] = "2026-09-23T21:01:00Z"
    else:
        props["InvocationID"] = "b" * 32
    # A matching old journal cannot override the current clock/identity authority.
    journal = {"last_attempt_started_at_utc": "2026-09-23T19:59:00Z",
               "last_attempt_completed_at_utc": "2026-09-23T20:01:00Z",
               "last_attempt_invocation_id": "a" * 32}
    current = inspect_openbb_receipt(props=props, journal=journal)
    assert "resource_pressure_summary" not in current
    assert "stage_resources" not in current


def test_openbb_journal_completion_retains_resources_after_cgroup_disappears(
    openbb_resources, inspect_openbb_receipt,
) -> None:
    journal = {"last_attempt_started_at_utc": "2026-09-23T19:59:00Z",
               "last_attempt_completed_at_utc": "2026-09-23T20:01:00Z",
               "last_attempt_invocation_id": "a" * 32}
    current = inspect_openbb_receipt(props={}, journal=journal)
    assert current["resource_pressure_summary"]["first_to_last_counter_deltas"]["cgroup_memory_high_events"] == 17130


def test_openbb_resource_pressure_does_not_overwrite_partial_business_state(
    openbb_receipt, openbb_resources, inspect_openbb_receipt,
) -> None:
    openbb_receipt.update(state="completed_with_failures", failed_segments=2)
    current = inspect_openbb_receipt()
    services = [{"unit": "openbb", "business_receipt": current}]
    assert audit.business_receipt_findings(services)[0]["receipt_state"] == "completed_with_failures"
    assert audit.resource_pressure_findings(services)[0]["reasons"] == [
        "memory_high_events_observed", "swap_observed",
    ]


def test_existing_runtime_trend_retains_completed_pressure_without_live_cgroup(
    tmp_path, monkeypatch, openbb_resources, inspect_openbb_receipt,
) -> None:
    from scripts import benchmark_dashboard_latency as benchmark
    from scripts import track_service_runtime_trends as trends

    inspect_openbb_receipt()  # Write the fixture receipt used by the real projection.
    unit = "stockagent-openbb-l1-compaction.service"
    journal = {"last_attempt_started_at_utc": "2026-09-23T19:59:00Z",
               "last_attempt_completed_at_utc": "2026-09-23T20:01:00Z",
               "last_attempt_invocation_id": "a" * 32,
               "last_attempt_wall_seconds": 120.0}
    snapshots = iter([
        {"observed_at": observed, "monotonic": monotonic,
         "units": {unit: {"ActiveState": "inactive", "MemoryCurrent": None}}}
        for observed, monotonic in (("2026-09-23T20:03:00Z", 500.0),
                                    ("2026-09-23T20:08:00Z", 800.0))
    ])
    monkeypatch.setattr(benchmark, "service_snapshot", lambda: next(snapshots))
    monkeypatch.setattr(audit, "_journal_last_completed_process", lambda _unit: journal)
    monkeypatch.setattr(trends, "_boot_id", lambda: "same-boot")
    monkeypatch.setattr(trends, "_recurring_timer_health", lambda _: {})
    monkeypatch.setattr(trends, "_filesystem_space", lambda: None)
    for monotonic in (500.0, 800.0):
        event = trends.sample_service_runtime(tmp_path / "trend-state.json", now_monotonic=monotonic)
        # This is the journal-ready event, not a replacement live cgroup metric.
        event = json.loads(json.dumps(event, allow_nan=False))
        row, = event["services"]
        assert row["memory_current_bytes"] is None
        assert row["cpu_cores_average"] is None
        assert row["last_attempt_invocation_id"] == "a" * 32
        assert row["business_receipt"]["state"] == "completed"
        assert row["business_receipt"]["resource_pressure_summary"]["first_to_last_counter_deltas"][
            "cgroup_memory_high_events"
        ] == 17130


@pytest.mark.parametrize("damage", ("unmatched", "identity", "scope", "invalid_maxima"))
def test_resource_findings_do_not_trust_unmatched_or_invalid_projection(
    openbb_resources, inspect_openbb_receipt, damage,
) -> None:
    current = inspect_openbb_receipt()
    if damage == "unmatched":
        current["matches_last_attempt"] = False
    elif damage == "identity":
        current["systemd_invocation_id"] = "SECRET"
    elif damage == "scope":
        current["resource_pressure_summary"]["scope"] = "SECRET"
    else:
        current["resource_pressure_summary"]["observed_sample_maxima"] = {
            key: True for key in audit.OPENBB_PRESSURE_SIGNALS
        }
    assert audit.resource_pressure_findings([{"unit": "openbb", "business_receipt": current}]) == []


@pytest.mark.parametrize("elapsed", [True, -1, float("nan"), float("inf"), "2", None, 121.01])
def test_openbb_invalid_or_beyond_run_stage_does_not_supply_timings(
    openbb_receipt, inspect_openbb_receipt, elapsed,
) -> None:
    openbb_receipt["stage_seconds"]["query_view_publish"] = elapsed
    current = inspect_openbb_receipt()
    assert current["matches_last_attempt"] is True
    assert "step_receipts" not in current
    assert audit._matched_step_timing_summary(current) is None


@pytest.mark.parametrize("timings", [{}, None, {"nested_source_scan": 1000.0}])
def test_openbb_empty_or_nested_only_timings_are_not_coverage(openbb_receipt, inspect_openbb_receipt, timings) -> None:
    openbb_receipt["stage_seconds"] = timings
    assert "step_receipts_scope" not in inspect_openbb_receipt()


@pytest.mark.parametrize("state", ["completed_with_failures", "completed_with_deferred_segments"])
def test_openbb_partial_completion_remains_a_finding(openbb_receipt, inspect_openbb_receipt, state) -> None:
    openbb_receipt["state"] = state
    openbb_receipt["failed_segments"] = 1 if state == "completed_with_failures" else 0
    openbb_receipt["deferred_failed_segments"] = 1 if state == "completed_with_deferred_segments" else 0
    current = inspect_openbb_receipt()
    findings = audit.business_receipt_findings([{"unit": "openbb", "business_receipt": current}])
    assert findings[0]["receipt_state"] == state
    assert "non_complete_state" in findings[0]["reasons"]


def test_openbb_unknown_policy_action_and_counts_are_not_projected(openbb_receipt, inspect_openbb_receipt) -> None:
    openbb_receipt.update(state="SECRET", failed_segments=True, pending_files=-1, pending_rows="900")
    openbb_receipt["query_view_policy"] = {
        "schema_grouped_sec_requested": "true", "schema_grouped_policy_revision": True,
        "physical_identity_revision": "SECRET",
    }
    openbb_receipt["view_endpoint_actions"]["regulators.sec.filing_headers"] = "SECRET"
    current = inspect_openbb_receipt()
    assert current["state"] == "unknown"
    for field in ("failed_segments", "pending_files", "pending_rows", "query_view_policy", "view_endpoint_actions"):
        assert field not in current
    assert "SECRET" not in json.dumps(current)


@pytest.mark.parametrize(("failed", "deferred"), [
    (1, 0), (0, 1), (True, 0), (None, 0), (0, -1), (0, "0"),
])
def test_openbb_completed_label_cannot_hide_invalid_or_nonzero_failures(
    openbb_receipt, inspect_openbb_receipt, failed, deferred,
) -> None:
    openbb_receipt.update(failed_segments=failed, deferred_failed_segments=deferred)
    current = inspect_openbb_receipt()
    assert current["matches_last_attempt"] is True
    assert current["state"] == "unknown"
    findings = audit.business_receipt_findings([{"unit": "openbb", "business_receipt": current}])
    assert "non_complete_state" in findings[0]["reasons"]


@pytest.mark.parametrize(("state", "reason", "code", "mode", "stage", "failed", "deferred"), [
    ("deferred", "archive_busy", 0, "compact", "not_attempted", 0, 0),
    ("deferred", "archive_busy", 0, "audit", "not_attempted", 0, 0),
    ("deferred", "archive_resumed", 0, "compact", "not_attempted", 2, 1),
    ("failed", "compactor_lock_busy", 1, "compact", "not_attempted", 0, 0),
    ("failed", "exception", 1, "compact", "query_publish_started", 0, 0),
    ("failed", "attempt_receipt_error", 1, "compact", "status_published", 0, 0),
    ("interrupted", "interrupted", 130, "compact", "query_published", 0, 0),
    ("interrupted", "interrupted", 0, "compact", "not_attempted", 0, 0),
    ("audit_completed", None, 0, "audit", "not_attempted", 0, 0),
    ("audit_failed", None, 2, "audit", "not_attempted", 1, 0),
])
def test_openbb_attempt_nonpublication_terminals_keep_state_and_resources(
    openbb_receipt, openbb_resources, inspect_openbb_receipt,
    state, reason, code, mode, stage, failed, deferred,
):
    openbb_receipt.update(
        state=state, reason=reason, exit_code=code, mode=mode, publication_stage=stage,
        failed_segments=failed, deferred_failed_segments=deferred,
    )
    current = inspect_openbb_receipt()
    assert current["matches_last_attempt"] is True
    assert current["state"] == state
    assert current["publication_stage"] == stage
    assert current["resource_pressure_summary"]["observed_sample_maxima"]["cgroup_memory_high_events"] == 17130
    findings = audit.business_receipt_findings([{"unit": "openbb", "business_receipt": current}])
    assert findings[0]["receipt_state"] == state


@pytest.mark.parametrize("change", [
    {"schema_version": True}, {"schema_version": 2}, {"event": "published"},
    {"attempt_id": "A" * 32}, {"attempt_id": None}, {"attempt_id": "x"},
    {"started_at_utc": "2026-09-23T19:59:00"},
    {"finished_at_utc": "2026-09-23T20:01:00"},
    {"finished_at_utc": "2026-09-23T20:04:00Z"},
    {"finished_at_utc": "2026-09-23T19:58:00Z"},
    {"elapsed_seconds": True}, {"elapsed_seconds": float("nan")},
    {"elapsed_seconds": -1}, {"elapsed_seconds": 100},
])
def test_openbb_attempt_invalid_envelope_is_not_current(openbb_receipt, inspect_openbb_receipt, change):
    openbb_receipt.update(change)
    current = inspect_openbb_receipt()
    assert current["matches_last_attempt"] is False
    assert "state" not in current and "stage_resources" not in current


@pytest.mark.parametrize("change", [
    {"exit_code": True}, {"exit_code": 1}, {"exit_code": None},
    {"publication_stage": "query_published"}, {"publication_stage": "SECRET"},
    {"reason": "SECRET"}, {"reason": "exception"}, {"mode": "audit"},
    {"state": "deferred", "reason": "archive_busy"},
    {"state": "failed", "reason": "exception"},
    {"state": "audit_completed", "mode": "audit"},
    {"new_segments": True}, {"stale_segments": -1},
])
def test_openbb_attempt_semantic_contradictions_are_not_success(
    openbb_receipt, inspect_openbb_receipt, change,
):
    openbb_receipt.update(change)
    current = inspect_openbb_receipt()
    assert current["matches_last_attempt"] is True
    assert current["state"] == "unknown"
    assert "publication_stage" not in current
    assert "SECRET" not in json.dumps(current)


def test_openbb_attempt_partial_stages_include_boundaries_without_filling_zero(
    openbb_receipt, inspect_openbb_receipt,
):
    openbb_receipt.update(state="deferred", reason="archive_resumed", publication_stage="not_attempted")
    openbb_receipt["stage_resources"] = {
        "before_attempt": {"cgroup_memory_high_events": 3, "monotonic_ns": 10},
        "after_segment_build": {"cgroup_memory_high_events": 8, "monotonic_ns": 20},
        "after_attempt": {"cgroup_memory_high_events": 9, "monotonic_ns": 30},
    }
    current = inspect_openbb_receipt()
    assert list(current["stage_resources"]) == ["before_attempt", *audit.OPENBB_RESOURCE_STAGES, "after_attempt"]
    summary = current["resource_pressure_summary"]
    assert summary["observed_sample_maxima"]["cgroup_memory_high_events"] == 9
    assert summary["first_to_last_counter_deltas"]["cgroup_memory_high_events"] is None
    assert current["stage_resources"]["after_query_view_publish"]["cgroup_memory_high_events"] is None
    assert summary["attempt_counter_deltas"]["cgroup_memory_high_events"] == 6
    finding, = audit.resource_pressure_findings([{"unit": "openbb", "business_receipt": current}])
    assert finding["pressure_counter_deltas"]["cgroup_memory_high_events"] == 6


@pytest.mark.parametrize("damage", [None, "zero", "null", "reset", "bad_clock", "missing_clock", "beyond_run"])
def test_openbb_attempt_delta_uses_only_real_contiguous_samples(
    openbb_receipt, inspect_openbb_receipt, damage,
):
    openbb_receipt.update(state="deferred", reason="archive_busy", publication_stage="not_attempted")
    resources = {
        "before_attempt": {"cgroup_memory_high_events": 20, "process_cpu_ns": 10, "monotonic_ns": 100},
        "after_attempt": {"cgroup_memory_high_events": 25, "process_cpu_ns": 20, "monotonic_ns": 200},
    }
    if damage == "zero":
        resources["after_attempt"]["cgroup_memory_high_events"] = 20
    elif damage in ("null", "reset"):
        resources["after_segment_build"] = {
            "cgroup_memory_high_events": None if damage == "null" else 19,
            "process_cpu_ns": 15, "monotonic_ns": 150,
        }
    elif damage == "bad_clock":
        resources["after_attempt"]["monotonic_ns"] = 99
    elif damage == "missing_clock":
        del resources["after_attempt"]["monotonic_ns"]
    elif damage == "beyond_run":
        resources["after_attempt"]["monotonic_ns"] = 999_000_000_000
    openbb_receipt["stage_resources"] = resources
    current = inspect_openbb_receipt()
    summary = current["resource_pressure_summary"]
    assert summary["first_to_last_counter_deltas"]["cgroup_memory_high_events"] is None
    expected = 5 if damage is None else 0 if damage == "zero" else None
    assert summary["attempt_counter_deltas"]["cgroup_memory_high_events"] == expected
    findings = audit.resource_pressure_findings([{"unit": "openbb", "business_receipt": current}])
    assert bool(findings) is (damage is None)
    if damage in (None, "zero", "null", "reset"):
        assert summary["attempt_counter_deltas"]["process_cpu_ns"] == 10


def test_openbb_attempt_active_without_current_start_cannot_use_old_journal(inspect_openbb_receipt):
    journal = {"last_attempt_started_at_utc": "2026-09-23T19:59:00Z",
               "last_attempt_completed_at_utc": "2026-09-23T20:01:00Z",
               "last_attempt_invocation_id": "a" * 32}
    current = inspect_openbb_receipt(props={"ActiveState": "active"}, journal=journal)
    assert current["matches_last_attempt"] is False


@pytest.mark.parametrize("props", [{"ExecMainStatus": 1}, {"Result": "exit-code"}, {"Result": "oom-kill"}])
def test_openbb_attempt_completed_cannot_overrule_failed_process(
    openbb_resources, inspect_openbb_receipt, props,
):
    current = inspect_openbb_receipt(props={
        "InvocationID": "a" * 32, "ExecMainStartTimestampMonotonic": 260_000_000,
        "ExecMainExitTimestampMonotonic": 380_000_000, "ActiveState": "failed", **props,
    })
    assert current["state"] == "unknown"
    assert current["matches_last_attempt"] is True
    assert "resource_pressure_summary" in current


@pytest.mark.parametrize("source", ["current", "journal"])
def test_openbb_interrupt_shell_status_is_compared_to_observed_signal(
    openbb_receipt, inspect_openbb_receipt, source,
):
    openbb_receipt.update(state="interrupted", reason="interrupted", exit_code=130,
                          exit_code_basis="shell_signal", signal_number=2)
    props = {"InvocationID": "a" * 32, "ExecMainStartTimestampMonotonic": 260_000_000,
             "ExecMainExitTimestampMonotonic": 380_000_000, "ActiveState": "failed",
             "ExecMainStatus": 2, "Result": "signal"}
    journal = {"last_attempt_started_at_utc": "2026-09-23T19:59:00Z",
               "last_attempt_completed_at_utc": "2026-09-23T20:01:00Z",
               "last_attempt_invocation_id": "a" * 32, "last_attempt_outcome": "failed",
               "last_attempt_exit_status": 2, "last_attempt_exit_code": "killed"}
    current = inspect_openbb_receipt(props=props if source == "current" else {}, journal=journal)
    assert current["state"] == "interrupted"
    assert current["exit_code"] == 130


def _openbb_journal_event(payload, **overrides):
    return {
        "_SYSTEMD_UNIT": "stockagent-openbb-l1-compaction.service",
        "_SYSTEMD_INVOCATION_ID": "a" * 32, "_BOOT_ID": "d" * 32,
        "__REALTIME_TIMESTAMP": str(int(datetime(2026, 9, 23, 20, 1, 1, tzinfo=UTC).timestamp() * 1e6)),
        "MESSAGE": json.dumps(payload), **overrides,
    }


@pytest.mark.parametrize("damage", [
    None, "unit", "identity", "boot", "missing_boot", "message_type", "message_json",
    "schema", "payload_identity", "future_event", "before_finish", "oversize",
])
def test_openbb_attempt_journal_recovery_uses_trusted_exact_bounded_event(
    monkeypatch, openbb_receipt, damage,
):
    payload = dict(openbb_receipt)
    if damage == "schema":
        payload["schema_version"] = True
    elif damage == "payload_identity":
        payload["systemd_invocation_id"] = "b" * 32
    event = _openbb_journal_event(payload)
    if damage in ("unit", "identity", "boot"):
        key = {"unit": "_SYSTEMD_UNIT", "identity": "_SYSTEMD_INVOCATION_ID", "boot": "_BOOT_ID"}[damage]
        event[key] = "b" * 32
    elif damage == "missing_boot":
        del event["_BOOT_ID"]
    elif damage == "message_type":
        event["MESSAGE"] = [event["MESSAGE"]]
    elif damage == "message_json":
        event["MESSAGE"] = "{"
    elif damage == "oversize":
        event["MESSAGE"] = "x" * 65536
    elif damage in ("future_event", "before_finish"):
        event["__REALTIME_TIMESTAMP"] = str(int(datetime(
            2026, 9, 23, 20, 4 if damage == "future_event" else 0, tzinfo=UTC,
        ).timestamp() * 1e6))

    def run(command, **kwargs):
        assert "_SYSTEMD_UNIT=stockagent-openbb-l1-compaction.service" in command
        assert "_SYSTEMD_INVOCATION_ID=" + "a" * 32 in command
        assert command[command.index("-n") + 1] == "32"
        assert "--since" in command and kwargs["timeout"] == 5
        return subprocess.CompletedProcess(command, 0, stdout=json.dumps(event))

    monkeypatch.setattr(audit.subprocess, "run", run)
    found = audit._openbb_attempt_journal(
        "a" * 32, datetime(2026, 9, 23, 19, 59, tzinfo=UTC),
        {"observed_at": "2026-09-23T20:03:00Z"}, "d" * 32,
    )
    assert (found == payload) if damage is None else found is None


@pytest.mark.parametrize("damage", [None, "missing_message", "null_message", "oversize_event", "oversize_output"])
def test_openbb_attempt_journal_requests_full_large_message_without_relaxing_bounds(
    monkeypatch, openbb_receipt, damage,
):
    # journalctl JSON replaces fields above 4096 bytes with null without --all.
    # Real terminal resource receipts exceed that threshold, unlike tiny mocks.
    payload = {**openbb_receipt, "diagnostic_padding": "x" * 5000}
    if damage == "oversize_event":
        payload["diagnostic_padding"] = "x" * 65536
    event = _openbb_journal_event(payload)
    assert len(event["MESSAGE"].encode()) > 4096

    def run(command, **kwargs):
        assert command[command.index("-n") + 1] == "32"
        assert kwargs["timeout"] == 5
        emitted = dict(event)
        if damage == "missing_message":
            emitted.pop("MESSAGE")
        elif damage == "null_message" or "--all" not in command:
            emitted["MESSAGE"] = None
        output = json.dumps(emitted)
        if damage == "oversize_output":
            output += "\n" + " " * (1024 * 1024)
        return subprocess.CompletedProcess(command, 0, stdout=output)

    monkeypatch.setattr(audit.subprocess, "run", run)
    found = audit._openbb_attempt_journal(
        "a" * 32, datetime(2026, 9, 23, 19, 59, tzinfo=UTC),
        {"observed_at": "2026-09-23T20:03:00Z"}, "d" * 32,
    )
    assert (found == payload) if damage is None else found is None


def test_openbb_journal_only_lock_failure_does_not_use_old_published_status(
    tmp_path, monkeypatch, openbb_receipt,
):
    unit = "stockagent-openbb-l1-compaction.service"
    path = tmp_path / "l1_compaction_attempt_latest.json"
    (tmp_path / "l1_compaction_latest.json").write_text(json.dumps(openbb_receipt))
    monkeypatch.setattr(audit, "BUSINESS_RECEIPTS", {unit: path})
    journal = {"last_attempt_started_at_utc": "2026-09-23T19:59:00Z",
               "last_attempt_completed_at_utc": "2026-09-23T20:01:00Z",
               "last_attempt_invocation_id": "a" * 32, "last_attempt_boot_id": "d" * 32}
    failure = {**openbb_receipt, "state": "failed", "reason": "compactor_lock_busy",
               "exit_code": 1, "publication_stage": "not_attempted"}
    monkeypatch.setattr(audit.subprocess, "run", lambda command, **kwargs: subprocess.CompletedProcess(
        command, 0, stdout=json.dumps(_openbb_journal_event(failure)),
    ))
    current = audit._business_receipt(unit, {}, {"observed_at": "2026-09-23T20:03:00Z"}, journal)
    assert current["matches_last_attempt"] is True
    assert current["state"] == "failed"
    assert current["receipt_source"] == "attempt_journal"
    monkeypatch.setattr(audit.subprocess, "run", lambda command, **kwargs: subprocess.CompletedProcess(
        command, 0, stdout="",
    ))
    assert audit._business_receipt(unit, {}, {"observed_at": "2026-09-23T20:03:00Z"}, journal)[
        "matches_last_attempt"
    ] is False


@pytest.mark.parametrize("failure_state", ["failed", "SECRET"])
def test_openbb_journal_latest_terminal_never_falls_back_to_earlier_success(
    monkeypatch, openbb_receipt, failure_state,
):
    failed = {**openbb_receipt, "state": failure_state, "reason": "attempt_receipt_error", "exit_code": 1}
    events = [_openbb_journal_event(openbb_receipt), _openbb_journal_event(failed)]
    monkeypatch.setattr(audit.subprocess, "run", lambda command, **kwargs: subprocess.CompletedProcess(
        command, 0, stdout="\n".join(json.dumps(event) for event in events),
    ))
    found = audit._openbb_attempt_journal(
        "a" * 32, datetime(2026, 9, 23, 19, 59, tzinfo=UTC),
        {"observed_at": "2026-09-23T20:03:00Z"}, "d" * 32,
    )
    assert found == failed
    assert audit._openbb_attempt_state(found) == ("failed" if failure_state == "failed" else "unknown")


@pytest.mark.parametrize("newer", ["running", "bad_clock", "bad_schema", "bad_attempt_id"])
def test_openbb_journal_new_attempt_barrier_prevents_old_success_recovery(
    monkeypatch, openbb_receipt, newer,
):
    later = {**openbb_receipt, "attempt_id": "e" * 32}
    if newer == "running":
        later.update(state="running", elapsed_seconds=0, finished_at_utc=None, exit_code=None)
    elif newer == "bad_clock":
        later["finished_at_utc"] = "malformed"
    elif newer == "bad_schema":
        later["schema_version"] = True
    else:
        later["attempt_id"] = "malformed"
    events = [_openbb_journal_event(openbb_receipt), _openbb_journal_event(later)]
    monkeypatch.setattr(audit.subprocess, "run", lambda command, **kwargs: subprocess.CompletedProcess(
        command, 0, stdout="\n".join(json.dumps(event) for event in events),
    ))
    found = audit._openbb_attempt_journal(
        "a" * 32, datetime(2026, 9, 23, 19, 59, tzinfo=UTC),
        {"observed_at": "2026-09-23T20:03:00Z"}, "d" * 32,
    )
    assert found == later if newer == "running" else found is None


def test_openbb_journal_unknown_boot_is_not_recovery(monkeypatch):
    monkeypatch.setattr(audit.subprocess, "run", lambda *_args, **_kwargs: pytest.fail("must not query unknown boot"))
    assert audit._openbb_attempt_journal(
        "a" * 32, datetime(2026, 9, 23, 19, 59, tzinfo=UTC),
        {"observed_at": "2026-09-23T20:03:00Z"},
    ) is None


def test_openbb_invalid_file_time_still_blocks_cross_attempt_journal(
    tmp_path, monkeypatch, openbb_receipt,
):
    path = tmp_path / "l1_compaction_attempt_latest.json"
    path.write_text(json.dumps({**openbb_receipt, "attempt_id": "e" * 32, "finished_at_utc": "bad"}))
    monkeypatch.setattr(audit, "_openbb_attempt_journal", lambda *_args: openbb_receipt)
    current = audit._openbb_attempt_receipt(
        path, {"InvocationID": "a" * 32, "ExecMainStartTimestampMonotonic": 260_000_000,
               "ExecMainExitTimestampMonotonic": 380_000_000, "ActiveState": "inactive"},
        {"observed_at": "2026-09-23T20:03:00Z", "monotonic": 500.0}, None,
    )
    assert current["matches_last_attempt"] is False


@pytest.mark.parametrize("same_attempt", [True, False])
def test_openbb_running_receipt_recovers_only_its_own_terminal(
    tmp_path, monkeypatch, openbb_receipt, same_attempt,
):
    path = tmp_path / "l1_compaction_attempt_latest.json"
    running = {**openbb_receipt, "state": "running", "finished_at_utc": None,
               "elapsed_seconds": 0, "exit_code": None, "publication_stage": "not_attempted"}
    path.write_text(json.dumps(running))
    recovered = {**openbb_receipt, "state": "failed", "reason": "attempt_receipt_error", "exit_code": 1}
    if not same_attempt:
        recovered["attempt_id"] = "e" * 32
    monkeypatch.setattr(audit, "_openbb_attempt_journal", lambda *_args: recovered)
    current = audit._openbb_attempt_receipt(
        path, {"InvocationID": "a" * 32, "ExecMainStartTimestampMonotonic": 260_000_000,
               "ExecMainExitTimestampMonotonic": 380_000_000, "ActiveState": "failed"},
        {"observed_at": "2026-09-23T20:03:00Z", "monotonic": 500.0}, None,
    )
    assert current["state"] == ("failed" if same_attempt else "running")
    assert current["receipt_source"] == ("attempt_journal" if same_attempt else "attempt_receipt")
    if not same_attempt:
        assert "publication_stage" not in current


def test_journal_recovers_only_paired_pid1_invocation(monkeypatch) -> None:
    def event(message_id: str, boot: str, invocation: str, monotonic: int,
              realtime: int, **extra: str) -> str:
        return json.dumps({
            "UNIT": "stockagent-job.service", "_BOOT_ID": boot,
            "INVOCATION_ID": invocation, "MESSAGE_ID": message_id,
            "__MONOTONIC_TIMESTAMP": str(monotonic),
            "__REALTIME_TIMESTAMP": str(realtime), **extra,
        })

    output = "\n".join([
        event(audit.JOURNAL_START_MESSAGE_ID, "old", "same", 1_000_000,
              1_790_000_000_000_000, JOB_TYPE="start"),
        event(audit.JOURNAL_RESOURCE_MESSAGE_ID, "new", "same", 4_000_000,
              1_790_000_100_000_000),  # different boot cannot be paired
        event(audit.JOURNAL_START_MESSAGE_ID, "new", "latest", 5_000_000,
              1_790_000_200_000_000, JOB_TYPE="start"),
        event(audit.JOURNAL_PROCESS_EXIT_MESSAGE_ID, "new", "latest", 7_900_000,
              1_790_000_202_900_000, EXIT_CODE="exited", EXIT_STATUS="1"),
        event(audit.JOURNAL_FAILURE_MESSAGE_ID, "new", "latest", 8_000_000,
              1_790_000_203_000_000),
        event(audit.JOURNAL_RESOURCE_MESSAGE_ID, "new", "latest", 8_250_000,
              1_790_000_203_250_000),
        event(audit.JOURNAL_START_MESSAGE_ID, "new", "newer", 9_000_000,
              1_790_000_204_000_000, JOB_TYPE="start"),
        event(audit.JOURNAL_PROCESS_EXIT_MESSAGE_ID, "new", "newer", 9_350_000,
              1_790_000_204_350_000, EXIT_CODE="exited", EXIT_STATUS="0"),
        event(audit.JOURNAL_SUCCESS_MESSAGE_ID, "new", "newer", 9_400_000,
              1_790_000_204_400_000),  # no resource line for a short job
    ])

    def run(command, **_kwargs):
        assert command[:3] == ["journalctl", "_PID=1", "UNIT=stockagent-job.service"]
        return subprocess.CompletedProcess(command, 0, stdout=output)

    monkeypatch.setattr(audit.subprocess, "run", run)
    monkeypatch.setattr(audit.Path, "read_text", lambda *_a, **_kw: "new")
    result = audit._journal_last_completed_process("stockagent-job.service")
    assert result is not None
    assert result["last_attempt_wall_seconds"] == 0.4
    assert result["last_attempt_outcome"] == "process_exited_zero"
    assert result["last_attempt_exit_status"] == 0
    assert result["last_attempt_exit_code"] == "exited"
    assert result["last_attempt_invocation_id"] == "newer"
    assert result["last_attempt_boot_id"] == "new"
    assert result["last_attempt_started_at_utc"] == "2026-09-21T14:16:44+00:00"
    assert result["last_attempt_completed_at_utc"] == "2026-09-21T14:16:44.400000+00:00"


@pytest.mark.parametrize("new_terminal", [True, False])
def test_journal_latest_attempt_uses_same_boot_monotonic_and_no_old_success_fallback(
    monkeypatch, new_terminal,
) -> None:
    unit, boot = "stockagent-job.service", "b" * 32
    rows = []
    for invocation, mono, real, terminal in (
        ("a" * 32, 100_000_000, 1_790_000_100_000_000, True),
        ("c" * 32, 200_000_000, 1_790_000_000_000_000, new_terminal),
    ):
        rows.append({"UNIT": unit, "_BOOT_ID": boot, "INVOCATION_ID": invocation,
                     "MESSAGE_ID": audit.JOURNAL_START_MESSAGE_ID, "JOB_TYPE": "start",
                     "__MONOTONIC_TIMESTAMP": str(mono), "__REALTIME_TIMESTAMP": str(real)})
        if terminal:
            rows.append({**rows[-1], "MESSAGE_ID": audit.JOURNAL_SUCCESS_MESSAGE_ID,
                         "__MONOTONIC_TIMESTAMP": str(mono + 5_000_000),
                         "__REALTIME_TIMESTAMP": str(real + 5_000_000)})
    monkeypatch.setattr(audit.subprocess, "run", lambda *a, **kw: subprocess.CompletedProcess(
        a[0], 0, stdout="\n".join(json.dumps(row) for row in rows)))
    result = audit._journal_last_completed_process(unit)
    if not new_terminal:
        assert result is None
    else:
        assert result["last_attempt_invocation_id"] == "c" * 32
        assert result["last_attempt_started_monotonic_us"] == 200_000_000
        assert result["last_attempt_completed_monotonic_us"] == 205_000_000


@pytest.mark.parametrize("case", ["delayed_old_resource", "current_boot", "ambiguous_boots", "orphan_terminal"])
def test_journal_attempt_order_needs_boot_and_start_evidence(monkeypatch, case) -> None:
    unit = "stockagent-job.service"
    boot_a, boot_b = "a" * 32, "b" * 32
    rows = []
    def add(invocation, boot, mono, realtime, message_id, **extra):
        rows.append({"UNIT": unit, "_BOOT_ID": boot, "INVOCATION_ID": invocation,
                     "MESSAGE_ID": message_id, "__MONOTONIC_TIMESTAMP": str(mono * 1_000_000),
                     "__REALTIME_TIMESTAMP": str(realtime * 1_000_000), **extra})
    for invocation, boot, mono, real in (
        ("old", boot_a, 100, 1_790_000_100),
        ("new", boot_a if case in ("delayed_old_resource", "orphan_terminal") else boot_b, 200, 1_790_000_000),
    ):
        if case != "orphan_terminal" or invocation == "old":
            add(invocation, boot, mono, real, audit.JOURNAL_START_MESSAGE_ID, JOB_TYPE="start")
        add(invocation, boot, mono + 5, real + 5, audit.JOURNAL_SUCCESS_MESSAGE_ID)
    if case == "delayed_old_resource":
        add("old", boot_a, 300, 1_790_000_300, audit.JOURNAL_RESOURCE_MESSAGE_ID)
    monkeypatch.setattr(audit.subprocess, "run", lambda *a, **kw: subprocess.CompletedProcess(
        a[0], 0, stdout="\n".join(json.dumps(row) for row in rows)))
    monkeypatch.setattr(audit.Path, "read_text", lambda *_a, **_kw: (
        boot_b if case == "current_boot" else "absent"
    ))
    result = audit._journal_last_completed_process(unit)
    if case in ("ambiguous_boots", "orphan_terminal"):
        assert result is None
    else:
        assert result["last_attempt_invocation_id"] == "new"


def test_taifex_auxiliary_business_hook_uses_shared_adapter(monkeypatch) -> None:
    from scripts import service_stage_journal
    props, snapshot, attempt = {"ActiveState": "inactive"}, {}, {}
    expected = {"matches_last_attempt": True, "state": "completed"}
    seen = []
    def project(*args):
        seen.append(args)
        return expected
    monkeypatch.setattr(service_stage_journal, "taifex_auxiliary_stage_receipt", project)
    assert audit._business_receipt(service_stage_journal.TAIFEX_AUXILIARY_UNIT,
                                   props, snapshot, attempt) is expected
    assert seen == [(props, snapshot, attempt)]


def test_business_receipt_matches_unloaded_unit_journal_attempt(
    tmp_path: Path, monkeypatch
) -> None:
    receipt = tmp_path / "latest.json"
    receipt.write_text(json.dumps({
        "started_at_taipei": "2026-09-24T00:00:48.721178+08:00",
        "status": "deferred", "reason": "canonical_refresh_lock_busy",
    }))
    monkeypatch.setattr(audit, "BUSINESS_RECEIPTS", {
        "stockagent-tw-public-cold-publish.service": receipt,
    })
    result = audit._business_receipt(
        "stockagent-tw-public-cold-publish.service",
        {"ExecMainStartTimestampMonotonic": 0},
        {"observed_at": "2026-09-24T04:00:00+00:00", "monotonic": 2000.0},
        {"last_attempt_started_at_utc": "2026-09-23T16:00:48.720000+00:00"},
    )
    assert result is not None
    assert result["matches_last_attempt"] is True
    assert result["state"] == "deferred"


def test_cold_publish_matches_immutable_run_when_latest_is_another_caller(
    tmp_path: Path, monkeypatch
) -> None:
    latest = tmp_path / "latest.json"
    latest.write_text(json.dumps({
        "started_at_taipei": "2026-09-24T08:03:49+08:00", "status": "deferred",
    }))
    runs = tmp_path / "runs"
    runs.mkdir()
    exact = runs / "20260924T000048721178.json"
    exact.write_text(json.dumps({
        "started_at_taipei": "2026-09-24T00:00:48.721178+08:00",
        "completed_at_taipei": "2026-09-24T00:00:49.161887+08:00",
        "status": "deferred", "reason": "canonical_refresh_lock_busy",
    }))
    monkeypatch.setattr(audit, "BUSINESS_RECEIPTS", {
        "stockagent-tw-public-cold-publish.service": latest,
    })
    result = audit._business_receipt(
        "stockagent-tw-public-cold-publish.service",
        {"ExecMainStartTimestampMonotonic": 0},
        {"observed_at": "2026-09-24T04:00:00+00:00", "monotonic": 2000.0},
        {"last_attempt_started_at_utc": "2026-09-23T16:00:48.720000+00:00"},
    )
    assert result is not None
    assert result["path"] == str(exact)
    assert result["matches_last_attempt"] is True
    assert result["reason"] == "canonical_refresh_lock_busy"
    assert result["finished_at_utc"] == "2026-09-23T16:00:49.161887+00:00"


def test_registered_run_receipt_matches_invocation_without_exposing_log_path(
    tmp_path: Path, monkeypatch
) -> None:
    unit = "stockagent-registered-data-backfill.service"
    record = tmp_path / "registered_backfill_runs.tsv"
    record.write_text(
        "timestamp_utc\trun_id\trun_mode\tcycle_id\tstatus\telapsed_sec\tfailed_steps\tlog_file\n"
        "2026-09-20T20:39:44Z\tregistered-backfill-20260920T022411Z\tonce\t1\t"
        "completed_with_failures\t65733\tbinance_perpetuals\t/private/secret.log\n"
    )
    run_id = "registered-backfill-20260920T022411Z"
    step_dir = tmp_path / "registered_backfill" / "step_receipts" / run_id
    step_dir.mkdir(parents=True)
    (step_dir / "okx_perp_1m_update.json").write_text(json.dumps({
        "schema_version": 1,
        "run_id": run_id,
        "step": "okx_perp_1m_update",
        "state": "complete",
        "elapsed_seconds": 65726,
        "exit_code": 0,
        "command": "--secret=private",
    }))
    (step_dir / "binance_perp_1m_update.json").write_text(json.dumps({
        "schema_version": 1,
        "run_id": run_id,
        "step": "binance_perp_1m_update",
        "state": "failed",
        "elapsed_seconds": 30213,
        "exit_code": 1,
    }))
    (step_dir / "unrelated.json").write_text(json.dumps({
        "schema_version": 1,
        "run_id": "registered-backfill-20260919T022411Z",
        "step": "unrelated",
        "state": "complete",
        "elapsed_seconds": 1,
    }))
    monkeypatch.setattr(audit, "REGISTERED_RUN_RECORDS", {unit: record})
    journal = {"last_attempt_started_at_utc": "2026-09-20T02:24:10.854526+00:00"}
    matched = audit._business_receipt(unit, {}, {}, journal)
    assert matched is not None
    assert matched["matches_last_attempt"] is True
    assert matched["state"] == "completed_with_failures"
    assert matched["failed_steps"] == ["binance_perpetuals"]
    assert matched["elapsed_seconds"] == 65733
    assert matched["step_receipts_scope"] == "exact_matched_run"
    assert matched["step_receipts"] == [
        {"step": "okx_perp_1m_update", "state": "complete",
         "elapsed_seconds": 65726.0, "exit_code": 0},
        {"step": "binance_perp_1m_update", "state": "failed",
         "elapsed_seconds": 30213.0, "exit_code": 1},
    ]
    assert "secret.log" not in json.dumps(matched)
    assert "private" not in json.dumps(matched)

    stale = audit._business_receipt(
        unit, {}, {},
        {"last_attempt_started_at_utc": "2026-09-21T02:24:10+00:00"},
    )
    assert stale is not None
    assert stale["matches_last_attempt"] is False
    assert "state" not in stale
    assert "step_receipts" not in stale


def test_registered_run_nanosecond_ids_keep_fast_retry_receipts_separate(
    tmp_path: Path, monkeypatch
) -> None:
    scope = "features"
    first_id = "registered-features-20260920T022411000100000Z"
    retry_id = "registered-features-20260920T022411900200000Z"
    assert audit._registered_run_started_utc(scope, first_id) != audit._registered_run_started_utc(
        scope, retry_id
    )
    assert audit._registered_run_started_utc(
        scope, "registered-features-20260920T022411Z"
    ) is not None
    assert audit._registered_run_started_utc(
        scope, "registered-features-20261320T022411900200000Z"
    ) is None

    record = tmp_path / "registered_features_runs.tsv"
    record.write_text(
        "timestamp_utc\trun_id\trun_mode\tcycle_id\tstatus\telapsed_sec\tfailed_steps\tlog_file\n"
        f"2026-09-20T02:24:11Z\t{first_id}\tonce\t1\tcompleted_with_failures\t0\tprovider\t/private/first.log\n"
        f"2026-09-20T02:24:12Z\t{retry_id}\tonce\t1\tcompleted\t0\t\t/private/retry.log\n"
    )
    for run_id, state in ((first_id, "failed"), (retry_id, "complete")):
        directory = tmp_path / "registered_features" / "step_receipts" / run_id
        directory.mkdir(parents=True)
        (directory / "provider.json").write_text(json.dumps({
            "schema_version": 1,
            "run_id": run_id,
            "step": "provider",
            "state": state,
            "elapsed_seconds": 0.1,
            "exit_code": 1 if state == "failed" else 0,
        }))
    monkeypatch.setattr(audit, "REGISTERED_RUN_RECORDS", {
        "stockagent-registered-data-features.service": record,
    })
    result = audit._business_receipt(
        "stockagent-registered-data-features.service", {}, {},
        {"last_attempt_started_at_utc": "2026-09-20T02:24:11.900200+00:00"},
    )
    assert result is not None
    assert result["matches_last_attempt"] is True
    assert result["state"] == "completed"
    assert result["step_receipts"] == [{
        "step": "provider", "state": "complete", "elapsed_seconds": 0.1,
        "exit_code": 0,
    }]


def test_registered_step_distinguishes_process_success_from_source_failures(
    tmp_path: Path,
) -> None:
    run_id = "registered-daily-20260925T010203123456789Z"
    record = tmp_path / "registered_daily_runs.tsv"
    record.write_text(
        "timestamp_utc\trun_id\trun_mode\tcycle_id\tstatus\telapsed_sec\tfailed_steps\tlog_file\n"
        f"2026-09-25T01:23:38Z\t{run_id}\tonce\t1\tcompleted\t1295\t\t/private/secret.log\n"
    )
    directory = tmp_path / "registered_daily" / "step_receipts" / run_id
    directory.mkdir(parents=True)
    (directory / "yahoo_us_stocks_daily_update.json").write_text(json.dumps({
        "schema_version": 1,
        "run_id": run_id,
        "step": "yahoo_us_stocks_daily_update",
        "state": "complete",
        "elapsed_seconds": 1295,
        "exit_code": 0,
        "source_summary_status": "matched",
        "source_summary": {
            "asset_class": "us_stocks",
            "mode": "daily-update",
            "status_counts": {"repaired": 12208, "failed": 12, "lagging_skip": 615},
            "private_command": "--api-key=secret",
        },
    }))
    steps = audit._registered_step_receipts(
        record, scope="daily", run_id=run_id,
    )
    assert steps == [{
        "step": "yahoo_us_stocks_daily_update",
        "state": "complete",
        "elapsed_seconds": 1295.0,
        "exit_code": 0,
        "source_summary_status": "matched",
        "source_status_counts": {"repaired": 12208, "failed": 12, "lagging_skip": 615},
        "source_gap_status_counts": {"failed": 12, "lagging_skip": 615},
        "source_unresolved_count": 627,
    }]
    assert "secret" not in json.dumps(steps)
    summary = audit._registered_run_receipt(
        "stockagent-registered-data-daily.service", record,
        datetime(2026, 9, 25, 1, 2, 3, tzinfo=UTC),
    )
    assert summary["state"] == "completed"
    assert summary["source_data_health"] == "reported_gaps"
    assert summary["source_unresolved_count"] == 627
    assert summary["source_gap_status_counts"] == {"failed": 12, "lagging_skip": 615}

    bad = json.loads((directory / "yahoo_us_stocks_daily_update.json").read_text())
    bad["source_summary"]["status_counts"] = {"failed": True}
    (directory / "yahoo_us_stocks_daily_update.json").write_text(json.dumps(bad))
    rejected = audit._registered_step_receipts(record, scope="daily", run_id=run_id)
    assert rejected is not None
    assert rejected[0]["source_summary_status"] == "unavailable"
    assert "source_unresolved_count" not in rejected[0]


def test_business_receipt_findings_separate_exit_zero_from_data_gaps() -> None:
    rows = [
        {"unit": "daily.service", "result": "success", "business_receipt": {
            "matches_last_attempt": True, "state": "completed",
            "source_unresolved_count": 630, "source_data_health": "reported_gaps",
            "source_gap_status_counts": {"failed": 12, "lagging_skip": 618},
        }},
        {"unit": "backfill.service", "result": "success", "business_receipt": {
            "matches_last_attempt": True, "state": "completed_with_failures",
            "failed_steps": ["binance_perpetuals"],
        }},
        {"unit": "old.service", "result": "success", "business_receipt": {
            "matches_last_attempt": False, "state": "failed",
        }},
        {"unit": "clean.service", "result": "success", "business_receipt": {
            "matches_last_attempt": True, "state": "ok",
        }},
        {"unit": "malformed.service", "result": "success", "business_receipt": {
            "matches_last_attempt": True, "state": "completed",
            "failed_steps": [7], "source_unresolved_count": True,
        }},
    ]
    assert audit.business_receipt_findings(rows) == [
        {
            "unit": "daily.service", "receipt_state": "completed",
            "reasons": ["source_unresolved", "source_data_health"],
            "failed_steps": [], "source_unresolved_count": 630,
            "source_data_health": "reported_gaps",
            "source_gap_status_counts": {"failed": 12, "lagging_skip": 618},
        },
        {
            "unit": "backfill.service", "receipt_state": "completed_with_failures",
            "reasons": ["non_complete_state", "failed_steps"],
            "failed_steps": ["binance_perpetuals"],
            "source_unresolved_count": 0, "source_data_health": None,
        },
        {
            "unit": "malformed.service", "receipt_state": "completed",
            "reasons": ["invalid_failed_steps", "invalid_source_unresolved_count"],
            "failed_steps": [], "source_unresolved_count": 0,
            "source_data_health": None,
        },
    ]


def test_business_receipt_findings_reject_mismatched_gap_breakdown() -> None:
    rows = [{"unit": "daily.service", "business_receipt": {
        "matches_last_attempt": True,
        "state": "completed",
        "source_unresolved_count": 630,
        "source_gap_status_counts": {"failed": 12, "lagging_skip": 617},
    }}]

    assert audit.business_receipt_findings(rows) == [{
        "unit": "daily.service",
        "receipt_state": "completed",
        "reasons": ["source_unresolved", "invalid_source_gap_breakdown"],
        "failed_steps": [],
        "source_unresolved_count": 630,
        "source_data_health": None,
    }]


def test_product_probes_separate_http_from_payload_health(monkeypatch) -> None:
    class Response(io.BytesIO):
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            self.close()

    def fake_urlopen(url: str, *, timeout: int):
        assert timeout == 3
        if url.endswith("/data-monitor/api/summary"):
            raise OSError("fixture unavailable")
        health = "degraded" if "/tw-day-trade/" in url else "active"
        return Response(json.dumps({"health": health, "private": "omit"}).encode())

    monkeypatch.setattr(audit, "urlopen", fake_urlopen)
    result = audit.product_probe_snapshot()
    assert len(result["probes"]) == 6
    day_trade = next(
        row for row in result["probes"]
        if row["path"] == "/tw-day-trade/api/status"
    )
    assert day_trade["http_status"] == 200
    assert day_trade["health"] == "degraded"
    assert "private" not in json.dumps(result)
    monitor = result["probes"][-1]
    assert monitor["http_status"] is None
    assert monitor["health"] is None
    assert monitor["error"] == "OSError"


def test_matched_step_timings_report_only_recorded_steps() -> None:
    receipt = {
        "matches_last_attempt": True,
        "step_receipts_scope": "exact_matched_run",
        "step_receipts": [
            {"step": "fast", "elapsed_seconds": 0},
            {"step": "slow", "elapsed_seconds": 7.25, "state": "failed"},
        ],
    }
    assert audit._matched_step_timing_summary(receipt) == {
        "scope": "recorded_steps_only",
        "recorded_step_count": 2,
        "slowest_step": "slow",
        "slowest_step_seconds": 7.25,
    }


@pytest.mark.parametrize("receipt", [
    None, {},
    {"matches_last_attempt": False},
    {"matches_last_attempt": True, "step_receipts_scope": "latest"},
    {"matches_last_attempt": True, "step_receipts_scope": "exact_matched_run",
     "step_receipts": []},
])
def test_unproven_or_empty_step_timing_is_not_promoted(receipt) -> None:
    assert audit._matched_step_timing_summary(receipt) is None


@pytest.mark.parametrize("elapsed", [True, -1, float("nan"), float("inf"), "2", None])
def test_invalid_step_timing_is_not_promoted(elapsed) -> None:
    assert audit._matched_step_timing_summary({
        "matches_last_attempt": True,
        "step_receipts_scope": "exact_matched_run",
        "step_receipts": [{"step": "source", "elapsed_seconds": elapsed}],
    }) is None


@pytest.mark.parametrize("matched_steps", [False, True])
def test_report_distinguishes_completed_job_wall_from_daemon_resource(
    monkeypatch, matched_steps,
) -> None:
    calls: list[str] = []
    snapshots = iter(
        [
            {
                "monotonic": 10.0,
                "units": {
                    "stockagent-job.service": {
                        "Id": "stockagent-job.service",
                        "MainPID": "0",
                        "NRestarts": "0",
                        "InvocationID": "job",
                        "CPUUsageNSec": 1_000_000,
                        "ActiveState": "failed",
                        "SubState": "failed",
                        "Result": "exit-code",
                        "ExecMainStatus": 1,
                        "last_attempt_seconds": 4.0,
                    },
                    "stockagent-daemon.service": {
                        "Id": "stockagent-daemon.service",
                        "Transient": "yes",
                        "UnitFileState": "transient",
                        "MainPID": "27",
                        "NRestarts": "0",
                        "InvocationID": "daemon",
                        "CPUUsageNSec": 2_000_000,
                        "MemoryHigh": 8 * 2**30,
                        "MemoryMax": 16 * 2**30,
                        "MemoryHighEvents": 106709,
                        "MemoryMaxEvents": 0,
                        "MemoryOomEvents": 0,
                        "MemoryOomKillEvents": 0,
                        "ActiveState": "active",
                        "SubState": "running",
                        "Result": "success",
                        "ExecMainStatus": 0,
                        "last_attempt_seconds": None,
                    },
                },
            },
            {
                "monotonic": 12.0,
                "units": {
                    "stockagent-job.service": {
                        "Id": "stockagent-job.service",
                        "MainPID": "0",
                        "NRestarts": "0",
                        "InvocationID": "job",
                        "CPUUsageNSec": 1_000_000,
                        "ActiveState": "failed",
                        "SubState": "failed",
                        "Result": "exit-code",
                        "ExecMainStatus": 1,
                        "last_attempt_seconds": 4.0,
                    },
                    "stockagent-daemon.service": {
                        "Id": "stockagent-daemon.service",
                        "Transient": "yes",
                        "UnitFileState": "transient",
                        "MainPID": "27",
                        "NRestarts": "0",
                        "InvocationID": "daemon",
                        "CPUUsageNSec": 1_002_000_000,
                        "MemoryHigh": 8 * 2**30,
                        "MemoryMax": 16 * 2**30,
                        "MemoryHighEvents": 106709,
                        "MemoryMaxEvents": 0,
                        "MemoryOomEvents": 0,
                        "MemoryOomKillEvents": 0,
                        "ActiveState": "active",
                        "SubState": "running",
                        "Result": "success",
                        "ExecMainStatus": 0,
                        "last_attempt_seconds": None,
                    },
                },
            },
        ]
    )
    def snapshot():
        calls.append("service")
        return next(snapshots)

    def schedules():
        calls.append("schedule")
        return {
            "timers": {"stockagent-job.timer": {"ActiveState": "active"}},
            "paths": {},
        }

    def startup():
        calls.append("startup")
        return {"windows_task": None}

    monkeypatch.setattr(audit, "service_snapshot", snapshot)
    monkeypatch.setattr(
        audit,
        "schedule_snapshot",
        schedules,
    )
    monkeypatch.setattr(audit, "startup_snapshot", startup)
    monkeypatch.setattr(audit, "product_probe_snapshot", lambda: {"probes": []})
    monkeypatch.setattr(audit, "auxiliary_schedule_snapshot", lambda: {"cron": {}})
    monkeypatch.setattr(audit.time, "sleep", lambda _seconds: calls.append("sleep"))
    monkeypatch.setattr(audit, "_business_receipt", lambda unit, *_args: {
        "matches_last_attempt": True,
        "step_receipts_scope": "exact_matched_run",
        "step_receipts": [{"step": "download", "elapsed_seconds": 3}],
    } if matched_steps and unit == "stockagent-job.service" else None)

    report = audit.build_report(sample_seconds=2)
    assert calls[:5] == ["schedule", "startup", "service", "sleep", "service"]
    assert report["counts"] == {"services": 2, "timers": 1, "paths": 0}
    assert report["service_origins"] == {"installed": 1, "transient": 1}
    assert report["auxiliary_schedules"] == {"cron": {}}
    assert report["resource_pressure_findings"] == []
    rows = {row["unit"]: row for row in report["services"]}
    assert rows["stockagent-job.service"]["last_attempt_wall_seconds"] == 4.0
    assert rows["stockagent-job.service"]["cpu_cores_during_sample"] is None
    assert rows["stockagent-job.service"]["operation_latency_coverage"] == (
        "matched_run_step_timings" if matched_steps else "last_completed_process_wall_only"
    )
    assert rows["stockagent-job.service"]["operation_latency_summary"] == (
        {"scope": "recorded_steps_only", "recorded_step_count": 1,
         "slowest_step": "download", "slowest_step_seconds": 3}
        if matched_steps else None
    )
    assert rows["stockagent-job.service"]["timer"] == "stockagent-job.timer"
    assert rows["stockagent-daemon.service"]["last_attempt_wall_seconds"] is None
    assert rows["stockagent-daemon.service"]["cpu_cores_during_sample"] == 0.5
    assert rows["stockagent-daemon.service"]["service_origin"] == "transient"
    assert rows["stockagent-daemon.service"]["memory_high_bytes"] == 8 * 2**30
    assert rows["stockagent-daemon.service"]["memory_high_events_total"] == 106709
    assert rows["stockagent-daemon.service"]["memory_high_events_during_sample"] == 0
    assert rows["stockagent-job.service"]["memory_high_events_during_sample"] is None
    assert rows["stockagent-daemon.service"]["operation_latency_coverage"] == (
        "resource_sample_only"
    )
    assert rows["stockagent-daemon.service"]["operation_latency_summary"] is None
