from __future__ import annotations

from datetime import UTC, datetime
import io
from pathlib import Path
import json
import subprocess

from scripts import audit_service_latency_coverage as audit


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

    bad = json.loads((directory / "yahoo_us_stocks_daily_update.json").read_text())
    bad["source_summary"]["status_counts"] = {"failed": True}
    (directory / "yahoo_us_stocks_daily_update.json").write_text(json.dumps(bad))
    rejected = audit._registered_step_receipts(record, scope="daily", run_id=run_id)
    assert rejected is not None
    assert rejected[0]["source_summary_status"] == "unavailable"
    assert "source_unresolved_count" not in rejected[0]


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


def test_report_distinguishes_completed_job_wall_from_daemon_resource(
    monkeypatch,
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

    report = audit.build_report(sample_seconds=2)
    assert calls[:5] == ["schedule", "startup", "service", "sleep", "service"]
    assert report["counts"] == {"services": 2, "timers": 1, "paths": 0}
    assert report["service_origins"] == {"installed": 1, "transient": 1}
    assert report["auxiliary_schedules"] == {"cron": {}}
    rows = {row["unit"]: row for row in report["services"]}
    assert rows["stockagent-job.service"]["last_attempt_wall_seconds"] == 4.0
    assert rows["stockagent-job.service"]["cpu_cores_during_sample"] is None
    assert rows["stockagent-job.service"]["operation_latency_coverage"] == (
        "last_completed_process_wall_only"
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
