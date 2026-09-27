#!/usr/bin/env python3
"""Read-only, per-unit latency coverage and Windows/WSL startup evidence.

An active daemon has no meaningful "duration".  Keep its resource sample
separate from a completed job's last wall time and from an HTTP operation.
This audit never starts a broker client, service, scheduled task, or WSL VM.
"""

from __future__ import annotations

import argparse
import csv
from datetime import UTC, datetime, timedelta
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shutil
import socket
import subprocess
import sys
import time
from typing import Callable, Sequence
from urllib.error import HTTPError
from urllib.request import urlopen
from zoneinfo import ZoneInfo


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.benchmark_dashboard_latency import service_deltas, service_snapshot  # noqa: E402
from downloader.status import (  # noqa: E402
    count_reported_source_gaps,
    reported_source_gap_counts,
)


UNIT_PROPERTIES = (
    "Id",
    "ActiveState",
    "SubState",
    "UnitFileState",
    "Unit",
    "Triggers",
    "LastTriggerUSec",
    "NextElapseUSecRealtime",
    "NextElapseUSecMonotonic",
    "TimersMonotonic",
    "Result",
)
JOURNAL_START_MESSAGE_ID = "7d4958e842da4a758f6c1cdc7b36dcc5"
JOURNAL_RESOURCE_MESSAGE_ID = "ae8f7b866b0347b9af31fe1c80b127c0"
JOURNAL_SUCCESS_MESSAGE_ID = "7ad2d189f7e94e70a38c781354912448"
JOURNAL_FAILURE_MESSAGE_ID = "d9b373ed55a64feb8242e02dbe79a49c"
JOURNAL_PROCESS_EXIT_MESSAGE_ID = "98e322203f7a4ed290d09fe03c09fe15"
BUSINESS_RECEIPTS = {
    "stockagent-crypto-training-refresh.service": (
        REPO_ROOT / "artifacts/data_quality/crypto_training_latest/refresh_receipt.json"
    ),
    "stockagent-tw-public-cold-publish.service": (
        REPO_ROOT / "artifacts/data_refresh/tw_public/cold_publish/latest.json"
    ),
    "stockagent-openbb-l1-compaction.service": (
        REPO_ROOT / "data_openBB/_state/l1_compaction_attempt_latest.json"
    ),
}
OPENBB_RESOURCE_STAGES = (
    "before_stale_contract_audit", "after_stale_contract_audit",
    "after_unassigned_source_load", "after_segment_build", "after_query_view_publish",
)
OPENBB_RESOURCE_COUNTERS = (
    "cgroup_memory_high_events", "cgroup_memory_max_events",
    "cgroup_memory_oom_events", "cgroup_memory_oom_kill_events",
    "process_cpu_ns", "monotonic_ns",
)
OPENBB_RESOURCE_GAUGES = (
    "cgroup_memory_bytes", "cgroup_memory_peak_bytes", "cgroup_memory_anon_bytes",
    "cgroup_memory_file_bytes", "cgroup_swap_bytes", "process_swap_bytes",
)
OPENBB_PRESSURE_SIGNALS = {
    "cgroup_memory_high_events": "memory_high_events_observed",
    "cgroup_memory_max_events": "memory_max_events_observed",
    "cgroup_memory_oom_events": "oom_events_observed",
    "cgroup_memory_oom_kill_events": "oom_kill_events_observed",
    "cgroup_swap_bytes": "swap_observed",
    "process_swap_bytes": "process_swap_observed",
}
OPENBB_ATTEMPT_REASONS = (
    "archive_busy", "archive_resumed", "compactor_lock_busy", "exception",
    "interrupted", "attempt_receipt_error",
)
OPENBB_PUBLICATION_STAGES = (
    "not_attempted", "query_publish_started", "query_published",
    "status_publish_started", "status_published",
)
REGISTERED_RUN_RECORDS = {
    f"stockagent-registered-data-{scope}.service": (
        REPO_ROOT / f"artifacts/daily_downloader/registered_{scope}_runs.tsv"
    )
    for scope in ("daily", "intraday", "features", "backfill")
}
PRODUCT_STATUS_PATHS = (
    "/taifex/api/status",
    "/tw-day-trade/api/status",
    "/tw-overnight/api/status",
    "/shioaji/api/status",
    "/openbb/api/status",
    "/data-monitor/api/summary",
)
PUBLIC_IPV6_HOSTNAME = "penguin72487.ddnsgeek.com"
PUBLIC_IPV6_RECEIPT = (
    REPO_ROOT / "artifacts/benchmarks/dashboards/public-ipv6-audit.json"
)
WINDOWS_TASK_QUERY = r"""
$ErrorActionPreference = 'Stop'
$tasks = @(Get-ScheduledTask | Where-Object {
  $_.TaskName -like 'StockAgent*' -or $_.TaskName -like 'WSL Daily Backup*' -or
  @($_.Actions | Where-Object {
    [string]$_.Execute -match '(?i)stockagent' -or
    [string]$_.Arguments -match '(?i)stockagent'
  }).Count -gt 0
})
$rows = @($tasks | ForEach-Object {
  $task = $_
  $info = Get-ScheduledTaskInfo -TaskName $task.TaskName
  [pscustomobject]@{
    name = $task.TaskName
    state = [string]$task.State
    trigger_types = @($task.Triggers | ForEach-Object { $_.CimClass.CimClassName })
    last_run = [string]$info.LastRunTime
    next_run = [string]$info.NextRunTime
    last_task_result = [int64]$info.LastTaskResult
    multiple_instances_policy = [string]$task.Settings.MultipleInstances
    repetition_intervals = @($task.Triggers | ForEach-Object {
      if ($_.Repetition -and $_.Repetition.Interval) {
        [string]$_.Repetition.Interval
      }
    })
    action_executables = @($task.Actions | ForEach-Object { $_.Execute })
    match_basis = if ($task.TaskName -like 'StockAgent*' -or
      $task.TaskName -like 'WSL Daily Backup*') { 'name' } else { 'action_reference' }
    principal_logon_type = [string]$task.Principal.LogonType
  }
})
ConvertTo-Json -InputObject $rows -Compress -Depth 4
"""
WINDOWS_CADDY_PROCESS_QUERY = r"""
$rows = @(Get-CimInstance Win32_Process -Filter "Name = 'caddy.exe'" |
  ForEach-Object {
    $process = $_
    $parent = Get-CimInstance Win32_Process -Filter "ProcessId = $($process.ParentProcessId)"
    [pscustomobject]@{
      pid = [int]$process.ProcessId
      parent_pid = [int]$process.ParentProcessId
      parent_name = if ($parent) { [string]$parent.Name } else { $null }
      started = [string]$process.CreationDate
    }
  })
ConvertTo-Json -InputObject $rows -Compress -Depth 3
"""
WINDOWS_WSL_VM_QUERY = r"""
$ErrorActionPreference = 'Stop'
$hosts = @(Get-CimInstance Win32_Process -Filter "Name = 'wslhost.exe'" |
  ForEach-Object {
    $vmMatch = [regex]::Match(
      [string]$_.CommandLine,
      '--vm-id\s+\{?([0-9a-fA-F-]{36})\}?'
    )
    if ($vmMatch.Success) {
      [pscustomobject]@{
        vm_id = $vmMatch.Groups[1].Value.ToLowerInvariant()
        started = [datetime]$_.CreationDate
      }
    }
  })
$ids = @($hosts | Select-Object -ExpandProperty vm_id -Unique)
if ($ids.Count -ne 1) {
  ConvertTo-Json -InputObject ([pscustomobject]@{
    available = $false
    reason = if ($ids.Count -eq 0) { 'no_wslhost_vm' } else { 'multiple_wsl_vms' }
  }) -Compress
  exit
}
$first = $hosts | Sort-Object started | Select-Object -First 1
$vmId = [string]$ids[0]
$loaded = @(Get-WinEvent -FilterHashtable @{
  LogName = 'System'
  ProviderName = 'Microsoft-Windows-Hyper-V-VmSwitch'
  Id = 102
  StartTime = $first.started.AddMinutes(-10)
  EndTime = $first.started.AddMinutes(1)
} -ErrorAction SilentlyContinue | Where-Object {
  $_.Message -match [regex]::Escape($vmId)
} | Sort-Object TimeCreated)
$lastLoad = $loaded | Select-Object -Last 1
ConvertTo-Json -InputObject ([pscustomobject]@{
  available = $true
  wsl_host_first_at_utc = $first.started.ToUniversalTime().ToString('o')
  vm_network_driver_loaded_at_utc = if ($lastLoad) {
    $lastLoad.TimeCreated.ToUniversalTime().ToString('o')
  } else { $null }
  vm_driver_event_count = $loaded.Count
  process_count = $hosts.Count
}) -Compress
"""
_STOCKAGENT_UNIT_IN_CGROUP = re.compile(r"(?:^|/)(stockagent-[^/]+\.service)(?:/|$)")
_CRON_SOURCES = (
    Path("/etc/crontab"), Path("/etc/cron.d"), Path("/etc/cron.hourly"),
    Path("/etc/cron.daily"), Path("/etc/cron.weekly"),
    Path("/etc/cron.monthly"), Path("/var/spool/cron/crontabs"),
    Path("/var/spool/cron"),
)
_PROJECT_SCHEDULE_MARKERS = ("stockagent", "stock_agent")


def _proc_ppid(status: str) -> int | None:
    for line in status.splitlines():
        if line.startswith("PPid:"):
            try:
                return int(line.partition(":")[2].strip())
            except ValueError:
                return None
    return None


def _self_ancestors(proc_root: Path) -> set[int]:
    ancestors: set[int] = set()
    pid = os.getpid()
    while pid > 1 and pid not in ancestors:
        ancestors.add(pid)
        try:
            parent = _proc_ppid((proc_root / str(pid) / "status").read_text())
        except OSError:
            break
        if parent is None:
            break
        pid = parent
    return ancestors


def repository_process_snapshot(
    repo_root: Path = REPO_ROOT, proc_root: Path = Path("/proc"),
    *, exclude_pids: set[int] | None = None,
) -> dict[str, object]:
    """Find repo-context processes outside systemd without revealing argv.

    A matching cwd or absolute repo path in argv is only a *candidate* for
    StockAgent work. This does not discover every remote job, open fd, or
    relative-path process launched from another directory.
    """

    repo = str(repo_root.resolve())
    repo_bytes = repo.encode()
    excluded = _self_ancestors(proc_root) if exclude_pids is None else exclude_pids
    rows: list[dict[str, object]] = []
    inspected = 0
    for entry in proc_root.iterdir():
        if not entry.name.isdecimal():
            continue
        inspected += 1
        pid = int(entry.name)
        if pid in excluded:
            continue
        try:
            cwd = os.readlink(entry / "cwd")
        except OSError:
            cwd = ""
        cwd_match = cwd == repo or cwd.startswith(repo + "/")
        argv_match = False
        if not cwd_match:
            try:
                with (entry / "cmdline").open("rb") as stream:
                    argv_match = repo_bytes in stream.read(65_536)
            except OSError:
                pass
        if not cwd_match and not argv_match:
            continue
        try:
            status = (entry / "status").read_text()
            comm = (entry / "comm").read_text().strip()
            cgroup = (entry / "cgroup").read_text()
            statm = (entry / "statm").read_text().split()
        except (OSError, UnicodeError):
            continue
        unit_match = _STOCKAGENT_UNIT_IN_CGROUP.search(cgroup)
        try:
            rss_bytes = int(statm[1]) * os.sysconf("SC_PAGE_SIZE")
        except (IndexError, ValueError):
            rss_bytes = None
        rows.append({
            "pid": pid,
            "ppid": _proc_ppid(status),
            "comm": comm,
            "match_basis": "cwd" if cwd_match else "argv_path",
            "stockagent_unit": unit_match.group(1) if unit_match else None,
            "rss_bytes": rss_bytes,
        })
    rows.sort(key=lambda row: int(row["pid"]))
    unmanaged = [row for row in rows if row["stockagent_unit"] is None]
    return {
        "observed_at_utc": datetime.now(UTC).isoformat(),
        "scope": (
            "Current local /proc processes with cwd under the repository or its "
            "absolute path in the first 64 KiB of argv; excludes this audit's "
            "own process ancestry. Unmanaged candidates include interactive "
            "tools and are not necessarily background services. No argv text, "
            "open-fd search, remote host, or Windows process inventory."
        ),
        "inspected_proc_count": inspected,
        "matched_count": len(rows),
        "managed_unit_process_count": len(rows) - len(unmanaged),
        "unmanaged_candidate_count": len(unmanaged),
        "processes": rows,
    }


def _project_schedule_line_count(content: str) -> int:
    """Count active references without retaining cron commands or credentials."""

    return sum(
        any(marker in line.casefold() for marker in _PROJECT_SCHEDULE_MARKERS)
        for line in content.splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    )


def auxiliary_schedule_snapshot(
    cron_sources: Sequence[Path] = _CRON_SOURCES,
    *,
    runner: Callable[..., subprocess.CompletedProcess[str]] | None = None,
) -> dict[str, object]:
    """Discover non-systemd schedulers without publishing their command text.

    This covers local cron files, root's crontab, and root's user systemd
    manager. It is not an inventory of other Linux users or remote hosts.
    """

    run = runner or subprocess.run
    matched: list[dict[str, object]] = []
    files_scanned = 0
    unreadable = 0
    oversized = 0
    for source in cron_sources:
        try:
            candidates = sorted(source.iterdir()) if source.is_dir() else [source]
        except OSError:
            unreadable += 1
            continue
        for path in candidates:
            try:
                if path.is_symlink() or not path.is_file():
                    continue
                if path.stat().st_size > 1_048_576:
                    oversized += 1
                    continue
                content = path.read_text(encoding="utf-8", errors="replace")
            except OSError:
                unreadable += 1
                continue
            files_scanned += 1
            references = _project_schedule_line_count(content)
            if references or any(
                marker in path.name.casefold() for marker in _PROJECT_SCHEDULE_MARKERS
            ):
                matched.append({
                    "path": str(path),
                    "active_reference_lines": references,
                    "name_match": any(
                        marker in path.name.casefold()
                        for marker in _PROJECT_SCHEDULE_MARKERS
                    ),
                })
    root_crontab: dict[str, object] = {"state": "unavailable", "active_reference_lines": None}
    try:
        process = run(
            ["crontab", "-l"], capture_output=True, text=True, timeout=5,
            check=False,
        )
        if process.returncode == 0:
            root_crontab = {
                "state": "read", "active_reference_lines":
                _project_schedule_line_count(process.stdout),
            }
        elif "no crontab" in process.stderr.casefold():
            root_crontab = {"state": "empty", "active_reference_lines": 0}
    except (OSError, subprocess.TimeoutExpired):
        pass
    user_systemd: dict[str, object] = {"state": "unavailable", "matching_units": []}
    try:
        process = run(
            ["systemctl", "--user", "list-unit-files", "--all", "--no-legend", "--no-pager"],
            capture_output=True, text=True, timeout=5, check=False,
        )
        if process.returncode == 0:
            units = [line.split()[0] for line in process.stdout.splitlines() if line.split()]
            user_systemd = {
                "state": "read", "unit_count": len(units),
                "matching_units": [
                    unit for unit in units
                    if any(marker in unit.casefold() for marker in _PROJECT_SCHEDULE_MARKERS)
                ],
            }
    except (OSError, subprocess.TimeoutExpired):
        pass
    return {
        "scope": (
            "Current local cron files, root crontab, and root user systemd unit "
            "names only. Commands and arguments are not published; references "
            "through aliases, other user managers, or remote hosts may be missed."
        ),
        "cron": {
            "files_scanned": files_scanned,
            "unreadable": unreadable,
            "oversized": oversized,
            "matching_files": matched,
            "root_crontab": root_crontab,
        },
        "root_user_systemd": user_systemd,
    }


def _unit_blocks(text: str, *, suffix: str) -> dict[str, dict[str, str]]:
    units: dict[str, dict[str, str]] = {}
    for block in text.split("\n\n"):
        values: dict[str, str] = {}
        for line in block.splitlines():
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            if key == "TimersMonotonic" and key in values:
                values[key] += "\n" + value
            else:
                values[key] = value
        unit = values.get("Id", "")
        if unit.startswith("stockagent-") and unit.endswith(suffix):
            units[unit] = values
    return units


def schedule_snapshot() -> dict[str, dict[str, dict[str, str]]]:
    result = subprocess.run(
        [
            "systemctl",
            "show",
            "stockagent-*.timer",
            "stockagent-*.path",
            "--all",
            "--no-pager",
            "--property=" + ",".join(UNIT_PROPERTIES),
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=15,
    )
    return {
        "timers": _unit_blocks(result.stdout, suffix=".timer"),
        "paths": _unit_blocks(result.stdout, suffix=".path"),
    }


def timer_schedule_findings(
    timers: dict[str, dict[str, str]],
    service_rows: list[dict[str, object]],
) -> list[dict[str, str]]:
    """Expose recurring timers with no next trigger and no active target."""

    service_states = {
        str(row["unit"]): str(row.get("active_state")) for row in service_rows
    }
    findings = []
    for unit, timer in sorted(timers.items()):
        if (
            timer.get("UnitFileState") != "enabled"
            or timer.get("ActiveState") != "active"
            or timer.get("SubState") != "elapsed"
        ):
            continue
        monotonic_rules = timer.get("TimersMonotonic", "")
        if not any(
            rule in monotonic_rules
            for rule in ("OnUnitActiveUSec", "OnUnitInactiveUSec")
        ):
            continue
        target = timer.get("Unit", "")
        if service_states.get(target) in {"active", "activating"}:
            continue
        next_times = (
            timer.get("NextElapseUSecRealtime", ""),
            timer.get("NextElapseUSecMonotonic", ""),
        )
        if any(value not in {"", "0", "infinity", "n/a"} for value in next_times):
            continue
        findings.append(
            {
                "unit": unit,
                "target": target,
                "target_state": service_states.get(target, "unknown"),
                "finding": "recurring_timer_without_next_trigger",
            }
        )
    return findings


def _sha256(path: Path) -> str | None:
    if not path.is_file():
        return None
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _windows_caddy_install_root() -> Path | None:
    # Resolve Windows LocalAppData, not WSL HOME or the parent Windows profile.
    local_appdata = os.environ.get("LOCALAPPDATA", "")
    if not local_appdata and shutil.which("powershell.exe"):
        result = subprocess.run(
            [
                "powershell.exe",
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                "[Environment]::GetFolderPath('LocalApplicationData')",
            ],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
        local_appdata = (
            result.stdout.replace("\x00", "").strip() if result.returncode == 0 else ""
        )
    # Convert C:\... to its mounted WSL view without starting another VM.
    drive, separator, relative = local_appdata.partition(":")
    if separator and len(drive) == 1:
        local_appdata = str(
            Path("/mnt") / drive.lower() / relative.lstrip("\\/").replace("\\", "/")
        )
    return Path(local_appdata) / "StockAgentPublic" if local_appdata else None


def _installed_caddy_files(installed_root: Path | None = None) -> dict[str, object]:
    if installed_root is None:
        installed_root = _windows_caddy_install_root()
    pairs = {
        "launcher": (
            REPO_ROOT / "scripts/start_windows_public_caddy.ps1",
            installed_root / "start-caddy.ps1" if installed_root else None,
        ),
        "caddyfile": (
            REPO_ROOT / "deploy/caddy/Caddyfile.windows",
            installed_root / "Caddyfile" if installed_root else None,
        ),
    }
    files: dict[str, object] = {}
    for name, (source, installed) in pairs.items():
        source_hash = _sha256(source)
        installed_hash = _sha256(installed) if installed else None
        files[name] = {
            "source_sha256": source_hash,
            "installed_sha256": installed_hash,
            "exact_match": (
                source_hash == installed_hash
                if source_hash is not None and installed_hash is not None
                else None
            ),
        }
    return files


def _wsl_userspace_at_utc() -> datetime | None:
    """Read the current boot's systemd userspace marker without rebooting WSL."""

    try:
        process = subprocess.run(
            [
                "systemctl", "show", "--property=UserspaceTimestamp", "--value",
                "--no-pager",
            ],
            env={**os.environ, "TZ": "UTC", "LC_ALL": "C"},
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if process.returncode != 0:
        return None
    try:
        return datetime.strptime(
            process.stdout.strip(), "%a %Y-%m-%d %H:%M:%S UTC"
        ).replace(tzinfo=UTC)
    except ValueError:
        return None


def _gateway_recovery_episodes(
    log_path: Path, *, now: datetime | None = None,
    current_userspace_at_utc: datetime | None = None,
    current_vm_evidence: dict[str, object] | None = None,
) -> dict[str, object]:
    """Measure observed Caddy-to-WSL backend recoveries, not cold boot time."""

    result: dict[str, object] = {
        "available": False,
        "last_episode": None,
        "slowest_7d_episode": None,
        "completed_7d_count": 0,
        "latest_wsl_dispatch_completion": None,
        "wsl_dispatch_completions_7d": 0,
        "ongoing_since": None,
    }
    try:
        lines = log_path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError):
        return result
    result["available"] = True
    now = now or datetime.now(UTC)
    cutoff = now - timedelta(days=7)
    active: dict[str, object] | None = None
    episodes: list[dict[str, object]] = []
    for line in lines:
        stamp, separator, message = line.partition(" ")
        if not separator:
            continue
        try:
            observed = datetime.fromisoformat(stamp).astimezone(UTC)
        except ValueError:
            continue
        if "WSL gateway dispatch completed" in message:
            exit_match = re.search(r"\bexit_code=(-?\d+)\b", message)
            elapsed_match = re.search(r"\belapsed_seconds=(\d+(?:\.\d+)?)\b", message)
            verb_match = re.search(r"\bverb=(start|restart)\b", message)
            reason_match = re.search(
                r"\breason=(supervisor_start|backend_unhealthy|backend_sustained_unresponsive)\b",
                message,
            )
            if exit_match and elapsed_match and verb_match and reason_match:
                result["latest_wsl_dispatch_completion"] = {
                    "observed_at_utc": observed.isoformat(),
                    "exit_code": int(exit_match.group(1)),
                    "elapsed_seconds": float(elapsed_match.group(1)),
                    "verb": verb_match.group(1),
                    "reason": reason_match.group(1),
                    "boundary": "wsl.exe command duration, not WSL VM boot or gateway readiness",
                }
                if observed >= cutoff:
                    result["wsl_dispatch_completions_7d"] = (
                        int(result["wsl_dispatch_completions_7d"]) + 1
                    )
        if "gateway backend healthy=False" in message:
            active = {
                "unhealthy_at_utc": observed.isoformat(),
                "first_wsl_dispatch_at_utc": None,
                "wsl_dispatch_count": 0,
                "gateway_restart_dispatch_count": 0,
            }
        elif active is not None and "WSL gateway " in message and " dispatched " in message:
            if active["first_wsl_dispatch_at_utc"] is None:
                active["first_wsl_dispatch_at_utc"] = observed.isoformat()
            active["wsl_dispatch_count"] = int(active["wsl_dispatch_count"]) + 1
            if "WSL gateway restart dispatched" in message:
                active["gateway_restart_dispatch_count"] = (
                    int(active["gateway_restart_dispatch_count"]) + 1
                )
        elif active is not None and "gateway backend healthy=True" in message:
            started = datetime.fromisoformat(str(active["unhealthy_at_utc"]))
            if observed >= started:
                active["healthy_at_utc"] = observed.isoformat()
                active["recovery_seconds"] = round((observed - started).total_seconds(), 3)
                first_dispatch = active["first_wsl_dispatch_at_utc"]
                active["dispatch_to_healthy_seconds"] = (
                    round((observed - datetime.fromisoformat(str(first_dispatch))).total_seconds(), 3)
                    if first_dispatch else None
                )
                # Only the episode containing the *current* WSL boot may be
                # split using its userspace marker; older boots are unknown.
                if (
                    first_dispatch and current_userspace_at_utc is not None
                    and datetime.fromisoformat(str(first_dispatch))
                    <= current_userspace_at_utc <= observed
                ):
                    active["current_userspace_at_utc"] = current_userspace_at_utc.isoformat()
                    active["dispatch_to_userspace_seconds"] = round(
                        (current_userspace_at_utc - datetime.fromisoformat(str(first_dispatch))).total_seconds(), 3
                    )
                    active["userspace_to_healthy_seconds"] = round(
                        (observed - current_userspace_at_utc).total_seconds(), 3
                    )
                    if current_vm_evidence and current_vm_evidence.get("available") is True:
                        host_stamp = current_vm_evidence.get("wsl_host_first_at_utc")
                        driver_stamp = current_vm_evidence.get("vm_network_driver_loaded_at_utc")
                        try:
                            host_at = datetime.fromisoformat(str(host_stamp))
                            driver_at = datetime.fromisoformat(str(driver_stamp))
                            if host_at.tzinfo is None or driver_at.tzinfo is None:
                                raise ValueError("Windows VM timestamps must include timezone")
                            host_at = host_at.astimezone(UTC)
                            driver_at = driver_at.astimezone(UTC)
                        except ValueError:
                            pass
                        else:
                            first_at = datetime.fromisoformat(str(first_dispatch))
                            # systemctl renders UserspaceTimestamp to whole
                            # seconds; Windows process creation retains
                            # fractions, so same-second start may appear up to
                            # one second after that rounded marker.
                            host_within_userspace_precision = (
                                host_at <= current_userspace_at_utc + timedelta(seconds=1)
                            )
                            if (
                                first_at <= driver_at <= current_userspace_at_utc
                                and driver_at <= host_at <= observed
                                and host_within_userspace_precision
                            ):
                                active["vm_network_driver_loaded_at_utc"] = driver_at.isoformat()
                                active["wsl_host_first_at_utc"] = host_at.isoformat()
                                active["dispatch_to_vm_driver_seconds"] = round(
                                    (driver_at - first_at).total_seconds(), 3
                                )
                                active["vm_driver_to_userspace_seconds"] = round(
                                    (current_userspace_at_utc - driver_at).total_seconds(), 3
                                )
                episodes.append(active)
            active = None
    if active is not None:
        result["ongoing_since"] = active["unhealthy_at_utc"]
    if episodes:
        result["last_episode"] = episodes[-1]
        recent = [
            episode for episode in episodes
            if datetime.fromisoformat(str(episode["healthy_at_utc"])) >= cutoff
        ]
        result["completed_7d_count"] = len(recent)
        if recent:
            result["slowest_7d_episode"] = max(
                recent, key=lambda episode: float(episode["recovery_seconds"])
            )
    return result


def _windows_caddy_processes() -> list[dict[str, object]] | None:
    process = subprocess.run(
        ["powershell.exe", "-NoProfile", "-NonInteractive", "-Command", WINDOWS_CADDY_PROCESS_QUERY],
        capture_output=True, text=True, timeout=15, check=False,
    )
    if process.returncode != 0:
        return None
    try:
        parsed = json.loads(process.stdout.replace("\x00", ""))
    except ValueError:
        return None
    if isinstance(parsed, dict):
        parsed = [parsed]
    return [row for row in parsed if isinstance(row, dict)] if isinstance(parsed, list) else None


def _windows_wsl_vm_evidence() -> dict[str, object] | None:
    """Observe the current VM's host process and matching vSwitch driver event."""

    try:
        process = subprocess.run(
            ["powershell.exe", "-NoProfile", "-NonInteractive", "-Command", WINDOWS_WSL_VM_QUERY],
            capture_output=True, text=True, timeout=15, check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if process.returncode != 0:
        return None
    try:
        parsed = json.loads(process.stdout.replace("\x00", ""))
    except ValueError:
        return None
    return parsed if isinstance(parsed, dict) else None


def _windows_task_result_context(task: dict[str, object]) -> str | None:
    """A running supervisor's last trigger result is not process liveness proof."""

    last_result = task.get("last_task_result")
    if task.get("state") != "Running" or type(last_result) is not int or last_result == 0:
        return None
    if task.get("multiple_instances_policy") == "IgnoreNew" and bool(
        task.get("repetition_intervals")
    ):
        return "ambiguous_nonzero_with_ignore_new_repetition"
    return "unresolved_nonzero_while_running"


def _resolve_public_ipv6(hostname: str) -> list[str]:
    return sorted({
        str(item[4][0])
        for item in socket.getaddrinfo(hostname, 443, socket.AF_INET6, socket.SOCK_STREAM)
    })


def _public_ipv6_external_evidence(
    receipt_path: Path = PUBLIC_IPV6_RECEIPT,
    *,
    now: datetime | None = None,
    resolve_ipv6: Callable[[str], list[str]] = _resolve_public_ipv6,
) -> dict[str, object]:
    """Use only a recent independent probe; never infer WAN state from WSL curl."""

    result: dict[str, object] = {
        "state": "missing_receipt",
        "receipt_path": str(receipt_path),
        "claim_boundary": (
            "A recent external probe response and matching current AAAA are evidence "
            "only for the observed interval, not continuous IPv6 availability. "
            "WSL same-host curl is not an external WAN test."
        ),
    }
    try:
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return result
    except (OSError, ValueError):
        result["state"] = "invalid_receipt"
        return result
    if not isinstance(receipt, dict) or receipt.get("hostname") != PUBLIC_IPV6_HOSTNAME:
        result["state"] = "invalid_receipt"
        return result
    try:
        completed = datetime.fromisoformat(str(receipt["completed_at_utc"]))
        if completed.tzinfo is None:
            raise ValueError("naive timestamp")
        age_seconds = ((now or datetime.now(UTC)) - completed).total_seconds()
    except (KeyError, TypeError, ValueError):
        result["state"] = "invalid_receipt"
        return result
    result["completed_at_utc"] = completed.isoformat()
    result["age_seconds"] = round(age_seconds, 3)
    if age_seconds < -60:
        result["state"] = "future_receipt"
        return result
    if age_seconds > 3600:
        result["state"] = "stale_receipt"
        return result
    if receipt.get("state") == "inconclusive_timeout":
        result["state"] = "external_probe_inconclusive"
        return result
    probe = receipt.get("external_probe")
    dns = receipt.get("dns")
    if not isinstance(probe, dict) or not isinstance(dns, dict):
        result["state"] = "invalid_receipt"
        return result
    if probe.get("probe_request_issued") is not True:
        result["state"] = "probe_age_unproven"
        return result
    try:
        current_aaaa = resolve_ipv6(PUBLIC_IPV6_HOSTNAME)
    except OSError:
        result["state"] = "dns_unavailable"
        return result
    result["current_aaaa"] = current_aaaa
    if not current_aaaa or current_aaaa != dns.get("aaaa"):
        result["state"] = "dns_changed_since_probe"
        return result
    ipv6_probe = probe.get("ipv6")
    if not isinstance(ipv6_probe, dict):
        result["state"] = "invalid_receipt"
        return result
    if ipv6_probe.get("done") is not True:
        result["state"] = "external_probe_inconclusive"
        return result
    result["state"] = (
        "external_pass_observed" if receipt.get("passed") is True
        and ipv6_probe.get("success") is True
        else "external_failure_observed"
    )
    return result


def startup_snapshot() -> dict[str, object]:
    installed_root = _windows_caddy_install_root()
    userspace_at_utc = _wsl_userspace_at_utc()
    vm_evidence = _windows_wsl_vm_evidence() if shutil.which("powershell.exe") else None
    result: dict[str, object] = {
        "claim_boundary": (
            "Read-only current configuration and liveness; no reboot, WSL shutdown, "
            "cold-start latency, or pre-login recovery was exercised."
        ),
        "windows_task": None,
        "windows_tasks": [],
        "windows_caddy_processes": None,
        "windows_wsl_vm_evidence": vm_evidence,
        "installed_caddy_files": _installed_caddy_files(installed_root),
        "gateway_recovery": _gateway_recovery_episodes(
            installed_root / "logs" / "startup.log",
            current_userspace_at_utc=userspace_at_utc,
            current_vm_evidence=vm_evidence,
        ) if installed_root else None,
        "wsl_systemd_enabled": None,
        "wsl_boot_id": None,
        "wsl_userspace_at_utc": (
            userspace_at_utc.isoformat() if userspace_at_utc else None
        ),
        "systemd_state": None,
        "gateway_health_http": None,
        "public_ipv6_external": _public_ipv6_external_evidence(),
    }
    try:
        config = Path("/etc/wsl.conf").read_text(encoding="utf-8")
        result["wsl_systemd_enabled"] = any(
            line.strip().casefold() == "systemd=true" for line in config.splitlines()
        )
        result["wsl_boot_id"] = (
            Path("/proc/sys/kernel/random/boot_id").read_text().strip()
        )
    except OSError:
        pass
    system = subprocess.run(
        ["systemctl", "is-system-running"],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    result["systemd_state"] = system.stdout.strip() or None
    try:
        with urlopen("http://127.0.0.1:8770/healthz", timeout=3) as response:
            result["gateway_health_http"] = response.status
    except OSError as error:
        result["gateway_health_error"] = type(error).__name__
    if shutil.which("powershell.exe"):
        result["windows_caddy_processes"] = _windows_caddy_processes()
        task = subprocess.run(
            [
                "powershell.exe",
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                WINDOWS_TASK_QUERY,
            ],
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )
        try:
            if task.returncode == 0:
                parsed = json.loads(task.stdout.replace("\x00", ""))
                result["windows_tasks"] = (
                    [item for item in parsed if isinstance(item, dict)]
                    if isinstance(parsed, list)
                    else [parsed]
                    if isinstance(parsed, dict)
                    else []
                )
                result["windows_task"] = next(
                    (
                        item
                        for item in result["windows_tasks"]
                        if item.get("name") == "StockAgent Public Caddy"
                    ),
                    None,
                )
                caddy_task = result["windows_task"]
                if isinstance(caddy_task, dict):
                    last_result = caddy_task.get("last_task_result")
                    result["windows_task_result_hex"] = (
                        f"0x{last_result & 0xffffffff:08X}"
                        if type(last_result) is int else None
                    )
                    result["windows_task_nonzero_while_running"] = bool(
                        caddy_task.get("state") == "Running"
                        and type(last_result) is int and last_result != 0
                    )
                    context = _windows_task_result_context(caddy_task)
                    if context is not None:
                        result["windows_task_result_context"] = context
            else:
                result["windows_task_error"] = "scheduled_task_query_failed"
        except ValueError:
            result["windows_task_error"] = "scheduled_task_json_invalid"
    else:
        result["windows_task_error"] = "powershell_unavailable"
    result["observed_at_utc"] = datetime.now(UTC).isoformat()
    return result


def product_probe_snapshot() -> dict[str, object]:
    """Measure local public projections without equating HTTP with data health."""

    rows: list[dict[str, object]] = []
    for path in PRODUCT_STATUS_PATHS:
        started = time.perf_counter()
        row: dict[str, object] = {"path": path, "http_status": None, "health": None}
        try:
            with urlopen(f"http://127.0.0.1:8770{path}", timeout=3) as response:
                row["http_status"] = response.status
                body = response.read(1_000_001)
            if len(body) > 1_000_000:
                row["error"] = "response_too_large"
            else:
                payload = json.loads(body)
                if isinstance(payload, dict):
                    health = payload.get("health")
                    if isinstance(health, str) and re.fullmatch(r"[a-z_]{1,40}", health):
                        row["health"] = health
                    row["response_valid"] = True
                else:
                    row["error"] = "response_not_object"
        except HTTPError as error:
            row["http_status"] = error.code
            row["error"] = "http_error"
        except (OSError, ValueError, UnicodeError) as error:
            row["error"] = type(error).__name__
        row["elapsed_ms"] = round((time.perf_counter() - started) * 1000, 3)
        rows.append(row)
    return {
        "claim_boundary": (
            "Six localhost public-projection GETs, including response transfer; "
            "not external-network, browser-paint, uncached backend, broker, "
            "source-completeness, or order-execution latency."
        ),
        "probes": rows,
    }


def pressure_snapshot(root: Path = Path("/proc/pressure")) -> dict[str, object]:
    """Observe host-wide contention; PSI is not attributable to one unit."""

    pressure: dict[str, object] = {}
    for resource in ("cpu", "memory", "io"):
        try:
            lines = (root / resource).read_text(encoding="utf-8").splitlines()
        except OSError:
            pressure[resource] = None
            continue
        rows: dict[str, dict[str, float | int]] = {}
        for line in lines:
            kind, *pairs = line.split()
            if kind not in {"some", "full"}:
                continue
            values: dict[str, float | int] = {}
            for pair in pairs:
                key, separator, raw = pair.partition("=")
                if not separator:
                    continue
                try:
                    values[key] = int(raw) if key == "total" else float(raw)
                except ValueError:
                    continue
            rows[kind] = values
        pressure[resource] = rows
    return pressure


def _unit_start_utc(
    props: dict[str, object], snapshot: dict[str, object],
    journal_attempt: dict[str, object] | None,
) -> datetime | None:
    observed: datetime | None = None
    observed_mono: float | None = None
    try:
        parsed = datetime.fromisoformat(str(snapshot["observed_at"]).replace("Z", "+00:00"))
        if parsed.tzinfo is not None:
            observed = parsed
            observed_mono = float(snapshot["monotonic"])
    except (OSError, UnicodeError, ValueError, TypeError, KeyError, AttributeError):
        pass
    start_mono_us = props.get("ExecMainStartTimestampMonotonic")
    if (
        observed is not None
        and observed_mono is not None
        and type(start_mono_us) is int
        and 0 < start_mono_us / 1e6 <= observed_mono
    ):
        return observed.astimezone(UTC) - timedelta(
            seconds=observed_mono - start_mono_us / 1e6
        )
    if journal_attempt and journal_attempt.get("last_attempt_started_at_utc"):
        try:
            parsed = datetime.fromisoformat(
                str(journal_attempt["last_attempt_started_at_utc"])
            )
            return parsed.astimezone(UTC) if parsed.tzinfo else None
        except ValueError:
            return None
    return None


def _registered_run_started_utc(scope: str, run_id: str) -> datetime | None:
    """Parse both historical second IDs and collision-resistant nanosecond IDs."""

    match = re.fullmatch(
        rf"registered-{re.escape(scope)}-(\d{{8}}T\d{{6}})(\d{{9}})?Z",
        run_id,
    )
    if match is None:
        return None
    try:
        started = datetime.strptime(match.group(1), "%Y%m%dT%H%M%S").replace(tzinfo=UTC)
    except ValueError:
        return None
    fraction = match.group(2)
    return started + timedelta(microseconds=int(fraction[:6])) if fraction else started


def _registered_run_receipt(
    unit: str, path: Path, systemd_start: datetime | None,
) -> dict[str, object]:
    """Match a bounded per-cycle TSV to its service start; never expose log paths."""

    result: dict[str, object] = {"path": str(path), "matches_last_attempt": False}
    if systemd_start is None or path.is_symlink():
        return result
    try:
        if path.stat().st_size > 5_000_000:
            return result
        with path.open(encoding="utf-8", newline="") as handle:
            records = list(csv.DictReader(handle, delimiter="\t"))
    except (OSError, UnicodeError, csv.Error):
        return result
    scope = unit.removeprefix("stockagent-registered-data-").removesuffix(".service")
    matches: list[tuple[datetime, dict[str, str]]] = []
    for row in records:
        run_id = str(row.get("run_id") or "")
        started = _registered_run_started_utc(scope, run_id)
        if started is None:
            continue
        try:
            finished = datetime.fromisoformat(str(row["timestamp_utc"]).replace("Z", "+00:00"))
            elapsed = int(row["elapsed_sec"])
        except (KeyError, TypeError, ValueError):
            continue
        if (
            finished.tzinfo is None
            or abs((started - systemd_start).total_seconds()) > 10
            or elapsed < 0
            or abs((finished - started).total_seconds() - elapsed) > 60
        ):
            continue
        matches.append((finished.astimezone(UTC), row))
    if not matches:
        return result
    finished, row = max(matches, key=lambda item: item[0])
    state = str(row.get("status") or "")
    if not re.fullmatch(r"[a-z_]{1,64}", state):
        return result
    failed_steps = [
        value for value in str(row.get("failed_steps") or "").split(",")
        if re.fullmatch(r"[a-z0-9_:-]{1,80}", value)
    ][:32]
    result.update({
        "matches_last_attempt": True,
        "state": state,
        "failed_steps": failed_steps,
        "started_at_utc": systemd_start.isoformat(),
        "finished_at_utc": finished.isoformat(),
        "elapsed_seconds": int(row["elapsed_sec"]),
    })
    run_id = str(row["run_id"])
    steps = _registered_step_receipts(path, scope=scope, run_id=run_id)
    if steps is not None:
        result["step_receipts_scope"] = "exact_matched_run"
        result["step_receipts"] = steps
        reported = [
            step for step in steps
            if step.get("source_summary_status") == "matched"
            and type(step.get("source_unresolved_count")) is int
        ]
        unknown = any(
            step.get("source_summary_status") == "unavailable"
            for step in steps
        )
        if reported:
            unresolved = sum(int(step["source_unresolved_count"]) for step in reported)
            result["source_unresolved_count"] = unresolved
            gap_counts: dict[str, int] = {}
            for step in reported:
                for status, count in step["source_gap_status_counts"].items():
                    gap_counts[status] = gap_counts.get(status, 0) + count
            if sum(gap_counts.values()) == unresolved:
                result["source_gap_status_counts"] = dict(sorted(gap_counts.items()))
            else:
                result["source_gap_breakdown_status"] = "invalid"
            if unresolved:
                result["source_data_health"] = "reported_gaps"
            elif unknown:
                result["source_data_health"] = "unverified"
            else:
                result["source_data_health"] = "no_counted_gaps"
        elif unknown:
            result["source_data_health"] = "unverified"
    return result


def _registered_step_receipts(
    record_path: Path, *, scope: str, run_id: str,
) -> list[dict[str, object]] | None:
    """Read bounded stage timing for one proved run, never mutable latest links."""

    if _registered_run_started_utc(scope, run_id) is None:
        return None
    directory = (
        record_path.parent / f"registered_{scope}" / "step_receipts" / run_id
    )
    if directory.is_symlink() or not directory.is_dir():
        return None
    try:
        entries = list(directory.iterdir())
    except OSError:
        return None
    if len(entries) > 128:
        return None
    steps: list[dict[str, object]] = []
    for entry in entries:
        if (
            entry.suffix != ".json"
            or not re.fullmatch(r"[a-z0-9_]{1,80}", entry.stem)
            or entry.is_symlink()
        ):
            continue
        try:
            if entry.stat().st_size > 16_384:
                continue
            payload = json.loads(entry.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, ValueError):
            continue
        if (
            not isinstance(payload, dict)
            or payload.get("schema_version") != 1
            or payload.get("run_id") != run_id
            or payload.get("step") != entry.stem
        ):
            continue
        state = payload.get("state")
        elapsed = payload.get("elapsed_seconds")
        if (
            not isinstance(state, str)
            or not re.fullmatch(r"[a-z_]{1,64}", state)
            or type(elapsed) not in (int, float)
            or not math.isfinite(elapsed)
            or elapsed < 0
        ):
            continue
        item: dict[str, object] = {
            "step": entry.stem,
            "state": state,
            "elapsed_seconds": float(elapsed),
            "exit_code": payload.get("exit_code")
            if type(payload.get("exit_code")) is int else None,
        }
        summary_status = payload.get("source_summary_status")
        if summary_status in {"matched", "unavailable"}:
            item["source_summary_status"] = "unavailable"
            source = payload.get("source_summary")
            if summary_status == "matched" and isinstance(source, dict):
                counts = source.get("status_counts")
                yahoo_match = re.fullmatch(
                    r"yahoo_(?P<asset>[a-z0-9_]+)_(?P<kind>daily_update|history_head_repair|1m_update)",
                    entry.stem,
                )
                source_modes = {
                    "daily_update": "daily-update",
                    "history_head_repair": "repair",
                    "1m_update": "incremental",
                }
                if (
                    yahoo_match is not None
                    and source.get("asset_class") == yahoo_match.group("asset")
                    and source.get("mode") == source_modes[yahoo_match.group("kind")]
                    and isinstance(counts, dict)
                    and len(counts) <= 64
                    and all(
                        isinstance(key, str)
                        and re.fullmatch(r"[a-z0-9_]{1,64}", key)
                        and type(value) is int
                        and 0 <= value <= 1_000_000_000
                        for key, value in counts.items()
                    )
                ):
                    item["source_summary_status"] = "matched"
                    item["source_status_counts"] = counts
                    item["source_gap_status_counts"] = reported_source_gap_counts(counts)
                    item["source_unresolved_count"] = count_reported_source_gaps(counts)
                    source_elapsed = source.get("elapsed_seconds")
                    if (
                        type(source_elapsed) in (int, float)
                        and math.isfinite(source_elapsed)
                        and 0 <= source_elapsed <= elapsed + 60
                    ):
                        item["source_elapsed_seconds"] = float(source_elapsed)
        steps.append(item)
    return sorted(steps, key=lambda item: (-item["elapsed_seconds"], item["step"]))


def _crypto_refresh_details(
    payload: dict[str, object], started: datetime,
) -> dict[str, object]:
    """Project v4+ receipt contracts without copying commands or source secrets.

    Call only after binding the receipt to a systemd invocation. Process timing
    is not proof that its source audit, publication, or report was accepted.
    """

    version = payload.get("refresh_contract_version")
    if type(version) is not int or version < 4:
        return {}
    core_states = {
        "pending", "running", "deferred", "failed", "ready", "published",
        "publication_unknown", "needs_reconciliation",
    }
    publication_states = {"not_attempted", "attempting", "published", "outcome_unknown"}
    report_states = {
        "running", "completed", "failed", "deferred_raw_writer", "deferred_source_changed",
    }

    def safe_state(value: object, allowed: set[str]) -> str:
        return value if isinstance(value, str) and value in allowed else "unknown"

    details: dict[str, object] = {
        "core_state": safe_state(payload.get("core_state"), core_states),
        "publication_state": safe_state(payload.get("publication_state"), publication_states),
        "reports": [],
    }
    if version >= 5:
        details["transport_state"] = safe_state(payload.get("transport_state"), {
            "not_attempted", "not_required", "scanning", "pending",
            "request_acknowledged", "no_pending",
        })
        lease_elapsed = payload.get("source_lease_elapsed_seconds")
        if (
            type(lease_elapsed) in (int, float)
            and math.isfinite(lease_elapsed) and lease_elapsed >= 0
        ):
            details["source_lease_elapsed_seconds"] = float(lease_elapsed)
    reports = payload.get("reports")
    if isinstance(reports, list) and len(reports) <= 3:
        names: set[str] = set()
        for report in reports:
            if (
                not isinstance(report, dict)
                or not isinstance(report.get("name"), str)
                or report["name"] not in {"coverage", "okx", "binance"}
                or report["name"] in names
            ):
                details["reports"] = []
                break
            names.add(report["name"])
            details["reports"].append({
                "name": report["name"],
                "state": safe_state(report.get("state"), report_states),
            })

    attempts = payload.get("step_attempts")
    if (
        not isinstance(attempts, list)
        or not attempts
        or len(attempts) > 128
    ):
        return details
    try:
        finished = datetime.fromisoformat(str(payload.get("finished_at_utc")).replace("Z", "+00:00"))
        if finished.tzinfo is None or finished < started:
            return details
    except (ValueError, TypeError):
        return details
    step_names = {
        "download_bybit_funding_history.py": "bybit_funding_history",
        "materialize_bybit_perpetual_daily.py": "bybit_daily_materialization",
        "build_bybit_venue_daily_features.py": "bybit_venue_features",
        "report_crypto_training_features.py": "bybit_training_report",
        "publish_data_releases.py": "bybit_cold_publish",
        "audit_crypto_historical_coverage.py": "crypto_coverage_report",
    }
    steps = []
    for attempt in attempts:
        if not isinstance(attempt, dict):
            return details
        command = attempt.get("command")
        elapsed = attempt.get("elapsed_seconds")
        status = attempt.get("status")
        if (
            not isinstance(command, list)
            or not 2 <= len(command) <= 64
            or not all(isinstance(arg, str) and arg for arg in command)
            or type(elapsed) not in (int, float)
            or not math.isfinite(elapsed)
            or elapsed < 0
            or not isinstance(status, str)
            or status not in {"process_completed", "failed", "timed_out"}
        ):
            return details
        basename = Path(command[1]).name
        name = step_names.get(basename)
        if basename == "publish_data_releases.py" and command[2:4] != ["publish", "bybit"]:
            return details
        if basename == "report_crypto_venue_1m_features.py":
            if len(command) < 3 or command[2] not in {"okx", "binance"}:
                return details
            name = f"{command[2]}_feature_report"
        if basename == "retry_packed_syncthing_scans.py":
            if (
                version < 5 or len(command) not in (6, 7)
                or (len(command) == 7 and command[-1] != "--batch-object-paths")
                or command[2:5] != ["--dataset", "bybit", "--receipt"]
                or command[5].startswith("-")
            ):
                return details
            name = "bybit_transport_scan"
        if name is None:
            return details
        if (
            ("reused_previous_started_at_utc" in payload or payload.get("no_op") is True)
            and name != "bybit_transport_scan"
        ):
            # A reused core may still have one freshly measured transport
            # retry. Never adopt prior core/report measurements into this run.
            return details
        try:
            step_started = datetime.fromisoformat(str(attempt.get("started_at_utc")).replace("Z", "+00:00"))
            if (
                step_started.tzinfo is None
                or not started <= step_started <= finished
                # elapsed is rounded to milliseconds; wall-clock collection
                # must still cover the measured process, including failures.
                or elapsed > (finished - step_started).total_seconds() + 1
            ):
                return details
        except (ValueError, TypeError):
            return details
        steps.append({"step": name, "state": status, "elapsed_seconds": float(elapsed)})
    details["step_receipts"] = steps
    details["step_receipts_scope"] = "exact_matched_run"
    return details


def _resource_integer(value: object) -> int | None:
    return value if type(value) is int and 0 <= value < 2**63 else None


def _openbb_resources_completed(
    props: dict[str, object], snapshot: dict[str, object],
    journal_attempt: dict[str, object] | None, finished: datetime | None,
) -> bool:
    """Use the same authority as the matched start, never an older terminal."""

    if props.get("ActiveState") in {"active", "activating", "deactivating"}:
        return False
    if finished is None or finished.tzinfo is None:
        return False
    try:
        observed = datetime.fromisoformat(str(snapshot["observed_at"]).replace("Z", "+00:00"))
        if observed.tzinfo is None or finished > observed:
            return False
        current_start = _unit_start_utc(props, snapshot, None)
        if current_start is not None:
            start = _resource_integer(props.get("ExecMainStartTimestampMonotonic"))
            end = _resource_integer(props.get("ExecMainExitTimestampMonotonic"))
            if (
                props.get("ActiveState") not in {"inactive", "failed"}
                or start is None or end is None
                or not start <= end <= float(snapshot["monotonic"]) * 1e6
            ):
                return False
            terminal = current_start + timedelta(microseconds=end - start)
        else:
            terminal = datetime.fromisoformat(str(
                (journal_attempt or {}).get("last_attempt_completed_at_utc")
            ).replace("Z", "+00:00"))
        matched_start = current_start or _unit_start_utc(props, snapshot, journal_attempt)
        return (
            terminal.tzinfo is not None and matched_start is not None
            and matched_start <= terminal <= observed
            and finished <= terminal + timedelta(seconds=10)
        )
    except (ValueError, TypeError, KeyError, OverflowError):
        return False


def _openbb_resource_details(
    payload: dict[str, object], maximum_seconds: float,
) -> dict[str, object]:
    """Retain bounded stage samples, not current cgroup state or whole-run peaks."""

    resources = payload.get("stage_resources")
    if not isinstance(resources, dict):
        return {}
    fields = (*OPENBB_RESOURCE_COUNTERS, *OPENBB_RESOURCE_GAUGES)
    stages = (
        (("before_attempt",) if "before_attempt" in resources else ())
        + OPENBB_RESOURCE_STAGES
        + (("after_attempt",) if "after_attempt" in resources else ())
    )
    samples = {}
    for stage in stages:
        raw = resources.get(stage)
        samples[stage] = {
            key: _resource_integer(raw.get(key)) if isinstance(raw, dict) else None
            for key in fields
        }
    if not any(value is not None for row in samples.values() for value in row.values()):
        return {}
    maxima = {}
    deltas = {}
    for key in fields:
        values = [samples[stage][key] for stage in stages]
        observed = [value for value in values if value is not None]
        maxima[key] = max(observed) if observed else None
        if key in OPENBB_RESOURCE_COUNTERS:
            # Do not bridge missing/invalid stages or hide a counter reset.
            valid = len(observed) == len(values) and all(
                later >= earlier for earlier, later in zip(observed, observed[1:])
            )
            delta = observed[-1] - observed[0] if valid else None
            if key == "monotonic_ns" and delta is not None and delta > maximum_seconds * 1e9:
                delta = None
            deltas[key] = delta
    attempt_deltas = None
    if (type(payload.get("schema_version")) is int and payload["schema_version"] == 1
            and payload.get("event") == "openbb_l1_compaction_attempt"
            and "before_attempt" in resources and "after_attempt" in resources):
        actual_stages = [stage for stage in stages if stage in resources]
        clocks = [samples[stage]["monotonic_ns"] for stage in actual_stages]
        clock_valid = (
            all(value is not None for value in clocks)
            and all(later >= earlier for earlier, later in zip(clocks, clocks[1:]))
            and clocks[-1] - clocks[0] <= maximum_seconds * 1e9
        )
        attempt_deltas = {}
        for key in OPENBB_RESOURCE_COUNTERS:
            values = [samples[stage][key] for stage in actual_stages]
            valid = (clock_valid and all(value is not None for value in values)
                     and all(later >= earlier for earlier, later in zip(values, values[1:])))
            attempt_deltas[key] = values[-1] - values[0] if valid else None
    return {
        "stage_resources": samples,
        "resource_pressure_summary": {
            "scope": "exact_matched_run_stage_samples",
            "observed_sample_maxima": maxima,
            "first_to_last_counter_deltas": deltas,
            **({"attempt_counter_deltas": attempt_deltas} if attempt_deltas is not None else {}),
        },
    }


def _openbb_attempt_state(payload: dict[str, object]) -> str:
    """A finished attempt is not necessarily a published compaction."""

    state, mode = payload.get("state"), payload.get("mode")
    publication = payload.get("publication_stage")
    reason, code = payload.get("reason"), payload.get("exit_code")
    if (
        mode not in ("compact", "audit") or publication not in OPENBB_PUBLICATION_STAGES
        or reason not in (None, *OPENBB_ATTEMPT_REASONS)
        or any(_resource_integer(payload.get(key)) is None for key in (
            "failed_segments", "deferred_failed_segments", "new_segments", "stale_segments",
        ))
        or (mode == "audit" and publication != "not_attempted")
    ):
        return "unknown"
    failed, deferred = payload["failed_segments"], payload["deferred_failed_segments"]
    if state == "running":
        return "running" if code is None and reason is None else "unknown"
    if type(code) is not int or not 0 <= code <= 255:
        return "unknown"
    if state in ("completed", "completed_with_failures", "completed_with_deferred_segments"):
        expected = (
            "completed_with_failures" if failed else
            "completed_with_deferred_segments" if deferred else "completed"
        )
        valid = mode == "compact" and publication == "status_published" and code == 0 and reason is None
        return state if valid and state == expected else "unknown"
    if state == "deferred":
        valid = (publication == "not_attempted" and code == 0
                 and (reason == "archive_busy" or (mode == "compact" and reason == "archive_resumed")))
    elif state == "failed":
        valid = code != 0 and reason in ("exception", "compactor_lock_busy", "attempt_receipt_error")
        if reason == "compactor_lock_busy":
            valid = valid and publication == "not_attempted"
    elif state == "interrupted":
        valid = reason == "interrupted"  # SystemExit(0) is still not a completed operation.
    elif state in ("audit_completed", "audit_failed"):
        valid = (mode == "audit" and reason is None and deferred == 0
                 and ((state == "audit_completed" and code == 0 and failed == 0)
                      or (state == "audit_failed" and code == 2 and failed > 0)))
    else:
        valid = False
    return state if valid else "unknown"


def _openbb_compaction_details(
    payload: dict[str, object], started: datetime, *, resources_completed: bool = False,
) -> dict[str, object]:
    """Project only bounded, fixed-name evidence from an identity-matched run."""

    details: dict[str, object] = {
        "state": _openbb_attempt_state(payload),
        "systemd_invocation_id": payload["systemd_invocation_id"],
    }
    for key, allowed in (
        ("mode", ("compact", "audit")), ("publication_stage", OPENBB_PUBLICATION_STAGES),
        ("reason", OPENBB_ATTEMPT_REASONS),
    ):
        if payload.get(key) in allowed:
            details[key] = payload[key]
    if type(payload.get("exit_code")) is int and 0 <= payload["exit_code"] <= 255:
        details["exit_code"] = payload["exit_code"]
    for key in (
        "failed_segments", "deferred_failed_segments", "new_segments", "stale_segments",
        "pending_files", "pending_rows", "compacted_files", "compacted_rows",
        "success_files", "success_rows", "active_segments", "endpoints",
        "source_bytes", "output_bytes",
    ):
        value = payload.get(key)
        if type(value) is int and 0 <= value < 2**63:
            details[key] = value
    policy = payload.get("query_view_policy")
    if isinstance(policy, dict):
        safe_policy = {}
        requested = policy.get("schema_grouped_sec_requested")
        if type(requested) is bool:
            safe_policy["schema_grouped_sec_requested"] = requested
        for key in ("schema_grouped_policy_revision", "physical_identity_revision"):
            revision = policy.get(key)
            if type(revision) is int and 0 <= revision < 2**31:
                safe_policy[key] = revision
        if safe_policy:
            details["query_view_policy"] = safe_policy
    actions = payload.get("view_endpoint_actions")
    endpoint = "regulators.sec.filing_headers"
    action = actions.get(endpoint) if isinstance(actions, dict) else None
    if isinstance(action, str) and action in {
        "reused_verified_schema_groups", "reused_verified_paths",
        "rebuilt_schema_groups", "rebuilt", "deferred",
    }:
        details["view_endpoint_actions"] = {endpoint: action}
    try:
        finished = datetime.fromisoformat(str(payload.get("finished_at_utc")).replace("Z", "+00:00"))
        if finished.tzinfo is None or finished < started:
            return details
        maximum = (finished - started).total_seconds() + 1.0
    except (ValueError, TypeError):
        return details
    # Keep persisted resource evidence independent of optional/invalid step
    # timings and of the business completion state. No timing sums are used.
    if resources_completed:
        details.update(_openbb_resource_details(payload, maximum))
    timings = payload.get("stage_seconds")
    if not isinstance(timings, dict):
        return details
    # Top-level, serial phases only. Nested source-load/view endpoint timings
    # overlap these measurements and must not be counted as separate stages.
    steps = []
    for name in (
        "source_journal_prepare", "stale_contract_audit", "source_journal_checkpoint",
        "unassigned_source_load", "batch_planning", "segment_build", "query_view_publish",
    ):
        if name not in timings:
            continue
        elapsed = timings[name]
        if (
            type(elapsed) not in (int, float)
            or not 0 <= elapsed <= maximum
            or not math.isfinite(elapsed)
        ):
            return details
        steps.append({"step": name, "state": "recorded", "elapsed_seconds": float(elapsed)})
    if steps:
        details.update(step_receipts=steps, step_receipts_scope="exact_matched_run")
    return details


def _openbb_attempt_envelope(
    payload: object, identity: str, systemd_start: datetime, snapshot: dict[str, object],
) -> tuple[datetime, datetime | None] | None:
    if (
        not isinstance(payload, dict) or type(payload.get("schema_version")) is not int
        or payload["schema_version"] != 1 or payload.get("event") != "openbb_l1_compaction_attempt"
        or payload.get("systemd_invocation_id") != identity
        or not isinstance(payload.get("attempt_id"), str)
        or re.fullmatch(r"[0-9a-f]{32}", payload["attempt_id"]) is None
    ):
        return None
    try:
        started = datetime.fromisoformat(str(payload.get("started_at_utc")).replace("Z", "+00:00"))
        observed = datetime.fromisoformat(str(snapshot.get("observed_at")).replace("Z", "+00:00"))
        elapsed = payload.get("elapsed_seconds")
        if (started.tzinfo is None or observed.tzinfo is None or started > observed
                or abs((started - systemd_start).total_seconds()) > 10
                or type(elapsed) not in (int, float) or not math.isfinite(elapsed) or elapsed < 0):
            return None
        if payload.get("state") == "running":
            return (started, None) if payload.get("finished_at_utc") is None and elapsed == 0 else None
        finished = datetime.fromisoformat(str(payload.get("finished_at_utc")).replace("Z", "+00:00"))
        if (finished.tzinfo is None or not started <= finished <= observed
                or abs(elapsed - (finished - started).total_seconds()) > 1):
            return None
        return started, finished
    except (ValueError, TypeError, OverflowError):
        return None


def _openbb_attempt_journal(
    identity: str, systemd_start: datetime, snapshot: dict[str, object], boot_id: str | None = None,
) -> dict[str, object] | None:
    """A bounded app event can recover a receipt, never PID 1's process wall time."""

    unit = "stockagent-openbb-l1-compaction.service"
    if (not isinstance(identity, str) or re.fullmatch(r"[0-9a-f]{32}", identity) is None
            or not isinstance(boot_id, str) or re.fullmatch(r"[0-9a-f]{32}", boot_id) is None):
        return None
    try:
        result = subprocess.run(
            ["journalctl", f"_SYSTEMD_UNIT={unit}", f"_SYSTEMD_INVOCATION_ID={identity}",
             "--since", (systemd_start - timedelta(seconds=10)).isoformat(),
             "-n", "32", "-o", "json", "--no-pager", "--all",
             "--output-fields=MESSAGE,_SYSTEMD_UNIT,_SYSTEMD_INVOCATION_ID,_BOOT_ID,__REALTIME_TIMESTAMP"],
            check=False, capture_output=True, text=True, timeout=5,
        )
        if result.returncode != 0 or len(result.stdout) > 1024 * 1024:
            return None
        observed = datetime.fromisoformat(str(snapshot.get("observed_at")).replace("Z", "+00:00"))
        candidates = []
        for line in result.stdout.splitlines()[-32:]:
            if len(line) > 65536:
                continue
            try:
                event = json.loads(line)
                if (not isinstance(event, dict) or event.get("_SYSTEMD_UNIT") != unit
                        or event.get("_SYSTEMD_INVOCATION_ID") != identity
                        or not isinstance(event.get("_BOOT_ID"), str)
                        or re.fullmatch(r"[0-9a-f]{32}", event["_BOOT_ID"]) is None
                        or event["_BOOT_ID"] != boot_id
                        or not isinstance(event.get("MESSAGE"), str)):
                    continue
                payload = json.loads(event["MESSAGE"])
                if (not isinstance(payload, dict) or payload.get("event") != "openbb_l1_compaction_attempt"
                        or payload.get("systemd_invocation_id") != identity):
                    continue
                emitted = datetime.fromtimestamp(int(event["__REALTIME_TIMESTAMP"]) / 1e6, UTC)
                if not systemd_start - timedelta(seconds=10) <= emitted <= observed:
                    continue
                candidates.append((emitted, payload))
            except (ValueError, TypeError, KeyError, OverflowError, RecursionError):
                continue
        if not candidates:
            return None
        # Select the latest matching event BEFORE validation. A newer running
        # or malformed attempt is a barrier against recovering an old success.
        emitted, payload = max(reversed(candidates), key=lambda item: item[0])
        envelope = _openbb_attempt_envelope(payload, identity, systemd_start, snapshot)
        if envelope is None or (envelope[1] is not None and emitted < envelope[1] - timedelta(seconds=1)):
            return None
        return payload
    except (OSError, subprocess.TimeoutExpired, ValueError, TypeError):
        return None


def _openbb_attempt_receipt(
    path: Path, props: dict[str, object], snapshot: dict[str, object],
    journal_attempt: dict[str, object] | None,
) -> dict[str, object]:
    result: dict[str, object] = {"path": str(path), "matches_last_attempt": False}
    current_start = _unit_start_utc(props, snapshot, None)
    active = props.get("ActiveState") in ("active", "activating", "deactivating")
    if current_start is None and active:
        return result  # An old journal is not the current active invocation.
    systemd_start = current_start or _unit_start_utc(props, snapshot, journal_attempt)
    identity = (props.get("InvocationID") if current_start is not None
                else (journal_attempt or {}).get("last_attempt_invocation_id"))
    if (systemd_start is None or not isinstance(identity, str)
            or re.fullmatch(r"[0-9a-f]{32}", identity) is None
            or (current_start is None and props.get("InvocationID") not in (None, "", identity))):
        return result
    payload = None
    try:
        if not path.is_symlink():
            with path.open("rb") as stream:
                raw = stream.read(65537)
            if len(raw) <= 65536:
                payload = json.loads(raw)
    except (OSError, ValueError, RecursionError):
        pass
    envelope = _openbb_attempt_envelope(payload, identity, systemd_start, snapshot)
    source = "attempt_receipt"
    if envelope is None or (envelope[1] is None and not active):
        boot_id = (journal_attempt or {}).get("last_attempt_boot_id") if current_start is None else None
        if current_start is not None:
            try:
                boot_id = Path("/proc/sys/kernel/random/boot_id").read_text().strip().replace("-", "")
            except OSError:
                pass
        recovered = _openbb_attempt_journal(identity, systemd_start, snapshot, boot_id)
        identifiable_file = (
            isinstance(payload, dict) and payload.get("systemd_invocation_id") == identity
            and isinstance(payload.get("attempt_id"), str)
            and re.fullmatch(r"[0-9a-f]{32}", payload["attempt_id"]) is not None
        )
        if (recovered is not None and identifiable_file
                and recovered.get("attempt_id") != payload["attempt_id"]):
            recovered = None
        if recovered is not None:
            payload = recovered
            envelope = _openbb_attempt_envelope(payload, identity, systemd_start, snapshot)
            source = "attempt_journal"
    if envelope is None:
        return result
    started, finished = envelope
    terminal = _openbb_resources_completed(props, snapshot, journal_attempt, finished)
    details = _openbb_compaction_details(payload, started, resources_completed=terminal)
    if finished is not None and not terminal:
        details["state"] = "unknown"
    if terminal:
        observed_code = (props.get("ExecMainStatus") if current_start is not None
                         else (journal_attempt or {}).get("last_attempt_exit_status"))
        outcome = (props.get("Result") if current_start is not None
                   else (journal_attempt or {}).get("last_attempt_outcome"))
        expected_code = payload.get("exit_code")
        signal = payload.get("signal_number")
        signalled = (outcome == "signal" if current_start is not None else
                     (journal_attempt or {}).get("last_attempt_exit_code") in ("killed", "dumped"))
        if (signalled and payload.get("exit_code_basis") == "shell_signal"
                and type(signal) is int and 0 < signal < 128 and expected_code == 128 + signal):
            expected_code = signal
        if ((_resource_integer(observed_code) is not None and observed_code != expected_code)
                or (payload.get("exit_code") == 0 and outcome not in (
                    None, "success", "process_exited_zero", "exit_status_unknown",
                ))):
            details["state"] = "unknown"
    if details["state"] in ("running", "unknown"):
        # The durable start snapshot may survive SIGKILL. Its initial phase is
        # not proof of how far publication progressed before interruption.
        details.pop("publication_stage", None)
    result.update(
        matches_last_attempt=True, receipt_source=source, attempt_id=payload["attempt_id"],
        started_at_utc=started.astimezone(UTC).isoformat(),
        finished_at_utc=finished.astimezone(UTC).isoformat() if finished else None,
        **details,
    )
    return result


def _business_receipt(
    unit: str, props: dict[str, object], snapshot: dict[str, object],
    journal_attempt: dict[str, object] | None = None,
) -> dict[str, object] | None:
    """Attach a job receipt only when it belongs to this systemd invocation.

    A previous successful receipt must not turn a newly failed or deferred run
    green. Monotonic systemd start time is converted using this boot's observed
    wall/monotonic pair; unavailable or mismatched evidence remains unknown.
    """

    if unit == "stockagent-taifex-auxiliary-daily.service":
        from scripts.service_stage_journal import taifex_auxiliary_stage_receipt

        return taifex_auxiliary_stage_receipt(props, snapshot, journal_attempt)
    systemd_start = _unit_start_utc(props, snapshot, journal_attempt)
    if unit in REGISTERED_RUN_RECORDS:
        return _registered_run_receipt(
            unit, REGISTERED_RUN_RECORDS[unit], systemd_start
        )
    path = BUSINESS_RECEIPTS.get(unit)
    if path is None:
        return None
    if unit == "stockagent-openbb-l1-compaction.service":
        return _openbb_attempt_receipt(path, props, snapshot, journal_attempt)
    result: dict[str, object] = {"path": str(path), "matches_last_attempt": False}
    if systemd_start is None:
        return result
    candidates = [path]
    if unit == "stockagent-tw-public-cold-publish.service":
        # Another publication caller may replace latest.json later the same
        # day. Its immutable run receipt can still prove this exact invocation.
        local_start = systemd_start.astimezone(ZoneInfo("Asia/Taipei"))
        prefixes = {
            (local_start + timedelta(seconds=offset)).strftime("%Y%m%dT%H%M")
            for offset in (-10, 0, 10)
        }
        run_dir = path.parent / "runs"
        for prefix in sorted(prefixes):
            candidates.extend(sorted(run_dir.glob(f"{prefix}*.json")))
    for candidate in candidates:
        if candidate.is_symlink():
            continue
        try:
            payload = json.loads(candidate.read_text(encoding="utf-8"))
            if not isinstance(payload, dict):
                continue
            started = datetime.fromisoformat(str(
                payload.get("started_at_utc") or payload.get("started_at_taipei")
            ).replace("Z", "+00:00"))
            if started.tzinfo is None:
                continue
        except (OSError, UnicodeError, ValueError, TypeError):
            continue
        if abs((started.astimezone(UTC) - systemd_start).total_seconds()) > 10:
            continue
        result["path"] = str(candidate)
        result["matches_last_attempt"] = True
        result["state"] = str(payload.get("state") or payload.get("status") or "unknown")
        result["reason"] = payload.get("reason")
        result["started_at_utc"] = started.astimezone(UTC).isoformat()
        finished_raw = payload.get("finished_at_utc") or payload.get(
            "completed_at_taipei"
        )
        finished = None
        try:
            finished = datetime.fromisoformat(str(finished_raw).replace("Z", "+00:00"))
            result["finished_at_utc"] = (
                finished.astimezone(UTC).isoformat() if finished.tzinfo else None
            )
        except ValueError:
            result["finished_at_utc"] = None
        if unit == "stockagent-crypto-training-refresh.service":
            result.update(_crypto_refresh_details(payload, started))
        break
    return result


def business_receipt_findings(
    services: Sequence[dict[str, object]],
) -> list[dict[str, object]]:
    """Surface explicit business failures separately from systemd exit status.

    Only a receipt bound to the observed attempt can supply a finding. Missing
    or older receipts remain unproven rather than being classified as success.
    """

    complete_states = {"completed", "complete", "ok", "ready", "success"}
    findings: list[dict[str, object]] = []
    for service in services:
        receipt = service.get("business_receipt")
        if not isinstance(receipt, dict) or receipt.get("matches_last_attempt") is not True:
            continue
        state = str(receipt.get("state") or "unknown")
        failed_steps = receipt.get("failed_steps")
        failed_step_names = (
            [step for step in failed_steps if isinstance(step, str) and step]
            if isinstance(failed_steps, list) else []
        )
        invalid_failed_steps = (
            failed_steps is not None
            and (
                not isinstance(failed_steps, list)
                or len(failed_step_names) != len(failed_steps)
            )
        )
        unresolved = receipt.get("source_unresolved_count")
        unresolved_count = unresolved if type(unresolved) is int and unresolved > 0 else 0
        invalid_unresolved = unresolved is not None and (
            type(unresolved) is not int or unresolved < 0
        )
        gap_counts = receipt.get("source_gap_status_counts")
        valid_gap_counts = (
            isinstance(gap_counts, dict)
            and len(gap_counts) <= 64
            and all(
                isinstance(key, str)
                and re.fullmatch(r"[a-z0-9_]{1,64}", key)
                and type(value) is int
                and value >= 0
                for key, value in gap_counts.items()
            )
            and sum(gap_counts.values()) == unresolved_count
        )
        invalid_gap_counts = (
            (gap_counts is not None and not valid_gap_counts)
            or receipt.get("source_gap_breakdown_status") == "invalid"
        )
        source_health = receipt.get("source_data_health")
        reasons = []
        if state not in complete_states:
            reasons.append("non_complete_state")
        if failed_step_names:
            reasons.append("failed_steps")
        if invalid_failed_steps:
            reasons.append("invalid_failed_steps")
        if unresolved_count:
            reasons.append("source_unresolved")
        if invalid_unresolved:
            reasons.append("invalid_source_unresolved_count")
        if invalid_gap_counts:
            reasons.append("invalid_source_gap_breakdown")
        if source_health not in (None, "complete", "healthy"):
            reasons.append("source_data_health")
        if reasons:
            finding: dict[str, object] = {
                "unit": service.get("unit"),
                "receipt_state": state,
                "reasons": reasons,
                "failed_steps": failed_step_names,
                "source_unresolved_count": unresolved_count,
                "source_data_health": source_health,
            }
            if valid_gap_counts:
                finding["source_gap_status_counts"] = gap_counts
            findings.append(finding)
    return findings


def resource_pressure_findings(
    services: Sequence[dict[str, object]],
) -> list[dict[str, object]]:
    """Observed run pressure is not a data failure or a current live sample."""

    findings = []
    for service in services:
        receipt = service.get("business_receipt")
        if not isinstance(receipt, dict) or receipt.get("matches_last_attempt") is not True:
            continue
        identity = receipt.get("systemd_invocation_id")
        summary = receipt.get("resource_pressure_summary")
        if (
            not isinstance(identity, str) or not re.fullmatch(r"[0-9a-f]{32}", identity)
            or not isinstance(summary, dict)
            or summary.get("scope") != "exact_matched_run_stage_samples"
        ):
            continue
        maxima = summary.get("observed_sample_maxima")
        if not isinstance(maxima, dict):
            continue
        observed = {key: _resource_integer(maxima.get(key)) for key in OPENBB_PRESSURE_SIGNALS}
        attempt_deltas = summary.get("attempt_counter_deltas")
        old_deltas = summary.get("first_to_last_counter_deltas")
        counter_deltas = {}
        for key in OPENBB_PRESSURE_SIGNALS.keys() & set(OPENBB_RESOURCE_COUNTERS):
            value = _resource_integer(attempt_deltas.get(key)) if isinstance(attempt_deltas, dict) else None
            if value is None:
                value = _resource_integer(old_deltas.get(key)) if isinstance(old_deltas, dict) else None
            counter_deltas[key] = value
        reasons = [
            reason for key, reason in OPENBB_PRESSURE_SIGNALS.items()
            if observed[key] is not None
            and (value := counter_deltas.get(key, observed[key])) is not None and value > 0
        ]
        if reasons:
            findings.append({
                "unit": service.get("unit"), "systemd_invocation_id": identity,
                "scope": "exact_matched_run_stage_samples", "reasons": reasons,
                "observed_sample_maxima": observed,
                "pressure_counter_deltas": counter_deltas,
            })
    return findings


def _journal_last_completed_process(unit: str) -> dict[str, object] | None:
    """Recover a GC'd oneshot's last wall time from PID 1's own events.

    systemctl drops ExecMain timestamps after unloading an idle unit. Pair
    same-boot, same-invocation start and terminal/resource events; a resource
    event alone, a daemon's age, or an application log is not execution latency.
    """

    try:
        result = subprocess.run(
            [
                "journalctl", "_PID=1", f"UNIT={unit}",
                "--since=14 days ago", "-n", "150", "-o", "json", "--no-pager",
            ],
            check=False, capture_output=True, text=True, timeout=5,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if result.returncode != 0:
        return None
    invocations: dict[tuple[str, str], dict[str, object]] = {}
    for line in result.stdout.splitlines():
        try:
            event = json.loads(line)
        except ValueError:
            continue
        if not isinstance(event, dict) or event.get("UNIT") != unit:
            continue
        boot = event.get("_BOOT_ID")
        invocation = event.get("INVOCATION_ID")
        try:
            timestamp = int(event["__MONOTONIC_TIMESTAMP"])
            realtime = int(event["__REALTIME_TIMESTAMP"])
        except (KeyError, TypeError, ValueError):
            continue
        if not isinstance(boot, str) or not isinstance(invocation, str):
            continue
        row = invocations.setdefault((boot, invocation), {})
        if timestamp >= row.get("last_seen_monotonic_us", -1):
            row["last_seen_monotonic_us"] = timestamp
            row["last_seen_realtime_us"] = realtime
        message_id = event.get("MESSAGE_ID")
        if message_id == JOURNAL_START_MESSAGE_ID and event.get("JOB_TYPE") == "start":
            row["started_monotonic_us"] = timestamp
            row["started_realtime_us"] = realtime
        elif message_id == JOURNAL_RESOURCE_MESSAGE_ID:
            row["finished_monotonic_us"] = timestamp
            row["finished_realtime_us"] = realtime
        elif message_id == JOURNAL_SUCCESS_MESSAGE_ID:
            row["outcome"] = "process_exited_zero"
            row["terminal_monotonic_us"] = timestamp
            row["terminal_realtime_us"] = realtime
        elif message_id == JOURNAL_FAILURE_MESSAGE_ID:
            row["outcome"] = "failed"
            row["terminal_monotonic_us"] = timestamp
            row["terminal_realtime_us"] = realtime
        elif message_id == JOURNAL_PROCESS_EXIT_MESSAGE_ID:
            try:
                row["exit_status"] = int(event["EXIT_STATUS"])
                row["exit_code"] = str(event["EXIT_CODE"])
            except (KeyError, TypeError, ValueError):
                pass
    # A newer incomplete attempt is a barrier, not permission to reuse an old
    # success. Within a boot only monotonic time orders invocations; wall-clock
    # corrections must not make an older run appear latest.
    latest_by_boot: dict[str, tuple[str, dict[str, object]]] = {}
    for (boot_id, invocation_id), row in invocations.items():
        previous = latest_by_boot.get(boot_id)
        if previous is None or row.get("started_monotonic_us", row["last_seen_monotonic_us"]) > previous[1].get(
            "started_monotonic_us", previous[1]["last_seen_monotonic_us"]
        ):
            latest_by_boot[boot_id] = (invocation_id, row)
    if not latest_by_boot:
        return None
    try:
        current_boot = Path("/proc/sys/kernel/random/boot_id").read_text().strip().replace("-", "")
    except OSError:
        current_boot = None
    if current_boot not in latest_by_boot and len(latest_by_boot) > 1:
        # Monotonic clocks cannot order different boots. Without a current-boot
        # event, wall time alone cannot prove which historical attempt is latest.
        return None
    selected_boot = current_boot if current_boot in latest_by_boot else max(
        latest_by_boot, key=lambda key: latest_by_boot[key][1]["last_seen_realtime_us"]
    )
    selected_invocation = latest_by_boot[selected_boot][0]
    candidates: list[dict[str, object]] = []
    for (boot_id, invocation_id), row in invocations.items():
        if (boot_id, invocation_id) != (selected_boot, selected_invocation):
            continue
        started = row.get("started_monotonic_us")
        # Short successful jobs may have no separate resource-accounting line.
        # A paired terminal unit event is still a measured completed process.
        finished = row.get("finished_monotonic_us", row.get("terminal_monotonic_us"))
        realtime = row.get("finished_realtime_us", row.get("terminal_realtime_us"))
        if not all(type(value) is int for value in (started, finished, realtime)):
            continue
        if finished < started or realtime <= 0:
            continue
        candidates.append({
            "last_attempt_wall_seconds": round((finished - started) / 1e6, 6),
            "last_attempt_outcome": row.get("outcome", "exit_status_unknown"),
            "last_attempt_exit_status": row.get("exit_status"),
            "last_attempt_exit_code": row.get("exit_code"),
            "last_attempt_invocation_id": invocation_id,
            "last_attempt_boot_id": boot_id,
            "last_attempt_started_monotonic_us": started,
            "last_attempt_completed_monotonic_us": finished,
            "last_attempt_started_at_utc": datetime.fromtimestamp(
                int(row["started_realtime_us"]) / 1e6, UTC
            ).isoformat(),
            "last_attempt_completed_at_utc": datetime.fromtimestamp(
                realtime / 1e6, UTC
            ).isoformat(),
        })
    return (
        max(candidates, key=lambda row: str(row["last_attempt_completed_at_utc"]))
        if candidates else None
    )


def _matched_step_timing_summary(
    business: dict[str, object] | None,
) -> dict[str, object] | None:
    """Describe recorded steps, without claiming all operations are instrumented."""

    if (
        not isinstance(business, dict)
        or business.get("matches_last_attempt") is not True
        or business.get("step_receipts_scope") != "exact_matched_run"
    ):
        return None
    steps = business.get("step_receipts")
    if not isinstance(steps, list) or not steps or len(steps) > 128:
        return None
    for step in steps:
        if not isinstance(step, dict):
            return None
        name = step.get("step")
        elapsed = step.get("elapsed_seconds")
        if (
            not isinstance(name, str)
            or not re.fullmatch(r"[a-z0-9_]{1,80}", name)
            or type(elapsed) not in (int, float)
            or not math.isfinite(elapsed)
            or elapsed < 0
        ):
            return None
    slowest = min(steps, key=lambda step: (-step["elapsed_seconds"], step["step"]))
    return {
        "scope": "recorded_steps_only",
        "recorded_step_count": len(steps),
        "slowest_step": slowest["step"],
        "slowest_step_seconds": slowest["elapsed_seconds"],
    }


def build_report(*, sample_seconds: float) -> dict[str, object]:
    if not 1 <= sample_seconds <= 60:
        raise ValueError("sample_seconds must be between 1 and 60")
    # Windows/WMI startup checks can take seconds. Keep them outside the
    # before/after CPU interval so a requested five-second sample is comparable
    # with the next run rather than measuring an unpredictable PowerShell wait.
    schedules = schedule_snapshot()
    auxiliary_schedules = auxiliary_schedule_snapshot()
    startup = startup_snapshot()
    product_probes = product_probe_snapshot()
    repository_processes = repository_process_snapshot()
    before = service_snapshot()
    time.sleep(sample_seconds)
    after = service_snapshot()
    host_pressure = pressure_snapshot()
    resources = service_deltas(before, after)
    service_rows = []
    for row in sorted(resources["rows"], key=lambda item: item["unit"]):
        unit = row["unit"]
        timer = unit.removesuffix(".service") + ".timer"
        path = unit.removesuffix(".service") + ".path"
        completed_attempt = bool(
            row.get("last_attempt_seconds") is not None
            and row.get("ActiveState") != "active"
        )
        journal_attempt = (
            _journal_last_completed_process(unit)
            if not completed_attempt and row.get("ActiveState") not in {"active", "activating"}
            else None
        )
        business = _business_receipt(unit, row, after, journal_attempt)
        step_timing = _matched_step_timing_summary(business)
        service_rows.append(
            {
                "unit": unit,
                "service_origin": (
                    "transient" if row.get("Transient") == "yes" else "installed"
                ),
                "unit_file_state": row.get("UnitFileState"),
                "active_state": row.get("ActiveState"),
                "sub_state": row.get("SubState"),
                "result": row.get("Result"),
                "exit_status": (
                    row.get("ExecMainStatus") if completed_attempt
                    else journal_attempt.get("last_attempt_exit_status")
                    if journal_attempt else None
                ),
                "exit_code": (
                    journal_attempt.get("last_attempt_exit_code")
                    if not completed_attempt and journal_attempt else None
                ),
                "last_attempt_outcome": (
                    row.get("last_attempt_outcome") if completed_attempt
                    else journal_attempt.get("last_attempt_outcome") if journal_attempt
                    else row.get("last_attempt_outcome")
                ),
                "business_receipt": business,
                "timer": timer if timer in schedules["timers"] else None,
                "path": path if path in schedules["paths"] else None,
                "last_attempt_wall_seconds": (
                    row["last_attempt_seconds"] if completed_attempt
                    else journal_attempt["last_attempt_wall_seconds"] if journal_attempt
                    else None
                ),
                "last_attempt_source": (
                    "systemd_current" if completed_attempt
                    else "systemd_journal" if journal_attempt else None
                ),
                "last_attempt_completed_at_utc": (
                    journal_attempt.get("last_attempt_completed_at_utc")
                    if journal_attempt else None
                ),
                "cpu_cores_during_sample": row.get("cpu_cores_average"),
                "memory_current_bytes": row.get("MemoryCurrent"),
                "memory_anon_bytes": row.get("MemoryAnon"),
                "memory_file_cache_bytes": row.get("MemoryFile"),
                "block_io_read_bytes_during_sample": row.get("CgroupIOReadBytesDelta"),
                "block_io_write_bytes_during_sample": row.get("CgroupIOWriteBytesDelta"),
                "memory_peak_bytes": row.get("MemoryPeak"),
                "memory_high_bytes": row.get("MemoryHigh"),
                "memory_max_bytes": row.get("MemoryMax"),
                "memory_high_events_total": row.get("MemoryHighEvents"),
                "memory_high_events_during_sample": row.get("MemoryHighEventsDelta"),
                "memory_max_events_during_sample": row.get("MemoryMaxEventsDelta"),
                "memory_oom_events_during_sample": row.get("MemoryOomEventsDelta"),
                "memory_oom_kill_events_during_sample": row.get("MemoryOomKillEventsDelta"),
                "operation_latency_coverage": (
                    "matched_run_step_timings"
                    if step_timing is not None
                    else "last_completed_process_wall_only"
                    if completed_attempt or journal_attempt
                    else "resource_sample_only"
                    if row.get("same_process")
                    else "not_measured"
                ),
                "operation_latency_summary": step_timing,
            }
        )
    return {
        "schema_version": 1,
        "observed_at_utc": datetime.now(UTC).isoformat(),
        "coverage_boundary": (
            "Every currently discoverable StockAgent systemd unit on this WSL host, "
            "including transient units separately marked by origin. "
            "MemoryCurrent is total cgroup memory, not process RSS; anon and file "
            "are current cgroup components and do not sum to all kernel charges. "
            "MemoryPeak and memory event totals are historical to the current "
            "cgroup; event deltas require one unchanged active invocation. "
            "Matched OpenBB receipts separately retain recorded stage resource "
            "samples after cgroup removal; their sample maxima are not current "
            "resources or unsampled whole-run peaks, and event totals can include "
            "pressure before the first stage. Counter deltas require all five "
            "valid nondecreasing samples. Resource findings do not change data health. "
            "Process wall/CPU is not per-operation latency. Matched run step "
            "timings cover only validated recorded steps, not all operations "
            "or the complete process wall time. Remote services "
            "and Windows applications beyond the named Caddy task and current "
            "WSL VM startup markers are not covered."
        ),
        "counts": {
            "services": len(service_rows),
            "timers": len(schedules["timers"]),
            "paths": len(schedules["paths"]),
        },
        "service_origins": {
            "installed": sum(row["service_origin"] == "installed" for row in service_rows),
            "transient": sum(row["service_origin"] == "transient" for row in service_rows),
        },
        "resource_sample_seconds": resources["sample_seconds"],
        "services": service_rows,
        "business_receipt_findings": business_receipt_findings(service_rows),
        "resource_pressure_findings": resource_pressure_findings(service_rows),
        "timers": schedules["timers"],
        "timer_schedule_findings": timer_schedule_findings(
            schedules["timers"], service_rows
        ),
        "paths": schedules["paths"],
        "auxiliary_schedules": auxiliary_schedules,
        "startup": startup,
        "product_probes": product_probes,
        "repository_processes": repository_processes,
        "host_pressure": host_pressure,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sample-seconds", type=float, default=10.0)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    report = build_report(sample_seconds=args.sample_seconds)
    output = args.output or REPO_ROOT / "artifacts/benchmarks" / (
        f"service-coverage-{datetime.now(UTC):%Y%m%dT%H%M%SZ}.json"
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(f".{output.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
        os.replace(temporary, output)
    finally:
        temporary.unlink(missing_ok=True)
    print(json.dumps({"receipt": str(output), "counts": report["counts"]}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
