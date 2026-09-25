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
from downloader.status import count_reported_source_gaps  # noqa: E402


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
}
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


def _business_receipt(
    unit: str, props: dict[str, object], snapshot: dict[str, object],
    journal_attempt: dict[str, object] | None = None,
) -> dict[str, object] | None:
    """Attach a job receipt only when it belongs to this systemd invocation.

    A previous successful receipt must not turn a newly failed or deferred run
    green. Monotonic systemd start time is converted using this boot's observed
    wall/monotonic pair; unavailable or mismatched evidence remains unknown.
    """

    systemd_start = _unit_start_utc(props, snapshot, journal_attempt)
    if unit in REGISTERED_RUN_RECORDS:
        return _registered_run_receipt(
            unit, REGISTERED_RUN_RECORDS[unit], systemd_start
        )
    path = BUSINESS_RECEIPTS.get(unit)
    if path is None:
        return None
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
        try:
            finished = datetime.fromisoformat(str(finished_raw).replace("Z", "+00:00"))
            result["finished_at_utc"] = (
                finished.astimezone(UTC).isoformat() if finished.tzinfo else None
            )
        except ValueError:
            result["finished_at_utc"] = None
        break
    return result


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
    candidates: list[dict[str, object]] = []
    for (boot_id, invocation_id), row in invocations.items():
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
                    "last_completed_process_wall_only"
                    if completed_attempt or journal_attempt
                    else "resource_sample_only"
                    if row.get("same_process")
                    else "not_measured"
                ),
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
            "Process wall/CPU is not per-operation latency; remote services "
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
