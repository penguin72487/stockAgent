"""Record bounded, read-only resource trends for every installed StockAgent unit.

The 30-second public-status worker calls this at most once per five minutes.
Its one atomic state file is a counter baseline, not an unbounded history;
journald retains the compact event stream under its configured rotation.
"""

from __future__ import annotations

from datetime import UTC, datetime
import json
import os
from pathlib import Path
import subprocess
import time
import uuid
from typing import Any, Mapping


INTERVAL_SECONDS = 300
SCHEMA_VERSION = 1
REPO_ROOT = Path(__file__).resolve().parents[1]


def _read_state(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, ValueError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _boot_id() -> str | None:
    try:
        return Path("/proc/sys/kernel/random/boot_id").read_text().strip()
    except OSError:
        return None


def _filesystem_space(path: Path = REPO_ROOT) -> dict[str, int] | None:
    """Sample the filesystem holding StockAgent, without walking its files."""

    try:
        device_before = path.stat().st_dev
        usage = os.statvfs(path)
        device_after = path.stat().st_dev
    except OSError:
        return None
    if device_before != device_after:
        return None
    block_size = usage.f_frsize or usage.f_bsize
    return {
        "device_id": device_before,
        "total_bytes": usage.f_blocks * block_size,
        "available_bytes": usage.f_bavail * block_size,
    }


def _counter_delta(
    old: Mapping[str, Any], current: Mapping[str, Any], key: str, *, same_process: bool
) -> int | None:
    before, after = old.get(key), current.get(key)
    if (
        not same_process
        or type(before) is not int
        or type(after) is not int
        or after < before
    ):
        return None
    return after - before


def _recurring_timer_health(current: Mapping[str, Any]) -> dict[str, Any]:
    """Reuse the audited systemd rule; a failed observation is not healthy."""

    from scripts.audit_service_latency_coverage import (
        schedule_snapshot, timer_schedule_findings,
    )

    try:
        service_rows = [
            {"unit": unit, "active_state": props.get("ActiveState")}
            for unit, props in current.get("units", {}).items()
        ]
        timers = schedule_snapshot()["timers"]
        if not timers:
            return {"state": "unavailable", "timer_count": 0, "findings": [],
                    "error_type": "NoTimersObserved"}
        findings = timer_schedule_findings(timers, service_rows)
    except (
        OSError, KeyError, TypeError, ValueError,
        subprocess.CalledProcessError, subprocess.TimeoutExpired,
    ) as exc:
        return {"state": "unavailable", "timer_count": None, "findings": [],
                "error_type": type(exc).__name__}
    return {
        "state": "attention" if findings else "observed_clear",
        "timer_count": len(timers),
        "findings": findings,
    }


def sample_service_runtime(
    state_path: Path, *, now_monotonic: float | None = None
) -> dict[str, Any] | None:
    """Return a journal-ready event, or None when the next sample is not due."""

    current_monotonic = time.monotonic() if now_monotonic is None else now_monotonic
    previous = _read_state(state_path)
    previous_sample = previous.get("sample")
    old_monotonic = (
        previous_sample.get("monotonic")
        if isinstance(previous_sample, Mapping) else None
    )
    boot_id = _boot_id()
    # ProcSubset=pid hides /proc/sys/kernel/random/boot_id from the hardened
    # snapshot service. In that sandbox, monotonic rollback forces a new
    # baseline; matching systemd InvocationID is still required before any
    # per-unit counter subtraction, so a reboot cannot create a false delta.
    same_boot = bool(
        boot_id == previous.get("boot_id")
        and (boot_id is not None or (
            type(old_monotonic) in (int, float)
            and current_monotonic >= old_monotonic
        ))
    )
    if (
        same_boot
        and type(old_monotonic) in (int, float)
        and 0 <= current_monotonic - old_monotonic < INTERVAL_SECONDS
    ):
        return None

    # Avoid the benchmark module's imports on the 9 out of 10 ordinary
    # public-status snapshots for which no new service sample is due.
    from scripts.benchmark_dashboard_latency import service_snapshot
    from scripts.audit_service_latency_coverage import (
        _business_receipt, _journal_last_completed_process,
    )

    current = service_snapshot()
    recurring_timers = _recurring_timer_health(current)
    filesystem = _filesystem_space()
    old_filesystem = previous.get("filesystem") if same_boot else None
    filesystem_available_delta = None
    if (
        isinstance(filesystem, Mapping)
        and isinstance(old_filesystem, Mapping)
        and filesystem.get("device_id") == old_filesystem.get("device_id")
        and type(filesystem.get("available_bytes")) is int
        and type(old_filesystem.get("available_bytes")) is int
    ):
        filesystem_available_delta = (
            filesystem["available_bytes"] - old_filesystem["available_bytes"]
        )
    old_units = (
        previous_sample.get("units", {})
        if same_boot and isinstance(previous_sample, Mapping) else {}
    )
    elapsed = (
        float(current["monotonic"]) - float(old_monotonic)
        if same_boot and type(old_monotonic) in (int, float) else None
    )
    if elapsed is not None and elapsed <= 0:
        elapsed = None
    rows: list[dict[str, Any]] = []
    for unit, props in sorted(current["units"].items()):
        old = old_units.get(unit, {}) if isinstance(old_units, Mapping) else {}
        same_process = bool(
            old.get("InvocationID")
            and old.get("InvocationID") == props.get("InvocationID")
            and old.get("MainPID") == props.get("MainPID")
            and old.get("NRestarts") == props.get("NRestarts")
        )
        completed = props.get("ActiveState") not in {"active", "activating"}
        journal_attempt = (
            _journal_last_completed_process(unit)
            if completed and props.get("last_attempt_seconds") is None
            else None
        )
        # A finished oneshot retains its InvocationID and cgroup counters.
        # Subtracting two idle snapshots yields a misleading "0 cores" for a
        # job that actually took substantial CPU during its last execution.
        live_process = same_process and not completed
        cpu_ns = _counter_delta(old, props, "CPUUsageNSec", same_process=live_process)
        # The local systemd IOReadBytes/IOWriteBytes properties can be unset
        # while cgroup io.stat still has valid counters. Match the short
        # benchmark's explicitly measured block-I/O source.
        read_bytes = _counter_delta(
            old, props, "CgroupIOReadBytes", same_process=live_process,
        )
        write_bytes = _counter_delta(
            old, props, "CgroupIOWriteBytes", same_process=live_process,
        )
        rows.append({
            "unit": unit,
            "state": props.get("ActiveState"),
            "result": props.get("Result"),
            "exit_status": (
                journal_attempt.get("last_attempt_exit_status")
                if journal_attempt else props.get("ExecMainStatus") if completed else None
            ),
            "exit_code": (
                journal_attempt.get("last_attempt_exit_code")
                if journal_attempt else None
            ),
            "last_attempt_outcome": (
                journal_attempt.get("last_attempt_outcome")
                if journal_attempt else props.get("last_attempt_outcome")
            ),
            "business_receipt": _business_receipt(
                unit, props, current, journal_attempt
            ),
            "last_attempt_invocation_id": (
                journal_attempt.get("last_attempt_invocation_id")
                if journal_attempt else props.get("InvocationID") if completed else None
            ),
            "last_attempt_boot_id": (
                journal_attempt.get("last_attempt_boot_id") if journal_attempt else None
            ),
            "last_completed_process_wall_seconds": (
                journal_attempt.get("last_attempt_wall_seconds")
                if journal_attempt else props.get("last_attempt_seconds") if completed else None
            ),
            "last_completed_process_source": (
                "systemd_journal" if journal_attempt else "systemd_current"
                if completed and props.get("last_attempt_seconds") is not None else None
            ),
            "last_completed_at_utc": (
                journal_attempt.get("last_attempt_completed_at_utc")
                if journal_attempt else None
            ),
            "cpu_cores_average": (
                round(cpu_ns / 1e9 / elapsed, 4)
                if cpu_ns is not None and elapsed is not None else None
            ),
            "read_bytes_delta": read_bytes,
            "write_bytes_delta": write_bytes,
            "memory_current_bytes": props.get("MemoryCurrent"),
            "memory_anon_bytes": props.get("MemoryAnon"),
            "memory_file_cache_bytes": props.get("MemoryFile"),
            "memory_peak_bytes": props.get("MemoryPeak"),
            "sample_continuous": live_process and elapsed is not None,
        })
    payload = {
        "schema_version": SCHEMA_VERSION,
        "boot_id": boot_id,
        "sample": current,
        "filesystem": filesystem,
    }
    state_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = state_path.with_name(f".{state_path.name}.{uuid.uuid4().hex}.tmp")
    try:
        temporary.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
        os.replace(temporary, state_path)
    finally:
        temporary.unlink(missing_ok=True)
    return {
        "event": "all_service_runtime_sample",
        "schema_version": SCHEMA_VERSION,
        "observed_at_utc": datetime.now(UTC).isoformat(),
        "interval_seconds": round(elapsed, 3) if elapsed is not None else None,
        "query_ms": round(float(current.get("query_ms") or 0), 3),
        "service_count": len(rows),
        "recurring_timers": recurring_timers,
        "filesystem": {
            **(filesystem or {}),
            "available_delta_bytes": filesystem_available_delta,
        } if filesystem is not None else None,
        "boundary": (
            "CPU and I/O are same-invocation counter deltas over the observed interval; "
            "I/O uses cgroup io.stat block bytes, may be unavailable, and is not logical file growth; "
            "MemoryCurrent is cgroup total, not process RSS; anon/file are current "
            "cgroup components and omit other kernel charges. "
            "completed process wall is only the latest attempt (deduplicate by "
            "invocation ID), not request latency "
            "or data-health evidence. Filesystem available-byte change is the "
            "whole StockAgent volume, not attributable to one service or equal "
            "to any cgroup I/O counter. A restart or boot change yields null deltas."
        ),
        "services": rows,
    }


__all__ = ["sample_service_runtime"]
