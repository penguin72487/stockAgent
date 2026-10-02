"""Record bounded, read-only resource trends for every installed StockAgent unit.

The 30-second public-status worker calls this at most once per five minutes.
Its one atomic state file is a counter baseline, not an unbounded history;
journald retains the compact event stream under its configured rotation.
"""

from __future__ import annotations

from datetime import UTC, datetime
import hashlib
import json
import math
import os
from pathlib import Path
import re
import subprocess
import time
import uuid
from typing import Any, Mapping


INTERVAL_SECONDS = 300
SCHEMA_VERSION = 1
JOURNAL_LINE_MAX_BYTES = 32 * 1024
BASELINE_MAX_BYTES = 4 * 1024 * 1024
REPO_ROOT = Path(__file__).resolve().parents[1]


def runtime_sample_journal_lines(sample: Mapping[str, Any]) -> list[str]:
    """Keep small events compatible; losslessly frame large stdout events.

    journald splits stream lines at LineMax (48 KiB by default). A split JSON
    document is no longer a usable historical sample. Each large-sample frame
    is independent JSON below 32 KiB; concatenate payload_fragment in part_index
    order and verify payload_sha256 before parsing the original sample. Missing
    frames must never be interpreted as a complete or smaller service inventory.
    """

    payload = json.dumps(sample, separators=(",", ":"), allow_nan=False)
    if len(payload.encode("ascii")) <= JOURNAL_LINE_MAX_BYTES:
        return [payload]
    envelope = {
        "event": "all_service_runtime_sample_chunk",
        "schema_version": 1,
        "sample_id": uuid.uuid4().hex,
        "payload_sha256": hashlib.sha256(payload.encode("ascii")).hexdigest(),
        "part_index": 0,
        "part_count": len(payload),  # Upper bound for decimal header width.
        "payload_fragment": "",
    }
    header_size = len(json.dumps(envelope, separators=(",", ":")))
    # The inner document is ASCII JSON, with no literal control characters.
    # Outer JSON can at most double a character (quotes and backslashes).
    # Reserve both index and count widths for even a very large source event.
    width = len(str(len(payload)))
    fragment_size = (JOURNAL_LINE_MAX_BYTES - header_size - width) // 2
    if fragment_size < 1:
        raise ValueError("runtime journal frame header exceeds line budget")
    part_count = (len(payload) + fragment_size - 1) // fragment_size
    lines = []
    for index in range(part_count):
        frame = {
            **envelope,
            "part_index": index,
            "part_count": part_count,
            "payload_fragment": payload[index * fragment_size:(index + 1) * fragment_size],
        }
        line = json.dumps(frame, separators=(",", ":"))
        if len(line.encode("ascii")) > JOURNAL_LINE_MAX_BYTES:
            raise ValueError("runtime journal frame exceeds line budget")
        lines.append(line)
    return lines


def _finite_json_float(value: str) -> float:
    parsed = float(value)
    if not math.isfinite(parsed):
        raise ValueError("non-finite runtime baseline")
    return parsed


def _reject_json_constant(value: str) -> None:
    raise ValueError("non-finite runtime baseline")


def _valid_sample(sample: object) -> bool:
    if not isinstance(sample, dict):
        return False
    stamp, units = sample.get("monotonic"), sample.get("units")
    query_ms = sample.get("query_ms")
    return (
        type(stamp) in (int, float) and math.isfinite(stamp) and stamp >= 0
        and (query_ms is None or (
            type(query_ms) in (int, float) and math.isfinite(query_ms) and query_ms >= 0
        ))
        and isinstance(units, dict)
        and all(isinstance(row, dict) for row in units.values())
    )


def _read_state(path: Path) -> tuple[dict[str, Any], str]:
    """A replaceable counter baseline must not permanently poison sampling."""
    try:
        with path.open("rb") as stream:
            raw = stream.read(BASELINE_MAX_BYTES + 1)
        if len(raw) > BASELINE_MAX_BYTES:
            return {}, "reset_oversized"
        payload = json.loads(
            raw, parse_constant=_reject_json_constant, parse_float=_finite_json_float,
        )
        if (
            not isinstance(payload, dict)
            or type(payload.get("schema_version")) is not int
            or payload["schema_version"] != SCHEMA_VERSION
            or not (payload.get("boot_id") is None or isinstance(payload.get("boot_id"), str))
            or not _valid_sample(payload.get("sample"))
        ):
            return {}, "reset_invalid"
    except FileNotFoundError:
        return {}, "initialized"
    except (OSError, UnicodeError, ValueError, RecursionError, OverflowError):
        return {}, "reset_unreadable"
    return payload, "loaded"


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
    total = usage.f_blocks * block_size
    available = usage.f_bavail * block_size
    free = usage.f_bfree * block_size
    return {
        "device_id": device_before,
        "total_bytes": total,
        "available_bytes": available,
        "free_bytes_including_reserved": free,
        "used_bytes": total - free,
        "reserved_or_unavailable_free_bytes": free - available,
    }


def _filesystem_counter_delta(
    old: Any, current: Any, key: str,
) -> int | None:
    """A volume net change, not bytes written or reserved-space permission."""
    if not isinstance(old, Mapping) or not isinstance(current, Mapping):
        return None
    for geometry in ("device_id", "total_bytes"):
        before, after = old.get(geometry), current.get(geometry)
        if type(before) is not int or type(after) is not int or before < 0 or before != after:
            return None
    total = current["total_bytes"]
    if total <= 0:
        return None
    before, after = old.get(key), current.get(key)
    if type(before) is not int or type(after) is not int or not (0 <= before <= total and 0 <= after <= total):
        return None
    return after - before


def _counter_delta(
    old: Mapping[str, Any], current: Mapping[str, Any], key: str, *, same_process: bool
) -> int | None:
    before, after = old.get(key), current.get(key)
    if (
        not same_process
        or type(before) is not int
        or type(after) is not int
        or before < 0
        or after < before
    ):
        return None
    return after - before


def _process_epoch_continuity(
    previous: Mapping[str, Any], current: Mapping[str, Any],
    *, boot_id: str | None, same_boot: bool,
) -> tuple[bool, str, str | None]:
    """Never compare unmanaged PID counters on an unproven host epoch.

    ProcSubset=pid can hide boot_id. An unchanged live systemd invocation is
    then an epoch witness; both its real UUID and main PID must still match.
    No anchor (or a changed boot) yields null process deltas, not false zeros.
    """

    if not same_boot:
        return False, "unproven", None
    if boot_id is not None and boot_id == previous.get("boot_id"):
        return True, "kernel_boot_id", None
    old_units = previous.get("sample", {}).get("units", {})
    for unit, props in sorted(current["units"].items()):
        old = old_units.get(unit, {})
        invocation, pid = props.get("InvocationID"), props.get("MainPID")
        if (
            props.get("ActiveState") == "active" and old.get("ActiveState") == "active"
            and isinstance(invocation, str) and re.fullmatch(r"[0-9a-f]{32}", invocation)
            and isinstance(pid, str) and pid.isdecimal() and int(pid) > 0
            and old.get("InvocationID") == invocation and old.get("MainPID") == pid
            and old.get("NRestarts") == props.get("NRestarts")
        ):
            return True, "systemd_invocation_anchor", unit
    return False, "unproven", None


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
    if type(current_monotonic) not in (int, float) or not math.isfinite(current_monotonic) or current_monotonic < 0:
        raise ValueError("invalid runtime sampling clock")
    previous, baseline_state = _read_state(state_path)
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
        previous
        and boot_id == previous.get("boot_id")
        and type(old_monotonic) in (int, float)
        and current_monotonic >= old_monotonic
    )
    if previous and not same_boot:
        baseline_state = (
            "reset_boot" if boot_id != previous.get("boot_id") else "reset_clock_rollback"
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
        repository_process_io_deltas, repository_process_snapshot,
    )

    current = service_snapshot()
    if not _valid_sample(current):
        raise ValueError("invalid current service snapshot")
    if same_boot and current["monotonic"] < old_monotonic:
        # Admission and the actual snapshot have different clock observations.
        # Never carry counters or volume deltas over a rollback between them.
        same_boot = False
        baseline_state = "reset_clock_rollback"
    recurring_timers = _recurring_timer_health(current)
    try:
        repository_processes = repository_process_snapshot()
    except (OSError, UnicodeError, ValueError) as exc:
        # Optional process visibility must not poison the all-unit baseline or
        # the authoritative public-status publication using this worker.
        repository_processes = {
            "inspection_state": "unavailable", "error_type": type(exc).__name__,
            "monotonic": time.monotonic(), "processes": [],
        }
    process_epoch, process_epoch_source, process_epoch_anchor = _process_epoch_continuity(
        previous, current, boot_id=boot_id, same_boot=same_boot,
    )
    old_processes = previous.get("repository_processes")
    process_io = repository_process_io_deltas(
        old_processes if isinstance(old_processes, dict) else None,
        repository_processes, same_host_epoch=process_epoch,
    )
    process_io["epoch_continuity_source"] = process_epoch_source
    process_io["epoch_continuity_anchor_unit"] = process_epoch_anchor
    filesystem = _filesystem_space()
    old_filesystem = previous.get("filesystem") if process_epoch else None
    filesystem_available_delta = _filesystem_counter_delta(
        old_filesystem, filesystem, "available_bytes",
    )
    filesystem_free_delta = _filesystem_counter_delta(
        old_filesystem, filesystem, "free_bytes_including_reserved",
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
        completed = props.get("ActiveState") not in {"active", "activating", "deactivating"}
        journal_attempt = (
            _journal_last_completed_process(unit)
            if completed and props.get("last_attempt_seconds") is None
            else None
        )
        # A finished oneshot retains its InvocationID and cgroup counters.
        # Subtracting two idle snapshots yields a misleading "0 cores" for a
        # job that actually took substantial CPU during its last execution.
        live_process = same_process and not completed and elapsed is not None
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
        "repository_processes": repository_processes,
    }
    state_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = state_path.with_name(f".{state_path.name}.{uuid.uuid4().hex}.tmp")
    try:
        encoded = json.dumps(payload, separators=(",", ":"), allow_nan=False)
        if len(encoded.encode("utf-8")) > BASELINE_MAX_BYTES:
            raise ValueError("runtime baseline exceeds size budget; no services truncated")
        temporary.write_text(encoded, encoding="utf-8")
        os.replace(temporary, state_path)
    finally:
        temporary.unlink(missing_ok=True)
    return {
        "event": "all_service_runtime_sample",
        "schema_version": SCHEMA_VERSION,
        "observed_at_utc": datetime.now(UTC).isoformat(),
        "interval_seconds": round(elapsed, 3) if elapsed is not None else None,
        "query_ms": (round(float(current["query_ms"]), 3)
                     if current.get("query_ms") is not None else None),
        "service_count": len(rows),
        "baseline_state": baseline_state,
        "recurring_timers": recurring_timers,
        "repository_process_io": process_io,
        "filesystem": {
            **(filesystem or {}),
            "available_delta_bytes": filesystem_available_delta,
            "free_delta_bytes_including_reserved": filesystem_free_delta,
            "used_delta_bytes": -filesystem_free_delta if filesystem_free_delta is not None else None,
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
            "to any cgroup I/O counter. Free bytes including reserved blocks and "
            "the corresponding used-byte change remain observable after ordinary "
            "available bytes reach zero; reserved blocks are not an acquisition budget "
            "and do not change admission floors. A restart or boot change yields null deltas."
            " Repository process I/O separately includes unmanaged context candidates, "
            "requires paired PID/starttime and epoch proof, includes waited-for "
            "children, and must not be summed with unit or other process counters."
        ),
        "services": rows,
    }


__all__ = ["runtime_sample_journal_lines", "sample_service_runtime"]
