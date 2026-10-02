"""Persistent supervision of the canonical serial TEJ queue, not another fetcher.

One bounded run_one owns the desktop at a time. Between queries the dataset
lock is released; waits use an Event rather than polling the source or spinning.
A dead/unknown Preview is never changed back to pending by a service restart.
"""
from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import json
import math
import os
from pathlib import Path
from threading import Event
from typing import Callable

from downloader.artifact_io import atomic_write_json
from downloader.dataset_lock import DatasetLockTimeout, exclusive_dataset_lock
from downloader.tej_history import connect, recover_evidence, run_one

CONTRACT = "persistent_serial_evidence_preserving_supervision_v1"
PROGRESS_STATES = frozenset({
    "completed_task", "source_key_layout_replanned",
    "metadata_preparation_failed_deferred", "vendor_metadata_allocation_failed_deferred",
})
SAFETY_STATES = frozenset({
    "desktop_interface_recovery_required", "inflight_requires_recovery",
    "unknown_outcome_no_auto_retry", "source_validation_failed", "local_storage_failed",
    "source_key_layout_replan_required", "date_input_prequery_needs_review",
    "source_period_replan_required",
    "list_selection_prequery_needs_review", "query_activation_prequery_needs_review",
})


@dataclass(frozen=True)
class WatchPolicy:
    idle_seconds: float = 60
    blocked_seconds: float = 60
    heartbeat_seconds: float = 30
    minimum_interval_seconds: float = 2

    @classmethod
    def from_config(cls, config: dict) -> "WatchPolicy":
        automatic = config.get("automation", {})
        if automatic.get("enabled") is not True:
            raise ValueError("TEJ automation requires explicit automation.enabled=true")
        values = {
            "idle_seconds": automatic.get("idle_poll_seconds", 60),
            "blocked_seconds": automatic.get("blocked_poll_seconds", 60),
            "heartbeat_seconds": automatic.get("heartbeat_seconds", 30),
            "minimum_interval_seconds": config.get("minimum_export_interval_seconds", 2),
        }
        for key, value in values.items():
            if (isinstance(value, bool) or not isinstance(value, (int, float))
                    or not math.isfinite(value) or not 0 < value <= 300):
                raise ValueError(f"Invalid TEJ supervision interval: {key}")
        return cls(**values)


def recover_complete_local_response(root: Path) -> bool:
    """Adopt only a finished current-attempt response; never invoke the desktop.

    Called under the canonical dataset lock. Existing strict adoption validates
    prepared request, stage, scope, keys, units and receipt hashes. An absent or
    invalid response remains an unresolved source action, not permission to retry.
    """
    with closing(connect(root)) as con:
        rows = con.execute(
            "SELECT task_id,active_attempt_id FROM tasks WHERE kind='download' AND "
            "(state='running' OR (state='blocked' AND last_error_code='unknown_outcome_no_auto_retry'))"
        ).fetchall()
    recovered = False
    for row in rows:
        attempt = row["active_attempt_id"]
        if not isinstance(attempt, str) or not attempt.startswith(row["task_id"] + "-"):
            continue
        output = root / "raw" / (attempt + ".json")
        if (output.resolve().parent != (root / "raw").resolve() or not output.is_file()
                or output.stat().st_size > 64 * 1024**2):
            continue
        try:
            recover_evidence(root, row["task_id"], output)
        except (ValueError, OSError, RuntimeError):
            continue
        recovered = True
    return recovered


def queue_wait(root: Path, observed: datetime, fallback: float) -> float:
    """Bounded metadata-only next wake; no historical blobs or provider calls."""
    with closing(connect(root)) as con:
        row = con.execute(
            "SELECT MIN(next_attempt_at_utc) FROM tasks WHERE state='pending' "
            "AND next_attempt_at_utc IS NOT NULL"
        ).fetchone()
    if not row or not row[0]:
        return fallback
    try:
        due = datetime.fromisoformat(row[0].replace("Z", "+00:00"))
        if due.tzinfo is None:
            return fallback
        delta = (due - observed).total_seconds()
    except (ValueError, TypeError):
        return fallback
    return min(fallback, max(1.0, delta)) if delta > 0 else fallback


def watch_queue(root: Path, bridge, config: dict, *, stop: Event | None = None,
                runner: Callable = run_one, max_cycles: int | None = None,
                emit: Callable = print, session_path: Path | None = None) -> int:
    """Serve the existing queue until shutdown, with no unsafe automatic replay.

    The outer service owns .scheduler.lock. CLI/manual actions retain their
    existing .download.lock and Windows mutex. A safety pause remains visible
    and is checked locally; a restart does not turn it into a fresh query.
    """
    policy = WatchPolicy.from_config(config)
    stop = stop if stop is not None else Event()
    started = datetime.now(UTC).isoformat()
    owner_ticks = Path(f"/proc/{os.getpid()}/stat").read_text().rsplit(") ", 1)[1].split()[19]
    cycles = completed = 0
    paused_reason: str | None = None
    last_result: str | None = None
    last_completed: str | None = None

    def publish(state: str, *, wait: float | None = None) -> None:
        observed = datetime.now(UTC)
        # A single query/metadata recovery is bounded separately by run_one.
        # This is supervisor liveness, never a renewal of a query's deadline.
        deadline = 1800 if state == "executing" else max(60, policy.heartbeat_seconds * 2)
        atomic_write_json(root / "scheduler_status.json", {
            "contract": CONTRACT, "state": state, "observed_at_utc": observed.isoformat(),
            "deadline_at_utc": (observed + timedelta(seconds=deadline)).isoformat(),
            "owner_pid": os.getpid(), "owner_start_ticks": owner_ticks,
            "started_at_utc": started, "continuous": True,
            "cycles": cycles, "completed_tasks": completed,
            "last_completed_at_utc": last_completed, "last_result": last_result,
            "paused_reason": paused_reason,
            "next_check_at_utc": (observed + timedelta(seconds=wait)).isoformat() if wait is not None else None,
            "unknown_outcome_auto_retry": False,
            "query_deadline_renewed_by_heartbeat": False,
        })

    def wait(state: str, seconds: float) -> None:
        remaining = seconds
        while remaining > 0 and not stop.is_set():
            publish(state, wait=remaining)
            step = min(remaining, policy.heartbeat_seconds)
            stop.wait(step)
            remaining -= step

    def local_barrier() -> str | None:
        with closing(connect(root)) as con:
            if con.execute("SELECT 1 FROM meta WHERE key='desktop_interface_recovery_required'").fetchone():
                return "desktop_interface_recovery_required"
            if con.execute("SELECT 1 FROM meta WHERE key='source_period_replan_required'").fetchone():
                return "source_period_replan_required"
            if con.execute("SELECT 1 FROM tasks WHERE state='running' OR "
                           "(state='blocked' AND last_error_code='unknown_outcome_no_auto_retry') LIMIT 1").fetchone():
                return "inflight_requires_recovery"
            # Read durable classifications on every start too. An in-memory
            # pause alone would be erased by systemd restarting the process.
            codes = tuple(sorted(SAFETY_STATES - {"inflight_requires_recovery", "desktop_interface_recovery_required"}))
            row = con.execute("SELECT last_error_code FROM tasks WHERE state='blocked' AND last_error_code IN (" +
                              ",".join("?" for _ in codes) + ") LIMIT 1", codes).fetchone()
            return row[0] if row else None

    publish("starting")
    try:
        while not stop.is_set() and (max_cycles is None or cycles < max_cycles):
            delay = policy.minimum_interval_seconds
            wait_state = "between_tasks"
            try:
                with exclusive_dataset_lock(root / ".download.lock", provider="tej_smart_wizard", timeout_seconds=0):
                    barrier = local_barrier()
                    if barrier == "inflight_requires_recovery":
                        recover_complete_local_response(root)
                        barrier = local_barrier()
                    if barrier:
                        last_result = paused_reason = barrier
                        wait_state, delay = "waiting_recovery", policy.blocked_seconds
                    else:
                        paused_reason = None
                        if session_path is not None:
                            # Reload only explicitly pinned private identity,
                            # after reconciliation. Never discover or select a
                            # different workbook/session on the service's own.
                            session = json.loads(session_path.read_text())
                            if (set(session) != {'TejProcessId','ExpectedWindow','ExpectedTitle','ExpectedWorkbook','ExpectedExcelWindow'}
                                    or any(not isinstance(session[k],int) or isinstance(session[k],bool) or session[k] <= 0
                                           for k in ('TejProcessId','ExpectedWindow','ExpectedExcelWindow'))
                                    or not isinstance(session['ExpectedWorkbook'],str)
                                    or session['ExpectedTitle'] != 'TEJ Smart Wizard (Version 4.1.1.7) -- ' + session['ExpectedWorkbook']):
                                raise ValueError('Exact private desktop session required')
                            bridge.session = session
                        publish("executing")
                        last_result = runner(root, bridge)
                        cycles += 1
                        emit(json.dumps({"event": "tej_automatic_task", "state": last_result, "cycle": cycles}), flush=True)
                        if last_result == "completed_task":
                            completed += 1
                            last_completed = datetime.now(UTC).isoformat()
                        elif last_result == "prequery_retry_scheduled":
                            wait_state, delay = "waiting_local_retry", 5
                        elif last_result == 'desktop_unavailable':
                            wait_state, delay = 'waiting_desktop', policy.blocked_seconds
                        elif last_result == "idle":
                            wait_state = "waiting_queue"
                            delay = queue_wait(root, datetime.now(UTC), policy.idle_seconds)
                        elif last_result == "local_disk_headroom_low":
                            wait_state, delay = "waiting_storage", policy.blocked_seconds
                        elif last_result in SAFETY_STATES:
                            paused_reason = last_result
                            wait_state, delay = "waiting_recovery", policy.blocked_seconds
                        elif last_result not in PROGRESS_STATES:
                            raise RuntimeError("Unreviewed TEJ worker state; automatic queries stopped")
            except DatasetLockTimeout:
                last_result = "another_writer_active"
                wait_state, delay = "waiting_owner", 5
            wait(wait_state, delay)
    finally:
        publish("stopped")
    return 0
