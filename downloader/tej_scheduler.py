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
import sqlite3
import subprocess
import time
from pathlib import Path
from threading import Event
from typing import Callable

from downloader.artifact_io import atomic_write_json
from downloader.dataset_lock import DatasetLockTimeout, exclusive_dataset_lock
from downloader.tej_history import CONTRACT_VERSION, connect, recover_evidence, run_one

CONTRACT = "persistent_serial_evidence_preserving_supervision_v1"
PROGRESS_STATES = frozenset({
    "completed_task", "source_key_layout_replanned", "source_capacity_replanned",
    "metadata_preparation_failed_deferred", "vendor_metadata_allocation_failed_deferred",
    "source_validation_failed_deferred",
    "prequery_failure_deferred",
    "api_scope_repartitioned",
})
SAFETY_STATES = frozenset({
    "desktop_interface_recovery_required", "inflight_requires_recovery",
    "unknown_outcome_no_auto_retry", "source_validation_failed", "local_storage_failed",
    "source_key_layout_replan_required", "date_input_prequery_needs_review",
    "source_period_replan_required",
    "source_capacity_requires_review",
    "list_selection_prequery_needs_review", "query_activation_prequery_needs_review",
})


@dataclass(frozen=True)
class WatchPolicy:
    idle_seconds: float = 60
    blocked_seconds: float = 60
    heartbeat_seconds: float = 30
    minimum_interval_seconds: float = 2
    interval_contract: str = 'minimum_completion_gap_v1'

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
        values['interval_contract'] = config.get('query_interval_contract','minimum_completion_gap_v1')
        if values['interval_contract'] not in ('minimum_completion_gap_v1','minimum_query_start_interval_v1'):
            raise ValueError('Unreviewed TEJ query interval contract')
        return cls(**values)


def remaining_query_interval(config: dict, elapsed_seconds: float) -> float:
    """Rate-limiter time already spent on the complete query is not idle time.

    A legacy finish-gap config keeps its original semantics. No overlap or
    parallel owner is added; retries and safety waits use their own clocks.
    """
    contract = config.get('query_interval_contract','minimum_completion_gap_v1')
    interval = config.get('minimum_export_interval_seconds',2)
    if (isinstance(interval,bool) or not isinstance(interval,(int,float))
            or not math.isfinite(interval) or not 0 < interval <= 300):
        raise ValueError('Invalid TEJ query interval')
    if contract == 'minimum_completion_gap_v1':
        return interval
    if contract != 'minimum_query_start_interval_v1':
        raise ValueError('Unreviewed TEJ query interval contract')
    if not math.isfinite(elapsed_seconds) or elapsed_seconds < 0:
        raise ValueError('Invalid monotonic query elapsed time')
    return max(0.0,interval-elapsed_seconds)


def finished_discovery_response(root: Path, task_id: str) -> Path | None:
    """Locate the completed current metadata readback, never repeat discovery.

    Discovery does not register a Preview attempt. Its prepared request, exact
    progress identity, complete response and failure clock must therefore agree
    before passing the response to the canonical evidence adopter.
    """
    import re
    from downloader.tej_desktop_attempts import prepared_request_matches
    from downloader.tej_history import CONTRACT_VERSION, DESKTOP_INPUT_CONTRACT, SOURCE_SCOPE_CONTRACT, task_request
    worker_path = root / "worker_status.json"
    if not worker_path.is_file():
        return None
    worker = json.loads(worker_path.read_text())
    if not isinstance(worker, dict) or worker.get("state") != "waiting_metadata":
        return None
    with closing(connect(root)) as con:
        task_row = con.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()
        if task_row is None:
            return None
        task = dict(task_row)
        if (task["kind"] != "discover" or task["state"] != "running"
                or task["scope_contract"] != SOURCE_SCOPE_CONTRACT or task.get("active_attempt_id") is not None
                or any(task.get(k) is not None for k in ("actual_rows", "receipt_path", "completed_at_utc"))
                or con.execute("SELECT count(*) FROM tasks WHERE state='running'").fetchone()[0] != 1
                or con.execute("SELECT 1 FROM desktop_attempts WHERE task_id=?", (task_id,)).fetchone()):
            return None
    started = datetime.fromisoformat(task["attempted_at_utc"])
    failed = datetime.fromisoformat(worker["observed_at_utc"])
    if (started.tzinfo is None or failed.tzinfo is None or failed > datetime.now(UTC)
            or not 0 <= (failed - started).total_seconds() <= 900):
        return None
    candidates = [p for p in (root / "requests").glob(task_id + "-*.json")
                  if started.timestamp() <= p.stat().st_mtime]
    if len(candidates) != 1 or candidates[0].stat().st_mtime > failed.timestamp():
        return None
    prepared = candidates[0]
    if not re.fullmatch(re.escape(task_id) + r"-[0-9a-f]{32}", prepared.stem):
        return None
    request = task_request(root, task)
    original = json.loads(prepared.read_text())
    if (not isinstance(original, dict) or request.get("action") != "plan" or request.get("contract_version") != CONTRACT_VERSION
            or original.get("desktop_input_contract") != DESKTOP_INPUT_CONTRACT
            or original.get("query_attempt_id") is not None
            or not prepared_request_matches(original, request, task)):
        return None
    output = root / "raw" / prepared.name
    progress = root / "progress" / (prepared.name + ".progress.json")
    if any(not p.is_file() or p.stat().st_size > 64 * 1024**2 or not (
            started.timestamp() <= p.stat().st_mtime <= failed.timestamp()) for p in (output, progress)):
        return None
    payload, step = json.loads(output.read_text()), json.loads(progress.read_text())
    if (not isinstance(payload, dict) or not isinstance(step, dict)
            or step.get("contract") != "tej_native_readback_progress_v1"
            or step.get("task_id") != task_id or step.get("attempt_id") != prepared.stem
            or step.get("stage") != "validating_and_saving"
            or payload.get("task_id") != task_id or payload.get("action") != "plan"
            or payload.get("contract_version") != CONTRACT_VERSION
            or any(payload.get(k) != request.get(k) for k in ("type", "smart_id", "table", "fields"))
            or payload.get("query_attempt_id") is not None):
        return None
    for timestamp in (payload.get("observed_at_utc"), step.get("observed_at_utc")):
        if not isinstance(timestamp, str):
            return None
        observed = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        if observed.tzinfo is None or not started <= observed <= failed:
            return None
    return output


def recover_complete_local_response(root: Path) -> bool:
    """Adopt only a finished current-attempt response; never invoke the desktop.

    Called under the canonical dataset lock. Existing strict adoption validates
    prepared request, stage, scope, keys, units and receipt hashes. An absent or
    invalid response remains an unresolved source action, not permission to retry.
    """
    with closing(connect(root)) as con:
        rows = con.execute(
            "SELECT task_id,kind,active_attempt_id FROM tasks WHERE kind IN ('download','discover') AND "
            "(state='running' OR (state='blocked' AND last_error_code='unknown_outcome_no_auto_retry'))"
        ).fetchall()
    recovered = False
    for row in rows:
        if row["kind"] == "discover":
            try:
                output = finished_discovery_response(root, row["task_id"])
                if output is not None:
                    recover_evidence(root, row["task_id"], output)
                    recovered = True
            except (ValueError, OSError, RuntimeError, TypeError):
                pass  # No complete current readback means the barrier stays.
            continue
        attempt = row["active_attempt_id"]
        if not isinstance(attempt, str) or not attempt.startswith(row["task_id"] + "-"):
            from downloader.tej_desktop_attempts import recover_unlaunched_metadata_claim
            try:
                recovered = recover_unlaunched_metadata_claim(root, row['task_id']) or recovered
            except (ValueError, OSError, RuntimeError):
                pass  # NULL alone never proves an unsent source action.
            continue
        from downloader.tej_desktop_attempts import recover_prequery_outcome
        try:
            if recover_prequery_outcome(root,row['task_id']):
                recovered = True
                continue
        except (ValueError,OSError,RuntimeError):
            continue  # Invalid/ambiguous evidence never licenses a query replay.
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
            "query_interval_contract": policy.interval_contract,
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
                    from downloader.tej_desktop_attempts import EXHAUSTED_PREQUERY_ERRORS, defer_unsent_prequery
                    if barrier in EXHAUSTED_PREQUERY_ERRORS and config.get('prequery_failure_isolation') is True:
                        with closing(connect(root)) as con:
                            row=con.execute("SELECT task_id FROM tasks WHERE state='blocked' AND last_error_code=? "
                                "ORDER BY priority,task_id LIMIT 1",(barrier,)).fetchone()
                        try:
                            defer_unsent_prequery(root,row['task_id'],bridge,
                                base_seconds=config.get('prequery_retry_base_seconds',60),
                                max_seconds=config.get('prequery_retry_max_seconds',900))
                        except (ValueError,OSError,RuntimeError,subprocess.SubprocessError):
                            pass  # Unfinished/unknown/foreign evidence never licenses a new query.
                        barrier=local_barrier()
                    if barrier:
                        last_result = paused_reason = barrier
                        wait_state, delay = "waiting_recovery", policy.blocked_seconds
                    else:
                        paused_reason = None
                        from downloader.tej_value_priority import refresh_if_due
                        refresh_if_due(root)
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
                        query_started = time.monotonic()
                        last_result = runner(root, bridge)
                        delay = remaining_query_interval(config,time.monotonic()-query_started)
                        cycles += 1
                        emit(json.dumps({"event": "tej_automatic_task", "state": last_result, "cycle": cycles}), flush=True)
                        if last_result == "completed_task":
                            completed += 1
                            last_completed = datetime.now(UTC).isoformat()
                        elif last_result == "prequery_retry_scheduled":
                            wait_state, delay = "waiting_local_retry", 5
                        elif last_result == 'local_metadata_busy':
                            wait_state, delay = 'waiting_metadata', 5
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
            except sqlite3.OperationalError as exc:
                from downloader.tej_desktop_attempts import metadata_busy
                if not metadata_busy(exc):
                    raise
                # Release the dataset lock and wait without a source retry.
                # Next cycle reconciles the exact response/negative proof.
                last_result = 'local_metadata_busy'
                wait_state, delay = 'waiting_metadata', 5
                atomic_write_json(root/'worker_status.json',{'contract_version':CONTRACT_VERSION,
                    'state':'waiting_metadata','observed_at_utc':datetime.now(UTC).isoformat()})
            wait(wait_state, delay)
    finally:
        publish("stopped")
    return 0
