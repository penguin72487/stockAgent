"""Recover proven metadata failures without replaying an unknown Preview.

Caller holds the canonical dataset lock. A negative source outcome identifies
the failed plan; a fresh owned-interface readback licenses only a delayed retry
of that exact plan. Original requests, failures and receipts remain untouched.
"""
from __future__ import annotations

from contextlib import closing
from datetime import UTC, datetime, timedelta
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import subprocess
import uuid

from downloader.artifact_io import atomic_write_json
from downloader.tej_operation_clock import CONTRACT as CLOCK_CONTRACT, OperationClockError, source_operation_clock

CONTRACT = "exact_metadata_only_table_retry_window_v1"
RECONCILIATION_CONTRACT = "exact_negative_plan_failure_commit_reconciliation_v1"
REQUIRES_RECOVERY = "metadata_preparation_requires_recovery"
RETRY_SCHEDULED = "metadata_retry_scheduled"
FAILURE_CODES = (
    REQUIRES_RECOVERY, "unknown_outcome_no_auto_retry",
    "metadata_preparation_failed_deferred", "vendor_metadata_allocation_failed_deferred",
)


def metadata_recovery_policy(config: dict) -> dict | None:
    contract = config.get("metadata_retry_contract")
    if contract is None:
        return None  # Legacy isolation is not permission to requeue a plan.
    base = config.get("metadata_retry_base_seconds", 60)
    maximum = config.get("metadata_retry_max_seconds", 900)
    if (contract != CONTRACT or config.get("metadata_failure_isolation") is not True
            or type(base) is not int or type(maximum) is not int
            or not 5 <= base <= maximum <= 3600):
        raise ValueError("Invalid bounded metadata-only retry policy")
    return {"contract": contract, "base_seconds": base, "max_seconds": maximum,
            "operation_clock_contract": CLOCK_CONTRACT,
            "acknowledge_known_notices": config.get("acknowledge_known_metadata_runtime_notices") is True}


def _read(path: Path, limit: int = 2 * 1024**2) -> dict:
    if path.is_symlink() or not path.is_file() or path.stat().st_size > limit:
        raise ValueError("Bounded original metadata evidence required")
    value = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(value, dict):
        raise ValueError("Object metadata evidence required")
    return value


def _clock(value: str, *, future: bool = False) -> datetime:
    observed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if observed.tzinfo is None or not future and observed > datetime.now(UTC) + timedelta(seconds=5):
        raise ValueError("Aware non-future metadata evidence clock required")
    return observed


def failed_plan_proof(root: Path, task: dict) -> dict:
    """An absent Preview is insufficient: require its exact negative outcome."""
    from downloader.tej_history import SOURCE_SCOPE_CONTRACT
    if (task["kind"] != "discover" or task["state"] != "blocked"
            or task["last_error_code"] not in FAILURE_CODES
            or task["scope_contract"] != SOURCE_SCOPE_CONTRACT
            or task.get("active_attempt_id") is not None
            or any(task.get(k) is not None for k in ("actual_rows", "receipt_path", "completed_at_utc"))):
        raise ValueError("Only an exact failed metadata plan is recoverable here")
    started = _clock(task["attempted_at_utc"])
    # Proof inspection never runs schema DDL or changes journal mode while a
    # collector commits. This is also usable by read-only acceptance tooling.
    with closing(sqlite3.connect((root/'queue.sqlite3').resolve().as_uri()+'?mode=ro',uri=True,timeout=2)) as con:
        con.row_factory=sqlite3.Row
        events = con.execute("SELECT * FROM traffic WHERE action='discover' AND started_at_utc=?",
                             (task["attempted_at_utc"],)).fetchall()
        if len(events) != 1 or events[0]["state"] != "failed" or con.execute(
                "SELECT 1 FROM desktop_attempts WHERE task_id=?", (task["task_id"],)).fetchone():
            raise ValueError("One finished metadata failure, never a Preview attempt, required")
    finished = _clock(events[0]["completed_at_utc"])
    return _plan_failure_evidence(root, task, started, finished, events[0]["event_id"])


def _plan_failure_evidence(root: Path, task: dict, started: datetime,
                           finished: datetime, event_id: str) -> dict:
    """The same exact source proof is required before and after its DB commit."""
    from downloader.tej_desktop_attempts import prepared_request_matches
    from downloader.tej_history import CONTRACT_VERSION, DESKTOP_INPUT_CONTRACT, task_request
    if not 0 <= (finished - started).total_seconds() <= 1200:
        raise ValueError("Metadata failure is outside its finite attempt window")
    request = task_request(root, task)
    candidates = []
    for path in (root / "requests").glob(task["task_id"] + "-*.json"):
        if not started.timestamp() <= path.stat().st_mtime <= finished.timestamp():
            continue
        original = _read(path)
        if original.get("action") == "plan":
            candidates.append((path, original))
    if len(candidates) != 1:
        raise ValueError("One unambiguous original plan request required")
    prepared, original = candidates[0]
    if (not re.fullmatch(re.escape(task["task_id"]) + r"-[0-9a-f]{32}\.json", prepared.name)
            or request.get("action") != "plan" or request.get("contract_version") != CONTRACT_VERSION
            or original.get("desktop_input_contract") != DESKTOP_INPUT_CONTRACT
            or original.get("query_attempt_id") is not None
            or not prepared_request_matches(original, request, task)):
        raise ValueError("Original metadata source scope differs")
    output = root / "raw" / prepared.name
    outcome_path = output.with_suffix(".json.outcome.json")
    outcome = _read(outcome_path, 4096)
    clock_evidence = source_operation_clock(outcome["observed_at_utc"], started, finished)
    if (output.exists() or output.with_suffix(".json.stage.json").exists()
            or not started.timestamp() <= outcome_path.stat().st_mtime <= finished.timestamp()
            or outcome.get("provider") != "tej_smart_wizard"
            or outcome.get("contract_version") != CONTRACT_VERSION or outcome.get("action") != "plan"
            or outcome.get("task_id") != task["task_id"]
            or outcome.get("error_code") != "metadata_preparation_failed_before_preview"
            or outcome.get("market_data_query_submission_possible") is not False
            or any(outcome.get(k) != request.get(k) for k in ("type", "smart_id", "table"))):
        raise ValueError("Exact source-bound no-Preview outcome required")
    manifest_path = root / "launches" / (prepared.stem + ".bridge.json")
    manifest = _read(manifest_path, 4096)
    digest = manifest.get("bridge_script_sha256", "")
    release = root / "bridge_releases" / (digest + ".ps1")
    if (manifest.get("contract") != "immutable_bridge_release_bound_to_prepared_request_v1"
            or manifest.get("attempt_id") != prepared.stem
            or manifest.get("request_path") != str(prepared.relative_to(root))
            or manifest.get("request_sha256") != hashlib.sha256(prepared.read_bytes()).hexdigest()
            or not re.fullmatch(r"[0-9a-f]{64}", digest)
            or manifest.get("bridge_release_path") != str(release.relative_to(root))
            or release.is_symlink() or not release.is_file() or release.stat().st_size > 256 * 1024
            or hashlib.sha256(release.read_bytes()).hexdigest() != digest):
        raise ValueError("Original immutable bridge release binding required")
    return {"prepared": prepared, "attempt_id": prepared.stem,
            "prepared_request_sha256": manifest["request_sha256"],
            "prequery_outcome_sha256": hashlib.sha256(outcome_path.read_bytes()).hexdigest(),
            "bridge_script_sha256": digest, "failed_event_id": event_id,
            "source_clock_evidence": clock_evidence}


def uncommitted_plan_failure_proof(root: Path, task: dict) -> dict:
    """Read a caught failure whose SQLite classification transaction rolled back.

    Neither a missing output nor NULL attempt identity is a negative proof.
    Require BOTH the finished bridge's exact negative outcome and the original
    exception record written before the failed transaction. No UI is touched.
    """
    from downloader.tej_history import SOURCE_SCOPE_CONTRACT
    if (task["kind"] != "discover" or task["state"] != "running"
            or task["scope_contract"] != SOURCE_SCOPE_CONTRACT
            or task.get("active_attempt_id") is not None
            or any(task.get(k) is not None for k in ("actual_rows", "receipt_path", "completed_at_utc"))):
        raise ValueError("One uncommitted metadata failure, not a data query, required")
    worker = _read(root / "worker_status.json", 65536)
    if (worker.get("state") != "waiting_metadata"
            or worker.get("task_id", task["task_id"]) != task["task_id"]
            or worker.get("kind", "discover") != "discover"):
        raise ValueError("Finished metadata-busy worker annotation required")
    started = _clock(task["attempted_at_utc"])
    caught = _clock(worker["observed_at_utc"])
    with closing(sqlite3.connect((root / 'queue.sqlite3').resolve().as_uri() + '?mode=ro',
                                uri=True, timeout=2)) as con:
        con.row_factory = sqlite3.Row
        events = con.execute("SELECT * FROM traffic WHERE action='discover' AND started_at_utc=?",
                             (task["attempted_at_utc"],)).fetchall()
        if (len(events) != 1 or events[0]["state"] != "running" or events[0]["completed_at_utc"] is not None
                or con.execute("SELECT COUNT(*) FROM tasks WHERE state='running'").fetchone()[0] != 1
                or con.execute("SELECT 1 FROM desktop_attempts WHERE task_id=?", (task["task_id"],)).fetchone()):
            raise ValueError("Unique uncommitted metadata traffic, never a Preview, required")
    event_id = events[0]["event_id"]
    if not re.fullmatch(r"[0-9a-f]{32}", event_id):
        raise ValueError("Exact failure diagnostic event identity required")
    diagnostic_path = root / "diagnostics" / (event_id + "-adoption.json")
    diagnostic = _read(diagnostic_path, 16384)
    failed = _clock(diagnostic["observed_at_utc"])
    if (diagnostic.get("task_id") != task["task_id"]
            or diagnostic.get("exception_type") != "MetadataPreparationError"
            or diagnostic.get("state") not in {REQUIRES_RECOVERY, "unknown_outcome_no_auto_retry"}
            or diagnostic.get("data_query_repeated") is not False
            or not started <= failed <= caught
            or not started.timestamp() <= diagnostic_path.stat().st_mtime <= caught.timestamp()):
        raise ValueError("Original caught metadata exception and finite failure clock required")
    proof = _plan_failure_evidence(root, task, started, failed, event_id)
    if worker.get("bridge_attempt_id", proof["attempt_id"]) != proof["attempt_id"]:
        raise ValueError("Worker annotation refers to another source action")
    return {**proof, "failed_at_utc": failed.isoformat(), "error_code": diagnostic["state"],
            "failure_diagnostic_path": str(diagnostic_path.relative_to(root)),
            "failure_diagnostic_sha256": hashlib.sha256(diagnostic_path.read_bytes()).hexdigest()}


def reconcile_uncommitted_plan_failure(root: Path) -> bool:
    """Finish ONLY the rolled-back local classification; never requeue a task.

    Caller holds .download.lock. The existing metadata policy still requires
    a fresh owned-interface check before any retry or healthy-source operation.
    """
    from downloader.tej_history import connect
    with closing(connect(root)) as con:
        rows = con.execute("SELECT * FROM tasks WHERE state='running' LIMIT 2").fetchall()
    if len(rows) != 1 or rows[0]["kind"] != "discover":
        return False
    task = dict(rows[0])
    try:
        proof = uncommitted_plan_failure_proof(root, task)
    except (ValueError, OSError, KeyError, TypeError):
        return False
    audit_path = root / "diagnostics" / (proof["attempt_id"] + "-failure_commit-" + uuid.uuid4().hex + ".json")
    audit = {"contract": RECONCILIATION_CONTRACT, "task_id": task["task_id"],
             "observed_at_utc": datetime.now(UTC).isoformat(), "original_evidence_preserved": True,
             "provider_queries_sent": 0, "task_requeued": False, "source_rows_adopted": False,
             **{k: v for k, v in proof.items() if k != "prepared"}}
    atomic_write_json(audit_path, audit, durable=True)
    with closing(connect(root)) as con, con:
        current = dict(con.execute("SELECT * FROM tasks WHERE task_id=?", (task["task_id"],)).fetchone())
        if uncommitted_plan_failure_proof(root, current) != proof:
            raise ValueError("Original metadata failure changed during reconciliation")
        if con.execute("UPDATE tasks SET state='blocked',last_error_code=? WHERE task_id=? "
                       "AND state='running' AND attempted_at_utc=? AND active_attempt_id IS NULL",
                       (proof["error_code"], task["task_id"], task["attempted_at_utc"])).rowcount != 1:
            raise ValueError("Metadata task changed before classification commit")
        if con.execute("UPDATE traffic SET state='failed',completed_at_utc=? WHERE event_id=? "
                       "AND state='running' AND completed_at_utc IS NULL",
                       (proof["failed_at_utc"], proof["failed_event_id"])).rowcount != 1:
            raise ValueError("Metadata traffic changed before classification commit")
        con.execute("UPDATE tables SET state='needs_review',last_error_code=? WHERE table_id=?",
                    (proof["error_code"], task["table_id"]))
        con.execute("INSERT OR REPLACE INTO meta VALUES (?,?)", (
            "metadata_failure_reconciliation." + task["task_id"],
            json.dumps({**audit, "audit_path": str(audit_path.relative_to(root))})))
    atomic_write_json(root / "worker_status.json", {"contract_version": 4,
        "state": proof["error_code"], "kind": "discover", "task_id": task["task_id"],
        "observed_at_utc": datetime.now(UTC).isoformat(), "metadata_failure_reconciled": True}, durable=True)
    return True


def recover_failed_metadata(root: Path, bridge, *, policy: dict | None,
                            task_id: str | None = None) -> dict | None:
    """One bounded recovery per cycle; bad evidence never licenses UI mutation."""
    if policy is None:
        return None
    if metadata_recovery_policy({"metadata_retry_contract": policy.get("contract"),
            "metadata_failure_isolation": True, "metadata_retry_base_seconds": policy.get("base_seconds"),
            "metadata_retry_max_seconds": policy.get("max_seconds")}) is None:
        raise ValueError("Explicit metadata retry policy required")
    from downloader.tej_history import connect, isolate_failed_metadata, mark_worker_wait
    with closing(connect(root)) as con:
        if con.execute("SELECT 1 FROM tasks WHERE state='running' LIMIT 1").fetchone():
            return None
        unknowns = con.execute("SELECT * FROM tasks WHERE state='blocked' AND "
                              "last_error_code='unknown_outcome_no_auto_retry'").fetchall()
        # Several proved metadata failures can be repaired serially, but one
        # unknown data query or missing proof forbids changing the shared UI.
        try:
            for row in unknowns:
                failed_plan_proof(root, dict(row))
        except (ValueError, OSError, KeyError, TypeError):
            return None
        rows = con.execute("SELECT * FROM tasks WHERE state='blocked' AND kind='discover' "
                           "AND last_error_code IN (?,?,?,?) "
                           + ("AND task_id=? " if task_id else "")
                           + "ORDER BY CASE WHEN last_error_code IN (?,?) THEN 0 ELSE 1 END,priority,task_id",
                           (*FAILURE_CODES, *((task_id,) if task_id else ()),
                            REQUIRES_RECOVERY, "unknown_outcome_no_auto_retry")).fetchall()
        task = proof = None
        now = datetime.now(UTC)
        for row in rows:
            candidate = dict(row)
            key = "metadata_recovery." + candidate["task_id"]
            saved = con.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
            previous = json.loads(saved[0]) if saved else {}
            if previous.get("contract") == CONTRACT and previous.get("attempted_at_utc") == candidate["attempted_at_utc"]:
                # Recheck an old LOCAL validation failure once after the clock
                # validator changes. Source/UI failures retain their cooldown;
                # a code upgrade is not permission to reset a source retry.
                same_validator = (previous.get("state") != "metadata_evidence_requires_review"
                                  or previous.get("evidence_clock_contract") == CLOCK_CONTRACT)
                if same_validator and now < _clock(previous["next_check_at_utc"], future=True) <= now + timedelta(seconds=policy["max_seconds"]):
                    continue
            try:
                proof = failed_plan_proof(root, candidate)
            except (ValueError, OSError, KeyError, TypeError) as exc:
                # Do not parse the same large legacy plan on every healthy
                # download. Failed local evidence checks have the same durable
                # bounded clock, but never grant a query or clear a barrier.
                count = previous.get('failures',0)
                count = count if type(count) is int and 0 <= count <= 100000 else 0
                seconds = min(policy['max_seconds'],policy['base_seconds']*2**min(count,12))
                check = {'contract':CONTRACT,'state':'metadata_evidence_requires_review',
                         'attempted_at_utc':candidate['attempted_at_utc'],'task_id':candidate['task_id'],
                         'observed_at_utc':now.isoformat(),'failures':count+1,
                         'next_check_at_utc':(now+timedelta(seconds=seconds)).isoformat(),
                         'exception_type':type(exc).__name__,'data_query_repeated':False,
                         'evidence_clock_contract':CLOCK_CONTRACT,
                         'failure_reason_code':str(exc) if isinstance(exc,OperationClockError)
                             else 'metadata_source_evidence_unverified'}
                with con:
                    con.execute('INSERT OR REPLACE INTO meta VALUES (?,?)',(key,json.dumps(check)))
                if candidate['last_error_code'] in {REQUIRES_RECOVERY, 'unknown_outcome_no_auto_retry'}:
                    atomic_write_json(root / 'metadata_recovery_status.json', check, durable=True)
                continue
            task = candidate
            break
    if task is None:
        return None
    failures = previous.get("failures", 0) if previous.get("attempt_id") == proof["attempt_id"] else 0
    failures = failures if type(failures) is int and 0 <= failures <= 100000 else 0
    seconds = min(policy["max_seconds"], policy["base_seconds"] * 2**min(failures, 12))
    status = {"contract": CONTRACT, "state": "checking_metadata_interface", "task_id": task["task_id"],
              "attempt_id": proof["attempt_id"], "attempted_at_utc": task["attempted_at_utc"],
              "observed_at_utc": now.isoformat(), "failures": failures + 1,
              "next_check_at_utc": (now + timedelta(seconds=seconds)).isoformat(),
              "data_query_repeated": False, "source_rows_adopted": False,
              "evidence_clock_contract": CLOCK_CONTRACT,
              "source_clock_evidence": proof["source_clock_evidence"]}
    def save():
        with closing(connect(root)) as con, con:
            con.execute("INSERT OR REPLACE INTO meta VALUES (?,?)", (key, json.dumps(status)))
        atomic_write_json(root / "metadata_recovery_status.json", status, durable=True)
    save()  # A killed recovery still retains its cooldown on the next start.
    mark_worker_wait(root, kind="discover", seconds=900, metadata_recovery=True)
    try:
        isolated = isolate_failed_metadata(root, task["task_id"], bridge, proof["prepared"],
            acknowledge_known_notices=policy["acknowledge_known_notices"], require_fresh_evidence=True,
            classify_failure=False)
        with closing(connect(root)) as con, con:
            current = dict(con.execute("SELECT * FROM tasks WHERE task_id=?", (task["task_id"],)).fetchone())
            rechecked = failed_plan_proof(root, current)
            if any(rechecked[k] != proof[k] for k in proof):
                raise ValueError("Original metadata evidence changed during recovery")
            previous_retry = con.execute("SELECT * FROM desktop_retry_windows WHERE table_id=?", (task["table_id"],)).fetchone()
            count = previous_retry["consecutive_failures"] + 1 if previous_retry else 1
            seconds = min(policy["max_seconds"], policy["base_seconds"] * 2**min(count - 1, 12))
            due = (datetime.now(UTC) + timedelta(seconds=seconds)).isoformat()
            audit_path = root / "diagnostics" / (proof["attempt_id"] + "-metadata_retry-" + uuid.uuid4().hex + ".json")
            status.update(state=RETRY_SCHEDULED, next_attempt_at_utc=due, next_check_at_utc=due,
                          backoff_seconds=seconds, consecutive_failures=count,
                          isolation_audit_path=isolated["audit_path"], original_evidence_preserved=True,
                          audit_path=str(audit_path.relative_to(root)))
            audit = {**status, **{k: v for k, v in proof.items() if k != "prepared"}}
            atomic_write_json(audit_path, audit, durable=True)
            con.execute("INSERT OR REPLACE INTO desktop_retry_windows VALUES (?,?,?,?,?,?,?)",
                        (task["table_id"], task["task_id"], proof["attempt_id"], count, due,
                         RETRY_SCHEDULED, str(audit_path.relative_to(root))))
            con.execute("UPDATE tasks SET state='pending',last_error_code=?,next_attempt_at_utc=? WHERE task_id=?",
                        (RETRY_SCHEDULED, due, task["task_id"]))
            con.execute("UPDATE tables SET state='waiting_local_retry',last_error_code=? WHERE table_id=?",
                        (RETRY_SCHEDULED, task["table_id"]))
            barrier = con.execute("SELECT value FROM meta WHERE key='desktop_interface_recovery_required'").fetchone()
            if barrier and json.loads(barrier[0]).get("reason") == "vendor_query_disabled_without_known_notice":
                # The same pinned interface has now been independently read
                # back as stable. Do not leave an old disabled-UI latch behind.
                con.execute("DELETE FROM meta WHERE key='desktop_interface_recovery_required'")
    except (ValueError, OSError, RuntimeError, subprocess.SubprocessError, KeyError, TypeError) as exc:
        status.update(state="waiting_metadata_interface", exception_type=type(exc).__name__,
                      next_check_at_utc=(datetime.now(UTC) + timedelta(seconds=seconds)).isoformat())
    status["observed_at_utc"] = datetime.now(UTC).isoformat()
    save()
    return status
