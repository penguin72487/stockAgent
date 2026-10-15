"""A busy metadata commit must not strand a proven unsent plan forever."""
from contextlib import closing
from datetime import UTC, datetime
import json
import sqlite3
import uuid

import pytest

from downloader.artifact_io import atomic_write_json
from downloader.tej_history import configure_runtime_policy, connect, run_one
from downloader.tej_scheduler import recover_complete_local_response, watch_queue
from test_tej_history import registry
from test_tej_metadata_recovery import (
    CONFIG, FreshBridge, NegativePlanBridge, failed_metadata,
)


@pytest.fixture
def uncommitted_failure(failed_metadata):
    _, root, task, prepared, outcome, *_ = failed_metadata
    failed = datetime.now(UTC).isoformat()
    event_id = uuid.uuid4().hex
    diagnostic = root / "diagnostics" / (event_id + "-adoption.json")
    atomic_write_json(diagnostic, {
        "task_id": task["task_id"], "state": "metadata_preparation_requires_recovery",
        "exception_type": "MetadataPreparationError", "observed_at_utc": failed,
        "data_query_repeated": False,
    })
    with closing(connect(root)) as con, con:
        con.execute("UPDATE tasks SET state='running',last_error_code='metadata_retry_scheduled'")
        con.execute("UPDATE traffic SET event_id=?,state='running',completed_at_utc=NULL", (event_id,))
    atomic_write_json(root / "worker_status.json", {
        "state": "waiting_metadata", "observed_at_utc": datetime.now(UTC).isoformat(),
    })
    return root, task, prepared, outcome, diagnostic


def test_uncommitted_negative_plan_is_reconciled_without_desktop_or_requery(uncommitted_failure, monkeypatch):
    root, task, prepared, outcome, diagnostic = uncommitted_failure
    original = [p.read_bytes() for p in (prepared, outcome, diagnostic)]
    monkeypatch.setattr("downloader.tej_history.DesktopBridge.execute",
                        lambda *_: pytest.fail("Failure reconciliation must be entirely local"))
    assert recover_complete_local_response(root)
    with closing(connect(root)) as con:
        row = con.execute("SELECT * FROM tasks WHERE task_id=?", (task["task_id"],)).fetchone()
        assert row["state"] == "blocked" and row["last_error_code"] == "metadata_preparation_requires_recovery"
        assert row["actual_rows"] is None and row["receipt_path"] is None
        event = con.execute("SELECT * FROM traffic WHERE event_id=?",
                            (diagnostic.name.removesuffix("-adoption.json"),)).fetchone()
        assert event["state"] == "failed"
        assert event["completed_at_utc"] == json.loads(diagnostic.read_text())["observed_at_utc"]
        assert con.execute("SELECT COUNT(*) FROM desktop_attempts").fetchone()[0] == 0
    assert [p.read_bytes() for p in (prepared, outcome, diagnostic)] == original
    assert not recover_complete_local_response(root)


def test_scheduler_reconciles_then_uses_existing_interface_and_retry_policy(uncommitted_failure):
    root, task, *_ = uncommitted_failure
    config = {**CONFIG, "automation": {
        "enabled": True, "idle_poll_seconds": 1, "blocked_poll_seconds": 1, "heartbeat_seconds": 1,
    }, "minimum_export_interval_seconds": 1}
    bridge = FreshBridge()
    assert watch_queue(root, bridge, config, runner=lambda *_: "idle", max_cycles=1,
                       emit=lambda *a, **k: None) == 0
    assert bridge.calls == ["inspect_notices", "confirm_metadata_error_cleared"]
    with closing(connect(root)) as con:
        assert con.execute("SELECT state FROM tasks WHERE task_id=?", (task["task_id"],)).fetchone()[0] == "pending"


def test_proved_reconciliation_does_not_wait_an_extra_blocked_poll(uncommitted_failure):
    root, task, *_ = uncommitted_failure
    class StopAfterFirstWait:
        stopped = False

        def is_set(self):
            return self.stopped

        def wait(self, seconds):
            with closing(connect(root)) as con:
                assert con.execute("SELECT state FROM tasks WHERE task_id=?",
                                   (task["task_id"],)).fetchone()[0] == "pending"
            self.stopped = True
            return True

    config = {**CONFIG, "automation": {"enabled": True}}
    assert watch_queue(root, FreshBridge(), config, runner=lambda *_: "idle", stop=StopAfterFirstWait(),
                       max_cycles=1, emit=lambda *a, **k: None) == 0


@pytest.mark.parametrize("fault", [
    "missing_outcome", "missing_diagnostic", "different_diagnostic_task", "wrong_exception",
    "query_possible", "active_attempt", "wrong_scope", "duplicate_event", "late_outcome",
    "live_bridge", "foreign_worker", "saved_response", "preview_stage",
])
def test_unproved_unfinished_work_is_not_classified_or_replayed(uncommitted_failure, fault):
    root, task, prepared, outcome, diagnostic = uncommitted_failure
    if fault in {"missing_outcome", "missing_diagnostic"}:
        (outcome if fault == "missing_outcome" else diagnostic).unlink()
    elif fault in {"different_diagnostic_task", "wrong_exception"}:
        doc = json.loads(diagnostic.read_text())
        doc["task_id" if fault == "different_diagnostic_task" else "exception_type"] = "foreign"
        atomic_write_json(diagnostic, doc)
    elif fault in {"query_possible", "late_outcome"}:
        doc = json.loads(outcome.read_text())
        doc["market_data_query_submission_possible" if fault == "query_possible" else "observed_at_utc"] = (
            True if fault == "query_possible" else "2099-01-01T00:00:00+00:00")
        atomic_write_json(outcome, doc)
    elif fault in {"active_attempt", "duplicate_event"}:
        with closing(connect(root)) as con, con:
            if fault == "active_attempt":
                con.execute("UPDATE tasks SET active_attempt_id=?", (prepared.stem,))
            else:
                con.execute("INSERT INTO traffic SELECT 'duplicate',action,started_at_utc,completed_at_utc,state FROM traffic")
    elif fault == "wrong_scope":
        doc = json.loads(prepared.read_text()); doc["fields"] = ["Foreign field"]
        atomic_write_json(prepared, doc)
    elif fault in {"live_bridge", "foreign_worker"}:
        worker = json.loads((root / "worker_status.json").read_text())
        worker["state" if fault == "live_bridge" else "task_id"] = "running" if fault == "live_bridge" else "foreign"
        atomic_write_json(root / "worker_status.json", worker)
    else:
        atomic_write_json(root / "raw" / (prepared.name + (".stage.json" if fault == "preview_stage" else "")), {})
    assert not recover_complete_local_response(root)
    with closing(connect(root)) as con:
        assert con.execute("SELECT state FROM tasks WHERE task_id=?", (task["task_id"],)).fetchone()[0] == "running"


def test_real_reader_contention_retains_identity_and_recovers_after_unlock(registry, monkeypatch):
    import downloader.tej_history as history
    _, root, _, config = registry
    configure_runtime_policy(root, {**config, **CONFIG})
    original_connect = history.connect
    reader = sqlite3.connect((root / "queue.sqlite3").resolve().as_uri() + "?mode=ro", uri=True)

    def short_commit_wait(directory):
        con = original_connect(directory)
        con.execute("PRAGMA busy_timeout=10")
        return con

    class ReaderContendedPlan(NegativePlanBridge):
        def execute(self, directory, task):
            try:
                return super().execute(directory, task)
            except history.MetadataPreparationError:
                reader.execute("BEGIN")
                reader.execute("SELECT task_id FROM tasks LIMIT 1").fetchone()
                monkeypatch.setattr(history, "connect", short_commit_wait)
                raise

    try:
        bridge = ReaderContendedPlan()
        assert run_one(root, bridge) == "local_metadata_busy"
        worker = json.loads((root / "worker_status.json").read_text())
        assert worker["state"] == "waiting_metadata"
        assert worker["kind"] == "discover" and worker["task_id"]
        assert worker["owner_pid"] > 0 and worker["owner_start_ticks"].isdecimal()
    finally:
        reader.close()
    assert recover_complete_local_response(root)
    assert bridge.calls == ["plan"]  # Reconciliation sends no source request.
