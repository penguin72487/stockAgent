"""A complete current discovery response can survive a local metadata error."""
from contextlib import closing
from datetime import UTC, datetime, timedelta
import json
import uuid

import pytest

from downloader.artifact_io import atomic_write_json
from downloader.tej_history import DESKTOP_INPUT_CONTRACT, connect, task_request
from downloader.tej_scheduler import finished_discovery_response, recover_complete_local_response
from test_tej_history import FakeBridge, registry


@pytest.fixture
def discovery(registry):
    _, root, _, _ = registry
    with closing(connect(root)) as con:
        task = dict(con.execute("SELECT * FROM tasks WHERE kind='discover'").fetchone())
    request = task_request(root, task)
    started = datetime.now(UTC) - timedelta(seconds=10)
    with closing(connect(root)) as con, con:
        con.execute("UPDATE tasks SET state='running',attempted_at_utc=? WHERE task_id=?", (started.isoformat(), task["task_id"]))
    name = task["task_id"] + "-" + uuid.uuid4().hex
    prepared = root / "requests" / (name + ".json")
    atomic_write_json(prepared, {**request, "task_id": task["task_id"], "desktop_input_contract": DESKTOP_INPUT_CONTRACT})
    payload, old_output, _ = FakeBridge().execute(root, {**task, "request_json": json.dumps(request)})
    output = root / "raw" / prepared.name
    payload["observed_at_utc"] = datetime.now(UTC).isoformat()
    atomic_write_json(output, payload)
    old_output.unlink()
    progress = root / "progress" / (prepared.name + ".progress.json")
    atomic_write_json(progress, dict(contract="tej_native_readback_progress_v1", task_id=task["task_id"],
        attempt_id=name, stage="validating_and_saving", observed_at_utc=datetime.now(UTC).isoformat()))
    atomic_write_json(root / "worker_status.json", dict(state="waiting_metadata", observed_at_utc=datetime.now(UTC).isoformat()))
    return root, task, prepared, output, progress


def test_current_complete_discovery_is_adopted_without_any_desktop_action(discovery, monkeypatch):
    root, task, _, output, _ = discovery
    monkeypatch.setattr("downloader.tej_history.DesktopBridge.execute", lambda *_: pytest.fail("No desktop action allowed"))
    assert finished_discovery_response(root, task["task_id"]) == output
    assert recover_complete_local_response(root)
    with closing(connect(root)) as con:
        assert con.execute("SELECT state FROM tasks WHERE task_id=?", (task["task_id"],)).fetchone()[0] == "complete"
        assert con.execute("SELECT count(*) FROM desktop_attempts").fetchone()[0] == 0
    assert output.is_file()
    assert not recover_complete_local_response(root)


@pytest.mark.parametrize("event_state", ["running", "source_validation_failed"])
def test_recovery_closes_only_its_running_traffic_and_preserves_recorded_failures(discovery, event_state):
    root, task, _, _, _ = discovery
    with closing(connect(root)) as con, con:
        started = con.execute("SELECT attempted_at_utc FROM tasks WHERE task_id=?", (task["task_id"],)).fetchone()[0]
        con.execute("INSERT INTO traffic VALUES('owned','discover',?,NULL,?)", (started, event_state))
        con.execute("INSERT INTO traffic VALUES('other','discover','2004-01-01',NULL,'running')")
    assert recover_complete_local_response(root)
    with closing(connect(root)) as con:
        expected = "complete" if event_state == "running" else event_state
        assert con.execute("SELECT state FROM traffic WHERE event_id='owned'").fetchone()[0] == expected
        assert con.execute("SELECT state FROM traffic WHERE event_id='other'").fetchone()[0] == "running"


def test_ambiguous_traffic_ownership_is_not_recovered(discovery):
    root, task, _, _, _ = discovery
    with closing(connect(root)) as con, con:
        started = con.execute("SELECT attempted_at_utc FROM tasks WHERE task_id=?", (task["task_id"],)).fetchone()[0]
        for event in ("one", "two"):
            con.execute("INSERT INTO traffic VALUES(?,'discover',?,NULL,'running')", (event, started))
    assert not recover_complete_local_response(root)


@pytest.mark.parametrize("fault", ["missing_response", "wrong_progress_attempt", "wrong_table", "stale_clock",
    "live_bridge", "unknown_discovery", "duplicate_prepared", "partial_progress", "data_query_identity"])
def test_incomplete_or_unbound_discovery_remains_blocked(discovery, fault):
    root, task, prepared, output, progress = discovery
    if fault == "missing_response":
        output.unlink()
    elif fault in {"wrong_table", "stale_clock", "data_query_identity"}:
        payload = json.loads(output.read_text())
        if fault == "wrong_table": payload["table"] = "Another source"
        elif fault == "stale_clock": payload["observed_at_utc"] = "2004-01-01T00:00:00+00:00"
        else: payload["query_attempt_id"] = prepared.stem
        atomic_write_json(output, payload)
        worker = json.loads((root / "worker_status.json").read_text())
        worker["observed_at_utc"] = datetime.now(UTC).isoformat()
        atomic_write_json(root / "worker_status.json", worker)
    elif fault in {"wrong_progress_attempt", "partial_progress"}:
        step = json.loads(progress.read_text())
        if fault == "wrong_progress_attempt": step["attempt_id"] = "another-attempt"
        else: step["stage"] = "reading_company_list"
        atomic_write_json(progress, step)
    elif fault == "live_bridge":
        atomic_write_json(root / "worker_status.json", dict(state="running", observed_at_utc=datetime.now(UTC).isoformat()))
    elif fault == "unknown_discovery":
        with closing(connect(root)) as con, con:
            con.execute("UPDATE tasks SET state='blocked',last_error_code='unknown_outcome_no_auto_retry' WHERE task_id=?", (task["task_id"],))
    elif fault == "duplicate_prepared":
        duplicate = root / "requests" / (task["task_id"] + "-" + uuid.uuid4().hex + ".json")
        duplicate.write_bytes(prepared.read_bytes())
    assert not recover_complete_local_response(root)
    with closing(connect(root)) as con:
        assert con.execute("SELECT state FROM tasks WHERE task_id=?", (task["task_id"],)).fetchone()[0] != "complete"
