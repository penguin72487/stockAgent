"""Metadata failure recovery must not weaken the unknown Preview boundary."""
from contextlib import closing
from datetime import UTC, datetime, timedelta
import hashlib
import json
import uuid

import pytest

from downloader.artifact_io import atomic_write_json
from downloader.tej_history import (DESKTOP_INPUT_CONTRACT, DesktopBridge, MetadataPreparationError,
                                    configure_runtime_policy, connect, run_one, task_request)
from downloader.tej_metadata_recovery import (
    CONTRACT, RETRY_SCHEDULED, failed_plan_proof, metadata_recovery_policy, recover_failed_metadata,
)
from downloader.tej_scheduler import watch_queue
from test_tej_history import FakeBridge, MetadataRecoveryBridge, registry, runtime_notice


CONFIG = {"metadata_retry_contract": CONTRACT, "metadata_failure_isolation": True,
          "metadata_retry_base_seconds": 60, "metadata_retry_max_seconds": 900,
          "acknowledge_known_metadata_runtime_notices": True}


@pytest.fixture
def failed_metadata(registry):
    repo, root, _, _ = registry
    with closing(connect(root)) as con:
        task = dict(con.execute("SELECT * FROM tasks WHERE kind='discover'").fetchone())
    started = datetime.now(UTC) - timedelta(seconds=10)
    name = task["task_id"] + "-" + uuid.uuid4().hex
    prepared = root / "requests" / (name + ".json")
    request = task_request(root, task)
    atomic_write_json(prepared, {**request, "task_id": task["task_id"], "desktop_input_contract": DESKTOP_INPUT_CONTRACT})
    outcome = root / "raw" / (prepared.name + ".outcome.json")
    atomic_write_json(outcome, {"action": "plan", "provider": "tej_smart_wizard", "contract_version": 4,
        "task_id": task["task_id"], **{k: request[k] for k in ("type", "smart_id", "table")},
        "market_data_query_submission_possible": False, "error_code": "metadata_preparation_failed_before_preview",
        "observed_at_utc": datetime.now(UTC).isoformat()})
    release_code = b"# immutable test metadata bridge"
    digest = hashlib.sha256(release_code).hexdigest()
    release = root / "bridge_releases" / (digest + ".ps1")
    release.parent.mkdir(exist_ok=True)
    release.write_bytes(release_code)
    manifest = root / "launches" / (name + ".bridge.json")
    atomic_write_json(manifest, {"contract": "immutable_bridge_release_bound_to_prepared_request_v1",
        "attempt_id": name, "request_path": str(prepared.relative_to(root)),
        "request_sha256": hashlib.sha256(prepared.read_bytes()).hexdigest(),
        "bridge_release_path": str(release.relative_to(root)), "bridge_script_sha256": digest})
    finished = datetime.now(UTC)
    with closing(connect(root)) as con, con:
        con.execute("UPDATE tasks SET state='blocked',attempted_at_utc=?,last_error_code='unknown_outcome_no_auto_retry' "
                    "WHERE task_id=?", (started.isoformat(), task["task_id"]))
        con.execute("INSERT INTO traffic VALUES ('original','discover',?,?,'failed')", (started.isoformat(), finished.isoformat()))
        task = dict(con.execute("SELECT * FROM tasks WHERE task_id=?", (task["task_id"],)).fetchone())
    return repo, root, task, prepared, outcome, manifest, release


class FreshBridge(MetadataRecoveryBridge):
    def __init__(self, *, fail_once=False, notices=None, stale=False):
        super().__init__(notices=notices)
        self.fail_once, self.stale = fail_once, stale

    def execute(self, root, task):
        if self.fail_once:
            self.fail_once = False
            self.calls.append(json.loads(task["request_json"])["action"])
            raise RuntimeError("finite_desktop_deadline_exceeded")
        payload, output, seconds = super().execute(root, task)
        payload["observed_at_utc"] = (datetime.now(UTC) - timedelta(hours=1) if self.stale else datetime.now(UTC)).isoformat()
        atomic_write_json(output, payload)
        return payload, output, seconds


def expire_probe(root, task):
    key = "metadata_recovery." + task["task_id"]
    with closing(connect(root)) as con, con:
        saved = json.loads(con.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()[0])
        saved["next_check_at_utc"] = (datetime.now(UTC) - timedelta(seconds=1)).isoformat()
        con.execute("UPDATE meta SET value=? WHERE key=?", (json.dumps(saved), key))


def test_timeout_is_rechecked_after_restart_without_preview_or_lost_evidence(failed_metadata):
    _, root, task, prepared, outcome, _, _ = failed_metadata
    original = prepared.read_bytes(), outcome.read_bytes()
    bridge = FreshBridge(fail_once=True)
    policy = metadata_recovery_policy(CONFIG)
    first = recover_failed_metadata(root, bridge, policy=policy)
    assert first["state"] == "waiting_metadata_interface"
    assert recover_failed_metadata(root, FreshBridge(), policy=policy) is None  # Durable restart cooldown.
    expire_probe(root, task)
    result = recover_failed_metadata(root, bridge, policy=policy)
    assert result["state"] == RETRY_SCHEDULED and result["backoff_seconds"] == 60
    assert bridge.calls == ["inspect_notices", "inspect_notices", "confirm_metadata_error_cleared"]
    assert (prepared.read_bytes(), outcome.read_bytes()) == original
    with closing(connect(root)) as con:
        row = con.execute("SELECT * FROM tasks WHERE task_id=?", (task["task_id"],)).fetchone()
        assert row["state"] == "pending" and row["actual_rows"] is None and row["receipt_path"] is None
        assert con.execute("SELECT state FROM traffic WHERE event_id='original'").fetchone()[0] == "failed"
        assert con.execute("SELECT count(*) FROM desktop_attempts").fetchone()[0] == 0
    assert run_one(root, FreshBridge()) == "idle"  # Not due; never re-run plan immediately.


def test_completed_metadata_retry_releases_window_for_new_downloads(failed_metadata):
    _, root, task, *_ = failed_metadata
    assert recover_failed_metadata(root, FreshBridge(), policy=metadata_recovery_policy(CONFIG))["state"] == RETRY_SCHEDULED
    with closing(connect(root)) as con, con:
        past = (datetime.now(UTC) - timedelta(seconds=1)).isoformat()
        con.execute("UPDATE tasks SET next_attempt_at_utc=?", (past,))
        con.execute("UPDATE desktop_retry_windows SET next_attempt_at_utc=?", (past,))
    assert run_one(root, FakeBridge()) == "completed_task"
    with closing(connect(root)) as con:
        assert con.execute("SELECT count(*) FROM desktop_retry_windows").fetchone()[0] == 0
        assert con.execute("SELECT count(*) FROM tasks WHERE kind='download' AND state='pending'").fetchone()[0] > 0
    assert run_one(root, FakeBridge()) == "completed_task"


@pytest.mark.parametrize("fault", ["missing_outcome", "wrong_table", "possible_query", "action", "stale_outcome",
    "preview_stage", "preview_result", "active_attempt", "kind", "fields", "missing_release", "release_hash",
    "manifest_scope", "duplicate_request", "ambiguous_failure", "running"])
def test_unproved_metadata_never_probes_or_requeues(failed_metadata, fault):
    _, root, task, prepared, outcome, manifest, release = failed_metadata
    if fault == "missing_outcome":
        outcome.unlink()
    elif fault in {"wrong_table", "possible_query", "action", "stale_outcome"}:
        value = json.loads(outcome.read_text())
        key, change = {"wrong_table": ("table", "Another source"), "possible_query": ("market_data_query_submission_possible", True),
            "action": ("action", "download"), "stale_outcome": ("observed_at_utc", "2004-01-01T00:00:00Z")}[fault]
        value[key] = change
        atomic_write_json(outcome, value)
    elif fault in {"preview_stage", "preview_result"}:
        atomic_write_json(root / "raw" / (prepared.name + (".stage.json" if fault == "preview_stage" else "")), {})
    elif fault in {"active_attempt", "kind", "running"}:
        column, value = {"active_attempt": ("active_attempt_id", prepared.stem), "kind": ("kind", "download"),
                         "running": ("state", "running")}[fault]
        with closing(connect(root)) as con, con:
            con.execute(f"UPDATE tasks SET {column}=?", (value,))
    elif fault == "fields":
        value = json.loads(prepared.read_text()); value["fields"] = ["Foreign field"]
        atomic_write_json(prepared, value)
    elif fault == "missing_release":
        release.unlink()
    elif fault == "release_hash":
        release.write_bytes(b"changed code")
    elif fault == "manifest_scope":
        value = json.loads(manifest.read_text()); value["attempt_id"] = "different-attempt"
        atomic_write_json(manifest, value)
    elif fault == "duplicate_request":
        atomic_write_json(prepared.parent / (task["task_id"] + "-" + uuid.uuid4().hex + ".json"), json.loads(prepared.read_text()))
    elif fault == "ambiguous_failure":
        with closing(connect(root)) as con, con:
            con.execute("INSERT INTO traffic SELECT 'duplicate',action,started_at_utc,completed_at_utc,state FROM traffic")
    # Move the failure boundary beyond fixture edits, as with a real failure.
    with closing(connect(root)) as con, con:
        con.execute("UPDATE traffic SET completed_at_utc=?", (datetime.now(UTC).isoformat(),))
    bridge = FreshBridge()
    assert recover_failed_metadata(root, bridge, policy=metadata_recovery_policy(CONFIG)) is None
    assert bridge.calls == []


@pytest.mark.parametrize("notice", [runtime_notice("Authentication required"), runtime_notice("Quota exceeded"),
    runtime_notice("Unknown error"), runtime_notice(owner=999)])
def test_foreign_auth_quota_notices_are_not_acknowledged(failed_metadata, notice):
    _, root, task, *_ = failed_metadata
    bridge = FreshBridge(notices=[notice])
    result = recover_failed_metadata(root, bridge, policy=metadata_recovery_policy(CONFIG))
    assert result["state"] == "waiting_metadata_interface" and bridge.calls == ["inspect_notices"]
    with closing(connect(root)) as con:
        assert con.execute("SELECT state FROM tasks WHERE task_id=?", (task["task_id"],)).fetchone()[0] == "blocked"


def test_stale_interface_proof_preserves_barrier(failed_metadata):
    _, root, _, *_ = failed_metadata
    bridge = FreshBridge(stale=True)
    assert recover_failed_metadata(root, bridge, policy=metadata_recovery_policy(CONFIG))["state"] == "waiting_metadata_interface"
    assert bridge.calls == ["inspect_notices"]


class NegativePlanBridge(FreshBridge):
    def execute(self, root, task):
        request = task_request(root, task)
        if request["action"] != "plan":
            return super().execute(root, task)
        self.calls.append("plan")
        name = task["task_id"] + "-" + uuid.uuid4().hex
        path = root / "requests" / (name + ".json")
        atomic_write_json(path, {**request, "task_id": task["task_id"], "desktop_input_contract": DESKTOP_INPUT_CONTRACT})
        from pathlib import Path
        DesktopBridge(Path.cwd(), {}).script_release(root, name, path)
        atomic_write_json(root / "raw" / (path.name + ".outcome.json"), {
            "action": "plan", "provider": "tej_smart_wizard", "contract_version": 4, "task_id": task["task_id"],
            **{k: request[k] for k in ("type", "smart_id", "table")},
            "market_data_query_submission_possible": False, "error_code": "metadata_preparation_failed_before_preview",
            "observed_at_utc": datetime.now(UTC).isoformat()})
        raise MetadataPreparationError(path)


@pytest.mark.parametrize("timeout", [False, True])
def test_worker_classifies_proved_metadata_separately_and_scheduler_can_resume(registry, timeout):
    _, root, _, config = registry
    configure_runtime_policy(root, {**config, **CONFIG})
    bridge = NegativePlanBridge(fail_once=timeout)
    expected = "metadata_preparation_requires_recovery" if timeout else RETRY_SCHEDULED
    assert run_one(root, bridge) == expected
    with closing(connect(root)) as con:
        task = dict(con.execute("SELECT * FROM tasks WHERE kind='discover'").fetchone())
        assert task["last_error_code"] == expected
    if timeout:
        assert run_one(root, FreshBridge()) == 'metadata_preparation_requires_recovery'
        expire_probe(root, task)
        assert recover_failed_metadata(root, bridge, policy=metadata_recovery_policy(CONFIG))["state"] == RETRY_SCHEDULED
    assert bridge.calls.count("plan") == 1
    assert "download" not in bridge.calls


def test_repeated_metadata_failures_keep_exponential_table_backoff_and_original_events(registry):
    _, root, _, config = registry
    configure_runtime_policy(root, {**config, **CONFIG})
    bridge = NegativePlanBridge()
    for expected in (60, 120, 240):
        assert run_one(root, bridge) == RETRY_SCHEDULED
        with closing(connect(root)) as con, con:
            retry = con.execute("SELECT * FROM desktop_retry_windows").fetchone()
            audit = json.loads((root / retry["audit_path"]).read_text())
            assert audit["backoff_seconds"] == expected
            past = (datetime.now(UTC) - timedelta(seconds=1)).isoformat()
            con.execute("UPDATE tasks SET next_attempt_at_utc=?", (past,))
            con.execute("UPDATE desktop_retry_windows SET next_attempt_at_utc=?", (past,))
    with closing(connect(root)) as con:
        assert con.execute("SELECT count(*) FROM traffic WHERE state='failed'").fetchone()[0] == 3
    assert bridge.calls.count("plan") == 3
    assert len(list((root / "raw").glob("*.outcome.json"))) == 3


@pytest.mark.parametrize("reason", ["vendor_query_disabled_without_known_notice", "explicit_operator_scratch_query_reset"])
def test_only_the_verified_disabled_metadata_latch_is_released(failed_metadata, reason):
    _, root, _, *_ = failed_metadata
    with closing(connect(root)) as con, con:
        con.execute("INSERT INTO meta VALUES ('desktop_interface_recovery_required',?)", (json.dumps({"reason": reason}),))
    assert recover_failed_metadata(root, FreshBridge(), policy=metadata_recovery_policy(CONFIG))["state"] == RETRY_SCHEDULED
    with closing(connect(root)) as con:
        barrier = con.execute("SELECT 1 FROM meta WHERE key='desktop_interface_recovery_required'").fetchone()
        assert bool(barrier) is (reason != "vendor_query_disabled_without_known_notice")


def test_scheduler_repairs_old_metadata_barrier_without_operator(failed_metadata):
    _, root, task, *_ = failed_metadata
    config = {**CONFIG, "automation": {"enabled": True, "idle_poll_seconds": 1, "blocked_poll_seconds": 1,
                                      "heartbeat_seconds": 1}, "minimum_export_interval_seconds": 1}
    assert watch_queue(root, FreshBridge(), config, runner=lambda *_: "idle", max_cycles=1, emit=lambda *a, **k: None) == 0
    with closing(connect(root)) as con:
        assert con.execute("SELECT state FROM tasks WHERE task_id=?", (task["task_id"],)).fetchone()[0] == "pending"


@pytest.mark.parametrize('task_state,expected',[('pending','metadata_retry_ready'),('running','metadata_retry_running'),
    ('complete','metadata_recovered'),('blocked',None)])
def test_public_recovery_binds_real_task_state_and_never_shows_expired_eta(failed_metadata,task_state,expected):
    from stockagent.live.tej_dashboard import _scheduler_public
    _,root,task,*_=failed_metadata
    result=recover_failed_metadata(root,FreshBridge(),policy=metadata_recovery_policy(CONFIG))
    result['next_check_at_utc']=(datetime.now(UTC)-timedelta(seconds=1)).isoformat()
    result['private_diagnostic']='secret-path'
    atomic_write_json(root/'metadata_recovery_status.json',result)
    atomic_write_json(root/'scheduler_status.json',{'contract':'persistent_serial_evidence_preserving_supervision_v1',
        'continuous':True,'state':'stopped','metadata_recovery':metadata_recovery_policy(CONFIG)})
    with closing(connect(root)) as con,con:
        con.execute('UPDATE tasks SET state=? WHERE task_id=?',(task_state,task['task_id']))
    public=_scheduler_public(root,datetime.now(UTC))['metadata_recovery']
    assert public['state']==expected and public['next_check_at_utc'] is None
    assert 'secret-path' not in json.dumps(public) and 'audit_path' not in json.dumps(public)


@pytest.mark.parametrize("change", [{"metadata_retry_contract": "unreviewed"}, {"metadata_failure_isolation": False},
    {"metadata_retry_base_seconds": True}, {"metadata_retry_max_seconds": 0}, {"metadata_retry_max_seconds": 100000}])
def test_invalid_recovery_policy_is_rejected_before_source_action(change):
    with pytest.raises(ValueError):
        metadata_recovery_policy({**CONFIG, **change})
    assert metadata_recovery_policy({}) is None
