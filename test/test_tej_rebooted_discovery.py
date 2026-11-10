"""A rebooted metadata action is retained as unknown, never replayed."""
from contextlib import closing
from datetime import UTC, datetime, timedelta
import hashlib
import json
import os
from pathlib import Path
import uuid

import pytest

from downloader.artifact_io import atomic_write_json
from downloader.tej_history import DESKTOP_INPUT_CONTRACT, connect, task_request
from downloader import tej_rebooted_discovery as recovery
from test_tej_history import registry


@pytest.fixture
def interrupted(registry, monkeypatch):
    _, root, _, _ = registry
    now = datetime.now(UTC)
    start = now - timedelta(days=3)
    with closing(connect(root)) as con, con:
        task = dict(con.execute("SELECT * FROM tasks WHERE kind='discover'").fetchone())
        con.execute("UPDATE tasks SET state='running',attempted_at_utc=? WHERE task_id=?",
                    (start.isoformat(), task['task_id']))
        con.execute("INSERT INTO traffic VALUES('reboot-fixture','discover',?,NULL,'running')", (start.isoformat(),))
    task['attempted_at_utc'] = start.isoformat()
    attempt = task['task_id'] + '-' + uuid.uuid4().hex
    prepared = root/'requests'/(attempt+'.json')
    atomic_write_json(prepared, {**task_request(root, task), 'task_id': task['task_id'],
                                 'desktop_input_contract': DESKTOP_INPUT_CONTRACT})
    os.utime(prepared, (start.timestamp()+1, start.timestamp()+1))
    code = b'reviewed fixture plan branch; no source action'
    digest = hashlib.sha256(code).hexdigest()
    release = root/'bridge_releases'/(digest+'.ps1')
    release.parent.mkdir(parents=True, exist_ok=True)
    release.write_bytes(code)
    monkeypatch.setattr(recovery, 'REVIEWED_PLAN_BRIDGES', frozenset({digest}))
    manifest = root/'launches'/(attempt+'.bridge.json')
    atomic_write_json(manifest, {'contract':'immutable_bridge_release_bound_to_prepared_request_v1',
        'attempt_id':attempt, 'request_path':str(prepared.relative_to(root)),
        'request_sha256':hashlib.sha256(prepared.read_bytes()).hexdigest(),
        'bridge_script_sha256':digest, 'bridge_release_path':str(release.relative_to(root))})
    worker = {'state':'running', 'kind':'discover', 'task_id':task['task_id'], 'bridge_attempt_id':attempt,
        'owner_pid':99999999, 'owner_start_ticks':'0', 'linux_boot_id':'00000000-0000-0000-0000-000000000000',
        'observed_at_utc':start.isoformat(), 'deadline_at_utc':(start+timedelta(seconds=900)).isoformat()}
    atomic_write_json(root/'worker_status.json', worker)
    return root, task, prepared, manifest, release, now, start+timedelta(days=1)


def test_rebooted_plan_is_quarantined_without_adoption_or_replay(interrupted):
    root, task, prepared, manifest, release, now, boot = interrupted
    originals = [path.read_bytes() for path in (prepared, manifest, release)]
    assert recovery.quarantine(root, now=now, boot_at=boot, dry_run=True)
    with closing(connect(root)) as con:
        assert con.execute('SELECT state FROM tasks WHERE task_id=?', (task['task_id'],)).fetchone()[0] == 'running'
    assert recovery.quarantine(root, now=now, boot_at=boot)
    with closing(connect(root)) as con:
        row = con.execute('SELECT * FROM tasks WHERE task_id=?', (task['task_id'],)).fetchone()
        assert row['state'] == 'blocked' and row['last_error_code'] == recovery.QUARANTINE
        assert row['receipt_path'] is None and row['actual_rows'] is None
        event = con.execute("SELECT * FROM traffic WHERE event_id='reboot-fixture'").fetchone()
        assert event['state'] == 'unknown_after_worker_loss' and event['completed_at_utc'] is None
        assert not con.execute('SELECT 1 FROM desktop_replays').fetchone()
    assert [path.read_bytes() for path in (prepared, manifest, release)] == originals
    audit_path = root/json.loads((root/'worker_status.json').read_text())['audit_path']
    audit = json.loads(audit_path.read_text())
    assert audit['original_outcome'] == 'unknown_not_adopted'
    assert audit['provider_queries_sent'] == 0 and audit['task_requeued'] is False
    assert not recovery.quarantine(root, now=now, boot_at=boot)


@pytest.mark.parametrize('fault', ['boot_before', 'boot_future', 'same_linux_boot', 'wrong_task',
                                   'changed_request', 'changed_bridge', 'unreviewed_bridge',
                                   'output', 'stage', 'outcome', 'duplicate_traffic', 'data_attempt'])
def test_unproved_or_conflicting_source_stays_blocked_in_running_state(interrupted, monkeypatch, fault):
    root, task, prepared, manifest, release, now, boot = interrupted
    if fault == 'boot_before': boot = now-timedelta(days=4)
    elif fault == 'boot_future': boot = now+timedelta(hours=1)
    elif fault in {'same_linux_boot', 'wrong_task'}:
        path = root/'worker_status.json'
        worker = json.loads(path.read_text())
        worker['linux_boot_id' if fault == 'same_linux_boot' else 'task_id'] = (
            Path('/proc/sys/kernel/random/boot_id').read_text().strip() if fault == 'same_linux_boot' else 'other')
        atomic_write_json(path, worker)
    elif fault == 'changed_request': prepared.write_text('{}')
    elif fault == 'changed_bridge': release.write_text('changed')
    elif fault == 'unreviewed_bridge': monkeypatch.setattr(recovery, 'REVIEWED_PLAN_BRIDGES', frozenset())
    elif fault in {'output', 'stage', 'outcome'}:
        suffix = {'output':'', 'stage':'.stage.json', 'outcome':'.outcome.json'}[fault]
        atomic_write_json(root/'raw'/(prepared.name+suffix), {})
    elif fault == 'duplicate_traffic':
        with closing(connect(root)) as con, con:
            con.execute("INSERT INTO traffic VALUES('duplicate','discover',?,NULL,'running')", (task['attempted_at_utc'],))
    elif fault == 'data_attempt':
        with closing(connect(root)) as con, con:
            con.execute("INSERT INTO desktop_attempts(attempt_id,task_id,request_path,raw_path,started_at_utc,state) "
                        "VALUES('unexpected',?,'r','p',?,'running')", (task['task_id'], task['attempted_at_utc']))
    assert not recovery.quarantine(root, now=now, boot_at=boot)
    with closing(connect(root)) as con:
        assert con.execute('SELECT state FROM tasks WHERE task_id=?', (task['task_id'],)).fetchone()[0] == 'running'
