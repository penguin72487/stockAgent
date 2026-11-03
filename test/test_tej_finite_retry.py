"""Finite menu retries must retain exact source and isolation boundaries."""
from contextlib import closing
from datetime import UTC, datetime, timedelta
import json
from pathlib import Path

import pytest

from downloader.artifact_io import atomic_write_json
from downloader import tej_finite_retry
from downloader.tej_history import _prepare_ready_task, _ready_task, connect, run_one
from downloader.tej_scheduler import queue_wait
from test_tej_finite_axis_priority import finite_plan, submit
from test_tej_planning import DynamicBridge, registry


@pytest.fixture
def finite_retry(registry, tmp_path, monkeypatch):
    batch = finite_plan(registry, tmp_path, monkeypatch)
    root, plan, proof, plan_path, receipt_path = batch
    plan.update(finite_axis_only=True, source_observation=plan['requests'][0]['finite_axis_observation'],
                provider_queries_by_planning=0, full_axis_admitted_to_plan=False,
                training_input_admitted=False)
    receipt = submit(batch)
    with closing(connect(root)) as con, con:
        discovery = dict(con.execute("SELECT * FROM tasks WHERE kind='discover'").fetchone())
        con.execute("UPDATE tasks SET state='pending',last_error_code='metadata_retry_scheduled',"
                    "next_attempt_at_utc='2099-01-01T00:00:00+00:00' WHERE task_id=?", (discovery['task_id'],))
        con.execute("UPDATE tables SET state='waiting_local_retry',last_error_code='metadata_retry_scheduled' "
                    "WHERE table_id=?", (discovery['table_id'],))
        con.execute('INSERT INTO desktop_retry_windows VALUES(?,?,?,?,?,?,?)',
                    (discovery['table_id'], discovery['task_id'], proof['query_attempt_id'], 1,
                     '2099-01-01T00:00:00+00:00', 'metadata_retry_scheduled', 'diagnostics/fixture.json'))
        retry = dict(con.execute('SELECT * FROM desktop_retry_windows').fetchone())
    monkeypatch.setattr(tej_finite_retry, '_retry_proof', lambda *_: retry)
    return root, plan_path, receipt_path, receipt, retry


def test_only_verified_finite_rectangle_passes_metadata_retry(finite_retry):
    root, plan, receipt_path, receipt, retry = finite_retry
    now = '2026-10-29T00:00:00+00:00'
    with closing(connect(root)) as con:
        assert _ready_task(con, now, 'download', {}, retry['table_id']) is None
    result = tej_finite_retry.enroll(root, plan, receipt_path)
    assert result['tasks'] == 2 and result['provider_queries'] == 0
    assert result['metadata_retry_preserved'] and not result['full_axis_admitted']
    assert tej_finite_retry.enroll(root, plan, receipt_path) == result
    with closing(connect(root)) as con, con:
        ready, _ = _prepare_ready_task(con, root, now, 'download', {}, retry['table_id'])
        assert ready['task_id'] in {item['task_id'] for item in receipt['tasks']}
        assert dict(con.execute('SELECT * FROM desktop_retry_windows').fetchone()) == retry
        con.execute("INSERT INTO tasks(task_id,table_id,kind,phase,priority,request_json) "
                    "VALUES('unverified',?,'download','P1',-99,'{}')", (retry['table_id'],))
        assert _ready_task(con, now, 'download', {}, retry['table_id'])['task_id'] != 'unverified'
        con.execute("INSERT INTO tasks(task_id,table_id,kind,phase,priority,request_json,state,last_error_code) "
                    "VALUES('quarantine',?,'download','P1',0,'{}','blocked','source_validation_failed_deferred')",
                    (retry['table_id'],))
        assert _ready_task(con, now, 'download', {}, retry['table_id']) is None


@pytest.mark.parametrize('fault', ['source', 'request', 'receipt', 'plan', 'retry'])
def test_changed_evidence_fails_before_a_desktop_claim(finite_retry, monkeypatch, fault):
    root, plan_path, receipt_path, receipt, retry = finite_retry
    tej_finite_retry.enroll(root, plan_path, receipt_path)
    task_id = sorted(item['task_id'] for item in receipt['tasks'])[0]
    if fault == 'source':
        plan = json.loads(plan_path.read_text())
        source = Path(plan['sources'][0]['path'])
        source.write_bytes(source.read_bytes() + b'changed')
    elif fault == 'request':
        with closing(connect(root)) as con, con:
            con.execute("UPDATE tasks SET request_json='{}' WHERE task_id=?", (task_id,))
    elif fault == 'receipt':
        atomic_write_json(receipt_path, {'changed': True})
    elif fault == 'plan':
        atomic_write_json(plan_path, {'changed': True})
    else:
        def reject(*_):
            raise ValueError('changed metadata retry proof')
        monkeypatch.setattr(tej_finite_retry, '_retry_proof', reject)
    bridge = DynamicBridge()
    assert run_one(root, bridge, kind='download', table_id=retry['table_id']) == 'finite_admission_rejected_before_query'
    assert bridge.requests == []
    with closing(connect(root)) as con:
        assert all(row[0] == 'pending' for row in con.execute("SELECT state FROM tasks WHERE kind='download'"))
        assert dict(con.execute('SELECT * FROM desktop_retry_windows').fetchone()) == retry
        assert con.execute('SELECT 1 FROM meta WHERE key=?', (tej_finite_retry.PREFIX + task_id,)).fetchone() is None
        rejected = con.execute('SELECT value FROM meta WHERE key LIKE ?',
                               (tej_finite_retry.REJECTION_PREFIX + task_id + ':%',)).fetchone()
        record = json.loads(rejected[0])
        assert record['task_id'] == task_id and record['provider_query_submitted'] is False
        assert json.loads(record['original_admission'])['task_id'] == task_id


def test_rejected_task_does_not_prevent_other_verified_work(finite_retry):
    root, plan, receipt_path, receipt, retry = finite_retry
    tej_finite_retry.enroll(root, plan, receipt_path)
    first = sorted(item['task_id'] for item in receipt['tasks'])[0]
    with closing(connect(root)) as con, con:
        con.execute("UPDATE tasks SET request_json='{}' WHERE task_id=?", (first,))
    bridge = DynamicBridge()
    assert run_one(root, bridge, kind='download', table_id=retry['table_id']) == 'finite_admission_rejected_before_query'
    assert bridge.requests == []
    assert run_one(root, bridge, kind='download', table_id=retry['table_id']) == 'completed_task'
    assert len(bridge.requests) == 1
    with closing(connect(root)) as con:
        assert con.execute('SELECT state FROM tasks WHERE task_id=?', (first,)).fetchone()[0] == 'pending'
        assert dict(con.execute('SELECT * FROM desktop_retry_windows').fetchone()) == retry


def test_other_retry_reason_never_uses_finite_admission(finite_retry):
    root, plan, receipt, _, retry = finite_retry
    tej_finite_retry.enroll(root, plan, receipt)
    with closing(connect(root)) as con, con:
        con.execute("UPDATE desktop_retry_windows SET error_code='source_input_failure'")
        assert _ready_task(con, '2026-10-29T00:00:00+00:00', 'download', {}, retry['table_id']) is None


def test_admission_never_reopens_an_already_completed_task(finite_retry):
    root, plan, receipt, items, _ = finite_retry
    completed = items['tasks'][0]['task_id']
    with closing(connect(root)) as con, con:
        con.execute("UPDATE tasks SET state='complete' WHERE task_id=?", (completed,))
    result = tej_finite_retry.enroll(root, plan, receipt)
    assert result['tasks'] == 1 and result['already_complete'] == 1
    with closing(connect(root)) as con:
        assert con.execute('SELECT state FROM tasks WHERE task_id=?', (completed,)).fetchone()[0] == 'complete'
        assert con.execute('SELECT 1 FROM meta WHERE key=?', (tej_finite_retry.PREFIX + completed,)).fetchone() is None


def test_normal_owner_completes_finite_download_without_releasing_full_retry(finite_retry):
    root, plan, receipt, _, retry = finite_retry
    tej_finite_retry.enroll(root, plan, receipt)
    bridge = DynamicBridge()
    assert run_one(root, bridge, kind='download', table_id=retry['table_id']) == 'completed_task'
    assert len(bridge.requests) == 1 and bridge.requests[0]['action'] == 'download'
    with closing(connect(root)) as con:
        assert dict(con.execute('SELECT * FROM desktop_retry_windows').fetchone()) == retry
        assert con.execute('SELECT discovery_path FROM tables').fetchone()[0] is None
        assert tuple(con.execute('SELECT state,last_error_code FROM tables').fetchone()) == (
            'waiting_local_retry', 'metadata_retry_scheduled')


def test_scheduler_wakes_for_finite_task_without_waiting_for_full_retry(finite_retry):
    root, plan, receipt, _, _ = finite_retry
    tej_finite_retry.enroll(root, plan, receipt)
    now = datetime.now(UTC)
    with closing(connect(root)) as con, con:
        con.execute("UPDATE tasks SET next_attempt_at_utc=? WHERE kind='download'",
                    ((now + timedelta(seconds=7)).isoformat(),))
    assert queue_wait(root, now, 60) == pytest.approx(7)
