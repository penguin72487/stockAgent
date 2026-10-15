"""A committed no-Preview failure must survive a rolled-back attempt annotation."""
from contextlib import closing
from datetime import UTC, datetime
import json
from pathlib import Path
import sqlite3
import uuid

import pytest

from downloader.tej_desktop_attempts import finish_attempt, PREQUERY_RETRY_CONTRACT
from downloader.tej_history import connect, DesktopBridge
from downloader.tej_scheduler import watch_queue
from test_tej_prequery_recovery import HealthyReadback, exhausted
from test_tej_preview_capacity import ready as ready, registry as registry
from test_tej_scheduler import InstantWait


def interrupted_annotation(root, monkeypatch):
    started = datetime.now(UTC).isoformat()
    task, outcome, prepared = exhausted(root)
    attempt = task['active_attempt_id']
    DesktopBridge(Path.cwd(), {}).script_release(root, attempt, prepared)
    with closing(connect(root)) as con, con:
        con.execute('UPDATE tasks SET attempted_at_utc=? WHERE task_id=?', (started, task['task_id']))
        task = dict(con.execute('SELECT * FROM tasks WHERE task_id=?', (task['task_id'],)).fetchone())
        con.execute("UPDATE desktop_attempts SET state='prepared',finished_at_utc=NULL WHERE attempt_id=?", (attempt,))
    with monkeypatch.context() as patch:
        def busy(*_):
            raise sqlite3.OperationalError('database is locked')
        patch.setattr('downloader.tej_desktop_attempts._connect', busy)
        finish_attempt(root, attempt, 'proven_not_submitted')
    note = root/'diagnostics'/(attempt+'-attempt_state_deferred.json')
    with closing(connect(root)) as con, con:
        con.execute("INSERT INTO traffic VALUES(?,?,?,?,'failed_before_preview')",
            (uuid.uuid4().hex, 'download', task['attempted_at_utc'], datetime.now(UTC).isoformat()))
    return task, outcome, prepared, note


def test_watch_reconciles_classified_unsent_attempt_without_reset_or_resubmission(ready, monkeypatch):
    root, config, _ = ready
    task, outcome, prepared, note = interrupted_annotation(root, monkeypatch)
    originals = {p: p.read_bytes() for p in (outcome, prepared, note)}
    bridge = HealthyReadback(); calls = []
    config = {**config, 'automation': {'enabled': True}, 'prequery_failure_isolation': True,
        'prequery_recovery_contract': PREQUERY_RETRY_CONTRACT}
    watch_queue(root, bridge, config, stop=InstantWait(limit=2), max_cycles=1,
        runner=lambda *_: calls.append('healthy-work') or 'completed_task', emit=lambda *_, **__: None)
    assert calls == ['healthy-work'] and bridge.calls == 1
    assert all(p.read_bytes() == body for p, body in originals.items())
    with closing(connect(root)) as con:
        assert con.execute('SELECT safe_prequery_retries FROM tasks WHERE task_id=?', (task['task_id'],)).fetchone()[0] == 2
        attempt = con.execute('SELECT state,finished_at_utc FROM desktop_attempts WHERE attempt_id=?',
            (task['active_attempt_id'],)).fetchone()
        assert attempt['state'] == 'proven_not_submitted'
        original_note = json.loads(note.read_text())
        assert attempt['finished_at_utc'] == original_note.get('requested_finished_at_utc', original_note['observed_at_utc'])
    assert json.loads((root/'scheduler_status.json').read_text())['paused_reason'] is None


def test_real_sqlite_reader_rolls_back_only_attempt_annotation_then_recovers(ready, monkeypatch):
    import downloader.tej_desktop_attempts as attempts
    root, _, _ = ready
    started = datetime.now(UTC).isoformat()
    task, outcome, prepared = exhausted(root)
    DesktopBridge(Path.cwd(), {}).script_release(root, task['active_attempt_id'], prepared)
    with closing(connect(root)) as con, con:
        con.execute('UPDATE tasks SET attempted_at_utc=? WHERE task_id=?', (started, task['task_id']))
        task = dict(con.execute('SELECT * FROM tasks WHERE task_id=?', (task['task_id'],)).fetchone())
        con.execute("UPDATE desktop_attempts SET state='prepared',finished_at_utc=NULL WHERE attempt_id=?",
            (task['active_attempt_id'],))
    original_connect = attempts._connect
    def short_wait(directory):
        con = original_connect(directory); con.execute('PRAGMA busy_timeout=10'); return con
    reader = sqlite3.connect((root/'queue.sqlite3').resolve().as_uri()+'?mode=ro', uri=True)
    try:
        reader.execute('BEGIN'); reader.execute('SELECT task_id FROM tasks LIMIT 1').fetchone()
        with monkeypatch.context() as patch:
            patch.setattr(attempts, '_connect', short_wait)
            finish_attempt(root, task['active_attempt_id'], 'proven_not_submitted')
    finally:
        reader.close()
    assert outcome.is_file()
    assert (root/'diagnostics'/(task['active_attempt_id']+'-attempt_state_deferred.json')).is_file()
    with closing(connect(root)) as con, con:
        con.execute("INSERT INTO traffic VALUES(?,?,?,?,'failed_before_preview')",
            (uuid.uuid4().hex, 'download', task['attempted_at_utc'], datetime.now(UTC).isoformat()))
    from downloader.tej_desktop_attempts import defer_unsent_prequery
    result = defer_unsent_prequery(root, task['task_id'], HealthyReadback())
    assert result['state'] == 'prequery_failure_deferred' and result['provider_queries_sent'] == 0


@pytest.mark.parametrize('mutation', ['note_attempt', 'note_state', 'submitted', 'note_clock',
    'response', 'stage', 'query_possible', 'classification', 'traffic_duplicate', 'traffic_unfinished',
    'traffic_clock', 'request', 'manifest', 'release'])
def test_incomplete_or_foreign_annotation_never_unblocks_or_calls_ui(ready, monkeypatch, mutation):
    from downloader.artifact_io import atomic_write_json, atomic_write_bytes
    from downloader.tej_desktop_attempts import defer_unsent_prequery
    root, _, _ = ready; task, outcome, prepared, note = interrupted_annotation(root, monkeypatch)
    attempt = task['active_attempt_id']
    if mutation.startswith('note_') or mutation == 'submitted':
        body = json.loads(note.read_text())
        key, value = {'note_attempt': ('attempt_id', 'foreign'),
            'note_state': ('requested_state', 'unknown_outcome'), 'submitted': ('provider_queries_sent', 1),
            'note_clock': ('requested_finished_at_utc', '2000-01-01T00:00:00+00:00')}[mutation]
        body[key] = value; atomic_write_json(note, body)
    elif mutation in {'response', 'stage'}:
        atomic_write_json(root/'raw'/(attempt+('.json' if mutation == 'response' else '.json.stage.json')), {})
    elif mutation == 'query_possible':
        body = json.loads(outcome.read_text()); body['market_data_query_submission_possible'] = True
        atomic_write_json(outcome, body)
    elif mutation == 'request':
        body = json.loads(prepared.read_text()); body['fields'] = []
        atomic_write_json(prepared, body)
    elif mutation in {'manifest', 'release'}:
        path = root/'launches'/(attempt+'.bridge.json'); body = json.loads(path.read_text())
        if mutation == 'manifest':
            body['request_sha256'] = 'f'*64; atomic_write_json(path, body)
        else:
            atomic_write_bytes(root/body['bridge_release_path'], b'changed original source release')
    else:
        with closing(connect(root)) as con, con:
            if mutation == 'classification':
                con.execute("UPDATE tasks SET last_error_code='date_input_prequery_needs_review'")
            elif mutation == 'traffic_duplicate':
                con.execute("INSERT INTO traffic SELECT ?,action,started_at_utc,completed_at_utc,state FROM traffic "
                    "WHERE action='download' AND started_at_utc=?", (uuid.uuid4().hex, task['attempted_at_utc']))
            elif mutation == 'traffic_unfinished':
                con.execute("UPDATE traffic SET state='running',completed_at_utc=NULL")
            else:
                con.execute("UPDATE traffic SET completed_at_utc='2099-01-01T00:00:00+00:00'")
    bridge = HealthyReadback()
    with pytest.raises((ValueError, OSError)):
        defer_unsent_prequery(root, task['task_id'], bridge)
    assert bridge.calls == 0
    with closing(connect(root)) as con:
        assert con.execute('SELECT state FROM desktop_attempts WHERE attempt_id=?', (attempt,)).fetchone()[0] == 'prepared'
        assert con.execute('SELECT safe_prequery_retries FROM tasks WHERE task_id=?', (task['task_id'],)).fetchone()[0] == 2


def test_annotation_reconciliation_is_local_idempotent_and_keeps_original_clocks(ready, monkeypatch):
    from downloader.tej_desktop_attempts import reconcile_unsent_attempt_annotation
    root, _, _ = ready; task, outcome, prepared, note = interrupted_annotation(root, monkeypatch)
    original = {p: p.read_bytes() for p in (outcome, prepared, note)}
    assert reconcile_unsent_attempt_annotation(root, task['task_id'])
    assert not reconcile_unsent_attempt_annotation(root, task['task_id'])
    assert all(p.read_bytes() == data for p, data in original.items())
    audits = list((root/'local_prequery_recovery').glob('*-annotation-*.json'))
    assert len(audits) == 1
    audit = json.loads(audits[0].read_text())
    assert audit['state'] == 'committed' and audit['provider_queries_sent'] == 0
    assert audit['retry_budget_changed'] is False and audit['query_deadline_renewed'] is False
    with closing(connect(root)) as con:
        assert con.execute('SELECT state FROM tasks WHERE task_id=?', (task['task_id'],)).fetchone()[0] == 'blocked'
        assert not con.execute('SELECT 1 FROM desktop_retry_windows').fetchone()


def test_legacy_busy_annotation_has_a_bounded_original_finish_not_recovery_time(ready, monkeypatch):
    from downloader.artifact_io import atomic_write_json
    from downloader.tej_desktop_attempts import reconcile_unsent_attempt_annotation
    root, _, _ = ready; task, _, _, note = interrupted_annotation(root, monkeypatch)
    body = json.loads(note.read_text()); body.pop('requested_finished_at_utc')
    atomic_write_json(note, body)
    with closing(connect(root)) as con, con:
        con.execute("UPDATE traffic SET completed_at_utc=? WHERE action='download' AND started_at_utc=?",
            (datetime.now(UTC).isoformat(), task['attempted_at_utc']))
    assert reconcile_unsent_attempt_annotation(root, task['task_id'])
    with closing(connect(root)) as con:
        assert con.execute('SELECT finished_at_utc FROM desktop_attempts WHERE attempt_id=?',
            (task['active_attempt_id'],)).fetchone()[0] == body['observed_at_utc']


def test_reconciliation_commit_busy_preserves_original_attempt_until_next_cycle(ready, monkeypatch):
    import downloader.tej_desktop_attempts as attempts
    root, _, _ = ready; task, _, _, _ = interrupted_annotation(root, monkeypatch)
    original_connect = attempts._connect
    def short_wait(directory):
        con = original_connect(directory); con.execute('PRAGMA busy_timeout=10'); return con
    reader = sqlite3.connect((root/'queue.sqlite3').resolve().as_uri()+'?mode=ro', uri=True)
    try:
        reader.execute('BEGIN'); reader.execute('SELECT task_id FROM tasks LIMIT 1').fetchone()
        with monkeypatch.context() as patch:
            patch.setattr(attempts, '_connect', short_wait)
            with pytest.raises(sqlite3.OperationalError):
                attempts.reconcile_unsent_attempt_annotation(root, task['task_id'])
    finally:
        reader.close()
    with closing(connect(root)) as con:
        assert con.execute('SELECT state FROM desktop_attempts WHERE attempt_id=?',
            (task['active_attempt_id'],)).fetchone()[0] == 'prepared'
    assert attempts.reconcile_unsent_attempt_annotation(root, task['task_id'])
