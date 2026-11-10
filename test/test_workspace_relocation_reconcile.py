import json
from pathlib import Path
import sqlite3

import pytest

from stockagent.data_sync import workspace_relocation as copy
from stockagent.data_sync import workspace_relocation_reconcile as reconcile


def _fixture(tmp_path):
    source = tmp_path/'source'; api = source/'api_trial_v1'; api.mkdir(parents=True)
    (source/'unchanged').write_bytes(b'financial source kept exactly')
    with sqlite3.connect(api/'queue.sqlite3') as db:
        db.execute('CREATE TABLE tasks(id INTEGER PRIMARY KEY)')
        db.execute('INSERT INTO tasks VALUES (1)')
    (api/'worker_status.json').write_text('{"state":"old"}')
    destination, evidence, journal = tmp_path/'dest', tmp_path/'proof', tmp_path/'reconciliation'
    copy.copy(source, destination, evidence, workers=2)
    # Simulate a completed file pass before the full namespace proof commits.
    (evidence/'copy-result.json').unlink()
    with sqlite3.connect(api/'queue.sqlite3') as db:
        db.execute('INSERT INTO tasks VALUES (2)')
    replacement = api/'status-new'
    replacement.write_text('{"state":"new"}')
    replacement.replace(api/'worker_status.json')
    return source, destination, evidence, journal


def _tasks(path):
    with sqlite3.connect(path.as_uri()+'?mode=ro', uri=True) as db:
        return [row[0] for row in db.execute('SELECT id FROM tasks ORDER BY id')]


def test_reconcile_preserves_both_old_evidence_and_new_native_queue(tmp_path):
    source, dest, evidence, journal = _fixture(tmp_path)
    result = reconcile.reconcile_subtree(source, dest, evidence, 'api_trial_v1', journal, workers=2)
    assert result['state'] == 'complete' and result['source_files_deleted'] == 0
    assert result['full_relocation_still_requires_verification'] is True
    assert _tasks(source/'api_trial_v1/queue.sqlite3') == [1, 2]
    assert _tasks(dest/'api_trial_v1/queue.sqlite3') == [1, 2]
    assert _tasks(journal/'staged-payload/queue.sqlite3') == [1]
    assert (journal/'index-before.sqlite').is_file()
    assert json.loads((evidence/'intent.json').read_text())['source_reconciliations'][0]['subtree'] == 'api_trial_v1'
    receipt = copy.copy(source, dest, evidence, workers=2)
    assert receipt['source_namespace_unchanged'] and receipt['source_files_deleted'] == 0
    assert (dest/'unchanged').read_bytes() == b'financial source kept exactly'


def test_interrupted_atomic_exchange_can_resume_without_losing_either_generation(tmp_path, monkeypatch):
    source, dest, evidence, journal = _fixture(tmp_path)
    exchange = reconcile.exchange_names
    def interrupt_after_exchange(left, right):
        exchange(left, right)
        raise RuntimeError('simulated interruption after atomic exchange')
    monkeypatch.setattr(reconcile, 'exchange_names', interrupt_after_exchange)
    with pytest.raises(RuntimeError, match='simulated interruption'):
        reconcile.reconcile_subtree(source, dest, evidence, 'api_trial_v1', journal, workers=2)
    assert _tasks(source/'api_trial_v1/queue.sqlite3') == [1, 2]
    assert _tasks(dest/'api_trial_v1/queue.sqlite3') == [1, 2]
    assert _tasks(journal/'staged-payload/queue.sqlite3') == [1]
    monkeypatch.setattr(reconcile, 'exchange_names', exchange)
    assert reconcile.reconcile_subtree(source, dest, evidence, 'api_trial_v1', journal, workers=2)['state'] == 'complete'
    assert copy.copy(source, dest, evidence, workers=2)['state'] == 'exact_destination_readback_verified'


def test_reconcile_never_accepts_changes_elsewhere_in_source(tmp_path):
    source, dest, evidence, journal = _fixture(tmp_path)
    (source/'unchanged').write_bytes(b'unknown change outside the authorized scope')
    result = reconcile.reconcile_subtree(source, dest, evidence, 'api_trial_v1', journal, workers=2)
    assert result['full_relocation_still_requires_verification']
    with pytest.raises(ValueError, match='source namespace changed'):
        copy.copy(source, dest, evidence, workers=2)
    assert (source/'unchanged').read_bytes() == b'unknown change outside the authorized scope'


def test_completed_reconciliation_rejects_later_source_generation(tmp_path):
    source, dest, evidence, journal = _fixture(tmp_path)
    reconcile.reconcile_subtree(source, dest, evidence, 'api_trial_v1', journal, workers=2)
    (source/'api_trial_v1/new-source-file').write_bytes(b'later update')
    with pytest.raises(ValueError, match='no longer describes'):
        reconcile.reconcile_subtree(source, dest, evidence, 'api_trial_v1', journal, workers=2)
