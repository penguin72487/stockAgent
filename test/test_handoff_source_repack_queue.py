import fcntl
import json
import os
from types import SimpleNamespace

import pytest

from scripts import run_handoff_source_repack_queue as queue
from stockagent.data_sync.desync_snapshots import SnapshotError


def completed(dataset):
    return {'contract': queue.handoff_cli.CONTRACT, 'dataset': dataset,
            'state': 'C_payload_retired_root_and_lock_preserved', 'C_payload_deleted': True,
            'prepared_sha256': 'a'*64, 'C_retirement': {
                'state': 'C_payload_retired_root_and_lock_preserved', 'prepared_sha256': 'a'*64,
                'source_root_inode': 100, 'source_lock_inode': 101}}


def test_queue_runs_next_only_after_matching_receipt(tmp_path, monkeypatch):
    monkeypatch.setattr(queue.handoff_cli, 'ENROLLED', {'first-source': 'first', 'second-source': 'second'})
    (tmp_path/'first.status.json').write_text(json.dumps(completed('first')))
    seen = []
    def runner(command, on_heartbeat, timeout):
        seen.append((command[-2:], timeout))
        on_heartbeat()
        (tmp_path/'second.status.json').write_text(json.dumps(completed('second')))
        return SimpleNamespace(returncode=0, stderr='')
    rows = queue.run_once(tmp_path, now=lambda: 1000, runner=runner, code_sha='a')
    assert [row['state'] for row in rows] == ['completed', 'completed']
    assert seen == [(['second-source', '--retire'], queue.CHILD_TIMEOUT_SECONDS)]
    assert json.loads((tmp_path/'queue-status.json').read_text())['observed_at_epoch'] == 1000


def test_failed_source_does_not_block_other_source_and_new_code_retries(tmp_path, monkeypatch):
    monkeypatch.setattr(queue.handoff_cli, 'ENROLLED', {'a-source': 'a', 'b-source': 'b'})
    def fail(_command, **_kw):
        return SimpleNamespace(returncode=1, stderr='transient failure')
    rows = queue.run_once(tmp_path, now=lambda: 1000, runner=fail, code_sha='old')
    assert [row['state'] for row in rows] == ['retrying', 'waiting']
    seen = []
    def succeed(command, **_kw):
        seen.append(command[-2])
        (tmp_path/'b.status.json').write_text(json.dumps(completed('b')))
        return SimpleNamespace(returncode=0, stderr='')
    rows = queue.run_once(tmp_path, now=lambda: 1001, runner=succeed, code_sha='old')
    assert seen == ['b-source'] and [row['state'] for row in rows] == ['retrying', 'completed']
    rows = queue.run_once(tmp_path, now=lambda: 1002, runner=fail, code_sha='new')
    assert [row['state'] for row in rows] == ['retrying', 'completed']
    assert rows[0]['retry_after_epoch'] == 1122


def test_zero_exit_without_exact_completion_receipt_retries(tmp_path, monkeypatch):
    monkeypatch.setattr(queue.handoff_cli, 'ENROLLED', {'source': 'dataset'})
    (tmp_path/'dataset.status.json').write_text(json.dumps({'state': 'completed'}))
    rows = queue.run_once(tmp_path, now=lambda: 1000, code_sha='a',
                          runner=lambda *_a, **_kw: SimpleNamespace(returncode=0, stderr=''))
    assert rows[0]['state'] == 'retrying' and rows[0]['error'] == 'completion_receipt_missing'


def test_existing_owner_lock_is_not_forced(tmp_path, monkeypatch):
    monkeypatch.setattr(queue.handoff_cli, 'ENROLLED', {'source': 'dataset'})
    descriptor = os.open(tmp_path/'queue-owner.lock', os.O_CREAT | os.O_RDWR, 0o600)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(SnapshotError, match='already has an owner'):
            queue.run_once(tmp_path, code_sha='a')
    finally:
        os.close(descriptor)
