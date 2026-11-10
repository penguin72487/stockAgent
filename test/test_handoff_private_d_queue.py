import json
from types import SimpleNamespace

from scripts import run_handoff_private_d_queue as queue
from test_handoff_source_repack_queue import completed


def completed_d(dataset):
    value = completed(dataset)
    value.update(original_manifest_sha256='c'*64, D_new_head_committed=True, D_E_stage_cleanup_complete=True,
        D_publication={'contract': 'stockagent.source_local_exact_repack.v1', 'dataset': dataset,
                       'old_manifest_sha256': 'c'*64, 'state': 'new_D_head_committed_from_exact_E_source'})
    return value


def test_D_queue_waits_for_C_and_only_completes_on_D_receipt(tmp_path, monkeypatch):
    monkeypatch.setattr(queue, 'DATASETS', ('a', 'b'))
    (tmp_path/'a.status.json').write_text(json.dumps(completed('a')))
    seen = []
    def run(command, on_heartbeat, timeout):
        seen.append(command[-1])
        on_heartbeat()
        (tmp_path/'a.status.json').write_text(json.dumps(completed_d('a')))
        return SimpleNamespace(returncode=0, stderr='')
    rows = queue.run_once(tmp_path, now=lambda: 1000, runner=run, code_sha='a')
    assert seen == ['a'] and [row['state'] for row in rows] == ['completed', 'waiting_input']


def test_D_queue_retries_failure_without_blocking_other_dataset(tmp_path, monkeypatch):
    monkeypatch.setattr(queue, 'DATASETS', ('a', 'b'))
    for name in ('a', 'b'):
        (tmp_path/(name+'.status.json')).write_text(json.dumps(completed(name)))
    fail = lambda *_a, **_kw: SimpleNamespace(returncode=1, stderr='failed')
    rows = queue.run_once(tmp_path, now=lambda: 1000, runner=fail, code_sha='a')
    assert [row['state'] for row in rows] == ['retrying', 'waiting']
    def run(command, **_kw):
        assert command[-1] == 'b'
        (tmp_path/'b.status.json').write_text(json.dumps(completed_d('b')))
        return SimpleNamespace(returncode=0, stderr='')
    rows = queue.run_once(tmp_path, now=lambda: 1001, runner=run, code_sha='a')
    assert [row['state'] for row in rows] == ['retrying', 'completed']


def test_private_D_queue_yields_next_writer_to_E_only_recovery(tmp_path, monkeypatch):
    monkeypatch.setattr(queue, 'DATASETS', ('a',))
    monkeypatch.setattr(queue.staged_head_capacity_relief, 'E_only_pending', lambda _workspace: True)
    (tmp_path/'a.status.json').write_text(json.dumps(completed('a')))
    def no_writer(*_args, **_kwargs):
        raise AssertionError('an E-only source has priority')
    rows = queue.run_once(tmp_path, now=lambda: 1000, runner=no_writer, code_sha='a')
    assert rows[0]['state'] == 'waiting_priority' and rows[0]['error'] == 'E_only_priority'
