import json
from types import SimpleNamespace

from scripts import run_handoff_index_d_queue as queue
from test_handoff_index_archive_queue import completed


def done(dataset):
    value = completed(dataset)
    value.update(source_identity='b'*64, D_archive_committed=True, D_E_stage_cleanup_complete=True,
        D_publication={'contract': 'stockagent.source_local_exact_repack.v1', 'dataset': dataset,
            'old_manifest_sha256': 'b'*64, 'state': 'new_D_head_committed_from_exact_E_source'})
    return value


def test_index_D_queue_requires_C_retirement_and_exact_D_receipt(tmp_path, monkeypatch):
    monkeypatch.setattr(queue.source_cli, 'ENROLLED', {'a-source': ('a', ['.download.lock'], False),
                                                       'b-source': ('b', ['.download.lock'], False)})
    (tmp_path/'a.status.json').write_text(json.dumps(completed('a')))
    def runner(command, on_heartbeat, timeout):
        assert command[-2:] == ['--dataset', 'a']
        on_heartbeat()
        (tmp_path/'a.status.json').write_text(json.dumps(done('a')))
        return SimpleNamespace(returncode=0, stderr='')
    rows = queue.run_once(tmp_path, now=lambda: 1000, runner=runner, code_sha='a')
    assert [row['state'] for row in rows] == ['completed', 'waiting_input']


def test_index_D_queue_capacity_retry_does_not_block_smaller_sources(tmp_path, monkeypatch):
    monkeypatch.setattr(queue.source_cli, 'ENROLLED', {'a-source': ('a', ['.download.lock'], False),
                                                       'b-source': ('b', ['.download.lock'], False)})
    for name in ('a', 'b'):
        (tmp_path/(name+'.status.json')).write_text(json.dumps(completed(name)))
    fail = lambda *_a, **_kw: SimpleNamespace(returncode=1, stderr='D free space below reserve')
    rows = queue.run_once(tmp_path, now=lambda: 1000, runner=fail, code_sha='a')
    assert rows[0]['state'] == 'retrying' and rows[0]['error'] == 'capacity'
    def runner(command, **_kw):
        assert command[-1] == 'b'
        (tmp_path/'b.status.json').write_text(json.dumps(done('b')))
        return SimpleNamespace(returncode=0, stderr='')
    rows = queue.run_once(tmp_path, now=lambda: 1001, runner=runner, code_sha='a')
    assert [row['state'] for row in rows] == ['retrying', 'completed'] and rows[0]['error'] == 'capacity'


def test_index_D_queue_reports_busy_writer_separately_from_capacity(tmp_path, monkeypatch):
    monkeypatch.setattr(queue.source_cli, 'ENROLLED', {'a-source': ('a', ['.download.lock'], False)})
    (tmp_path/'a.status.json').write_text(json.dumps(completed('a')))
    fail = lambda *_a, **_kw: SimpleNamespace(returncode=1, stderr='publication owner busy: cold-install-write-owner.lock')
    rows = queue.run_once(tmp_path, now=lambda: 1000, runner=fail, code_sha='a')
    assert rows[0]['state'] == 'retrying' and rows[0]['error'] == 'owner_busy'
    assert rows[0]['retry_after_epoch'] == 1015
    rows = queue.run_once(tmp_path, now=lambda: 1001, runner=fail, code_sha='a')
    assert rows[0]['error'] == 'owner_busy'
    attempts = []
    def retried(*_args, **_kwargs):
        attempts.append(True)
        return fail()
    rows = queue.run_once(tmp_path, now=lambda: 1015, runner=retried, code_sha='a')
    assert attempts and rows[0]['error'] == 'owner_busy' and rows[0]['retry_after_epoch'] == 1030


def test_index_D_queue_reports_the_contended_shared_retention_owner(tmp_path, monkeypatch):
    monkeypatch.setattr(queue.source_cli, 'ENROLLED', {'a-source': ('a', ['.download.lock'], False)})
    (tmp_path/'a.status.json').write_text(json.dumps(completed('a')))
    fail = lambda *_a, **_kw: SimpleNamespace(returncode=1, stderr='publication owner busy: publish-retention-global.lock')
    rows = queue.run_once(tmp_path, now=lambda: 1000, runner=fail, code_sha='a')
    assert rows[0]['owner_lock'] == 'publish_retention_global'
    rows = queue.run_once(tmp_path, now=lambda: 1001, runner=fail, code_sha='a')
    assert rows[0]['owner_lock'] == 'publish_retention_global'


def test_index_D_queue_gives_E_only_recovery_the_next_writer(tmp_path, monkeypatch):
    monkeypatch.setattr(queue.source_cli, 'ENROLLED', {'a-source': ('a', ['.download.lock'], False)})
    monkeypatch.setattr(queue.staged_head_capacity_relief, 'E_only_pending', lambda _workspace: True)
    (tmp_path/'a.status.json').write_text(json.dumps(completed('a')))
    def no_writer(*_args, **_kwargs):
        raise AssertionError('an E-only source has priority')
    rows = queue.run_once(tmp_path, now=lambda: 1000, runner=no_writer, code_sha='a')
    assert rows[0]['state'] == 'waiting_priority' and rows[0]['error'] == 'E_only_priority'
