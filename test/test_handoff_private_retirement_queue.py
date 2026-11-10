import json
from types import SimpleNamespace

from scripts import run_handoff_private_retirement_queue as queue
from test_handoff_private_d_queue import completed_d


def test_retirement_queue_preserves_D_input_and_retries_busy_owner(tmp_path, monkeypatch):
    monkeypatch.setattr(queue, 'DATASETS', ('a', 'b'))
    (tmp_path/'a.status.json').write_text(json.dumps(completed_d('a')))
    fail = lambda *_a, **_kw: SimpleNamespace(returncode=1, stderr='retirement owner busy: publication')
    rows = queue.run_once(tmp_path, now=lambda: 1000, runner=fail, code_sha='a')
    assert [row['state'] for row in rows] == ['retrying', 'waiting_input']
    assert not json.loads((tmp_path/'a.status.json').read_text()).get('D_old_release_removed', False)
    (tmp_path/'b.status.json').write_text(json.dumps(completed_d('b')))
    def success(command, **_kw):
        assert command[-3:] == ['--dataset', 'b', '--apply']
        value = completed_d('b')
        value.update(D_old_release_removed=True, D_retirement={'dataset': 'b', 'state': 'completed',
            'apply': True, 'old_manifest_sha256': 'c'*64, 'new_manifest_sha256': 'd'*64})
        value['D_publication']['new_manifest_sha256'] = 'd'*64
        (tmp_path/'b.status.json').write_text(json.dumps(value))
        return SimpleNamespace(returncode=0, stderr='')
    rows = queue.run_once(tmp_path, now=lambda: 1001, runner=success, code_sha='a')
    assert [row['state'] for row in rows] == ['retrying', 'completed']
    assert rows[0]['error'] == 'owner_busy'
