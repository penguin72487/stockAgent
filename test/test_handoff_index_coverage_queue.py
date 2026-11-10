import json

from scripts import run_handoff_index_coverage_queue as queue
from stockagent.data_sync.desync_snapshots import SnapshotError


def test_coverage_precheck_reuses_one_metadata_comparison_without_retirement(tmp_path, monkeypatch):
    root, private = tmp_path/'root', tmp_path/'private'
    root.mkdir(); private.mkdir()
    stage, index = tmp_path/'stage', tmp_path/'index'
    item = ('tw-futures-universe725-workspace', 'a'*64, stage, index, 'root',
            {'files':10, 'logical_bytes':100, 'identity_sha256':'c'*64})
    monkeypatch.setattr(queue, 'candidates', lambda _root: iter([item]))
    monkeypatch.setattr(queue.existing, 'cached_covering_match', lambda *_a, **_k: None)
    def match(*_args, progress, **_kwargs):
        progress({'stage':'comparing_D_existing_inventory', 'files_completed':10,
                  'files_total':10, 'logical_bytes_processed':100, 'logical_bytes_total':100})
        return {'files':10, 'logical_bytes':100, 'identity_sha256':'b'*64}
    monkeypatch.setattr(queue.existing, 'match', match)
    value = queue.run_once(root, private, now=lambda:1000)
    assert value['state'] == 'completed' and value['matched_files'] == 10
    assert value['D_or_NAS_readback'] is False and value['source_files_deleted'] == 0
    saved = json.loads((root/'coverage-precheck-status.json').read_text())
    assert saved == value and saved['source_identity'] == 'a'*64


def test_coverage_precheck_skips_valid_cache_and_records_failure(tmp_path, monkeypatch):
    root, private = tmp_path/'root', tmp_path/'private'
    root.mkdir(); private.mkdir()
    item = ('tw-futures-universe725-workspace', 'a'*64, tmp_path/'stage', tmp_path/'index', 'root',
            {'files':1, 'logical_bytes':2, 'identity_sha256':'c'*64})
    monkeypatch.setattr(queue, 'candidates', lambda _root: iter([item]))
    monkeypatch.setattr(queue.existing, 'cached_covering_match', lambda *_a, **_k: {'valid':True})
    assert queue.run_once(root, private, now=lambda:1000)['state'] == 'waiting'
    monkeypatch.setattr(queue.existing, 'cached_covering_match', lambda *_a, **_k: None)
    def mismatch(*_args, **_kwargs):
        raise SnapshotError('fixed inventory mismatch')
    monkeypatch.setattr(queue.existing, 'match', mismatch)
    result = queue.run_once(root, private, now=lambda:1000)
    assert result['state'] == 'retrying' and result['retry_after_epoch'] == 1120
    assert result['reason'] == 'SnapshotError'


def test_coverage_owner_busy_retries_without_a_long_cooldown(tmp_path, monkeypatch):
    root, private = tmp_path/'root', tmp_path/'private'
    root.mkdir(); private.mkdir()
    item = ('tw-futures-universe765-workspace', 'a'*64, tmp_path/'stage', tmp_path/'index', 'root',
            {'files':1, 'logical_bytes':2, 'identity_sha256':'c'*64})
    monkeypatch.setattr(queue, 'candidates', lambda _root: iter([item]))
    monkeypatch.setattr(queue.existing, 'cached_covering_match', lambda *_a, **_k: None)
    def busy(*_args, **_kwargs):
        raise queue.PackedPublicationBusy('owner busy')
    monkeypatch.setattr(queue.existing, 'match', busy)
    value = queue.run_once(root, private, now=lambda:1000)
    assert value['state'] == 'retrying' and value['retry_after_epoch'] == 1015
    # An older owner could have left the former 120-second retry clock.
    value['retry_after_epoch'] = 1120
    (root/'coverage-precheck-status.json').write_text(json.dumps(value))
    monkeypatch.setattr(queue.existing, 'match', lambda *_a, **_k: {
        'files':1, 'logical_bytes':2, 'identity_sha256':'d'*64})
    value = queue.run_once(root, private, now=lambda:1020)
    assert value['state'] == 'completed'
