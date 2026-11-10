import gzip
import json
import zipfile
import fcntl

import pytest

from scripts import attribute_staged_cold_objects as attribution
from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync.packed_backup import signature


def test_attribution_binds_inventories_to_staged_objects_without_reading_payloads(tmp_path):
    inventory, payload = tmp_path/'inventory.jsonl.gz', tmp_path/'data.blob'
    payload.write_bytes(b'opaque')
    rows = [{'kind': 'file', 'path': 'source/a.parquet', 'size': 6,
             'storage': {'kind': 'chunks', 'chunks': [{'object_sha256': 'b'*64, 'bytes': 6}]}},
            {'kind': 'file', 'path': 'source/other', 'size': 4,
             'storage': {'kind': 'pack', 'object_sha256': 'c'*64}}]
    with gzip.open(inventory, 'wt') as stream:
        for row in rows:
            stream.write(json.dumps(row)+'\n')
    objects = {'a'*64: {'path': inventory, 'relative': 'objects/inventories/a.jsonl.gz',
                        'bytes': inventory.stat().st_size, 'E_signature': list(signature(inventory))},
               'b'*64: {'path': payload, 'relative': 'objects/blobs/b.blob',
                        'bytes': 6, 'E_signature': list(signature(payload))}}
    result = attribution.attribute(objects)
    assert result['object_count'] == 2 and result['attributed_objects'] == 1
    assert result['attributed_bytes'] == 6 and result['unattributed_objects'] == 1
    assert result['matches'][0]['example_paths'] == ['source/a.parquet']
    assert result['payloads_read'] is False and result['deletion_authorized'] is False


def test_attribution_rejects_noncanonical_chunk_identity(tmp_path):
    inventory = tmp_path/'inventory.jsonl.gz'
    with gzip.open(inventory, 'wt') as stream:
        stream.write(json.dumps({'kind': 'file', 'path': 'data/a', 'size': 6,
            'storage': {'kind': 'chunks', 'chunks': [{'sha256': 'b'*64, 'bytes': 6}]}})+'\n')
    objects = {'a'*64: {'path': inventory, 'relative': 'objects/inventories/a.jsonl.gz',
                        'bytes': inventory.stat().st_size, 'E_signature': list(signature(inventory))}}
    result = attribution.attribute(objects)
    assert result['attributed_objects'] == 0
    assert result['inventory_errors'] == [{'sha256': 'a'*64, 'error_type': 'SnapshotError'}]


def test_attribution_rejects_changed_inventory_generation(tmp_path):
    inventory = tmp_path/'inventory.jsonl.gz'
    with gzip.open(inventory, 'wt') as stream:
        stream.write(json.dumps({'path': 'x', 'kind': 'directory'})+'\n')
    objects = {'a'*64: {'path': inventory, 'relative': 'objects/inventories/a.jsonl.gz',
                        'bytes': inventory.stat().st_size, 'E_signature': list(signature(inventory))}}
    objects['a'*64]['E_signature'][2] += 1
    result = attribution.attribute(objects)
    assert result['inventory_errors'] == [{'sha256': 'a'*64, 'error_type': 'SnapshotError'}]
    assert result['attributed_objects'] == 0 and result['unattributed_objects'] == 1


def test_archive_name_census_does_not_claim_dataset_or_open_payload(tmp_path, monkeypatch):
    def package(name, paths):
        path = tmp_path/name
        with zipfile.ZipFile(path, 'w') as stream:
            for item in paths:
                stream.writestr(item, b'payload')
        return {'path': path, 'relative': 'objects/packs/'+name, 'bytes': path.stat().st_size,
                'E_signature': list(signature(path))}
    objects = {'a'*64: package('one.zip', ['data_a/a', 'data_a/b']),
               'b'*64: package('mixed.zip', ['data_a/a', 'data_b/b'])}
    monkeypatch.setattr(zipfile.ZipFile, 'open', lambda *_a, **_kw: (_ for _ in ()).throw(
        AssertionError('member payload should not be opened')))
    result = attribution.attribute(objects)
    assert result['archive_prefix_candidates'] == 1 and result['archive_mixed_or_unknown'] == 1
    assert next(item for item in result['archive_name_census'] if item['sha256'] == 'a'*64)[
        'single_path_prefix_candidate'] == 'data_a'
    assert result['payloads_read'] is False and result['complete_dataset_reconstructed'] is False


def test_unfinished_batch_does_not_hide_other_closed_batch(tmp_path, monkeypatch):
    a, b = 'a'*64, 'b'*64
    (tmp_path/(a+'.plan.json')).touch()
    (tmp_path/(b+'.plan.json')).touch()
    def metadata(path):
        key = path.name[:64]
        if key == b and path.name.endswith('.result.json'):
            raise FileNotFoundError('still being prepared')
        if path.name.endswith('.plan.json'):
            return {'identity_sha256': key}
        if path.name.endswith('.handoff.json'):
            return {'contract': attribution.retired.CONTRACT, 'state': 'D_names_removed_E_copy_retained',
                    'plan_identity_sha256': key, 'destination': str(tmp_path/(key+'.payload')),
                    'removed': [{'relative': 'objects/blobs/c.blob', 'bytes': 6}]}
        return {}
    monkeypatch.setattr(attribution.staging, '_stable_json', metadata)
    monkeypatch.setattr(attribution.retired, 'closed_E_handoff', lambda *_a: [
        {'relative': 'objects/blobs/c.blob', 'bytes': 6, 'sha256': 'c'*64, 'E_signature': [1, 2, 3]}])
    objects, deferred = attribution.collect(tmp_path)
    assert list(objects) == ['c'*64] and deferred == [{'plan': b+'.plan.json', 'error_type': 'FileNotFoundError'}]


def test_attribution_owner_is_nonblocking(tmp_path):
    lock = tmp_path/'orphan-attribution-owner.lock'
    lock.touch()
    with lock.open('r+') as owner:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(SnapshotError, match='already has an owner'):
            attribution.run(tmp_path)


def test_original_owner_refreshes_index_candidates_without_rescanning_unchanged_payload_metadata(tmp_path, monkeypatch):
    from scripts import attribute_staged_recovery_index as recovery
    objects = {'a'*64: {'bytes': 6, 'E_signature': [1, 2, 3], 'relative': 'objects/blobs/a.blob'}}
    monkeypatch.setattr(attribution, 'collect', lambda _root: (objects, []))
    scans, refreshes = [], []
    def scan(_objects, **_kwargs):
        scans.append(1)
        return {'inventory_errors': [], 'archive_errors': [], 'matches': [], 'object_count': 1,
                'object_bytes': 6, 'attributed_objects': 0, 'attributed_bytes': 0, 'unattributed_objects': 1,
                'payloads_read': False, 'complete_dataset_reconstructed': False, 'deletion_authorized': False}
    monkeypatch.setattr(attribution, 'attribute', scan)
    monkeypatch.setattr(recovery, 'refresh_with_status', lambda *args: refreshes.append(args))
    assert attribution.run(tmp_path)['state'] == 'metadata_attributed'
    assert attribution.run(tmp_path)['state'] == 'metadata_attributed'
    assert scans == [1] and len(refreshes) == 2


def test_recovery_index_failure_does_not_stop_original_attribution(tmp_path, monkeypatch):
    from scripts import attribute_staged_recovery_index as recovery
    monkeypatch.setattr(attribution, 'collect', lambda _root: ({}, []))
    monkeypatch.setattr(attribution, 'attribute', lambda _objects, **_kwargs: {
        'inventory_errors': [], 'archive_errors': [], 'matches': [], 'object_count': 0, 'object_bytes': 0,
        'attributed_objects': 0, 'attributed_bytes': 0, 'unattributed_objects': 0,
        'payloads_read': False, 'complete_dataset_reconstructed': False, 'deletion_authorized': False})
    def unavailable(*_args):
        raise SnapshotError('historical index unavailable')
    monkeypatch.setattr(recovery, 'refresh_with_status', unavailable)
    assert attribution.run(tmp_path)['state'] == 'metadata_attributed'
    assert json.loads((tmp_path/'orphan-attribution-v1.json').read_text())['object_count'] == 0
