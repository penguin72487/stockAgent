import hashlib
import json
from pathlib import Path

import pytest

from scripts import attribute_staged_recovery_index as match
from scripts.verify_backup_delivery import identity
from stockagent.data_sync.desync_snapshots import SnapshotError


def sign(value):
    return {**value, 'identity_sha256': identity(value)}


@pytest.fixture
def inputs(tmp_path, monkeypatch):
    root = tmp_path/'stage'
    root.mkdir()
    a, b, c = 'a'*64, 'b'*64, 'c'*64
    objects = {sha: {'bytes': size, 'relative': f'objects/blobs/{sha[:2]}/{sha}.blob',
                     'E_signature': [1, i, 33152, size, 100, 101, 1]}
               for i, (sha, size) in enumerate(((a, 10), (b, 20), (c, 30)), 1)}
    known, deferred = {a}, []
    source_rows = [{'sha256': sha, 'bytes': row['bytes'], 'relative': row['relative']}
                   for sha, row in objects.items()]
    source_rows[2]['bytes'] += 1  # SHA alone is insufficient.
    index = sign({'contract': 'fixed_nas_snapshot_recovery_index_v1', 'repository_id': 'd'*64,
        'nas_provenance_verified_by_index': False, 'full_history_backup_verified': False,
        'canonical_reconstruction_verified': False,
        'deliveries': [{'envelope_identity_sha256': 'e'*64, 'files': source_rows},
                       {'envelope_identity_sha256': 'f'*64, 'files': [source_rows[0]]}]})
    index_path = tmp_path/'index.json'
    index_path.write_text(json.dumps(index, ensure_ascii=False))
    receipt = sign({'contract': 'stockagent.lab203_training_fixed_manifest_index_lookup_non_ack.v1',
        'not_NAS_or_recovery_ACK': True, 'source_index_file_sha256': hashlib.sha256(index_path.read_bytes()).hexdigest(),
        'source_index_identity_sha256': index['identity_sha256'], 'repository_id': index['repository_id'],
        'paired_producer_device_id': 'producer', 'paired_receiver_device_id': 'receiver',
        'observed_at_utc': '2026-11-08T15:32:45+00:00'})
    receipt_path = tmp_path/'receipt.json'
    receipt_path.write_text(json.dumps(receipt))
    monkeypatch.setattr(match, 'RECEIPT_ID', receipt['identity_sha256'])
    primary_key = hashlib.sha256(json.dumps({'objects': sorted((sha, row['bytes'], row['E_signature'])
        for sha, row in objects.items()), 'deferred': deferred}, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    primary = {'contract': 'stockagent.staged_cold_orphan_attribution.v1', 'state': 'metadata_attributed',
        'payloads_read': False, 'complete_dataset_reconstructed': False, 'deletion_authorized': False,
        'input_identity_sha256': primary_key, 'matches': [{'sha256': a}]}
    return root, objects, known, deferred, index_path, receipt_path, primary


def test_exact_identity_matches_are_provenance_candidates_only(inputs):
    _, objects, known, deferred, index, receipt, _ = inputs
    value = match.build(objects, known, deferred, index_path=index, receipt_path=receipt)
    assert value['historical_index_matched_objects'] == 2 and value['historical_index_matched_bytes'] == 30
    assert value['not_locally_referenced_objects'] == 1 and value['not_locally_referenced_bytes'] == 20
    assert len(value['records'][0]['historical_envelope_identities']) == 2
    assert value['payloads_read'] is False and value['current_NAS_readback'] is False
    assert value['complete_dataset_reconstructed'] is False and value['deletion_authorized'] is False


def test_changed_index_cannot_reuse_old_pinned_receipt(inputs):
    _, objects, known, deferred, index, receipt, _ = inputs
    index.write_bytes(index.read_bytes()+b' ')
    with pytest.raises(SnapshotError, match='identity or evidence scope'):
        match.build(objects, known, deferred, index_path=index, receipt_path=receipt)


def test_redirected_index_and_changed_local_primary_are_rejected(inputs, tmp_path):
    root, objects, _, deferred, index, receipt, primary = inputs
    redirected = tmp_path/'redirected'
    redirected.symlink_to(index)
    with pytest.raises(SnapshotError, match='regular file'):
        match.refresh(root, objects, primary, deferred, index_path=redirected, receipt_path=receipt)
    primary['input_identity_sha256'] = '0'*64
    with pytest.raises(SnapshotError, match='generation differs'):
        match.refresh(root, objects, primary, deferred, index_path=index, receipt_path=receipt)


def test_unchanged_index_reuses_prior_result_without_reparsing(inputs, monkeypatch):
    root, objects, _, deferred, index, receipt, primary = inputs
    value = match.refresh(root, objects, primary, deferred, index_path=index, receipt_path=receipt)
    monkeypatch.setattr(match, 'build', lambda *_a, **_kw: pytest.fail('unchanged index was reparsed'))
    again = match.refresh(root, objects, primary, deferred, index_path=index, receipt_path=receipt)
    assert again['identity_sha256'] == value['identity_sha256']


def test_tampered_candidate_result_is_rebuilt_not_trusted(inputs):
    root, objects, _, deferred, index, receipt, primary = inputs
    match.refresh(root, objects, primary, deferred, index_path=index, receipt_path=receipt)
    path = root/match.OUTPUT
    value = json.loads(path.read_text())
    value['historical_index_matched_objects'] = 999
    path.write_text(json.dumps(value))
    assert match.refresh(root, objects, primary, deferred, index_path=index, receipt_path=receipt)[
        'historical_index_matched_objects'] == 2


def test_unavailable_remote_index_does_not_delete_previous_evidence_or_block_owner(inputs, monkeypatch):
    root, objects, _, deferred, index, receipt, primary = inputs
    first = match.refresh(root, objects, primary, deferred, index_path=index, receipt_path=receipt)
    def unavailable(*_a, **_kwargs):
        raise SnapshotError('source not mounted')
    monkeypatch.setattr(match, 'refresh', unavailable)
    assert match.refresh_with_status(root, objects, primary, deferred) is None
    assert json.loads((root/match.OUTPUT).read_text())['identity_sha256'] == first['identity_sha256']
    assert json.loads((root/match.STATUS).read_text())['state'] == 'unavailable'


def new_index_and_receipt(index_path, anchor_path, *, producer='producer'):
    index = json.loads(index_path.read_text())
    index.pop('identity_sha256')
    index['deliveries'][0]['files'][2]['bytes'] -= 1
    index = sign(index)
    index_path.write_text(json.dumps(index))
    anchor = json.loads(anchor_path.read_text())
    anchor.pop('identity_sha256')
    anchor.update(source_index_file_sha256=hashlib.sha256(index_path.read_bytes()).hexdigest(),
                  source_index_identity_sha256=index['identity_sha256'],
                  paired_producer_device_id=producer)
    receipt = sign(anchor)
    path = anchor_path.parent/('lab203-training-fixed-manifest-lookup-non-ack-v1-'
                               +receipt['identity_sha256']+'.json')
    path.write_text(json.dumps(receipt))
    return path, receipt


def test_updated_index_is_matched_only_to_its_exact_same_peer_receipt(inputs, monkeypatch):
    root, objects, _, deferred, index, anchor, primary = inputs
    first = match.refresh(root, objects, primary, deferred, index_path=index, receipt_path=anchor)
    path, receipt = new_index_and_receipt(index, anchor)
    result = match.refresh(root, objects, primary, deferred, index_path=index, receipt_path=anchor)
    assert result['historical_index_matched_objects'] == 3
    assert result['source_receipt_identity_sha256'] == receipt['identity_sha256']
    assert result['source_receipt_name'] == path.name
    assert result['anchor_receipt_identity_sha256'] == first['source_receipt_identity_sha256']
    assert result['current_NAS_readback'] is False and result['deletion_authorized'] is False
    monkeypatch.setattr(match, 'build', lambda *_a, **_kw: pytest.fail('unchanged updated index was reparsed'))
    assert match.refresh(root, objects, primary, deferred, index_path=index, receipt_path=anchor)[
        'identity_sha256'] == result['identity_sha256']


def test_updated_index_with_wrong_peer_never_reuses_old_receipt(inputs):
    _, objects, known, deferred, index, anchor, _ = inputs
    new_index_and_receipt(index, anchor, producer='different-peer')
    with pytest.raises(SnapshotError, match='matching source receipt'):
        match.build(objects, known, deferred, index_path=index, receipt_path=anchor)
