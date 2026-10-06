import json

import pytest

from stockagent.data_sync import preservation_coverage as coverage
from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync.nas_coverage import combined_coverage


def test_transport_observer_defers_during_api_outage_and_recovers(monkeypatch):
    from scripts.audit_nas_sync import observe_transport
    from scripts import configure_artifact_ingress_syncthing as syncthing
    monkeypatch.setattr(syncthing, 'credentials', lambda: ('local', 'private-key'))
    outage = True
    def api(base, key, path, query=None):
        if outage:
            raise ConnectionRefusedError('private endpoint diagnostic must not be exposed')
        if path == '/rest/system/connections':
            return {'connections': {'paired': {'connected': True}}}
        if path in ('/rest/system/error', '/rest/folder/errors'):
            return {'errors': []}
        if path == '/rest/db/status':
            return {'state': 'idle', 'needTotalItems': 0, 'needBytes': 0, 'errors': 0}
        if path == '/rest/db/completion':
            assert query['device'] == 'paired'
            return {'needBytes': 0, 'needItems': 0, 'needDeletes': 0, 'completion': 100}
        raise AssertionError(path)
    monkeypatch.setattr(syncthing, 'request', api)
    failed = observe_transport('paired')
    assert len(failed) == 2 and all(row['converged'] is False for row in failed)
    assert all(row['error_type'] == 'ConnectionRefusedError' for row in failed)
    assert 'private' not in str(failed)
    outage = False
    assert all(row['converged'] is True for row in observe_transport('paired'))

def test_received_bytes_wait_for_real_original_recovery_and_exact_member_identity(tmp_path, monkeypatch):
    monkeypatch.setattr(coverage, 'INCOMING', tmp_path)
    batch = tmp_path / '20261006T000000-abcdef012345'; batch.mkdir()
    cohort = {'contract': coverage.CONTRACT, 'carriers': [{
        'batch': batch.name, 'scope': 'cache', 'compressed_bytes': 123,
        'compressed_sha256': 'a' * 64, 'member_fingerprint_sha256': 'b' * 64}]}
    waiting = coverage.received_publications(cohort)
    assert not waiting['all_received_originals_verified'] and not waiting['publications']
    proof = {'dataset': 'exact', 'snapshot_id': 'fixed', 'manifest_sha256': 'c' * 64,
             'source_scope': 'cache', 'compressed_sha256': 'a' * 64}
    (batch / 'cache.cold-proof.json').write_text(json.dumps(proof))
    monkeypatch.setattr(coverage, 'verify_retained_preservation', lambda _: (_ for _ in ()).throw(SnapshotError('not verified')))
    assert not coverage.received_publications(cohort)['all_received_originals_verified']
    index = tmp_path / 'index.json'
    index.write_text(json.dumps({'compressed_bytes': 123, 'compressed_sha256': 'a' * 64,
                                'member_fingerprint_sha256': 'b' * 64}))
    monkeypatch.setattr(coverage, 'verify_retained_preservation', lambda _: {'member_inventory.json': index})
    ready = coverage.received_publications(cohort)
    assert ready['all_received_originals_verified'] and len(ready['publications']) == 1
    index.write_text(json.dumps({'compressed_bytes': 123, 'compressed_sha256': 'a' * 64,
                                'member_fingerprint_sha256': 'd' * 64}))
    with pytest.raises(ValueError, match='member set'):
        coverage.received_publications(cohort)


def test_fixed_received_release_cohort_includes_original_head_and_waits_for_missing_registration():
    published = {'dataset': 'selected', 'snapshot_id': 'fixed', 'manifest_sha256': 'a' * 64}
    head = {'dataset': 'selected', 'snapshot_id': 'fixed', 'manifest_sha256': 'a' * 64}
    rows = [
        {'relative': 'manifest', 'sha256': 'a' * 64, 'bytes': 100, 'role': 'cold_metadata'},
        {'relative': 'blob', 'sha256': 'b' * 64, 'bytes': 200, 'role': 'packed_object'},
        {'relative': 'head', 'sha256': 'c' * 64, 'bytes': 50, 'role': 'cold_metadata', 'captured_bytes_utf8': json.dumps(head)},
        {'relative': 'future-other-source', 'sha256': 'd' * 64, 'bytes': 9999, 'role': 'packed_object'},
    ]
    release = {**published, 'missing_objects': [], 'required_file_keys': ['manifest@' + 'a' * 64, 'blob@' + 'b' * 64]}
    catalog = {'files': rows, 'releases': [release], 'observed_at_utc': 'fixed-time'}
    frozen = coverage.publication_cohort(catalog, [published], upstream_sha256='e' * 64)
    assert {r['relative'] for r in frozen['files']} == {'manifest', 'blob', 'head'}
    assert coverage.publication_cohort({**catalog, 'releases': []}, [published], upstream_sha256='e' * 64) is None
    assert coverage.publication_cohort({**catalog, 'files': rows[1:]}, [published], upstream_sha256='e' * 64) is None
    restic = {'deliveries': {'ack': {'files': rows[:2], 'acceptance': True}}}
    result = combined_coverage(frozen, restic, {'deliveries': {}})
    assert result['machine_pending_file_count'] == 1 and not result['all_available_cold_files_verified']


def test_merged_scope_deduplicates_exact_versions_but_keeps_advanced_heads():
    shared = {'relative': 'blob', 'sha256': 'a' * 64, 'bytes': 200}
    head = {'relative': 'head', 'sha256': 'b' * 64, 'bytes': 50}
    advanced = {**head, 'sha256': 'c' * 64, 'bytes': 60}
    first = {'files': [shared, head], 'identity_sha256': 'old', 'observed_at_utc': 'old'}
    second = {'files': [shared, advanced], 'identity_sha256': 'new', 'observed_at_utc': 'new'}
    merged = coverage.merge_cohorts(first, second)
    result = combined_coverage(merged, {'deliveries': {}}, {'deliveries': {}})
    assert result['available_file_count'] == 3 and result['available_bytes'] == 310
