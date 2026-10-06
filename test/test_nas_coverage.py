import json
from pathlib import Path

import pytest

from stockagent.data_sync.nas_coverage import archive_keys, combined_coverage, read_ledger, transport_converged


def row(name, size, sha='a' * 64):
    return {'relative': name, 'sha256': sha, 'bytes': size}


def test_union_counts_identical_versions_once_and_preserves_unverified_baseline():
    shared, old, new, pilot = [row(k, n) for k, n in [('same', 100), ('old', 200), ('new', 300), ('pilot', 40)]]
    catalog = {'files': [shared, old, new, pilot], 'missing_objects': [], 'metadata_errors': []}
    restic = {'deliveries': {'restic': {'files': [shared, old], 'acceptance': {'verified': True}}},
              'reported_baseline': [{'files': [pilot]}]}
    archive = {'deliveries': {'arc': {'file_keys': [r['relative'] + '@' + r['sha256'] for r in [shared, new]],
               'nas_acceptance': {'state': 'nas_archive_file_recovery_verified', 'delivery_identity_sha256': 'arc'}}}}
    result = combined_coverage(catalog, restic, archive)
    assert result['nas_covered_bytes'] == 640
    assert result['machine_verified_bytes'] == 600 and result['overlap_bytes'] == 100
    assert result['reported_only_file_count'] == 1
    assert result['pending_bytes'] == 0 and not result['all_available_cold_files_verified']
    assert result['all_history_backup_verified'] is False


def test_pending_wrong_version_and_foreign_inventory_are_not_coverage():
    wanted = row('objects/file', 10)
    stale = row('objects/file', 10, 'b' * 64)
    catalog = {'files': [wanted]}
    restic = {'deliveries': {'old': {'files': [stale], 'acceptance': True},
                             'pending': {'files': [wanted]}}}
    archive = {'deliveries': {'x': {'file_keys': ['elsewhere@' + wanted['sha256']]}}}
    result = combined_coverage(catalog, restic, archive)
    assert result['nas_covered_bytes'] == 0 and result['pending_file_count'] == 1


def test_available_scope_completion_requires_machine_proof_and_no_source_errors():
    wanted = row('objects/file', 10)
    restic = {'deliveries': {'d': {'files': [wanted], 'acceptance': True}}}
    assert combined_coverage({'files': [wanted]}, restic, {'deliveries': {}})['all_available_cold_files_verified']
    assert not combined_coverage({'files': [wanted], 'metadata_errors': ['invalid source']}, restic,
                                 {'deliveries': {}})['all_available_cold_files_verified']
    assert not combined_coverage({'files': []}, restic, {'deliveries': {}})['all_available_cold_files_verified']


def test_archive_proof_from_another_delivery_is_rejected():
    with pytest.raises(ValueError, match='another delivery'):
        archive_keys({'deliveries': {'x': {'file_keys': ['file@sha'],
            'nas_acceptance': {'state': 'nas_archive_file_recovery_verified', 'delivery_identity_sha256': 'other'}}}})


def test_private_ledger_reader_refuses_symlink_and_invalid_structure(tmp_path):
    path = tmp_path / 'ledger.json'
    assert read_ledger(path) == {'deliveries': {}}
    path.write_text(json.dumps({'deliveries': {}}))
    alias = tmp_path / 'alias'
    alias.symlink_to(path)
    with pytest.raises(ValueError, match='redirected'):
        read_ledger(alias)
    path.write_text('[]')
    with pytest.raises(ValueError, match='invalid'):
        read_ledger(path)


def test_cached_completion_does_not_prove_connected_error_free_convergence():
    local = {'state': 'idle', 'needTotalItems': 0, 'needBytes': 0, 'errors': 0}
    peer = {'needBytes': 0, 'needItems': 0, 'needDeletes': 0, 'completion': 100}
    state = {'connected': True, 'folder_errors': [], 'system_errors': []}
    assert transport_converged(local, peer, **state)
    assert not transport_converged(local, peer, **{**state, 'connected': False})
    assert not transport_converged(local, peer, **{**state, 'system_errors': [{'message': 'error'}]})
    assert not transport_converged({**local, 'state': 'scanning'}, peer, **state)
    assert not transport_converged(local, {**peer, 'needDeletes': 1}, **state)
