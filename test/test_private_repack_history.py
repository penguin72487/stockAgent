import copy
import hashlib
import json

import pytest

from stockagent.data_sync import packed_snapshots as packed, private_handoff_retirement as direct
from stockagent.data_sync import private_repack_history as history, stream_repack
from stockagent.data_sync.desync_snapshots import SnapshotError
from test_stream_repack import _fixture
from test_private_handoff_retirement import _add_futures_dependency


def setup_history(tmp_path, monkeypatch):
    source, cold, stage, first = _fixture(tmp_path, monkeypatch)
    (source/'alpha/a.txt').write_bytes(b'new value')
    second = packed.publish_packed_snapshot(cold, 'fixture', source, node_id='test',
        loose_file_threshold_bytes=12, file_chunk_bytes=16, pack_buckets=8)
    monkeypatch.setattr(stream_repack, 'PRIVATE_REPACK_ROOT', cold)
    monkeypatch.setattr(stream_repack, 'PRIVATE_REPACK_DATASETS', frozenset({'fixture'}))
    monkeypatch.setattr(packed, '_check_native_pack_read_authority', lambda *_a: None)
    result = stream_repack.repack(cold, stage, 'fixture', second.manifest_sha256, node_id='test')
    receipt = {'contract': 'stockagent.source_local_exact_repack.v1', 'private': True,
        'state': 'new_D_head_committed_from_exact_E_source', 'dataset': 'fixture',
        'source_mode': 'D_streamed_to_E_dataset_pack', 'old_manifest_sha256': second.manifest_sha256,
        'new_manifest_sha256': result['new_manifest_sha256'], 'new_snapshot_id': result['snapshot_id']}
    body = {'manifests': {}}
    body['identity_sha256'] = hashlib.sha256(json.dumps(body, sort_keys=True, separators=(',', ':'),
                                                          ensure_ascii=False).encode()).hexdigest()
    index = tmp_path/'public-index.json'
    index.write_text(json.dumps(body))
    materialized, state = tmp_path/'materialized', tmp_path/'retention'
    materialized.mkdir()
    state.mkdir()
    options = {'public_index': index, 'materialized': materialized, 'retention_state': state,
               'process_check': lambda _paths: []}
    direct.retire(cold, stage, receipt, apply=True, **options)
    receipt['old_release_removed'] = True
    return cold, stage, first, receipt, options


def test_private_history_retires_old_version_but_keeps_other_manifest_and_current_head(tmp_path, monkeypatch):
    cold, stage, first, receipt, options = setup_history(tmp_path, monkeypatch)
    other = copy.deepcopy(first.manifest)
    other.update(dataset='other', snapshot_id='other-snapshot')
    shared = cold/'manifests/other/other-snapshot.json'
    shared.parent.mkdir(parents=True)
    packed.write_immutable_json(shared, other)
    plan = history.retire(cold, stage, receipt, **options)
    assert plan['state'] == 'planned' and first.manifest_path.is_file()
    outcome = history.retire(cold, stage, receipt, apply=True, **options)
    assert outcome['state'] == 'completed' and outcome['versions_removed'] == 1
    assert len(list(stage.glob('private-history-retirement-*.json'))) == 1
    assert not first.manifest_path.exists() and shared.is_file()
    assert all((cold/row['relpath']).is_file() for row in first.manifest['archive']['objects'])
    assert packed.resolve_latest_packed(cold, 'fixture').manifest_sha256 == receipt['new_manifest_sha256']
    assert history.retire(cold, stage, receipt, apply=True, **options)['state'] == 'no_unpinned_history'


@pytest.mark.parametrize('shares_target', [False,True])
def test_publication_during_census_is_rechecked_without_losing_new_references(tmp_path,monkeypatch,shares_target):
    cold,stage,first,receipt,options=setup_history(tmp_path,monkeypatch)
    before=direct._manifest_census(cold)
    real_read=history.r._read_manifest
    added=cold/'manifests/concurrent/concurrent-snapshot.json'
    def append_publication(root,relative):
        value=real_read(root,relative)
        if not added.exists():
            current=packed.resolve_latest_packed(cold,'fixture')
            donor=first.manifest if shares_target else current.manifest
            manifest=copy.deepcopy(donor)
            manifest.update(dataset='concurrent',snapshot_id='concurrent-snapshot')
            added.parent.mkdir(parents=True)
            packed.write_immutable_json(added,manifest)
        return value
    monkeypatch.setattr(history.r,'_read_manifest',append_publication)
    done=history.retire(cold,stage,receipt,apply=True,**options)
    assert done['state']=='completed' and done['appended_manifests_reconciled']==1
    assert not first.manifest_path.exists() and added.exists()
    if shares_target:
        assert all((cold/item['relpath']).is_file() for item in first.manifest['archive']['objects'])
    else:
        assert done['object_bytes_removed']>0
    assert len(direct._manifest_census(cold))==len(before)


def test_mutated_old_manifest_is_not_treated_as_a_concurrent_publication(tmp_path,monkeypatch):
    cold,stage,first,receipt,options=setup_history(tmp_path,monkeypatch)
    real_read=history.r._read_manifest
    def change_generation(root,relative):
        value=real_read(root,relative)
        path=cold/relative
        path.touch()
        return value
    monkeypatch.setattr(history.r,'_read_manifest',change_generation)
    with pytest.raises(SnapshotError,match='changed during reference census'):
        history.retire(cold,stage,receipt,apply=True,**options)
    assert first.manifest_path.exists()


def test_concurrent_reference_recheck_bounds_manifest_bytes_before_opening(tmp_path):
    too_large={'manifests/large/version.json':[1,2,0o100600,history.MAX_APPENDED_MANIFEST_BYTES+1,0,0,1]}
    with pytest.raises(SnapshotError,match='exceeds bounded'):
        history._include_appended_references(tmp_path,{},too_large,{},guard=lambda:None)


def test_new_source_link_dependency_during_census_still_vetoes_retirement(tmp_path,monkeypatch):
    cold,stage,first,receipt,options=setup_history(tmp_path,monkeypatch)
    real_read=history.r._read_manifest
    added=[]
    def new_dependency(root,relative):
        value=real_read(root,relative)
        if not added:
            added.append(True)
            _add_futures_dependency(cold,first,monkeypatch)
        return value
    monkeypatch.setattr(history.r,'_read_manifest',new_dependency)
    with pytest.raises(SnapshotError,match='gained a head or research pin'):
        history.retire(cold,stage,receipt,apply=True,**options)
    assert first.manifest_path.exists()


def test_private_history_respects_pins_and_active_readers(tmp_path, monkeypatch):
    cold, stage, first, receipt, options = setup_history(tmp_path, monkeypatch)
    root = options['retention_state']/'research-pins/study'
    root.mkdir(parents=True)
    marker = options['retention_state']/history.history.packed_retention.RESEARCH_PIN_MARKER
    marker.write_text(json.dumps({'contract': history.history.packed_retention.RESEARCH_PIN_CONTRACT,
                                  'schema_version': 1}))
    pin = root/'version.pin.json'
    pin.write_text(json.dumps({'snapshot_id': first.manifest['snapshot_id']}))
    assert history.retire(cold, stage, receipt, apply=True, **options)['state'] == 'no_unpinned_history'
    assert first.manifest_path.is_file()
    pin.unlink()
    with pytest.raises(SnapshotError, match='running process'):
        history.retire(cold, stage, receipt, apply=True, **{**options, 'process_check': lambda _paths: ['reader']})
    assert first.manifest_path.is_file()


def test_private_history_respects_independent_retained_source_pin(tmp_path, monkeypatch):
    cold, stage, first, receipt, options = setup_history(tmp_path, monkeypatch)
    monkeypatch.setattr(direct, '_retained_source_pin_protections',
                        lambda _cold, _dataset: {first.manifest['snapshot_id']: first.manifest_sha256})
    result = history.retire(cold, stage, receipt, apply=True, **options)
    assert result['state'] == 'no_unpinned_history'
    assert first.manifest_path.is_file()


def test_private_history_rechecks_dependency_pin_inside_final_owner(tmp_path, monkeypatch):
    cold, stage, first, receipt, options = setup_history(tmp_path, monkeypatch)
    observed = iter(({}, {first.manifest['snapshot_id']: first.manifest_sha256}))
    monkeypatch.setattr(direct, '_source_link_protections', lambda *_: next(observed))
    with pytest.raises(SnapshotError, match='gained a head or research pin'):
        history.retire(cold, stage, receipt, apply=True, **options)
    assert first.manifest_path.exists()


def test_private_history_keeps_fixed_source_link_until_dependent_is_removed(tmp_path, monkeypatch):
    cold, stage, first, receipt, options = setup_history(tmp_path, monkeypatch)
    dependent = _add_futures_dependency(cold, first, monkeypatch)
    result = history.retire(cold, stage, receipt, apply=True, **options)
    assert result['state'] == 'no_unpinned_history' and first.manifest_path.exists()
    dependent.unlink()
    result = history.retire(cold, stage, receipt, apply=True, **options)
    assert result['state'] == 'completed' and not first.manifest_path.exists()


def test_private_history_recovers_after_exact_manifest_unlink(tmp_path, monkeypatch):
    cold, stage, first, receipt, options = setup_history(tmp_path, monkeypatch)
    original = type(first.manifest_path).unlink
    def interrupted(path, *args, **kwargs):
        result = original(path, *args, **kwargs)
        if path == first.manifest_path:
            raise OSError('interrupted after manifest unlink')
        return result
    monkeypatch.setattr(type(first.manifest_path), 'unlink', interrupted)
    with pytest.raises(OSError, match='interrupted after manifest unlink'):
        history.retire(cold, stage, receipt, apply=True, **options)
    monkeypatch.setattr(type(first.manifest_path), 'unlink', original)
    assert history.retire(cold, stage, receipt, apply=True, **options)['state'] == 'completed'
    assert packed.resolve_latest_packed(cold, 'fixture').manifest_sha256 == receipt['new_manifest_sha256']


def test_private_prepared_history_retires_only_unpinned_old_versions_before_D_install(tmp_path, monkeypatch):
    source, cold, stage, first = _fixture(tmp_path, monkeypatch)
    (source/'alpha/a.txt').write_bytes(b'new value')
    current = packed.publish_packed_snapshot(cold, 'fixture', source, node_id='test',
        loose_file_threshold_bytes=12, file_chunk_bytes=16, pack_buckets=8)
    monkeypatch.setattr(stream_repack, 'PRIVATE_REPACK_DATASETS', frozenset({'fixture'}))
    with pytest.raises(SnapshotError, match='D full'):
        stream_repack.repack(cold, stage, 'fixture', current.manifest_sha256, node_id='test',
            admit_install=lambda *_a: (_ for _ in ()).throw(SnapshotError('D full')))
    body = {'manifests': {}}
    body['identity_sha256'] = hashlib.sha256(json.dumps(body, sort_keys=True, separators=(',', ':'),
                                                          ensure_ascii=False).encode()).hexdigest()
    index = tmp_path/'public-index.json'
    index.write_text(json.dumps(body))
    materialized, state = tmp_path/'materialized', tmp_path/'retention'
    materialized.mkdir()
    state.mkdir()
    receipt = {'contract': 'stockagent.source_local_prepared_history.v1', 'private': True,
               'state': 'source_hashed_E_packages_prepared', 'dataset': 'fixture',
               'old_manifest_sha256': current.manifest_sha256}
    outcome = history.retire(cold, stage, receipt, public_index=index, materialized=materialized,
        retention_state=state, process_check=lambda _paths: [], apply=True, prepared_only=True)
    assert outcome['state'] == 'completed' and not first.manifest_path.exists()
    assert len(list(stage.glob('private-prepared-history-retirement-*.json'))) == 1
    assert packed.resolve_latest_packed(cold, 'fixture').manifest_sha256 == current.manifest_sha256
    assert stream_repack._read_prepared(stage, 'fixture', current.manifest_sha256)
