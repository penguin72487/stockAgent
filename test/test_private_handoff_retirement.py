import copy
import hashlib
import json

import pytest

from scripts import publish_handoff_source_repack as publish
from stockagent.data_sync import packed_snapshots as packed, private_handoff_retirement as retirement, stream_repack
from stockagent.data_sync.desync_snapshots import SnapshotError
from test_publish_handoff_source_repack import setup_publication
from test_stream_repack import _fixture as stream_fixture


def _add_futures_dependency(cold, target, monkeypatch):
    from stockagent.data_sync import private_source_links as links

    monkeypatch.setattr(links, 'TARGET_DATASET', target.manifest['dataset'])
    original = '/original/margin_sources'
    binding = {'name': 'margin_sources', 'original_link': {'target': original},
               'original_target': original, 'dataset': target.manifest['dataset'],
               'snapshot_id': target.manifest['snapshot_id'], 'manifest_sha256': target.manifest_sha256,
               'subtree': 'preparation_sources/margin_sources', 'target_stability_sha256': 'a'*64,
               'files': 1, 'logical_bytes': 4}
    dependent = copy.deepcopy(target.manifest)
    dependent.update(dataset='local-tw-futures', snapshot_id='dependent-futures')
    dependent['source']['excluded_subtrees'] = ['margin_sources']
    dependent.setdefault('metadata', {}).update(links.source_link_metadata([binding]))
    path = cold/'manifests/local-tw-futures/dependent-futures.json'
    path.parent.mkdir(parents=True, exist_ok=True)
    packed.write_immutable_json(path, dependent)
    return path


def setup_retirement(tmp_path, monkeypatch):
    cold, stage, old, _status = setup_publication(tmp_path, monkeypatch)
    value = publish.publish('fixture')
    cache = tmp_path/'public-catalog-index.json'
    body = {'manifests': {}}
    body['identity_sha256'] = hashlib.sha256(json.dumps(body, sort_keys=True, separators=(',', ':'),
                                                          ensure_ascii=False).encode()).hexdigest()
    cache.write_text(json.dumps(body))
    materialized = tmp_path/'materialized'
    materialized.mkdir()
    state = tmp_path/'retention'
    state.mkdir()
    options = {'public_index': cache, 'materialized': materialized, 'retention_state': state,
               'process_check': lambda _paths: []}
    return cold, stage, old, value['D_publication'], options


def test_private_retirement_is_dry_run_then_deletes_only_unshared_old_encoding(tmp_path, monkeypatch):
    cold, stage, old, receipt, options = setup_retirement(tmp_path, monkeypatch)
    plan = retirement.retire(cold, stage, receipt, **options)
    assert plan['state'] == 'planned' and plan['reclaimable_bytes'] > 0
    assert packed.resolve_packed_snapshot_id(cold, 'fixture', old.manifest['snapshot_id'])
    result = retirement.retire(cold, stage, receipt, apply=True, **options)
    assert result['state'] == 'completed' and result['deleted_object_bytes'] > 0
    assert not (cold/'manifests/fixture'/(old.manifest['snapshot_id']+'.json')).exists()
    assert packed.resolve_latest_packed(cold, 'fixture').manifest_sha256 == receipt['new_manifest_sha256']


def test_private_retirement_preserves_object_referenced_by_another_manifest(tmp_path, monkeypatch):
    cold, stage, old, receipt, options = setup_retirement(tmp_path, monkeypatch)
    other = copy.deepcopy(old.manifest)
    other.update(dataset='other', snapshot_id='other-snapshot')
    path = cold/'manifests/other/other-snapshot.json'
    path.parent.mkdir(parents=True)
    packed.write_immutable_json(path, other)
    old_objects = {row['relpath'] for row in old.manifest['archive']['objects']}
    result = retirement.retire(cold, stage, receipt, apply=True, **options)
    assert result['state'] == 'completed'
    assert all((cold/relative).exists() for relative in old_objects)
    assert path.is_file()


def test_private_retirement_does_not_guess_a_new_dependency_contract(tmp_path, monkeypatch):
    cold, stage, old, receipt, options = setup_retirement(tmp_path, monkeypatch)
    other = copy.deepcopy(old.manifest)
    other.update(dataset='other', snapshot_id='other-snapshot')
    other.setdefault('metadata', {})['external_dependency_binding'] = 'unknown format'
    path = cold/'manifests/other/other-snapshot.json'
    path.parent.mkdir(parents=True)
    packed.write_immutable_json(path, other)
    with pytest.raises(SnapshotError, match='unsupported private dependency metadata'):
        retirement.retire(cold, stage, receipt, apply=True, **options)
    assert old.manifest_path.exists() and path.exists()


def test_private_retirement_protects_research_pin_and_live_reader(tmp_path, monkeypatch):
    cold, stage, old, receipt, options = setup_retirement(tmp_path, monkeypatch)
    with pytest.raises(SnapshotError, match='in use'):
        retirement.retire(cold, stage, receipt, **{**options, 'process_check': lambda _paths: ['reader']})
    root = options['retention_state']/'research-pins/study'
    root.mkdir(parents=True)
    marker = options['retention_state']/retirement.packed_retention.RESEARCH_PIN_MARKER
    marker.write_text(json.dumps({'contract': retirement.packed_retention.RESEARCH_PIN_CONTRACT, 'schema_version': 1}))
    (root/'version.pin.json').write_text(json.dumps({'snapshot_id': old.manifest['snapshot_id']}))
    with pytest.raises(SnapshotError, match='pinned'):
        retirement.retire(cold, stage, receipt, apply=True, **options)
    assert packed.resolve_packed_snapshot_id(cold, 'fixture', old.manifest['snapshot_id'])


def test_private_retirement_keeps_a_fixed_source_link_dependency(tmp_path, monkeypatch):
    cold, stage, old, receipt, options = setup_retirement(tmp_path, monkeypatch)
    dependent = _add_futures_dependency(cold, old, monkeypatch)
    with pytest.raises(SnapshotError, match='pinned'):
        retirement.retire(cold, stage, receipt, apply=True, **options)
    assert old.manifest_path.exists()
    dependent.unlink()
    assert retirement.retire(cold, stage, receipt, apply=True, **options)['state'] == 'completed'


def test_pending_parent_preparation_protects_sqlite_state_before_manifest_commit(tmp_path):
    parent, relative = 'local-fixture', 'state/queue.sqlite3'
    dataset = parent+'-state-'+hashlib.sha256(relative.encode()).hexdigest()[:12]
    cold, control = tmp_path/'D-private', tmp_path/'control'
    operation = control/'operations'/('a'*16+'-'+'b'*32)
    (operation/'preparations').mkdir(parents=True)
    (control/'current.json').write_text(json.dumps({'contract': 'all_catalog_incremental_migration_v1',
        'operation_directory': str(operation)}))
    path = operation/'preparations'/(parent+'.json')
    value = {'contract': 'all_catalog_incremental_migration_v1', 'dataset': parent, 'state': 'prepared',
        'sqlite_states': [{'source_relative': relative, 'dataset': dataset,
            'snapshot_id': 'fixed-observation', 'manifest_sha256': 'a'*64}]}
    path.write_text(json.dumps(value))
    options = {'control': control, 'expected_cold': cold}
    assert retirement._pending_preparation_protections(cold, dataset, **options) == {'fixed-observation': 'a'*64}
    value['sqlite_states'][0]['source_relative'] = 'another.sqlite3'
    path.write_text(json.dumps(value))
    with pytest.raises(SnapshotError, match='dependency source differs'):
        retirement._pending_preparation_protections(cold, dataset, **options)


def test_private_retirement_rechecks_dependency_pin_inside_final_owner(tmp_path, monkeypatch):
    cold, stage, old, receipt, options = setup_retirement(tmp_path, monkeypatch)
    observed = iter(({}, {old.manifest['snapshot_id']: old.manifest_sha256}))
    monkeypatch.setattr(retirement, '_source_link_protections', lambda *_: next(observed))
    with pytest.raises(SnapshotError, match='pinned'):
        retirement.retire(cold, stage, receipt, apply=True, **options)
    assert old.manifest_path.exists()


def test_private_retirement_fails_closed_on_wrong_fixed_dependency_sha(tmp_path, monkeypatch):
    cold, stage, old, receipt, options = setup_retirement(tmp_path, monkeypatch)
    dependent = _add_futures_dependency(cold, old, monkeypatch)
    value = json.loads(dependent.read_text())
    key = 'private_source_link_bindings'
    metadata = json.loads(value['metadata'][key])
    metadata['links'][0]['manifest_sha256'] = 'b'*64
    value['metadata'][key] = json.dumps(metadata)
    dependent.write_text(json.dumps(value))
    with pytest.raises(SnapshotError, match='dependency manifest changed or differs'):
        retirement.retire(cold, stage, receipt, apply=True, **options)
    assert old.manifest_path.exists()


def test_private_retirement_resumes_after_old_manifest_unlink(tmp_path, monkeypatch):
    cold, stage, old, receipt, options = setup_retirement(tmp_path, monkeypatch)
    path = cold/'manifests/fixture'/(old.manifest['snapshot_id']+'.json')
    original = type(path).unlink
    def interrupted(candidate, *args, **kw):
        result = original(candidate, *args, **kw)
        if candidate == path:
            raise OSError('after manifest unlink')
        return result
    monkeypatch.setattr(type(path), 'unlink', interrupted)
    with pytest.raises(OSError, match='after manifest unlink'):
        retirement.retire(cold, stage, receipt, apply=True, **options)
    assert not path.exists()
    monkeypatch.setattr(type(path), 'unlink', original)
    result = retirement.retire(cold, stage, receipt, apply=True, **options)
    assert result['state'] == 'completed'
    assert packed.resolve_latest_packed(cold, 'fixture').manifest_sha256 == receipt['new_manifest_sha256']


def test_private_stream_retirement_keeps_shared_history_and_new_head(tmp_path, monkeypatch):
    _source, cold, stage, old = stream_fixture(tmp_path, monkeypatch)
    monkeypatch.setattr(stream_repack, 'PRIVATE_REPACK_ROOT', cold)
    monkeypatch.setattr(stream_repack, 'PRIVATE_REPACK_DATASETS', frozenset({'fixture'}))
    monkeypatch.setattr(packed, '_check_native_pack_read_authority', lambda *_a: None)
    result = stream_repack.repack(cold, stage, 'fixture', old.manifest_sha256, node_id='test')
    receipt = {'contract': 'stockagent.source_local_exact_repack.v1', 'private': True,
        'state': 'new_D_head_committed_from_exact_E_source', 'dataset': 'fixture',
        'source_mode': 'D_streamed_to_E_dataset_pack', 'old_manifest_sha256': old.manifest_sha256,
        'new_manifest_sha256': result['new_manifest_sha256'], 'new_snapshot_id': result['snapshot_id']}
    other = copy.deepcopy(old.manifest)
    other.update(dataset='other', snapshot_id='other-snapshot')
    shared = cold/'manifests/other/other-snapshot.json'
    shared.parent.mkdir(parents=True)
    packed.write_immutable_json(shared, other)
    body = {'manifests': {}}
    body['identity_sha256'] = hashlib.sha256(json.dumps(body, sort_keys=True, separators=(',', ':'),
                                                          ensure_ascii=False).encode()).hexdigest()
    index = tmp_path/'public-index.json'
    index.write_text(json.dumps(body))
    materialized, state = tmp_path/'materialized', tmp_path/'retention'
    materialized.mkdir()
    state.mkdir()
    allowed = stream_repack.private_repack_source_allowed
    monkeypatch.setattr(stream_repack, 'private_repack_source_allowed', lambda *_args: False)
    with pytest.raises(SnapshotError, match='excluded source owner'):
        retirement.retire(cold, stage, receipt, public_index=index, materialized=materialized,
                          retention_state=state, process_check=lambda _paths: [], apply=True)
    assert old.manifest_path.exists()
    monkeypatch.setattr(stream_repack, 'private_repack_source_allowed', allowed)
    outcome = retirement.retire(cold, stage, receipt, public_index=index, materialized=materialized,
                                retention_state=state, process_check=lambda _paths: [], apply=True)
    assert outcome['state'] == 'completed' and shared.is_file()
    assert all((cold/row['relpath']).is_file() for row in old.manifest['archive']['objects'])
    assert packed.resolve_latest_packed(cold, 'fixture').manifest_sha256 == result['new_manifest_sha256']
