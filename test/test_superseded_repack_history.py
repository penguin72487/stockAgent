import json
import os
import sys

import pytest

from stockagent.data_sync import packed_snapshots as packed, repack_retirement, stream_repack, superseded_repack_history
from stockagent.data_sync.packed_backup import signature
from stockagent.data_sync.desync_snapshots import SnapshotError


def test_protection_fallback_rechecks_cross_authority_pins(tmp_path, monkeypatch):
    cold = tmp_path/'cold'
    (cold/'manifests/public').mkdir(parents=True)
    calls = []
    def include(_cold, _materialized, known):
        calls.append(True)
        return set(known) | ({'private-snapshot'} if len(calls) > 1 else set())
    def load(_materialized, known, **_kwargs):
        if 'private-snapshot' not in known:
            return set(), [], ['the newly created private pin is not in the first census']
        return {'private-snapshot'}, [], []
    monkeypatch.setattr(repack_retirement, 'include_other_authority_repack_pins', include)
    monkeypatch.setattr(superseded_repack_history.packed_retention, '_local_protected_snapshot_ids', load)
    assert superseded_repack_history._protect(tmp_path/'materialized', tmp_path/'state', set(), cold=cold) == {'private-snapshot'}
    assert len(calls) == 2


def test_protection_fallback_still_rejects_unresolved_pins(tmp_path, monkeypatch):
    cold = tmp_path/'cold'
    (cold/'manifests/public').mkdir(parents=True)
    monkeypatch.setattr(repack_retirement, 'include_other_authority_repack_pins',
                        lambda _cold, _materialized, known: set(known))
    monkeypatch.setattr(superseded_repack_history.packed_retention, '_local_protected_snapshot_ids',
                        lambda *_a, **_kw: (set(), [], ['unresolved pin']))
    with pytest.raises(SnapshotError, match='protection metadata unknown'):
        superseded_repack_history._protect(tmp_path/'materialized', tmp_path/'state', set(), cold=cold)


def _fixture(tmp_path, monkeypatch):
    source, cold, stage = tmp_path/'source', tmp_path/'cold', tmp_path/'stage'
    source.mkdir()
    (source/'a.txt').write_bytes(b'one')
    (source/'b.bin').write_bytes(b'two'*100)
    old = packed.publish_packed_snapshot(cold, 'fixture', source, node_id='test',
                                         loose_file_threshold_bytes=16, file_chunk_bytes=32)
    older_path = cold/'manifests/fixture/fixture-older.json'
    older = {**old.manifest, 'snapshot_id': 'fixture-older'}
    older_path.write_text(json.dumps(older))
    monkeypatch.setattr(packed, '_notify_packed_publication', lambda *_a, **_k: None)
    result = stream_repack.repack(cold, stage, 'fixture', old.manifest_sha256, node_id='test')
    receipt = {'contract': 'stockagent.source_local_exact_repack.v1',
        'state': 'new_D_head_committed_from_exact_E_source', 'source_mode': 'D_streamed_to_E_dataset_pack',
        'dataset': 'fixture', 'old_manifest_sha256': old.manifest_sha256,
        'new_manifest_sha256': result['new_manifest_sha256'], 'new_snapshot_id': result['snapshot_id'],
        'old_release_removed': True}
    paths = [old.manifest_path, older_path]
    metadata = {path.relative_to(cold).as_posix(): list(signature(path)) for path in paths}
    refs = [old.manifest['archive']['inventory'], *old.manifest['archive']['objects']]
    keys = [row['relpath']+'@'+row['sha256'] for row in refs]
    catalog = tmp_path/'catalog.json'
    catalog.write_text(json.dumps({'releases': [{'manifest_relative': path.relative_to(cold).as_posix(),
                                                'required_file_keys': keys} for path in paths],
                                   'metadata_errors': [], 'excluded_manifests': []}))
    def index(*_a, **_k):
        return metadata, {}, 'catalog-id'
    monkeypatch.setattr(repack_retirement.old_retirement, 'catalog_index', index)
    monkeypatch.setattr(superseded_repack_history, 'catalog_index', index)
    monkeypatch.setattr(repack_retirement, '_private_catalog_generation', lambda *_a: [1])
    monkeypatch.setattr(superseded_repack_history, '_private_catalog_generation', lambda *_a: [1])
    monkeypatch.setattr(repack_retirement, 'process_references_many', lambda *_a, **_k: [])
    monkeypatch.setattr(superseded_repack_history, 'process_references_many', lambda *_a, **_k: [])
    monkeypatch.setattr(packed_retention := superseded_repack_history.packed_retention,
                        '_local_protected_snapshot_ids', lambda *_a, **_k: (set(), [], []))
    kwargs = {'catalog': catalog, 'observation': tmp_path/'obs', 'cache': tmp_path/'cache',
              'materialized': tmp_path/'materialized', 'retention_state': tmp_path/'retention'}
    # Simulate that the direct source-version retirement completed first.
    direct_receipt = {**receipt, 'old_release_removed': False}
    repack_retirement.retire(cold, stage, direct_receipt, **kwargs, apply=True)
    return cold, stage, older_path, receipt, kwargs, refs, packed_retention


def test_history_retirement_removes_only_unpinned_superseded_versions(tmp_path, monkeypatch):
    cold, stage, older, receipt, kwargs, refs, _ = _fixture(tmp_path, monkeypatch)
    plan = superseded_repack_history.retire(cold, stage, receipt, **kwargs)
    assert plan['state'] == 'planned' and len(plan['plan']['versions']) == 1
    result = superseded_repack_history.retire(cold, stage, receipt, **kwargs, apply=True)
    assert result['state'] == 'completed' and result['versions_removed'] == 1 and not older.exists()
    assert result['object_bytes_removed'] > 0 and all(not (cold/ref['relpath']).exists() for ref in refs)
    assert packed.resolve_latest_packed(cold, 'fixture').manifest_sha256 == receipt['new_manifest_sha256']


def test_shared_manifest_witness_reduces_catalog_targets_and_retains_objects(tmp_path, monkeypatch):
    cold, stage, older, receipt, kwargs, refs, _ = _fixture(tmp_path, monkeypatch)
    witness = older.with_name('fixture-witness.json')
    value = json.loads(older.read_text())
    value['snapshot_id'] = witness.stem
    witness.write_text(json.dumps(value))
    monkeypatch.setattr(superseded_repack_history, 'MAX_VERSIONS', 1)
    seen = []
    original = repack_retirement.catalog_references
    def counted(catalog, root, metadata, targets, generation, **options):
        seen.append(set(targets))
        return original(catalog, root, metadata, targets, generation, **options)
    monkeypatch.setattr(repack_retirement, 'catalog_references', counted)

    result = superseded_repack_history.retire(cold, stage, receipt, **kwargs, apply=True)
    assert result['state'] == 'completed' and result['versions_removed'] == 1
    assert not older.exists() and witness.exists()
    assert seen == [set()] and result['plan']['catalog_graph_target_count'] == 0
    assert result['plan']['reference_witnesses'][0]['relative'] == witness.relative_to(cold).as_posix()
    assert result['object_bytes_removed'] == 0
    assert all((cold/ref['relpath']).exists() for ref in refs)


def test_partially_shared_history_scans_and_retires_only_unwitnessed_objects(tmp_path, monkeypatch):
    cold, stage, older, receipt, kwargs, refs, _ = _fixture(tmp_path, monkeypatch)
    source = tmp_path/'different-source'
    source.mkdir()
    (source/'a.txt').write_bytes(b'one')
    (source/'b.bin').write_bytes(b'two'*99+b'END')
    witness_store = tmp_path/'witness-store'
    packed.publish_packed_snapshot(witness_store, 'fixture', tmp_path/'source', node_id='test',
                                   loose_file_threshold_bytes=16, file_chunk_bytes=32)
    witness = packed.publish_packed_snapshot(witness_store, 'fixture', source, node_id='test',
                                              loose_file_threshold_bytes=16, file_chunk_bytes=32)
    for relative in repack_retirement._refs(witness.manifest):
        source_object, destination = witness_store/relative, cold/relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        if destination.exists():
            assert destination.read_bytes() == source_object.read_bytes()
        else:
            destination.write_bytes(source_object.read_bytes())
    witness_path = older.with_name('fixture-witness.json')
    witness_manifest = {**witness.manifest, 'snapshot_id': witness_path.stem}
    witness_path.write_text(json.dumps(witness_manifest))
    monkeypatch.setattr(superseded_repack_history, 'MAX_VERSIONS', 1)
    old_refs = {item['relpath'] for item in refs}
    witness_refs = repack_retirement._refs(witness_manifest)
    shared, unique = old_refs & witness_refs, old_refs - witness_refs
    assert shared and unique
    seen = []
    original = repack_retirement.catalog_references
    def counted(catalog, root, metadata, targets, generation, **options):
        seen.append(set(targets))
        return original(catalog, root, metadata, targets, generation, **options)
    monkeypatch.setattr(repack_retirement, 'catalog_references', counted)

    result = superseded_repack_history.retire(cold, stage, receipt, **kwargs, apply=True)
    assert result['state'] == 'completed' and not older.exists()
    assert seen == [unique] and result['plan']['catalog_graph_target_count'] == len(unique)
    assert all((cold/ref).exists() for ref in shared)
    assert all(not (cold/ref).exists() for ref in unique)
    assert packed.resolve_latest_packed(cold, 'fixture').manifest_sha256 == receipt['new_manifest_sha256']
    assert witness_path.is_file()


def test_changed_manifest_witness_blocks_before_any_history_unlink(tmp_path, monkeypatch):
    cold, stage, older, receipt, kwargs, refs, _ = _fixture(tmp_path, monkeypatch)
    witness = older.with_name('fixture-witness.json')
    value = json.loads(older.read_text())
    value['snapshot_id'] = witness.stem
    witness.write_text(json.dumps(value))
    monkeypatch.setattr(superseded_repack_history, 'MAX_VERSIONS', 1)
    original = repack_retirement.fresh_manifest_references
    def changed_after_scan(*args, **options):
        result = original(*args, **options)
        witness.write_text(witness.read_text()+' ')
        return result
    monkeypatch.setattr(repack_retirement, 'fresh_manifest_references', changed_after_scan)

    with pytest.raises(SnapshotError, match='shared-reference witness changed'):
        superseded_repack_history.retire(cold, stage, receipt, **kwargs, apply=True)
    assert older.exists() and witness.exists() and all((cold/ref['relpath']).exists() for ref in refs)


def test_missing_witness_blocks_before_any_history_unlink(tmp_path, monkeypatch):
    cold, stage, older, receipt, kwargs, refs, _ = _fixture(tmp_path, monkeypatch)
    witness = older.with_name('fixture-witness.json')
    value = json.loads(older.read_text())
    value['snapshot_id'] = witness.stem
    witness.write_text(json.dumps(value))
    monkeypatch.setattr(superseded_repack_history, 'MAX_VERSIONS', 1)
    original = repack_retirement.fresh_manifest_references
    def removed_after_scan(*args, **options):
        result = original(*args, **options)
        witness.unlink()
        return result
    monkeypatch.setattr(repack_retirement, 'fresh_manifest_references', removed_after_scan)

    with pytest.raises(SnapshotError, match='missing or redirected|witness'):
        superseded_repack_history.retire(cold, stage, receipt, **kwargs, apply=True)
    assert older.exists() and all((cold/ref['relpath']).exists() for ref in refs)


def test_shared_witnesses_do_not_bypass_incomplete_catalog(tmp_path, monkeypatch):
    cold, stage, older, receipt, kwargs, refs, _ = _fixture(tmp_path, monkeypatch)
    witness = older.with_name('fixture-witness.json')
    value = json.loads(older.read_text())
    value['snapshot_id'] = witness.stem
    witness.write_text(json.dumps(value))
    monkeypatch.setattr(superseded_repack_history, 'MAX_VERSIONS', 1)
    catalog = json.loads(kwargs['catalog'].read_text())
    catalog['metadata_errors'] = ['incomplete']
    kwargs['catalog'].write_text(json.dumps(catalog))

    with pytest.raises(SnapshotError, match='catalog graph is incomplete'):
        superseded_repack_history.retire(cold, stage, receipt, **kwargs, apply=True)
    assert older.exists() and witness.exists() and all((cold/ref['relpath']).exists() for ref in refs)


def test_other_dataset_retires_during_pinned_stream_with_stale_catalog(tmp_path, monkeypatch):
    original_protection = superseded_repack_history.packed_retention._local_protected_snapshot_ids
    cold, stage, older, receipt, kwargs, _refs, retention = _fixture(tmp_path, monkeypatch)
    monkeypatch.setattr(retention, '_local_protected_snapshot_ids', original_protection)
    kwargs['materialized'].mkdir()
    source = tmp_path/'other-source'
    source.mkdir()
    (source/'other.bin').write_bytes(b'other source')
    other = packed.publish_packed_snapshot(cold, 'another', source, node_id='test')
    # The fixed catalog predates "another". Its real current manifest must
    # make the concurrent pin known without weakening pin/error checks.
    build = stream_repack._build
    completed = []
    def build_while_retiring(*args, **kw):
        retired = superseded_repack_history.retire(cold, stage, receipt, **kwargs, apply=True)
        completed.append(retired['state'])
        assert not older.exists()
        return build(*args, **kw)
    monkeypatch.setattr(stream_repack, '_build', build_while_retiring)
    result = stream_repack.repack(cold, tmp_path/'other-stage', 'another', other.manifest_sha256,
        node_id='test', materialized_pin_root=kwargs['materialized'])
    assert completed == ['completed'] and result['state'] == 'published'
    assert not list(kwargs['materialized'].rglob('*.pin.json'))


def test_history_retirement_preserves_research_pin(tmp_path, monkeypatch):
    cold, stage, older, receipt, kwargs, _refs, retention = _fixture(tmp_path, monkeypatch)
    monkeypatch.setattr(retention, '_local_protected_snapshot_ids',
                        lambda *_a, **_k: ({'fixture-older'}, [], []))
    result = superseded_repack_history.retire(cold, stage, receipt, **kwargs, apply=True)
    assert result['state'] == 'no_unpinned_history' and result['protected_versions'] == 1 and older.exists()


def test_history_retirement_checks_busy_owner_before_catalog_scan(tmp_path, monkeypatch):
    cold, stage, _older, receipt, kwargs, _refs, _ = _fixture(tmp_path, monkeypatch)
    from contextlib import contextmanager
    @contextmanager
    def busy(*_args, **_kwargs):
        raise SnapshotError('retirement owner busy: source-replication-owner.lock')
        yield
    monkeypatch.setattr(repack_retirement, '_boundary', busy)
    monkeypatch.setattr(superseded_repack_history, 'catalog_index',
                        lambda *_a, **_kw: pytest.fail('catalog scanned despite busy owner'))
    with pytest.raises(SnapshotError, match='source-replication-owner.lock'):
        superseded_repack_history.retire(cold, stage, receipt, **kwargs, apply=True)


def test_empty_history_census_skips_catalog_without_deleting_anything(tmp_path, monkeypatch):
    cold, stage, older, receipt, kwargs, _refs, _ = _fixture(tmp_path, monkeypatch)
    older.unlink()
    monkeypatch.setattr(superseded_repack_history, 'catalog_index',
                        lambda *_a, **_kw: pytest.fail('empty history does not need the catalog'))
    result = superseded_repack_history.retire(cold, stage, receipt, **kwargs, apply=True)
    assert result['state'] == 'no_unpinned_history'
    assert result['versions_removed'] == result['object_bytes_removed'] == 0
    assert packed.resolve_latest_packed(cold, 'fixture').manifest_sha256 == receipt['new_manifest_sha256']


@pytest.mark.parametrize('damage', [None, 'extra_history', 'bad_head', 'unfinished_journal'])
def test_missing_old_anchor_only_reports_empty_history_for_a_valid_sole_current_head(tmp_path, monkeypatch, damage):
    cold, stage, older, receipt, kwargs, refs, _ = _fixture(tmp_path, monkeypatch)
    original = cold/'manifests/fixture'/(receipt['new_snapshot_id']+'.json')
    (tmp_path/'source/a.txt').write_bytes(b'newer generation')
    current = packed.publish_packed_snapshot(cold, 'fixture', tmp_path/'source', node_id='test',
                                             loose_file_threshold_bytes=16, file_chunk_bytes=32)
    original.unlink()  # Simulate a later, separately journalled history sweep.
    if damage != 'extra_history':
        older.unlink()
    if damage == 'bad_head':
        head = json.loads(current.head_path.read_text())
        head['manifest_sha256'] = '0'*64
        current.head_path.write_text(json.dumps(head))
    if damage == 'unfinished_journal':
        monkeypatch.setattr(superseded_repack_history, '_active_journal',
                            lambda *_a, **_k: (stage/'pending.json', {'state': 'planned'}))
    monkeypatch.setattr(superseded_repack_history, 'catalog_index',
                        lambda *_a, **_k: pytest.fail('no catalog scan should be needed'))
    if damage is None:
        result = superseded_repack_history.retire(cold, stage, receipt, **kwargs, apply=True)
        assert result['state'] == 'no_unpinned_history'
        assert result['original_anchor_missing_without_successor'] is True
        assert result['observed_current_head'] == {
            'snapshot_id': current.manifest['snapshot_id'], 'manifest_sha256': current.manifest_sha256}
        assert result['versions_removed'] == result['object_bytes_removed'] == 0
        assert current.manifest_path.exists() and all((cold/row['relpath']).exists() for row in refs)
    else:
        with pytest.raises(SnapshotError):
            superseded_repack_history.retire(cold, stage, receipt, **kwargs, apply=True)
        assert current.manifest_path.exists()
        if damage == 'extra_history':
            assert older.exists()


@pytest.mark.parametrize('apply', [False, True])
def test_cli_keeps_missing_anchor_observation_distinct_in_the_receipt(tmp_path, monkeypatch, capsys, apply):
    from scripts import retire_superseded_repack_history as cli
    old, new = 'a'*64, 'b'*64
    workspace = tmp_path/'workspace'
    stage = workspace/'migration/stream-source-repack-v1/fixture'/old
    stage.mkdir(parents=True)
    (stage/'install.json').write_text(json.dumps({'new_manifest_sha256': new}))
    receipt_path = workspace/'migration/source-local-exact-repack/fixture'/(new+'.json')
    receipt_path.parent.mkdir(parents=True)
    receipt_path.write_text(json.dumps({'old_manifest_sha256': old}))
    before = receipt_path.read_bytes()
    observed = {'snapshot_id': 'fixture-newer', 'manifest_sha256': 'c'*64}
    monkeypatch.setattr(cli.workspace_volume, 'configuration', lambda: {'mount_point': str(workspace)})
    monkeypatch.setattr(cli.workspace_volume, 'check', lambda **_k: None)
    monkeypatch.setattr(cli, '_check_d_primary_mount', lambda *_a: None)
    monkeypatch.setattr(cli.superseded_repack_history, 'retire', lambda *_a, **_k: {
        'state': 'no_unpinned_history', 'versions_removed': 0, 'object_bytes_removed': 0,
        'original_anchor_missing_without_successor': True, 'observed_current_head': observed})
    args = ['retire-history', '--dataset', 'fixture', '--old-manifest-sha256', old]
    monkeypatch.setattr(sys, 'argv', args + (['--apply'] if apply else []))
    assert cli.main() == 0
    result = json.loads(capsys.readouterr().out)
    assert result['original_anchor_missing_without_successor'] is True
    assert result['observed_current_head'] == observed and result['object_bytes'] == 0
    assert result['current_D_head_preserved'] is True
    if apply:
        saved = json.loads(receipt_path.read_text())['history_retirement']
        assert saved['original_anchor_missing_without_successor'] is True
        assert saved['observed_current_head'] == observed
        assert saved['historical_versions_independently_recovered'] is False
    else:
        assert receipt_path.read_bytes() == before


def test_history_retirement_preserves_another_nodes_current_head(tmp_path, monkeypatch):
    cold, stage, older, receipt, kwargs, _refs, _ = _fixture(tmp_path, monkeypatch)
    (cold/'heads/fixture/peer.json').write_text(json.dumps({'dataset': 'fixture', 'snapshot_id': 'fixture-older'}))
    result = superseded_repack_history.retire(cold, stage, receipt, **kwargs, apply=True)
    assert result['state'] == 'no_unpinned_history' and result['protected_versions'] == 1 and older.exists()


def test_history_retirement_preserves_private_hardlink_without_counting_its_space(tmp_path, monkeypatch):
    cold, stage, _older, receipt, kwargs, refs, _ = _fixture(tmp_path, monkeypatch)
    source = cold/refs[0]['relpath']
    alias = tmp_path/'private-object'
    os.link(source, alias)
    result = superseded_repack_history.retire(cold, stage, receipt, **kwargs, apply=True)
    item = next(row for row in result['plan']['items'] if row['relative'] == refs[0]['relpath'])
    assert item['allocated_bytes'] == 0 and alias.is_file() and not source.exists()


def test_history_retirement_resumes_after_partial_batch_unlink(tmp_path, monkeypatch):
    cold, stage, older, receipt, kwargs, _refs, _ = _fixture(tmp_path, monkeypatch)
    original = packed._fsync_directory
    failed = False
    def interrupted(path):
        nonlocal failed
        if path == older.parent and not failed:
            failed = True
            raise OSError('interrupted after history unlink')
        return original(path)
    monkeypatch.setattr(packed, '_fsync_directory', interrupted)
    with pytest.raises(OSError, match='interrupted after history unlink'):
        superseded_repack_history.retire(cold, stage, receipt, **kwargs, apply=True)
    monkeypatch.setattr(packed, '_fsync_directory', original)
    result = superseded_repack_history.retire(cold, stage, receipt, **kwargs, apply=True)
    assert result['state'] == 'completed' and not older.exists()


@pytest.mark.parametrize('mode', ['ordinary', 'pinned', 'interrupted'])
def test_history_retirement_follows_verified_semantic_successor(tmp_path, monkeypatch, mode):
    cold, stage, older, receipt, kwargs, _refs, retention = _fixture(tmp_path, monkeypatch)
    current_sha, current_id = receipt['new_manifest_sha256'], receipt['new_snapshot_id']
    successor = {'manifest_sha256': current_sha, 'snapshot_id': current_id,
                 'comparison_identity_sha256': 'c'*64}
    missing_sha, missing_id = receipt['old_manifest_sha256'], 'fixture-retired-repack'
    receipt.update(new_manifest_sha256=missing_sha, new_snapshot_id=missing_id)
    calls = []
    def checked_successor(_cold, _stage, dataset, old_sha, old_id):
        assert (dataset, old_sha, old_id) == ('fixture', missing_sha, missing_id)
        calls.append(True)
        return successor
    monkeypatch.setattr(superseded_repack_history, '_semantic_successor', checked_successor)
    if mode == 'pinned':
        monkeypatch.setattr(retention, '_local_protected_snapshot_ids',
                            lambda *_a, **_k: ({'fixture-older'}, [], []))
        result = superseded_repack_history.retire(cold, stage, receipt, **kwargs, apply=True)
        assert result['state'] == 'no_unpinned_history' and older.exists()
        assert result['effective_new_manifest_sha256'] == current_sha
    else:
        dry = superseded_repack_history.retire(cold, stage, receipt, **kwargs)
        assert dry['state'] == 'planned' and older.exists()
        assert dry['semantic_successor'] == successor
        if mode == 'interrupted':
            original = packed._fsync_directory
            failed = False
            def interrupted(path):
                nonlocal failed
                if path == older.parent and not failed:
                    failed = True
                    raise OSError('interrupted after successor history unlink')
                return original(path)
            monkeypatch.setattr(packed, '_fsync_directory', interrupted)
            with pytest.raises(OSError, match='interrupted after successor'):
                superseded_repack_history.retire(cold, stage, receipt, **kwargs, apply=True)
            monkeypatch.setattr(packed, '_fsync_directory', original)
        result = superseded_repack_history.retire(cold, stage, receipt, **kwargs, apply=True)
        assert result['state'] == 'completed' and not older.exists()
        assert result['new_manifest_sha256'] == current_sha
        assert result['original_new_manifest_sha256'] == missing_sha
    assert calls and packed.resolve_latest_packed(cold, 'fixture', require_objects=True).manifest_sha256 == current_sha


@pytest.mark.parametrize('mode', ['ordinary', 'pinned', 'interrupted'])
def test_history_retirement_preserves_latest_fixed_successor_scope(tmp_path, monkeypatch, mode):
    from stockagent.data_sync import latest_repack_successor

    cold, stage, older, receipt, kwargs, _refs, retention = _fixture(tmp_path, monkeypatch)
    current_sha, current_id = receipt['new_manifest_sha256'], receipt['new_snapshot_id']
    missing_sha, missing_id = receipt['old_manifest_sha256'], 'fixture-retired-repack'
    receipt.update(new_manifest_sha256=missing_sha, new_snapshot_id=missing_id)
    successor = {'manifest_sha256': current_sha, 'snapshot_id': current_id,
                 'comparison_identity_sha256': 'c' * 64, 'changed_member_values_compared': False,
                 'old_historical_value_coverage': 'not_compared', 'uncompared_changed_members': 2}
    calls = []
    def checked(_cold, _stage, dataset, old_sha, old_id):
        assert (dataset, old_sha, old_id) == ('fixture', missing_sha, missing_id)
        calls.append(True)
        return successor
    monkeypatch.setattr(latest_repack_successor, 'latest_fixed_successor', checked)
    if mode == 'pinned':
        monkeypatch.setattr(retention, '_local_protected_snapshot_ids',
                            lambda *_a, **_k: ({'fixture-older'}, [], []))
        result = superseded_repack_history.retire(cold, stage, receipt, **kwargs, apply=True)
        assert result['state'] == 'no_unpinned_history' and older.exists()
        assert result['latest_fixed_successor'] == successor
    else:
        dry = superseded_repack_history.retire(cold, stage, receipt, **kwargs)
        assert dry['state'] == 'planned' and older.exists()
        assert dry['latest_fixed_successor'] == successor and 'semantic_successor' not in dry
        if mode == 'interrupted':
            original = packed._fsync_directory
            failed = False
            def interrupted(path):
                nonlocal failed
                if path == older.parent and not failed:
                    failed = True
                    raise OSError('interrupted after latest successor')
                return original(path)
            monkeypatch.setattr(packed, '_fsync_directory', interrupted)
            with pytest.raises(OSError, match='interrupted after latest successor'):
                superseded_repack_history.retire(cold, stage, receipt, **kwargs, apply=True)
            monkeypatch.setattr(packed, '_fsync_directory', original)
        result = superseded_repack_history.retire(cold, stage, receipt, **kwargs, apply=True)
        assert result['state'] == 'completed' and not older.exists()
        assert result['latest_fixed_successor'] == successor
        assert 'semantic_successor' not in result
    assert calls and packed.resolve_latest_packed(cold, 'fixture', require_objects=True).manifest_sha256 == current_sha


def _prepared_fixture(tmp_path, monkeypatch):
    cold, _stage, older, _receipt, kwargs, refs, retention = _fixture(tmp_path, monkeypatch)
    current = packed.resolve_latest_packed(cold, 'fixture')
    stage = tmp_path/'large-E-prepared'
    def no_D_space(_prepared, _journal):
        raise SnapshotError('D full')
    with pytest.raises(SnapshotError, match='D full'):
        stream_repack.repack(cold, stage, 'fixture', current.manifest_sha256, admit_install=no_D_space)
    receipt = {'contract': 'stockagent.source_local_prepared_history.v1',
               'state': 'source_hashed_E_packages_prepared', 'dataset': 'fixture',
               'old_manifest_sha256': current.manifest_sha256}
    return cold, stage, older, receipt, kwargs, refs, retention, current


def test_prepared_history_preserves_current_D_head_and_E_packages(tmp_path, monkeypatch):
    cold, stage, older, receipt, kwargs, _refs, _retention, current = _prepared_fixture(tmp_path, monkeypatch)
    current_refs = [current.manifest['archive']['inventory'], *current.manifest['archive']['objects']]
    result = superseded_repack_history.retire(cold, stage, receipt, **kwargs, apply=True, prepared_only=True)
    assert result['state'] == 'completed' and not older.exists()
    assert packed.resolve_latest_packed(cold, 'fixture').manifest_sha256 == current.manifest_sha256
    assert all((cold/ref['relpath']).is_file() for ref in current_refs)
    assert stream_repack._read_prepared(stage, 'fixture', current.manifest_sha256)['source_member_sha_streamed']
    assert list(stage.glob('prepared-history-retirement-*.json'))


def test_prepared_history_refuses_changed_current_head(tmp_path, monkeypatch):
    cold, stage, older, receipt, kwargs, _refs, _retention, _current = _prepared_fixture(tmp_path, monkeypatch)
    (tmp_path/'source'/'new.txt').write_bytes(b'new current source')
    packed.publish_packed_snapshot(cold, 'fixture', tmp_path/'source', node_id='test')
    with pytest.raises(SnapshotError, match='current D head changed'):
        superseded_repack_history.retire(cold, stage, receipt, **kwargs, apply=True, prepared_only=True)
    assert older.is_file()


def test_prepared_history_preserves_research_pin(tmp_path, monkeypatch):
    cold, stage, older, receipt, kwargs, _refs, retention, current = _prepared_fixture(tmp_path, monkeypatch)
    monkeypatch.setattr(retention, '_local_protected_snapshot_ids',
                        lambda *_a, **_k: ({'fixture-older'}, [], []))
    result = superseded_repack_history.retire(cold, stage, receipt, **kwargs, apply=True, prepared_only=True)
    assert result['state'] == 'no_unpinned_history' and older.is_file()
    assert packed.resolve_latest_packed(cold, 'fixture').manifest_sha256 == current.manifest_sha256


def test_prepared_history_rejects_changed_E_generation(tmp_path, monkeypatch):
    cold, stage, older, receipt, kwargs, _refs, _retention, current = _prepared_fixture(tmp_path, monkeypatch)
    prepared = stream_repack._read_prepared(stage, 'fixture', current.manifest_sha256)
    package = stage/prepared['packages'][0]['name']
    package.write_bytes(b'x'*package.stat().st_size)
    with pytest.raises(SnapshotError, match='generation changed'):
        superseded_repack_history.retire(cold, stage, receipt, **kwargs, apply=True, prepared_only=True)
    assert older.is_file()


def test_prepared_history_resumes_interrupted_unlink(tmp_path, monkeypatch):
    cold, stage, older, receipt, kwargs, _refs, _retention, current = _prepared_fixture(tmp_path, monkeypatch)
    original = packed._fsync_directory
    failed = False
    def interrupted(path):
        nonlocal failed
        if path == older.parent and not failed:
            failed = True
            raise OSError('interrupted pre-install history')
        return original(path)
    monkeypatch.setattr(packed, '_fsync_directory', interrupted)
    with pytest.raises(OSError, match='interrupted pre-install history'):
        superseded_repack_history.retire(cold, stage, receipt, **kwargs, apply=True, prepared_only=True)
    monkeypatch.setattr(packed, '_fsync_directory', original)
    result = superseded_repack_history.retire(cold, stage, receipt, **kwargs, apply=True, prepared_only=True)
    assert result['state'] == 'completed' and not older.exists()
    assert packed.resolve_latest_packed(cold, 'fixture').manifest_sha256 == current.manifest_sha256
