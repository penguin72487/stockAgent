import json
import os
import subprocess
from pathlib import Path

import pytest

from stockagent.data_sync import packed_snapshots as packed, repack_retirement as retirement
from stockagent.data_sync import staged_head_capacity_relief as relief, stream_repack
from stockagent.data_sync import temporary_e_authority as temporary
from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync.packed_backup import signature
from scripts import run_prepared_stream_install_queue as queue


def test_relief_scheduler_prioritizes_possible_capacity_release_without_granting_it():
    small = {'dataset': 'small', 'encoded_bytes_remaining': 3,
             'D_shortfall_bytes': 20, 'D_old_object_bytes_upper_bound': 5}
    unknown = {'dataset': 'unknown', 'encoded_bytes_remaining': 2, 'D_shortfall_bytes': 20}
    large = {'dataset': 'large', 'encoded_bytes_remaining': 40,
             'D_shortfall_bytes': 60, 'D_old_object_bytes_upper_bound': 180}
    assert sorted([small, unknown, large], key=queue.relief_priority) == [large, unknown, small]
    ready_small = {**small, 'expected_manifest_sha256': 'a'*64, 'state': 'waiting'}
    ready_large = {**large, 'expected_manifest_sha256': 'b'*64, 'state': 'waiting'}
    assert queue.choose_candidate([ready_small, ready_large], {('large', 'b'*64)}) == ready_large
    assert queue.choose_candidate([ready_small], {('large', 'b'*64)}) is None


@pytest.mark.parametrize('alias_state', ['completed', 'no_matching_aliases'])
def test_bulk_capacity_owner_captures_only_small_parts_metadata_before_any_D_unlink(
        tmp_path, monkeypatch, capsys, alias_state):
    import sys
    from types import SimpleNamespace
    from scripts import run_staged_head_capacity_relief as cli
    calls = []
    monkeypatch.setattr(sys, 'argv', ['relief', '--dataset', 'fixture', '--old-manifest-sha256', 'a'*64, '--apply'])
    monkeypatch.setattr(cli.workspace_volume, 'configuration', lambda: {'mount_point': str(tmp_path)})
    monkeypatch.setattr(cli.workspace_volume, 'check', lambda **_kw: None)
    monkeypatch.setattr(cli, '_check_d_primary_mount', lambda *_: None)
    monkeypatch.setattr(cli, 'TradingPriorityGuard', lambda: SimpleNamespace(check=lambda: None))
    monkeypatch.setattr(cli, 'metadata_path', lambda path: path)
    monkeypatch.setattr(cli, 'install_reserve_bytes', lambda *_a, **_kw: 0)
    def run_relief(*_a, **_kw):
        calls.append('relief')
        return {'contract': relief.CONTRACT, 'dataset': 'fixture',
                'state': 'insufficient_exclusive_space' if calls.count('relief') == 1 else 'completed'}
    monkeypatch.setattr(cli.relief, 'relieve', run_relief)
    monkeypatch.setattr(cli.landing, 'release', lambda *_a, **_kw: calls.append('aliases') or {'state': alias_state})
    monkeypatch.setattr(cli.carrier, 'capture', lambda *_a, **_kw: calls.append('metadata') or {'state': 'metadata_captured'})
    cli.main()
    assert calls == (['relief', 'aliases', 'metadata', 'relief'] if alias_state == 'completed'
                     else ['relief', 'aliases', 'metadata'])
    assert json.loads(capsys.readouterr().out)['landing_alias_state'] == alias_state


def setup_case(tmp_path, monkeypatch, *, private=False, publisher='penguin'):
    source, stage = tmp_path/'source', tmp_path/'E-stage'
    cold = tmp_path/'public/.local-state/private-history' if private else tmp_path/'cold'
    materialized, retention = tmp_path/'materialized', tmp_path/'retention'
    source.mkdir()
    materialized.mkdir()
    (source/'a.bin').write_bytes(b'a'*300)
    (source/'b.bin').write_bytes(b'b'*400)
    old = packed.publish_packed_snapshot(cold, 'fixture', source, node_id='private-owner' if private else publisher,
                                        loose_file_threshold_bytes=10, file_chunk_bytes=1000)
    if private:
        monkeypatch.setattr(stream_repack, 'PRIVATE_REPACK_DATASETS', frozenset({'fixture'}))
        with packed._try_publication_lock(cold.parent/'locks/publish-retention-global.lock'):
            pass
    monkeypatch.setattr(packed, '_notify_packed_publication', lambda *_a, **_kw: None)
    with pytest.raises(SnapshotError, match='capacity wait'):
        stream_repack.repack(cold, stage, 'fixture', old.manifest_sha256,
            materialized_pin_root=materialized,
            admit_install=lambda *_a: (_ for _ in ()).throw(SnapshotError('capacity wait')))
    temporary.expose(cold, stage, 'fixture', old.manifest_sha256)
    refs = [old.manifest['archive']['inventory'], *old.manifest['archive']['objects']]
    relative = old.manifest_path.relative_to(cold).as_posix()
    metadata = {relative: list(signature(old.manifest_path))}
    catalog = tmp_path/'catalog.json'
    catalog.write_text(json.dumps({'releases': [{'manifest_relative': relative,
        'required_file_keys': [x['relpath']+'@'+x['sha256'] for x in refs]}],
        'metadata_errors': [], 'excluded_manifests': []}))
    monkeypatch.setattr(retirement.old_retirement, 'catalog_index',
                        lambda *_a, **_kw: (metadata, {}, 'catalog-id'))
    monkeypatch.setattr(retirement, '_private_catalog_generation', lambda *_a: [1])
    monkeypatch.setattr(relief, '_private_catalog_generation', lambda *_a: [1])
    monkeypatch.setattr(relief, 'process_references_many', lambda *_a, **_kw: [])
    monkeypatch.setattr(retirement, 'process_references_many', lambda *_a, **_kw: [])
    prepared = stream_repack._read_prepared(stage, 'fixture', old.manifest_sha256)
    remaining = sum(x['bytes'] for x in [*prepared['packages'], prepared['inventory']])
    kwargs = {'catalog': catalog, 'observation': tmp_path/'observation', 'cache': tmp_path/'cache',
              'materialized': materialized, 'retention_state': retention,
              'install_lock': tmp_path/'install-owner.lock', 'reserve_bytes': 0,
              'free_bytes': max(0, remaining-1), 'headroom': 0}
    return source, cold, stage, old, kwargs, refs


def test_private_E_first_cutover_reconciles_exact_pre_retirement(tmp_path, monkeypatch):
    from stockagent.data_sync import private_handoff_retirement as private_retirement
    source, cold, stage, old, kwargs, refs = setup_case(tmp_path, monkeypatch, private=True)
    private_kwargs = {**kwargs, 'public_cold': cold.parent.parent}
    planned = relief.relieve_private(cold, stage, 'fixture', old.manifest_sha256, **private_kwargs)
    assert planned['state'] == 'planned' and planned['private'] is True and planned['apply'] is False
    assert all((cold/x['relpath']).exists() for x in refs)
    removed = relief.relieve_private(cold, stage, 'fixture', old.manifest_sha256, apply=True, **private_kwargs)
    assert removed['state'] == 'completed' and removed['removed']
    assert (cold/refs[0]['relpath']).exists()
    with temporary.borrow(stage, 'fixture', old.manifest_sha256) as view:
        restored = packed.fetch_packed_snapshot(stage, tmp_path/'restore-private', view)
        assert (restored/'a.bin').read_bytes() == (source/'a.bin').read_bytes()
        assert (restored/'b.bin').read_bytes() == (source/'b.bin').read_bytes()
    result = stream_repack.repack(cold, stage, 'fixture', old.manifest_sha256,
                                  node_id='private-owner', materialized_pin_root=kwargs['materialized'])
    receipt = {'contract': 'stockagent.source_local_exact_repack.v1', 'private': True,
               'state': 'new_D_head_committed_from_exact_E_source', 'dataset': 'fixture',
               'old_manifest_sha256': old.manifest_sha256, 'new_manifest_sha256': result['new_manifest_sha256'],
               'new_snapshot_id': result['snapshot_id'], 'source_mode': 'D_streamed_to_E_dataset_pack'}
    monkeypatch.setattr(private_retirement, '_public_known', lambda _: set())
    retired = private_retirement.retire(cold, stage, receipt, public_index=tmp_path/'public-index',
        materialized=kwargs['materialized'], retention_state=kwargs['retention_state'],
        process_check=lambda _paths: [], apply=True)
    assert retired['state'] == 'completed'
    assert set(retired['plan']['already_removed_by_E_first_relief']) == set(removed['removed'])
    assert packed.resolve_latest_packed(cold, 'fixture').manifest_sha256 == receipt['new_manifest_sha256']


def test_private_E_first_keeps_research_pin_and_external_hardlinks(tmp_path, monkeypatch):
    _source, cold, stage, old, kwargs, refs = setup_case(tmp_path, monkeypatch, private=True)
    private_kwargs = {**kwargs, 'public_cold': cold.parent.parent}
    pin = kwargs['materialized']/'study.pin.json'
    pin.write_text(json.dumps({'snapshot_id': old.manifest['snapshot_id']}))
    with pytest.raises(SnapshotError, match='another or missing source pin'):
        relief.relieve_private(cold, stage, 'fixture', old.manifest_sha256, apply=True, **private_kwargs)
    pin.unlink()
    for row in refs[1:]:
        os.link(cold/row['relpath'], tmp_path/('private-alias-'+row['sha256']))
    result = relief.relieve_private(cold, stage, 'fixture', old.manifest_sha256, apply=True, **private_kwargs)
    assert result['state'] == 'insufficient_exclusive_space'
    assert all((cold/x['relpath']).exists() for x in refs)


def test_private_E_first_rechecks_dependency_pin_inside_final_owner(tmp_path, monkeypatch):
    from stockagent.data_sync import private_handoff_retirement as private_retirement
    _source, cold, stage, old, kwargs, refs = setup_case(tmp_path, monkeypatch, private=True)
    observed = iter(({}, {old.manifest['snapshot_id']: old.manifest_sha256}))
    monkeypatch.setattr(private_retirement, '_source_link_protections', lambda *_: next(observed))
    with pytest.raises(SnapshotError, match='gained a fixed dependency pin'):
        relief.relieve_private(cold, stage, 'fixture', old.manifest_sha256,
                               public_cold=cold.parent.parent, apply=True, **kwargs)
    assert all((cold/x['relpath']).exists() for x in refs)


def test_private_E_first_resumes_interrupted_unlink_before_install(tmp_path, monkeypatch):
    _source, cold, stage, old, kwargs, refs = setup_case(tmp_path, monkeypatch, private=True)
    private_kwargs = {**kwargs, 'public_cold': cold.parent.parent}
    original = packed._fsync_directory
    failed = False
    def fail_once(path):
        nonlocal failed
        if path.is_relative_to(cold/'objects') and not failed:
            failed = True
            raise OSError('private interrupted after unlink')
        return original(path)
    monkeypatch.setattr(packed, '_fsync_directory', fail_once)
    with pytest.raises(OSError, match='private interrupted'):
        relief.relieve_private(cold, stage, 'fixture', old.manifest_sha256, apply=True, **private_kwargs)
    with pytest.raises(SnapshotError, match='relief is incomplete'):
        stream_repack.repack(cold, stage, 'fixture', old.manifest_sha256,
            node_id='private-owner', materialized_pin_root=kwargs['materialized'])
    monkeypatch.setattr(packed, '_fsync_directory', original)
    assert relief.relieve_private(cold, stage, 'fixture', old.manifest_sha256,
                                  apply=True, **private_kwargs)['state'] == 'completed'
    assert (cold/refs[0]['relpath']).exists()


def test_private_E_first_preserves_fixed_dependency_and_live_reader(tmp_path, monkeypatch):
    from stockagent.data_sync import private_handoff_retirement as private_retirement
    _source, cold, stage, old, kwargs, refs = setup_case(tmp_path, monkeypatch, private=True)
    private_kwargs = {**kwargs, 'public_cold': cold.parent.parent}
    monkeypatch.setattr(private_retirement, '_source_link_protections',
                        lambda *_a: {old.manifest['snapshot_id']: old.manifest_sha256})
    with pytest.raises(SnapshotError, match='fixed dependency pin'):
        relief.relieve_private(cold, stage, 'fixture', old.manifest_sha256, apply=True, **private_kwargs)
    monkeypatch.setattr(private_retirement, '_source_link_protections', lambda *_a: {})
    monkeypatch.setattr(relief, 'process_references_many', lambda *_a, **_kw: ['pid=1:fd'])
    with pytest.raises(SnapshotError, match='running process'):
        relief.relieve_private(cold, stage, 'fixture', old.manifest_sha256, apply=True, **private_kwargs)
    assert all((cold/x['relpath']).exists() for x in refs)


def test_private_E_first_does_not_bypass_public_authority_lock(tmp_path, monkeypatch):
    _source, cold, stage, old, kwargs, refs = setup_case(tmp_path, monkeypatch, private=True)
    public = cold.parent.parent
    lock = public/'.local-state/locks/publish-retention-global.lock'
    with packed._try_publication_lock(lock):
        with pytest.raises(SnapshotError, match='owner busy'):
            relief.relieve_private(cold, stage, 'fixture', old.manifest_sha256,
                                    public_cold=public, apply=True, **kwargs)
    assert all((cold/x['relpath']).exists() for x in refs)


def test_private_E_first_uses_real_observed_catalog_generation(tmp_path, monkeypatch):
    catalog_index = retirement.old_retirement.catalog_index
    catalog_generation = relief._private_catalog_generation
    _source, cold, stage, old, kwargs, refs = setup_case(tmp_path, monkeypatch, private=True)
    monkeypatch.setattr(retirement.old_retirement, 'catalog_index', catalog_index)
    monkeypatch.setattr(relief, '_private_catalog_generation', catalog_generation)
    catalog = kwargs['catalog']
    catalog.write_text(json.dumps({'metadata_errors': [], 'files': []}))
    catalog.chmod(0o600)
    generation = catalog_generation(catalog)
    assert generation is not None
    observation = {'contract': 'stockagent.backup_catalog_observation.v1',
                   'catalog_generation': [0]*len(generation), 'catalog_identity_sha256': 'a'*64}
    kwargs['observation'].write_text(json.dumps(observation))
    with pytest.raises(SnapshotError, match='cold catalog and fixed observation differ'):
        relief.relieve_private(cold, stage, 'fixture', old.manifest_sha256,
                                public_cold=cold.parent.parent, apply=True, **kwargs)
    assert all((cold/x['relpath']).exists() for x in refs)
    observation['catalog_generation'] = generation
    kwargs['observation'].write_text(json.dumps(observation))
    result = relief.relieve_private(cold, stage, 'fixture', old.manifest_sha256,
                                    public_cold=cold.parent.parent, apply=True, **kwargs)
    assert result['state'] == 'completed' and result['removed']
    assert result['plan']['catalog_identity_sha256'] == 'a'*64


def test_public_E_first_does_not_resume_private_authority_journal(tmp_path, monkeypatch):
    _source, cold, stage, old, kwargs, refs = setup_case(tmp_path, monkeypatch)
    monkeypatch.setattr(relief, 'load', lambda *_a: {'private': True})
    with pytest.raises(SnapshotError, match='journal authority differs'):
        relief.relieve(cold, stage, 'fixture', old.manifest_sha256, apply=True, **kwargs)
    assert all((cold/x['relpath']).exists() for x in refs)


@pytest.mark.parametrize('private', [False, True])
def test_E_first_rejects_catalog_generation_change_during_census(tmp_path, monkeypatch, private):
    _source, cold, stage, old, kwargs, refs = setup_case(tmp_path, monkeypatch, private=private)
    generations = iter(([1], [2]))
    monkeypatch.setattr(relief, '_private_catalog_generation', lambda _: next(generations))
    operation = relief.relieve_private if private else relief.relieve
    extra = {'public_cold': cold.parent.parent} if private else {}
    with pytest.raises(SnapshotError, match='catalog changed during census'):
        operation(cold, stage, 'fixture', old.manifest_sha256, apply=True, **kwargs, **extra)
    assert all((cold/x['relpath']).exists() for x in refs)


def test_private_E_only_queue_identity_requires_private_completion_receipt(tmp_path, monkeypatch):
    monkeypatch.setattr(stream_repack, 'PRIVATE_REPACK_DATASETS', frozenset({'fixture'}))
    sha, new = 'a'*64, 'b'*64
    stage = tmp_path/'migration/stream-source-repack-v1/fixture'/sha
    stage.mkdir(parents=True)
    journal = {'contract': relief.CONTRACT, 'dataset': 'fixture', 'old_manifest_sha256': sha,
               'private': True, 'state': 'completed', 'removed_count': 1}
    (stage/relief.JOURNAL).write_text(json.dumps(journal))
    receipts = tmp_path/'migration/source-local-exact-repack/fixture'
    receipts.mkdir(parents=True)
    receipt = {'contract': 'stockagent.source_local_exact_repack.v1',
               'state': 'new_D_head_committed_from_exact_E_source', 'dataset': 'fixture',
               'old_manifest_sha256': sha, 'new_manifest_sha256': new}
    (receipts/(new+'.json')).write_text(json.dumps(receipt))
    assert relief.E_only_pending(tmp_path) is True  # A public receipt cannot release private priority.
    receipt['private'] = True
    (receipts/(new+'.json')).write_text(json.dumps(receipt))
    assert relief.E_only_pending(tmp_path) is False


def test_E_first_cutover_and_new_D_commit_preserve_exact_originals(tmp_path, monkeypatch):
    source, cold, stage, old, kwargs, refs = setup_case(tmp_path, monkeypatch)
    planned = relief.relieve(cold, stage, 'fixture', old.manifest_sha256, **kwargs)
    assert planned['state'] == 'planned' and planned['apply'] is False
    assert all((cold/x['relpath']).exists() for x in refs)
    removed = relief.relieve(cold, stage, 'fixture', old.manifest_sha256, apply=True, **kwargs)
    assert removed['state'] == 'completed' and removed['removed']
    assert (cold/refs[0]['relpath']).exists()  # original inventory retained
    with temporary.borrow(stage, 'fixture', old.manifest_sha256) as view:
        restored = packed.fetch_packed_snapshot(stage, tmp_path/'restore', view)
        assert (restored/'a.bin').read_bytes() == (source/'a.bin').read_bytes()
        assert (restored/'b.bin').read_bytes() == (source/'b.bin').read_bytes()
    result = stream_repack.repack(cold, stage, 'fixture', old.manifest_sha256,
                                  node_id='penguin', materialized_pin_root=kwargs['materialized'])
    receipt = {'contract': 'stockagent.source_local_exact_repack.v1',
               'state': 'new_D_head_committed_from_exact_E_source', 'dataset': 'fixture',
               'old_manifest_sha256': old.manifest_sha256, 'new_manifest_sha256': result['new_manifest_sha256'],
               'new_snapshot_id': result['snapshot_id'], 'source_mode': 'D_streamed_to_E_dataset_pack'}
    result = retirement.retire(cold, stage, receipt, apply=True, **{
        k: kwargs[k] for k in ('catalog', 'observation', 'cache', 'materialized', 'retention_state')})
    assert result['state'] == 'completed'
    assert set(result['plan']['already_removed_by_E_first_relief']) == set(removed['removed'])
    assert packed.resolve_latest_packed(cold, 'fixture').manifest_sha256 == receipt['new_manifest_sha256']


def test_fixed_retired_lab203_E_first_resumes_if_topology_changes_after_unlink(tmp_path, monkeypatch):
    from scripts import run_stream_packed_repack as stream_cli

    source, cold, stage, old, kwargs, refs = setup_case(tmp_path, monkeypatch, publisher='lab203')
    prepared = stream_repack._read_prepared(stage, 'fixture', old.manifest_sha256)
    monkeypatch.setattr(stream_cli, '_RETIRED_LAB203_DATASET', 'fixture')
    monkeypatch.setattr(stream_cli, '_RETIRED_LAB203_MANIFEST', old.manifest_sha256)
    monkeypatch.setattr(stream_cli, '_RETIRED_LAB203_PREPARED', prepared['identity_sha256'])
    monkeypatch.setattr(stream_cli, '_retired_lab203_successor_ready', lambda *_a: False)
    with pytest.raises(SnapshotError, match='not admitted'):
        relief.relieve(cold, stage, 'fixture', old.manifest_sha256, apply=True, **kwargs)
    assert all((cold/x['relpath']).exists() for x in refs)
    monkeypatch.setattr(stream_cli, '_retired_lab203_successor_ready', lambda *_a: True)
    result = relief.relieve(cold, stage, 'fixture', old.manifest_sha256, apply=True, **kwargs)
    assert result['state'] == 'completed' and result['removed']
    assert old.head_path.exists() and old.manifest_path.exists()
    with temporary.borrow(stage, 'fixture', old.manifest_sha256) as view:
        restored = packed.fetch_packed_snapshot(stage, tmp_path/'restore-retired-lab', view)
        assert (restored/'a.bin').read_bytes() == (source/'a.bin').read_bytes()
        assert (restored/'b.bin').read_bytes() == (source/'b.bin').read_bytes()
    monkeypatch.setattr(stream_cli, '_retired_lab203_successor_ready', lambda *_a: False)
    def before_commit(value):
        stream_cli.require_retired_lab203_before_commit(cold, stage, 'fixture', old.manifest_sha256,
                                                         'penguin', value)
    result = stream_repack.repack(cold, stage, 'fixture', old.manifest_sha256, node_id='penguin',
        materialized_pin_root=kwargs['materialized'], before_head_commit=before_commit)
    assert old.head_path.exists() and old.manifest_path.exists()
    assert packed.resolve_latest_packed(cold, 'fixture').manifest_sha256 == result['new_manifest_sha256']


def test_fixed_retired_lab203_interrupted_unlink_recovers_after_topology_change(tmp_path, monkeypatch):
    from scripts import run_stream_packed_repack as stream_cli

    _source, cold, stage, old, kwargs, _refs = setup_case(tmp_path, monkeypatch, publisher='lab203')
    prepared = stream_repack._read_prepared(stage, 'fixture', old.manifest_sha256)
    monkeypatch.setattr(stream_cli, '_RETIRED_LAB203_DATASET', 'fixture')
    monkeypatch.setattr(stream_cli, '_RETIRED_LAB203_MANIFEST', old.manifest_sha256)
    monkeypatch.setattr(stream_cli, '_RETIRED_LAB203_PREPARED', prepared['identity_sha256'])
    monkeypatch.setattr(stream_cli, '_retired_lab203_successor_ready', lambda *_a: True)
    fsync = packed._fsync_directory
    def fail_after_unlink(path):
        if path.is_relative_to(cold/'objects'):
            raise OSError('interrupted after unlink')
        return fsync(path)
    monkeypatch.setattr(packed, '_fsync_directory', fail_after_unlink)
    with pytest.raises(OSError, match='interrupted after unlink'):
        relief.relieve(cold, stage, 'fixture', old.manifest_sha256, apply=True, **kwargs)
    interrupted = relief.load(stage, 'fixture', old.manifest_sha256, prepared)
    assert interrupted['state'] == 'removing' and interrupted['pending']
    monkeypatch.setattr(packed, '_fsync_directory', fsync)
    monkeypatch.setattr(stream_cli, '_retired_lab203_successor_ready', lambda *_a: False)
    resumed = relief.relieve(cold, stage, 'fixture', old.manifest_sha256, apply=True, **kwargs)
    assert resumed['state'] == 'completed' and resumed['removed']
    assert old.head_path.exists() and old.manifest_path.exists()


def test_interrupted_unlink_blocks_install_until_original_owner_resumes(tmp_path, monkeypatch):
    _source, cold, stage, old, kwargs, _refs = setup_case(tmp_path, monkeypatch)
    original = packed._fsync_directory
    failed = False
    def fail_after_unlink(path):
        nonlocal failed
        if path.is_relative_to(cold/'objects') and not failed:
            failed = True
            raise OSError('interrupted after unlink')
        return original(path)
    monkeypatch.setattr(packed, '_fsync_directory', fail_after_unlink)
    with pytest.raises(OSError, match='interrupted after unlink'):
        relief.relieve(cold, stage, 'fixture', old.manifest_sha256, apply=True, **kwargs)
    with pytest.raises(SnapshotError, match='relief is incomplete'):
        stream_repack.repack(cold, stage, 'fixture', old.manifest_sha256,
                             materialized_pin_root=kwargs['materialized'])
    monkeypatch.setattr(packed, '_fsync_directory', original)
    resumed = relief.relieve(cold, stage, 'fixture', old.manifest_sha256, apply=True, **kwargs)
    assert resumed['state'] == 'completed'


def test_parallel_batch_retries_one_busy_object_without_repeating_completed_unlinks(tmp_path, monkeypatch):
    _source, cold, stage, old, kwargs, refs = setup_case(tmp_path, monkeypatch)
    blocked = cold/refs[1]['relpath']
    unlink = Path.unlink
    def busy(path, *args, **options):
        if path == blocked:
            raise PermissionError('simulated Windows sharing violation')
        return unlink(path, *args, **options)
    monkeypatch.setattr(Path, 'unlink', busy)
    with pytest.raises(SnapshotError, match='temporarily unavailable'):
        relief.relieve(cold, stage, 'fixture', old.manifest_sha256, apply=True, **kwargs)
    assert blocked.exists() and any(not (cold/x['relpath']).exists() for x in refs[2:])
    monkeypatch.setattr(Path, 'unlink', unlink)
    assert relief.relieve(cold, stage, 'fixture', old.manifest_sha256, apply=True, **kwargs)['state'] == 'completed'


def test_other_research_pin_or_active_reader_blocks_current_head_removal(tmp_path, monkeypatch):
    _source, cold, stage, old, kwargs, refs = setup_case(tmp_path, monkeypatch)
    pin = kwargs['materialized']/'study.pin.json'
    pin.write_text(json.dumps({'snapshot_id': old.manifest['snapshot_id']}))
    with pytest.raises(SnapshotError, match='another or missing source pin'):
        relief.relieve(cold, stage, 'fixture', old.manifest_sha256, apply=True, **kwargs)
    pin.unlink()
    monkeypatch.setattr(relief, 'process_references_many', lambda *_a, **_kw: ['pid=1:fd'])
    with pytest.raises(SnapshotError, match='used by a running process'):
        relief.relieve(cold, stage, 'fixture', old.manifest_sha256, apply=True, **kwargs)
    assert all((cold/x['relpath']).exists() for x in refs)


def test_missing_or_changed_E_source_never_removes_D(tmp_path, monkeypatch):
    _source, cold, stage, old, kwargs, refs = setup_case(tmp_path, monkeypatch)
    prepared = stream_repack._read_prepared(stage, 'fixture', old.manifest_sha256)
    (stage/prepared['packages'][0]['name']).write_bytes(b'changed')
    with pytest.raises(SnapshotError, match='generation changed'):
        relief.relieve(cold, stage, 'fixture', old.manifest_sha256, apply=True, **kwargs)
    assert all((cold/x['relpath']).exists() for x in refs)


def test_other_publisher_head_blocks_E_first_deletion(tmp_path, monkeypatch):
    _source, cold, stage, old, kwargs, refs = setup_case(tmp_path, monkeypatch)
    head = cold/'heads/fixture/other.json'
    head.write_text((cold/'heads/fixture/penguin.json').read_text())
    with pytest.raises(SnapshotError, match='one penguin publisher'):
        relief._require_single_local_head(cold, 'fixture', old.manifest_sha256, old.manifest['snapshot_id'])
    with pytest.raises(SnapshotError):
        relief.relieve(cold, stage, 'fixture', old.manifest_sha256, apply=True, **kwargs)
    assert all((cold/x['relpath']).exists() for x in refs)


def test_shared_or_unavailable_allocation_does_not_make_head_incomplete(tmp_path, monkeypatch):
    _source, cold, stage, old, kwargs, refs = setup_case(tmp_path, monkeypatch)
    for row in refs[1:]:
        path = cold/row['relpath']
        # Removing one hardlink does not free the shared allocation.
        os.link(path, tmp_path/('alias-'+row['sha256']))
    result = relief.relieve(cold, stage, 'fixture', old.manifest_sha256, apply=True, **kwargs)
    assert result['state'] == 'insufficient_exclusive_space'
    assert all((cold/x['relpath']).exists() for x in refs)


def test_another_manifest_reference_preserves_its_shared_old_object(tmp_path, monkeypatch):
    _source, cold, stage, old, kwargs, refs = setup_case(tmp_path, monkeypatch)
    shared = refs[1]
    other = dict(old.manifest)
    other.update(dataset='other', snapshot_id='other-version')
    other['archive'] = dict(other['archive'])
    other['archive']['objects'] = [shared]
    path = cold/'manifests/other/other-version.json'
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(other))
    result = relief.relieve(cold, stage, 'fixture', old.manifest_sha256, apply=True, **kwargs)
    assert result['state'] == 'completed'
    assert (cold/shared['relpath']).exists()
    assert shared['relpath'] not in result['removed']


def test_old_manifest_is_not_retired_if_a_removed_object_gains_a_foreign_reference(tmp_path, monkeypatch):
    _source, cold, stage, old, kwargs, _refs = setup_case(tmp_path, monkeypatch)
    outcome = relief.relieve(cold, stage, 'fixture', old.manifest_sha256, apply=True, **kwargs)
    assert outcome['removed']
    foreign = dict(old.manifest)
    foreign.update(dataset='other', snapshot_id='other-version')
    path = cold/'manifests/other/other-version.json'
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(foreign))
    result = stream_repack.repack(cold, stage, 'fixture', old.manifest_sha256,
                                  node_id='penguin', materialized_pin_root=kwargs['materialized'])
    receipt = {'contract': 'stockagent.source_local_exact_repack.v1',
               'state': 'new_D_head_committed_from_exact_E_source', 'dataset': 'fixture',
               'old_manifest_sha256': old.manifest_sha256, 'new_manifest_sha256': result['new_manifest_sha256'],
               'new_snapshot_id': result['snapshot_id'], 'source_mode': 'D_streamed_to_E_dataset_pack'}
    with pytest.raises(SnapshotError, match='gained another manifest reference'):
        retirement.retire(cold, stage, receipt, apply=True, **{
            k: kwargs[k] for k in ('catalog', 'observation', 'cache', 'materialized', 'retention_state')})
    assert old.manifest_path.exists()


def test_prepared_queue_automates_public_relief_only_when_explicitly_enabled(tmp_path, monkeypatch):
    source, cold, workspace = tmp_path/'source', tmp_path/'cold', tmp_path/'workspace'
    source.mkdir()
    (source/'sample').write_bytes(b'an exact source')
    old = packed.publish_packed_snapshot(cold, 'fixture', source, node_id='penguin')
    monkeypatch.setattr(packed, '_notify_packed_publication', lambda *_a, **_kw: None)
    stage = workspace/'migration/stream-source-repack-v1/fixture'/old.manifest_sha256
    with pytest.raises(SnapshotError, match='capacity wait'):
        stream_repack.repack(cold, stage, 'fixture', old.manifest_sha256,
            admit_install=lambda *_a: (_ for _ in ()).throw(SnapshotError('capacity wait')))
    root = workspace/'migration/source-local-exact-repack'
    root.mkdir(parents=True)
    (root/'queue-status.json').write_text(json.dumps({'contract': queue.q.CONTRACT+'.status', 'entries': [
        {'dataset': 'fixture', 'expected_manifest_sha256': old.manifest_sha256, 'origin': 'cold',
         'state': 'retrying', 'E_prepared': True}]}))
    calls = []
    def runner(command, *, on_heartbeat):
        calls.append(command)
        on_heartbeat()
        return subprocess.CompletedProcess(command, 0, json.dumps({'contract': relief.CONTRACT,
            'dataset': 'fixture', 'state': 'completed'}), '')
    result, = queue.run_once(workspace, cold, free_bytes=0, runner=runner, now=lambda: 1000,
                             code_sha='test', enable_relief=True)
    assert result['state'] == 'relief_completed'
    assert len(calls) == 1 and calls[0][1].endswith('run_staged_head_capacity_relief.py')
    assert calls[0][-1] == '--apply'
    assert packed.resolve_latest_packed(cold, 'fixture').manifest_sha256 == old.manifest_sha256


def test_prepared_queue_enrolls_fixed_retired_peer_relief_when_D_is_short(tmp_path, monkeypatch):
    from scripts import run_stream_packed_repack as stream_cli

    source, cold, workspace = tmp_path/'source', tmp_path/'cold', tmp_path/'workspace'
    source.mkdir()
    (source/'sample').write_bytes(b'an exact source')
    old = packed.publish_packed_snapshot(cold, 'fixture', source, node_id='lab203')
    monkeypatch.setattr(packed, '_notify_packed_publication', lambda *_a, **_kw: None)
    stage = workspace/'migration/stream-source-repack-v1/fixture'/old.manifest_sha256
    with pytest.raises(SnapshotError, match='capacity wait'):
        stream_repack.repack(cold, stage, 'fixture', old.manifest_sha256,
            admit_install=lambda *_a: (_ for _ in ()).throw(SnapshotError('capacity wait')))
    prepared = stream_repack._read_prepared(stage, 'fixture', old.manifest_sha256)
    monkeypatch.setattr(stream_cli, '_RETIRED_LAB203_DATASET', 'fixture')
    monkeypatch.setattr(stream_cli, '_RETIRED_LAB203_MANIFEST', old.manifest_sha256)
    monkeypatch.setattr(stream_cli, '_RETIRED_LAB203_PREPARED', prepared['identity_sha256'])
    monkeypatch.setattr(stream_cli, '_retired_lab203_successor_ready', lambda *_a: True)
    root = workspace/'migration/source-local-exact-repack'
    root.mkdir(parents=True)
    (root/'queue-status.json').write_text(json.dumps({'contract': queue.q.CONTRACT+'.status', 'entries': [
        {'dataset': 'fixture', 'expected_manifest_sha256': old.manifest_sha256, 'origin': 'cold',
         'state': 'retrying', 'reason': 'repack_foreign_head_wait', 'retry_after_epoch': 1300,
         'E_prepared': True}]}))
    calls = []
    def runner(command, *, on_heartbeat):
        calls.append(command)
        on_heartbeat()
        return subprocess.CompletedProcess(command, 0, json.dumps({'contract': relief.CONTRACT,
            'dataset': 'fixture', 'state': 'completed'}), '')
    result, = queue.run_once(workspace, cold, free_bytes=0, runner=runner, now=lambda: 1000,
                             code_sha='test', enable_relief=True)
    assert result['state'] == 'relief_completed'
    assert len(calls) == 1 and calls[0][1].endswith('run_staged_head_capacity_relief.py')
    assert old.head_path.exists() and old.manifest_path.exists()


def test_E_only_priority_persists_until_exact_publication_receipt(tmp_path):
    sha, new = 'a'*64, 'b'*64
    stage = tmp_path/'migration/stream-source-repack-v1/fixture'/sha
    stage.mkdir(parents=True)
    path = stage/relief.JOURNAL
    value = {'contract': relief.CONTRACT, 'dataset': 'fixture', 'old_manifest_sha256': sha,
             'state': 'planned', 'removed_count': 0}
    path.write_text(json.dumps(value))
    assert relief.E_only_pending(tmp_path) is False
    value.update(state='removing', pending=['objects/example'])
    path.write_text(json.dumps(value))
    assert relief.E_only_pending(tmp_path) is True
    value.update(state='completed', removed_count=10)
    value.pop('pending')
    path.write_text(json.dumps(value))
    assert relief.E_only_pending(tmp_path) is True
    receipts = tmp_path/'migration/source-local-exact-repack/fixture'
    receipts.mkdir(parents=True)
    receipt = {'contract': 'stockagent.source_local_exact_repack.v1',
               'state': 'new_D_head_committed_from_exact_E_source', 'dataset': 'fixture',
               'old_manifest_sha256': sha, 'new_manifest_sha256': new}
    (receipts/(new+'.json')).write_text(json.dumps(receipt))
    assert relief.E_only_pending(tmp_path) is False
    value['dataset'] = 'wrong'
    path.write_text(json.dumps(value))
    with pytest.raises(SnapshotError, match='priority journal invalid'):
        relief.E_only_pending(tmp_path)
