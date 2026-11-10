import json
import hashlib
import os
from types import SimpleNamespace

import pytest

from stockagent.data_sync import dedup_stage_handoff as handoff, dedup_stream_candidate as candidate
from stockagent.data_sync import packed_dedup_plan as plan, packed_snapshots as packed, stream_repack
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json


def setup_case(tmp_path, monkeypatch):
    source, cold, materialized = tmp_path/'source', tmp_path/'cold', tmp_path/'materialized'
    source.mkdir()
    materialized.mkdir()
    (source/'a').write_bytes(b'content')
    (source/'b').write_bytes(b'content')
    monkeypatch.setattr(packed, '_notify_packed_publication', lambda *_a, **_kw: None)
    old = packed.publish_packed_snapshot(cold, 'fixture', source, node_id='penguin',
                                         loose_file_threshold_bytes=8, file_chunk_bytes=4)
    stage = tmp_path/'candidate'/'fixture'/old.manifest_sha256
    stream = tmp_path/'stream'/'fixture'/old.manifest_sha256
    stage.parent.mkdir(parents=True)
    stream.mkdir(parents=True, mode=0o700)
    report = tmp_path/'reports'
    report.mkdir()
    index = plan.build_plan(cold, 'fixture', old.manifest_sha256, report)['index_path']
    candidate.prepare(cold, old, stage, index)
    prepared = candidate.prepare_stream_adoption(cold, stage, 'fixture', old.manifest_sha256)
    atomic_write_json(stream/'intent.json', {'contract': stream_repack.CONTRACT+'.intent',
                      'dataset': 'fixture', 'old_manifest_sha256': old.manifest_sha256})
    payload = b'incomplete old encoding'
    name = 'pack-'+hashlib.sha256(payload).hexdigest()+'.zip'
    (stream/name).write_bytes(payload)
    old_checkpoint = stream_repack._Checkpoint(stream, 'fixture', old.manifest_sha256)
    old_checkpoint.commit({'name': name, 'sha256': hashlib.sha256(payload).hexdigest(),
        'bytes': len(payload), 'file_count': 1, 'logical_bytes': 10,
        'E_signature': stream_repack._signature(stream/name)}, [SimpleNamespace(path='old', size=10)])
    old_checkpoint.close()
    (stream/('pack-'+'a'*32+'.partial')).write_bytes(b'partial')
    stream_repack._repack_pin(materialized, 'fixture', old.manifest_sha256,
                              old.manifest['snapshot_id'], stream)
    monkeypatch.setattr(handoff, 'process_references_many', lambda *_a, **_kw: [])
    return cold, stage, stream, materialized, old, prepared


def test_atomic_handoff_preserves_inodes_and_old_head_and_is_idempotent(tmp_path, monkeypatch):
    cold, stage, stream, materialized, old, prepared = setup_case(tmp_path, monkeypatch)
    candidate_ino, legacy_ino = stage.stat().st_ino, stream.stat().st_ino
    payload = stage/prepared['packages'][0]['name']
    payload_ino = payload.stat().st_ino
    result = handoff.handoff(cold, stage, stream, 'fixture', old.manifest_sha256, materialized=materialized)
    assert result['state'] == 'exchanged'
    assert not result['payload_copied'] and not result['payload_deleted']
    assert stage.stat().st_ino == legacy_ino and stream.stat().st_ino == candidate_ino
    assert (stream/prepared['packages'][0]['name']).stat().st_ino == payload_ino
    assert (stage/'checkpoint.sqlite').is_file()
    assert packed.resolve_latest_packed(cold, 'fixture').manifest_sha256 == old.manifest_sha256
    assert handoff.handoff(cold, stage, stream, 'fixture', old.manifest_sha256,
                           materialized=materialized) == result


def test_handoff_recovers_after_exchange_before_journal_completion(tmp_path, monkeypatch):
    cold, stage, stream, materialized, old, _prepared = setup_case(tmp_path, monkeypatch)
    exchange = handoff._exchange
    def interrupted(a, b):
        exchange(a, b)
        raise OSError('lost process after exchange')
    monkeypatch.setattr(handoff, '_exchange', interrupted)
    with pytest.raises(OSError, match='after exchange'):
        handoff.handoff(cold, stage, stream, 'fixture', old.manifest_sha256, materialized=materialized)
    monkeypatch.setattr(handoff, '_exchange', exchange)
    result = handoff.handoff(cold, stage, stream, 'fixture', old.manifest_sha256, materialized=materialized)
    assert result['state'] == 'exchanged'
    assert (stage/'checkpoint.sqlite').exists() and (stream/'candidate.json').exists()


def test_handoff_only_accepts_a_trusted_workspace_device_renumber(tmp_path, monkeypatch):
    cold, stage, stream, materialized, old, _prepared = setup_case(tmp_path, monkeypatch)
    handoff.handoff(cold, stage, stream, 'fixture', old.manifest_sha256, materialized=materialized)
    journal_path = stage.parent/'fixture.handoff.json'
    journal = json.loads(journal_path.read_text())
    journal['candidate_directory'][0] += 1
    journal['legacy_directory'][0] += 1
    journal['identity_sha256'] = stream_repack._identity(journal)
    journal_path.write_text(json.dumps(journal))
    with pytest.raises(SnapshotError, match='unknown'):
        handoff.handoff(cold, stage, stream, 'fixture', old.manifest_sha256, materialized=materialized)
    monkeypatch.setattr(stream_repack, '_trusted_device_renumbering', lambda *_a: True)
    assert handoff.handoff(cold, stage, stream, 'fixture', old.manifest_sha256,
                           materialized=materialized)['state'] == 'exchanged'


def test_incomplete_old_E_checkpoint_is_pruned_after_bound_handoff(tmp_path, monkeypatch):
    cold, stage, stream, materialized, old, prepared = setup_case(tmp_path, monkeypatch)
    handoff.handoff(cold, stage, stream, 'fixture', old.manifest_sha256, materialized=materialized)
    result = handoff.prune_legacy(cold, stage, stream, 'fixture', old.manifest_sha256, materialized=materialized)
    assert result['state'] == 'completed' and len(result['removed']) == 2
    assert result['reclaimed_allocated_bytes'] > 0 and not result['independent_D_readback']
    assert (stage/'checkpoint.sqlite').is_file()
    assert (stream/prepared['packages'][0]['name']).is_file()
    assert packed.resolve_latest_packed(cold, 'fixture').manifest_sha256 == old.manifest_sha256
    assert handoff.prune_legacy(cold, stage, stream, 'fixture', old.manifest_sha256,
                                materialized=materialized) == result


def test_incomplete_old_E_checkpoint_unlink_resumes_after_crash(tmp_path, monkeypatch):
    cold, stage, stream, materialized, old, _prepared = setup_case(tmp_path, monkeypatch)
    handoff.handoff(cold, stage, stream, 'fixture', old.manifest_sha256, materialized=materialized)
    fsync = packed._fsync_directory
    failed = False
    def interrupted(path):
        nonlocal failed
        if path == stage and not failed:
            failed = True
            raise OSError('crash after unlink')
        return fsync(path)
    monkeypatch.setattr(packed, '_fsync_directory', interrupted)
    with pytest.raises(OSError, match='crash after unlink'):
        handoff.prune_legacy(cold, stage, stream, 'fixture', old.manifest_sha256, materialized=materialized)
    monkeypatch.setattr(packed, '_fsync_directory', fsync)
    result = handoff.prune_legacy(cold, stage, stream, 'fixture', old.manifest_sha256, materialized=materialized)
    assert result['state'] == 'completed' and len(result['removed']) == 2


def test_incomplete_old_E_checkpoint_is_preserved_when_generation_changes(tmp_path, monkeypatch):
    cold, stage, stream, materialized, old, _prepared = setup_case(tmp_path, monkeypatch)
    handoff.handoff(cold, stage, stream, 'fixture', old.manifest_sha256, materialized=materialized)
    pack = next(stage.glob('*.zip'))
    pack.write_bytes(b'tampered')
    with pytest.raises(SnapshotError, match='generation changed'):
        handoff.prune_legacy(cold, stage, stream, 'fixture', old.manifest_sha256, materialized=materialized)
    assert pack.read_bytes() == b'tampered'


def test_v3_E_first_relief_install_and_old_D_retirement_form_one_resumable_chain(tmp_path, monkeypatch):
    from stockagent.data_sync import repack_retirement, staged_head_capacity_relief as relief
    from stockagent.data_sync import temporary_e_authority as temporary, workspace_volume, cache_capacity
    from stockagent.data_sync.packed_backup import signature
    from contextlib import nullcontext
    cold, stage, stream, materialized, old, prepared = setup_case(tmp_path, monkeypatch)
    monkeypatch.setattr(workspace_volume, 'ENROLLMENT', tmp_path/'no-e-enrollment')
    monkeypatch.setattr(cache_capacity, 'reserve_cache_write', lambda *_a, **_kw: nullcontext())
    handoff.handoff(cold, stage, stream, 'fixture', old.manifest_sha256, materialized=materialized)
    handoff.prune_legacy(cold, stage, stream, 'fixture', old.manifest_sha256, materialized=materialized)
    stream_repack.activate_content_alias_install(stream, 'fixture', old.manifest_sha256,
                                                 reader_contract='packed-v3-content-alias')
    temporary.expose(cold, stream, 'fixture', old.manifest_sha256)
    refs = [old.manifest['archive']['inventory'], *old.manifest['archive']['objects']]
    relative = old.manifest_path.relative_to(cold).as_posix()
    metadata = {relative: list(signature(old.manifest_path))}
    catalog = tmp_path/'catalog.json'
    catalog.write_text(json.dumps({'releases': [{'manifest_relative': relative,
        'required_file_keys': [x['relpath']+'@'+x['sha256'] for x in refs]}],
        'metadata_errors': [], 'excluded_manifests': []}))
    monkeypatch.setattr(repack_retirement.old_retirement, 'catalog_index',
                        lambda *_a, **_kw: (metadata, {}, 'catalog-id'))
    monkeypatch.setattr(repack_retirement, '_private_catalog_generation', lambda *_a: [1])
    monkeypatch.setattr(relief, '_private_catalog_generation', lambda *_a: [1])
    monkeypatch.setattr(relief, 'process_references_many', lambda *_a, **_kw: [])
    monkeypatch.setattr(repack_retirement, 'process_references_many', lambda *_a, **_kw: [])
    remaining = sum(item['bytes'] for item in [*prepared['packages'], prepared['inventory']])
    kwargs = {'catalog': catalog, 'observation': tmp_path/'observation', 'cache': tmp_path/'cache',
              'materialized': materialized, 'retention_state': tmp_path/'retention',
              'install_lock': tmp_path/'install-owner.lock', 'reserve_bytes': 0,
              'free_bytes': max(0, remaining-1), 'headroom': 0}
    removed = relief.relieve(cold, stream, 'fixture', old.manifest_sha256, apply=True, **kwargs)
    assert removed['state'] == 'completed' and removed['removed']
    with temporary.borrow(stream, 'fixture', old.manifest_sha256) as view:
        assert packed.read_packed_files(stream, view, names=['a', 'b'],
            maximum_bytes=100, maximum_object_bytes=1024**2) == {'a': b'content', 'b': b'content'}
    installed = stream_repack.repack(cold, stream, 'fixture', old.manifest_sha256,
                                     node_id='penguin', materialized_pin_root=materialized)
    new = packed.resolve_latest_packed(cold, 'fixture')
    assert new.manifest['schema_version'] == 3 and new.manifest_sha256 == installed['new_manifest_sha256']
    receipt = {'contract': 'stockagent.source_local_exact_repack.v1',
               'state': 'new_D_head_committed_from_exact_E_source', 'dataset': 'fixture',
               'old_manifest_sha256': old.manifest_sha256, 'new_manifest_sha256': installed['new_manifest_sha256'],
               'new_snapshot_id': installed['snapshot_id'], 'source_mode': 'D_streamed_to_E_dataset_pack'}
    result = repack_retirement.retire(cold, stream, receipt, apply=True, **{
        key: kwargs[key] for key in ('catalog', 'observation', 'cache', 'materialized', 'retention_state')})
    assert result['state'] == 'completed'
    temporary.retire(cold, stream, receipt)
    assert stream_repack.cleanup_stage(cold, stream, receipt)['state'] == 'completed'
    assert packed.read_packed_files(cold, new, names=['a', 'b'],
        maximum_bytes=100, maximum_object_bytes=1024**2) == {'a': b'content', 'b': b'content'}


@pytest.mark.parametrize('damage', ['active', 'pin', 'prepared', 'legacy_intent'])
def test_handoff_never_swaps_an_unbound_or_active_generation(tmp_path, monkeypatch, damage):
    cold, stage, stream, materialized, old, _prepared = setup_case(tmp_path, monkeypatch)
    a, b = stage.stat().st_ino, stream.stat().st_ino
    if damage == 'active':
        monkeypatch.setattr(handoff, 'process_references_many', lambda *_a, **_kw: ['busy'])
    elif damage == 'pin':
        pin = materialized/'.stream-repack-pins'/'fixture'/(old.manifest_sha256+'.pin.json')
        value = json.loads(pin.read_text())
        value['stage'] = str(stage)
        pin.write_text(json.dumps(value))
    elif damage == 'prepared':
        value = json.loads((stage/'candidate.json').read_text())
        value['logical_bytes'] += 1
        (stage/'candidate.json').write_text(json.dumps(value))
    else:
        (stream/'intent.json').write_text('{}')
    with pytest.raises(SnapshotError):
        handoff.handoff(cold, stage, stream, 'fixture', old.manifest_sha256, materialized=materialized)
    assert stage.stat().st_ino == a and stream.stat().st_ino == b
    assert packed.resolve_latest_packed(cold, 'fixture').manifest_sha256 == old.manifest_sha256


def test_legacy_prune_preserves_bound_completed_history_control_metadata(tmp_path, monkeypatch):
    cold, stage, stream, materialized, old, _prepared = setup_case(tmp_path, monkeypatch)
    status = {'contract': 'stockagent.source_local_prepared_history.v1', 'dataset': 'fixture',
              'old_manifest_sha256': old.manifest_sha256, 'state': 'source_hashed_E_packages_prepared'}
    plan = {'items': [], 'versions': []}
    key = stream_repack._identity(plan)
    receipt = {'contract': 'stockagent.private_stream_repack_history.v1', 'dataset': 'fixture',
               'new_manifest_sha256': old.manifest_sha256, 'state': 'completed', 'plan_sha256': key,
               'plan_ref': 'retirement-plan-'+key+'.json', 'removed_count': 0}
    for name, value in [('prepared-history-sweep.json', status),
                        ('private-prepared-history-retirement-'+key+'.json', receipt),
                        (receipt['plan_ref'], plan)]:
        atomic_write_json(stream/name, value)
    handoff.handoff(cold, stage, stream, 'fixture', old.manifest_sha256, materialized=materialized)
    result = handoff.prune_legacy(cold, stage, stream, 'fixture', old.manifest_sha256, materialized=materialized)
    assert result['state'] == 'completed'
    assert (stage/'prepared-history-sweep.json').exists()
    assert (stage/receipt['plan_ref']).exists()
    assert not list(stage.glob('pack-*.zip'))
