import fcntl
import json
import os
from pathlib import Path

import pytest

from stockagent.data_sync import bulk_landing_alias_retirement as landing
from stockagent.data_sync import packed_snapshots as packed, repack_retirement
from stockagent.data_sync import staged_head_capacity_relief, stream_repack, temporary_e_authority
from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync.packed_backup import signature


@pytest.fixture
def case(tmp_path, monkeypatch):
    cold, workspace, incoming = tmp_path/'cold', tmp_path/'workspace', tmp_path/'incoming'
    batch = incoming/'20261004T102032-b371004176fb'
    source = batch/'publish-markets-parts-v2'
    source.mkdir(parents=True)
    (source/'payload.part-00000.zst').write_bytes(b'a'*300)
    (source/'member_inventory.json').write_bytes(b'b'*400)
    name = 'legacy-vast-bulk-markets-parts-v2-fixture'
    digest = 'a'*64
    monkeypatch.setattr(packed, '_notify_packed_publication', lambda *_a, **_kw: None)
    old = packed.publish_packed_snapshot(cold, name, source, node_id='penguin',
        loose_file_threshold_bytes=10, file_chunk_bytes=1000, metadata={
            'preservation_contract': 'bounded_zstd_tar_preservation_v2',
            'source_scope': 'markets', 'compressed_sha256': digest})
    stage = workspace/'migration/stream-source-repack-v1'/name/old.manifest_sha256
    (tmp_path/'materialized').mkdir()
    with pytest.raises(SnapshotError, match='capacity wait'):
        stream_repack.repack(cold, stage, name, old.manifest_sha256,
            materialized_pin_root=tmp_path/'materialized',
            admit_install=lambda *_: (_ for _ in ()).throw(SnapshotError('capacity wait')))
    temporary_e_authority.expose(cold, stage, name, old.manifest_sha256)
    publication = {'dataset': name, 'manifest_sha256': old.manifest_sha256,
        'snapshot_id': old.manifest['snapshot_id'], 'compressed_sha256': digest,
        'preservation_contract': 'bounded_zstd_tar_preservation_v2'}
    receipt = {'scope': 'markets', 'compressed_sha256': digest}
    (batch/'markets.cold-publication.json').write_text(json.dumps(publication))
    (batch/'markets.receipt.json').write_text(json.dumps(receipt))
    objects = {item['sha256']: item for item in old.manifest['archive']['objects']}
    blobs = []
    for row in packed._iter_inventory(cold, old.manifest):
        alias = source/row['path']
        blob = cold/objects[row['sha256']]['relpath']
        alias.unlink()
        os.link(blob, alias)
        blobs.append(blob)
    options = dict(incoming=incoming, coordinator_lock=tmp_path/'coordinator.lock',
        ingress_lock=tmp_path/'ingress.lock', install_lock=tmp_path/'install.lock',
        process_check=lambda *_a, **_kw: [])
    return dict(cold=cold, stage=stage, dataset=name, old_sha=old.manifest_sha256,
                batch=batch, source=source, receipt=receipt, blobs=blobs, options=options,
                workspace=workspace)


def perform(case, *, apply=True, **overrides):
    options = {**case['options'], **overrides}
    return landing.release(case['cold'], case['stage'], case['dataset'], case['old_sha'],
                           apply=apply, **options)


def test_exact_alias_handoff_preserves_original_blob_and_E_generation(case, tmp_path):
    plan = perform(case, apply=False)
    assert plan['state'] == 'planned' and len(plan['plan']['items']) == 2
    assert all(path.stat().st_nlink == 2 for path in case['blobs'])
    assert not (case['stage']/landing.JOURNAL).exists()
    result = perform(case)
    assert result['state'] == 'completed' and result['cursor'] == 2
    assert result['allocated_bytes_reclaimed'] == 0
    assert all(path.exists() and path.stat().st_nlink == 1 for path in case['blobs'])
    assert list(case['source'].iterdir()) == []
    assert landing.organizer_deferred(case['batch'], case['receipt'], case['workspace'])
    with temporary_e_authority.borrow(case['stage'], case['dataset'], case['old_sha']) as view:
        restored = packed.fetch_packed_snapshot(case['stage'], tmp_path/'restore', view)
    assert (restored/'payload.part-00000.zst').read_bytes() == b'a'*300
    assert (restored/'member_inventory.json').read_bytes() == b'b'*400


def test_interrupted_unlink_resumes_only_its_durable_intent(case, monkeypatch):
    first = case['source']/'member_inventory.json'
    unlink = Path.unlink
    failed = False
    def interrupt(path, *args, **kwargs):
        nonlocal failed
        if path == first and not failed:
            failed = True
            unlink(path, *args, **kwargs)
            raise OSError('interrupted after unlink')
        return unlink(path, *args, **kwargs)
    monkeypatch.setattr(Path, 'unlink', interrupt)
    with pytest.raises(OSError, match='interrupted'):
        perform(case)
    state = json.loads((case['stage']/landing.JOURNAL).read_bytes())
    assert state['cursor'] == 0 and state['pending'] == 0
    assert landing.organizer_deferred(case['batch'], case['receipt'], case['workspace'])
    monkeypatch.setattr(Path, 'unlink', unlink)
    result = perform(case)
    assert result['state'] == 'completed' and result['cursor'] == 2
    assert all(path.exists() and path.stat().st_nlink == 1 for path in case['blobs'])


def test_active_reader_or_busy_existing_owner_cannot_remove_landing(case):
    with pytest.raises(SnapshotError, match='used by a running process'):
        perform(case, process_check=lambda *_a, **_kw: ['pid=42:fd'])
    assert all(path.stat().st_nlink == 2 for path in case['blobs'])
    lock = case['options']['coordinator_lock']
    with lock.open('a+b') as owner:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(SnapshotError, match='owner busy'):
            perform(case)
    assert all(path.stat().st_nlink == 2 for path in case['blobs'])


def test_unrelated_landing_name_or_mismatched_receipt_is_never_deleted(case):
    alias = case['source']/'member_inventory.json'
    alias.unlink()
    alias.write_bytes(b'unrelated')
    result = perform(case)
    assert result['state'] == 'completed' and result['cursor'] == 1
    assert alias.read_bytes() == b'unrelated'
    with pytest.raises(SnapshotError, match='identity differ'):
        landing.organizer_deferred(case['batch'], {**case['receipt'], 'compressed_sha256': 'b'*64},
                                    case['workspace'])


def test_changed_E_generation_prevents_any_removal(case):
    prepared = stream_repack._read_prepared(case['stage'], case['dataset'], case['old_sha'])
    (case['stage']/prepared['packages'][0]['name']).write_bytes(b'changed')
    with pytest.raises(SnapshotError, match='generation changed'):
        perform(case)
    assert all(path.stat().st_nlink == 2 for path in case['blobs'])
    assert len(list(case['source'].iterdir())) == 2


def test_extra_hardlink_stays_and_is_not_counted_as_reclaimed(case, tmp_path):
    os.link(case['blobs'][0], tmp_path/'third-name')
    result = perform(case)
    assert result['state'] == 'completed' and result['cursor'] == 1
    assert (tmp_path/'third-name').exists()
    assert case['blobs'][0].stat().st_nlink == 3


def test_alias_retirement_does_not_read_back_D_blobs_or_E_pack_payloads(case, monkeypatch):
    prepared = stream_repack._read_prepared(case['stage'], case['dataset'], case['old_sha'])
    payloads = set(case['blobs']) | {case['stage']/item['name'] for item in prepared['packages']}
    original_open = Path.open
    def check_open(path, *args, **kwargs):
        if path in payloads:
            pytest.fail('unexpected independent payload readback')
        return original_open(path, *args, **kwargs)
    monkeypatch.setattr(Path, 'open', check_open)
    assert perform(case)['state'] == 'completed'


def test_landing_alias_handoff_unblocks_existing_D_relief_and_install(case, tmp_path, monkeypatch):
    old = packed.resolve_latest_packed(case['cold'], case['dataset'], require_objects=False)
    refs = [old.manifest['archive']['inventory'], *old.manifest['archive']['objects']]
    relative = old.manifest_path.relative_to(case['cold']).as_posix()
    catalog = tmp_path/'catalog.json'
    catalog.write_text(json.dumps({'releases': [{'manifest_relative': relative,
        'required_file_keys': [x['relpath']+'@'+x['sha256'] for x in refs]}],
        'metadata_errors': [], 'excluded_manifests': []}))
    monkeypatch.setattr(repack_retirement.old_retirement, 'catalog_index',
                        lambda *_a, **_kw: ({relative: list(signature(old.manifest_path))}, {}, 'catalog-id'))
    monkeypatch.setattr(repack_retirement, '_private_catalog_generation', lambda *_a: [1])
    monkeypatch.setattr(staged_head_capacity_relief, '_private_catalog_generation', lambda *_a: [1])
    monkeypatch.setattr(staged_head_capacity_relief, 'process_references_many', lambda *_a, **_kw: [])
    kwargs = {'catalog': catalog, 'observation': tmp_path/'observation', 'cache': tmp_path/'cache',
              'materialized': tmp_path/'materialized', 'retention_state': tmp_path/'retention',
              'install_lock': tmp_path/'install.lock', 'reserve_bytes': 0,
              'free_bytes': 0, 'headroom': 0}
    before = staged_head_capacity_relief.relieve(case['cold'], case['stage'], case['dataset'],
                                                 case['old_sha'], **kwargs)
    assert before['state'] == 'insufficient_exclusive_space'
    assert perform(case)['state'] == 'completed'
    released = staged_head_capacity_relief.relieve(case['cold'], case['stage'], case['dataset'],
                                                   case['old_sha'], apply=True, **kwargs)
    assert released['state'] == 'completed' and len(released['removed']) == 2
    assert released['estimated_allocated_bytes_reclaimed'] > 0
    installed = stream_repack.repack(case['cold'], case['stage'], case['dataset'], case['old_sha'],
                                     node_id='penguin', materialized_pin_root=tmp_path/'materialized')
    assert packed.resolve_latest_packed(case['cold'], case['dataset']).manifest_sha256 == installed['new_manifest_sha256']


def test_bulk_organizer_defers_an_exact_committed_repack_without_calling_it_recovery(case, monkeypatch):
    from types import SimpleNamespace
    new_sha = 'f'*64
    root = case['workspace']/'migration/source-local-exact-repack'/case['dataset']
    root.mkdir(parents=True)
    (root/(new_sha+'.json')).write_text(json.dumps({
        'contract': 'stockagent.source_local_exact_repack.v1',
        'state': 'new_D_head_committed_from_exact_E_source',
        'source_mode': 'D_streamed_to_E_dataset_pack', 'private': False,
        'dataset': case['dataset'], 'old_manifest_sha256': case['old_sha'],
        'new_manifest_sha256': new_sha, 'new_snapshot_id': 'new-snapshot'}))
    monkeypatch.setattr(packed, 'resolve_packed_snapshot_id', lambda *_a, **_kw: SimpleNamespace(
        manifest_sha256=new_sha, manifest={'metadata': {'physical_repack_original_manifest_sha256': case['old_sha'],
            'compressed_sha256': case['receipt']['compressed_sha256'], 'source_scope': 'markets',
            'preservation_contract': 'bounded_zstd_tar_preservation_v2'}}))
    assert landing.organizer_deferred(case['batch'], case['receipt'], case['workspace'], case['cold'])
    monkeypatch.setattr(packed, 'resolve_packed_snapshot_id', lambda *_a, **_kw: SimpleNamespace(
        manifest_sha256='e'*64, manifest={'metadata': {'physical_repack_original_manifest_sha256': case['old_sha']}}))
    with pytest.raises(SnapshotError, match='does not match'):
        landing.organizer_deferred(case['batch'], case['receipt'], case['workspace'], case['cold'])
