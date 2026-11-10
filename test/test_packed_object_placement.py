import hashlib
import json
import os
import time
from pathlib import Path

import pytest

from stockagent.data_sync import packed_object_placement as placement, packed_snapshots as p
from stockagent.data_sync.desync_snapshots import SnapshotError


@pytest.fixture
def archive(tmp_path, monkeypatch):
    public, workspace, state, source = (tmp_path/name for name in ('cold', 'E', 'state', 'source'))
    workspace.mkdir(); source.mkdir()
    (source/'values.bin').write_bytes(bytes(range(256))*32)
    (source/'small.txt').write_text('original raw values\n')
    monkeypatch.setattr(p, '_notify_packed_publication', lambda *_a, **_k: None)
    cold = public/'.local-state/private-history'
    release = p.publish_packed_snapshot(cold, 'warm', source, node_id='fixture', loose_file_threshold_bytes=1)
    locations = placement.Locations(state, workspace, public)
    monkeypatch.setattr(placement, 'resolve_read', locations.resolve)
    monkeypatch.setattr(placement, 'WORKSPACE', workspace)
    return cold, workspace, source, release, locations


def test_move_preserves_manifest_and_all_standard_original_readers(archive, tmp_path, monkeypatch):
    cold, workspace, source, release, locations = archive
    old_manifest = release.manifest_path.read_bytes()
    result = placement.relocate(locations, cold, 'warm', release.manifest_sha256,
                               release.manifest['archive']['objects'])
    assert result['wave_files'] == len(release.manifest['archive']['objects'])
    assert release.manifest_path.read_bytes() == old_manifest
    current = p.resolve_latest_packed(cold, 'warm')
    assert current.manifest_sha256 == release.manifest_sha256
    materialized = p.fetch_packed_snapshot(cold, tmp_path/'materialized', current)
    assert (materialized/'values.bin').read_bytes() == (source/'values.bin').read_bytes()
    read = p.read_packed_files(cold, current, names=['small.txt'], maximum_bytes=1024, maximum_object_bytes=16384)
    assert read == {'small.txt': (source/'small.txt').read_bytes()}
    p.verify_packed_snapshot(cold, current, reconstruct_all=True)
    p.audit_packed_store(cold)
    for item in release.manifest['archive']['objects']:
        assert not (cold/item['relpath']).exists()
        row = locations.get('private', item['relpath'])
        assert row['state'] == 'E_authoritative'
        assert hashlib.sha256(locations.physical(row).read_bytes()).hexdigest() == item['sha256']
    again = placement.relocate(locations, cold, 'warm', release.manifest_sha256,
                              release.manifest['archive']['objects'])
    assert again['wave_files'] == 0
    assert not result['independent_readback'] and not result['NAS_accepted']
    from stockagent.data_sync import storage_progress
    (locations.state/'status.json').write_text(json.dumps({'contract':placement.CONTRACT+'.queue',
        'observed_at_epoch':time.time(), 'entries':[]}))
    original_subject = storage_progress.source_subject
    monkeypatch.setattr(storage_progress, 'source_subject',
                        lambda name: 'local-tw-minute' if name=='warm' else original_subject(name))
    total_row, = storage_progress.packed_archive_placement_tasks(locations.state)
    assert total_row['kind'] == 'packed_archive_total' and total_row['complete_evidence']
    assert total_row['completed_bytes'] == sum(item['bytes'] for item in release.manifest['archive']['objects'])
    (locations.state/'status.json').write_text(json.dumps({'contract':placement.CONTRACT+'.queue',
        'observed_at_epoch':time.time(),'state':'waiting_reader_rollout'}))
    waiting,retained_total=storage_progress.packed_archive_placement_tasks(locations.state)
    assert waiting['state']=='waiting' and retained_total['completed_bytes']==total_row['completed_bytes']


def test_durable_unlink_intent_resumes_without_another_payload_read(archive, monkeypatch):
    cold, workspace, source, release, locations = archive
    item = release.manifest['archive']['objects'][0]
    original = cold/item['relpath']
    destination = workspace/'archives/warm/fixed'/Path(item['relpath']).name
    destination.parent.mkdir(parents=True)
    destination.write_bytes(original.read_bytes())
    row = locations.register(authority='private', relative=item['relpath'], digest=item['sha256'],
        size=item['bytes'], physical=destination, dataset='warm', source_manifest=release.manifest_sha256,
        original_signature=placement.signature(original))
    def interrupt():
        raise RuntimeError('injected interruption after durable intent')
    with pytest.raises(RuntimeError, match='injected'):
        placement.finish_handoff(locations, row, before_unlink=interrupt)
    assert original.exists()
    pending = locations.get('private', item['relpath'])
    assert pending['state'] == 'D_unlink_pending'
    assert placement.finish_handoff(locations, pending)['state'] == 'E_authoritative'
    assert not original.exists()


def test_changed_E_generation_and_missing_E_never_fall_back_to_D(archive):
    cold, workspace, source, release, locations = archive
    item = release.manifest['archive']['objects'][0]
    original = cold/item['relpath']
    destination = workspace/'archives/warm/fixed/value'
    destination.parent.mkdir(parents=True)
    destination.write_bytes(original.read_bytes())
    locations.register(authority='private', relative=item['relpath'], digest=item['sha256'],
        size=item['bytes'], physical=destination, dataset='warm', source_manifest=release.manifest_sha256,
        original_signature=placement.signature(original))
    destination.write_bytes(b'x'*item['bytes'])
    with pytest.raises(SnapshotError, match='generation'):
        locations.resolve(cold, item['relpath'])
    destination.unlink()
    with pytest.raises(FileNotFoundError):
        locations.resolve(cold, item['relpath'])
    assert original.is_file()


def test_checksum_error_preserves_D_and_never_registers_E(archive, monkeypatch):
    cold, workspace, source, release, locations = archive
    item = dict(release.manifest['archive']['objects'][0])
    original = cold/item['relpath']
    original.write_bytes(b'x'*item['bytes'])
    with pytest.raises(SnapshotError, match='checksum|canonical selection'):
        placement.relocate(locations, cold, 'warm', release.manifest_sha256, [item])
    assert original.exists() and locations.get('private', item['relpath']) is None


def test_shared_or_active_D_source_is_retained(archive):
    cold, workspace, source, release, locations = archive
    item = release.manifest['archive']['objects'][0]
    original = cold/item['relpath']
    with pytest.raises(SnapshotError, match='in use'):
        placement.relocate(locations, cold, 'warm', release.manifest_sha256, [item],
                           process_check=lambda _p: ['active fixture reader'])
    assert original.exists()
    os.link(original, original.with_name('external-link'))
    with pytest.raises(SnapshotError, match='shared'):
        placement.relocate(locations, cold, 'warm', release.manifest_sha256, [item])
    assert original.exists()


def test_changed_D_after_durable_E_registration_is_retained(archive):
    cold, workspace, source, release, locations = archive
    item = release.manifest['archive']['objects'][0]
    original = cold/item['relpath']
    destination = workspace/'archives/warm/fixed/value'
    destination.parent.mkdir(parents=True)
    destination.write_bytes(original.read_bytes())
    row = locations.register(authority='private', relative=item['relpath'], digest=item['sha256'],
        size=item['bytes'], physical=destination, dataset='warm', source_manifest=release.manifest_sha256,
        original_signature=placement.signature(original))
    original.write_bytes(b'x'*item['bytes'])
    with pytest.raises(SnapshotError, match='D source changed'):
        placement.finish_handoff(locations, row)
    assert original.exists()


def test_router_does_not_modify_unenrolled_cold_paths(tmp_path):
    value = placement.resolve_read(tmp_path/'ordinary', 'objects/blobs/aa/'+'a'*64+'.blob')
    assert value == tmp_path/'ordinary/objects/blobs/aa'/('a'*64+'.blob')


def test_queue_moves_old_encoding_before_the_latest_generation(archive):
    from scripts.run_packed_object_placement import select_archive_generation
    cold,workspace,source,old,locations=archive
    (source/'later.txt').write_text('a new observation')
    latest=p.publish_packed_snapshot(cold,'warm',source,node_id='fixture')
    selected=select_archive_generation(cold,'warm',latest,{})
    assert selected.manifest_sha256==old.manifest_sha256
    enrolled={item['relpath']:True for item in old.manifest['archive']['objects']}
    assert select_archive_generation(cold,'warm',latest,enrolled).manifest_sha256==latest.manifest_sha256


def test_missing_old_objects_do_not_block_later_generation(archive):
    from scripts.run_packed_object_placement import select_archive_generation
    cold,workspace,source,old,locations=archive
    (source/'later.txt').write_text('a new observation')
    latest=p.publish_packed_snapshot(cold,'warm',source,node_id='fixture')
    for item in old.manifest['archive']['objects']:
        (cold/item['relpath']).unlink()
    selected=select_archive_generation(cold,'warm',latest,{})
    assert selected.manifest_sha256==latest.manifest_sha256


def test_original_backup_catalog_and_closed_export_follow_placed_objects(archive, tmp_path):
    from stockagent.data_sync import backup_stream, offhost_backup
    cold, workspace, source, release, locations = archive
    placement.relocate(locations, cold, 'warm', release.manifest_sha256,
                       release.manifest['archive']['objects'])
    catalog_path = tmp_path/'catalog.json'
    catalog_path.write_text(json.dumps({'datasets': [{'dataset': 'warm', 'publish': True}]}))
    catalog = backup_stream.capture_catalog(cold, catalog_path, metadata_workers=2)
    assert all(not item['missing_objects'] for item in catalog['releases'])
    objects = [item for item in catalog['files'] if item['role'] == 'packed_object']
    assert len(objects) == len(release.manifest['archive']['objects'])+1  # inventory stays on D
    result = offhost_backup.export_incremental_delivery(cold, objects, tmp_path/'delivery',
        catalog_identity=catalog['identity_sha256'], copy_workers=2, verify_workers=2,
        namespace='private-history')
    assert result['state'] == 'closed_incremental_delivery_verified'
    for item in objects:
        assert hashlib.sha256((tmp_path/'delivery/private-history'/item['relative']).read_bytes()).hexdigest() == item['sha256']


def test_original_retirement_plan_deletes_E_only_after_its_durable_intent(archive, tmp_path, monkeypatch):
    from stockagent.data_sync import repack_retirement as retirement, stream_repack
    cold, workspace, source, release, locations = archive
    placement.relocate(locations, cold, 'warm', release.manifest_sha256,
                       release.manifest['archive']['objects'])
    monkeypatch.setattr(placement, 'enrolled_locations', lambda _cold: locations)
    descriptor = release.manifest['archive']['objects'][0]
    item = retirement.plan_member(cold, 'object', descriptor['relpath'])
    physical = retirement.member_path(cold, item)
    assert physical.is_relative_to(workspace)
    plan = {'items': [item]}
    journal = {'plan': plan, 'plan_sha256': stream_repack._identity(plan),
               'removed': [], 'state': 'planned', 'dataset': 'warm'}
    retirement.journalled_unlink(cold, tmp_path/'retirement.json', journal)
    assert not physical.exists()
    assert locations.get('private', descriptor['relpath'])['state'] == 'E_retired'
    assert journal['removed'] == [descriptor['relpath']]
    # The original durable journal remains idempotent after the E unlink.
    retirement.journalled_unlink(cold, tmp_path/'retirement.json', journal)


def test_installed_generation_bridge_is_bound_to_the_original_D_signature(archive, tmp_path, monkeypatch):
    from stockagent.data_sync import stream_repack
    cold, workspace, source, release, locations = archive
    item = release.manifest['archive']['objects'][0]
    original = stream_repack._signature(cold/item['relpath'])
    placement.relocate(locations, cold, 'warm', release.manifest_sha256,
                       release.manifest['archive']['objects'])
    # Only the local operator enrollment enables the physical-generation bridge.
    policy = tmp_path/'enrolled.json'
    policy.write_text('{}')
    monkeypatch.setattr(placement, 'POLICY', policy)
    monkeypatch.setattr(placement, 'enrolled_locations', lambda _cold: locations)
    path = locations.resolve(cold, item['relpath'])
    assert stream_repack._signature_matches(path, original)
    changed = list(original); changed[1] += 1
    assert not stream_repack._signature_matches(path, changed)


def test_original_queue_copy_and_short_exclusive_handoff_use_distinct_leases(archive, tmp_path, monkeypatch):
    from scripts import run_packed_object_placement as queue
    cold, workspace, source, release, locations = archive
    policy = {'workspace_root': str(workspace), 'cold_root': str(locations.authority),
              'state_root': str(locations.state), 'warm_private_datasets': ['warm'],
              'maximum_wave_bytes': 64*1024**2, 'reserve_bytes': 1024**2}
    monkeypatch.setattr(placement, 'load_policy', lambda: policy)
    monkeypatch.setattr(placement, 'require_reader_rollout', lambda _state: {})
    monkeypatch.setattr(queue.workspace_volume, 'check', lambda: None)
    monkeypatch.setattr(queue.workspace_volume, 'admit', lambda *_a: None)
    monkeypatch.setattr(queue.workspace_volume, 'configuration', lambda: {'minimum_free_bytes': 1024**2})
    monkeypatch.setattr(queue, '_check_d_primary_mount', lambda _p: None)
    monkeypatch.setattr(queue, 'process_references_many', lambda *_a, **_kw: [])
    (workspace/'migration/provider-state-temporary-20261103/capacity-leases').mkdir(parents=True)
    original_lock = p._try_publication_lock
    def isolated_lock(path, **kwargs):
        if str(path).startswith('/var/lib/stockagent/'):
            path = tmp_path/'owners'/path.name
        return original_lock(path, **kwargs)
    monkeypatch.setattr(p, '_try_publication_lock', isolated_lock)
    original_select = queue.select_archive_generation
    observations = []
    def observed_selection(*args, **kwargs):
        observation = json.loads((locations.state/'status.json').read_bytes())
        observations.append(observation)
        assert observation['state'] == 'checking'
        assert observation['entries'][-1]['state'] == 'checking'
        assert 'progress' not in observation['entries'][-1]
        kwargs['guard']()
        return original_select(*args, **kwargs)
    monkeypatch.setattr(queue, 'select_archive_generation', observed_selection)
    from stockagent.data_sync import retirement_turn
    turn_root=workspace/'migration/source-local-exact-repack'
    turn_root.mkdir()
    with retirement_turn.reservation(turn_root):
        waiting=queue.run(apply=True,selected_dataset='warm')
        assert waiting['entries'][0]['state']=='waiting'
        assert waiting['entries'][0]['wait_kind']=='retirement_turn'
        assert all((cold/item['relpath']).exists() for item in release.manifest['archive']['objects'])
        # A waiting placement owns no publication lease and cannot starve GC.
        with original_lock(cold/'.local-state/locks/publish-retention-global.lock'):
            pass
    with original_lock(locations.authority/'.local-state/locks/publish-retention-global.lock', shared=True), \
            original_lock(cold/'.local-state/locks/publish-retention-global.lock', shared=True):
        result = queue.run(apply=True, selected_dataset='warm')
    assert result['state'] == 'wave_completed'
    assert observations
    assert result['entries'][0]['completed_bytes'] > 0
    assert all(not (cold/item['relpath']).exists() for item in release.manifest['archive']['objects'])
    assert p.resolve_latest_packed(cold, 'warm').manifest_sha256 == release.manifest_sha256
    plans = list((workspace/'archives/warm'/release.manifest_sha256).glob('*.plan.json'))
    assert len(plans) == 1
    saved = json.loads(plans[0].read_bytes())
    assert saved['source_manifest_sha256'] == release.manifest_sha256
    assert saved['files'] == len(release.manifest['archive']['objects'])
    assert queue.remaining_wave_growth(locations, cold, saved) == 0


def test_metadata_selection_is_running_without_a_copy_counter_or_old_wait_reason(archive, monkeypatch):
    from stockagent.data_sync import storage_progress
    cold, workspace, source, release, locations = archive
    locations.state.mkdir(parents=True)
    (locations.state/'status.json').write_text(json.dumps({
        'contract':placement.CONTRACT+'.queue','observed_at_epoch':time.time(),
        'state':'checking','entries':[{'dataset':'warm','state':'checking'}]}))
    monkeypatch.setattr(storage_progress, 'source_subject', lambda _name:'local-tw-minute')
    row, = storage_progress.packed_archive_placement_tasks(locations.state)
    assert row['state']=='running' and row['phase']=='archive_tier_check'
    assert row['reason'] is None
    assert row['completed_bytes'] is None and row['total_bytes'] is None
    assert row['complete_evidence'] is False


def test_large_bounded_wave_control_does_not_relax_small_operator_policy_limit(tmp_path):
    path = tmp_path/'plan.json'
    path.write_text(json.dumps({'objects': ['x'*300]*512}))
    path.chmod(0o600)
    with pytest.raises(SnapshotError, match='size'):
        placement.regular_document(path)
    assert len(placement.regular_document(path, limit=2*1024**2)['objects']) == 512
