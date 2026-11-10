"""Same-root reuse keeps dataset identities and never invents source rows."""
import dataclasses
import hashlib
import fcntl
import json
import shutil

import pytest

from stockagent.data_sync import packed_snapshots as p, private_subset_reuse as reuse, stream_repack
from stockagent.data_sync.desync_snapshots import SnapshotError


def fixture(tmp_path, monkeypatch, *, donor_extra=True, change=False, enrolled=False):
    monkeypatch.setattr(p, '_notify_packed_publication', lambda *_a, **_k: None)
    left = tmp_path/'left'/'data_same'
    right = tmp_path/'right'/'data_same'
    for root in (left, right):
        (root/'daily').mkdir(parents=True)
        (root/'daily'/'a').write_bytes(b'fixed source bytes')
        (root/'daily'/'b').write_bytes(b'another member')
        (root/'link').symlink_to('daily/a')
    if donor_extra:
        (right/'daily'/'extra').write_bytes(b'donor-only')
    if change:
        (right/'daily'/'a').write_bytes(b'different data')
    cold = tmp_path/'cold'
    opts = dict(node_id='test', pack_layout=p.SOURCE_DATASET_PACK_LAYOUT,
                pack_target_bytes=1024*1024, loose_file_threshold_bytes=1024*1024)
    original = p.publish_packed_snapshot(cold, 'local-bybit' if enrolled else 'target', left, **opts)
    donor = p.publish_packed_snapshot(cold, 'bybit' if enrolled else 'donor', right, **opts)
    stage = tmp_path/'E-stage'
    stage.mkdir(mode=0o700)
    return cold, stage, original, donor


def test_subset_inventory_covers_target_and_reuses_donor_packs(tmp_path, monkeypatch):
    cold, stage, original, donor = fixture(tmp_path, monkeypatch)
    prepared = reuse.prepare(cold, stage, original, donor)
    inventory = prepared['inventory']
    dest = cold/prepared['archive']['inventory']['relpath']
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(stage/inventory['name'], dest)
    new = {**original.manifest, 'archive': prepared['archive']}
    before = list(p._iter_inventory(cold, original.manifest))
    after = list(p._iter_inventory(cold, new))
    assert [{k:v for k,v in x.items() if k != 'storage'} for x in before] == [
        {k:v for k,v in x.items() if k != 'storage'} for x in after]
    assert {x['path'] for x in after} == {'daily', 'daily/a', 'daily/b', 'link'}
    assert any(o.get('member_selection') == 'subset' for o in new['archive']['objects'])
    p._validate_inventory(new, iter(after), retain_object_members=False)
    assert prepared['source_payload_reread'] is False
    assert prepared['destination_independent_readback'] is False
    assert prepared['NAS_accepted'] is False
    assert p.resolve_latest_packed(cold, 'target').manifest_sha256 == original.manifest_sha256
    assert p.resolve_latest_packed(cold, 'donor').manifest_sha256 == donor.manifest_sha256


def test_subset_inventory_rejects_changed_file(tmp_path, monkeypatch):
    cold, stage, original, donor = fixture(tmp_path, monkeypatch, change=True)
    with pytest.raises(SnapshotError, match='metadata or source SHA differs'):
        reuse.prepare(cold, stage, original, donor)
    assert p.resolve_latest_packed(cold, 'target').manifest_sha256 == original.manifest_sha256
    assert not (stage/'subset-prepared.json').exists()


def test_subset_inventory_rejects_missing_path(tmp_path, monkeypatch):
    cold, stage, original, donor = fixture(tmp_path, monkeypatch)
    # Both releases remain real immutable manifests; the other direction is
    # intentionally not a subset, because the donor has an additional path.
    with pytest.raises(SnapshotError, match='does not cover every target path'):
        reuse.prepare(cold, stage, donor, original)


def test_subset_inventory_rejects_different_roots(tmp_path, monkeypatch):
    cold, stage, original, donor = fixture(tmp_path, monkeypatch)
    donor.manifest['source']['snapshot_root_name'] = 'another-root'
    with pytest.raises(SnapshotError, match='same source root'):
        reuse.prepare(cold, stage, original, donor)


def test_canonical_repack_publishes_subset_and_restores_target(tmp_path, monkeypatch):
    cold, stage, original, donor = fixture(tmp_path, monkeypatch, enrolled=True)
    monkeypatch.setattr(stream_repack, 'PRIVATE_REPACK_ROOT', cold)
    monkeypatch.setattr(stream_repack, '_notify_committed_repack', lambda *_a, **_k: None)
    monkeypatch.setattr(stream_repack, 'private_repack_source_allowed', lambda *_a: True)
    proof = {'contract': reuse.CONTRACT+'.donor-proof', 'dataset': 'bybit',
             'manifest_sha256': donor.manifest_sha256, 'source_member_sha_streamed': True,
             'source_layout_confirmed': True}
    proof['identity_sha256'] = stream_repack._identity(proof)
    result = stream_repack.repack(cold, stage, 'local-bybit', original.manifest_sha256,
        node_id='test', subset_donor=('bybit', donor.manifest_sha256),
        subset_layout_check=lambda *_a: proof)
    assert result['state'] == 'published'
    fixed = p.resolve_latest_packed(cold, 'local-bybit')
    assert fixed.manifest_sha256 == result['new_manifest_sha256']
    restored = p.fetch_packed_snapshot(cold, tmp_path/'restored', fixed)
    assert (restored/'daily'/'a').read_bytes() == b'fixed source bytes'
    assert (restored/'daily'/'b').read_bytes() == b'another member'
    assert (restored/'link').is_symlink()
    assert not (restored/'daily'/'extra').exists()
    assert p.resolve_latest_packed(cold, 'bybit').manifest_sha256 == donor.manifest_sha256
    prepared = stream_repack._prepared_metadata(stage, 'local-bybit', original.manifest_sha256)
    assert prepared['source_member_sha_streamed'] is False
    assert prepared['source_inventory_sha_compared'] is True
    installed = json.loads((stage/'install.json').read_text())['installed']
    assert {x['sha256'] for x in installed} == {
        x['sha256'] for x in [*fixed.manifest['archive']['objects'], fixed.manifest['archive']['inventory']]}


def test_subset_publish_resume_refresh_and_retire_preserves_donor(tmp_path, monkeypatch):
    from scripts.run_source_local_repack_queue import discover_pending_retirements, source_layout_confirmed
    from scripts.retire_stream_repacked_release import packing_evidence
    from stockagent.data_sync import dataset_migration, private_handoff_retirement, repack_retirement, storage_progress
    cold, _stage, old, donor = fixture(tmp_path, monkeypatch, enrolled=True)
    monkeypatch.setattr(stream_repack, 'PRIVATE_REPACK_ROOT', cold)
    monkeypatch.setattr(stream_repack, '_notify_committed_repack', lambda *_a, **_k: None)
    monkeypatch.setattr(stream_repack, 'private_repack_source_allowed', lambda *_a: True)
    workspace = tmp_path/'workspace'
    stage = workspace/'migration/stream-source-repack-v1/local-bybit'/old.manifest_sha256
    proof = {'contract': reuse.CONTRACT+'.donor-proof', 'dataset': 'bybit',
             'manifest_sha256': donor.manifest_sha256, 'source_member_sha_streamed': True,
             'source_layout_confirmed': True}
    proof['identity_sha256'] = stream_repack._identity(proof)
    kwargs = dict(node_id='test', subset_donor=('bybit', donor.manifest_sha256),
                  subset_layout_check=lambda *_a: proof)
    def interrupted(_prepared):
        raise SnapshotError('interrupted before head')
    with pytest.raises(SnapshotError, match='interrupted before head'):
        stream_repack.repack(cold, stage, 'local-bybit', old.manifest_sha256,
                             before_head_commit=interrupted, **kwargs)
    assert p.resolve_latest_packed(cold, 'local-bybit').manifest_sha256 == old.manifest_sha256
    result = stream_repack.repack(cold, stage, 'local-bybit', old.manifest_sha256, **kwargs)
    latest = p.resolve_latest_packed(cold, 'local-bybit')
    assert source_layout_confirmed(cold, 'local-bybit', latest.manifest, workspace=workspace)
    assert not source_layout_confirmed(cold, 'local-bybit', latest.manifest, workspace=tmp_path/'missing')
    receipt = {'contract':'stockagent.source_local_exact_repack.v1',
        'state':'new_D_head_committed_from_exact_E_source', 'dataset':'local-bybit',
        'old_manifest_sha256':old.manifest_sha256, 'new_manifest_sha256':result['new_manifest_sha256'],
        'new_snapshot_id':result['snapshot_id'], 'files':result['files'], 'logical_bytes':result['logical_bytes'],
        'source_mode':reuse.SOURCE_MODE, 'private':True, 'packed_format':reuse.FORMAT,
        'source_inventory_sha_compared':True, 'source_member_sha_streamed':False,
        'donor_manifest_sha256':donor.manifest_sha256, 'old_release_removed':False}
    receipt_path = workspace/'migration/source-local-exact-repack/local-bybit'/f"{result['new_manifest_sha256']}.json"
    receipt_path.parent.mkdir(parents=True)
    receipt_path.write_text(json.dumps(receipt))
    discovered = discover_pending_retirements(workspace/'migration/source-local-exact-repack')
    assert len(discovered) == 1 and discovered[0]['old_manifest_sha256'] == old.manifest_sha256
    assert packing_evidence(receipt) == {'old_source_member_hashes_checked_while_packing':False,
        'source_inventory_sha_compared':True, 'source_payload_reread':False}
    previous = {'domain':'host_private', 'snapshot_id':old.manifest['snapshot_id'],
                'manifest_sha256':old.manifest_sha256}
    binding = dataset_migration._exact_repack_refresh_successor(cold, workspace,
        {'dataset':'local-bybit','publish':False}, previous)
    assert binding['source_member_sha_streamed'] is False and binding['source_inventory_sha_compared'] is True
    status = workspace/'migration/source-local-exact-repack/private-subset-status.json'
    status.write_text(json.dumps({'contract':'stockagent.private_same_root_subset_queue.v1',
        'observed_at_epoch':1794220000, 'independent_D_readback':False, 'NAS_accepted':False,
        'entries':[{'dataset':'local-bybit','state':'published','generation':old.manifest_sha256,
                    'new_manifest_sha256':result['new_manifest_sha256'],'logical_bytes':result['logical_bytes']}]}))
    card, = storage_progress.private_subset_reuse_tasks(workspace, workspace_checked=True, cold=cold)
    assert card['state'] == 'completed' and card['complete_evidence'] is True
    assert card['phase'] == 'private_subset_published'
    published_status = status.read_text()
    status.write_text(json.dumps({'contract':'stockagent.private_same_root_subset_queue.v1',
        'observed_at_epoch':1794220001, 'independent_D_readback':False, 'NAS_accepted':False,
        'entries':[{'dataset':'local-bybit','state':'waiting','reason':'original_owner_busy'}]}))
    busy_card, = storage_progress.private_subset_reuse_tasks(workspace, workspace_checked=True, cold=cold)
    assert busy_card['state'] == 'completed' and busy_card['complete_evidence'] is True
    # A busy owner plus an old/missing receipt never proves publication.
    receipt_path.unlink()
    busy_card, = storage_progress.private_subset_reuse_tasks(workspace, workspace_checked=True, cold=cold)
    assert busy_card['state'] == 'waiting' and busy_card['complete_evidence'] is False
    receipt_path.write_text(json.dumps(receipt))
    status.write_text(published_status)
    # A later head invalidates the old completion even if the old fixed
    # snapshot and its installation journal remain readable.
    resolve_current = p.resolve_latest_packed
    with monkeypatch.context() as local:
        local.setattr(p, 'resolve_latest_packed', lambda *a, **k:
            dataclasses.replace(resolve_current(*a, **k), manifest_sha256='f'*64))
        advanced, = storage_progress.private_subset_reuse_tasks(workspace, workspace_checked=True, cold=cold)
        assert advanced['state'] == 'waiting' and advanced['complete_evidence'] is False
    receipt_path.unlink()
    card, = storage_progress.private_subset_reuse_tasks(workspace, workspace_checked=True, cold=cold)
    assert card['state'] == 'blocked' and card['complete_evidence'] is False
    receipt_path.write_text(json.dumps(receipt))
    cleanup = stream_repack.cleanup_stage(cold, stage, receipt)
    assert cleanup['state'] == 'completed'
    prepared = stream_repack._prepared_metadata(stage, 'local-bybit', old.manifest_sha256)
    replacement = repack_retirement.check_replacement(cold, stage, 'local-bybit', old.manifest_sha256,
        result['new_manifest_sha256'], result['snapshot_id'], prepared)
    assert replacement.manifest_sha256 == result['new_manifest_sha256']
    index = tmp_path/'index.json'
    body = {'manifests':{}}
    body['identity_sha256'] = hashlib.sha256(json.dumps(body, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    index.write_text(json.dumps(body))
    materialized, retention = tmp_path/'materialized', tmp_path/'retention'
    materialized.mkdir(); retention.mkdir()
    outcome = private_handoff_retirement.retire(cold, stage, receipt, public_index=index,
        materialized=materialized, retention_state=retention, process_check=lambda _paths: [], apply=True)
    assert outcome['state'] == 'completed'
    assert not old.manifest_path.exists()
    assert p.resolve_latest_packed(cold, 'bybit').manifest_sha256 == donor.manifest_sha256
    fixed = p.resolve_latest_packed(cold, 'local-bybit')
    restored = p.fetch_packed_snapshot(cold, tmp_path/'post-retirement', fixed)
    assert (restored/'daily/a').read_bytes() == b'fixed source bytes'


def test_subset_monitor_reports_phase_and_private_blocker_without_false_completion(tmp_path):
    from stockagent.data_sync import storage_progress
    status = tmp_path/'migration/source-local-exact-repack/private-subset-status.json'
    status.parent.mkdir(parents=True)
    status.write_text(json.dumps({'contract':'stockagent.private_same_root_subset_queue.v1',
        'observed_at_epoch':1794220000, 'independent_D_readback':False, 'NAS_accepted':False,
        'entries':[{'dataset':'local-bybit','state':'blocked','reason':'donor_generation_unproven'},
                   {'dataset':'local-taifex-public-history','state':'running','generation':'a'*64,
                    'progress':{'stage':'comparing_fixed_inventories','items_completed':100,'items_total':500}}]}))
    blocked, running = storage_progress.private_subset_reuse_tasks(tmp_path, workspace_checked=True)
    assert blocked['reason'] == 'subset_donor_unproven' and not blocked['complete_evidence']
    assert running['phase'] == 'private_subset_comparing'
    assert (running['completed_items'], running['total_items']) == (100,500)
    status.write_text(json.dumps({'contract':'stockagent.private_same_root_subset_queue.v1',
        'observed_at_epoch':1794220001, 'independent_D_readback':False, 'NAS_accepted':False,
        'entries':[{'dataset':'local-taifex-public-history','state':'running','generation':'a'*64,
            'progress':{'stage':'streaming_D_object','objects_completed':0,'objects_total':1,
                        'encoded_bytes_streamed':40,'encoded_bytes_total':100}}]}))
    installing, = storage_progress.private_subset_reuse_tasks(tmp_path, workspace_checked=True)
    assert installing['phase'] == 'source_local_D_installing'
    assert (installing['completed_bytes'], installing['total_bytes']) == (40,100)
    assert storage_progress.private_subset_reuse_tasks(tmp_path, workspace_checked=False) == []


def test_busy_install_probe_only_defers_and_never_owns_the_real_write(tmp_path):
    from scripts.run_private_subset_reuse_queue import _install_owner_busy
    lock = tmp_path/'migration/cold-install-write-owner.lock'
    lock.parent.mkdir()
    assert _install_owner_busy(tmp_path) is False
    with lock.open('w') as owner:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert _install_owner_busy(tmp_path) is True
    assert _install_owner_busy(tmp_path) is False


def test_committed_public_hardlink_proves_exact_donor_generation(tmp_path, monkeypatch):
    from stockagent.data_sync import semantic_link_promotion as promotion
    from test_semantic_link_promotion import build_latest_candidate
    cold, private, _old, donor, options = build_latest_candidate(tmp_path, monkeypatch, dataset='bybit')
    workspace = tmp_path/'workspace'
    plan = options['plan']
    journal = workspace/'migration/handoff-index-archive-v1/bybit'/f"latest-promotion-{plan['identity_sha256']}.json"
    journal.parent.mkdir(parents=True)
    options['journal_path'] = journal
    result = promotion.promote(cold, private, **options)
    assert result['state'] == 'committed'
    refs = [*donor.manifest['archive']['objects'], donor.manifest['archive']['inventory']]
    installed = {row['sha256']:row for row in options['install']['installed']}
    proof = reuse._committed_link_proof(private, workspace, donor, refs, installed)
    assert proof['comparison_sha256'] == plan['identity_sha256']
    data = json.loads(journal.read_text())
    data['linked'][0]['source_signature'][1] += 1
    journal.write_text(json.dumps(data))
    with pytest.raises(SnapshotError, match='object generation changed'):
        reuse._committed_link_proof(private, workspace, donor, refs, installed)


def test_canonical_repack_rejects_changed_donor_and_preserves_target(tmp_path, monkeypatch):
    cold, stage, original, donor = fixture(tmp_path, monkeypatch, enrolled=True)
    monkeypatch.setattr(stream_repack, 'PRIVATE_REPACK_ROOT', cold)
    monkeypatch.setattr(stream_repack, 'private_repack_source_allowed', lambda *_a: True)
    with pytest.raises(SnapshotError, match='donor changed'):
        stream_repack.repack(cold, stage, 'local-bybit', original.manifest_sha256,
            node_id='test', subset_donor=('bybit', 'a'*64),
            subset_layout_check=lambda *_a: True)
    assert p.resolve_latest_packed(cold, 'local-bybit').manifest_sha256 == original.manifest_sha256


def test_incompatible_subset_is_cached_and_releases_the_original_repack_owner(tmp_path, monkeypatch):
    from scripts.run_private_subset_reuse_queue import _select
    cold, _stage, original, donor = fixture(tmp_path, monkeypatch, enrolled=True, change=True)
    monkeypatch.setattr(stream_repack, 'PRIVATE_REPACK_ROOT', cold)
    monkeypatch.setattr(stream_repack, 'private_repack_source_allowed', lambda *_a: True)
    monkeypatch.setattr(stream_repack, '_notify_committed_repack', lambda *_a, **_k: None)
    workspace, pins = tmp_path/'workspace', tmp_path/'pins'
    pins.mkdir()
    stage = workspace/'migration/stream-source-repack-v1/local-bybit'/original.manifest_sha256
    proof = {'contract':reuse.CONTRACT+'.donor-proof', 'dataset':'bybit',
             'manifest_sha256':donor.manifest_sha256, 'source_member_sha_streamed':True,
             'source_layout_confirmed':True}
    proof['identity_sha256'] = stream_repack._identity(proof)
    with pytest.raises(SnapshotError, match='metadata or source SHA differs'):
        stream_repack.repack(cold, stage, 'local-bybit', original.manifest_sha256,
            node_id='test', materialized_pin_root=pins, subset_donor=('bybit', donor.manifest_sha256),
            subset_layout_check=lambda *_a: proof)
    assert not stage.exists()
    assert reuse.incompatible(workspace, 'local-bybit', original.manifest_sha256, donor.manifest_sha256)
    assert not reuse.incompatible(workspace, 'local-bybit', original.manifest_sha256, 'a'*64)
    progress = {'datasets':{'local-bybit':{'state':'verified','domain':'host_private',
        'snapshot_id':original.manifest['snapshot_id'],'manifest_sha256':original.manifest_sha256}}}
    selected, reason = _select(cold, workspace, 'local-bybit', 'bybit', progress)
    assert selected is None and reason == 'donor_not_covering_target'
    assert not list((pins/'.stream-repack-pins/local-bybit').glob('*.pin.json'))
    result = stream_repack.repack(cold, stage, 'local-bybit', original.manifest_sha256, node_id='test')
    assert result['state'] == 'published'


@pytest.mark.parametrize('installed', [False, True])
def test_changed_donor_preserves_old_e_stage_and_automatically_reenrolls(tmp_path, monkeypatch, installed):
    cold, _stage, original, donor = fixture(tmp_path, monkeypatch, enrolled=True)
    monkeypatch.setattr(stream_repack, 'PRIVATE_REPACK_ROOT', cold)
    monkeypatch.setattr(stream_repack, 'private_repack_source_allowed', lambda *_a: True)
    monkeypatch.setattr(stream_repack, '_notify_committed_repack', lambda *_a, **_k: None)
    workspace, pins = tmp_path/'workspace', tmp_path/'pins'
    pins.mkdir()
    stage = workspace/'migration/stream-source-repack-v1/local-bybit'/original.manifest_sha256
    def proof(fixed):
        value = {'contract':reuse.CONTRACT+'.donor-proof', 'dataset':'bybit',
                 'manifest_sha256':fixed.manifest_sha256, 'source_member_sha_streamed':True,
                 'source_layout_confirmed':True}
        value['identity_sha256'] = stream_repack._identity(value)
        return value
    def defer(*_args):
        raise SnapshotError('writer busy')
    with pytest.raises(SnapshotError, match='writer busy'):
        stream_repack.repack(cold, stage, 'local-bybit', original.manifest_sha256,
            node_id='test', materialized_pin_root=pins, subset_donor=('bybit', donor.manifest_sha256),
            subset_layout_check=lambda *_a: proof(donor), admit_install=defer)
    assert (stage/'prepared.json').is_file()
    (tmp_path/'right/data_same/daily/extra2').write_bytes(b'new donor member')
    next_donor = p.publish_packed_snapshot(cold, 'bybit', tmp_path/'right/data_same', node_id='test',
        pack_layout=p.SOURCE_DATASET_PACK_LAYOUT, pack_target_bytes=1024*1024,
        loose_file_threshold_bytes=1024*1024)
    if installed:
        (stage/'install.json').write_text('{}')
        with pytest.raises(SnapshotError, match='installed or unknown content'):
            stream_repack.repack(cold, stage, 'local-bybit', original.manifest_sha256,
                node_id='test', materialized_pin_root=pins, subset_donor=('bybit', next_donor.manifest_sha256),
                subset_layout_check=lambda *_a: proof(next_donor))
        assert (stage/'prepared.json').is_file() and (stage/'install.json').is_file()
        assert len(list((pins/'.stream-repack-pins/bybit').glob('*.pin.json'))) == 1
        assert p.resolve_latest_packed(cold, 'local-bybit').manifest_sha256 == original.manifest_sha256
        return
    result = stream_repack.repack(cold, stage, 'local-bybit', original.manifest_sha256,
        node_id='test', materialized_pin_root=pins, subset_donor=('bybit', next_donor.manifest_sha256),
        subset_layout_check=lambda *_a: proof(next_donor))
    assert result['state'] == 'published'
    quarantine = workspace/'migration/private-subset-superseded-v1/local-bybit'/original.manifest_sha256
    preserved = list(quarantine.iterdir())
    assert len(preserved) == 1 and (preserved[0]/'prepared.json').is_file()
    assert json.loads((preserved[0]/'superseded.json').read_text())['D_install_started'] is False
    assert not list((pins/'.stream-repack-pins/bybit').glob('*.pin.json'))


def test_started_install_resumes_its_pinned_donor_when_donor_head_changes(tmp_path, monkeypatch):
    from scripts.run_private_subset_reuse_queue import _select
    cold, _stage, original, donor = fixture(tmp_path, monkeypatch, enrolled=True)
    monkeypatch.setattr(stream_repack, 'PRIVATE_REPACK_ROOT', cold)
    monkeypatch.setattr(stream_repack, 'private_repack_source_allowed', lambda *_a: True)
    monkeypatch.setattr(stream_repack, '_notify_committed_repack', lambda *_a, **_k: None)
    workspace, pins = tmp_path/'workspace', tmp_path/'pins'
    pins.mkdir()
    stage = workspace/'migration/stream-source-repack-v1/local-bybit'/original.manifest_sha256
    proof = {'contract':reuse.CONTRACT+'.donor-proof', 'dataset':'bybit',
             'manifest_sha256':donor.manifest_sha256, 'source_member_sha_streamed':True,
             'source_layout_confirmed':True}
    proof['identity_sha256'] = stream_repack._identity(proof)
    kwargs = dict(node_id='test', materialized_pin_root=pins, subset_donor=('bybit', donor.manifest_sha256),
                  subset_layout_check=lambda *_a: proof)
    with pytest.raises(SnapshotError, match='before commit'):
        stream_repack.repack(cold, stage, 'local-bybit', original.manifest_sha256, **kwargs,
            before_head_commit=lambda _p: (_ for _ in ()).throw(SnapshotError('before commit')))
    assert (stage/'install.json').is_file()
    assert len(list((pins/'.stream-repack-pins/bybit').glob('*.pin.json'))) == 1
    (tmp_path/'right/data_same/daily/extra2').write_bytes(b'changed donor head')
    next_donor = p.publish_packed_snapshot(cold, 'bybit', tmp_path/'right/data_same', node_id='test',
        pack_layout=p.SOURCE_DATASET_PACK_LAYOUT, pack_target_bytes=1024*1024,
        loose_file_threshold_bytes=1024*1024)
    progress = {'datasets':{'local-bybit':{'state':'verified','domain':'host_private',
        'snapshot_id':original.manifest['snapshot_id'],'manifest_sha256':original.manifest_sha256}}}
    selected, reason = _select(cold, workspace, 'local-bybit', 'bybit', progress)
    assert reason is None and selected['donor_sha'] == donor.manifest_sha256
    result = stream_repack.repack(cold, stage, 'local-bybit', original.manifest_sha256, **kwargs)
    assert result['state'] == 'published'
    assert not list((pins/'.stream-repack-pins/bybit').glob('*.pin.json'))
    assert p.resolve_latest_packed(cold, 'bybit').manifest_sha256 == next_donor.manifest_sha256


def test_committed_subset_after_interruption_clears_both_owned_pins(tmp_path, monkeypatch):
    cold, _stage, original, donor = fixture(tmp_path, monkeypatch, enrolled=True)
    monkeypatch.setattr(stream_repack, 'PRIVATE_REPACK_ROOT', cold)
    monkeypatch.setattr(stream_repack, 'private_repack_source_allowed', lambda *_a: True)
    workspace, pins = tmp_path/'workspace', tmp_path/'pins'
    pins.mkdir()
    stage = workspace/'migration/stream-source-repack-v1/local-bybit'/original.manifest_sha256
    proof = {'contract':reuse.CONTRACT+'.donor-proof', 'dataset':'bybit',
             'manifest_sha256':donor.manifest_sha256, 'source_member_sha_streamed':True,
             'source_layout_confirmed':True}
    proof['identity_sha256'] = stream_repack._identity(proof)
    kwargs = dict(node_id='test', materialized_pin_root=pins, subset_donor=('bybit', donor.manifest_sha256),
                  subset_layout_check=lambda *_a: proof)
    monkeypatch.setattr(stream_repack, '_notify_committed_repack',
                        lambda *_a, **_k: (_ for _ in ()).throw(SnapshotError('after head commit')))
    with pytest.raises(SnapshotError, match='after head commit'):
        stream_repack.repack(cold, stage, 'local-bybit', original.manifest_sha256, **kwargs)
    assert len(list((pins/'.stream-repack-pins/bybit').glob('*.pin.json'))) == 1
    assert len(list((pins/'.stream-repack-pins/local-bybit').glob('*.pin.json'))) == 1
    monkeypatch.setattr(stream_repack, '_notify_committed_repack', lambda *_a, **_k: None)
    result = stream_repack.repack(cold, stage, 'local-bybit', original.manifest_sha256, **kwargs)
    assert result['recovered_after_head_commit'] is True
    assert not list((pins/'.stream-repack-pins/bybit').glob('*.pin.json'))
    assert not list((pins/'.stream-repack-pins/local-bybit').glob('*.pin.json'))
