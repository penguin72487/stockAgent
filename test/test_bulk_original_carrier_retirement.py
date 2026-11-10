import fcntl
import hashlib
import json
import os
from pathlib import Path

import pytest

from stockagent.data_sync import bulk_archive, bulk_original_carrier_retirement as carrier
from stockagent.data_sync import packed_snapshots as packed, stream_repack, temporary_e_authority
from stockagent.data_sync.desync_snapshots import SnapshotError


@pytest.fixture(params=['markets.tar.zst.partial', 'markets.tar.zst'])
def case(tmp_path, monkeypatch, request):
    cold, workspace, incoming = tmp_path/'cold', tmp_path/'workspace', tmp_path/'incoming'
    batch = incoming/'20261004T102032-b371004176fb'
    source = batch/'publish-markets-parts-v2'
    source.mkdir(parents=True)
    original = batch/request.param
    alias = batch/'publish-markets'/'payload.tar.zst'
    alias.parent.mkdir()
    content = (b'fixed original compressed bytes'*8) + b'tail'
    original.write_bytes(content)
    os.link(original, alias)
    digest = hashlib.sha256(content).hexdigest()
    parts = []
    for i, start in enumerate(range(0, len(content), 70)):
        data = content[start:start+70]
        name = f'payload.part-{i:05d}.zst'
        (source/name).write_bytes(data)
        parts.append({'path': name, 'bytes': len(data), 'sha256': hashlib.sha256(data).hexdigest()})
    plan = {'schema_version': 1, 'preservation_contract': bulk_archive.PARTS_CONTRACT,
            'compressed_sha256': digest, 'compressed_bytes': len(content), 'part_bytes': 70, 'parts': parts}
    raw = json.dumps(plan, sort_keys=True).encode()
    (source/'compressed_parts.json').write_bytes(raw)
    (source/'member_inventory.json').write_bytes(b'fixed legacy member index')
    name = 'legacy-vast-bulk-markets-parts-v2-fixture'
    monkeypatch.setattr(packed, '_notify_packed_publication', lambda *_a, **_kw: None)
    old = packed.publish_packed_snapshot(cold, name, source, node_id='penguin', metadata={
        'preservation_contract': bulk_archive.PARTS_CONTRACT, 'source_scope': 'markets',
        'compressed_sha256': digest, 'compressed_bytes': str(len(content)),
        'compressed_parts_sha256': hashlib.sha256(raw).hexdigest()})
    receipt = {'scope': 'markets', 'compressed_sha256': digest, 'received_bytes': len(content),
               'payload': str(original)}
    publication = {'dataset': name, 'manifest_sha256': old.manifest_sha256,
        'snapshot_id': old.manifest['snapshot_id'], 'compressed_sha256': digest,
        'source_scope': 'markets', 'compressed_parts_sha256': hashlib.sha256(raw).hexdigest(),
        'scope_completeness': 'must-reconcile', 'preservation_contract': bulk_archive.PARTS_CONTRACT}
    (batch/'markets.receipt.json').write_text(json.dumps(receipt))
    (batch/'markets.cold-publication.json').write_text(json.dumps(publication))
    stage = workspace/'migration/stream-source-repack-v1'/name/old.manifest_sha256
    materialized = tmp_path/'materialized'
    materialized.mkdir()
    with pytest.raises(SnapshotError, match='capacity wait'):
        stream_repack.repack(cold, stage, name, old.manifest_sha256, materialized_pin_root=materialized,
            admit_install=lambda *_: (_ for _ in ()).throw(SnapshotError('capacity wait')))
    temporary_e_authority.expose(cold, stage, name, old.manifest_sha256)
    assert carrier.capture(stage, name, old.manifest_sha256)['state'] == 'metadata_captured'
    new = stream_repack.repack(cold, stage, name, old.manifest_sha256,
                               node_id='penguin', materialized_pin_root=materialized)
    receipts = workspace/'migration/source-local-exact-repack'/name
    receipts.mkdir(parents=True)
    new_receipt = {'contract': 'stockagent.source_local_exact_repack.v1',
        'state': 'new_D_head_committed_from_exact_E_source', 'source_mode': 'D_streamed_to_E_dataset_pack',
        'private': False, 'dataset': name, 'old_manifest_sha256': old.manifest_sha256,
        'new_manifest_sha256': new['new_manifest_sha256'], 'new_snapshot_id': new['snapshot_id']}
    (receipts/(new['new_manifest_sha256']+'.json')).write_text(json.dumps(new_receipt))
    options = dict(workspace=workspace, cold=cold, incoming=incoming,
        coordinator_lock=tmp_path/'coordinator.lock', ingress_lock=tmp_path/'ingress.lock',
        install_lock=tmp_path/'install.lock', process_check=lambda *_a, **_kw: [])
    return dict(batch=batch, original=original, alias=alias, stage=stage, receipt=receipt,
                content=content, options=options, new_receipt=new_receipt)


def perform(case, *, apply=True, **overrides):
    options = {**case['options'], **overrides}
    lock = options['coordinator_lock']
    with lock.open('a+b') as owner:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return carrier.retire(case['batch'], case['receipt'], coordinator_fd=owner.fileno(), apply=apply, **options)


def test_exact_committed_parts_release_both_old_names_once(case):
    planned = perform(case, apply=False)
    assert planned['state'] == 'planned' and case['original'].exists() and case['alias'].exists()
    result = perform(case)
    assert result['state'] == 'completed' and result['cursor'] == 2
    assert result['estimated_allocated_bytes_reclaimed'] > 0
    assert result['independent_D_readback'] is False and result['NAS_accepted'] is False
    assert not case['original'].exists() and not case['alias'].exists()
    assert perform(case)['state'] == 'completed'
    assert carrier.completed_reclamation(case['options']['workspace'], case['batch'], 'markets') == result['estimated_allocated_bytes_reclaimed']


def test_crash_after_last_unlink_resumes_durable_intent(case, monkeypatch):
    original_unlink = Path.unlink
    failed = False
    def interrupted(path, *args, **kwargs):
        nonlocal failed
        if path == case['alias'] and not failed:
            failed = True
            original_unlink(path, *args, **kwargs)
            raise OSError('after final unlink')
        return original_unlink(path, *args, **kwargs)
    monkeypatch.setattr(Path, 'unlink', interrupted)
    with pytest.raises(OSError, match='after final unlink'):
        perform(case)
    state = json.loads((case['stage']/carrier.JOURNAL).read_bytes())
    assert state['cursor'] == 1 and state['pending'] == 1
    monkeypatch.setattr(Path, 'unlink', original_unlink)
    assert perform(case)['state'] == 'completed'


def test_interrupted_carrier_retirement_survives_verified_device_renumber(case, monkeypatch):
    original_unlink = Path.unlink
    failed = False
    def interrupted(path, *args, **kwargs):
        nonlocal failed
        if path == case['original'] and not failed:
            failed = True
            original_unlink(path, *args, **kwargs)
            raise OSError('after first unlink')
        return original_unlink(path, *args, **kwargs)
    monkeypatch.setattr(Path, 'unlink', interrupted)
    with pytest.raises(OSError, match='after first unlink'):
        perform(case)
    monkeypatch.setattr(Path, 'unlink', original_unlink)
    old_signature = stream_repack._signature
    monkeypatch.setattr(stream_repack, '_signature', lambda path: [old_signature(path)[0]+1, *old_signature(path)[1:]])
    monkeypatch.setattr(stream_repack, '_trusted_device_renumbering', lambda *_a: True)
    assert perform(case)['state'] == 'completed'


def test_changed_original_or_active_reader_prevents_deletion(case):
    with pytest.raises(SnapshotError, match='running process'):
        perform(case, process_check=lambda *_a, **_kw: ['pid=42:fd'])
    assert case['original'].exists() and case['alias'].exists()
    os.utime(case['original'], None)
    with pytest.raises(SnapshotError, match='generation or link count changed'):
        perform(case)
    assert case['original'].exists() and case['alias'].exists()


def test_missing_commit_or_changed_D_object_prevents_deletion(case):
    new = case['new_receipt']
    receipt = case['options']['workspace']/'migration/source-local-exact-repack'/new['dataset']/(new['new_manifest_sha256']+'.json')
    saved = receipt.read_bytes()
    receipt.unlink()
    assert perform(case)['state'] == 'waiting_D_commit'
    assert case['original'].exists()
    receipt.write_bytes(saved)
    fixed = packed.resolve_packed_snapshot_id(case['options']['cold'], new['dataset'], new['new_snapshot_id'])
    obj = case['options']['cold']/fixed.manifest['archive']['objects'][0]['relpath']
    obj.write_bytes(b'changed')
    with pytest.raises(SnapshotError, match='generation changed'):
        perform(case)
    assert case['original'].exists() and case['alias'].exists()


def test_original_payload_is_not_independently_read_again(case, monkeypatch):
    original_open = Path.open
    def check_open(path, *args, **kwargs):
        if path in (case['original'], case['alias']):
            pytest.fail('unexpected original carrier readback')
        return original_open(path, *args, **kwargs)
    monkeypatch.setattr(Path, 'open', check_open)
    assert perform(case)['state'] == 'completed'


def test_original_carrier_requires_the_original_coordinator_lock(case):
    with pytest.raises(SnapshotError, match='owner or scope'):
        carrier.retire(case['batch'], case['receipt'], coordinator_fd=-1, apply=True, **case['options'])
    assert case['original'].exists() and case['alias'].exists()


def test_missing_E_plan_reads_only_small_committed_D_member(case):
    path = case['stage']/carrier.PLAN
    saved = path.read_bytes()
    path.unlink()
    planned = perform(case, apply=False)
    assert planned['state'] == 'planned' and not path.exists()
    assert case['original'].exists() and case['alias'].exists()
    assert perform(case)['state'] == 'completed'
    assert path.read_bytes() == saved


def test_exclusive_single_name_carrier_is_reclaimed_after_exact_repack(case):
    case['alias'].unlink()
    case['alias'].parent.rmdir()
    publication = case['batch']/'markets.cold-publication.json'
    original_ctime = case['original'].stat().st_ctime_ns
    os.utime(publication, ns=(original_ctime+1_000_000_000, original_ctime+1_000_000_000))
    planned = perform(case, apply=False)
    assert planned['state'] == 'planned' and len(planned['plan']['names']) == 1
    result = perform(case)
    assert result['state'] == 'completed' and result['cursor'] == 1
    assert result['estimated_allocated_bytes_reclaimed'] > 0
    assert not case['original'].exists()
    assert perform(case)['state'] == 'completed'


def test_unplanned_extra_carrier_hardlink_is_never_deleted(case, tmp_path):
    os.link(case['original'], tmp_path/'untracked-link')
    with pytest.raises(SnapshotError, match='changed since'):
        perform(case)
    assert case['original'].exists() and case['alias'].exists() and (tmp_path/'untracked-link').exists()


def test_committed_native_plan_reader_selects_only_the_small_member(tmp_path, monkeypatch):
    import io
    from contextlib import contextmanager
    from stockagent.data_sync import windows_dedup_zip_io
    raw = b'bounded plan'
    digest = hashlib.sha256(raw).hexdigest()
    obj = {'sha256': 'b'*64, 'relpath': 'objects/packs/bb/'+('b'*64)+'.zip', 'bytes': 1234}
    row = {'path': 'compressed_parts.json', 'size': len(raw), 'sha256': digest,
           'storage': {'kind': 'pack', 'member': 'compressed_parts.json', 'object_sha256': obj['sha256']}}
    seen = []
    monkeypatch.setattr(packed, '_path_under', lambda *_a: tmp_path/'pack.zip')
    @contextmanager
    def reader(groups):
        seen.extend(groups)
        yield io.BytesIO(raw)
    monkeypatch.setattr(windows_dedup_zip_io, 'binary_zip_members', reader)
    assert carrier._committed_plan(Path('/srv/stockagent-packed'), {'archive': {'objects': [obj]}}, [row]) == raw
    assert seen[0]['members'] == [{'name': 'compressed_parts.json', 'size': len(raw)}]


def test_redirected_alias_parent_is_rejected_before_removing_original(case, tmp_path):
    real = case['alias'].parent
    moved = tmp_path/'moved-original-alias'
    real.rename(moved)
    real.symlink_to(moved, target_is_directory=True)
    with pytest.raises(SnapshotError, match='alias parent redirected'):
        perform(case)
    assert case['original'].exists() and (moved/'payload.tar.zst').exists()
