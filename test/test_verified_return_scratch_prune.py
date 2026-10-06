import json
import os
import time

import pytest

from scripts import prune_verified_return_scratch as cleanup
from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync.legacy_artifact_archive import LegacyArchiveSpec, publish_archive, verify_cold_archive


@pytest.fixture
def saved(tmp_path, monkeypatch):
    cohort=tmp_path/'cohort'
    dataset='legacy-scratch-test'
    scratch=cohort/'staging'/dataset
    artifacts=scratch/'artifacts'
    source=artifacts/'markets/old-run'
    source.mkdir(parents=True)
    file=source/'original.bin'
    file.write_bytes(b'unique historical result, retained on D')
    old=time.time()-2*86400
    os.utime(file,(old,old))
    (source/'empty-original').mkdir()
    (scratch/'transfer-intent.json').write_text(json.dumps({'relative_root':'markets/old-run',
        'dataset':dataset,'remote_fingerprint':'a'*64}))
    spec=LegacyArchiveSpec(dataset,'markets/old-run',7,cohort/'encoded',12)
    packed=tmp_path/'packed'
    publish_archive(spec,artifacts,packed,repo_root=artifacts.parent,manual_capture=True)
    monkeypatch.setattr(cleanup,'PACKED',packed)
    monkeypatch.setattr(cleanup,'_check_d_primary_mount',lambda path:None)
    monkeypatch.setattr(cleanup,'process_references_many',lambda paths:[])
    monkeypatch.setattr(cleanup,'verify_cold_archive',lambda spec,root,**kwargs:verify_cold_archive(spec,root))
    return spec,cohort,source,packed


def test_pruning_local_copies_does_not_need_or_delete_remote_originals(saved,tmp_path):
    spec,cohort,source,packed=saved
    before=cleanup.plan(spec,cohort)
    receipts=tmp_path/'receipts';receipts.mkdir()
    result=cleanup.apply(spec,cohort,before,receipts)
    assert result['pruned'] and result['reclaimable_allocated_file_bytes']>0
    assert result['remote_sources_deleted']==result['cold_objects_deleted']==result['nas_snapshots_deleted']==0
    assert not (cohort/'staging'/spec.dataset).exists()
    assert verify_cold_archive(spec,packed)['decoded_originals_verified']


@pytest.mark.parametrize('failure',[None,'unknown-file','unknown-directory','cold-identity','local-bytes','ready','unknown-workspace'])
def test_only_registered_complete_recovery_scratch_can_be_pruned(saved,tmp_path,monkeypatch,failure):
    from stockagent.data_sync.packed_snapshots import fetch_packed_snapshot,resolve_latest_packed
    spec,cohort,source,packed=saved
    orphan=cohort/'verification-scratch/stockagent-legacy-verify-enrolled'
    resolved=resolve_latest_packed(packed,spec.dataset)
    archive=fetch_packed_snapshot(packed,orphan,resolved)
    registered={'path':str(orphan),'snapshot_id':resolved.manifest['snapshot_id'],
                'manifest_sha256':resolved.manifest_sha256}
    monkeypatch.setattr(cleanup,'REGISTERED_RECOVERY_SCRATCH',{} if failure=='unknown-workspace' else {(cohort,spec.dataset):registered})
    if failure=='unknown-file':(orphan/'extra').write_bytes(b'unknown diagnostic content')
    elif failure=='unknown-directory':(orphan/'extra-empty').mkdir()
    elif failure=='cold-identity':registered['manifest_sha256']='0'*64
    elif failure=='local-bytes':next((archive/'payload').rglob('*.raw')).write_bytes(b'corrupt')
    elif failure=='ready':next((orphan/spec.dataset).glob('*.READY.json')).write_text('{}')
    receipts=tmp_path/'receipts';receipts.mkdir()
    if failure and failure!='unknown-workspace':
        with pytest.raises((SnapshotError,ValueError)):
            cleanup.plan(spec,cohort)
        assert orphan.exists() and source.exists()
    else:
        before=cleanup.plan(spec,cohort)
        result=cleanup.apply(spec,cohort,before,receipts,owned_verified_plan=before)
        assert result['pruned'] and not source.exists()
        assert orphan.exists() is (failure=='unknown-workspace')
        assert verify_cold_archive(spec,packed)['decoded_originals_verified']


@pytest.mark.parametrize('failure',['bytes','extra-file','empty-directory','control-intent','encoded-receipt','shared-inode','active-reference','cold-object'])
def test_every_failed_gate_retains_local_copies(saved,tmp_path,monkeypatch,failure):
    spec,cohort,source,packed=saved
    if failure=='bytes':
        file=source/'original.bin';info=file.stat();file.write_bytes(b'X'*info.st_size);os.utime(file,ns=(info.st_atime_ns,info.st_mtime_ns))
    elif failure=='extra-file':
        (source/'unknown-source').write_bytes(b'do not discard')
    elif failure=='empty-directory':
        (source/'unknown-empty').mkdir()
    elif failure=='control-intent':
        (cohort/'staging'/spec.dataset/'transfer-intent.json').write_text('{}')
    elif failure=='encoded-receipt':
        next((spec.stage_root/spec.dataset/'receipts').iterdir()).write_text('{}')
    elif failure=='shared-inode':
        os.link(source/'original.bin',tmp_path/'external-name')
    elif failure=='active-reference':
        monkeypatch.setattr(cleanup,'process_references_many',lambda paths:['pid=123:fd'])
    else:
        from stockagent.data_sync.packed_snapshots import resolve_latest_packed
        resolved=resolve_latest_packed(packed,spec.dataset)
        (packed/resolved.manifest['archive']['objects'][0]['relpath']).write_bytes(b'corrupt cold object')
    with pytest.raises((SnapshotError,ValueError)):
        cleanup.plan(spec,cohort)
    assert source.is_dir() and (spec.stage_root/spec.dataset).exists()


def test_apply_rehashes_and_rejects_changed_dry_run(saved,tmp_path):
    spec,cohort,source,packed=saved
    before=cleanup.plan(spec,cohort)
    (source/'original.bin').touch()
    receipts=tmp_path/'receipts';receipts.mkdir()
    with pytest.raises(SnapshotError):
        cleanup.apply(spec,cohort,before,receipts)
    assert source.is_dir()


def test_batched_reference_gate_retains_exact_quarantine_before_unlink(saved,tmp_path,monkeypatch):
    spec,cohort,source,packed=saved
    before=cleanup.plan(spec,cohort)
    expected={r['path'] for r in before['trees'][0]['rows'] if r['kind']=='file'}
    def references(paths):
        return ['pid=123:mapped-quarantine'] if any('.verified-prune-' in p.name for p in paths) else []
    monkeypatch.setattr(cleanup,'process_references_many',references)
    receipts=tmp_path/'receipts';receipts.mkdir()
    with pytest.raises(SnapshotError,match='referenced'):
        cleanup.apply(spec,cohort,before,receipts,owned_verified_plan=before)
    journal=json.loads((receipts/(spec.dataset+'.journal.json')).read_text())
    quarantine=cleanup.Path(journal['trees'][0]['quarantine'])
    assert journal['state']=='prepared' and journal['trees'][0]['state']=='renamed'
    assert {str(p.relative_to(quarantine)) for p in quarantine.rglob('*') if p.is_file()}==expected
    assert verify_cold_archive(spec,packed)['decoded_originals_verified']


def test_recorded_local_prune_does_not_claim_remote_retirement(saved,tmp_path):
    spec,cohort,source,packed=saved
    (cohort/'progress.json').write_text(json.dumps({'state':'batch-finished-with-protected-items','items':[
        {'relative_root':spec.relative_root,'state':'cold-verified-needs-audit','error':'remote peer offline'}]}))
    before=cleanup.plan(spec,cohort)
    receipts=tmp_path/'receipts';receipts.mkdir()
    result=cleanup.apply(spec,cohort,before,receipts)
    cleanup.record_local_retirement(spec,cohort,result)
    ledger=json.loads((cohort/'progress.json').read_text())
    row=ledger['items'][0]
    assert ledger['state']=='batch-finished-with-protected-items'
    assert row['state']=='cold-verified-needs-audit' and row['error']=='remote peer offline'
    assert row['private_scratch_removed'] and row['manifest_sha256']==result['manifest_sha256']
    assert row['local_scratch_retirement']['remote_source_retirement_asserted'] is False


@pytest.mark.parametrize('persistent_failure',[False,True])
def test_completed_prune_reconciliation_retries_and_does_not_block_next_dataset(
        saved,tmp_path,monkeypatch,persistent_failure):
    import sys
    spec,cohort,source,packed=saved
    receipts=tmp_path/'receipts';receipts.mkdir()
    before=cleanup.plan(spec,cohort)
    completed=cleanup.apply(spec,cohort,before,receipts)
    (receipts/'progress.json').write_text(json.dumps({'items':[completed]}))

    next_spec=LegacyArchiveSpec('legacy-scratch-next','markets/next-run',7,cohort/'encoded',12)
    next_artifacts=cohort/'staging'/next_spec.dataset/'artifacts'
    next_source=next_artifacts/next_spec.relative_root
    next_source.mkdir(parents=True)
    original=next_source/'original.bin';original.write_bytes(b'next independent historical result')
    old=time.time()-2*86400;os.utime(original,(old,old))
    (next_artifacts.parent/'transfer-intent.json').write_text(json.dumps({
        'relative_root':next_spec.relative_root,'dataset':next_spec.dataset,'remote_fingerprint':'b'*64}))
    publish_archive(next_spec,next_artifacts,packed,repo_root=next_artifacts.parent,manual_capture=True)
    (cohort/'progress.json').write_text(json.dumps({'state':'retained','items':[
        {'relative_root':spec.relative_root,'state':'cold-verified-needs-audit'},
        {'relative_root':next_spec.relative_root,'state':'cold-verified-needs-audit'}]}))
    monkeypatch.setattr(cleanup,'STATE_ROOTS',{cohort})
    monkeypatch.setattr(cleanup,'PUBLICATION_OWNER',tmp_path/'common.lock')
    monkeypatch.setattr(cleanup,'load_legacy_specs',lambda path:{spec.dataset:spec,next_spec.dataset:next_spec})
    real_verify=cleanup.verify_cold_archive
    attempts=[]
    def flaky_verify(selected,root,**kwargs):
        if selected.dataset==spec.dataset:
            attempts.append(selected.dataset)
            if persistent_failure or len(attempts)==1:
                raise SnapshotError('WSL native startup timeout; retain previous successful fact')
        return real_verify(selected,root,**kwargs)
    monkeypatch.setattr(cleanup,'verify_cold_archive',flaky_verify)
    waits=[];monkeypatch.setattr(cleanup.time,'sleep',waits.append)
    monkeypatch.setattr(sys,'argv',['prune_verified_return_scratch.py','--state-root',str(cohort),
        '--receipt-dir',str(receipts),'--apply'])
    assert cleanup.main()==(75 if persistent_failure else 0)
    assert len(attempts)==(3 if persistent_failure else 2)
    assert waits==([15,60] if persistent_failure else [15])
    result=json.loads((receipts/'progress.json').read_text())
    prior=next(row for row in result['items'] if row['dataset']==spec.dataset)
    assert prior['pruned'] is True
    assert prior['reclaimable_allocated_file_bytes']==completed['reclaimable_allocated_file_bytes']
    assert prior['cohort_ledger_reconciled'] is (not persistent_failure)
    assert not source.exists() and not next_source.exists()
    ledger=json.loads((cohort/'progress.json').read_text())['items']
    assert bool(ledger[0].get('private_scratch_removed')) is (not persistent_failure)
    assert ledger[1]['private_scratch_removed'] is True
    assert all(row['state']=='cold-verified-needs-audit' for row in ledger)
    assert verify_cold_archive(next_spec,packed)['decoded_originals_verified']


@pytest.mark.parametrize('failure',['cold-identity','reappeared-scratch','changed-remote-source'])
def test_resume_after_local_cleanup_never_refetches_and_rejects_changed_proof(saved,tmp_path,monkeypatch,failure):
    from scripts import return_remote_legacy_archives as returns
    from types import SimpleNamespace
    from stockagent.data_sync.packed_snapshots import resolve_latest_packed
    spec,cohort,source,packed=saved
    proof=verify_cold_archive(spec,packed)
    row={'relative_root':spec.relative_root,'fingerprint':'original','private_scratch_removed':True,
         'cold_verified':True,'snapshot_id':proof['snapshot_id'],'manifest_sha256':proof['manifest_sha256']}
    if failure!='reappeared-scratch':
        import shutil
        shutil.rmtree(cohort/'staging'/spec.dataset);shutil.rmtree(spec.stage_root/spec.dataset)
    observed={'fingerprint':'changed' if failure=='changed-remote-source' else 'original','process_references':[]}
    if failure=='cold-identity':
        row['manifest_sha256']='0'*64
    monkeypatch.setattr(returns,'remote',lambda *a:observed)
    monkeypatch.setattr(returns,'_check_d_primary_mount',lambda *a:None)
    monkeypatch.setattr(returns,'verify_cold_archive',lambda *a,**kw:proof)
    monkeypatch.setattr(returns,'resolve_latest_packed',lambda *a:resolve_latest_packed(packed,spec.dataset))
    monkeypatch.setattr(returns,'_rsync_candidate',lambda *a,**kw:pytest.fail('must not refetch removed scratch'))
    monkeypatch.setattr(returns,'commit_prepared_archive',lambda *a,**kw:pytest.fail('must reject changed proof'))
    args=SimpleNamespace(state_root=cohort,sync_root=packed)
    with pytest.raises(SnapshotError):
        returns.archive_one(args,{},row,spec)


def test_verified_cold_reuse_routes_to_existing_acknowledgement_without_copy(saved,monkeypatch):
    from scripts import return_remote_legacy_archives as returns
    from types import SimpleNamespace
    from stockagent.data_sync.packed_snapshots import resolve_latest_packed
    import shutil
    spec,cohort,source,packed=saved
    proof=verify_cold_archive(spec,packed)
    row={'relative_root':spec.relative_root,'fingerprint':'original','private_scratch_removed':True,
         'cold_verified':True,'snapshot_id':proof['snapshot_id'],'manifest_sha256':proof['manifest_sha256']}
    shutil.rmtree(cohort/'staging'/spec.dataset);shutil.rmtree(spec.stage_root/spec.dataset)
    observed={'fingerprint':'original','process_references':[]}
    monkeypatch.setattr(returns,'remote',lambda *a:observed)
    monkeypatch.setattr(returns,'_check_d_primary_mount',lambda *a:None)
    monkeypatch.setattr(returns,'verify_cold_archive',lambda *a,**kw:proof)
    monkeypatch.setattr(returns,'resolve_latest_packed',lambda *a:resolve_latest_packed(packed,spec.dataset))
    monkeypatch.setattr(returns,'_rsync_candidate',lambda *a,**kw:pytest.fail('must not refetch removed scratch'))
    seen=[]
    monkeypatch.setattr(returns,'commit_prepared_archive',lambda *a,**kw:seen.append((a,kw)))
    returns.archive_one(SimpleNamespace(state_root=cohort,sync_root=packed),{},row,spec)
    assert len(seen)==1 and seen[0][0][5] is None
    assert seen[0][1]['reused_proof']['decoded_originals_verified']
    assert not (cohort/'staging'/spec.dataset).exists()


@pytest.mark.parametrize('failure',[None,'expired','local-mutation','cold-mutation'])
def test_owned_full_plan_reuse_keeps_every_final_generation_gate(saved,tmp_path,monkeypatch,failure):
    spec,cohort,source,packed=saved
    before=cleanup.plan(spec,cohort)
    if failure=='expired':
        before['recovery_verified_at_epoch']=time.time()-1501
        before['plan_fingerprint']=cleanup.plan_identity(before)
    elif failure=='local-mutation':
        file=source/'original.bin';info=file.stat();file.write_bytes(b'X'*info.st_size)
        os.utime(file,ns=(info.st_atime_ns,info.st_mtime_ns))
    elif failure=='cold-mutation':
        from stockagent.data_sync.packed_snapshots import resolve_latest_packed
        resolved=resolve_latest_packed(packed,spec.dataset)
        file=packed/resolved.manifest['archive']['objects'][0]['relpath'];info=file.stat()
        file.write_bytes(b'X'*info.st_size);os.utime(file,ns=(info.st_atime_ns,info.st_mtime_ns))
    monkeypatch.setattr(cleanup,'plan',lambda *a:pytest.fail('the already owned full proof must not run under mutation lock again'))
    receipts=tmp_path/'receipts';receipts.mkdir()
    if failure:
        with pytest.raises(SnapshotError):
            cleanup.apply(spec,cohort,before,receipts,owned_verified_plan=before)
        assert source.exists()
    else:
        result=cleanup.apply(spec,cohort,before,receipts,owned_verified_plan=before)
        assert result['pruned'] and not source.exists()
        assert verify_cold_archive(spec,packed)['decoded_originals_verified']
