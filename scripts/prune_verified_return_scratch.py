#!/usr/bin/env python3
"""Retire exact local return scratch after independent canonical D recovery.

Remote originals, source protection, D objects and NAS retention are unchanged.
Reuse the retained cohort owner and legacy recovery verifier; no SSH is needed
to prove that these private local copies are redundant.
"""
from __future__ import annotations
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import stat
import sys
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.return_remote_legacy_archives import cohort_owner, PUBLICATION_OWNER
from stockagent.data_sync.cold_primary import _check_d_primary_mount
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json, sha256_file
from stockagent.data_sync.legacy_artifact_archive import load_legacy_specs, verify_cold_archive, verify_archive_directory
from stockagent.data_sync.materialized_cache import process_references_many
from stockagent.data_sync.packed_snapshots import resolve_latest_packed, _load_inventory, _verify_materialized
from stockagent.data_sync.remote_legacy_return import identity, metadata_tree, real, signature

STATE_ROOTS = {Path('/var/lib/stockagent-vast-legacy-return'), Path('/var/lib/stockagent-vast-legacy-return-partitions')}
PACKED = Path('/srv/stockagent-packed')
# Only these independently inventoried complete failed verification workspaces
# are enrolled. Unknown partial/failed directories remain diagnostic evidence.
REGISTERED_RECOVERY_SCRATCH = {
    (Path('/var/lib/stockagent-vast-legacy-return'), 'legacy-vast-return-4fd23701b2fcda4cb3759e49'): {
        'path': '/var/lib/stockagent-vast-legacy-return/verification-scratch/stockagent-legacy-verify-m2sl_qnf',
        'snapshot_id': 'legacy-vast-return-4fd23701b2fcda4cb3759-20261004T120348244306878Z-l0-penguin-006c0ea85a482edf',
        'manifest_sha256': '147dd154273dc93b1eadb5e340c90250edd9a58ef75d3edc555ab6b5cabd9e0c',
    },
    (Path('/var/lib/stockagent-vast-legacy-return'), 'legacy-vast-return-118fde2d0725e3a42d1969be'): {
        'path': '/var/lib/stockagent-vast-legacy-return/verification-scratch/stockagent-legacy-verify-fxsjuxm4',
        'snapshot_id': 'legacy-vast-return-118fde2d0725e3a42d196-20261005T102736789182404Z-l0-penguin-d1695c5581f6ef10',
        'manifest_sha256': '1a293d2f0fda9cb44329eff6c1cdd2b3ace1b6bb088937c876af8f75ca2e80c8',
    },
}


def registered_recovery_tree(spec, state_root):
    registered = REGISTERED_RECOVERY_SCRATCH.get((state_root, spec.dataset))
    if registered is None:
        return None
    path = Path(registered['path'])
    if (path.parent != state_root / 'verification-scratch'
            or not path.name.startswith('stockagent-legacy-verify-')):
        raise SnapshotError('registered recovery scratch is outside its private cohort')
    return real(path) if path.exists() or path.is_symlink() else None


def validate_recovery_scratch(spec, state_root, path, generation, original_manifest):
    registered = REGISTERED_RECOVERY_SCRATCH[(state_root, spec.dataset)]
    if any(registered[k] != generation[k] for k in ('snapshot_id', 'manifest_sha256')):
        raise SnapshotError('registered recovery scratch differs from the fixed cold release')
    before = safe_tree(path)
    snapshot = generation['snapshot_id']
    prefix = spec.dataset + '/' + snapshot
    archive = path / prefix
    resolved = resolve_latest_packed(PACKED, spec.dataset)
    entries = _load_inventory(PACKED, resolved.manifest)
    _verify_materialized(archive, resolved.manifest, entries)
    if verify_archive_directory(archive, spec=spec)['manifest'] != original_manifest:
        raise SnapshotError('registered recovery scratch original inventory differs')
    ready_name = spec.dataset + '/.' + snapshot + '.READY.json'
    ready = json.loads((path / ready_name).read_text())
    if (set(ready) not in ({'snapshot_id', 'manifest_sha256', 'verified_at'},
                          {'snapshot_id', 'manifest_sha256', 'verified_at', 'inventory_sha256'})
            or any(ready.get(k) != generation[k] for k in ('snapshot_id', 'manifest_sha256'))
            or not isinstance(ready['verified_at'], str)
            or ('inventory_sha256' in ready and ready['inventory_sha256'] != resolved.manifest['archive']['inventory']['sha256'])):
        raise SnapshotError('registered recovery READY identity differs')
    lock_name = '.locks/fetch-' + spec.dataset + '-' + snapshot + '.lock'
    if (path / lock_name).stat().st_size != 0:
        raise SnapshotError('registered recovery lock is not an empty canonical control')
    expected = {ready_name, lock_name} | {prefix + '/' + row['path'] for row in entries if row['kind'] == 'file'}
    directories = parent_directories(expected) | {prefix + '/' + row['path'] for row in entries if row['kind'] == 'directory'}
    if ({r['path'] for r in before['rows'] if r['kind'] == 'file'} != expected
            or {r['path'] for r in before['rows'] if r['kind'] == 'directory'} != directories
            or safe_tree(path)['rows'] != before['rows']):
        raise SnapshotError('registered recovery scratch has unknown paths or changed during validation')


def compare_source(source, manifest):
    tree = metadata_tree(source)
    expected = {r['path']:r for r in manifest['files']}
    actual = {r['path']:r for r in tree['rows'] if r['kind']=='file'}
    if set(actual) != set(expected):
        raise SnapshotError('return source scratch has missing or unknown files')
    for relative, member in expected.items():
        file = source / relative
        info = file.lstat()
        portable = member['source']
        if (info.st_size != portable['size'] or info.st_mtime_ns != portable['mtime_ns']
                or stat.S_IMODE(info.st_mode) != portable['mode']
                or sha256_file(file) != member['original_sha256']):
            raise SnapshotError('return source scratch differs from the exact cold original: ' + relative)
    if metadata_tree(source)['rows'] != tree['rows']:
        raise SnapshotError('return source scratch changed during full hashing')
    directories = manifest.get('directories')
    if directories is None:
        raise SnapshotError('original directory set was not preserved by this archive')
    actual_dirs = {r['path'] for r in tree['rows'] if r['kind']=='directory'}
    if actual_dirs != {r['path'] for r in directories}:
        raise SnapshotError('return source scratch has unknown or missing directories')


def parent_directories(files):
    return {str(p) for file in files for p in Path(file).parents if str(p) != '.'}


def safe_tree(tree):
    data = metadata_tree(real(tree))
    if (process_references_many((tree,)) or any(r['kind']=='unsupported' or r['cross_filesystem']
            or (r['kind']=='file' and r['signature'][6] != 1) for r in data['rows'])):
        raise SnapshotError('private scratch is referenced, redirected, shared or crosses a filesystem')
    return data


def cold_generation(dataset):
    resolved=resolve_latest_packed(PACKED,dataset)
    refs=[resolved.manifest['archive']['inventory'],*resolved.manifest['archive']['objects']]
    observed=[]
    for member in refs:
        path=real(PACKED/member['relpath'])
        info=path.lstat()
        if not stat.S_ISREG(info.st_mode) or info.st_size!=member['bytes']:
            raise SnapshotError('cold recovery object generation is unavailable')
        observed.append({'relative':member['relpath'],'signature':signature(info)})
    return {'snapshot_id':resolved.manifest['snapshot_id'],'manifest_sha256':resolved.manifest_sha256,
            'objects':observed}


def plan_identity(value):
    # A repeated full dry run observes a new verification time while preserving
    # the same exact generation, files and reclaim plan. Never extend this time
    # when reusing a plan; it is checked independently at the owned mutation.
    return identity({k:v for k,v in value.items() if k not in {'plan_fingerprint','recovery_verified_at_epoch'}})


def plan(spec, state_root):
    _check_d_primary_mount(PACKED)
    scratch = state_root / 'staging' / spec.dataset
    encoded = spec.stage_root / spec.dataset
    recovery_tree = registered_recovery_tree(spec, state_root)
    trees = [t for t in (scratch, encoded, recovery_tree) if t is not None and t.exists()]
    if not trees:
        raise SnapshotError('no local scratch remains')
    generation=cold_generation(spec.dataset)
    proof = verify_cold_archive(spec, PACKED, verification_root=state_root / 'verification-scratch')
    manifest = proof['manifest']
    if recovery_tree is not None:
        validate_recovery_scratch(spec, state_root, recovery_tree, generation, manifest)
    if scratch.exists():
        intent = scratch / 'transfer-intent.json'
        document = json.loads(intent.read_text())
        if (set(document) != {'relative_root','remote_fingerprint','dataset'}
                or document.get('relative_root') != spec.relative_root or document.get('dataset') != spec.dataset
                or not isinstance(document['remote_fingerprint'],str) or len(document['remote_fingerprint']) != 64
                or any(c not in '0123456789abcdef' for c in document['remote_fingerprint'])):
            raise SnapshotError('transfer intent is outside the selected cohort')
        compare_source(scratch / 'artifacts' / spec.relative_root, manifest)
        expected = {'transfer-intent.json'} | {'artifacts/' + spec.relative_root + '/' + r['path'] for r in manifest['files']}
        actual = {r['path'] for r in metadata_tree(scratch)['rows'] if r['kind']=='file'}
        if actual != expected:
            raise SnapshotError('outer return scratch has unknown files')
        expected_dirs = parent_directories(expected) | {'artifacts/' + spec.relative_root + '/' + r['path'] for r in manifest['directories']}
        if {r['path'] for r in metadata_tree(scratch)['rows'] if r['kind']=='directory'} != expected_dirs:
            raise SnapshotError('outer return scratch has unknown directories')
    if encoded.exists():
        check = verify_archive_directory(encoded / 'archive', spec=spec)
        if check['manifest'] != manifest:
            raise SnapshotError('encoded local manifest differs from recovered canonical D')
        controls = {'source_plan.json', 'archive/legacy_archive_manifest.json'}
        for member in manifest['files']:
            name = 'receipts/' + hashlib.sha256(member['path'].encode()).hexdigest() + '.json'
            if json.loads((encoded / name).read_text()) != member:
                raise SnapshotError('encoded scratch receipt changed')
            controls.add(name)
            controls.add('archive/' + member['encoded_path'])
        if json.loads((encoded / 'source_plan.json').read_text()) != [
                {'path':m['path'], 'source':m['source']} for m in manifest['files']]:
            raise SnapshotError('encoded source plan differs')
        if {r['path'] for r in metadata_tree(encoded)['rows'] if r['kind']=='file'} != controls:
            raise SnapshotError('encoded scratch has unknown files')
        if {r['path'] for r in metadata_tree(encoded)['rows'] if r['kind']=='directory'} - (parent_directories(controls) | {'codec-trials'}):
            raise SnapshotError('encoded scratch has unknown directories')
    observed = [{"path":str(tree), **safe_tree(tree)} for tree in trees]
    if cold_generation(spec.dataset)!=generation or any(proof[k]!=generation[k] for k in ('snapshot_id','manifest_sha256')):
        raise SnapshotError('cold generation changed during complete local scratch validation')
    value = {'dataset':spec.dataset, 'snapshot_id':proof['snapshot_id'], 'manifest_sha256':proof['manifest_sha256'],
             'trees':observed, 'cold_originals_independently_verified':True,
             'cold_generation':generation, 'recovery_verified_at_epoch':proof['verified_at_epoch'],
             'reclaimable_allocated_file_bytes':sum(t['reclaimable_allocated_file_bytes'] for t in observed),
             'remote_sources_deleted':0, 'cold_objects_deleted':0, 'nas_snapshots_deleted':0}
    value['plan_fingerprint'] = plan_identity(value)
    return value


def apply(spec, state_root, before, receipt_dir, *, owned_verified_plan=None):
    if owned_verified_plan is None:
        current = plan(spec, state_root)
    else:
        # Only the same CLI/cohort owner passes its freshly computed full plan.
        # Hold the common mutation owner for these final generation/signature
        # checks and exact unlink, not for another identical complete recovery.
        current=owned_verified_plan
        if (plan_identity(current)!=current['plan_fingerprint'] or current['dataset']!=spec.dataset
                or current.get('cold_originals_independently_verified') is not True
                or not 0<=time.time()-current['recovery_verified_at_epoch']<=1500
                or cold_generation(spec.dataset)!=current['cold_generation']):
            raise SnapshotError('owned full recovery expired or its fixed generation changed')
        for tree in current['trees']:
            if safe_tree(Path(tree['path']))['rows']!=tree['rows']:
                raise SnapshotError('private scratch changed while awaiting its mutation owner')
    if current['plan_fingerprint'] != before['plan_fingerprint']:
        raise SnapshotError('private scratch changed after dry run')
    resolved = resolve_latest_packed(PACKED, spec.dataset)
    if resolved.manifest_sha256 != current['manifest_sha256']:
        raise SnapshotError('exact D release changed after full recovery')
    journal = {'dataset':spec.dataset, 'plan_fingerprint':current['plan_fingerprint'], 'state':'prepared', 'trees':[]}
    journal_path = receipt_dir / (spec.dataset + '.journal.json')
    atomic_write_json(journal_path, journal)
    for tree in current['trees']:
        source = Path(tree['path'])
        quarantine = source.with_name(source.name + '.verified-prune-' + uuid.uuid4().hex)
        source.rename(quarantine)
        journal['trees'].append({'source':str(source),'quarantine':str(quarantine),'state':'renamed'})
        atomic_write_json(journal_path, journal)
        if safe_tree(quarantine)['rows'] != tree['rows']:
            raise SnapshotError('scratch changed after quarantine; retained')
        _check_d_primary_mount(PACKED)
        if cold_generation(spec.dataset)!=current['cold_generation']:
            raise SnapshotError('D release changed after quarantine; retained')
        for index, row in enumerate(tree['rows']):
            if row['kind'] != 'file':
                continue
            path = quarantine / row['path']
            if signature(path.lstat()) != row['signature']:
                raise SnapshotError('scratch changed immediately before exact unlink; retained')
            # The same two names need one complete /proc scan. Keep the exact
            # batch frequency and every fd/mmap/cwd check while holding owner.
            if index % 128 == 0 and process_references_many((source, quarantine)):
                raise SnapshotError('scratch became referenced; retained')
            path.unlink()
        for row in sorted((r for r in tree['rows'] if r['kind']=='directory'), key=lambda r:-len(Path(r['path']).parts)):
            (quarantine / row['path']).rmdir()
        quarantine.rmdir()
        journal['trees'][-1]['state']='pruned'
        atomic_write_json(journal_path, journal)
    journal['state']='pruned'
    journal['reclaimed_allocated_file_bytes']=current['reclaimable_allocated_file_bytes']
    atomic_write_json(journal_path, journal)
    return {k:v for k,v in current.items() if k!='trees'} | {'pruned':True}


def record_local_retirement(spec, state_root, result):
    """Keep the existing cohort writer from fetching these deleted copies again.

    Caller holds the original cohort owner. This changes only the local scratch
    fact and exact D identity; remote retirement/completion state is preserved.
    """
    if result.get('pruned') is not True or result.get('dataset') != spec.dataset:
        raise SnapshotError('only a completed exact local prune may update its cohort')
    if (state_root/'staging'/spec.dataset).exists() or (spec.stage_root/spec.dataset).exists():
        raise SnapshotError('local scratch still exists; do not record its removal')
    path = real(state_root/'progress.json')
    ledger = json.loads(path.read_text())
    matches = [row for row in ledger['items'] if row['relative_root'] == spec.relative_root]
    if len(matches) != 1:
        raise SnapshotError('local retirement has no unique original cohort row')
    row = matches[0]
    row.update(private_scratch_removed=True, cold_verified=True,
               decoded_originals_verified=True, snapshot_id=result['snapshot_id'],
               manifest_sha256=result['manifest_sha256'],
               local_scratch_retirement={
                   'contract':'verified-local-return-scratch-retirement-v1',
                   'dataset':spec.dataset, 'plan_fingerprint':result['plan_fingerprint'],
                   'snapshot_id':result['snapshot_id'], 'manifest_sha256':result['manifest_sha256'],
                   'remote_source_retirement_asserted':False,
               })
    atomic_write_json(path,ledger)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--state-root',type=Path,required=True)
    parser.add_argument('--dataset',action='append')
    parser.add_argument('--receipt-dir',type=Path,required=True)
    parser.add_argument('--apply',action='store_true')
    parser.add_argument('--wait-for-owner',action='store_true',
                        help='Queue this exact local cleanup behind the retained original cohort')
    args=parser.parse_args()
    if args.state_root not in STATE_ROOTS or real(args.state_root) != args.state_root:
        parser.error('only the two retained private return cohorts are enrolled')
    os.umask(0o077)
    args.receipt_dir.mkdir(parents=True,exist_ok=True)
    with cohort_owner(args.state_root, wait=args.wait_for_owner):
        specs=load_legacy_specs(args.state_root / 'archive-catalog.json')
        names=args.dataset or sorted(name for name,spec in specs.items()
            if (args.state_root/'staging'/name).exists() or (spec.stage_root/name).exists()
            or registered_recovery_tree(spec,args.state_root) is not None)
        prior_path=args.receipt_dir/'progress.json'
        prior=json.loads(prior_path.read_text()).get('items',[]) if prior_path.exists() else []
        results=[row for row in prior if row.get('pruned') is True]
        # Upgrade handoff preserves earlier successful facts. Backfill only
        # after another complete recovery of the identical fixed D release.
        if args.apply:
            ledger=json.loads((args.state_root/'progress.json').read_text())
            for result in results:
                spec=specs[result['dataset']]
                row=next(item for item in ledger['items'] if item['relative_root']==spec.relative_root)
                if row.get('local_scratch_retirement',{}).get('plan_fingerprint') == result['plan_fingerprint']:
                    continue
                for attempt in range(3):
                    try:
                        proof=verify_cold_archive(spec,PACKED,verification_root=args.state_root/'verification-scratch')
                        if (proof['snapshot_id'] != result['snapshot_id']
                                or proof['manifest_sha256'] != result['manifest_sha256']):
                            raise SnapshotError('prior local prune differs from the independently recovered D release')
                        with PUBLICATION_OWNER.open('a') as common:
                            fcntl.flock(common,fcntl.LOCK_EX)
                            if resolve_latest_packed(PACKED,spec.dataset).manifest_sha256 != proof['manifest_sha256']:
                                raise SnapshotError('D head changed before recording prior local retirement')
                            record_local_retirement(spec,args.state_root,result)
                        result.update(cohort_ledger_reconciled=True)
                        result.pop('cohort_ledger_error',None)
                        break
                    except (OSError,ValueError,SnapshotError) as error:
                        result.update(cohort_ledger_reconciled=False,cohort_ledger_error=str(error))
                        atomic_write_json(args.receipt_dir/'progress.json',{'items':results,
                            'checked_at_epoch':time.time(),'reconciling_dataset':spec.dataset,
                            'reconciliation_attempt':attempt+1})
                        if attempt<2:
                            time.sleep((15,60)[attempt])
                atomic_write_json(args.receipt_dir/'progress.json',{'items':results,'checked_at_epoch':time.time(),
                    'reclaimed_allocated_file_bytes':sum(r.get('reclaimable_allocated_file_bytes',0)
                                                        for r in results if r.get('pruned'))})
        for name in names:
            try:
                if name not in specs:
                    raise SnapshotError('scratch dataset is not in the retained cohort')
                spec=specs[name]
                dry=plan(spec,args.state_root)
                atomic_write_json(args.receipt_dir/(name+'.plan.json'),dry)
                if args.apply:
                    for attempt in range(3):
                        with PUBLICATION_OWNER.open('a') as common:
                            fcntl.flock(common,fcntl.LOCK_EX)
                            if 0<=time.time()-dry['recovery_verified_at_epoch']<=1500:
                                result=apply(spec,args.state_root,dry,args.receipt_dir,owned_verified_plan=dry)
                                record_local_retirement(spec,args.state_root,result)
                                break
                        dry=plan(spec,args.state_root)
                        atomic_write_json(args.receipt_dir/(name+'.plan.json'),dry)
                    else:
                        raise SnapshotError('complete local recovery repeatedly expired before mutation')
                else:
                    result={k:v for k,v in dry.items() if k!='trees'} | {'pruned':False,'apply_ready':True}
            except (OSError,ValueError,SnapshotError) as error:
                result={'dataset':name,'pruned':False,'error':str(error)}
            results=[old for old in results if old.get('dataset') != name]
            results.append(result)
            atomic_write_json(args.receipt_dir/'progress.json',{'items':results,'checked_at_epoch':time.time(),
                'reclaimed_allocated_file_bytes':sum(r.get('reclaimable_allocated_file_bytes',0) for r in results if r.get('pruned'))})
            print(json.dumps(result),flush=True)
    return 0 if all((r.get('pruned') or r.get('apply_ready'))
                    and r.get('cohort_ledger_reconciled') is not False for r in results) else 75


if __name__=='__main__':
    raise SystemExit(main())
