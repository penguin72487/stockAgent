"""Bounded immutable SFTP hydration under the canonical packed transport owner."""
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import time

from stockagent.data_sync.offhost_backup import private_json
from stockagent.data_sync.packed_backup import signature
from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.remote_ssh import ssh_base, validate_ssh_target
from stockagent.runtime_identity import identity_sha256

POLICY = Path('/etc/stockagent/packed-rclone-transport.json')

# This locally maintained program is sent over the already authorized SSH
# identity. Peer JSON supplies data paths only; it never supplies Python/shell.
REMOTE_ADAPTER = r'''
import hashlib,json,os,re,sys,time
from pathlib import Path,PurePosixPath
p=json.load(sys.stdin); root=Path(p['remote_root'])
def safe(relative):
    rel=PurePosixPath(relative)
    if rel.is_absolute() or '..' in rel.parts or rel.as_posix()!=relative or any(c in relative for c in (chr(92),chr(13),chr(10),chr(0))):
        raise ValueError('unsafe packed member')
    path=root/relative
    if any(q.is_symlink() for q in (path,*path.parents)):raise ValueError('redirected packed member')
    return path
def sig(path):
    s=path.stat();return [s.st_dev,s.st_ino,s.st_size,s.st_mtime_ns,s.st_ctime_ns]
def full(path,spec):
    before=sig(path)
    if not path.is_file() or before[2]!=spec['bytes']:raise ValueError('immutable size differs')
    with path.open('rb') as f:actual=hashlib.file_digest(f,'sha256').hexdigest()
    if actual!=spec['sha256'] or sig(path)!=before:raise ValueError('immutable bytes changed')
    return {'signature':before,'sha256':actual,'bytes':spec['bytes'],'verified_epoch':time.time()}
def fence():
    state=Path(p['remote_state_root'])/'state.json'
    ignore=safe('.stignore-edge')
    if hashlib.sha256(state.read_bytes()).hexdigest()!=p['state_sha256'] or hashlib.sha256(ignore.read_bytes()).hexdigest()!=p['ignore_sha256']:
        raise ValueError('exact demand changed; retain unpublished staging')
    value=json.loads(state.read_bytes())
    if value.get('schema_version')!=1 or value.get('mode')!='index-only':raise ValueError('edge role changed')
if root.is_symlink() or not root.is_dir():raise ValueError('packed root unavailable')
for relative,spec in p['files'].items():
    if not re.fullmatch(r'objects/(?:blobs|packs)/[0-9a-f]{2}/[0-9a-f]{64}\.(?:blob|zip)',relative) or Path(relative).name.split('.')[0]!=spec['sha256']:
        raise ValueError('object descriptor differs from exact manifest')
fence(); mode=p['mode'];result={}
if mode=='observe':
    import shutil
    missing=[]
    for relative,spec in p['files'].items():
        target=safe(relative)
        if not target.exists():missing.append(relative);continue
        cached=p.get('cached',{}).get(relative,{})
        age=time.time()-cached.get('verified_epoch',0)
        if cached.get('signature')==sig(target) and cached.get('sha256')==spec['sha256'] and cached.get('bytes')==spec['bytes'] and 0<=age<=3600:
            result[relative]=cached
        else:result[relative]=full(target,spec)
    fence();print(json.dumps({'files':result,'missing':missing,'free_bytes':shutil.disk_usage(root).free}))
elif mode in ('prepare','finish'):
    if not re.fullmatch(r'[0-9a-f]{64}',p['transfer_identity_sha256']):raise ValueError('invalid transfer identity')
    stage=safe('.local-state/staging/rclone-'+p['transfer_identity_sha256']); payload=stage/'payload'
    marker={'contract':'fixed_packed_rclone_transfer_v1','files':p['files'],'producer_device_id':p['producer_device_id'],'receiver_device_id':p['receiver_device_id']}
    if mode=='prepare':
        stage.mkdir(parents=True,exist_ok=True);payload.mkdir(exist_ok=True)
        mark=stage/'transfer.json'
        if mark.exists() and json.loads(mark.read_bytes())!=marker:raise ValueError('owned staging differs')
        if not mark.exists():mark.write_text(json.dumps(marker,sort_keys=True))
        print(json.dumps({'state':'prepared','staging':str(payload)}))
    else:
        if json.loads((stage/'transfer.json').read_bytes())!=marker:raise ValueError('staging marker differs')
        observed={q.relative_to(payload).as_posix() for q in payload.rglob('*') if q.is_file()}
        if observed!=set(p['files']) or any(q.is_symlink() for q in payload.rglob('*')):raise ValueError('staging exact file set differs')
        staged={relative:full(payload/relative,spec) for relative,spec in p['files'].items()}
        # Check every existing final member before any promotion; same-size
        # corrupt objects remain untouched and block the entire wave.
        for relative,spec in p['files'].items():
            target=safe(relative)
            if target.exists():full(target,spec)
        fence()
        for relative,spec in p['files'].items():
            target=safe(relative);target.parent.mkdir(parents=True,exist_ok=True)
            try:os.link(payload/relative,target,follow_symlinks=False)
            except FileExistsError:full(target,spec)
            result[relative]=full(target,spec)
            if sig(payload/relative)[:3]!=staged[relative]['signature'][:3]:raise ValueError('staging inode changed during atomic promotion')
            staged[relative]['signature']=sig(payload/relative)
        fence()
        for relative in p['files']:
            q=payload/relative
            if sig(q)!=staged[relative]['signature']:raise ValueError('staged bytes changed before cleanup')
            q.unlink()
        for parent,_,_ in sorted(os.walk(payload),key=lambda x:len(Path(x[0]).parts),reverse=True):Path(parent).rmdir()
        (stage/'transfer.json').unlink();stage.rmdir()
        # Removing the temporary hardlink changes the target's ctime. Pin the
        # final, independently hashed inode after that known metadata change.
        result={relative:full(safe(relative),spec) for relative,spec in p['files'].items()}
        print(json.dumps({'state':'verified','files':result,'source_objects_deleted':False}))
else:raise ValueError('unknown fixed transfer action')
'''


def configuration(path=POLICY):
    if not path.exists() and not path.is_symlink():
        return None
    if path.is_symlink() or path.stat().st_uid != os.geteuid() or path.stat().st_mode & 0o077:
        raise SnapshotError('rclone edge policy must be private and locally owned')
    c = json.loads(path.read_bytes())
    if c.get('schema_version') != 1 or c.get('backend') != 'rclone-sftp' or not re.fullmatch(r'[A-Za-z0-9_-]+', c['remote_name']):
        raise SnapshotError('unsupported immutable edge transport policy')
    if (c['remote_root'] != '/srv/stockagent-packed' or c['remote_state_root'] != '/var/lib/stockagent-packed-edge'
            or type(c['workers']) is not int or not 1 <= c['workers'] <= 4
            or type(c['maximum_files']) is not int or not 1 <= c['maximum_files'] <= 128
            or not 1024**2 <= c['maximum_bytes'] <= 1024**3
            or not c['maximum_bytes'] <= c['maximum_single_object_bytes'] <= 8*1024**3
            or c['reserve_bytes'] < 8*1024**3):
        raise SnapshotError('immutable edge resource/role policy is outside its bounded contract')
    for name in ('rclone', 'rclone_config'):
        value = Path(c[name])
        if value.is_symlink() or not value.is_file():
            raise SnapshotError('rclone transport binary/config is redirected')
    if Path(c['rclone_config']).stat().st_mode & 0o077:
        raise SnapshotError('rclone SFTP connection must remain private')
    with Path(c['rclone']).open('rb') as stream:
        if hashlib.file_digest(stream, 'sha256').hexdigest() != c['rclone_sha256']:
            raise SnapshotError('immutable edge rclone binary changed')
    return c


def remote(args, c, payload):
    command = [*ssh_base(args.identity_file, args.ssh_port), '-o', 'StrictHostKeyChecking=yes',
               validate_ssh_target(args.ssh_target), 'cd ' + shlex.quote(c['remote_repo_root'])
               + ' && source scripts/runtime_env.sh && run_fintech_python -c ' + shlex.quote(REMOTE_ADAPTER)]
    result = subprocess.run(command, input=json.dumps(payload), text=True, capture_output=True, timeout=180)
    if result.returncode:
        raise SnapshotError('fixed remote immutable object verification failed; no overwrite/delete fallback')
    return json.loads(result.stdout)


def deliver(args, c, demand, allowed, *, remote_call=remote):
    started = time.perf_counter()
    expected, before = {}, {}
    for relative in sorted(allowed):
        source = args.sync_root / relative
        if source.is_symlink() or not source.is_file():
            raise SnapshotError('requested source object is unavailable')
        before[relative] = signature(source)
        expected[relative] = {'sha256': source.name.split('.')[0], 'bytes': source.stat().st_size}
    proof_path = args.state_root / 'rclone-payload-proofs.json'
    cached = json.loads(proof_path.read_bytes()).get('files', {}) if proof_path.exists() else {}
    base = {'remote_root': c['remote_root'], 'remote_state_root': c['remote_state_root'],
            'state_sha256': demand['state_sha256'], 'ignore_sha256': demand['ignore_sha256'],
            'producer_device_id': c['producer_device_id'], 'receiver_device_id': c['receiver_device_id']}
    observed = remote_call(args, c, {**base, 'mode': 'observe', 'files': expected, 'cached': cached})
    room = observed['free_bytes'] - c['reserve_bytes']
    chosen, size = {}, 0
    for relative in observed['missing']:
        count = expected[relative]['bytes']
        if count > room:
            continue
        if chosen and (len(chosen) >= c['maximum_files'] or size + count > c['maximum_bytes']):
            break
        if not chosen and count > c['maximum_single_object_bytes']:
            continue
        chosen[relative] = expected[relative];size += count
    pending_path = args.state_root / 'rclone-transfer.json'
    if pending_path.exists():
        pending = json.loads(pending_path.read_bytes())
        if (pending.get('state') == 'copying' and pending.get('producer_device_id') == c['producer_device_id']
                and pending.get('receiver_device_id') == c['receiver_device_id'] and pending.get('files')
                and all(expected.get(k) == row for k, row in pending['files'].items())):
            chosen = pending['files']
            size = sum(r['bytes'] for r in chosen.values())
    codes = []
    if chosen and args.apply:
        identity = identity_sha256({k: base[k] for k in ('remote_root', 'producer_device_id', 'receiver_device_id')} | {'files': chosen})
        request = {**base, 'files': chosen, 'transfer_identity_sha256': identity}
        prepared = remote_call(args, c, {**request, 'mode': 'prepare'})
        filelist = args.state_root / 'rclone-files.txt'
        filelist.write_text(''.join(relative + '\n' for relative in chosen));filelist.chmod(0o600)
        private_json(args.state_root / 'rclone-transfer.json', {'state': 'copying', **request})
        result = subprocess.run([c['rclone'], 'copy', str(args.sync_root), c['remote_name'] + ':' + prepared['staging'],
            '--config', c['rclone_config'], '--files-from-raw', str(filelist), '--immutable', '--checksum',
            '--transfers', str(c['workers']), '--checkers', str(c['workers']), '--buffer-size', '8Mi',
            '--retries', '1', '--low-level-retries', '2', '--stats', '0'], capture_output=True, timeout=1200)
        if result.returncode:
            raise SnapshotError('immutable rclone SFTP transfer failed; unpublished staging retained')
        for relative in chosen:
            if signature(args.sync_root / relative) != before[relative]:
                raise SnapshotError('source object changed during immutable transfer')
        finished = remote_call(args, c, {**request, 'mode': 'finish'})
        observed['files'].update(finished['files'])
        codes = [0, 0]
        private_json(args.state_root / 'rclone-transfer.json', {'state': 'verified', 'transfer_identity_sha256': identity, 'bytes': size, 'command_exit_codes': codes})
    if args.apply:
        private_json(proof_path, {'files': observed['files']})
    missing = set(expected) - set(observed['files'])
    return {'state': 'verified' if not missing else 'waiting_exact_objects', 'verified_objects': len(observed['files']),
            'requested_objects': len(expected), 'copied_objects': len(chosen) if codes else 0, 'copied_bytes': size if codes else 0,
            'pending_objects': len(missing), 'all_requested_objects_sha256_verified': not missing,
            'command_exit_codes': codes, 'complete_workflow_seconds': time.perf_counter() - started,
            'immutable_source_deleted': False, 'training_or_artifact_deleted': False}
