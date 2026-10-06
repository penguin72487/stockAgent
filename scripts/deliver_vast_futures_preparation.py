#!/usr/bin/env python3
"""Deliver and verify the narrowly authorized futures accidental-deletion rescue.

Wait for canonical original reconstruction; one tar/zstd SSH stream preserves
hardlinks and avoids per-file round trips. Verify the entire original index on
Vast, then atomically expose only at an absent or still-empty original path.
Cold bytes, existing data, code, models and services are never deleted/replaced.
"""
from __future__ import annotations

import argparse
import ctypes
import hashlib
import inspect
import json
import os
from pathlib import Path
import shlex
import stat
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.ingest_remote_cold_artifacts import _ssh_base
from scripts.return_remote_legacy_archives import private_control_prefix
from scripts.recover_vast_futures_preparation import DESTINATION, WORK, RECEIPTS, RELATIVE_ROOT
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json, sha256_file
from stockagent.data_sync.remote_legacy_return import real, metadata_tree, signature, recovery_hold_references
from stockagent.data_sync.materialized_cache import process_references


def verify_restored_tree(source: Path, index_path: Path, expected_index_sha256: str) -> dict:
    source = real(source)
    if sha256_file(index_path) != expected_index_sha256:
        raise SnapshotError('private recovery inventory differs from D original index')
    original = json.loads(index_path.read_text())
    prefix = original['relative_root']
    expected = {r['path'][len(prefix) + 1:]: r for r in original['rows'] if r['path'] != prefix}
    top = next(r for r in original['rows'] if r['path'] == prefix)
    baseline = metadata_tree(source)
    if {r['path'] for r in baseline['rows']} != set(expected):
        raise SnapshotError('restoration has extra/missing original names')
    digests, files, logical = {}, 0, 0
    for row in [{'path': '', 'kind': 'directory'}, *baseline['rows']]:
        path = source / row['path']
        actual = path.lstat()
        wanted = top if not row['path'] else expected[row['path']]
        if (row['kind'] != wanted['kind'] or path.is_symlink()
                or row.get('cross_filesystem') or actual.st_mtime_ns != wanted['mtime_ns']
                or stat.S_IMODE(actual.st_mode) != wanted['mode']
                or actual.st_uid != wanted['uid'] or actual.st_gid != wanted['gid']):
            raise SnapshotError('reconstructed original portable metadata differs')
        if row['kind'] == 'file':
            inode = (actual.st_dev, actual.st_ino)
            if inode not in digests:
                digests[inode] = sha256_file(path)
            if (actual.st_size != wanted['size'] or digests[inode] != wanted['sha256']
                    or signature(path.lstat()) != row['signature']):
                raise SnapshotError('reconstructed original SHA/size/signature differs')
            files += 1
            logical += actual.st_size
    if metadata_tree(source) != baseline:
        raise SnapshotError('reconstructed original mutated during verification')
    return {'state': 'entire_original_tree_verified', 'files': files, 'logical_bytes': logical,
            'fingerprint': baseline['fingerprint'], 'original_index_sha256': expected_index_sha256,
            'compressed_sha256': original['compressed_sha256'], 'cold_deleted': False}


def rename_no_replace(source: Path, destination: Path):
    libc = ctypes.CDLL(None, use_errno=True)
    rename = libc.renameat2
    rename.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
    rename.restype = ctypes.c_int
    if rename(-100, os.fsencode(source), -100, os.fsencode(destination), 1):
        number = ctypes.get_errno()
        raise OSError(number, os.strerror(number))


def control(action: str, index_sha256: str, logical_bytes: int) -> dict:
    code = private_control_prefix() + '''
import json,stat,os,time,ctypes
from pathlib import Path
from stockagent.data_sync.desync_snapshots import sha256_file,atomic_write_json,SnapshotError
from stockagent.data_sync.materialized_cache import process_references
'''
    code += inspect.getsource(verify_restored_tree) + '\n' + inspect.getsource(rename_no_replace)
    code += f"action={action!r}\nwork=Path({str(WORK)!r})\nindex_sha={index_sha256!r}\nrequired={logical_bytes!r}\n"
    code += '''
root=Path('/root/stockAgent')
target=real(root/'artifacts/markets/tw_futures_v8_margin_preparation')
stage=real(work/'received-preparation')
if not recovery_hold_references(target,root):raise SnapshotError('explicit recovery hold absent')
if action=='prepare':
 if target.exists() and (not target.is_dir() or list(target.iterdir())):raise SnapshotError('original path has new contents; do not overwrite')
 work.mkdir(parents=True,mode=0o700,exist_ok=True)
 fs=os.statvfs(work)
 if fs.f_bavail*fs.f_frsize<required+32*1024**3:raise SnapshotError('remote restore lacks original bytes plus 32 GiB reserve')
 stage.mkdir(mode=0o700)
 result={'state':'private_remote_restore_admitted','destination':str(stage),'cold_deleted':False}
elif action=='verify-promote':
 result=verify_restored_tree(stage,work/'expected-originals.json',index_sha)
 atomic_write_json(work/'historical-tree-verified.json',result)
 if process_references(stage):raise SnapshotError('private reconstruction is in use')
 if target.exists():
  before=signature(target.lstat())
  if not target.is_dir() or list(target.iterdir()) or process_references(target):raise SnapshotError('original path changed or has a reader; do not replace')
  empty=real(work/'original-empty-directory')
  rename_no_replace(target,empty)
  if list(empty.iterdir()) or signature(empty.lstat())[:4]!=before[:4]:raise SnapshotError('old empty directory changed; preserve both paths')
 if metadata_tree(stage)['fingerprint']!=result['fingerprint'] or process_references(stage):raise SnapshotError('stage changed before exposure')
 rename_no_replace(stage,target)
 fd=os.open(target.parent,os.O_RDONLY|os.O_DIRECTORY)
 try:os.fsync(fd)
 finally:os.close(fd)
 result.update(state='historical_original_root_restored',destination=str(target),existing_files_overwritten=0)
 atomic_write_json(work/'historical-root-promoted.json',result)
else:raise SnapshotError('unknown fixed recovery operation')
print(json.dumps(result),flush=True)
'''
    process = subprocess.run([*_ssh_base(Path('/root/.ssh/stockagent_vastai1t_ed25519'), 40032),
                              'root@114.32.64.6',
                              'cd /root/stockAgent && source scripts/runtime_env.sh && run_fintech_python -'],
                             input=code, text=True, capture_output=True, timeout=3600)
    if process.returncode:
        atomic_write_json(RECEIPTS / ('remote-' + action + '-error.json'),
                          {'stderr': process.stderr, 'returncode': process.returncode})
        raise SnapshotError('remote rescue rejected; private error receipt retained')
    return json.loads(process.stdout.splitlines()[-1])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--watch', action='store_true')
    args = parser.parse_args()
    if not args.apply:
        print(json.dumps({'state': 'no_overwrite_delivery_plan', 'source': str(DESTINATION),
                          'remote_root': RELATIVE_ROOT, 'cold_deleted': False}))
        return
    ready = RECEIPTS / 'received-carrier-restored.json'
    deadline = time.monotonic() + 7200
    while not ready.exists():
        if not args.watch or time.monotonic() > deadline:
            raise SnapshotError('independent original reconstruction is not complete')
        time.sleep(15)
    proof = json.loads(ready.read_text())
    if proof['state'] != 'original_root_restored_verified' or proof['relative_root'] != RELATIVE_ROOT:
        raise SnapshotError('wrong reconstruction receipt')
    index_path = WORK / 'expected-originals.json'
    index_sha = sha256_file(index_path)
    local = verify_restored_tree(DESTINATION, index_path, index_sha)
    atomic_write_json(RECEIPTS / 'local-before-delivery-verified.json', local)
    admitted = control('prepare', index_sha, local['logical_bytes'])
    atomic_write_json(RECEIPTS / 'remote-restore-admission.json', admitted)
    transport = ['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=12', '-i',
                 '/root/.ssh/stockagent_vastai1t_ed25519', '-p', '40032']
    subprocess.run(['rsync', '-a', '--compress', '--ignore-existing', '-e', shlex.join(transport),
                    str(index_path), 'root@114.32.64.6:' + str(index_path)], check=True)
    with (RECEIPTS / 'delivery-stderr.log').open('ab') as errors:
        producer = subprocess.Popen(['tar', '--format=pax', '--pax-option=delete=atime,delete=ctime',
                                     '--numeric-owner', '--sparse', '-cf', '-', '-C', str(DESTINATION), '.'],
                                    stdout=subprocess.PIPE, stderr=errors)
        compressor = subprocess.Popen(['zstd', '-T4', '-1', '-c'], stdin=producer.stdout,
                                       stdout=subprocess.PIPE, stderr=errors)
        producer.stdout.close()
        command = 'set -o pipefail; zstd -dq | tar --numeric-owner --same-owner --same-permissions --delay-directory-restore -xf - -C ' + shlex.quote(str(DESTINATION))
        consumer = subprocess.Popen([*transport, 'root@114.32.64.6', command],
                                     stdin=compressor.stdout, stdout=sys.stdout, stderr=errors)
        compressor.stdout.close()
        try:
            codes = [consumer.wait(timeout=7200), compressor.wait(timeout=30), producer.wait(timeout=30)]
            if any(codes):
                raise SnapshotError('rescue stream failed; private remote source retained')
        finally:
            for process in (consumer, compressor, producer):
                if process.poll() is None:
                    process.terminate()
                    process.wait(timeout=30)
    result = control('verify-promote', index_sha, local['logical_bytes'])
    atomic_write_json(RECEIPTS / 'remote-historical-root-restored.json', result)
    print(json.dumps(result), flush=True)


if __name__ == '__main__':
    main()
