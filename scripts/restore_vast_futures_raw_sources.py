#!/usr/bin/env python3
"""Restore receipt-bound TAIFEX originals to data_tw_index_futures, never artifacts.

The old absolute receipt paths get data-only compatibility aliases. No receipt,
financial operand, source hash, service or cold carrier is changed or removed.
"""
from __future__ import annotations

import argparse
import inspect
import json
from pathlib import Path, PurePosixPath
import shlex
import stat
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.deliver_vast_futures_preparation import rename_no_replace
from scripts.return_remote_legacy_archives import private_control_prefix
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json, sha256_file
from stockagent.data_sync.remote_legacy_return import signature

NAME = 'official_final_settlement_all_asset_classes_20260929'
LEGACY = ROOT / 'artifacts/markets/tw_futures_v8_margin_preparation' / NAME
CANONICAL = ROOT / 'data_tw_index_futures/preparation_sources' / NAME
WORK = Path('/var/lib/stockagent-futures-recovery/20261004')
RECEIPTS = ROOT / 'artifacts/operations/futures_preparation_recovery_20261004'
TRANSPORT = ['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=12', '-i',
             '/root/.ssh/stockagent_vastai1t_ed25519', '-p', '40032']


def raw_inventory(receipts: list[dict], source: Path) -> list[dict]:
    """Full SHA/size validation; receipt paths may not escape the named raw root."""
    rows, seen = [], set()
    for receipt in receipts:
        old = Path(receipt['path'])
        if not old.is_absolute() or not old.is_relative_to(LEGACY / 'raw'):
            raise SnapshotError('raw receipt is outside the deleted official-source root')
        relative = old.relative_to(LEGACY / 'raw').as_posix()
        if any(p in {'..', '.'} for p in PurePosixPath(relative).parts) or relative in seen:
            raise SnapshotError('unsafe or duplicate raw receipt')
        seen.add(relative)
        path = source / relative
        for parent in [path, *path.parents]:
            if parent == source.parent:
                break
            if parent.is_symlink():
                raise SnapshotError('raw source redirects through a symlink')
        before = path.lstat()
        if (not stat.S_ISREG(before.st_mode) or before.st_size != receipt['bytes']
                or sha256_file(path) != receipt['sha256']
                or signature(path.lstat()) != signature(before)):
            raise SnapshotError('available C original differs from immutable source receipt')
        rows.append({'relative': relative, 'sha256': receipt['sha256'], 'bytes': before.st_size,
                     'source_signature': signature(before), 'url': receipt.get('url')})
    if not rows:
        raise SnapshotError('empty raw recovery inventory')
    return rows


def remote(code: str) -> dict:
    result = subprocess.run([*TRANSPORT, 'root@114.32.64.6',
                             'cd /root/stockAgent && source scripts/runtime_env.sh && run_fintech_python -'],
                            input=private_control_prefix() + code, text=True,
                            capture_output=True, timeout=600)
    if result.returncode:
        atomic_write_json(RECEIPTS / 'raw-source-remote-error.json',
                          {'returncode': result.returncode, 'stderr': result.stderr})
        raise SnapshotError('raw source rescue failed closed; private remote error retained')
    return json.loads(result.stdout.splitlines()[-1])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    discovery = remote('''
from pathlib import Path
from stockagent.data_sync.desync_snapshots import sha256_file
root=Path('/root/stockAgent')
source=root/'data_tw_futures/margin_sources'
manifest=json.loads((source/'source_manifest.json').read_text())
final=source/'final/manifest.json'
if sha256_file(final)!=manifest['files']['final/manifest.json']['sha256']:
 raise RuntimeError('final source manifest is not bound to the retained source release')
print(json.dumps({'source_manifest_sha256':sha256_file(source/'source_manifest.json'),
 'final_manifest_sha256':sha256_file(final),'receipts':json.loads(final.read_text())['receipts']}))
''')
    source = CANONICAL / 'raw' if (CANONICAL / 'raw').is_dir() else LEGACY / 'raw'
    rows = raw_inventory(discovery['receipts'], source)
    plan = {'state': 'all_bound_raw_sources_verified_on_penguin', 'files': len(rows),
            'bytes': sum(r['bytes'] for r in rows), 'source_manifest_sha256': discovery['source_manifest_sha256'],
            'final_manifest_sha256': discovery['final_manifest_sha256'], 'destination': str(CANONICAL / 'raw'),
            'cold_deleted': False, 'receipt_values_changed': False, 'rows': rows}
    atomic_write_json(RECEIPTS / 'raw-source-recovery-plan.json', plan)
    print(json.dumps({k: v for k, v in plan.items() if k != 'rows'}), flush=True)
    if not args.apply:
        return
    setup = f'''
import os
from pathlib import Path
from stockagent.data_sync.desync_snapshots import atomic_write_json
work=Path({str(WORK)!r})
stage=work/'raw-final-stage'
destination=Path({str(CANONICAL / 'raw')!r})
root=Path('/root/stockAgent')
if not recovery_hold_references(root/'artifacts/markets/tw_futures_v8_margin_preparation',root):raise RuntimeError('recovery hold missing')
for parent in [destination,*destination.parents]:
 if parent.is_symlink():raise RuntimeError('canonical source path redirects')
if destination.exists() or destination.is_symlink():raise FileExistsError('source destination exists; preserve it')
work.mkdir(parents=True,mode=0o700,exist_ok=True)
stage.mkdir(mode=0o700)
atomic_write_json(work/'raw-source-restore-intent.json',{plan!r})
print(json.dumps({{'state':'exclusive_source_stage_admitted'}}))
'''
    remote(setup)
    with (RECEIPTS / 'raw-source-transfer.log').open('ab') as errors:
        producer = subprocess.Popen(['tar', '--format=pax', '--pax-option=delete=atime,delete=ctime',
                                     '--numeric-owner', '-C', str(source), '--null', '-T', '-', '-cf', '-'],
                                    stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=errors)
        compressor = subprocess.Popen(['zstd', '-T2', '-1', '-c'], stdin=producer.stdout,
                                      stdout=subprocess.PIPE, stderr=errors)
        producer.stdout.close()
        command = 'set -o pipefail; zstd -dq | tar --numeric-owner --same-owner --same-permissions -xf - -C ' + shlex.quote(str(WORK / 'raw-final-stage'))
        consumer = subprocess.Popen([*TRANSPORT, 'root@114.32.64.6', command], stdin=compressor.stdout,
                                    stdout=subprocess.DEVNULL, stderr=errors)
        compressor.stdout.close()
        try:
            producer.stdin.write(b''.join(r['relative'].encode() + b'\0' for r in rows))
            producer.stdin.close()
            codes = [consumer.wait(timeout=600), compressor.wait(timeout=30), producer.wait(timeout=30)]
            if any(codes):
                raise SnapshotError('raw source transport failed; source/stage retained')
        finally:
            for process in (consumer, compressor, producer):
                if process.poll() is None:
                    process.terminate()
                    process.wait(timeout=30)
    for row in rows:
        if signature((source / row['relative']).lstat()) != row['source_signature']:
            raise SnapshotError('C raw source changed during delivery')
    promote = '''
import os,stat,ctypes
from pathlib import Path
from stockagent.data_sync.desync_snapshots import sha256_file,atomic_write_json
from stockagent.data_sync.materialized_cache import process_references
'''
    promote += inspect.getsource(rename_no_replace)
    promote += f'''
work=Path({str(WORK)!r})
stage=work/'raw-final-stage'
destination=Path({str(CANONICAL / 'raw')!r})
legacy=Path({str(LEGACY)!r})
intent=json.loads((work/'raw-source-restore-intent.json').read_text())
actual=[]
for p in stage.rglob('*'):
 if p.is_symlink() or not (p.is_dir() or p.is_file()):raise RuntimeError('raw reconstruction has redirected/unsupported names')
 if p.is_file():actual.append(p.relative_to(stage).as_posix())
if set(actual)!=set(r['relative'] for r in intent['rows']):raise RuntimeError('raw source complete-name set differs')
for row in intent['rows']:
 p=stage/row['relative']
 if p.stat().st_size!=row['bytes'] or sha256_file(p)!=row['sha256']:raise RuntimeError('raw source bytes differ')
if process_references(stage) or legacy.exists() or legacy.is_symlink():raise RuntimeError('stage in use or previous compatibility path exists')
destination.parent.mkdir(parents=True,exist_ok=True)
if destination.parent.stat().st_dev!=stage.stat().st_dev:raise RuntimeError('source rename would cross filesystem')
rename_no_replace(stage,destination)
os.symlink(str(destination.parent),legacy,target_is_directory=True)
for row in intent['rows']:
 if sha256_file(legacy/'raw'/row['relative'])!=row['sha256']:raise RuntimeError('receipt compatibility does not resolve exact source')
result={{'state':'all_bound_raw_sources_restored_at_canonical_data_root','files':len(intent['rows']),
 'bytes':intent['bytes'],'destination':str(destination),'compatibility_alias':str(legacy),
 'source_manifest_sha256':intent['source_manifest_sha256'],'final_manifest_sha256':intent['final_manifest_sha256'],
 'raw_bytes_under_artifacts':0,'cold_deleted':False,'existing_files_overwritten':0}}
atomic_write_json(work/'raw-source-restored.json',result)
print(json.dumps(result))
'''
    result = remote(promote)
    atomic_write_json(RECEIPTS / 'raw-source-restored-on-vast.json', result)
    print(json.dumps(result), flush=True)


if __name__ == '__main__':
    main()
