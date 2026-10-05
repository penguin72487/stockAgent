#!/usr/bin/env python3
"""Independent full-SHA check of all aliases in the exact compaction receipt."""
import argparse
import json
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.ingest_remote_cold_artifacts import _ssh_base, _validate_ssh_target
from stockagent.data_sync.desync_snapshots import atomic_write_json


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--apply-receipt', type=Path, required=True)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--ssh-target', default='root@114.32.64.6')
    p.add_argument('--ssh-port', type=int, default=40032)
    p.add_argument('--identity-file', type=Path, default=Path('/root/.ssh/stockagent_vastai1t_ed25519'))
    args = p.parse_args()
    local = json.loads(args.apply_receipt.read_text())
    receipt = Path(local['receipt'])
    if receipt.parent != Path('/var/lib/stockagent-legacy-return/cache-compaction'):
        p.error('Exact private remote compaction receipt required')
    body = f'receipt_path={str(receipt)!r}\n' + '''
from pathlib import Path
import hashlib,json,os,time
root=Path('/root/stockAgent/artifacts/cache')
receipt=Path(receipt_path)
assert receipt.resolve()==receipt and not receipt.is_symlink()
value=json.loads(receipt.read_text());checked={};files=0
for row in value['replaced']:
 paths=[root/row[key] for key in ('canonical','path')]
 for path in paths:
  assert root in path.parents and path.resolve()==path and not path.is_symlink()
 left,right=paths
 a,b=left.lstat(),right.lstat()
 assert os.path.samestat(a,b) and a.st_ino==row['new_inode'] and a.st_size==row['size']
 key=(a.st_dev,a.st_ino)
 if key not in checked:
  h=hashlib.sha256()
  with left.open('rb') as source:
   while block:=source.read(8*1024*1024): h.update(block)
  after=left.lstat()
  fields=('st_dev','st_ino','st_size','st_mtime_ns','st_ctime_ns','st_mode','st_nlink')
  assert all(getattr(a,f)==getattr(after,f) for f in fields)
  checked[key]=h.hexdigest()
 assert checked[key]==row['sha256'] and left.samefile(right)
 assert right.stat().st_mtime_ns==row['new_mtime_ns']
 files+=1
fs=os.statvfs(root)
print(json.dumps({'state':'all_compacted_cache_aliases_independently_sha_verified',
 'aliases_verified':files,'unique_inodes_fully_hashed':len(checked),
 'reclaimed_allocated_bytes':value['reclaimed_allocated_bytes'],
 'logical_paths_removed':value['logical_paths_removed'],
 'unique_payloads_removed':value['unique_payloads_removed'],
 'available_bytes':fs.f_bavail*fs.f_frsize,'observed_at_epoch':time.time()}),flush=True)
'''
    command = [*_ssh_base(args.identity_file, args.ssh_port), _validate_ssh_target(args.ssh_target),
               'cd /root/stockAgent && source scripts/runtime_env.sh && run_fintech_python -']
    result = subprocess.run(command, input=body, text=True, capture_output=True, timeout=1800)
    if result.returncode:
        atomic_write_json(args.output.with_suffix('.error.json'),
                          {'state':'independent_verification_failed', 'stderr':result.stderr,
                           'returncode':result.returncode, 'observed_at_epoch':time.time()})
        raise RuntimeError('Independent compaction verification failed; private receipt retained')
    value = json.loads(result.stdout.splitlines()[-1])
    atomic_write_json(args.output, value)
    print(json.dumps(value), flush=True)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
