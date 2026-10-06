#!/usr/bin/env python3
"""Expose only the byte-identical current rebuild; no overwrite or activation."""
import argparse
import inspect
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.deliver_vast_futures_preparation import rename_no_replace
from scripts.restore_vast_futures_raw_sources import remote, RECEIPTS, WORK
from stockagent.data_sync.desync_snapshots import atomic_write_json


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    original = json.loads((ROOT / 'artifacts/operations/futures_margin_prepare_20261004/remote_build_acceptance.json').read_text())
    expected = {k: original[k] for k in ('builder_sha256', 'source_manifest_sha256',
                                        'daily_sha256', 'rules_sha256', 'products', 'rows')}
    code = '''
import os,ctypes
from pathlib import Path
from stockagent.data_sync.desync_snapshots import SnapshotError,atomic_write_json,sha256_file
from stockagent.data_sync.materialized_cache import process_references
'''
    code += inspect.getsource(rename_no_replace)
    code += f'''
root=Path('/root/stockAgent')
work=Path({str(WORK)!r})
expected={expected!r}
apply={args.apply!r}
source=real(work/'current-build-restored-sources')
target=real(root/'artifacts/markets/tw_futures_v8_margin_preparation/margin_components_current')
receipt=work/'current-rebuild-verified.json'
proof=json.loads(receipt.read_text())
if (proof['state']!='same_release_contents_regenerated_verified' or proof['expected']!=expected
    or proof['output']!=str(source)):raise SnapshotError('original release identity differs')
if not recovery_hold_references(target,root):raise SnapshotError('explicit recovery hold missing')
if target.exists() or target.is_symlink():raise FileExistsError('current path already exists; no overwrite')
for key,path in [('daily_sha256',source/'release/daily/continuous_daily.parquet'),
                 ('rules_sha256',source/'release/rules/rules.parquet')]:
 if sha256_file(path)!=expected[key] or proof['observed'][key]!=expected[key]:
  raise SnapshotError('original release bytes differ')
baseline=metadata_tree(source)
if process_references(source):raise SnapshotError('current build still has readers/writers')
result={{'state':'byte_identical_current_rebuild_would_promote','expected':expected,
 'source':str(source),'destination':str(target),'receipt_sha256':sha256_file(receipt),
 'fingerprint':baseline['fingerprint'],'existing_files_overwritten':0,
 'cold_deleted':False,'model_activation':False}}
if apply:
 if process_references(source) or metadata_tree(source)!=baseline:raise SnapshotError('build changed before exposure')
 rename_no_replace(source,target)
 if metadata_tree(target)['fingerprint']!=baseline['fingerprint']:raise SnapshotError('promoted build changed; preserve it for audit')
 fd=os.open(target.parent,os.O_RDONLY|os.O_DIRECTORY)
 try:os.fsync(fd)
 finally:os.close(fd)
 result['state']='byte_identical_current_release_restored_at_original_path'
 atomic_write_json(work/'current-release-promoted.json',result)
print(json.dumps(result))
'''
    result = remote(code)
    atomic_write_json(RECEIPTS / ('current-release-promoted.json' if args.apply else 'current-release-promotion-plan.json'), result)
    print(json.dumps(result), flush=True)


if __name__ == '__main__':
    main()
