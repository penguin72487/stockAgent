#!/usr/bin/env python3
"""Move retained source bundles in place; preserve exact files and old readers.

Run after all source readers/builders have finished. Atomic same-filesystem
renames do not copy data. Old data_tw_futures paths remain compatibility links;
data_tw_index_futures/preparation_sources owns the actual observations/evidence.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.deliver_vast_futures_preparation import rename_no_replace
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json
from stockagent.data_sync.materialized_cache import process_references
from stockagent.data_sync.remote_legacy_return import metadata_tree, real

NAMES = ('margin_sources', 'margin_repair_pending', 'margin_repair_inputs')
FINAL_NAME = 'official_final_settlement_all_asset_classes_20260929'


def relocate(root: Path, work: Path, *, apply: bool) -> dict:
    root, work = real(root), real(work)
    destination_root = real(root / 'data_tw_index_futures/preparation_sources')
    source_root = real(root / 'data_tw_futures')
    if not source_root.is_dir() or not destination_root.parent.is_dir():
        raise SnapshotError('canonical live source roots are unavailable')
    plans = []
    paths = [(source_root / name, destination_root / name) for name in NAMES]
    final = root / 'artifacts/markets/tw_futures_v8_margin_preparation' / FINAL_NAME
    if final.exists() or final.is_symlink():
        paths.append((final, destination_root / FINAL_NAME))
    for source, destination in paths:
        if source.is_symlink():
            if source.resolve() != destination or not destination.is_dir():
                raise SnapshotError('existing source alias is not the canonical data root')
            plans.append({'source': str(source), 'destination': str(destination),
                          'state': 'already_moved', 'fingerprint': metadata_tree(destination)['fingerprint']})
            continue
        real(source)
        real(destination)
        if not source.is_dir() or destination.exists() or destination.is_symlink():
            raise SnapshotError('missing source or occupied canonical destination; no overwrite')
        if process_references(source):
            raise SnapshotError('source is currently in use; wait for the existing reader')
        baseline = metadata_tree(source)
        if any(r['kind'] == 'unsupported' or r.get('cross_filesystem') for r in baseline['rows']):
            raise SnapshotError('source contains unsupported or mounted content')
        if source.stat().st_dev != destination_root.parent.stat().st_dev:
            raise SnapshotError('source move would copy across filesystems')
        plans.append({'source': str(source), 'destination': str(destination),
                      'state': 'would_move_without_copy', 'fingerprint': baseline['fingerprint'],
                      'files': sum(r['kind'] == 'file' for r in baseline['rows']),
                      'logical_bytes': baseline['logical_bytes']})
    result = {'state': 'source_layout_plan', 'moves': plans, 'payload_copies_created': 0,
              'cold_deleted': False, 'original_receipts_changed': False}
    if not apply:
        return result
    work.mkdir(parents=True, mode=0o700, exist_ok=True)
    atomic_write_json(work / 'source-layout-intent.json', result)
    destination_root.mkdir(exist_ok=True)
    for row in plans:
        if row['state'] == 'already_moved':
            continue
        source, destination = Path(row['source']), Path(row['destination'])
        if (process_references(source)
                or metadata_tree(source)['fingerprint'] != row['fingerprint']):
            raise SnapshotError('source activity or contents changed; preserve remaining paths')
        rename_no_replace(source, destination)
        os.symlink(str(destination), source, target_is_directory=True)
        if metadata_tree(destination)['fingerprint'] != row['fingerprint']:
            raise SnapshotError('moved source identity changed; preserve data and audit paths')
        row['state'] = 'same_files_moved_and_old_path_resolves'
        atomic_write_json(work / 'source-layout-progress.json', result)
    for parent in [source_root, destination_root]:
        fd = os.open(parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    result.update(state='canonical_source_layout_migrated_without_data_copy',
                  canonical_root=str(destination_root))
    atomic_write_json(work / 'source-layout-verified.json', result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--remote-vast', action='store_true')
    args = parser.parse_args()
    work = Path('/var/lib/stockagent-futures-recovery/20261004')
    if args.remote_vast:
        import inspect
        from scripts.restore_vast_futures_raw_sources import remote
        code = '''
import ctypes,os
from pathlib import Path
from stockagent.data_sync.desync_snapshots import SnapshotError,atomic_write_json
from stockagent.data_sync.materialized_cache import process_references
'''
        code += inspect.getsource(rename_no_replace) + '\n' + inspect.getsource(relocate)
        code += f'\nNAMES={NAMES!r}\nFINAL_NAME={FINAL_NAME!r}\nprint(json.dumps(relocate(Path({str(ROOT)!r}),Path({str(work)!r}),apply={args.apply!r})))\n'
        result = remote(code)
        atomic_write_json(ROOT / 'artifacts/operations/futures_preparation_recovery_20261004/remote-source-layout-verified.json', result)
    else:
        result = relocate(ROOT, work, apply=args.apply)
    print(json.dumps(result), flush=True)


if __name__ == '__main__':
    main()
