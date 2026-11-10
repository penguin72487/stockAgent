#!/usr/bin/env python3
"""Reclaim only old, unused, reproducible torch graph cache files."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import stat
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json
from stockagent.data_sync.materialized_cache import process_references_many

CONTRACT = 'stockagent.stale_torch_graph_cache_retirement.v1'
DEFAULT_ROOT = Path('/root/.cache/torchinductor/fxgraph')


def census(root: Path, *, now: float, minimum_age_days=7, references=process_references_many) -> list[dict]:
    root = Path(root).absolute()
    if (type(minimum_age_days) is not int or minimum_age_days < 7 or root.is_symlink()
            or not root.is_dir() or root.resolve(strict=True) != root):
        raise SnapshotError('torch graph cache scope or age is invalid')
    cutoff = now-minimum_age_days*86400
    device = root.stat().st_dev
    candidates = []
    for directory in sorted(root.iterdir()):
        try:
            info = directory.lstat()
            if (not stat.S_ISDIR(info.st_mode) or info.st_dev != device or info.st_mtime >= cutoff
                    or directory.name.startswith('.')):
                continue
            files, eligible = [], True
            def walk_error(error):
                raise error
            for base, dirs, names in os.walk(directory, followlinks=False, onerror=walk_error):
                for item in dirs:
                    entry = (Path(base)/item).lstat()
                    if not stat.S_ISDIR(entry.st_mode) or entry.st_dev != device or entry.st_mtime >= cutoff:
                        eligible = False
                        break
                for item in names:
                    path = Path(base)/item
                    entry = path.lstat()
                    if (not stat.S_ISREG(entry.st_mode) or entry.st_dev != device or entry.st_nlink != 1
                            or entry.st_uid != os.geteuid() or entry.st_mtime >= cutoff or entry.st_atime >= cutoff):
                        eligible = False
                        break
                    files.append({'path': str(path), 'allocated_bytes': entry.st_blocks*512,
                                  'signature': [entry.st_dev, entry.st_ino, entry.st_mode, entry.st_uid,
                                                entry.st_size, entry.st_mtime_ns, entry.st_ctime_ns,
                                                entry.st_atime_ns, entry.st_nlink]})
                    if len(files) > 20000:
                        eligible = False
                        break
                if not eligible:
                    break
            if eligible and files:
                candidates.append({'directory': str(directory), 'files': files,
                                   'allocated_bytes': sum(item['allocated_bytes'] for item in files)})
        except OSError:
            continue  # Unknown or changing scope is not eligible.
    if candidates and references([Path(item['directory']) for item in candidates]):
        raise SnapshotError('a running process references the selected graph-cache scopes')
    return sorted(candidates, key=lambda item: (-item['allocated_bytes'], item['directory']))


def reclaim(root: Path, *, apply=False, now=None, minimum_age_days=7, max_bytes=2*1024**3,
            journal: Path | None = None, references=process_references_many) -> dict:
    if type(apply) is not bool or type(max_bytes) is not int or max_bytes <= 0:
        raise SnapshotError('torch graph cache retirement options are invalid')
    now = time.time() if now is None else now
    candidates = census(root, now=now, minimum_age_days=minimum_age_days, references=references)
    selected, total = [], 0
    for item in candidates:
        if total+item['allocated_bytes'] > max_bytes:
            continue
        selected.append(item)
        total += item['allocated_bytes']
    result = {'contract': CONTRACT, 'state': 'planned', 'observed_at_epoch': now,
              'minimum_age_days': minimum_age_days, 'candidate_directories': len(selected),
              'candidate_files': sum(len(item['files']) for item in selected),
              'candidate_allocated_bytes': total, 'removed_files': 0, 'reclaimed_allocated_bytes': 0,
              'source_data_deleted': False, 'training_output_deleted': False}
    if not apply:
        return result
    if journal is None or journal.exists() or journal.is_symlink():
        raise SnapshotError('torch graph cache needs a fresh journal')
    journal.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    if journal.parent.is_symlink() or journal.parent.resolve(strict=True) != journal.parent:
        raise SnapshotError('torch graph cache journal is redirected')
    result['state'] = 'removing'
    atomic_write_json(journal, result, durable=True)
    if selected and references([Path(item['directory']) for item in selected]):
        raise SnapshotError('graph cache became active before retirement')
    for item in selected:
        for entry in item['files']:
            path = Path(entry['path'])
            info = path.lstat()
            signature = [info.st_dev, info.st_ino, info.st_mode, info.st_uid, info.st_size,
                         info.st_mtime_ns, info.st_ctime_ns, info.st_atime_ns, info.st_nlink]
            if signature != entry['signature'] or path.is_symlink():
                raise SnapshotError('graph cache file changed before retirement')
            result['pending'] = entry['path']
            atomic_write_json(journal, result, durable=True)
            path.unlink()
            result['removed_files'] += 1
            result['reclaimed_allocated_bytes'] += entry['allocated_bytes']
            result.pop('pending', None)
            atomic_write_json(journal, result, durable=True)
    result.update(state='completed', completed_at_epoch=time.time())
    atomic_write_json(journal, result, durable=True)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--minimum-age-days', type=int, default=7)
    parser.add_argument('--max-bytes', type=int, default=2*1024**3)
    args = parser.parse_args()
    journal = (Path('/var/lib/stockagent-stale-graph-cache')/(str(time.time_ns())+'.json')
               if args.apply else None)
    print(json.dumps(reclaim(DEFAULT_ROOT, apply=args.apply, minimum_age_days=args.minimum_age_days,
                             max_bytes=args.max_bytes, journal=journal), sort_keys=True))
    return 0


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except (OSError, ValueError, SnapshotError) as error:
        print(f'{type(error).__name__}: {error}', file=sys.stderr)
        raise SystemExit(1)
