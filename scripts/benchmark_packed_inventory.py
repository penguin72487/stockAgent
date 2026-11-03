"""ABBA exact inventory load/validate/query benchmark; no payload acceptance.

Uses the canonical reader and validator on one private synthetic inventory.
Child processes separate allocator/RSS history. Only aggregate evidence stays.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import gzip
import hashlib
import json
from pathlib import Path
import resource
import statistics
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json
from stockagent.data_sync import packed_snapshots as packed


def trial(path: Path, adaptive: bool) -> dict:
    manifest = json.loads(path.read_bytes())
    started, cpu = time.perf_counter(), time.process_time()
    with packed._open_inventory(path.parent, manifest, adaptive_memory=adaptive) as rows:
        proof = packed._validate_inventory(manifest, rows)
        digest = hashlib.sha256()
        for row in rows:
            digest.update(json.dumps(row, sort_keys=True).encode() + b'\n')
        members = sum(1 for names in proof['object_members'].values() for _ in names)
        index_bytes = (Path(rows._scratch.name) / 'index.sqlite').stat().st_size if rows.spilled else 0
        result = {'adaptive': adaptive, 'rows': len(rows), 'members': members,
                  'spilled': rows.spilled, 'index_bytes': index_bytes,
                  'counts': proof['counts'], 'inventory_sha256': digest.hexdigest()}
    return {**result, 'wall_seconds': time.perf_counter() - started,
            'cpu_seconds': time.process_time() - cpu,
            'peak_rss_bytes': resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024}


def fixture(root: Path, count: int) -> Path:
    inventory = root / 'inventory.jsonl.gz'
    pack_sha = hashlib.sha256(b'private synthetic pack identity, no payload').hexdigest()
    with gzip.open(inventory, 'wt', encoding='utf-8') as stream:
        for number in range(count):
            name = f'observations/{number:09}.json'
            row = {'path': name, 'kind': 'file', 'mode': 0o644, 'size': 256,
                   'sha256': hashlib.sha256(name.encode()).hexdigest(),
                   'storage': {'kind': 'pack', 'object_sha256': pack_sha, 'member': name}}
            stream.write(json.dumps(row, separators=(',', ':')) + '\n')
    manifest = {'schema_version': 2,
        'source': {'files': count, 'directories': 0, 'symlinks': 0, 'logical_bytes': count * 256},
        'archive': {'inventory': {'relpath': inventory.name, 'bytes': inventory.stat().st_size,
                                'sha256': hashlib.sha256(inventory.read_bytes()).hexdigest()},
                    'objects': [{'kind': 'pack', 'sha256': pack_sha, 'bytes': count * 64,
                                 'file_count': count, 'logical_bytes': count * 256}]}}
    path = root / 'manifest.json'
    path.write_text(json.dumps(manifest))
    return path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--files', type=int, default=100000)
    parser.add_argument('--output', type=Path)
    parser.add_argument('--trial-manifest', type=Path)
    parser.add_argument('--adaptive', action='store_true')
    args = parser.parse_args()
    if args.trial_manifest:
        print(json.dumps(trial(args.trial_manifest, args.adaptive)))
        return
    if not args.output or not 1 <= args.files <= 1000000:
        parser.error('select --output and 1..1000000 fixture rows')
    code_paths = [Path(__file__), ROOT / 'stockagent/data_sync/packed_metadata.py',
                  ROOT / 'stockagent/data_sync/packed_snapshots.py']
    before = {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in code_paths}
    with tempfile.TemporaryDirectory(prefix='stockagent-inventory-benchmark-', dir='/var/tmp') as work:
        path = fixture(Path(work), args.files)
        trials = []
        for adaptive in (False, True, True, False):
            command = [sys.executable, __file__, '--trial-manifest', str(path)]
            if adaptive:
                command.append('--adaptive')
            result = subprocess.run(command, check=True, capture_output=True, text=True)
            trials.append(json.loads(result.stdout))
            print(json.dumps(trials[-1]), flush=True)
    for row in trials:
        if (row['inventory_sha256'], row['counts'], row['members']) != (
                trials[0]['inventory_sha256'], trials[0]['counts'], trials[0]['members']):
            raise ValueError('complete inventory and object membership parity failed')
    if before != {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in code_paths}:
        raise ValueError('benchmark implementation changed')
    means = {label: statistics.mean(t['wall_seconds'] for t in trials if t['adaptive'] == flag)
             for label, flag in [('spilled', False), ('adaptive', True)]}
    report = {'contract': 'packed_inventory_exact_abba_v1', 'state': 'verified',
              'observed_at_utc': datetime.now(timezone.utc).isoformat(),
              'scope': 'complete synthetic inventory loading, validation and queries; no payload restore',
              'trials': trials, 'mean_seconds': means,
              'observed_speedup': means['spilled'] / means['adaptive'], 'code_sha256': before,
              'provider_calls': 0, 'source_files_deleted': 0, 'production_payload_acceptance': False}
    atomic_write_json(args.output, report)
    print(json.dumps({'observed_speedup': report['observed_speedup'], 'mean_seconds': means}))


if __name__ == '__main__':
    main()
