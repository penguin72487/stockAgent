"""Read-only pinned comparison of exact stored URL-head statistics queries."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import sqlite3
import statistics
import time

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.news_fulltext_queue import utc_now


def benchmark(root, repetitions=9):
    db = sqlite3.connect(f"file:{(root / 'queue.sqlite3').resolve()}?mode=ro", uri=True)
    try:
        db.execute('BEGIN')
        owner = db.execute("SELECT value FROM queue_settings WHERE key='current_owner_id'").fetchone()
        oracle = dict(db.execute('SELECT provider,count(*) FROM articles WHERE body_sha256 IS NOT NULL GROUP BY provider'))
        actual = dict(db.execute('SELECT provider,heads FROM stored_body_metrics WHERE heads!=0'))
        measurements, outputs = {'scan': [], 'incremental': []}, {}
        statements = {'scan': 'SELECT count(*) FROM articles WHERE body_sha256 IS NOT NULL',
                      'incremental': 'SELECT coalesce(sum(heads),0) FROM stored_body_metrics'}
        for repetition in range(repetitions):
            for name in (('scan', 'incremental') if repetition % 2 == 0 else ('incremental', 'scan')):
                wall, cpu = time.perf_counter(), time.process_time()
                outputs[name] = db.execute(statements[name]).fetchone()[0]
                measurements[name].append({'wall_seconds': time.perf_counter()-wall, 'cpu_seconds': time.process_time()-cpu})
        ratios = {}
        for unit in ('wall_seconds', 'cpu_seconds'):
            before = statistics.median(m[unit] for m in measurements['scan'])
            after = statistics.median(m[unit] for m in measurements['incremental'])
            ratios[unit] = {'scan_median': before, 'incremental_median': after, 'speedup': before/after if after else None}
        return {'accepted': oracle == actual and outputs['scan'] == outputs['incremental'] == sum(oracle.values()),
                'observed_at_utc': utc_now(), 'owner_id': owner[0] if owner else None,
                'stored_url_heads': outputs['incremental'], 'per_provider_oracle': oracle, 'per_provider_incremental': actual,
                'snapshot_identity_sha256': hashlib.sha256(json.dumps(oracle, sort_keys=True).encode()).hexdigest(),
                'repetitions': repetitions, 'ratios': ratios, 'measurements': measurements,
                'scope': 'same WAL snapshot, stored-head query only; not total service CPU or throughput', 'publish': False}
    finally:
        db.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path('data_news_fulltext'))
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--repeat', type=int, default=9)
    args = parser.parse_args(argv)
    if not 3 <= args.repeat <= 31:
        parser.error('bounded repeated query count required')
    result = benchmark(args.root, args.repeat)
    result['implementation_sha256'] = {str(p): sha256_file(p) for p in (
        Path('downloader/news_fulltext_queue.py'), Path(__file__))}
    atomic_write_json(args.output, result)
    print(json.dumps({k: result[k] for k in ('accepted', 'stored_url_heads', 'ratios', 'scope')}))
    return 0 if result['accepted'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
