"""Bounded ABBA of the canonical durable FinMind ledger, with no API calls."""
from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from datetime import UTC, datetime
import json
from pathlib import Path
import sqlite3
import statistics
import tempfile
import threading
import time
from unittest.mock import patch

from downloader import download_finmind_free as worker
from downloader.artifact_io import atomic_write_json, sha256_file


def benchmark(*, workers=16, per_worker=32):
    if not 1 <= workers <= 32 or not 1 <= per_worker <= 64:
        raise ValueError('bounded ledger workload required')
    original = worker._write_request_start
    source = Path(worker.__file__).resolve()
    before = sha256_file(source)
    stamp = datetime.now(UTC)
    trials = []
    expected = Counter({(worker._iso(stamp), 'initial'): 1})
    expected.update({(worker._iso(stamp), str(index)): per_worker for index in range(workers)})
    with tempfile.TemporaryDirectory(prefix='finmind-ledger-abba-') as directory, \
            patch.object(worker, '_utc_now', return_value=stamp):
        for index, mode in enumerate(('baseline', 'candidate', 'candidate', 'baseline')):
            root = Path(directory) / str(index)
            root.mkdir()
            worker._record_request_start(root, 'initial')
            gate = threading.Lock()
            barrier = threading.Barrier(workers)
            active = peak = 0

            def observed(*args, **kwargs):
                nonlocal active, peak
                with gate:
                    active += 1
                    peak = max(peak, active)
                try:
                    return original(*args, **kwargs)
                finally:
                    with gate:
                        active -= 1

            def record(lane):
                barrier.wait(timeout=10)
                for _ in range(per_worker):
                    if mode == 'baseline':
                        observed(root, str(lane), timeout=worker.TRAFFIC_BUSY_TIMEOUT_SECONDS)
                    else:
                        worker._record_request_start(root, str(lane))

            wall, cpu = time.perf_counter(), time.process_time()
            with patch.object(worker, '_write_request_start', observed), ThreadPoolExecutor(workers) as pool:
                list(pool.map(record, range(workers)))
            elapsed, processor = time.perf_counter()-wall, time.process_time()-cpu
            with closing(sqlite3.connect(root / 'request_traffic.sqlite3')) as conn:
                actual = Counter(dict(((stamp, lane), count) for stamp, lane, count in conn.execute(
                    'SELECT started_at_utc,dataset,count(*) FROM requests GROUP BY 1,2')))
                assert conn.execute('PRAGMA journal_mode').fetchone()[0] == 'wal'
                assert conn.execute('PRAGMA synchronous').fetchone()[0] == 2
            assert actual == expected and active == 0
            trials.append(dict(mode=mode, requests=workers*per_worker, seconds=elapsed,
                               cpu_seconds=processor, peak_local_sqlite_writers=peak,
                               complete_ledger_values_equal=True))
    return dict(schema_version=1, observed_at_utc=datetime.now(UTC).isoformat(), trials=trials,
                means={mode: {field: statistics.mean(row[field] for row in trials if row['mode'] == mode)
                              for field in ('seconds', 'cpu_seconds')}
                       for mode in ('baseline', 'candidate')},
                exact_output_parity=True, source_code_sha256=before,
                code_stable=before == sha256_file(source), provider_calls=0, production_writes=0,
                caveats=['Small disposable WAL fixture, not a native API saturation proof.',
                         'Both sides close every connection and retain FULL pre-request durability.'])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error('new output required')
    result = benchmark()
    atomic_write_json(args.output, result)
    print(json.dumps({key: result[key] for key in ('exact_output_parity', 'code_stable', 'means')}))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
