"""Measure bounded complete broker derive/publication on verified local sources.

No provider calls or live queue writes. Compare exact stored values to accepted
reference receipts, with fsync/SHA/receipt publication included in wall time.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor
from datetime import UTC, datetime
import gc
import json
import multiprocessing
import os
from pathlib import Path
import resource
import tempfile
import time

import pyarrow as pa
import pyarrow.parquet as pq

from downloader.artifact_io import atomic_write_json
from downloader.parquet_integrity import parquet_receipt_error


def initialize(threads):
    pa.set_cpu_count(threads)
    pa.set_io_thread_count(threads)


def execute(source_root, output_root, partition):
    from downloader import download_finmind_complement as worker
    started = time.monotonic()
    task = worker.Task('TaiwanStockTradingDailyReportSecIdAgg', '', partition, 'derived', 7, 'pending')
    rows, metadata = worker._derive_broker_aggregate(Path(source_root), task)
    derived = time.monotonic()
    receipt = worker._store(Path(output_root), task, rows, datetime.now(UTC), request_metadata=metadata)
    return {'partition': partition, 'rows': len(rows), 'derive_seconds': derived-started,
            'total_seconds': time.monotonic()-started, 'receipt': receipt,
            'peak_rss_bytes': resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path('data_finmind/complement'))
    parser.add_argument('--partition', action='append', required=True)
    parser.add_argument('--candidate', action='append', default=[], help='CPU processes:Arrow threads')
    parser.add_argument('--repeat', type=int, default=1)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    source_root = args.root.resolve()
    candidates = [tuple(map(int, value.split(':'))) for value in args.candidate] or [(1, 1), (1, 4), (2, 2), (4, 1)]
    if not 1 <= args.repeat <= 4 or any(not 1 <= jobs <= 8 or not 1 <= threads <= 16 for jobs, threads in candidates):
        raise ValueError('invalid bounded candidates')
    references = {}
    source_ids = []
    for partition in args.partition:
        parent = json.loads((source_root / 'receipts/TaiwanStockTradingDailyReport/all' / f'{partition}.json').read_bytes())
        reference = json.loads((source_root / 'receipts/TaiwanStockTradingDailyReportSecIdAgg/all' / f'{partition}.json').read_bytes())
        if (parent.get('status') != 'complete' or reference.get('status') != 'complete'
                or reference.get('request', {}).get('parent_sha256') != parent['sha256']
                or parquet_receipt_error(source_root, parent) or parquet_receipt_error(source_root, reference)):
            raise ValueError('source/reference receipt mismatch')
        references[partition] = pq.read_table(source_root / reference['parquet_path'], use_threads=False)
        source_ids.append({'partition': partition, 'source_sha256': parent['sha256'], 'reference_sha256': reference['sha256'], 'rows': parent['rows']})
    measurements = []
    # Reverse the second pass to avoid always favoring the later warm-cache run.
    for repeat in range(args.repeat):
        for processes, threads in (candidates if repeat % 2 == 0 else reversed(candidates)):
            with tempfile.TemporaryDirectory(prefix='finmind-pipeline-bench-', dir=args.output.parent) as scratch:
                started = time.monotonic()
                with ProcessPoolExecutor(max_workers=processes, mp_context=multiprocessing.get_context('spawn'),
                                         initializer=initialize, initargs=(threads,)) as pool:
                    futures = [pool.submit(execute, str(source_root), str(Path(scratch) / str(index)), partition)
                               for index, partition in enumerate(args.partition)]
                    outputs = [future.result() for future in futures]
                elapsed = time.monotonic() - started
                equivalent = True
                for index, item in enumerate(outputs):
                    root = Path(scratch) / str(index)
                    if parquet_receipt_error(root, item['receipt']):
                        equivalent = False
                    observed = pq.read_table(root / item['receipt']['parquet_path'], use_threads=False)
                    equivalent &= references[item['partition']].equals(observed)
                measurement = {'processes': processes, 'arrow_threads': threads, 'repeat': repeat,
                               'wall_seconds': elapsed, 'partitions_per_hour': len(outputs)*3600/elapsed,
                               'exact_values_equivalent': equivalent, 'jobs': outputs}
                measurements.append(measurement)
                print(json.dumps({key: value for key, value in measurement.items() if key != 'jobs'}), flush=True)
                gc.collect()
    accepted = all(item['exact_values_equivalent'] for item in measurements)
    report = {'schema_version': 1, 'state': 'accepted' if accepted else 'rejected',
              'scope': 'full verified read/Decimal derive/durable Parquet/SHA/receipt; excludes HTTP; warm-cache balanced trials',
              'observed_at_utc': datetime.now(UTC).isoformat(), 'cpu_affinity': sorted(os.sched_getaffinity(0)),
              'provider_calls': 0, 'live_queue_writes': 0, 'sources': source_ids, 'measurements': measurements}
    atomic_write_json(args.output, report)
    return 0 if accepted else 1


if __name__ == '__main__':
    raise SystemExit(main())
