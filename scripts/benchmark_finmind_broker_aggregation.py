"""Compare exact broker derivations on one existing receipt, without API calls."""
from __future__ import annotations

import argparse
import gc
import hashlib
import json
import os
import statistics
from pathlib import Path
import time

import pyarrow.parquet as pq

from downloader.artifact_io import atomic_write_json
from downloader.finmind_broker_aggregation import FIELDS, aggregate_brokers_columnar
from downloader.finmind_supplemental import aggregate_brokers
from downloader.parquet_integrity import parquet_receipt_error


def fingerprint(rows):
    digest = hashlib.sha256()
    for row in rows:
        digest.update(json.dumps(row, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode())
        digest.update(b'\n')
    return digest.hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path('data_finmind/complement'))
    parser.add_argument('--receipt', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--repeat', type=int, choices=range(1, 8), default=3)
    args = parser.parse_args()
    os.nice(10)
    receipt = json.loads(args.receipt.read_bytes())
    if receipt.get('status') != 'complete' or parquet_receipt_error(args.root, receipt):
        raise ValueError('verified parent receipt required')
    source = args.root / receipt['parquet_path']
    def rows():
        for batch in pq.ParquetFile(source).iter_batches(batch_size=65536, use_threads=False):
            yield from batch.to_pylist()
    measured = {}
    for name, execute in (
        ('reference', lambda: aggregate_brokers(rows())),
        ('columnar', lambda: aggregate_brokers_columnar(pq.read_table(source, columns=list(FIELDS), use_threads=False))),
    ):
        started = time.monotonic()
        output = execute()
        elapsed = time.monotonic() - started
        if output is None:
            raise ValueError('source required reference fallback; not a speed acceptance')
        measured[name] = {'seconds': elapsed, 'rows': len(output), 'sha256': fingerprint(output)}
        del output
        gc.collect()
    equivalent = measured['reference']['sha256'] == measured['columnar']['sha256']
    # Preserve the cold comparison above. Balanced repetitions distinguish
    # Arrow's first-use initialization from recurring work on small responses.
    trials = []
    for repeat in range(args.repeat):
        for name in (('reference', 'columnar', 'columnar', 'reference') if repeat % 2 == 0
                     else ('columnar', 'reference', 'reference', 'columnar')):
            started = time.monotonic()
            output = (aggregate_brokers(rows()) if name == 'reference' else
                      aggregate_brokers_columnar(pq.read_table(source, columns=list(FIELDS), use_threads=False)))
            seconds = time.monotonic() - started
            if output is None or fingerprint(output) != measured['reference']['sha256']:
                raise ValueError('steady output differs from exact reference')
            trials.append({'mode': name, 'seconds': seconds, 'rows': len(output)})
            del output
            gc.collect()
    medians = {name: statistics.median(row['seconds'] for row in trials if row['mode'] == name)
               for name in ('reference', 'columnar')}
    if parquet_receipt_error(args.root, receipt):
        raise ValueError('parent source changed during paired benchmark')
    report = {'schema_version': 1, 'state': 'accepted' if equivalent else 'rejected',
              'scope': 'one full source partition read plus exact aggregation; excludes HTTP and output publication',
              'source_sha256': receipt['sha256'], 'source_rows': receipt['rows'],
              'provider_calls': 0, 'source_mutations': 0, 'measurements': measured,
              'exact_output_equivalent': equivalent,
              'speedup': measured['reference']['seconds'] / measured['columnar']['seconds'],
              'steady_trials': trials, 'steady_median_seconds': medians,
              'steady_speedup': medians['reference'] / medians['columnar'],
              'source_unchanged': True, 'single_source_not_full_provider_claim': True}
    atomic_write_json(args.output, report)
    print(json.dumps(report, ensure_ascii=False), flush=True)
    return 0 if equivalent else 1


if __name__ == '__main__':
    raise SystemExit(main())
