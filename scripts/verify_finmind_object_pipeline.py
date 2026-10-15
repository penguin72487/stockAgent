"""Bounded, read-only proof of accepted FinMind transport/CPU work after deployment.

No provider API, queue migration, source publication or synthetic coverage. Live
rates include transport, validation, mandatory derivation and durable publication.
"""
from __future__ import annotations

import argparse
from contextlib import closing
from datetime import UTC, datetime
import json
from pathlib import Path
import sqlite3
import time

import pyarrow as pa
import pyarrow.parquet as pq

from downloader.artifact_io import atomic_write_json
from downloader import finmind_object_pipeline as pipeline
from downloader.finmind_supplemental import aggregate_brokers
from downloader.parquet_integrity import parquet_receipt_error
from scripts.verify_finmind_progress_deployment import service_state, process_resources


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--baseline', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--root', type=Path, default=Path('data_finmind/complement'))
    parser.add_argument('--verify-values', action='store_true', help='Full row-oracle check of one newly accepted source day')
    args = parser.parse_args()
    before = json.loads(args.baseline.read_bytes())
    since = before['observed_at_utc']
    root = args.root.resolve()
    config = pipeline.settings()
    with closing(sqlite3.connect(f'file:{root / "queue.sqlite3"}?mode=ro', uri=True, timeout=10)) as connection:
        connection.execute('PRAGMA query_only=ON')
        # A coherent bounded read; the canonical owner remains the only writer.
        connection.execute('BEGIN')
        samples = [dict(requests=calls, seconds=seconds, raw_rows=rows,
                        observed_at_utc=stamp, accepted_partitions_per_hour=calls*3600/seconds)
                   for calls, seconds, rows, stamp in connection.execute(
            'SELECT requests,seconds,rows,observed_at_utc FROM finmind_pipeline_samples '
            'WHERE dataset=? AND configuration_sha256=? AND observed_at_utc>=? '
            'ORDER BY id DESC LIMIT 32', (pipeline.DATASET, config['configuration_sha256'], since))]
        claims = dict(connection.execute('SELECT dataset,count(*) FROM finmind_object_prefetch_claims GROUP BY dataset'))
        uk_exhausted = connection.execute("SELECT count(*) FROM tasks WHERE dataset='UKStockPrice' AND state='retry_exhausted'").fetchone()[0]
        partitions = [row[0] for row in connection.execute(
            "SELECT partition FROM tasks WHERE dataset=? AND data_id='' AND state='complete' "
            'AND last_attempt_at_utc>=? ORDER BY last_attempt_at_utc DESC LIMIT 128', (pipeline.DATASET, since))]
        failures = [dict(dataset=dataset, state=state, error_code=code, tasks=count) for dataset, state, code, count in connection.execute(
            "SELECT dataset,state,error_code,count(*) FROM tasks WHERE dataset IN (?,?) "
            "AND state IN ('failed','retry_exhausted') AND last_attempt_at_utc>=? "
            'GROUP BY dataset,state,error_code', (pipeline.DATASET, pipeline.DATASET + 'SecIdAgg', since))]
    assert len(samples) >= 2, 'need_two_accepted_pipeline_batches'
    assert all(sample['requests'] > 0 and sample['seconds'] > 0 for sample in samples)
    assert claims.get(pipeline.DATASET, 0) <= config['download_workers']
    assert claims.get(pipeline.DATASET + 'SecIdAgg', 0) <= config['derivation_workers'] * 2
    previous_exhausted = sum(row['tasks'] for row in before['errors']
                             if row['dataset'] == 'UKStockPrice' and row['state'] == 'retry_exhausted')
    assert uk_exhausted == previous_exhausted, 'terminal_uk_keys_changed'
    assert not failures, 'new_pipeline_failures'
    verified = []
    value_check = None
    for partition in partitions:
        source = json.loads((root / 'receipts' / pipeline.DATASET / 'all' / f'{partition}.json').read_bytes())
        child_path = root / 'receipts' / (pipeline.DATASET + 'SecIdAgg') / 'all' / f'{partition}.json'
        if source.get('request', {}).get('pipeline_configuration_sha256') != config['configuration_sha256'] or not child_path.is_file():
            continue  # Already accepted legacy parent or a still-running CPU tail.
        child = json.loads(child_path.read_bytes())
        if child.get('request', {}).get('pipeline_configuration_sha256') != config['configuration_sha256']:
            continue
        assert source['status'] == child['status'] == 'complete'
        assert source['source_bytes_preserved'] is True
        assert source['request']['request_count'] == 1 and child['request']['request_count'] == 0
        assert child['request']['parent_sha256'] == source['sha256']
        assert child['request']['aggregation_engine'] == 'arrow_decimal128_table_exact_v2'
        assert not parquet_receipt_error(root, source) and not parquet_receipt_error(root, child)
        verified.append(dict(partition=partition, raw_rows=source['rows'], derived_rows=child['rows'],
                             raw_bytes=source['parquet_size_bytes'], derived_bytes=child['parquet_size_bytes'],
                             source_sha256=source['sha256'], derived_sha256=child['sha256']))
        if args.verify_values and value_check is None:
            started = time.monotonic()
            batches = pq.ParquetFile(root / source['parquet_path']).iter_batches(batch_size=65536, use_threads=False)
            expected = pa.Table.from_pylist(aggregate_brokers(row for batch in batches for row in batch.to_pylist()))
            actual = pq.read_table(root / child['parquet_path'], use_threads=False)
            assert actual.equals(expected), 'production_derivation_differs_from_row_oracle'
            value_check = dict(partition=partition, raw_rows=source['rows'], derived_rows=child['rows'],
                               exact_all_values_equal=True, seconds=time.monotonic()-started)
            del expected, actual
        if len(verified) >= 32:
            break
    assert len(verified) >= min(32, sum(sample['requests'] for sample in samples)), 'accepted_parent_children_not_verified'
    unit = 'stockagent-finmind-complement.service'
    resources = service_state(unit)
    processes = process_resources(resources['MainPID'])['processes']
    # CPU children must never inherit the owner's SQLite writer descriptors.
    assert sum(bool(process['queue_descriptors']) for process in processes) == 1
    cgroup = Path('/sys/fs/cgroup/stockagent.slice/stockagent-heavy.slice/stockagent-heavy-data.slice') / unit
    memory_events = dict(line.split() for line in (cgroup / 'memory.events').read_text().splitlines())
    assert not any(int(memory_events[key]) for key in ('high', 'oom', 'oom_kill'))
    total_seconds = sum(sample['seconds'] for sample in samples)
    total_calls = sum(sample['requests'] for sample in samples)
    proof = dict(schema_version=1, state='accepted', observed_at_utc=datetime.now(UTC).isoformat(),
                 configuration=config, accepted_pipeline_batches=samples,
                 accepted_partitions_per_hour=total_calls*3600/total_seconds,
                 raw_rows_per_second=sum(sample['raw_rows'] for sample in samples)/total_seconds,
                 verified_source_child_receipts=verified, exact_production_value_check=value_check,
                 active_claims=claims, uk_retry_exhausted=uk_exhausted, new_failures=failures,
                 service=resources, processes=processes, memory_events=memory_events,
                 single_queue_owner=True, provider_calls=0, queue_writes=0,
                 all_history_complete_claim=False,
                 scope='Current broker-detail phase; full accepted batch wall, not an isolated CPU or quota rate')
    atomic_write_json(args.output, proof)
    print({key: proof[key] for key in ('state', 'accepted_partitions_per_hour', 'raw_rows_per_second', 'uk_retry_exhausted')})
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
