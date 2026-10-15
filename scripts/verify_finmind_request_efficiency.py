"""Read-only acceptance of the FinMind JSON owner, quota and source partitions."""
from __future__ import annotations

import argparse
from contextlib import closing
from datetime import UTC, datetime, timedelta
import json
from pathlib import Path
import sqlite3

import pyarrow.parquet as pq

from downloader.artifact_io import atomic_write_json
from downloader import finmind_json_pipeline as pipeline
from downloader.finmind_supplemental import frontier_status, validate_response
from downloader.parquet_integrity import parquet_receipt_error
from scripts.verify_finmind_progress_deployment import service_state, process_resources, fatal_log_counts


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--baseline', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--root', type=Path, default=Path('data_finmind/complement'))
    args = parser.parse_args(argv)
    before = json.loads(args.baseline.read_bytes())
    now = datetime.now(UTC)
    since = before['observed_at_utc']
    root = args.root.resolve()
    config = pipeline.settings()
    dataset = 'USStockPriceMinute'
    with closing(sqlite3.connect((root / 'queue.sqlite3').as_uri() + '?mode=ro', uri=True, timeout=10)) as conn:
        conn.execute('PRAGMA query_only=ON')
        conn.execute('BEGIN')
        samples = [dict(requests=calls, attributable_cycle_seconds=seconds, fetch_validate_publish_seconds=fetch,
                        rows=rows, nonempty_requests=nonempty, observed_at_utc=stamp,
                        cycle_id=cycle_id, full_cycle_seconds=wall, measured_other_work_seconds=other,
                        cycle_started_at_utc=started, cycle_finished_at_utc=finished,
                        accepted_requests_per_hour=calls*3600/seconds)
                   for calls, seconds, fetch, rows, nonempty, stamp, cycle_id, wall, other, started, finished in conn.execute(
            'SELECT requests,seconds,fetch_validate_publish_seconds,rows,nonempty_requests,observed_at_utc,'
            'cycle_id,cycle_wall_seconds,excluded_work_seconds,cycle_started_at_utc,cycle_finished_at_utc '
            'FROM finmind_json_pipeline_samples WHERE dataset=? AND configuration_sha256=? '
            'AND cycle_wall_verified=1 AND observed_at_utc>=? ORDER BY id DESC LIMIT 32',
            (dataset, config['configuration_sha256'], since))]
        measured = pipeline.measured_rates(conn, dataset, now)
        claims = dict(conn.execute('SELECT dataset,count(*) FROM finmind_object_prefetch_claims GROUP BY dataset'))
        states = dict(conn.execute('SELECT state,count(*) FROM tasks WHERE dataset=? GROUP BY state', (dataset,)))
        uk_exhausted = conn.execute("SELECT count(*) FROM tasks WHERE dataset='UKStockPrice' AND state='retry_exhausted'").fetchone()[0]
        keys = conn.execute('SELECT data_id,partition,rows,receipt_path FROM tasks '
                            "WHERE dataset=? AND state IN ('complete','observed_empty') AND last_attempt_at_utc>=? "
                            'ORDER BY last_attempt_at_utc DESC LIMIT 64', (dataset, since)).fetchall()
        errors = [dict(code=code, count=count) for code, count in conn.execute(
            "SELECT error_code,count(*) FROM tasks WHERE dataset=? AND state IN ('failed','invalid_request','retry_exhausted') "
            'AND last_attempt_at_utc>=? GROUP BY error_code', (dataset, since))]
        frontier = frontier_status(conn, now)[dataset]
    assert len(samples) >= 2 and measured, 'need_two_accepted_full_cycle_batches'
    cycles = {row['cycle_id']: row for row in samples if row['cycle_id']}
    assert len(cycles) >= 2, 'need_two_complete_owner_cycles'
    assert claims.get(dataset, 0) <= min(config['download_workers'], config['max_buffered_response_bytes']//(64*1024**2))
    prior_exhausted = sum(row['tasks'] for row in before['errors']
                          if row['dataset'] == 'UKStockPrice' and row['state'] == 'retry_exhausted')
    assert uk_exhausted >= prior_exhausted, 'terminal_uk_keys_were_reopened'
    assert not errors, 'new_json_pipeline_failure'
    receipts = []
    for identifier, day, count, relative in keys:
        receipt = json.loads((root / relative).read_bytes())
        request = receipt.get('request', {})
        if request.get('pipeline_configuration_sha256') != config['configuration_sha256']:
            continue
        assert request['request_count'] == 1 and request['json_pipeline_contract_version'] == pipeline.CONTRACT_VERSION
        assert receipt['data_id'] == identifier and receipt['partition'] == day and receipt['rows'] == count
        if count:
            assert not parquet_receipt_error(root, receipt)
            rows = pq.read_table(root / receipt['parquet_path'], use_threads=False).to_pylist()
            assert len(rows) == count
            validate_response(dataset, identifier, day, now.date(), rows)
        else:
            assert receipt['status'] == 'observed_empty', 'empty_is_not_complete'
        receipts.append({'data_id': identifier, 'partition': day, 'rows': count,
                         'status': receipt['status'], 'sha256': receipt.get('sha256')})
    assert len(receipts) >= 16, 'not_enough_actual_partition_receipts'
    unit = 'stockagent-finmind-complement.service'
    resources = service_state(unit)
    processes = process_resources(resources['MainPID'])['processes']
    assert sum(bool(p['queue_descriptors']) for p in processes) == 1, 'multiple_queue_writers'
    threads = 0
    for process in processes:
        try:
            threads += len(list(Path(f'/proc/{process["pid"]}/task').iterdir()))
        except OSError:
            pass
    assert threads < 128, 'limiter_dispatcher_thread_leak'
    logs = fatal_log_counts((unit,), datetime.fromisoformat(since))
    assert not any(logs.values())
    with closing(sqlite3.connect((root.parent / 'request_traffic.sqlite3').as_uri()+'?mode=ro', uri=True)) as traffic:
        actual = {str(minutes)+'m': dict(traffic.execute('SELECT dataset,count(*) FROM requests WHERE '
                  'started_at_utc>? AND started_at_utc<=? GROUP BY dataset',
                  ((now-timedelta(minutes=minutes)).isoformat(), now.isoformat()))) for minutes in (1, 5, 60)}
        cycle_traffic = [dict(cycle_id=row['cycle_id'], seconds=row['full_cycle_seconds'],
                             requests_by_dataset=dict(traffic.execute(
                                 'SELECT dataset,count(*) FROM requests WHERE started_at_utc>? AND started_at_utc<=? GROUP BY dataset',
                                 (row['cycle_started_at_utc'], row['cycle_finished_at_utc'])))) for row in cycles.values()]
    actual_cycle_requests = sum(sum(row['requests_by_dataset'].values()) for row in cycle_traffic)
    actual_cycle_seconds = sum(row['seconds'] for row in cycle_traffic)
    actual_cycle_rate = actual_cycle_requests*3600/actual_cycle_seconds
    account = json.loads((root.parent / 'account_status.json').read_bytes())
    report = dict(schema_version=1, observed_at_utc=now.isoformat(), accepted=True,
                  configuration=config, actual_full_cycles=samples, capacity=measured,
                  official_requests_per_hour=account['official_requests_per_hour'],
                  official_api_utilization=actual_cycle_rate/account['official_requests_per_hour'],
                  actual_cycle_requests_per_hour=actual_cycle_rate, actual_cycle_traffic=cycle_traffic,
                  queue_states=states, frontier=frontier, actual_request_starts=actual,
                  verified_receipts=receipts, uk_exhausted_tasks=uk_exhausted,
                  limiter_threads=threads, process_resources=processes,
                  fatal_log_counts=logs, provider_calls=0, queue_writes=0,
                  all_history_complete_claim=False, historical_identifier_lifetimes_verified=False)
    atomic_write_json(args.output, report)
    print({key: report[key] for key in ('accepted','official_api_utilization','limiter_threads')},
          {'requests_per_hour': measured['rates']['central'], 'samples': len(samples), 'verified_receipts': len(receipts)})
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
