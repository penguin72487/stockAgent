"""Read-only acceptance of deployed calendar/market-day history efficiency.

Checks the live queue, one newly downloaded byte-verified receipt, and the
public metadata projection. Does not issue FinMind requests or mutate queues.
"""
from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
from pathlib import Path
import sqlite3

import pyarrow.parquet as pq
import requests

from downloader import download_finmind_complement as worker
from downloader.artifact_io import atomic_write_json
from downloader.finmind_history_calendar import load_closures
from downloader.finmind_supplemental import CONTRACT_VERSION
from downloader.finmind_eta import SNAPSHOT_CONTRACT_VERSION


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path('data_finmind'))
    parser.add_argument('--since', type=datetime.fromisoformat, required=True)
    parser.add_argument('--base-url', default='http://127.0.0.1:8770')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.since.tzinfo is None:
        parser.error('--since requires timezone')
    root = args.root.resolve()
    complement = root / 'complement'
    with sqlite3.connect((complement / 'queue.sqlite3').as_uri() + '?mode=ro', uri=True, timeout=3) as conn:
        # One coherent queue snapshot for proof and migration checks.
        conn.execute('BEGIN')
        closures = load_closures(conn)
        assert len(closures.days) > 0 and len(closures.receipt_sha256 or '') == 64
        assert 'TaiwanStockPriceTick' in closures.datasets
        assert not ({'TaiwanFuturesTick', 'TaiwanOptionTick', 'TaiwanFuturesSpreadTick'} & closures.datasets)
        frontiers = dict(conn.execute(
            "SELECT dataset,COUNT(*) FROM finmind_source_frontiers WHERE data_id='' GROUP BY dataset"))
        assert frontiers.get('TaiwanFuturesSpreadTick') == 1
        legacy_pending = conn.execute(
            "SELECT COUNT(*) FROM tasks WHERE dataset='TaiwanFuturesSpreadTick' AND data_id!='' "
            "AND state!='deprecated_query_shape'").fetchone()[0]
        assert legacy_pending == 0
        migrated = conn.execute(
            "SELECT COUNT(*) FROM finmind_query_shape_migrations WHERE dataset='TaiwanFuturesSpreadTick'"
        ).fetchone()[0]
        assert migrated > 0
        task = conn.execute(
            "SELECT dataset,data_id,partition,rows,receipt_path,last_attempt_at_utc FROM tasks "
            "WHERE dataset IN ('TaiwanStockKBar','TaiwanFuturesKBar','TaiwanStockTradingDailyReport') "
            "AND state='complete' AND rows>0 AND last_attempt_at_utc>=? ORDER BY last_attempt_at_utc DESC LIMIT 1",
            (args.since.astimezone(UTC).isoformat(),)).fetchone()
        assert task is not None, 'no_new_nonempty_receipt_after_deployment'
        delegated = worker._sponsor_delegated(complement, datetime.now(UTC))
        error_states: dict[str, int] = {}
        retained_delegated_errors: dict[str, int] = {}
        for dataset, state, count in conn.execute(
                "SELECT dataset,state,COUNT(*) FROM tasks WHERE state IN ('failed','not_entitled','invalid_request') "
                "GROUP BY dataset,state"):
            target = retained_delegated_errors if dataset in delegated else error_states
            target[state] = target.get(state, 0) + count
    path = (complement / task[4]).resolve()
    assert path.is_relative_to(complement)
    receipt = json.loads(path.read_bytes())
    parquet = (complement / receipt['parquet_path']).resolve()
    assert parquet.is_relative_to(complement)
    assert worker._sha256(parquet) == receipt['sha256']
    assert receipt['rows'] == task[3] == pq.read_metadata(parquet).num_rows
    assert receipt['request']['supplemental_contract_version'] == CONTRACT_VERSION
    response = requests.get(args.base_url.rstrip('/') + '/finmind/api/status', timeout=20)
    response.raise_for_status()
    public = response.json()
    acquisition = public['acquisition']
    assert public['read_only'] and not public['production_control_possible']
    assert acquisition['materialized_tasks'] + acquisition['unseeded_candidate_tasks'] == acquisition['total_tasks']
    tick = next(row for row in public['datasets'] if row['id'] == 'TaiwanStockPriceTick')
    assert tick['historical_frontier']['excluded_calendar_candidates'] > 0
    eta = json.loads((root / 'eta_status.json').read_bytes())
    assert eta['snapshot_contract_version'] == SNAPSHOT_CONTRACT_VERSION
    from downloader.finmind_eta import SNAPSHOT_CONTRACT_VERSION
    assert eta['estimate']['schema_version'] == SNAPSHOT_CONTRACT_VERSION
    assert eta['estimate']['workload']['planned_requests'] == eta['workload']['summary']['current_plan_requests']
    report = {
        'schema_version': 1, 'observed_at_utc': datetime.now(UTC).isoformat(), 'passed': True,
        'deployment_since_utc': args.since.astimezone(UTC).isoformat(),
        'calendar': {'receipt_sha256': closures.receipt_sha256, 'first': closures.first, 'last': closures.last,
                     'closed_dates': len(closures.days), 'datasets': sorted(closures.datasets)},
        'spread_tick': {'active_market_frontiers': 1, 'archived_legacy_tasks': migrated, 'legacy_pending': 0},
        'new_nonempty_receipt': {'dataset': task[0], 'data_id': task[1], 'partition': task[2],
                                'rows': task[3], 'attempt_at_utc': task[5], 'sha256': receipt['sha256'],
                                'parquet_bytes': parquet.stat().st_size, 'contract_version': CONTRACT_VERSION},
        'queue_error_states': error_states, 'retained_delegated_error_states': retained_delegated_errors,
        'public_health': public['health'],
        'public_generated_at_utc': public['generated_at_utc'],
        'eta_snapshot_contract': SNAPSHOT_CONTRACT_VERSION, 'planned_requests': eta['workload']['summary']['current_plan_requests'],
        'provider_calls': 0, 'queue_writes': 0, 'all_history_complete_claim': False,
    }
    atomic_write_json(args.output, report)
    print({'passed': True, 'new_receipt_dataset': task[0], 'rows': task[3], 'output': str(args.output)})
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
