"""Zero-API acceptance of FinMind query optimization, cached probes and receipts."""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path
import sqlite3

import pyarrow.parquet as pq

from downloader.artifact_io import atomic_write_json
from downloader.parquet_integrity import parquet_receipt_error
from scripts.audit_finmind_query_ranges import registry


DATASET = 'TaiwanStockConvertibleBondMonthlyAnalysis'


def multiset(rows):
    return Counter(json.dumps(row, ensure_ascii=False, sort_keys=True) for row in rows)


def verify_ranges(root: Path, report_dir: Path) -> dict:
    probes = {}
    for name in ('contract_probes.json', 'range_boundary_probes.json'):
        for item in json.loads((report_dir / name).read_bytes())['probes']:
            probes[item['name']] = item

    def load(name):
        item = probes[name]
        path = (root / item['payload_path']).resolve()
        if not path.is_relative_to(root.resolve()):
            raise ValueError('probe_path_outside_root')
        raw = path.read_bytes()
        if hashlib.sha256(raw).hexdigest() != item['payload_sha256']:
            raise ValueError('probe_payload_hash_mismatch')
        return json.loads(raw)

    rows = load('bond_month_all')
    single = load('bond_month_one')
    boundary = load('bond_month_boundary')
    # Test an interior boundary: using the latest returned month cannot prove
    # the provider honored end_date rather than silently ignoring it.
    first, end = '2026-05-01', '2026-07-01'
    matches = multiset([r for r in rows if r['cb_id'] == '13166']) == multiset(single)
    subset = [row for row in rows if first <= row['date'] <= end]
    end_matches = (multiset(boundary) == multiset(subset)
                   and any(row['date'] == end for row in boundary)
                   and any(row['date'] > end for row in rows))
    result = {
        'schema_version': 1, 'observed_at_utc': datetime.now(UTC).isoformat(),
        'dataset': DATASET, 'verification_api_requests': 0,
        'documentation_url': 'https://finmind.github.io/tutor/TaiwanMarket/ConvertibleBond/',
        'optional_data_id_contract': 'https://api.finmindtrade.com/openapi.json',
        'query_shape': 'whole_market_inclusive_date_range',
        'per_id_all_column_multiset_equal': matches, 'inclusive_end_verified': end_matches,
        'whole_market_rows': len(rows), 'known_returned_ids': len({r['cb_id'] for r in rows}),
        'observed_first_date': min(r['date'] for r in rows), 'observed_last_date': max(r['date'] for r in rows),
        'historical_point_in_time': False, 'provider_completeness_independently_verified': False,
        'probe_payload_sha256': {name: probes[name]['payload_sha256'] for name in
                                 ('bond_month_all', 'bond_month_one', 'bond_month_boundary')},
    }
    result['accepted'] = matches and end_matches
    atomic_write_json(report_dir / 'range_acceptance.json', result)
    return result


def runtime(root: Path) -> dict:
    owner_root = root / 'complement'
    with sqlite3.connect((owner_root / 'queue.sqlite3').resolve().as_uri() + '?mode=ro', uri=True) as conn:
        states = dict(conn.execute('SELECT state,count(*) FROM tasks WHERE dataset=? GROUP BY state', (DATASET,)))
        head = conn.execute("SELECT state,rows,receipt_path,next_attempt_at_utc FROM tasks "
                            "WHERE dataset=? AND data_id='' AND partition='history'", (DATASET,)).fetchone()
        migrated = conn.execute('SELECT count(*) FROM finmind_query_shape_migrations WHERE dataset=?', (DATASET,)).fetchone()[0]
    receipt = json.loads((owner_root / head[2]).read_bytes()) if head and head[2] else {}
    error = parquet_receipt_error(owner_root, receipt) if receipt else 'no_receipt'
    rows = pq.read_table(owner_root / receipt['parquet_path']).to_pylist() if error is None else []
    request = receipt.get('request', {})
    accepted = bool(head and head[0] == 'complete' and head[1] == len(rows) and rows
                    and receipt.get('dataset') == DATASET and receipt.get('data_id') == ''
                    and receipt.get('partition') == 'history'
                    and request.get('query_shape') == 'whole_market_inclusive_date_range'
                    and request.get('supplemental_contract_version') == 2 and error is None)
    return {'observed_at_utc': datetime.now(UTC).isoformat(), 'dataset': DATASET, 'queue_states': states,
            'accepted': accepted, 'known_returned_ids': len({row['cb_id'] for row in rows}),
            'observed_month_rows': dict(sorted(Counter(row['date'] for row in rows).items())),
            'migrated_per_id_tasks': migrated, 'per_id_tasks_marked_downloaded_by_migration': 0,
            'remaining_market_history_tasks': int(head[0] not in {'complete', 'observed_empty'}) if head else None,
            'receipt_integrity_error': error,
            'receipt': {key: receipt.get(key) for key in ('status', 'rows', 'source_first_date', 'source_last_date',
                                                        'sha256', 'fetched_at_utc', 'request')},
            'next_attempt_at_utc': head[3] if head else None,
            'registered_query_shape': registry()[DATASET]['query_shape']}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path('data_finmind'))
    parser.add_argument('--report-dir', type=Path, default=Path('artifacts/data_quality/finmind_call_efficiency_2026-09-30'))
    parser.add_argument('--runtime', action='store_true')
    args = parser.parse_args(argv)
    if args.runtime:
        result = runtime(args.root)
        atomic_write_json(args.report_dir / 'runtime_acceptance.json', result)
        print({key: result[key] for key in ('dataset', 'queue_states', 'migrated_per_id_tasks', 'receipt_integrity_error')})
        if not result['accepted']:
            return 1
    else:
        result = verify_ranges(args.root, args.report_dir)
        print({key: result[key] for key in ('dataset', 'accepted', 'whole_market_rows', 'known_returned_ids')})
        if not result['accepted']:
            return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
