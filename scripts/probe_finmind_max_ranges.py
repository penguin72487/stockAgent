"""One bounded full-span verification per range source; no production writes."""

import argparse
from collections import Counter
from datetime import UTC, date, datetime, timedelta
import hashlib
import json
import os
from pathlib import Path
import sqlite3

import pyarrow.parquet as pq
import requests

from downloader.artifact_io import atomic_write_json
from downloader.common import load_env_file
from downloader.download_finmind_complement import SourceError, _fetch_rows, _sha256
from downloader.finmind_account import backfill_budget, rate_limiter, verified_account
from downloader.finmind_scheduling import SOURCES, TAIPEI, fixed_incremental_demand

DATASETS = (
    'TaiwanBusinessIndicator', 'CnnFearGreedIndex', 'TaiwanOptionVix',
    'TaiwanTotalExchangeMarginMaintenance', 'TaiwanStockCapitalReductionReferencePrice',
    'TaiwanStockSuspended', 'TaiwanStockConvertibleBondPutProvision',
    'TaiwanStockInfoWithWarrantSummary',
)
BOUNDARY_DATES = {
    'TaiwanStockCapitalReductionReferencePrice': date(2025, 12, 29),
    'TaiwanStockSuspended': date(2025, 12, 30),
    'TaiwanStockConvertibleBondPutProvision': date(2025, 12, 25),
}
REMAINING_RANGES = {
    'TaiwanStockMarginShortSaleSuspension': date(2025, 12, 30),
    'TaiwanStockDayTradingSuspension': date(2025, 12, 31),
    'TaiwanStockDispositionSecuritiesPeriod': date(2025, 12, 31),
}


def compare_rows(original, actual):
    """Compare provider column names before comparing the complete row multiset.

    None of DATASETS receives generated columns from annotate_stock_share_units;
    their stored Parquet columns are provider columns, so none are ignored.
    Parquet can fill a missing cell with null; column disappearance is a schema
    mismatch even when every old value happened to be null.
    """
    local_columns = {key for row in original for key in row}
    response_columns = {key for row in actual for key in row}
    columns = sorted(local_columns | response_columns)
    schema_equal = local_columns == response_columns
    rows_equal = (Counter(tuple(row.get(k) for k in columns) for row in original)
                  == Counter(tuple(row.get(k) for k in columns) for row in actual))
    return {
        'local_columns': sorted(local_columns),
        'response_columns': sorted(response_columns),
        'compared_columns': columns,
        'missing_response_columns': sorted(local_columns - response_columns),
        'unexpected_response_columns': sorted(response_columns - local_columns),
        'schema_equal': schema_equal,
        'rows_equal': rows_equal,
        'equal': schema_equal and rows_equal,
    }


def boundary_result(rows, start_day, end_day):
    dates = [date.fromisoformat(row['date'][:10]) for row in rows]
    at_end = sum(day == end_day for day in dates)
    outside = sum(day < start_day or day > end_day for day in dates)
    return {
        'rows': len(rows),
        'response_first_date': str(min(dates)) if dates else None,
        'response_last_date': str(max(dates)) if dates else None,
        'response_columns': sorted({key for row in rows for key in row}),
        'end_date_rows': at_end,
        'outside_request_rows': outside,
        'inclusive_end_verified': at_end > 0 and outside == 0,
        'upstream_total_rows_provided': False,
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument('--boundary-only', action='store_true',
                        help='At most three known-event end-date checks; no full-history probes')
    modes.add_argument('--remaining-ranges', action='store_true',
                       help='Three unbatched event ranges: full span, schema parity and end boundary')
    modes.add_argument('--settlement-contracts', action='store_true',
                       help='Four bounded explicit-product versus no-ID settlement checks')
    args = parser.parse_args(argv)
    repo = Path(__file__).resolve().parents[1]
    root = repo / 'data_finmind'
    load_env_file(repo / '.env', allowed_names=('FINMIND_TOKEN',))
    token = os.environ.get('FINMIND_TOKEN', '').strip()
    specs = {s.dataset: s for s in SOURCES}
    now = datetime.now(UTC)
    datasets = (tuple(BOUNDARY_DATES) if args.boundary_only else
                tuple(REMAINING_RANGES) if args.remaining_ranges else DATASETS)
    if args.settlement_contracts:
        datasets = ('TaiwanFuturesFinalSettlementPrice', 'TaiwanOptionFinalSettlementPrice')
    report = {'observed_at_utc': now.isoformat(), 'production_queue_writes': 0,
              'mode': ('inclusive_end_boundary' if args.boundary_only else
                       'remaining_ranges' if args.remaining_ranges else
                       'settlement_contracts' if args.settlement_contracts else 'full_span'),
              'comparison_contract_version': 2,
              'schema_comparison': 'exact_provider_column_names',
              'ignored_generated_columns': [],
              'max_data_requests': len(datasets) * (2 if args.settlement_contracts else 1), 'probes': []}
    with requests.Session() as session, sqlite3.connect(f'file:{root}/sponsor/queue.sqlite3?mode=ro', uri=True) as conn:
        account = verified_account(session, token, root)
        limiter = rate_limiter(account)
        queries = [(dataset, None) for dataset in datasets]
        if args.settlement_contracts:
            queries = [(dataset, product) for dataset, symbol in zip(datasets, ('TX', 'TXO'))
                       for product in (symbol, None)]
        for dataset, data_id in queries:
            now = datetime.now(UTC)
            budget = backfill_budget(account, root, fixed_incremental_requests=fixed_incremental_demand(root, now), now=now)
            if not budget['allowed']:
                report['halted'] = budget['basis']
                break
            # Closed periods avoid current-period publication changing during a
            # parity check. Future runs calculate this boundary, not a fixed year.
            spec = specs[dataset]
            today = now.astimezone(TAIPEI).date()
            end = (date(today.year, today.month, 1).toordinal() - 1 if spec.grain == 'month'
                   else date(today.year, 1, 1).toordinal() - 1)
            end_day = date.fromordinal(end)
            start_day = spec.first_date
            if args.boundary_only:
                end_day = BOUNDARY_DATES[dataset]
                start_day = end_day - timedelta(days=7)
            elif args.remaining_ranges:
                end_day = REMAINING_RANGES[dataset]
            params = {'dataset': dataset, 'start_date': str(start_day), 'end_date': str(end_day)}
            if data_id:
                params['data_id'] = data_id
            item = {'params': params, 'started_at_utc': now.isoformat()}
            try:
                rows = _fetch_rows(session, limiter, root, dataset, token, params,
                                   max_response_bytes=64 * 1024 * 1024)
                if args.settlement_contracts:
                    dates = [date.fromisoformat(row['date'][:10]) for row in rows]
                    raw = json.dumps(rows, sort_keys=True, ensure_ascii=False, separators=(',', ':')).encode()
                    digest = hashlib.sha256(raw).hexdigest()
                    payload_path = root / 'diagnostics' / 'range_responses' / f'{digest}.json'
                    # Private provenance cache enables a later canonical import
                    # without another API request. Never publish raw rows here.
                    from downloader.artifact_io import atomic_write_bytes
                    if not payload_path.exists():
                        atomic_write_bytes(payload_path, raw, durable=True)
                    field = 'futures_id' if dataset.startswith('TaiwanFutures') else 'option_id'
                    item.update({'rows': len(rows), 'response_columns': sorted({k for r in rows for k in r}),
                                 'response_first_date': str(min(dates)) if dates else None,
                                 'response_last_date': str(max(dates)) if dates else None,
                                 'outside_request_rows': sum(d < start_day or d > end_day for d in dates),
                                 'returned_ids': sorted({str(r.get(field)) for r in rows}),
                                 'payload_path': str(payload_path.relative_to(root)),
                                 'payload_sha256': digest, 'payload_bytes': len(raw),
                                 'completed_at_utc': datetime.now(UTC).isoformat()})
                    report['probes'].append(item)
                    continue
                if args.boundary_only:
                    item.update(boundary_result(rows, start_day, end_day))
                    item['completed_at_utc'] = datetime.now(UTC).isoformat()
                    report['probes'].append(item)
                    continue
                dates = [date.fromisoformat(row['date'][:10]) for row in rows]
                response_columns = sorted({key for row in rows for key in row})
                by_partition = {}
                for row in rows:
                    d = date.fromisoformat(row['date'][:10])
                    nominal = date(d.year, d.month if spec.grain == 'month' else 1, 1)
                    partition = str(max(nominal, spec.first_date))
                    by_partition.setdefault(partition, []).append(row)
                compared = matched = compared_rows = 0
                mismatched = []
                comparisons = []
                compared_columns = set()
                for part, receipt_path in conn.execute(
                    "SELECT partition,receipt_path FROM tasks WHERE dataset=? AND state='complete' "
                    "AND partition<=? AND priority!=0 ORDER BY partition", (dataset, str(end_day)),
                ):
                    receipt = json.loads((root / 'sponsor' / receipt_path).read_text())
                    parquet = root / 'sponsor' / receipt['parquet_path']
                    if _sha256(parquet) != receipt['sha256']:
                        raise ValueError('local proof mismatch')
                    original = [r for r in pq.read_table(parquet).to_pylist()
                                if r['date'][:10] <= str(end_day)]
                    actual = by_partition.get(part, [])
                    comparison = compare_rows(original, actual)
                    comparisons.append({'partition': part, **comparison})
                    compared_columns.update(comparison['compared_columns'])
                    equal = comparison['equal']
                    compared += 1
                    matched += equal
                    compared_rows += len(original)
                    if not equal:
                        mismatched.append(part)
                item.update({'rows': len(rows), 'response_first_date': str(min(dates)) if dates else None,
                             'response_last_date': str(max(dates)) if dates else None,
                             'returned_partitions': len(by_partition), 'verified_local_partitions': compared,
                             'matched_local_partitions': matched, 'verified_local_rows': compared_rows,
                             'mismatched_partitions': mismatched,
                             'response_columns': response_columns,
                             'compared_columns': sorted(compared_columns),
                             'partition_comparisons': comparisons,
                             'outside_request_rows': sum(d < spec.first_date or d > end_day for d in dates),
                             'oldest_requested_year_rows': sum(d.year == spec.first_date.year for d in dates),
                             'exact_available_local_overlap': compared > 0 and matched == compared,
                             'upstream_total_rows_provided': False})
                if args.remaining_ranges:
                    item.update(boundary_result(rows, start_day, end_day))
            except SourceError as exc:
                item['error_code'] = exc.code
                if exc.code in {'rate_limited','not_entitled','invalid_token','ip_banned'}:
                    report['halted'] = exc.code
            except (OSError, ValueError, TypeError, KeyError) as exc:
                item['verification_error_type'] = type(exc).__name__
            item['completed_at_utc'] = datetime.now(UTC).isoformat()
            report['probes'].append(item)
            if report.get('halted'):
                break
    prefix = 'finmind_range_boundaries' if args.boundary_only else 'finmind_max_ranges'
    target = repo / 'artifacts/data_quality' / f"{prefix}_{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')}.json"
    atomic_write_json(target, report)
    print(json.dumps({'report': str(target), **report}, ensure_ascii=False))


if __name__ == '__main__':
    main()
