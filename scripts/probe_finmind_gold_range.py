"""One GoldPrice timestamp-boundary probe; never write the production queue."""

import argparse
from collections import Counter
from datetime import UTC, datetime
import hashlib
import json
import os
from pathlib import Path

import pyarrow.parquet as pq
import requests

from downloader.artifact_io import atomic_write_bytes, atomic_write_json
from downloader.common import load_env_file
from downloader.download_finmind_complement import SourceError, _fetch_rows, _sha256
from downloader.finmind_account import backfill_budget, rate_limiter
from downloader.finmind_scheduling import fixed_incremental_demand


DATASET = "GoldPrice"
START = "2016-01-01"
END = "2018-01-02"
MAX_BYTES = 64 * 1024 * 1024


def _stamp(row):
    value = row.get('date')
    if not isinstance(value, str):
        raise ValueError('missing source timestamp')
    stamp = datetime.fromisoformat(value)
    if stamp.tzinfo is not None:
        raise ValueError('unexpected source timestamp timezone')
    return stamp


def compare_gold_rows(local_rows, response_rows):
    """Check full provider rows; account explicitly for old Dec-31 truncation."""
    start, end = datetime.fromisoformat(START), datetime.fromisoformat(END)
    local_end_day = [row for row in local_rows if _stamp(row).date() == end.date()]
    local = [row for row in local_rows if start <= _stamp(row) <= end]
    columns = sorted({key for row in local for key in row})
    actual_columns = sorted({key for row in response_rows for key in row})
    raw_encode = lambda row: tuple(row.get(key) for key in columns)
    raw_missing = Counter(raw_encode(row) for row in local) - Counter(raw_encode(row) for row in response_rows)
    # Mixed daily/intraday responses render date-only values as midnight.
    # Normalize only the comparison key; retain both original payloads verbatim.
    encode = lambda row: tuple(_stamp(row).isoformat() if key == 'date' else row.get(key) for key in columns)
    expected = Counter(encode(row) for row in local)
    actual = Counter(encode(row) for row in response_rows)
    missing = expected - actual
    added = actual - expected
    added_rows = [dict(zip(columns, key)) for key, count in added.items() for _ in range(count)]
    repairs = [row for row in added_rows if _stamp(row).month == 12 and _stamp(row).day == 31
               and _stamp(row) > _stamp(row).replace(hour=0, minute=0, second=0, microsecond=0)]
    end_rows = [row for row in response_rows if _stamp(row).date() == end.date()]
    outside = [row for row in response_rows if not start <= _stamp(row) <= end]
    midnight_present = any(_stamp(row) == end for row in end_rows)
    upper_verified = (len(local_end_day) > 1 and midnight_present and len(end_rows) == 1 and not outside)
    schema_equal = columns == actual_columns
    return {
        'local_columns': columns, 'response_columns': actual_columns,
        'full_column_schema_equal': schema_equal,
        'verified_local_overlap_rows': len(local), 'missing_local_rows': sum(missing.values()),
        'raw_date_rendering_mismatch_rows': sum(raw_missing.values()),
        'comparison_date_normalization': 'parse_naive_timestamp_date_only_is_midnight_no_raw_mutation',
        'additional_rows': len(added_rows), 'annual_final_day_repair_rows': len(repairs),
        'annual_final_day_repairs': dict(Counter(str(_stamp(row).year) for row in repairs)),
        'unexpected_extra_rows': len(added_rows) - len(repairs),
        'response_rows': len(response_rows), 'response_end_day_rows': len(end_rows),
        'verified_local_end_day_rows': len(local_end_day),
        'response_end_day_timestamps': [row['date'] for row in end_rows],
        'outside_requested_timestamp_rows': len(outside),
        'timestamp_midnight_inclusive_upper_verified': upper_verified,
        'normalized_old_rows_preserved': not missing,
        'raw_old_rows_preserved': not raw_missing,
        'contract_verified': (upper_verified and schema_equal and not missing
                              and len(added_rows) == len(repairs)),
        'source_timezone': 'not_declared_naive_provider_timestamp',
        'upstream_completeness_proven': False,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--replay-payload', type=Path,
                        help='Recheck one hash-named private response locally; sends zero API calls')
    args = parser.parse_args()
    repo = Path(__file__).resolve().parents[1]
    root = repo / 'data_finmind'
    complement = root / 'complement'
    now = datetime.now(UTC)
    report = {'observed_at_utc': now.isoformat(), 'dataset': DATASET,
              'documentation_url': 'https://finmind.github.io/tutor/Materials/',
              'production_queue_writes': 0, 'max_data_requests': 1, 'data_requests': 0,
              'account_api_requests': 0, 'max_decoded_response_bytes': MAX_BYTES,
              'params': {'dataset': DATASET, 'start_date': START, 'end_date': END}}
    try:
        local_rows, proofs = [], []
        for year in ('2016', '2017', '2018'):
            receipt_path = complement / 'receipts' / DATASET / 'all' / f'{year}.json'
            receipt = json.loads(receipt_path.read_text())
            path = (complement / receipt['parquet_path']).resolve()
            if (not path.is_relative_to(complement.resolve()) or receipt.get('status') != 'complete'
                    or receipt.get('dataset') != DATASET or receipt.get('partition') != year
                    or path.stat().st_size != receipt['parquet_size_bytes']
                    or _sha256(path) != receipt['sha256']):
                raise ValueError('local source proof mismatch')
            rows = pq.read_table(path).to_pylist()
            local_rows.extend(rows)
            proofs.append({'year': year, 'receipt_path': str(receipt_path.relative_to(root)),
                           'receipt_sha256': _sha256(receipt_path), 'parquet_sha256': receipt['sha256'],
                           'parquet_size_bytes': receipt['parquet_size_bytes'], 'rows': len(rows)})
        report['local_proofs'] = proofs
        if args.replay_payload is not None:
            target = (root / args.replay_payload).resolve()
            if (not target.is_relative_to((root / 'diagnostics/range_responses').resolve())
                    or _sha256(target) != target.stem):
                raise ValueError('private response hash or path mismatch')
            raw = target.read_bytes()
            rows = json.loads(raw)
            report.update({'replayed_local_payload': True,
                           'payload_path': str(target.relative_to(root)), 'payload_sha256': target.stem,
                           'payload_bytes': len(raw), 'comparison': compare_gold_rows(local_rows, rows)})
        else:
            # A fresh canonical account receipt avoids spending a second API call.
            account = json.loads((root / 'account_status.json').read_text())
            budget = backfill_budget(account, root, fixed_incremental_requests=fixed_incremental_demand(root, now), now=now)
            report['admission'] = budget
            if not budget['allowed']:
                report['halted'] = budget['basis']
            else:
                load_env_file(repo / '.env', allowed_names=('FINMIND_TOKEN',))
                token = os.environ.get('FINMIND_TOKEN', '').strip()
                if not token:
                    raise ValueError('missing configured token')
                with requests.Session() as session:
                    report['data_requests'] = 1
                    rows = _fetch_rows(session, rate_limiter(account), root, DATASET, token, report['params'],
                                       max_response_bytes=MAX_BYTES)
                raw = json.dumps(rows, sort_keys=True, ensure_ascii=False, separators=(',', ':')).encode()
                digest = hashlib.sha256(raw).hexdigest()
                target = root / 'diagnostics' / 'range_responses' / f'{digest}.json'
                if target.exists():
                    if _sha256(target) != digest:
                        raise ValueError('existing content addressed response corrupt')
                else:
                    atomic_write_bytes(target, raw, durable=True)
                report.update({'payload_path': str(target.relative_to(root)), 'payload_sha256': digest,
                               'payload_bytes': len(raw), 'comparison': compare_gold_rows(local_rows, rows)})
    except SourceError as exc:
        report['error_code'] = exc.code
    except (OSError, ValueError, TypeError, KeyError) as exc:
        report['verification_error_type'] = type(exc).__name__
    report['completed_at_utc'] = datetime.now(UTC).isoformat()
    target = repo / 'artifacts' / 'data_quality' / f"finmind_gold_range_{now.strftime('%Y%m%dT%H%M%S%fZ')}.json"
    atomic_write_json(target, report)
    print(json.dumps({'report': str(target), **report}, ensure_ascii=False))
    return 0 if report.get('comparison', {}).get('contract_verified') else 1


if __name__ == '__main__':
    raise SystemExit(main())
