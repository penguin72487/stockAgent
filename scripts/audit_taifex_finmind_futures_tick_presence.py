#!/usr/bin/env python3
"""Compare official positive futures contract-days with retained FinMind ticks.

This is a source-presence audit, not an execution, account, PIT or training
admission. An observation anywhere in a natural-day tick file does not prove
that the official day session or either opening clock has executable prices.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import json
from pathlib import Path
import sqlite3
import sys
import time

import polars as pl
import pyarrow.parquet as pq

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from downloader.artifact_io import atomic_write_json, sha256_file


CONTRACT = 'taifex_finmind_futures_contract_day_presence_v1'


def _safe_path(root: Path, relative: str) -> Path:
    path = (root / relative).resolve()
    if Path(relative).is_absolute() or not path.is_relative_to(root.resolve()):
        raise ValueError('unsafe provider payload path')
    return path


def _load_provider(root: Path, day: date) -> tuple[pl.DataFrame | None, dict]:
    receipt = root / 'receipts' / 'TaiwanFuturesTick' / 'all' / f'{day.isoformat()}.json'
    if not receipt.is_file():
        return None, {'status': 'receipt_missing'}
    try:
        raw = receipt.read_bytes()
        if len(raw) > 1024 * 1024:
            raise ValueError('oversized receipt')
        import hashlib
        receipt_sha = hashlib.sha256(raw).hexdigest()
        item = json.loads(raw)
        if (item.get('dataset') != 'TaiwanFuturesTick' or item.get('data_id') != ''
                or item.get('partition') != day.isoformat()):
            raise ValueError('receipt identity mismatch')
        meta = {'receipt_path': str(receipt), 'receipt_sha256': receipt_sha,
                'provider_status': item.get('status'), 'fetched_at_utc': item.get('fetched_at_utc')}
        if item.get('status') != 'complete':
            if receipt.read_bytes() != raw:
                raise ValueError('receipt changed during readback')
            return None, {'status': 'receipt_not_complete', **meta}
        payload = _safe_path(root, item['parquet_path'])
        before = payload.stat()
        identity = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns)
        if (type(item.get('rows')) is not int or item['rows'] <= 0
                or before.st_size != item.get('parquet_size_bytes')
                or pq.ParquetFile(payload).metadata.num_rows != item['rows']):
            raise ValueError('payload size or row count mismatch')
        frame = pl.read_parquet(payload, columns=['date', 'futures_id', 'contract_date', 'volume'])
        if frame.height != item['rows'] or frame.filter(
                pl.col('date').is_null() | (pl.col('date').cast(pl.String).str.slice(0, 10) != day.isoformat())
        ).height:
            raise ValueError('payload dates or row count mismatch')
        after = payload.stat()
        if ((after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns) != identity
                or sha256_file(payload) != item.get('sha256') or receipt.read_bytes() != raw):
            raise ValueError('payload or receipt changed, or hash mismatch')
        typed = frame.select(
            pl.col('futures_id').cast(pl.String).str.strip_chars().str.to_uppercase().alias('product'),
            pl.col('contract_date').cast(pl.String).str.strip_chars().str.to_uppercase().alias('contract'),
            'volume',
        )
        unkeyable = typed.filter(pl.col('product').is_null() | (pl.col('product') == '') |
                                 pl.col('contract').is_null() | (pl.col('contract') == '') |
                                 pl.col('volume').is_null()).height
        keys = typed.filter(pl.col('volume') > 0).drop_nulls().filter(
            (pl.col('product') != '') & (pl.col('contract') != '')
        ).select('product', 'contract').unique()
        return keys, {'status': 'verified_source_payload', **meta, 'payload_path': str(payload),
                      'payload_sha256': item['sha256'], 'provider_rows': frame.height,
                      'unkeyable_provider_rows': unkeyable}
    except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError, pl.exceptions.PolarsError) as exc:
        return None, {'status': 'receipt_or_payload_invalid', 'error': type(exc).__name__}


def audit(official_path: Path, expected_sha256: str, provider_root: Path, *,
          start: date, end: date) -> tuple[dict, list[dict], list[dict]]:
    if start > end or sha256_file(official_path) != expected_sha256:
        raise ValueError('invalid date range or official source digest')
    official = (pl.scan_parquet(official_path)
                .filter(pl.col('date').is_between(start, end) & (pl.col('volume') > 0)
                        & (pl.col('session') == '一般') & pl.col('series_type').is_in(['monthly', 'weekly']))
                .select('date', 'product', 'contract', 'series_type', 'session_reported')
                .with_columns(pl.col('product').str.strip_chars().str.to_uppercase(),
                              pl.col('contract').str.strip_chars().str.to_uppercase())
                .unique().collect())
    if official.filter(pl.col('product').is_null() | (pl.col('product') == '') |
                       pl.col('contract').is_null() | (pl.col('contract') == '')).height:
        raise ValueError('official positive rows contain an unkeyable contract')
    # Keep the source session-reporting property in diagnostics, not in the
    # match key; duplicates must not count as separate physical contracts.
    expected = (official.group_by('date', 'product', 'contract').agg(
        pl.col('series_type').unique().sort().str.join('|').alias('series_types'),
        pl.col('session_reported').all().alias('session_reported_all'),
    ).sort('date', 'product', 'contract'))
    db = (provider_root / 'queue.sqlite3').resolve().as_uri() + '?mode=ro'
    with sqlite3.connect(db, uri=True, timeout=5) as con:
        queue = {row[0]: {'queue_state': row[1], 'queue_error': row[2]}
                 for row in con.execute("SELECT partition,state,error_code FROM tasks "
                                        "WHERE dataset='TaiwanFuturesTick' AND data_id='' "
                                        "AND partition>=? AND partition<=?", (start.isoformat(), end.isoformat()))}
    daily, unmatched = [], []
    partitions = expected.partition_by('date', as_dict=True, maintain_order=True)
    for index, (key, group) in enumerate(partitions.items(), 1):
        day = key[0]
        keys, info = _load_provider(provider_root, day)
        result = {'date': day.isoformat(), 'official_positive_contracts': group.height,
                  **queue.get(day.isoformat(), {'queue_state': 'missing_task', 'queue_error': None}), **info}
        if keys is not None:
            missing = group.join(keys, on=['product', 'contract'], how='anti')
            result.update({'matched_contracts': group.height - missing.height,
                           'unmatched_contracts': missing.height})
            unmatched.extend({**row, 'date': day.isoformat()} for row in missing.to_dicts())
        daily.append(result)
        if index % 100 == 0:
            print(json.dumps({'state': 'auditing', 'processed_days': index,
                              'total_days': len(partitions)}, ensure_ascii=False), flush=True)
    if sha256_file(official_path) != expected_sha256:
        raise ValueError('official source changed during audit')
    summary = {'contract': CONTRACT, 'observed_at_utc': datetime.now(timezone.utc).isoformat(),
               'start': start.isoformat(), 'end': end.isoformat(), 'official_path': str(official_path),
               'official_sha256': expected_sha256, 'provider_root': str(provider_root),
               'official_positive_contract_days': expected.height, 'official_positive_dates': len(daily),
               'verified_provider_dates': sum(x['status'] == 'verified_source_payload' for x in daily),
               'provider_unavailable_or_invalid_dates': sum(x['status'] != 'verified_source_payload' for x in daily),
               'matched_contract_days_on_natural_date': sum(x.get('matched_contracts', 0) for x in daily),
               'unmatched_contract_days_on_verified_natural_dates': len(unmatched),
               'official_contract_days_without_verified_provider_payload': sum(
                   x['official_positive_contracts'] for x in daily if x['status'] != 'verified_source_payload'),
               'limitations': ['Positive official source rows are not a complete dated listing universe.',
                 'A natural-date tick match may belong to another session and proves no opening-clock fill.',
                 'Unmatched keys and unavailable files require source and contract-normalization review; neither proves no trading.',
                 'This audit neither verifies all ticks/prices, provider independence, PIT availability, execution nor accounting.']}
    return summary, daily, unmatched


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--official-manifest', type=Path, required=True)
    parser.add_argument('--provider-root', type=Path, required=True)
    parser.add_argument('--start', type=date.fromisoformat, required=True)
    parser.add_argument('--end', type=date.fromisoformat, required=True)
    parser.add_argument('--output-dir', type=Path, required=True)
    args = parser.parse_args()
    if args.output_dir.exists() and any(args.output_dir.iterdir()):
        raise ValueError('refusing to overwrite an existing audit')
    raw = args.official_manifest.read_bytes()
    import hashlib
    manifest_sha = hashlib.sha256(raw).hexdigest()
    manifest = json.loads(raw)
    source = manifest['all_futures_daily']
    started = time.monotonic()
    summary, daily, unmatched = audit(Path(source['path']), source['sha256'], args.provider_root,
                                      start=args.start, end=args.end)
    if args.official_manifest.read_bytes() != raw:
        raise ValueError('official manifest changed during audit')
    args.output_dir.mkdir(parents=True, exist_ok=True)
    daily_path, unmatched_path = args.output_dir / 'days.json', args.output_dir / 'unmatched.json'
    atomic_write_json(daily_path, daily)
    atomic_write_json(unmatched_path, unmatched)
    summary.update({'official_manifest_path': str(args.official_manifest),
                    'official_manifest_sha256': manifest_sha, 'audit_script_sha256': sha256_file(Path(__file__)),
                    'days_path': str(daily_path), 'days_sha256': sha256_file(daily_path),
                    'unmatched_path': str(unmatched_path), 'unmatched_sha256': sha256_file(unmatched_path),
                    'elapsed_seconds': round(time.monotonic() - started, 3)})
    atomic_write_json(args.output_dir / 'summary.json', summary)
    print(json.dumps(summary, ensure_ascii=False), flush=True)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
