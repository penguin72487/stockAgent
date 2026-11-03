#!/usr/bin/env python3
"""Compare receipted current-year daily option opens with FinMind day objects.

This reuses the existing official contract parser and vendor receipt audit.
Keys are source-presence candidates, not proof of a decision-clock fill or PIT.
"""
from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import csv
from datetime import date, datetime, timezone
import hashlib
import io
import json
from pathlib import Path

from downloader.artifact_io import atomic_write_bytes, atomic_write_json, sha256_file
from scripts.audit_taifex_option_contract_presence import KEYS, audit_day, official_year
from scripts.audit_taifex_option_daily_tick_coverage import receipt_status
from scripts.taifex_daily_download_common import month_ranges


def audit(summary_path: Path, root: Path, workers: int = 4) -> tuple[list[dict], list[dict], dict]:
    if not 1 <= workers <= 16:
        raise ValueError('workers must be in 1..16')
    summary_bytes = summary_path.read_bytes()
    reference = json.loads(summary_bytes)
    if reference.get('contract') != 'tw_option_all_product_monthly_range_readback_v1':
        raise ValueError('monthly source readback contract mismatch')
    start, end = date.fromisoformat(reference['start']), date.fromisoformat(reference['end'])
    if start.year != end.year or start > end or reference.get('months_verified') != len(reference['sources']):
        raise ValueError('monthly source scope mismatch')
    expected_ranges = [(first.isoformat(), last.isoformat()) for first, last in month_ranges(start, end)]
    if [(item['start'], item['end']) for item in reference['sources']] != expected_ranges:
        raise ValueError('monthly source ranges are not contiguous')
    root = root.resolve()
    daily, stats = {}, []
    previous = None
    for source in reference['sources']:
        first, last = date.fromisoformat(source['start']), date.fromisoformat(source['end'])
        if first.year != start.year or not start <= first <= last <= end or (previous and first <= previous):
            raise ValueError('overlapping or out-of-scope source ranges')
        previous = last
        path, receipt_path = Path(source['path']), Path(source['receipt'])
        if any(item.is_symlink() or not item.is_file() for item in (path, receipt_path)):
            raise ValueError('missing or redirected monthly source')
        receipt_bytes = receipt_path.read_bytes()
        receipt = json.loads(receipt_bytes)
        if (receipt.get('contract') != 'taifex_option_all_products_daily_raw_v1'
                or receipt.get('range_start') != first.isoformat() or receipt.get('range_end') != last.isoformat()
                or receipt.get('point_in_time_snapshot') is not False or receipt.get('path') != str(path)
                or receipt.get('sha256') != source['sha256'] or receipt.get('bytes') != source['bytes']
                or path.stat().st_size != source['bytes']):
            raise ValueError('monthly receipt identity differs from verified source')
        days, official_stats = official_year(path, source['sha256'], start.year)
        if any(not first <= date.fromisoformat(day) <= last or day in daily for day in days):
            raise ValueError('official contracts outside their monthly source')
        if receipt_path.read_bytes() != receipt_bytes:
            raise ValueError('monthly receipt changed during readback')
        daily.update(days)
        stats.append({'start': first.isoformat(), 'end': last.isoformat(),
                      'official_open_dates': len(days), **official_stats})
        print(json.dumps(stats[-1]), flush=True)
    if not daily or reference['sources'][0]['start'] != start.isoformat() or previous != end:
        raise ValueError('incomplete monthly source range')

    def inspect(day: str) -> tuple[dict, list[dict]]:
        receipt = receipt_status(date.fromisoformat(day), root, verify_payload_sha=True)
        row = {'date': day, 'finmind_status': receipt['status'],
               'finmind_receipt_path': receipt.get('receipt_path', ''),
               'finmind_receipt_sha256': receipt.get('receipt_sha256', ''),
               'finmind_parquet_path': receipt.get('parquet_path', ''),
               'finmind_parquet_sha256': receipt.get('parquet_sha256', '')}
        result, missing = audit_day(row, root, daily[day])
        return {**row, **result}, missing

    with ThreadPoolExecutor(max_workers=workers) as pool:
        readbacks = list(pool.map(inspect, sorted(daily)))
    if summary_path.read_bytes() != summary_bytes:
        raise ValueError('monthly source readback changed during comparison')
    rows = [item[0] for item in readbacks]
    missing = [entry for _, entries in readbacks for entry in entries]
    return rows, missing, {
        'contract': 'taifex_option_monthly_open_contract_vs_finmind_v2',
        'observed_at_utc': datetime.now(timezone.utc).isoformat(),
        'source_readback': str(summary_path.resolve()),
        'source_readback_sha256': hashlib.sha256(summary_bytes).hexdigest(),
        'finmind_root': str(root), 'audit_sha256': sha256_file(Path(__file__)),
        'official_parser_sha256': sha256_file(Path('scripts/audit_taifex_option_contract_presence.py')),
        'series_normalizer_sha256': sha256_file(Path('downloader/finmind_option_contract_keys.py')),
        'date_count': len(rows), 'source_states': dict(sorted(Counter(row['source_status'] for row in rows).items())),
        'verified_dates_with_missing_contracts': sum(row['missing_contracts'] > 0 for row in rows),
        'verified_missing_contract_day_keys': sum(row['missing_contracts'] for row in rows),
        'sources': stats,
        'limitations': ['Only positive daily opening and volume in day or unreported session are included.',
                        'Unreported sessions are not day-session proof; natural-day vendor keys can include night trades.',
                        'Contract presence or absence is not a decision-clock fill, PIT or tick-completeness proof.',
                        'Vendor monthly YYYYMM.0 is compared as YYYYMM; weekly series and other decimals are not converted.',
                        'Strike keys use vendor float32 precision; distinct original strikes could collide.']}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-readback', type=Path, required=True)
    parser.add_argument('--finmind-root', type=Path, default=Path('data_finmind/complement'))
    parser.add_argument('--workers', type=int, default=4)
    parser.add_argument('--output-dir', type=Path, required=True)
    args = parser.parse_args()
    rows, missing, summary = audit(args.source_readback, args.finmind_root, args.workers)
    args.output_dir.mkdir(parents=True, exist_ok=False)

    def write_csv(path: Path, items: list[dict], fields: list[str]) -> str:
        buf = io.StringIO(newline='')
        writer = csv.DictWriter(buf, fieldnames=fields)
        writer.writeheader()
        writer.writerows(items)
        content = buf.getvalue().encode()
        atomic_write_bytes(path, content)
        return hashlib.sha256(content).hexdigest()

    days = args.output_dir / 'option_contract_presence_by_day.csv'
    misses = args.output_dir / 'option_missing_contracts.csv'
    summary['day_inventory'] = str(days.resolve())
    summary['day_inventory_sha256'] = write_csv(days, rows, list(dict.fromkeys(k for row in rows for k in row)))
    summary['missing_inventory'] = str(misses.resolve())
    summary['missing_inventory_sha256'] = write_csv(misses, missing, ['date', *KEYS, 'session'])
    atomic_write_json(args.output_dir / 'option_contract_presence.summary.json', summary)
    print(json.dumps({'output': str(args.output_dir), 'dates': len(rows),
                      'missing_keys': summary['verified_missing_contract_day_keys']}), flush=True)


if __name__ == '__main__':
    main()
