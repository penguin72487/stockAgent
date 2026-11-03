#!/usr/bin/env python3
"""Compare one full-prefix source-window day to a native CSV oracle.

This verifies the derived reduction, not market eligibility or execution.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta, timezone
import json
from pathlib import Path
from zoneinfo import ZoneInfo

import polars as pl

from downloader.artifact_io import atomic_write_json, sha256_file
from scripts.audit_taifex_option_reference_oracle import native_reference
from scripts.build_taifex_option_reference_source_window import CONTRACT


def audit(window: Path, day: date) -> dict:
    manifest_path = window / 'manifest.json'
    manifest_sha = sha256_file(manifest_path)
    manifest = json.loads(manifest_path.read_bytes())
    if manifest.get('contract') != CONTRACT or manifest.get('training_ready') is not False:
        raise ValueError('a source-only reference window is required')
    data = Path(manifest['data_path'])
    policy = Path(manifest['policy_path'])
    if sha256_file(data) != manifest['data_sha256'] or sha256_file(policy) != manifest['policy_sha256']:
        raise ValueError('source-window data or candidate policy changed')
    doc = json.loads(policy.read_bytes())
    windows = {key: tuple(value['day_minutes']) for key, value in doc['products'].items()}
    sources = [row for row in manifest['source_pins'] if date.fromisoformat(row['date']) <= day]
    if not sources or sources[-1]['date'] != day.isoformat():
        raise ValueError('selected day is not an original source archive day')
    expected, counts = native_reference(sources, day, windows)
    selected = pl.read_parquet(data).filter(pl.col('decision_date') == day)
    comparison = {}
    for clock in ('08:45', '09:00'):
        observed = {}
        cutoff = datetime.combine(day, datetime.strptime(clock, '%H:%M').time(), ZoneInfo('Asia/Taipei'))
        rows = selected.filter(pl.col('decision_time') == clock)
        for row in rows.iter_rows(named=True):
            if row['available_by'] != row['trade_ts'] + timedelta(seconds=1) or row['available_by'] > cutoff:
                raise ValueError('observed reference violates the completed-second cutoff')
            key = tuple(row[name] for name in ('product_raw', 'series_raw', 'strike_decimal', 'right_raw'))
            if key in observed:
                raise ValueError('duplicate decision contract key')
            observed[key] = (row['reference_kind'], row['reference_price_decimal'],
                             row['trade_ts'].strftime('%Y%m%d%H%M%S'), tuple(row['source_hashes']))
        baseline = expected[clock]
        missing, extra = set(baseline) - set(observed), set(observed) - set(baseline)
        mismatched = [key for key in baseline.keys() & observed.keys() if baseline[key] != observed[key]]
        if missing or extra or mismatched:
            raise ValueError(f'{clock}: missing={len(missing)} extra={len(extra)} mismatched={len(mismatched)}')
        comparison[clock] = {'keys_compared': len(observed), 'missing': 0, 'extra': 0,
                             'kind_price_second_or_provenance_mismatch': 0}
    if (sha256_file(manifest_path) != manifest_sha or sha256_file(data) != manifest['data_sha256']
            or sha256_file(policy) != manifest['policy_sha256']):
        raise ValueError('source-window identities changed during oracle')
    return {'contract': 'taifex_option_reference_source_window_native_csv_oracle_v1',
            'observed_at_utc': datetime.now(timezone.utc).isoformat(), 'day': day.isoformat(),
            'window_manifest': str(manifest_path.resolve()), 'window_manifest_sha256': manifest_sha,
            'data_sha256': manifest['data_sha256'], 'source_archives_read': len(sources),
            'native_csv_counts': counts, 'decisions': comparison,
            'script_sha256': sha256_file(Path(__file__)), 'training_ready': False,
            'limitations': ['Same source originals; independent reduction does not prove tape completeness.',
                            'Reference keys may be expired or ineligible; neither quote nor account is certified.']}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--window', required=True, type=Path)
    parser.add_argument('--date', required=True, type=date.fromisoformat)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    if args.output.exists():
        raise FileExistsError('preserve earlier source-window audit')
    result = audit(args.window.resolve(), args.date)
    atomic_write_json(args.output, result)
    print(json.dumps({'output': str(args.output), 'decisions': result['decisions']}), flush=True)


if __name__ == '__main__':
    main()
