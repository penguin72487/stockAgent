#!/usr/bin/env python3
"""Native official-CSV checks for explicitly selected expiry-shift cases.

The examined daily bars and final settlements are retrospective. They cannot
be used as decision-time features, listing evidence, quotes, or fills.
"""
from __future__ import annotations

import argparse
import csv
from datetime import date, datetime, timezone
import io
import json
from pathlib import Path
import re
import sqlite3
from zipfile import ZipFile

import polars as pl

from downloader.artifact_io import atomic_write_json, sha256_file
from scripts.audit_txo_nominal_expiry_impact import _group_events, _strike_key


def _native_csv_keys(stream, *, day: date, series: str) -> tuple[set, set]:
    rows = csv.DictReader(stream)
    required = {'交易日期', '契約', '到期月份(週別)', '履約價', '買賣權', '成交量', '交易時段'}
    if rows.fieldnames is None or required - set(rows.fieldnames):
        raise ValueError('official CSV source columns missing')
    all_keys, positive = set(), set()
    for row in rows:
        if (row['契約'].strip().upper() != 'TXO' or row['到期月份(週別)'].strip() != series
                or row['交易時段'].strip() != '一般'):
            continue
        try:
            d = date(*(int(part) for part in row['交易日期'].strip().split('/')))
        except (TypeError, ValueError):
            raise ValueError('invalid official CSV date') from None
        if d != day:
            continue
        right = {'買權': 'C', '賣權': 'P'}.get(row['買賣權'].strip())
        if right is None:
            raise ValueError('invalid official option right')
        key = (_strike_key(row['履約價'].strip()), right)
        volume = row['成交量'].strip().replace(',', '')
        if not volume.isdigit():
            raise ValueError('invalid official option volume')
        all_keys.add(key)
        if int(volume) > 0:
            positive.add(key)
    return all_keys, positive


def audit(impact_path: Path, union_path: Path, monthly_manifest: Path,
          weekly_manifest: Path, cases: list[tuple[date, str]]) -> dict:
    paths = [Path(__file__).resolve(), Path('scripts/audit_txo_nominal_expiry_impact.py').resolve(),
             impact_path, union_path, monthly_manifest, weekly_manifest]
    pins = {str(path): sha256_file(path) for path in paths}
    impact = json.loads(impact_path.read_bytes())
    union = json.loads(union_path.read_bytes())
    if (impact.get('contract') != 'txo_nominal_expiry_source_impact_diagnostic_v1'
            or impact.get('training_ready') is not False
            or union.get('contract') != 'tw_option_source_union_manifest_v1'
            or union.get('training_ready') is not False):
        raise ValueError('source-only impact and union receipts required')
    impact_pins = impact.get('input_sha256', {})
    for path in (paths[1], union_path, monthly_manifest, weekly_manifest):
        if impact_pins.get(str(path)) != pins[str(path)]:
            raise ValueError('impact receipt does not bind the current input or implementation')
    db_path = Path(union['parent_database'])
    if sha256_file(db_path) != union['parent_sha256'] or impact_pins.get(str(db_path)) != union['parent_sha256']:
        raise ValueError('compact index differs from the source union')
    pins[str(db_path)] = union['parent_sha256']
    case_map = {(date.fromisoformat(row['observed_natural_date']), row['series']): row
                for row in impact['details']}
    if not cases or len(cases) != len(set(cases)) or any(case not in case_map for case in cases):
        raise ValueError('cases must be distinct observations from the impact receipt')

    manifests = {'monthly': json.loads(monthly_manifest.read_bytes()),
                 'weekly': json.loads(weekly_manifest.read_bytes())}
    details = []
    with sqlite3.connect(db_path.as_uri() + '?mode=ro&immutable=1', uri=True) as conn:
        for day, series in cases:
            scope = 'weekly' if 'W' in series or 'F' in series else 'monthly'
            manifest = manifests[scope]
            candidates = [entry for entry in manifest['receipts']
                          if Path(entry['path']).name == f'{day.year}_opt.zip']
            if len(candidates) != 1:
                raise ValueError('case needs one explicitly receipted annual archive')
            archive = Path(candidates[0]['path'])
            chain = Path(manifest['full_chain_path'])
            for path, digest in ((archive, candidates[0]['sha256']), (chain, manifest['full_chain_sha256'])):
                if sha256_file(path) != digest:
                    raise ValueError('official archive or legacy chain changed')
                pins[str(path)] = digest
            member = f'{day.year}_opt_{day.month:02d}.csv'
            with ZipFile(archive) as source:
                if source.namelist().count(member) != 1:
                    raise ValueError('one official monthly CSV member required')
                with source.open(member) as raw:
                    with io.TextIOWrapper(raw, encoding='cp950', newline='') as stream:
                        official, positive = _native_csv_keys(stream, day=day, series=series)
            entry = case_map[(day, series)]
            source_keys = _group_events(conn, series, date.fromisoformat(entry['nominal_date']),
                                        date.fromisoformat(entry['later_final_settlement_date']))[day]
            chain_frame = pl.scan_parquet(chain).filter(
                (pl.col('date') == day) & (pl.col('option_series') == series)).select(
                    'strike', 'option_right').collect()
            chain_keys = {(_strike_key(strike), right) for strike, right in chain_frame.iter_rows()}
            if len(source_keys) != entry['compact_source_distinct_contract_keys']:
                raise ValueError('compact index differs from the prior diagnostic')
            details.append({'day': day.isoformat(), 'series': series, 'official_archive': str(archive),
                            'official_member': member, 'official_day_session_keys': len(official),
                            'official_positive_volume_keys': len(positive),
                            'compact_natural_date_keys': len(source_keys),
                            'compact_and_official_positive_keys': len(source_keys & positive),
                            'legacy_chain_keys': len(chain_keys),
                            'official_positive_keys_in_legacy_chain': len(positive & chain_keys)})
    if any(sha256_file(Path(path)) != digest for path, digest in pins.items()):
        raise ValueError('source or implementation changed during audit')
    return {'contract': 'txo_nominal_expiry_native_case_readback_v1',
            'observed_at_utc': datetime.now(timezone.utc).isoformat(), 'input_sha256': pins,
            'details': details, 'training_ready': False,
            'limitations': ['Only these explicitly selected series and dates were inspected.',
                'A daily bar, later final settlement and natural-date event are retrospective source evidence.',
                'They do not establish decision-time listing, a synchronized quote, an executable fill, or an account.']}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--impact', required=True, type=Path)
    parser.add_argument('--union', required=True, type=Path)
    parser.add_argument('--monthly-manifest', required=True, type=Path)
    parser.add_argument('--weekly-manifest', required=True, type=Path)
    parser.add_argument('--case', required=True, action='append', help='YYYY-MM-DD:series')
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    cases = []
    for value in args.case:
        match = re.fullmatch(r'(\d{4}-\d{2}-\d{2}):(\d{6}(?:[WF][1-5])?)', value)
        if not match:
            parser.error('invalid case')
        cases.append((date.fromisoformat(match.group(1)), match.group(2)))
    if args.output.exists():
        raise FileExistsError('preserve prior native case readback')
    result = audit(*(p.resolve() for p in (args.impact, args.union,
                                           args.monthly_manifest, args.weekly_manifest)), cases)
    atomic_write_json(args.output, result)
    print(json.dumps({'output': str(args.output), 'details': result['details']}, ensure_ascii=False))


if __name__ == '__main__':
    main()
