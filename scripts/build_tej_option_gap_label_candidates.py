#!/usr/bin/env python3
"""Link source-menu labels to official daily gap keys as research candidates only."""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
import json
from pathlib import Path
import re

import polars as pl
import pyarrow.parquet as pq

from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file
from downloader.tej_company_axis_snapshot import iter_snapshot
from scripts.audit_tej_option_axis_labels import parse_label
from scripts.check_acquisition_capacity import priority_dispatch_status

ROOT = Path(__file__).resolve().parents[1]


def identity(product: str, series: str, strike: str, side: str) -> tuple:
    if (not re.fullmatch(r'[A-Z0-9]{3}', product or '')
            or not re.fullmatch(r'20[0-9]{2}(?:0[1-9]|1[0-2])(?:[WF][1-5])?', series or '')
            or side not in {'C', 'P'}):
        raise ValueError('unrecognized exact candidate contract identity')
    try:
        value = Decimal(str(strike).strip().replace(',', ''))
    except InvalidOperation:
        raise ValueError('unrecognized exact decimal strike') from None
    if not value.is_finite() or value <= 0:
        raise ValueError('positive finite decimal strike required')
    return product, series, value, side


def link(rows: list[dict], labels) -> list[dict]:
    wanted = set()
    for row in rows:
        if type(row['date']) is not date or row['session'] not in {'day', 'unreported'}:
            raise ValueError('official daily candidate date/session differs')
        wanted.add(identity(row['product'], row['series'], row['strike_raw'], row['right']))
    matches = defaultdict(list)
    for label in labels:
        part = parse_label(label)
        if part is None:
            continue
        key = identity(part['prefix'], part['year'] + part['month'] + (part['week'] or ''),
                       part['strike'], part['side'])
        if key in wanted:
            matches[key].append(label)
    result = []
    for row in rows:
        key = identity(row['product'], row['series'], row['strike_raw'], row['right'])
        found = matches[key]
        result.append({'date': row['date'], 'product': row['product'], 'series': row['series'],
                       'official_strike_raw': row['strike_raw'], 'right': row['right'],
                       'official_session': row['session'], 'official_source_member': row['source_member'],
                       'official_source_record_ordinal': row['source_record_ordinal'],
                       'official_source_sha256': row['source_sha256'], 'candidate_labels': found,
                       'candidate_count': len(found),
                       'status': 'unique_label_candidate' if len(found) == 1 else
                                 'ambiguous_label_candidates' if found else 'no_label_candidate'})
    return result


def build(official_manifest: Path, profile_path: Path, output_dir: Path, report: Path) -> dict:
    official_manifest, profile_path = official_manifest.resolve(), profile_path.resolve()
    output_dir, report = output_dir.resolve(), report.resolve()
    if (output_dir.exists() or report.exists()
            or not output_dir.is_relative_to(ROOT / 'data_tw_index_options_daily/research')
            or not report.is_relative_to(ROOT / 'artifacts/markets')):
        raise ValueError('new research data and report paths required')
    capacity = priority_dispatch_status(output_dir.parent, additional_headroom_bytes=16 * 1024**2)
    if capacity.get('state') not in {'admitted', 'not_enrolled'}:
        raise RuntimeError('research candidate capacity not admitted')
    off = json.loads(official_manifest.read_bytes())
    menu = json.loads(profile_path.read_bytes())
    if (off.get('contract') != 'taifex_option_daily_finmind_empty_product_days_v1'
            or off.get('training_ready') is not False
            or menu.get('contract') != 'tej_option_source_menu_label_profile_v1'
            or menu.get('market_observations_obtained') is not False
            or menu.get('full_axis_admitted_to_plan') is not False
            or menu.get('training_input_admitted') is not False):
        raise ValueError('source-only official and TEJ receipts required')
    official_data = Path(off['data_path'])
    snapshot = Path(menu['source_snapshot_path'])
    source_pins = {str(official_manifest): sha256_file(official_manifest),
                   str(profile_path): sha256_file(profile_path),
                   str(official_data): off['data_sha256'],
                   str(snapshot): menu['source_snapshot_sha256'],
                   **off['official_archives'],
                   **{v['receipt_path']: v['receipt_sha256'] for v in off['finmind_empty_receipts'].values()}}
    for path, digest in source_pins.items():
        if sha256_file(Path(path)) != digest:
            raise ValueError('source receipt or content differs')
    code_paths = [Path(__file__).resolve(), ROOT / 'scripts/audit_tej_option_axis_labels.py',
                  ROOT / 'downloader/tej_company_axis_snapshot.py']
    code_pins = {str(p): sha256_file(p) for p in code_paths}
    if menu['implementation_sha256'] != code_pins[str(code_paths[1])]:
        raise ValueError('menu label profile implementation differs')
    data = pl.read_parquet(official_data)
    if data.height != off['official_positive_rows']:
        raise ValueError('official positive row count differs')
    selected = data.select('date', 'product', 'series', 'strike_raw', 'right', 'session',
                           'source_member', 'source_record_ordinal', 'source_sha256').to_dicts()
    records = link(selected, iter_snapshot(snapshot))
    counters = defaultdict(Counter)
    for row in records:
        counters[row['date'].isoformat()][row['status']] += 1
    output_dir.mkdir(parents=True)
    report.parent.mkdir(parents=True, exist_ok=True)
    result_path = output_dir / 'candidate_labels.parquet'
    atomic_write_parquet(result_path, pl.DataFrame(records))
    if pq.read_table(result_path).to_pylist() != records:
        raise ValueError('independent Arrow candidate readback differs')
    if any(sha256_file(Path(p)) != h for p, h in {**source_pins, **code_pins}.items()):
        raise ValueError('source or implementation changed during candidate creation')
    result = {'contract': 'tej_option_official_gap_menu_candidates_v1',
              'created_at_utc': datetime.now(timezone.utc).isoformat(),
              'data_path': str(result_path), 'data_sha256': sha256_file(result_path),
              'source_pins': source_pins, 'implementation_sha256': code_pins,
              'official_rows': len(records), 'match_status_by_date': {k: dict(v) for k, v in sorted(counters.items())},
              'source_table': menu['source_table'], 'provider_queries': 0,
              'market_observations_obtained': False, 'full_axis_admitted_to_plan': False,
              'training_ready': False,
              'limitations': ['Matches are display-identity candidates, not verified physical generations or provider records.',
                  'The source table period is not proven for any candidate, especially 2014.',
                  'Source dates, fields, permissions and native query result still require independent verification.',
                  'Daily bars are retrospective and neither decision-clock prices nor executable fills.']}
    atomic_write_json(output_dir / 'manifest.json', result)
    atomic_write_json(report, result)
    if json.loads(report.read_bytes()) != result or json.loads((output_dir / 'manifest.json').read_bytes()) != result:
        raise ValueError('candidate manifests did not read back')
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('official-manifest', 'profile', 'output-dir', 'report'):
        parser.add_argument('--' + name, type=Path, required=True)
    args = parser.parse_args()
    result = build(args.official_manifest, args.profile, args.output_dir, args.report)
    print(json.dumps({'report': str(args.report), 'official_rows': result['official_rows'],
                      'match_status_by_date': result['match_status_by_date'],
                      'training_ready': False}, ensure_ascii=False))


if __name__ == '__main__':
    main()
