#!/usr/bin/env python3
"""Independently re-read candidate links against official CSV and retained TEJ labels."""
from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
from decimal import Decimal
import io
import json
from pathlib import Path
from zipfile import ZipFile

import pyarrow.parquet as pq

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.tej_company_axis_snapshot import iter_snapshot


def audit(manifest_path: Path, output: Path) -> dict:
    if output.exists():
        raise ValueError('new independent readback path required')
    manifest_path = manifest_path.resolve()
    manifest = json.loads(manifest_path.read_bytes())
    if (manifest.get('contract') != 'tej_option_official_gap_menu_candidates_v1'
            or manifest.get('training_ready') is not False
            or manifest.get('market_observations_obtained') is not False):
        raise ValueError('candidate-only manifest required')
    inputs = {**manifest['source_pins'], **manifest['implementation_sha256'],
              str(manifest_path): sha256_file(manifest_path),
              manifest['data_path']: manifest['data_sha256'], str(Path(__file__).resolve()): sha256_file(Path(__file__))}
    if any(sha256_file(Path(p)) != h for p, h in inputs.items()):
        raise ValueError('pinned source or implementation differs')
    records = pq.read_table(manifest['data_path']).to_pylist()
    if len(records) != manifest['official_rows']:
        raise ValueError('candidate row count differs')
    by_source = {}
    wanted_labels = set()
    for row in records:
        key = (row['official_source_member'], row['official_source_record_ordinal'])
        if key in by_source or row['candidate_count'] != len(row['candidate_labels']):
            raise ValueError('candidate source ordinal or count ambiguous')
        by_source[key] = row
        wanted_labels.update(row['candidate_labels'])
    snapshot = next((p for p in manifest['source_pins'] if p.endswith('.company-axis.snapshot.bin')), None)
    if snapshot is None:
        raise ValueError('TEJ native source snapshot missing')
    found_labels = {label for label in iter_snapshot(Path(snapshot)) if label in wanted_labels}
    if found_labels != wanted_labels:
        raise ValueError('candidate labels differ from the retained native menu')
    seen = set()
    for archive in sorted(p for p in manifest['source_pins'] if p.endswith('_opt.zip')):
        with ZipFile(archive) as source:
            for member in source.namelist():
                qualified = Path(archive).name + ':' + member
                with io.TextIOWrapper(source.open(member), encoding='cp950', newline='') as stream:
                    reader = csv.DictReader(stream)
                    for ordinal, raw in enumerate(reader, 1):
                        key = (qualified, ordinal)
                        if key not in by_source:
                            continue
                        row = by_source[key]
                        side = {'買權': 'C', '賣權': 'P'}.get(raw['買賣權'].strip())
                        if (raw['契約'].strip().upper() != row['product']
                                or raw['到期月份(週別)'].strip().upper() != row['series']
                                or raw['履約價'] != row['official_strike_raw'] or side != row['right']
                                or int(raw['成交量'].replace(',', '')) <= 0
                                or raw['交易日期'].strip() != f"{row['date'].year}/{row['date'].month}/{row['date'].day}"):
                            raise ValueError('candidate differs from native official CSV')
                        prefix = row['product'] + row['series'] + row['right']
                        for label in row['candidate_labels']:
                            code, description = label.split('==>', 1)
                            if (not code.startswith(prefix) or not description
                                    or Decimal(code[len(prefix):]) != Decimal(row['official_strike_raw'].replace(',', ''))):
                                raise ValueError('TEJ label identity differs from native official row')
                        seen.add(key)
    if seen != set(by_source) or any(sha256_file(Path(p)) != h for p, h in inputs.items()):
        raise ValueError('native rows missing or inputs changed during readback')
    result = {'contract': 'tej_option_gap_candidates_native_readback_v1',
              'observed_at_utc': datetime.now(timezone.utc).isoformat(),
              'input_sha256': inputs, 'official_native_rows_verified': len(seen),
              'source_menu_labels_verified': len(found_labels),
              'candidate_link_differences': 0, 'provider_queries': 0, 'training_ready': False,
              'limitations': ['Independent parsing uses the same retained official and TEJ sources.',
                              'Matches do not prove table coverage, physical generations, market prices or execution.']}
    atomic_write_json(output, result)
    if json.loads(output.read_bytes()) != result:
        raise ValueError('native readback report differs')
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--manifest', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    result = audit(args.manifest, args.output)
    print(json.dumps({'report': str(args.output), 'verified_native_rows': result['official_native_rows_verified'],
                      'verified_native_labels': result['source_menu_labels_verified'],
                      'training_ready': False}, ensure_ascii=False))


if __name__ == '__main__':
    main()
