#!/usr/bin/env python3
"""Exercise the legacy TXO chain's rejection gate on retained official CSV.

This only proves that three source-bound counterexamples are rejected. It
does not repair the legacy chain or establish a decision-time expiry feed.
"""
from __future__ import annotations

import argparse
import csv
from datetime import date, datetime, timezone
import io
import json
from pathlib import Path
import tempfile
from zipfile import ZipFile

from downloader.artifact_io import atomic_write_json, sha256_file
from scripts.download_taifex_option_daily_history import _builder_fingerprint
from stockagent.data.tw_index_options_daily import build_taifex_option_full_chain


def audit(native_path: Path, monthly_manifest: Path, weekly_manifest: Path) -> dict:
    paths = [Path(__file__).resolve(), native_path, monthly_manifest, weekly_manifest]
    pins = {str(path): sha256_file(path) for path in paths}
    builder = _builder_fingerprint()
    native = json.loads(native_path.read_bytes())
    if native.get('contract') != 'txo_nominal_expiry_native_case_readback_v1' or native.get('training_ready') is not False:
        raise ValueError('native source-only case readback required')
    native_pins = native['input_sha256']
    manifests = {'monthly': json.loads(monthly_manifest.read_bytes()),
                 'weekly': json.loads(weekly_manifest.read_bytes())}
    results = []
    for case in native['details']:
        day = date.fromisoformat(case['day'])
        series = case['series']
        scope = 'weekly' if 'W' in series or 'F' in series else 'monthly'
        manifest = manifests[scope]
        archive = Path(case['official_archive'])
        future = Path(manifest['futures_path'])
        for path, expected in ((archive, native_pins[str(archive)]), (future, manifest['futures_sha256'])):
            if sha256_file(path) != expected:
                raise ValueError('source archive or futures reference changed')
            pins[str(path)] = expected
        if (case['official_positive_volume_keys'] <= 0 or case['legacy_chain_keys'] != 0):
            raise ValueError('case no longer demonstrates the legacy exclusion')
        with tempfile.TemporaryDirectory(prefix='txo-expiry-guard-', dir='/var/tmp') as root:
            root = Path(root)
            source_path, target = root / 'day.csv', root / 'chain.parquet'
            count = 0
            with ZipFile(archive) as source, source.open(case['official_member']) as raw:
                with io.TextIOWrapper(raw, encoding='cp950', newline='') as stream, \
                     source_path.open('w', encoding='cp950', newline='') as output:
                    reader = csv.reader(stream)
                    header = next(reader)
                    fields = {name.strip().lstrip('\ufeff'): index for index, name in enumerate(header)}
                    if {'交易日期', '契約'} - fields.keys():
                        raise ValueError('official source header changed')
                    writer = csv.writer(output)
                    writer.writerow(header)
                    for row in reader:
                        if len(row) <= max(fields['交易日期'], fields['契約']):
                            continue
                        try:
                            source_date = date(*(int(part) for part in row[fields['交易日期']].strip().split('/')))
                        except (TypeError, ValueError):
                            raise ValueError('invalid official date') from None
                        if source_date == day and row[fields['契約']].strip() == 'TXO':
                            writer.writerow(row)
                            count += 1
            if count == 0:
                raise ValueError('official day fixture is empty')
            try:
                build_taifex_option_full_chain([source_path], future, target, series_scope=scope)
            except ValueError as exc:
                error = str(exc)
                if 'point-in-time expiry evidence' not in error or series not in error:
                    raise
            else:
                raise ValueError('legacy builder failed to reject the source contradiction')
            if target.exists() or target.with_suffix(target.suffix + '.tmp').exists():
                raise ValueError('failed build left a target or temporary chain')
            results.append({'day': day.isoformat(), 'series': series, 'official_txo_day_source_rows': count,
                            'official_positive_volume_keys': case['official_positive_volume_keys'],
                            'guard': 'rejected', 'temporary_output_removed': True, 'reason': error})
    if (_builder_fingerprint() != builder
            or any(sha256_file(Path(path)) != digest for path, digest in pins.items())):
        raise ValueError('source or builder changed during guard verification')
    return {'contract': 'txo_expiry_guard_official_source_cases_v1',
            'observed_at_utc': datetime.now(timezone.utc).isoformat(),
            'input_sha256': pins, 'builder_fingerprint': builder, 'cases': results,
            'training_ready': False, 'limitations': [
                'Only the listed source cases and the direct chain builder were exercised.',
                'Previously generated v1 chains remain unchanged and need separate quarantine or rebuilding.',
                'Rejection is not a repair or proof of point-in-time expiry, listing, execution or option accounting.']}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--native', required=True, type=Path)
    parser.add_argument('--monthly-manifest', required=True, type=Path)
    parser.add_argument('--weekly-manifest', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    if args.output.exists():
        raise FileExistsError('preserve the earlier guard receipt')
    result = audit(*(p.resolve() for p in (args.native, args.monthly_manifest, args.weekly_manifest)))
    atomic_write_json(args.output, result)
    print(json.dumps({'output': str(args.output), 'cases': result['cases']}, ensure_ascii=False))


if __name__ == '__main__':
    main()
