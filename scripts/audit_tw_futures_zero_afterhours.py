#!/usr/bin/env python3
"""Independently check explicit zero after-hours volumes in TAIFEX originals.

These retrospective observations are not order fills, reference prices,
point-in-time features, or a training release. An absent row is never zero.
"""
from __future__ import annotations

import argparse
import csv
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import io
import json
from pathlib import Path
import zipfile

import polars as pl

from downloader.artifact_io import atomic_write_json, atomic_write_parquet
from scripts.build_taifex_option_finmind_reference_window import capacity_check


def digest(path: Path) -> str:
    result = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(4 * 1024**2), b''):
            result.update(chunk)
    return result.hexdigest()


def verify_original_rows(stream, expected: set[tuple[str, str, str]]) -> int:
    """Check selected keys directly, rejecting conflicting or duplicate raw rows."""
    reader = csv.DictReader(stream)
    required = {'交易日期', '契約', '到期月份(週別)', '交易時段', '成交量'}
    if reader.fieldnames is None:
        raise ValueError('official original has no header')
    reader.fieldnames = [name.lstrip('\ufeff').strip() for name in reader.fieldnames]
    if not required.issubset(reader.fieldnames):
        raise ValueError('official original lacks a required field')
    found = set()
    for row in reader:
        if (row.get('交易時段') or '').strip() != '盤後':
            continue
        raw_day = (row.get('交易日期') or '').strip().replace('/', '-')
        # Older TAIFEX CSVs use e.g. 2017/5/16 rather than 2017/05/16.
        # Parse an actual date rather than relying on textual zero-padding.
        text_day = datetime.strptime(raw_day, '%Y-%m-%d').date().isoformat()
        key = text_day, (row.get('契約') or '').strip().upper(), (row.get('到期月份(週別)') or '').strip()
        if key not in expected:
            continue
        if key in found:
            raise ValueError(f'duplicate raw after-hours key: {key}')
        value = (row.get('成交量') or '').strip().replace(',', '')
        try:
            zero = bool(value) and Decimal(value).is_finite() and Decimal(value) == 0
        except InvalidOperation:
            zero = False
        if not zero:
            raise ValueError(f'raw after-hours volume is not explicit zero: {key}')
        found.add(key)
    if found != expected:
        raise ValueError(f'{len(expected - found)} selected keys were not found in original')
    return len(found)


def build(workspace: Path, input_manifest: Path, output: Path,
          originals_root: Path | None = None) -> dict:
    workspace, input_manifest, output = workspace.resolve(), input_manifest.resolve(), output.resolve()
    if (not output.is_relative_to(workspace) or not output.relative_to(workspace).parts[0].startswith('data_')
            or output.exists() or output.is_symlink()):
        raise ValueError('a new data_* output under the workspace is required')
    manifest = json.loads(input_manifest.read_text())
    input_path = input_manifest.parent / 'all_futures_daily_sessions.parquet'
    input_sha, manifest_sha = digest(input_path), digest(input_manifest)
    if input_sha != manifest['outputs'][input_path.name]['sha256']:
        raise ValueError('official observations differ from their manifest')
    capacity = capacity_check(output.parent)
    selected = (pl.scan_parquet(input_path)
                .filter((pl.col('date') >= date(2017, 5, 15)) & (pl.col('date') <= date(2026, 9, 24))
                        & (pl.col('session') == '盤後') & pl.col('session_reported')
                        & (pl.col('series_type') == 'monthly') & (pl.col('volume') == 0))
                .select('date', 'product', 'contract', 'source_file', 'source_sha256').collect()
                .sort('date', 'product', 'contract'))
    if selected.is_empty() or selected.select('date', 'product', 'contract').is_duplicated().any():
        raise ValueError('zero-volume physical keys are empty or duplicated')
    inventory = {item['sha256']: item for item in manifest['sources']}
    by_source = selected.partition_by('source_sha256', as_dict=True)
    proofs = []
    for (sha,), group in sorted(by_source.items()):
        info = inventory.get(sha)
        if info is None:
            raise ValueError(f'official source not in manifest: {sha}')
        recorded_source = Path(info['path'])
        source = (recorded_source if originals_root is None
                  else originals_root.resolve() / recorded_source.name)
        if originals_root is not None and (source.is_symlink() or not source.is_file()):
            raise ValueError(f'relocated official source is missing or is a symlink: {source}')
        if digest(source) != sha:
            raise ValueError(f'official source hash changed: {source}')
        members = group.partition_by('source_file', as_dict=True)
        checked = 0
        for (name,), rows in members.items():
            expected = {(str(row['date']), row['product'], row['contract']) for row in rows.to_dicts()}
            if source.suffix.lower() == '.zip':
                prefix, member = name.split(':', 1)
                if prefix != source.name:
                    raise ValueError('official zip identity differs')
                with zipfile.ZipFile(source) as archive, archive.open(member) as raw:
                    with io.TextIOWrapper(raw, encoding='cp950', errors='strict', newline='') as stream:
                        checked += verify_original_rows(stream, expected)
            elif source.suffix.lower() == '.csv' and name == source.name:
                with source.open('r', encoding='cp950', errors='strict', newline='') as stream:
                    checked += verify_original_rows(stream, expected)
            else:
                raise ValueError('unsupported original identity')
        if checked != group.height or digest(source) != sha:
            raise ValueError('official original changed or did not fully match')
        proofs.append({'path': str(source), 'recorded_source_path': str(recorded_source),
                       'sha256': sha, 'verified_rows': checked})
        print(json.dumps({'stage': 'official_zero_volume_readback', 'sources_checked': len(proofs),
                          'sources_total': len(by_source)}), flush=True)
    if digest(input_path) != input_sha or digest(input_manifest) != manifest_sha:
        raise ValueError('official source snapshot changed during readback')
    output.mkdir(parents=True, exist_ok=False)
    payload = output / 'zero_afterhours.parquet'
    atomic_write_parquet(payload, selected)
    if not selected.equals(pl.read_parquet(payload)):
        raise ValueError('zero-volume evidence parquet did not round-trip')
    summary = {'contract': 'taifex_explicit_zero_afterhours_source_evidence_v1',
               'created_at_utc': datetime.now(timezone.utc).isoformat(),
               'source_manifest': str(input_manifest), 'source_manifest_sha256': manifest_sha,
               'source_data': str(input_path), 'source_data_sha256': input_sha,
               'originals': proofs, 'rows': selected.height,
               'products': selected['product'].n_unique(), 'dates': selected['date'].n_unique(),
               'data_path': str(payload), 'data_sha256': digest(payload),
               'implementation_sha256': digest(Path(__file__)), 'capacity': capacity,
               'training_ready': False,
               'limitations': ['Official end-of-session volume is retrospective, not a historical feed receipt.',
                               'Zero volume alone is not a last-trade price, execution or broker fill.',
                               'Only explicit source rows were retained; absent rows are unknown.']}
    atomic_write_json(output / 'manifest.json', summary)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--workspace', type=Path, required=True)
    parser.add_argument('--input-manifest', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--originals-root', type=Path,
                        help='directory containing byte-identical originals by recorded basename')
    args = parser.parse_args()
    result = build(args.workspace, args.input_manifest, args.output, args.originals_root)
    print(json.dumps({key: result[key] for key in ('rows', 'products', 'dates', 'training_ready')}), flush=True)


if __name__ == '__main__':
    main()
