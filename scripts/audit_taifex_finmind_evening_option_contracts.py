#!/usr/bin/env python3
"""Audit retained evening reference contracts against native vendor days.

Each missing key is proven absent from the positive-price, positive-volume
vendor object for its physical source date. This is source identity evidence,
not a complete night-session audit or permission to train/trade that contract.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import json
from pathlib import Path

import polars as pl

from downloader.artifact_io import atomic_write_json, sha256_file
from scripts.audit_taifex_option_contract_presence import compare_contracts
from scripts.build_taifex_option_finmind_reference_window import pin_source


def missing_for_day(references: pl.DataFrame, raw: pl.DataFrame, day: date) -> tuple[pl.DataFrame, int]:
    """Select observed prior-evening references and check whole-day keys."""
    selected = references.filter((pl.col('trade_ts').dt.date() == day)
                                 & (pl.col('trade_ts').dt.hour() >= 15)
                                 & (pl.col('decision_date') > day))
    keys = (selected.select(pl.col('product_raw').alias('product'),
                            pl.col('series_raw').alias('series'),
                            pl.col('strike_decimal'),
                            pl.col('strike_decimal').cast(pl.Float32, strict=True).alias('strike'),
                            pl.col('right_raw').alias('right')).unique())
    if keys.filter(pl.col('strike').is_null() | ~pl.col('strike').is_finite()
                   | (pl.col('strike') <= 0)).height:
        raise ValueError('invalid exchange strike for vendor comparison')
    collisions = (keys.group_by('product', 'series', 'strike', 'right')
                  .agg(pl.col('strike_decimal').n_unique().alias('native_values'))
                  .filter(pl.col('native_values') > 1))
    if collisions.height:
        raise ValueError('Float32 contract identity loses distinct exchange strikes')
    wanted = keys.select('product', 'series', 'strike', 'right').unique()
    valid = raw.filter((pl.col('volume') > 0) & (pl.col('price') > 0))
    return compare_contracts(wanted, valid).sort('product', 'series', 'strike', 'right'), selected.height


def audit(root: Path, official_manifest: Path, output: Path) -> dict:
    root, official_manifest, output = root.resolve(), official_manifest.resolve(), output.resolve()
    workspace = root.parents[1]
    if (not official_manifest.is_relative_to(workspace)
            or not output.is_relative_to(workspace / 'artifacts/markets')
            or output.exists() or output.is_symlink()):
        raise ValueError('new in-workspace audit output and official manifest are required')
    manifest_sha = sha256_file(official_manifest)
    manifest = json.loads(official_manifest.read_bytes())
    if (manifest.get('contract') != 'taifex_option_observed_reference_source_window_v1'
            or manifest.get('training_ready') is not False):
        raise ValueError('an explicit official source-only reference window is required')
    data = Path(manifest['data_path']).resolve()
    if not data.is_relative_to(workspace) or sha256_file(data) != manifest['data_sha256']:
        raise ValueError('official reference payload differs from its manifest')
    source_pins = manifest['source_pins']
    for source in source_pins:
        source_path, receipt = Path(source['path']).resolve(), Path(source['receipt_path']).resolve()
        if (not source_path.is_relative_to(workspace) or not receipt.is_relative_to(workspace)
                or sha256_file(source_path) != source['sha256']
                or sha256_file(receipt) != source['receipt_sha256']):
            raise ValueError('official raw source or native receipt differs')
    references = pl.read_parquet(data)
    days = (references.filter((pl.col('trade_ts').dt.hour() >= 15)
                              & (pl.col('trade_ts').dt.date() < pl.col('decision_date')))
            .select(pl.col('trade_ts').dt.date().unique().sort()).to_series().to_list())
    checked, unavailable = [], []
    for day in days:
        receipt = root / 'receipts/TaiwanOptionTick/all' / f'{day.isoformat()}.json'
        if not receipt.exists():
            unavailable.append({'source_day': day.isoformat(), 'reason': 'receipt_missing'})
            continue
        status = json.loads(receipt.read_bytes()).get('status')
        if status != 'complete':
            unavailable.append({'source_day': day.isoformat(), 'reason': f'receipt_{status}'})
            continue
        source = pin_source(root, day)
        missing, reference_rows = missing_for_day(references, pl.read_parquet(source['path']), day)
        for product in missing['product'].unique().sort().to_list():
            contracts = missing.filter(pl.col('product') == product).rows()
            checked.append({'source_day': day.isoformat(), 'product': product,
                            'official_reference_rows_for_source_day': reference_rows,
                            'original_source': source, 'missing_contracts': [list(row) for row in contracts]})
        print(json.dumps({'source_day': day.isoformat(), 'reference_rows': reference_rows,
                          'missing_contracts': missing.height}, ensure_ascii=False), flush=True)
    if (sha256_file(official_manifest) != manifest_sha or sha256_file(data) != manifest['data_sha256']
            or any(sha256_file(Path(s['path'])) != s['sha256']
                   or sha256_file(Path(s['receipt_path'])) != s['receipt_sha256'] for s in source_pins)
            or any(sha256_file(Path(row['original_source']['path'])) != row['original_source']['sha256']
                   or sha256_file(Path(row['original_source']['receipt_path']))
                   != row['original_source']['receipt_sha256'] for row in checked)):
        raise ValueError('source evidence changed during the audit')
    result = {'contract': 'taifex_finmind_evening_option_contract_audit_v1',
              'generated_at_utc': datetime.now(timezone.utc).isoformat(),
              'official_manifest': str(official_manifest), 'official_manifest_sha256': manifest_sha,
              'official_data': str(data), 'official_data_sha256': manifest['data_sha256'],
              'implementation_sha256': sha256_file(Path(__file__)),
              'source_days_considered': len(days), 'unavailable_source_days': unavailable,
              'missing_product_days': len(checked),
              'missing_contract_days': sum(len(row['missing_contracts']) for row in checked),
              'missing': checked, 'training_ready': False,
              'limitations': ['Only prior-evening contracts appearing as retained official reference candidates.',
                              'One source day can appear in multiple later reference clocks; counts are not trade counts.',
                              'Natural-day Float32 contract identity is not exact tick, PIT, session or fill completeness.',
                              'Missing or empty vendor days remain unknown and are not called closed markets.']}
    atomic_write_json(output, result)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path('data_finmind/complement'))
    parser.add_argument('--official-manifest', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    result = audit(args.root, args.official_manifest, args.output)
    print(json.dumps({'output': str(args.output), 'source_days_considered': result['source_days_considered'],
                      'unavailable_source_days': result['unavailable_source_days'],
                      'missing_product_days': result['missing_product_days'],
                      'missing_contract_days': result['missing_contract_days'], 'training_ready': False}), flush=True)


if __name__ == '__main__':
    main()
