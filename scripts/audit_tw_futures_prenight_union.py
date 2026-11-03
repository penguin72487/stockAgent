#!/usr/bin/env python3
"""Count verified research-only reference candidates without double counting clocks."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import polars as pl

from downloader.artifact_io import atomic_write_json


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(4 * 1024**2), b''):
            value.update(block)
    return value.hexdigest()


def summarize(frame: pl.DataFrame) -> dict:
    keys = ['date', 'physical_contract', 'decision_ts']
    if any(frame[name].null_count() for name in (*keys, 'product', 'regime')):
        raise ValueError('candidate identity contains null values')
    if frame.select(pl.struct(keys).is_duplicated().any()).item():
        raise ValueError('candidate physical contract and decision clock overlap')
    clocks = (frame.group_by(pl.col('decision_ts').dt.strftime('%H:%M').alias('clock'))
              .agg(pl.len().alias('rows'), pl.col('product').n_unique().alias('products'))
              .sort('clock').to_dicts())
    days = (frame.group_by(['date', 'physical_contract'])
            .agg(pl.col('decision_ts').n_unique().alias('clock_count')))
    regimes = (frame.group_by('regime')
               .agg(pl.len().alias('rows'), pl.col('product').n_unique().alias('products'),
                    pl.col('date').min().cast(pl.String).alias('first_date'),
                    pl.col('date').max().cast(pl.String).alias('last_date'))
               .sort('regime').to_dicts())
    return {'candidate_rows': frame.height,
            'distinct_physical_contract_days': days.height,
            'physical_contract_days_at_multiple_clocks': days.filter(pl.col('clock_count') > 1).height,
            'distinct_products': frame['product'].n_unique(),
            'distinct_candidate_dates': frame['date'].n_unique(),
            'by_clock': clocks, 'by_regime': regimes}


def build(pairs: list[tuple[Path, Path]], report: Path) -> dict:
    if report.exists() or report.is_symlink():
        raise ValueError('union report already exists')
    frames, pins = [], []
    for manifest_path, readback_path in pairs:
        manifest = json.loads(manifest_path.read_text())
        readback = json.loads(readback_path.read_text())
        data = Path(manifest['data_path'])
        manifest_sha, data_sha = digest(manifest_path), digest(data)
        if (manifest.get('training_ready') is not False or readback.get('training_ready') is not False
                or readback.get('accepted') is not True or readback.get('failure_count') != 0
                or manifest_sha != readback.get('candidate_manifest_sha256')
                or data_sha != manifest.get('data_sha256') or data_sha != readback.get('candidate_sha256')):
            raise ValueError('candidate or its independent readback is not source-bound and accepted')
        frame = (pl.read_parquet(data, columns=['date', 'physical_contract', 'decision_ts', 'product'])
                 .with_columns(pl.lit(manifest['contract']).alias('regime')))
        if frame.height != manifest['candidate_rows'] or frame.height != readback['candidate_rows']:
            raise ValueError('candidate row count differs from its manifests')
        frames.append(frame)
        pins.append({'manifest': str(manifest_path), 'manifest_sha256': manifest_sha,
                     'data': str(data), 'data_sha256': data_sha,
                     'independent_readback': str(readback_path), 'readback_sha256': digest(readback_path)})
    if not frames:
        raise ValueError('at least one candidate and readback pair is required')
    result = {'contract': 'tw_futures_prenight_candidate_union_v1', **summarize(pl.concat(frames)),
              'sources': pins, 'implementation_sha256': digest(Path(__file__)),
              'training_ready': False,
              'limitations': ['Research reference prices only; not execution capacity or a broker fill.',
                              'Historical real-time delivery and full training admission remain unverified.']}
    atomic_write_json(report, result)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--pair', action='append', nargs=2, required=True, type=Path,
                        metavar=('MANIFEST', 'INDEPENDENT_READBACK'))
    parser.add_argument('--report', required=True, type=Path)
    args = parser.parse_args()
    result = build(args.pair, args.report)
    print(json.dumps({key: result[key] for key in ('candidate_rows', 'distinct_physical_contract_days',
                                                  'distinct_products', 'training_ready')}), flush=True)


if __name__ == '__main__':
    main()
