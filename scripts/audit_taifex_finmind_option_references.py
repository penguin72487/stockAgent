#!/usr/bin/env python3
"""Compare two retained option-reference windows at stored Float32 precision.

Both windows use the shared reducer and candidate policy. Agreement checks
observed source values, not PIT availability, sessions, executable prices or
absence of additional source rows. No data outside the comparison is changed.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import json
from pathlib import Path

import polars as pl

from downloader.artifact_io import atomic_write_json, sha256_file


KEYS = ['decision_date', 'decision_time', 'product_raw', 'series_raw', 'strike_f32', 'right_raw']


def _prepare(frame: pl.DataFrame, dates: list[date], first_source: date) -> pl.DataFrame:
    result = (frame.filter(pl.col('decision_date').is_in(dates)
                           & (pl.col('trade_ts').dt.date() >= first_source))
              .with_columns(pl.col('strike_decimal').cast(pl.Float32, strict=True).alias('strike_f32'),
                            pl.col('reference_price_decimal').cast(pl.Float32, strict=True).alias('price_f32')))
    if result.filter(pl.col('strike_f32').is_null() | ~pl.col('strike_f32').is_finite()
                     | (pl.col('strike_f32') <= 0)).height:
        raise ValueError('invalid Float32 comparison strike')
    if result.group_by(KEYS).len().filter(pl.col('len') > 1).height:
        raise ValueError('Float32 contract keys collide in a comparison source')
    return result


def _examples(frame: pl.DataFrame, columns: list[str]) -> list[dict]:
    available = [column for column in columns if column in frame.columns]
    return (frame.select(available).head(20)
            .with_columns(pl.col(pl.Date, pl.Datetime).cast(pl.String)).to_dicts())


def compare(official: pl.DataFrame, vendor: pl.DataFrame, dates: list[date]) -> dict:
    if not dates or dates != sorted(set(dates)):
        raise ValueError('an ordered, unique source date scope is required')
    a, b = _prepare(official, dates, dates[0]), _prepare(vendor, dates, dates[0])
    left = a.select(*KEYS, pl.col('reference_kind').alias('official_kind'),
                    pl.col('price_f32').alias('official_price'), pl.col('trade_ts').alias('official_ts'))
    right = b.select(*KEYS, pl.col('reference_kind').alias('vendor_kind'),
                     pl.col('price_f32').alias('vendor_price'), pl.col('trade_ts').alias('vendor_ts'))
    matched = left.join(right, on=KEYS, how='inner', validate='1:1')
    differences = matched.filter(~pl.col('official_kind').eq_missing(pl.col('vendor_kind'))
                                 | ~pl.col('official_price').eq_missing(pl.col('vendor_price'))
                                 | ~pl.col('official_ts').eq_missing(pl.col('vendor_ts')))
    only_official = left.join(right.select(*KEYS), on=KEYS, how='anti')
    only_vendor = right.join(left.select(*KEYS), on=KEYS, how='anti')
    def by_clock(frame: pl.DataFrame) -> list[dict]:
        return (frame.group_by('decision_date', 'decision_time').len()
                .sort('decision_date', 'decision_time')
                .with_columns(pl.col('decision_date').cast(pl.String)).to_dicts())
    columns = [*KEYS, 'official_kind', 'official_price', 'official_ts',
               'vendor_kind', 'vendor_price', 'vendor_ts']
    return {'official_rows': a.height, 'vendor_rows': b.height, 'matched_keys': matched.height,
            'matched_value_differences': differences.height,
            'official_only_keys': only_official.height, 'vendor_only_keys': only_vendor.height,
            'official_by_clock': by_clock(a), 'vendor_by_clock': by_clock(b),
            'differences_by_clock': by_clock(differences),
            'official_only_by_clock': by_clock(only_official), 'vendor_only_by_clock': by_clock(only_vendor),
            'difference_examples': _examples(differences, columns),
            'official_only_examples': _examples(only_official, columns),
            'vendor_only_examples': _examples(only_vendor, columns)}


def build(official_manifest: Path, vendor_manifest: Path, output: Path) -> dict:
    if output.exists() or output.is_symlink():
        raise ValueError('an unused audit output is required')
    official_manifest, vendor_manifest = official_manifest.resolve(), vendor_manifest.resolve()
    manifest_pins = {str(p): sha256_file(p) for p in (official_manifest, vendor_manifest)}
    official, vendor = (json.loads(p.read_bytes()) for p in (official_manifest, vendor_manifest))
    if (official.get('contract') != 'taifex_option_observed_reference_source_window_v1'
            or vendor.get('contract') != 'finmind_option_observed_reference_source_window_v1'
            or official.get('training_ready') is not False or vendor.get('training_ready') is not False
            or official.get('policy_sha256') != vendor.get('policy_sha256')):
        raise ValueError('incompatible candidate source contracts or policies')
    dates = [date.fromisoformat(value) for value in vendor['source_dates']]
    if not set(vendor['source_dates']).issubset(set(official['archive_days'])):
        raise ValueError('official source window lacks comparison dates')
    data_pins = {}
    for doc in (official, vendor):
        data = Path(doc['data_path']).resolve()
        if sha256_file(data) != doc['data_sha256']:
            raise ValueError('source-window data differs from manifest')
        data_pins[str(data)] = doc['data_sha256']
    result = compare(pl.read_parquet(official['data_path']), pl.read_parquet(vendor['data_path']), dates)
    if (any(sha256_file(Path(p)) != sha for p, sha in manifest_pins.items())
            or any(sha256_file(Path(p)) != sha for p, sha in data_pins.items())):
        raise ValueError('a compared source changed during the audit')
    report = {'contract': 'taifex_finmind_option_reference_source_comparison_v1',
              'generated_at_utc': datetime.now(timezone.utc).isoformat(),
              'comparison_dates': vendor['source_dates'], 'source_manifest_pins': manifest_pins,
              'data_pins': data_pins, 'policy_sha256': official['policy_sha256'],
              'comparison': result, 'training_ready': False,
              'limitations': [
                  'Both sources use the same reducer and candidate session policy; this is a source comparison.',
                  'Official observations before the first vendor source date are excluded.',
                  'Prices and strikes compare at retained Float32 precision, not original vendor decimal precision.',
                  'No within-source Float32 key collision is allowed, but cross-source original decimals may differ.',
                  'Neither source proves historical PIT delivery, listing, expiry, a fill or account admission.']}
    output.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_json(output, report)
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--official-manifest', type=Path, required=True)
    parser.add_argument('--vendor-manifest', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    result = build(args.official_manifest, args.vendor_manifest, args.output)
    print(json.dumps({'output': str(args.output), 'comparison': result['comparison']}), flush=True)


if __name__ == '__main__':
    main()
