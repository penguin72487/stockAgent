#!/usr/bin/env python3
"""Compare retained TX/MTX 08:45 references with later official real trades.

This is a source audit, not a training admission.  The exchange archive is
retrospective; its presence does not establish historical data delivery.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, time, timezone
import json
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq

from downloader.artifact_io import atomic_write_json, sha256_file
from scripts.download_taifex_recent_index_derivatives_ticks import _parse_zip
from stockagent.data.tw_futures_official_ticks import pin_official_tick_inventory


def compare_prior(references: pl.DataFrame, official: pl.DataFrame, day: date) -> list[dict]:
    """Prove a stale reference only with a strictly later same-contract print.

    Timestamps are naive Taipei wall clocks.  A current-minute print is not
    included at 08:45.  Conflicting prices in the last second stay ambiguous.
    """
    if references.select('physical_contract').is_duplicated().any():
        raise ValueError('duplicate physical reference identity on selected date')
    cutoff = datetime.combine(day, time(8, 45))
    refs = references.with_columns(
        pl.col('physical_contract').str.extract(r'^(TX|MTX):([^@#]+)', 1).alias('product'),
        pl.col('physical_contract').str.extract(r'^(TX|MTX):([^@#]+)', 2).alias('contract'))
    refs = refs.filter(pl.col('product').is_not_null())
    prints = official.filter(pl.col('event_ts') < cutoff)
    keys = ['product', 'contract']
    last = prints.group_by(keys).agg(pl.col('event_ts').max().alias('official_last_ts'))
    last = (prints.join(last, on=keys, how='inner')
            .filter(pl.col('event_ts') == pl.col('official_last_ts'))
            .group_by(keys).agg(pl.col('official_last_ts').first(),
                                pl.col('price').n_unique().alias('last_second_price_count'),
                                pl.col('price').first().alias('official_last_price')))
    joined = refs.join(last, on=keys, how='left', validate='m:1')
    records = []
    for row in joined.iter_rows(named=True):
        source_time, old_time = row['official_last_ts'], row['reference_trade_ts']
        old_date = row.get('reference_trade_date')
        later = (source_time is not None and row['reference_kind'] == 'last_observed_trade'
                 and ((old_time is not None and source_time > old_time)
                      or (old_time is None and old_date is not None and source_time.date() > old_date)))
        formerly_unavailable = source_time is not None and row['reference_kind'] == 'unavailable'
        unique = row['last_second_price_count'] == 1
        record = {'physical_contract': row['physical_contract'],
                  'reference_kind': row['reference_kind'],
                  'reference_quote': row['reference_quote'],
                  'reference_trade_ts': old_time.isoformat() if old_time else None,
                  'official_last_ts': source_time.isoformat() if source_time else None,
                  'official_last_second_price_count': row['last_second_price_count'],
                  'official_last_price_if_unambiguous': row['official_last_price'] if unique else None,
                  'strictly_later_official_trade': later,
                  'official_trade_where_old_reference_unavailable': formerly_unavailable,
                  'price_differs_if_unambiguous': (row['reference_quote'] != row['official_last_price']
                                                   if later and unique else None)}
        records.append(record)
    return sorted(records, key=lambda r: r['physical_contract'])


def audit(manifest: Path, references: Path, day: date, output: Path) -> dict:
    manifest, references, output = manifest.resolve(), references.resolve(), output.resolve()
    workspace = Path.cwd().resolve()
    if (not output.is_relative_to(workspace / 'artifacts/markets') or output.exists()
            or output.is_symlink() or not references.is_relative_to(workspace)):
        raise ValueError('new in-workspace audit output and reference source are required')
    reference_manifest = references.with_name('opening_reference_manifest.json')
    reference_manifest_sha = sha256_file(reference_manifest)
    reference_doc = json.loads(reference_manifest.read_bytes())
    reference_sha = sha256_file(references)
    if (reference_doc.get('dataset') != 'tw_futures_observed_open_or_last_trade_v1'
            or reference_doc.get('status') != 'complete'
            or reference_doc.get('contract', {}).get('decision_time') != '08:45'
            or reference_doc.get('data_sha256') != reference_sha
            or reference_doc.get('rows') != pq.ParquetFile(references).metadata.num_rows):
        raise ValueError('opening reference payload or manifest identity differs')
    source = pin_official_tick_inventory(manifest, [day])[0]
    archive = Path(source['path'])
    if archive.stat().st_size != source['bytes'] or sha256_file(archive) != source['sha256']:
        raise ValueError('official archive differs from its receipt')
    parsed = _parse_zip(archive, kind='futures', trading_date=day,
        source_sha256=source['sha256'], futures_products=('TX', 'MTX'),
        futures_outright_contracts_only=True,
        futures_day_sessions={'TX': (0, 1440), 'MTX': (0, 1440)})
    official = parsed.select('product', pl.col('delivery_month_week').alias('contract'),
                             pl.col('event_ts').dt.replace_time_zone(None).alias('event_ts'), 'price')
    selected = (pl.scan_parquet(references).filter(pl.col('date') == day)
                .select('physical_contract', 'reference_kind', 'reference_quote', 'reference_trade_ts',
                        'reference_trade_date')
                .collect())
    details = compare_prior(selected, official, day)
    affected = [r for r in details if r['strictly_later_official_trade']]
    if (sha256_file(reference_manifest) != reference_manifest_sha
            or sha256_file(references) != reference_sha
            or sha256_file(archive) != source['sha256']
            or sha256_file(Path(source['receipt_path'])) != source['receipt_sha256']):
        raise ValueError('source changed during audit')
    result = {'contract': 'taifex_futures_0845_prior_trade_reference_audit_v1',
              'generated_at_utc': datetime.now(timezone.utc).isoformat(), 'date': day.isoformat(),
              'products': ['TX', 'MTX'], 'official_source': source,
              'reference_path': str(references), 'reference_sha256': reference_sha,
              'reference_manifest': str(reference_manifest), 'reference_manifest_sha256': reference_manifest_sha,
              'implementation_sha256': sha256_file(Path(__file__)),
              'reference_contracts_compared': len(details),
              'contracts_with_strictly_later_official_print': len(affected),
              'contracts_with_official_print_where_old_reference_unavailable': sum(
                  r['official_trade_where_old_reference_unavailable'] for r in details),
              'contracts_with_unambiguous_different_price': sum(r['price_differs_if_unambiguous'] is True for r in affected),
              'contracts_with_ambiguous_last_second': sum(r['official_last_second_price_count'] > 1 for r in affected),
              'details': details, 'training_ready': False,
              'limitations': ['Only TX and MTX at 08:45 on one date; absence is not proof of complete sources.',
                              'Exchange historical file timing does not establish historical provider delivery.',
                              'Same-second differing prices remain ambiguous; no price is selected for that case.',
                              'This is not an executable price, fill, margin, accounting or strategy-performance audit.']}
    atomic_write_json(output, result)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--official-manifest', type=Path, required=True)
    parser.add_argument('--references', type=Path, required=True)
    parser.add_argument('--date', type=date.fromisoformat, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    result = audit(args.official_manifest, args.references, args.date, args.output)
    print(json.dumps({key: result[key] for key in ('date', 'products', 'reference_contracts_compared',
        'contracts_with_strictly_later_official_print', 'contracts_with_unambiguous_different_price',
        'contracts_with_official_print_where_old_reference_unavailable',
        'contracts_with_ambiguous_last_second', 'training_ready')}, ensure_ascii=False), flush=True)


if __name__ == '__main__':
    main()
