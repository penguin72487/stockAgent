#!/usr/bin/env python3
"""Compare a receipt-backed FinLab futures volume axis to a dated catalog.

This is a source-identity audit, not contract eligibility, PIT or trade proof.
Only columns absent from the dated catalog are decoded; known products are
compared from the Parquet schema. The original source and receipts are untouched.
"""
from __future__ import annotations

import argparse
import csv
from datetime import UTC, date, datetime
import json
from pathlib import Path
import re

import polars as pl
import pyarrow.parquet as pq

from downloader.artifact_io import atomic_write_json, sha256_file


def audit(root: Path, receipt_path: Path, catalog: Path, *, start: date, cutoff: date) -> dict:
    root = root.resolve()
    receipt_path = receipt_path.resolve()
    catalog = catalog.resolve()
    if start > cutoff:
        raise ValueError('invalid historical comparison interval')
    pins = {str(path): sha256_file(path) for path in (receipt_path, catalog)}
    receipt = json.loads(receipt_path.read_text(encoding='utf8'))
    if (receipt.get('dataset') != 'futures_price:成交量'
            or receipt.get('status') != 'downloaded_unverified_for_pit'):
        raise ValueError('expected the FinLab futures-volume source receipt')
    relative = Path(receipt.get('parquet_path', ''))
    if relative.is_absolute() or relative.parent != Path('datasets') or '..' in relative.parts:
        raise ValueError('unsafe FinLab payload identity')
    payload = root / relative
    if payload.resolve() != payload or not payload.is_file():
        raise ValueError('missing or redirected FinLab payload')
    payload_digest = sha256_file(payload)
    if (payload_digest != receipt.get('sha256') or
            payload.stat().st_size != receipt.get('parquet_size_bytes')):
        raise ValueError('FinLab receipt payload hash or size mismatch')
    with catalog.open(newline='', encoding='utf8') as stream:
        rows = list(csv.DictReader(stream))
    prior = [row.get('product', '') for row in rows]
    if (not prior or any(not re.fullmatch(r'[A-Z0-9]{2,8}', code) for code in prior)
            or len(prior) != len(set(prior))):
        raise ValueError('catalog must contain unique explicit product codes')
    with pq.ParquetFile(payload) as parquet:
        columns = parquet.schema_arrow.names
        if (not columns or columns[0] != 'source_index' or len(columns) != len(set(columns))
                or parquet.metadata.num_rows != receipt.get('rows')
                or len(columns) - 1 != receipt.get('field_columns')):
            raise ValueError('FinLab receipt and parquet axes differ')
    axes: dict[str, dict[str, str]] = {}
    for column in columns[1:]:
        match = re.fullmatch(r'([A-Z0-9]{2,8})(一般|盤後)', column)
        if not match:
            raise ValueError(f'unknown FinLab futures axis: {column}')
        product, session = match.groups()
        if session in axes.setdefault(product, {}):
            raise ValueError('duplicate product/session axis')
        axes[product][session] = column
    new_codes = sorted(set(axes) - set(prior))
    selected = ['source_index'] + [axes[code][session]
        for code in new_codes for session in sorted(axes[code])]
    frame = pl.read_parquet(payload, columns=selected).with_columns(
        pl.col('source_index').str.strptime(pl.Date, '%Y-%m-%d %H:%M:%S', strict=False).alias('_date'))
    if frame['_date'].null_count() or frame['_date'].is_duplicated().any():
        raise ValueError('FinLab source index is invalid or duplicated')
    differences = []
    for code in new_codes:
        sessions = {}
        for session, column in sorted(axes[code].items()):
            values = frame.filter(pl.col(column).is_not_null())
            invalid = values.filter(~pl.col(column).is_finite() | (pl.col(column) < 0)).height
            positive = values.filter((pl.col(column) > 0) & pl.col(column).is_finite())
            historical = positive.filter(pl.col('_date').is_between(start, cutoff))
            dates = positive['_date']
            sessions[session] = dict(
                positive_source_rows=positive.height,
                positive_rows_in_requested_interval=historical.height,
                invalid_source_rows=invalid,
                first_positive_source_date=str(dates.min()) if len(dates) else None,
                last_positive_source_date=str(dates.max()) if len(dates) else None,
            )
        differences.append(dict(product=code, sessions=sessions))
    pins[str(payload)] = payload_digest
    if pins != {path: sha256_file(Path(path)) for path in pins}:
        raise ValueError('input changed during source-axis audit')
    return dict(contract='finlab_futures_axis_catalog_comparison_v1',
        completed_at_utc=datetime.now(UTC).isoformat(),
        source_checked_at_utc=receipt.get('source_checked_at_utc'),
        source_status=receipt['status'], requested_start=str(start), catalog_cutoff=str(cutoff),
        catalog_codes=len(prior), source_axis_products=len(axes),
        source_session_columns=len(columns)-1, common_codes=len(set(axes) & set(prior)),
        source_codes_outside_catalog=differences,
        catalog_codes_absent_from_source_axis=sorted(set(prior)-set(axes)),
        source_index_is_publication_time=False, source_axis_is_complete_catalog=False,
        price_clock_or_contract_admitted=False, currency_or_accounting_admitted=False,
        training_admitted=False, provider_calls=0, input_pins=pins)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--receipt', type=Path, required=True)
    parser.add_argument('--catalog', type=Path, required=True)
    parser.add_argument('--start', type=date.fromisoformat, required=True)
    parser.add_argument('--cutoff', type=date.fromisoformat, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise FileExistsError('preserve prior source-axis audit')
    result = audit(args.root, args.receipt, args.catalog, start=args.start, cutoff=args.cutoff)
    atomic_write_json(args.output, result)
    print(json.dumps({key: result[key] for key in ('catalog_codes', 'source_axis_products',
        'common_codes', 'source_codes_outside_catalog', 'catalog_codes_absent_from_source_axis')},
        ensure_ascii=False))


if __name__ == '__main__':
    main()
