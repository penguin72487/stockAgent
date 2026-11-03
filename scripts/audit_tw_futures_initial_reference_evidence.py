#!/usr/bin/env python3
"""Compare retained daily changes with independent native opening references.

This is a retrospective evidence audit. It does not supply an opening-time
feature, an executable price, a margin rule, or a training-admissible source.
"""
from __future__ import annotations

import argparse
import csv
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
import io
from pathlib import Path
import sys
import zipfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import polars as pl
from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file


def number(value: object) -> Decimal | None:
    if value is None or str(value).strip() in ('', '-', '--'):
        return None
    try:
        result = Decimal(str(value).strip().replace(',', ''))
    except InvalidOperation:
        return None
    return result if result.is_finite() else None


def candidate(close: object, change: object) -> Decimal | None:
    c, d = number(close), number(change)
    if c is None or d is None or c <= 0 or c - d <= 0:
        return None
    return c - d


def source_rows(path: Path):
    if path.suffix.lower() == '.zip':
        with zipfile.ZipFile(path) as archive:
            members = [name for name in archive.namelist() if name.lower().endswith('.csv')]
            if len(members) != 1:
                raise ValueError('annual source must contain one CSV member')
            with archive.open(members[0]) as raw, io.TextIOWrapper(raw, encoding='big5') as stream:
                yield from csv.DictReader(stream)
    else:
        with path.open('r', encoding='big5', newline='') as stream:
            yield from csv.DictReader(stream)


def audit(*, references: Path, reference_sha: str, omissions: Path, sources: list[Path],
          start: date, end: date, data_output: Path, report_output: Path) -> dict:
    if start > end or len(set(map(str, sources))) != len(sources):
        raise ValueError('ordered interval and distinct original sources required')
    if data_output.exists() or report_output.exists():
        raise FileExistsError('preserve prior output; choose another version')
    pins = {str(path.resolve()): sha256_file(path) for path in [references, omissions, *sources]}
    if pins[str(references.resolve())] != reference_sha:
        raise ValueError('native reference source SHA mismatch')
    ref = (pl.read_parquet(references).filter(pl.col('date').is_between(start, end))
           .select('date', 'product', 'contract', 'opening_reference_quote'))
    if ref.select('date', 'product', 'contract').is_duplicated().any():
        raise ValueError('ambiguous native reference at physical date/product/month')
    native = {(str(r['date']), r['product'], r['contract']): str(r['opening_reference_quote'])
              for r in ref.iter_rows(named=True)}
    omitted = (pl.read_csv(omissions, try_parse_dates=True,
               schema_overrides={'product': pl.String, 'contract': pl.String})
               .filter(pl.col('date').is_between(start, end) & pl.col('materialized_warmup')
                       & (pl.col('outright_volume') > 0)))
    missed = {(str(r['date']), r['product'], r['contract']) for r in omitted.iter_rows(named=True)}
    wanted = set(native) | missed
    found = {}
    required = {'交易日期', '契約', '到期月份(週別)', '收盤價', '漲跌價', '交易時段'}
    for path in sources:
        for row in source_rows(path):
            if not required <= row.keys():
                raise ValueError('original daily source lacks required native columns')
            if (row.get('交易時段') or '').strip() != '一般':
                continue
            raw_day = (row.get('交易日期') or '').strip().replace('/', '-')
            key = (raw_day, (row.get('契約') or '').strip(), (row.get('到期月份(週別)') or '').strip())
            if key not in wanted:
                continue
            if key in found:
                raise ValueError('multiple original regular-session records for one target key')
            close, change = row.get('收盤價'), row.get('漲跌價')
            val = candidate(close, change)
            known = number(native.get(key))
            found[key] = dict(date=raw_day, product=key[1], contract=key[2],
                source_name=path.name, source_close=close, source_change=change,
                derived_reference=None if val is None else str(val),
                native_reference=None if known is None else str(known),
                exact_reference_match=None if val is None or known is None else val == known,
                omitted_positive_outright_warmup=key in missed)
    for name, digest in pins.items():
        if sha256_file(Path(name)) != digest:
            raise ValueError('source changed during audit')
    rows = list(found.values())
    compared = [r for r in rows if r['exact_reference_match'] is not None]
    matches = sum(r['exact_reference_match'] is True for r in compared)
    misses = [r for r in rows if r['omitted_positive_outright_warmup']]
    data_output.parent.mkdir(parents=True, exist_ok=True)
    report_output.parent.mkdir(parents=True, exist_ok=True)
    schema = {'date': pl.String, 'product': pl.String, 'contract': pl.String,
              'source_name': pl.String, 'source_close': pl.String, 'source_change': pl.String,
              'derived_reference': pl.String, 'native_reference': pl.String,
              'exact_reference_match': pl.Boolean, 'omitted_positive_outright_warmup': pl.Boolean}
    atomic_write_parquet(data_output, pl.DataFrame(rows, schema=schema))
    report = dict(contract='retrospective_daily_change_reference_evidence_v1',
        completed_at_utc=datetime.now(UTC).isoformat(), start=str(start), end=str(end), inputs=pins,
        native_reference_keys=len(native), omitted_positive_outright_warmup_keys=len(missed),
        original_daily_rows_matched=len(rows), native_pairs_compared=len(compared),
        native_pairs_exactly_equal=matches, native_pairs_different=len(compared)-matches,
        omitted_warmup_daily_rows_matched=len(misses),
        omitted_warmup_with_numeric_derived_value=sum(r['derived_reference'] is not None for r in misses),
        outputs={str(data_output.resolve()): sha256_file(data_output)},
        interpretation='last close minus reported daily change is an unaccepted end-of-day reconstruction',
        daily_source_available_at_decision_time=False, historical_vendor_delivery_clock_verified=False,
        legal_semantics_for_new_month_verified=False, execution_price=False,
        training_or_financial_admitted=False)
    atomic_write_json(report_output, report)
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--references', type=Path, required=True)
    parser.add_argument('--reference-sha256', required=True)
    parser.add_argument('--omissions', type=Path, required=True)
    parser.add_argument('--official', type=Path, action='append', required=True)
    parser.add_argument('--start', type=date.fromisoformat, required=True)
    parser.add_argument('--end', type=date.fromisoformat, required=True)
    parser.add_argument('--data-output', type=Path, required=True)
    parser.add_argument('--report-output', type=Path, required=True)
    args = parser.parse_args()
    import json
    print(json.dumps(audit(references=args.references, reference_sha=args.reference_sha256,
        omissions=args.omissions, sources=args.official, start=args.start, end=args.end,
        data_output=args.data_output, report_output=args.report_output), ensure_ascii=False), flush=True)


if __name__ == '__main__':
    main()
