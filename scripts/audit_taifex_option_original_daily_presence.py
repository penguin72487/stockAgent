#!/usr/bin/env python3
"""Compare retained all-product option trade keys with an official daily source.

The result is a source-level existence check, not evidence of a complete tape,
an opening execution, historical data availability, listing or account rules.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
import re

import polars as pl

from downloader.artifact_io import atomic_write_json, sha256_file
from stockagent.data.tw_futures_official_ticks import pin_official_option_tick_inventory
from stockagent.data.tw_option_product_daily import iter_option_product_daily_batches
from stockagent.data.tw_option_product_ticks import iter_official_option_trade_batches


def _key(product: str, series: str, strike: str, right: str) -> tuple[str, str, Decimal, str] | None:
    if (not isinstance(product, str) or not isinstance(series, str) or not isinstance(strike, str)
            or not re.fullmatch(r'\d{6}(?:[WF][1-5])?', series) or right not in {'C', 'P'}):
        return None
    try:
        value = Decimal(strike.replace(',', ''))
    except InvalidOperation:
        return None
    if not value.is_finite() or value <= 0:
        return None
    return product, series, value, right


def read_daily_keys(days: set[date], daily_source: Path, daily_sha256: str) -> dict:
    """Read a pinned source once even when many trading days are needed."""
    result = {day: {'expected': set(), 'unreported': set(), 'unsupported': 0} for day in days}
    for frame in iter_option_product_daily_batches(daily_source, expected_sha256=daily_sha256,
                                                   trading_dates=days):
        selected = frame.filter(
            pl.col('session').is_in(['day', 'unreported'])
            & (pl.col('open_raw').str.replace_all(',', '').cast(pl.Float64, strict=False) > 0)
            & (pl.col('volume_raw').str.replace_all(',', '').cast(pl.Int64, strict=False) > 0))
        for row in selected.select('date', 'product', 'series', 'strike_raw', 'right', 'session').unique().iter_rows(named=True):
            day_result = result[row['date']]
            key = _key(row['product'], row['series'], row['strike_raw'], row['right'])
            if key is None:
                day_result['unsupported'] += 1
            else:
                day_result['expected'].add(key)
                if row['session'] == 'unreported':
                    day_result['unreported'].add(key)
    return result


def compare_pinned_day(day: date, daily: dict, source: dict) -> dict:
    expected = daily['expected']
    if not expected or source.get('date') != day.isoformat():
        raise ValueError('no supported official positive contracts or a mismatched source date')
    seen: set[tuple[str, str, Decimal, str]] = set()
    all_products: set[str] = set()
    rows = natural_rows = 0
    for frame in iter_official_option_trade_batches(source):
        rows += frame.height
        all_products.update(frame['product_raw'].unique().to_list())
        on_day = frame.filter(pl.col('event_date') == day)
        natural_rows += on_day.height
        for row in on_day.select('product_raw', 'series_raw', 'strike_raw', 'right_raw').unique().iter_rows():
            key = _key(*row)
            if key is not None:
                seen.add(key)
    missing = expected - seen
    matched = expected & seen
    expected_products = Counter(key[0] for key in expected)
    matched_products = Counter(key[0] for key in matched)
    return {
        'contract': 'official_option_daily_vs_original_tick_contract_existence_v1',
        'observed_at_utc': datetime.now(timezone.utc).isoformat(), 'date': day.isoformat(),
        'tick_source': source,
        'archive_side_rows': rows, 'natural_day_side_rows': natural_rows,
        'archive_product_count': len(all_products), 'archive_products': sorted(all_products),
        'official_positive_supported_contracts': len(expected),
        'official_positive_unreported_session_contracts': len(daily['unreported']),
        'unsupported_daily_key_rows': daily['unsupported'],
        'natural_day_tick_contracts': len(seen), 'expected_contracts_with_tick': len(matched),
        'expected_contracts_without_tick': len(missing),
        'by_product': [{'product': product, 'official_daily_contracts': count,
                        'with_tick': matched_products[product]}
                       for product, count in sorted(expected_products.items())],
        'missing_contracts': [{'product': product, 'series': series, 'strike': str(strike), 'right': right}
                              for product, series, strike, right in sorted(missing)],
        'limitations': [
            'Both sources are the same exchange, not independent vendor confirmation.',
            'The whole-natural-day contract key can include a night trade; it is not day-session completeness.',
            'Public side rows are not independent fills or a quote; exact duplicates remain in source order.',
            'Existence does not prove a first session print, decision-clock price, PIT access, listing, margin or training readiness.',
        ],
    }


def audit(day: date, daily_source: Path, daily_sha256: str, tick_manifest: Path) -> dict:
    audit_path = Path(__file__).resolve()
    reader_path = Path('stockagent/data/tw_option_product_ticks.py').resolve()
    audit_sha256, reader_sha256 = sha256_file(audit_path), sha256_file(reader_path)
    daily = read_daily_keys({day}, daily_source, daily_sha256)[day]
    source = pin_official_option_tick_inventory(tick_manifest, [day])[0]
    result = compare_pinned_day(day, daily, source)
    if sha256_file(audit_path) != audit_sha256 or sha256_file(reader_path) != reader_sha256:
        raise ValueError('audit implementation changed during readback')
    return {**result, 'daily_source': str(daily_source.resolve()), 'daily_source_sha256': daily_sha256,
            'audit_sha256': audit_sha256, 'reader_sha256': reader_sha256}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--date', type=date.fromisoformat, required=True)
    parser.add_argument('--daily-source', type=Path, required=True)
    parser.add_argument('--daily-sha256', required=True)
    parser.add_argument('--tick-manifest', type=Path, default=Path('data_tw_index_derivatives_ticks/manifest.json'))
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise FileExistsError('preserve the previous audit; choose a new output path')
    result = audit(args.date, args.daily_source, args.daily_sha256, args.tick_manifest)
    atomic_write_json(args.output, result)
    print(f"{args.output}: {result['expected_contracts_with_tick']}/"
          f"{result['official_positive_supported_contracts']} contract keys present")


if __name__ == '__main__':
    main()
