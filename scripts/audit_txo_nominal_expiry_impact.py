#!/usr/bin/env python3
"""Retrospectively audit TXO nominal dates against retained source events.

Neither later final settlement nor an end-of-day event can establish what was
known at an earlier decision clock. This is a diagnostic, never a model input
or an executable-trade, listing, contract-generation, or account proof.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation
import json
from pathlib import Path
import sqlite3
from zoneinfo import ZoneInfo

import polars as pl

from downloader.artifact_io import atomic_write_json, sha256_file
from scripts.download_taifex_final_settlement_history import parse_taifex_txo_final_settlement_html
from stockagent.data.tw_index_derivatives_tick import taifex_option_expiry


ZONE = ZoneInfo('Asia/Taipei')
CONTRACT = 'txo_nominal_expiry_source_impact_diagnostic_v1'


def _strike_key(raw: object) -> str:
    try:
        value = Decimal(str(raw))
    except (InvalidOperation, ValueError):
        raise ValueError('invalid option strike identity') from None
    if not value.is_finite() or value <= 0 or value != value.to_integral_value():
        raise ValueError('TXO integer strike required for cross-source identity')
    return str(int(value))


def _group_events(conn: sqlite3.Connection, series: str, nominal: date,
                  actual: date) -> dict[date, set[tuple[str, str]]]:
    """Indexed compact events after the nominal date and through settlement."""
    if actual <= nominal:
        raise ValueError('a forward settlement shift is required')
    start = int(datetime.combine(nominal + timedelta(days=1), time.min, ZONE).timestamp()) * 1_000_000
    end = int(datetime.combine(actual + timedelta(days=1), time.min, ZONE).timestamp()) * 1_000_000
    grouped: dict[date, set[tuple[str, str]]] = defaultdict(set)
    rows = conn.execute(
        'SELECT strike,right,event_us FROM events WHERE product=? AND series=? '
        'AND event_us>=? AND event_us<?', ('TXO', series, start, end))
    for strike, right, event_us in rows:
        if right not in {'C', 'P'} or not isinstance(event_us, int):
            raise ValueError('invalid compact option event')
        day = datetime.fromtimestamp(event_us / 1_000_000, ZONE).date()
        if not nominal < day <= actual:
            raise ValueError('compact event local-day boundary mismatch')
        grouped[day].add((_strike_key(strike), right))
    return dict(grouped)


def audit(union_path: Path, settlement_manifest: Path,
          monthly_manifest: Path, weekly_manifest: Path) -> dict:
    implementations = [Path(__file__).resolve(),
        Path('stockagent/data/tw_index_derivatives_tick.py').resolve(),
        Path('scripts/download_taifex_final_settlement_history.py').resolve()]
    pins = {str(p): sha256_file(p) for p in implementations}
    for path in (union_path, settlement_manifest, monthly_manifest, weekly_manifest):
        pins[str(path)] = sha256_file(path)
    union = json.loads(union_path.read_bytes())
    if union.get('contract') != 'tw_option_source_union_manifest_v1' or union.get('training_ready') is not False:
        raise ValueError('source-only option union is required')
    database = Path(union['parent_database'])
    if sha256_file(database) != union['parent_sha256']:
        raise ValueError('compact event index changed')
    pins[str(database)] = union['parent_sha256']

    settlement = json.loads(settlement_manifest.read_bytes())
    if settlement.get('status') != 'complete' or settlement.get('product') != 'TXO':
        raise ValueError('complete TXO final-settlement receipt required')
    raw_info, normalized_info = settlement['raw_receipt'], settlement['normalized']
    raw, normalized = Path(raw_info['path']), Path(normalized_info['path'])
    for info in (raw_info, normalized_info):
        path = Path(info['path'])
        if sha256_file(path) != info['sha256']:
            raise ValueError('final-settlement source digest mismatch')
        pins[str(path)] = info['sha256']
    parsed = parse_taifex_txo_final_settlement_html(
        raw.read_bytes(), start_date=date.fromisoformat(settlement['requested_start_date']),
        end_date=date.fromisoformat(settlement['requested_end_date']), source_file=str(raw),
        source_sha256=raw_info['sha256'], source_url=settlement['query_url'])
    if not parsed.equals(pl.read_parquet(normalized)) or parsed['option_series'].is_duplicated().any():
        raise ValueError('final-settlement source and normalized rows disagree')
    shifts = []
    for series, actual in parsed.select('option_series', 'settlement_date').iter_rows():
        nominal = taifex_option_expiry(series)
        if actual < nominal:
            raise ValueError('unexpected early final settlement needs separate review')
        if actual > nominal:
            shifts.append((series, nominal, actual))

    legacy: dict[tuple[date, str], set[tuple[str, str]]] = defaultdict(set)
    legacy_days: dict[str, set[date]] = {}
    target_pairs: set[tuple[date, str]] = set()
    observations = []
    uri = database.as_uri() + '?mode=ro&immutable=1'
    with sqlite3.connect(uri, uri=True) as conn:
        for series, nominal, actual in shifts:
            for day, keys in sorted(_group_events(conn, series, nominal, actual).items()):
                target_pairs.add((day, series))
                observations.append((series, nominal, actual, day, keys))

    for scope, manifest_path in (('monthly', monthly_manifest), ('weekly', weekly_manifest)):
        manifest = json.loads(manifest_path.read_bytes())
        chain = Path(manifest['full_chain_path'])
        if manifest.get('status') != 'complete' or sha256_file(chain) != manifest['full_chain_sha256']:
            raise ValueError('legacy chain receipt or data changed')
        pins[str(chain)] = manifest['full_chain_sha256']
        frame = pl.read_parquet(chain, columns=['date', 'option_series', 'strike', 'option_right'])
        legacy_days[scope] = set(frame['date'].unique().to_list())
        for day, series, strike, right in frame.iter_rows():
            if (day, series) in target_pairs:
                if right not in {'C', 'P'}:
                    raise ValueError('invalid legacy option right')
                legacy[(day, series)].add((_strike_key(strike), right))

    details = []
    for series, nominal, actual, day, keys in observations:
        scope = 'weekly' if 'W' in series or 'F' in series else 'monthly'
        covered = day in legacy_days[scope]
        matched = len(keys & legacy.get((day, series), set())) if covered else None
        details.append({'series': series, 'nominal_date': nominal.isoformat(),
                        'later_final_settlement_date': actual.isoformat(),
                        'observed_natural_date': day.isoformat(),
                        'compact_source_distinct_contract_keys': len(keys),
                        'legacy_chain_has_any_rows_on_date': covered,
                        'source_keys_found_in_legacy_chain': matched})

    if any(sha256_file(Path(path)) != digest for path, digest in pins.items()):
        raise ValueError('input or implementation changed during audit')
    covered = [row for row in details if row['legacy_chain_has_any_rows_on_date']]
    return {'contract': CONTRACT, 'observed_at_utc': datetime.now(timezone.utc).isoformat(),
            'input_sha256': pins, 'official_settlement_rows': parsed.height,
            'settlement_shifted_series': len(shifts),
            'shifted_series_with_compact_source_events': len({row['series'] for row in details}),
            'distinct_series_natural_dates_with_events': len(details),
            'distinct_series_natural_date_contract_keys': sum(row['compact_source_distinct_contract_keys'] for row in details),
            'legacy_chain_dates_with_any_rows': len(covered),
            'source_keys_on_legacy_chain_dates': sum(row['compact_source_distinct_contract_keys'] for row in covered),
            'source_keys_found_on_legacy_chain_dates': sum(row['source_keys_found_in_legacy_chain'] for row in covered),
            'details': details, 'training_ready': False,
            'limitations': [
                'Final-settlement dates are retrospective and cannot be used as decision-time inputs.',
                'Compact source observations are not a complete tape, current quotes, fills, or listing evidence.',
                'A natural date does not itself identify the option trading session.',
                'Chain date presence does not prove each compared key existed in that legacy source receipt.',
                'Missing chain dates are not attributed to the expiry formula.',
                'Product, date, series and strike identity do not prove an adjusted contract generation.',
            ]}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--union', required=True, type=Path)
    parser.add_argument('--settlement-manifest', required=True, type=Path)
    parser.add_argument('--monthly-manifest', required=True, type=Path)
    parser.add_argument('--weekly-manifest', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    if args.output.exists():
        raise FileExistsError('preserve prior expiry diagnostic')
    result = audit(*(p.resolve() for p in (args.union, args.settlement_manifest,
                                           args.monthly_manifest, args.weekly_manifest)))
    atomic_write_json(args.output, result)
    print(json.dumps({key: value for key, value in result.items() if key not in {'input_sha256', 'details'}}, ensure_ascii=False))


if __name__ == '__main__':
    main()
