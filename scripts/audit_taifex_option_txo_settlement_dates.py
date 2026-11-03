#!/usr/bin/env python3
"""Cross-check original scheduled dates against later TXO settlement sources.

The final-settlement record is retrospective. It cannot be used as an 08:45 or
09:00 feature, and a same-day settlement has not yet occurred at those clocks.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import date, datetime, timezone
import json
from pathlib import Path

import polars as pl

from downloader.artifact_io import atomic_write_json, sha256_file
from scripts.audit_taifex_option_original_daily_presence import _key
from scripts.audit_taifex_option_reference_daily_source import daily_index, expiry_state
from scripts.build_taifex_option_reference_source_window import CONTRACT
from scripts.download_taifex_final_settlement_history import parse_taifex_txo_final_settlement_html
from stockagent.data.tw_option_product_daily import iter_option_product_daily_batches, load_option_daily_source_plan


def compare(references: pl.DataFrame, daily: dict, settlements: dict) -> list[dict]:
    """Report cases where a scheduled date is before the decision date."""
    counts = Counter()
    for row in references.iter_rows(named=True):
        if row['product_raw'] != 'TXO':
            continue
        key = _key(row['product_raw'], row['series_raw'], row['strike_decimal'], row['right_raw'])
        evidence = daily.get((row['decision_date'], *key)) if key is not None else None
        if expiry_state(row['decision_date'], evidence) != 'scheduled_date_before':
            continue
        scheduled = next(iter(evidence['expiry_texts']))
        actual = settlements.get(row['series_raw'])
        relation = ('missing_final_settlement' if actual is None else
                    'settled_before_decision_day' if actual < row['decision_date'] else
                    'settled_on_decision_day' if actual == row['decision_date'] else
                    'settled_after_decision_day')
        counts[(row['decision_date'], row['decision_time'], row['series_raw'], scheduled, actual, relation)] += 1
    return [{'decision_date': str(day), 'clock': clock, 'series': series, 'scheduled_date_raw': scheduled,
             'later_reported_settlement_date': None if actual is None else str(actual),
             'settlement_relation': relation, 'reference_contract_keys': count}
            for (day, clock, series, scheduled, actual, relation), count in sorted(counts.items())]


def audit(window: Path, plan_path: Path, plan_sha: str, settlement_manifest: Path) -> dict:
    implementations = [Path(__file__).resolve(), Path('scripts/audit_taifex_option_reference_daily_source.py').resolve(),
                       Path('scripts/audit_taifex_option_original_daily_presence.py').resolve(),
                       Path('scripts/download_taifex_final_settlement_history.py').resolve(),
                       Path('stockagent/data/tw_option_product_daily.py').resolve()]
    pins = {str(p): sha256_file(p) for p in implementations}
    window_manifest = window / 'manifest.json'
    window_sha = sha256_file(window_manifest)
    source_window = json.loads(window_manifest.read_bytes())
    if source_window.get('contract') != CONTRACT or source_window.get('training_ready') is not False:
        raise ValueError('a source-only reference window is required')
    reference_path = Path(source_window['data_path'])
    if sha256_file(reference_path) != source_window['data_sha256']:
        raise ValueError('reference source changed')
    refs = pl.read_parquet(reference_path).filter(pl.col('product_raw') == 'TXO')
    days = set(refs['decision_date'].to_list())
    sources = load_option_daily_source_plan(plan_path, expected_sha256=plan_sha)
    daily, selected_sources = {}, []
    for source in sources:
        selected = {day for day in days if source.start <= day <= source.end}
        if not selected:
            continue
        subset, _ = daily_index(list(iter_option_product_daily_batches(
            source.path, expected_sha256=source.sha256, products={'TXO'}, trading_dates=selected)))
        if set(daily) & set(subset):
            raise ValueError('overlapping daily source keys')
        daily.update(subset)
        selected_sources.append({'path': str(source.path), 'sha256': source.sha256,
                                 'days': sorted(day.isoformat() for day in selected)})
    if sorted(day for source in selected_sources for day in source['days']) != sorted(day.isoformat() for day in days):
        raise ValueError('daily source dates are missing or overlapping')

    final_manifest_sha = sha256_file(settlement_manifest)
    final_manifest = json.loads(settlement_manifest.read_bytes())
    if final_manifest.get('status') != 'complete' or final_manifest.get('product') != 'TXO':
        raise ValueError('a complete TXO settlement receipt is required')
    raw_info, normalized_info = final_manifest['raw_receipt'], final_manifest['normalized']
    raw, normalized = Path(raw_info['path']), Path(normalized_info['path'])
    if sha256_file(raw) != raw_info['sha256'] or sha256_file(normalized) != normalized_info['sha256']:
        raise ValueError('settlement source receipt mismatch')
    parsed = parse_taifex_txo_final_settlement_html(
        raw.read_bytes(), start_date=date.fromisoformat(final_manifest['requested_start_date']),
        end_date=date.fromisoformat(final_manifest['requested_end_date']), source_file=str(raw),
        source_sha256=raw_info['sha256'], source_url=final_manifest['query_url'])
    stored = pl.read_parquet(normalized)
    if not parsed.equals(stored) or parsed['option_series'].is_duplicated().any():
        raise ValueError('official final-settlement source and normalized series differ')
    settlements = dict(parsed.select('option_series', 'settlement_date').iter_rows())
    result = compare(refs, daily, settlements)
    if (sha256_file(window_manifest) != window_sha or sha256_file(reference_path) != source_window['data_sha256']
            or sha256_file(plan_path) != plan_sha or sha256_file(settlement_manifest) != final_manifest_sha
            or sha256_file(raw) != raw_info['sha256'] or sha256_file(normalized) != normalized_info['sha256']
            or any(sha256_file(Path(src['path'])) != src['sha256'] for src in selected_sources)
            or any(sha256_file(Path(path)) != digest for path, digest in pins.items())):
        raise ValueError('source changed during retrospective comparison')
    return {'contract': 'txo_scheduled_vs_final_settlement_diagnostic_v1',
            'observed_at_utc': datetime.now(timezone.utc).isoformat(),
            'reference_window_sha256': window_sha, 'reference_data_sha256': source_window['data_sha256'],
            'daily_plan_sha256': plan_sha, 'daily_sources': selected_sources,
            'settlement_manifest': str(settlement_manifest), 'settlement_manifest_sha256': final_manifest_sha,
            'settlement_raw': raw_info, 'settlement_normalized': normalized_info,
            'source_vs_normalized_rows': parsed.height, 'implementation_sha256': pins,
            'scheduled_date_before_decision_by_actual_settlement': result, 'training_ready': False,
            'limitations': ['Final settlement occurs after the decision and is never a decision-time feature.',
                            'This diagnostic covers TXO and observed reference keys only.',
                            'Neither scheduled nor later final settlement establishes decision-time listing, quote, or fill.',
                            'A date-only notice requires explicit recorded availability; snapshots cannot be backdated.']}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--window', required=True, type=Path)
    parser.add_argument('--daily-plan', required=True, type=Path)
    parser.add_argument('--daily-plan-sha256', required=True)
    parser.add_argument('--settlement-manifest', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    if args.output.exists():
        raise FileExistsError('preserve the earlier settlement audit')
    result = audit(args.window.resolve(), args.daily_plan.resolve(), args.daily_plan_sha256,
                   args.settlement_manifest.resolve())
    atomic_write_json(args.output, result)
    print(json.dumps({'output': str(args.output), 'cases': result['scheduled_date_before_decision_by_actual_settlement']}))


if __name__ == '__main__':
    main()
