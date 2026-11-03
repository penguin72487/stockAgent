#!/usr/bin/env python3
"""Retrospectively compare option carry candidates to official daily keys.

Official day-end records are diagnostic evidence, not point-in-time listing,
eligibility, quotes, or executable prices. Exact decimal comparison is retained;
the FinMind candidate strike may already have been quantized to Float32.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
import json
from pathlib import Path

import polars as pl

from downloader.artifact_io import atomic_write_json, sha256_file
from scripts.audit_taifex_option_original_daily_presence import _key
from scripts.audit_taifex_option_reference_daily_source import daily_index, expiry_state
from scripts.build_finmind_option_reference_boundary import KEYS
from scripts.run_finmind_option_reference_boundaries import verify
from stockagent.data.tw_option_product_daily import iter_option_product_daily_batches, load_option_daily_source_plan


def summarize(rows: pl.DataFrame, daily: dict) -> dict:
    counts: dict[str, Counter] = defaultdict(Counter)
    series: Counter = Counter()
    for row in rows.iter_rows(named=True):
        clock, day = row['decision_time'], row['decision_date']
        if clock not in {'08:45', '09:00'}:
            raise ValueError('unexpected decision clock')
        key = _key(row['product_raw'], row['series_raw'], row['strike_decimal'], row['right_raw'])
        counts[clock]['reference_rows'] += 1
        if key is None:
            counts[clock]['unsupported_reference_key'] += 1
            continue
        evidence = daily.get((day, *key))
        state = expiry_state(day, evidence)
        counts[clock][state] += 1
        if evidence is not None:
            counts[clock]['daily_key_present'] += 1
            if 'day' in evidence['sessions']:
                counts[clock]['daily_day_session_key_present'] += 1
        series[(day.isoformat(), clock, row['product_raw'], row['series_raw'], state)] += 1
    return {'by_clock': {clock: dict(value) for clock, value in sorted(counts.items())},
            'by_day_clock_product_series_state': [
                {'date': day, 'clock': clock, 'product': product, 'series': month,
                 'state': state, 'rows': value}
                for (day, clock, product, month, state), value in sorted(series.items())]}


def audit(windows: list[Path], history_plan: Path, root: Path,
          daily_plan: Path, daily_plan_sha: str) -> dict:
    implementation = [Path(__file__).resolve(), Path('scripts/audit_taifex_option_reference_daily_source.py').resolve(),
                      Path('scripts/audit_taifex_option_original_daily_presence.py').resolve(),
                      Path('stockagent/data/tw_option_product_daily.py').resolve()]
    implementation_pins = {str(path): sha256_file(path) for path in implementation}
    history_plan_sha = sha256_file(history_plan)
    plan = json.loads(history_plan.read_bytes())
    planned = {window['start']: window for window in plan['windows']}
    prepared = []
    all_days: set[date] = set()
    for window in windows:
        doc = json.loads((window / 'manifest.json').read_bytes())
        start = doc['parents'][1]['window'][0]
        target = planned[start]
        verified = verify(window, history_plan, target, root)
        seeded = pl.read_parquet(doc['data_path'])
        parent = Path(doc['parents'][1]['manifest']).parent / 'observed_references.parquet'
        old = pl.read_parquet(parent)
        if old.select(KEYS).is_duplicated().any() or seeded.select(KEYS).is_duplicated().any():
            raise ValueError('duplicate reference keys')
        added = seeded.join(old.select(KEYS), on=KEYS, how='anti')
        if added.height != doc['comparison']['new_candidate_keys']:
            raise ValueError('added keys differ from boundary receipt')
        days = set(seeded['decision_date'].to_list())
        if days != {date.fromisoformat(pin['date']) for pin in target['source_receipt_pins']}:
            raise ValueError('boundary candidate days differ from pinned target')
        all_days.update(days)
        prepared.append((window, target, verified, doc, old, seeded, added))
    sources = load_option_daily_source_plan(daily_plan, expected_sha256=daily_plan_sha)
    groups: dict[int, set[date]] = defaultdict(set)
    for day in all_days:
        matching = [i for i, source in enumerate(sources) if source.start <= day <= source.end]
        if len(matching) != 1:
            raise ValueError(f'no unique official daily source for {day}')
        groups[matching[0]].add(day)
    daily, unsupported, used = {}, 0, []
    for index, days in sorted(groups.items()):
        source = sources[index]
        frames = list(iter_option_product_daily_batches(source.path, expected_sha256=source.sha256,
                                                        trading_dates=days))
        subset, invalid = daily_index(frames)
        if set(daily) & set(subset):
            raise ValueError('official daily source plan overlaps')
        daily.update(subset)
        unsupported += invalid
        used.append({'path': str(source.path), 'sha256': source.sha256,
                     'selected_days': sorted(day.isoformat() for day in days)})
    results = []
    for window, target, verified, doc, old, seeded, added in prepared:
        first = date.fromisoformat(target['start'])
        results.append({'window': [target['start'], target['end']], 'boundary': verified,
                        'known_missing_days_before_window': doc['known_missing_official_positive_days_between_windows'],
                        'baseline': summarize(old, daily), 'seeded': summarize(seeded, daily),
                        'added': summarize(added, daily),
                        'added_first_day': summarize(added.filter(pl.col('decision_date') == first), daily)})
        if verify(window, history_plan, target, root) != verified:
            raise ValueError('boundary changed during diagnostic')
    if (sha256_file(history_plan) != history_plan_sha or sha256_file(daily_plan) != daily_plan_sha
            or any(sha256_file(Path(item['path'])) != item['sha256'] for item in used)
            or any(sha256_file(Path(path)) != digest for path, digest in implementation_pins.items())):
        raise ValueError('pinned plan, source or implementation changed during diagnostic')
    return {'contract': 'finmind_option_boundary_daily_diagnostic_v1',
            'generated_at_utc': datetime.now(timezone.utc).isoformat(),
            'history_plan': str(history_plan), 'history_plan_sha256': history_plan_sha,
            'daily_plan': str(daily_plan), 'daily_plan_sha256': daily_plan_sha,
            'daily_sources': used, 'implementation_sha256': implementation_pins,
            'distinct_supported_daily_contract_days': len(daily),
            'unsupported_distinct_daily_rows': unsupported, 'windows': results, 'training_ready': False,
            'limitations': [
                'Official daily reports are post-session; results cannot establish information available at decision time.',
                'A matching daily key does not prove the same generation, listing, trade permission, price or execution.',
                'A missing daily key does not prove a contract was absent or expired.',
                'Some historical daily archives omit expiry; planned expiry may differ from actual expiration.',
                'FinMind Float32 strike values may not exactly match native official decimals.',
                'Source gaps may hide more recent trades; no account, settlement or training admission is certified.']}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--window', type=Path, action='append', required=True)
    parser.add_argument('--history-plan', type=Path, required=True)
    parser.add_argument('--root', type=Path, default=Path('data_finmind/complement'))
    parser.add_argument('--daily-plan', type=Path, required=True)
    parser.add_argument('--daily-plan-sha256', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists() or args.output.is_symlink():
        raise FileExistsError('preserve previous diagnostic')
    result = audit([path.resolve() for path in args.window], args.history_plan.resolve(), args.root.resolve(),
                   args.daily_plan.resolve(), args.daily_plan_sha256)
    atomic_write_json(args.output, result)
    print(json.dumps({'output': str(args.output), 'windows': [
        {'window': item['window'], 'added_first_day': item['added_first_day']['by_clock']}
        for item in result['windows']]}, ensure_ascii=False), flush=True)


if __name__ == '__main__':
    main()
