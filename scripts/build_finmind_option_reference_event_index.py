#!/usr/bin/env python3
"""Publish a receipt-bound, compact FinMind option event index for research.

This is an alternate data representation, not a second provider downloader.
It reuses the native source pinning, candidate policy and reference reducer.
It does not establish historical delivery, contract eligibility or executions.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import hashlib
import json
from pathlib import Path
import tempfile

import polars as pl

from downloader.artifact_io import atomic_write_json, durable_replace, sha256_file
from scripts.build_taifex_option_finmind_reference_window import (
    CapacityNotAdmitted, candidate_policy, capacity_check, pin_source,
)
from scripts.run_finmind_option_reference_history import window_dates
from stockagent.data.tw_option_finmind_reference import candidate_rows
from stockagent.data.tw_option_reference_candidates import compact_last_observations, reference_candidates
from stockagent.data.tw_option_reference_event_index import CONTRACT, compact_decision_events


def _sorted_references(frame: pl.DataFrame) -> pl.DataFrame:
    return frame.sort('product_raw', 'series_raw', 'strike_decimal', 'right_raw')


def build(root: Path, plan_path: Path, days: list[date], output: Path) -> dict:
    if output.is_symlink():
        raise ValueError('a new regular research data output is required')
    root, plan_path, output = (p.resolve() for p in (root, plan_path, output))
    workspace = root.parents[1]
    if (not days or days != sorted(set(days)) or output.exists() or output.is_symlink()
            or not output.is_relative_to(workspace / 'data_finmind' / 'research')):
        raise ValueError('ordered source dates and a new research data output are required')
    plan_sha = sha256_file(plan_path)
    plan = json.loads(plan_path.read_bytes())
    if (plan.get('contract') != 'finmind_option_reference_history_plan_v1'
            or plan.get('training_ready') is not False):
        raise ValueError('pinned source history plan required')
    planned = {}
    for window in plan['windows']:
        for item, planned_day in zip(window['source_receipt_pins'], window_dates(window), strict=True):
            if planned_day in planned:
                raise ValueError('source history plan repeats a date')
            planned[planned_day] = item
    if any(day not in planned for day in days):
        raise ValueError('requested source date is not in the pinned history plan')
    policy = Path(plan['policy_path']).resolve()
    if sha256_file(policy) != plan['policy_sha256']:
        raise ValueError('candidate policy differs from the pinned plan')
    windows, linked_pins = candidate_policy(policy)
    implementation = [Path(__file__).resolve(),
        Path('stockagent/data/tw_option_reference_event_index.py').resolve(),
        Path('stockagent/data/tw_option_reference_candidates.py').resolve(),
        Path('stockagent/data/tw_option_finmind_reference.py').resolve(),
        Path('downloader/finmind_option_contract_keys.py').resolve(),
        Path('scripts/build_taifex_option_finmind_reference_window.py').resolve()]
    pins = {str(p): sha256_file(p) for p in implementation}
    capacity_start = capacity_check(output.parent)
    parts, sources, stats = [], [], []
    full_prior = index_prior = None
    for day in days:
        source = pin_source(root, day)
        if (source['sha256'] != planned[day]['parquet_sha256']
                or source['receipt_sha256'] != planned[day]['receipt_sha256']):
            raise ValueError(f'planned source identity differs: {day}')
        raw, counts = candidate_rows(pl.read_parquet(source['path']), day=day,
                                     source_sha256=source['sha256'], candidate_products=set(windows))
        if raw.is_empty():
            raise ValueError(f'no retained source observations for {day}')
        index = compact_decision_events(raw, day=day, session_windows=windows)
        full = raw if full_prior is None else pl.concat([full_prior, raw])
        reduced = index if index_prior is None else pl.concat([index_prior, index])
        checked = {}
        for clock in ('08:45', '09:00'):
            expected = _sorted_references(reference_candidates(full, trading_date=day,
                                                               decision_time=clock, session_windows=windows))
            actual = _sorted_references(reference_candidates(reduced, trading_date=day,
                                                             decision_time=clock, session_windows=windows))
            if not expected.equals(actual):
                raise ValueError(f'index differs from native source reference for {day} {clock}')
            checked[clock] = expected.height
        full_prior, index_prior = compact_last_observations(full), compact_last_observations(reduced)
        if not full_prior.equals(index_prior):
            raise ValueError(f'index differs from native end-of-day state for {day}')
        parts.append(index.with_columns(pl.lit(day).cast(pl.Date).alias('source_date')))
        sources.append(source)
        stats.append({'day': day.isoformat(), **counts, 'index_rows': index.height,
                      'checked_reference_keys': checked, 'checked_end_state_rows': full_prior.height})
        print(json.dumps(stats[-1], ensure_ascii=False), flush=True)
    data = pl.concat(parts).sort('source_date', 'product_raw', 'series_raw', 'strike_raw',
                                 'right_raw', 'event_ts', 'price_raw', 'source_sha256')
    if (sha256_file(plan_path) != plan_sha or sha256_file(policy) != plan['policy_sha256']
            or any(sha256_file(Path(p)) != h for p, h in linked_pins.items())
            or any(sha256_file(Path(p)) != h for p, h in pins.items())
            or any(sha256_file(Path(s['receipt_path'])) != s['receipt_sha256']
                   or sha256_file(Path(s['path'])) != s['sha256'] for s in sources)):
        raise ValueError('source, policy, plan or code changed during index generation')
    capacity_publish = capacity_check(output.parent)
    output.mkdir(parents=True, exist_ok=False)
    payload = output / 'observed_events.parquet'
    with tempfile.NamedTemporaryFile(dir=output, prefix='.events-', suffix='.tmp', delete=False) as stream:
        tmp = Path(stream.name)
    try:
        data.write_parquet(tmp, compression='zstd', statistics=True)
        if not data.equals(pl.read_parquet(tmp)):
            raise ValueError('independent index payload readback differs')
        durable_replace(tmp, payload)
    finally:
        tmp.unlink(missing_ok=True)
    identity = {'contract': CONTRACT, 'plan_sha256': plan_sha, 'policy_sha256': plan['policy_sha256'],
                'source_sha256s': [s['sha256'] for s in sources], 'implementation_sha256': pins}
    fingerprint = hashlib.sha256(json.dumps(identity, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    missing = [item for item in plan.get('not_planned_days', [])
               if days[0].isoformat() <= item['date'] <= days[-1].isoformat()]
    receipt = {'contract': CONTRACT, 'generated_at_utc': datetime.now(timezone.utc).isoformat(),
               'data_fingerprint': fingerprint, 'plan_path': str(plan_path), 'plan_sha256': plan_sha,
               'policy_path': str(policy), 'policy_sha256': plan['policy_sha256'],
               'policy_linked_pins': linked_pins, 'implementation_sha256': pins,
               'source_pins': sources, 'source_counts': stats, 'known_missing_source_days': missing,
               'data_path': str(payload), 'data_sha256': sha256_file(payload), 'rows': data.height,
               'capacity_at_start': capacity_start, 'capacity_before_publication': capacity_publish,
               'training_ready': False,
               'limitations': [
                   'Index covers only selected retained source days; missing days and prior history remain unknown.',
                   'Full-source equivalence is for the named two clocks and compact observed state only.',
                   'Native Float32 precision and same-second ambiguity remain unchanged.',
                   'Retrospective session policy does not establish historic availability, listing or expiry.',
                   'Observed trade candidates are not quotes, executions, positions, margin or training admission.']}
    atomic_write_json(output / 'manifest.json', receipt)
    return receipt


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path('data_finmind/complement'))
    parser.add_argument('--plan', type=Path, required=True)
    parser.add_argument('--dates', nargs='+', type=date.fromisoformat, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    try:
        result = build(args.root, args.plan, args.dates, args.output)
    except CapacityNotAdmitted as error:
        print(json.dumps({'state': 'capacity_deferred', 'capacity': error.status}), flush=True)
        raise SystemExit(75) from error
    print(json.dumps({'manifest': str(args.output / 'manifest.json'), 'rows': result['rows'],
                      'training_ready': False}), flush=True)


if __name__ == '__main__':
    main()
