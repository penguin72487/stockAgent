#!/usr/bin/env python3
"""Extend selected option references after the full source union is verified.

Only keys observed in a previously validated finite official window are
requested. Retrospective sources, known gaps and unverified eligibility remain
explicit: this neither discovers all contracts nor admits training data.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import json
from pathlib import Path
import sqlite3
import tempfile
import time

import polars as pl
import pyarrow.parquet as pq

from downloader.artifact_io import atomic_write_json, durable_replace, sha256_file
from scripts.audit_tw_option_cross_source_references import audit_rows
from scripts.build_taifex_option_finmind_reference_window import capacity_check
from scripts.build_option_official_gap_overlay import check_strike_identity
from scripts.build_tw_option_cross_source_references import merge_reference_oracles, reference_map
from scripts.build_tw_option_preclock_window_union import GROUP, merge_candidates
from stockagent.data.tw_option_reference_candidates import KEYS
from stockagent.data.tw_option_reference_gap_guard import mark_known_missing_days
from stockagent.data.tw_option_source_union import lookup_pinned_union, open_pinned_union


def verify_strike_identity(conn: sqlite3.Connection, official: pl.DataFrame) -> dict:
    """Check exact official strike identities against all matching source groups."""
    groups = official.select('product_raw', 'series_raw', 'right_raw').unique()
    conn.execute('CREATE TEMP TABLE IF NOT EXISTS selected_option_strike_groups ('
                 'product TEXT,series TEXT,right TEXT,PRIMARY KEY(product,series,right)) WITHOUT ROWID')
    conn.execute('DELETE FROM selected_option_strike_groups')
    conn.executemany('INSERT INTO selected_option_strike_groups VALUES (?,?,?)', list(groups.iter_rows()))
    records = conn.execute('SELECT DISTINCT e.product,e.series,e.strike,e.right '
                           'FROM selected_option_strike_groups g JOIN events e '
                           'ON e.product=g.product AND e.series=g.series AND e.right=g.right').fetchall()
    vendor = pl.DataFrame(records, schema=['product_raw', 'series_raw', 'strike_raw', 'right_raw'],
                          orient='row')
    if vendor.is_empty():
        raise ValueError('no overlapping source groups for exact strike identity check')
    return check_strike_identity(official.rename({'strike_decimal': 'strike_raw'}), vendor)


def combine_clock(window: pl.DataFrame, history: pl.DataFrame, *, day: date,
                  clock: str, missing_days: list[date]) -> tuple[pl.DataFrame, dict]:
    if (window.filter((pl.col('decision_date') != day) | (pl.col('decision_time') != clock)).height
            or window.select(GROUP).is_duplicated().any()):
        raise ValueError('window contains a wrong or duplicated decision key')
    expected_keys = {tuple(row) for row in window.select(*KEYS).iter_rows()}
    history = history.drop('latest_known_missing_source_day', 'known_missing_day_since_reference')
    if not {tuple(row) for row in history.select(*KEYS).iter_rows()} <= expected_keys:
        raise ValueError('full-history lookup returned unrequested keys')
    history = history.with_columns(pl.lit(day).cast(pl.Date).alias('decision_date'),
                                   pl.lit(clock).alias('decision_time'))
    combined = merge_candidates(window, history)
    before, observed, result = reference_map(window), reference_map(history), reference_map(combined)
    if result != merge_reference_oracles(before, observed) or result.keys() != before.keys():
        raise ValueError('full-history union differs from its scalar oracle or requested keys')
    marked = mark_known_missing_days(combined, missing_days=missing_days,
                                     trading_date=day, decision_time=clock)
    return marked, {'requested_window_keys': len(before), 'history_observed_keys': len(observed),
                    'result_keys': len(result), 'reference_or_provenance_changed': sum(
                        result[key] != before[key] for key in before),
                    'reference_value_or_time_changed': sum(
                        result[key][:3] != before[key][:3] for key in before),
                    'ambiguous_keys': sum(value[1] is None for value in result.values()),
                    'known_gap_since_selected_trade': marked['known_missing_day_since_reference'].sum(),
                    'scalar_oracle_mismatches': 0}


def build(union_manifest: Path, window_manifest: Path, official_manifest: Path,
          audit: Path, day: date, output: Path, report: Path) -> dict:
    workspace = Path.cwd().resolve()
    union_manifest, window_manifest, official_manifest, audit, output, report = (
        path.resolve() for path in (union_manifest, window_manifest, official_manifest, audit, output, report))
    if (not output.is_relative_to(workspace / 'data_finmind/research')
            or not report.is_relative_to(workspace / 'artifacts/markets')
            or output.exists() or report.exists()):
        raise ValueError('new research data and market report locations are required')
    inputs = [union_manifest, window_manifest, official_manifest, audit]
    pins = {str(path): sha256_file(path) for path in inputs}
    window, official, previous_audit = (json.loads(path.read_bytes())
                                       for path in (window_manifest, official_manifest, audit))
    if (window.get('contract') != 'tw_option_preclock_window_union_v1'
            or window.get('training_ready') is not False or window.get('decision_date') != day.isoformat()
            or official.get('contract') != 'taifex_option_observed_reference_source_window_v1'
            or official.get('training_ready') is not False
            or window['input_pins'].get(str(official_manifest)) != pins[str(official_manifest)]
            or previous_audit.get('contract') != 'tw_option_full_source_union_two_oracle_audit_v1'
            or previous_audit.get('training_ready') is not False
            or previous_audit['input_pins'].get(str(union_manifest)) != pins[str(union_manifest)]):
        raise ValueError('the verified union and finite reference identities are required')
    for path, digest in window['input_pins'].items():
        if path in pins and pins[path] != digest:
            raise ValueError('source input identity conflicts')
        pins[path] = digest
    pins[window['data_path']] = window['data_sha256']
    for path in (Path(__file__), Path('scripts/build_tw_option_preclock_window_union.py'),
                 Path('scripts/audit_tw_option_cross_source_references.py'),
                 Path('scripts/build_option_official_gap_overlay.py'),
                 Path('scripts/build_tw_option_cross_source_references.py'),
                 Path('stockagent/data/tw_option_reference_gap_guard.py')):
        path = path.resolve()
        digest = sha256_file(path)
        if str(path) in pins and pins[str(path)] != digest:
            raise ValueError('source implementation changed since finite reference publication')
        pins[str(path)] = digest
    if any(Path(p).is_symlink() or not Path(p).is_file() or sha256_file(Path(p)) != h for p, h in pins.items()):
        raise ValueError('source, receipt or implementation pin changed')
    capacity = capacity_check(output.parent)
    prior = pl.read_parquet(window['data_path'])
    official_hashes = {s['sha256'] for s in official['source_pins']}
    if window['input_pins'].get(official['data_path']) != official['data_sha256']:
        raise ValueError('finite window lacks its pinned official reference payload')
    official_rows = pl.read_parquet(official['data_path']).filter(pl.col('decision_date') == day)
    rows, checks = [], {}
    with open_pinned_union(union_manifest) as (conn, doc):
        missing = sorted({date.fromisoformat(item['date']) for item in doc['known_provider_gap_days']})
        strikes = verify_strike_identity(conn, official_rows)
        for clock in ('08:45', '09:00'):
            finite = prior.filter(pl.col('decision_time') == clock)
            keys = [tuple(row) for row in finite.select(*KEYS).iter_rows()]
            if len(keys) != window['clock_comparison'][clock]['union_keys']:
                raise ValueError('finite reference clock count differs')
            history = lookup_pinned_union(conn, doc, keys, day=day, clock=clock)
            marked, checks[clock] = combine_clock(finite, history, day=day, clock=clock,
                                                   missing_days=missing)
            rows.append(marked)
    combined = pl.concat(rows).sort(*GROUP)
    hashes = {digest for values in combined['source_hashes'].to_list() for digest in values}
    finmind_hashes = hashes - official_hashes
    plain = combined.drop('latest_known_missing_source_day', 'known_missing_day_since_reference')
    independent = audit_rows(plain.to_dicts(), day, official_hashes, finmind_hashes)
    if any(sha256_file(Path(p)) != h for p, h in pins.items()):
        raise ValueError('an input changed during full-history lookup')
    capacity_check(output.parent)
    output.mkdir(parents=True, exist_ok=False)
    payload = output / 'observed_references.parquet'
    with tempfile.NamedTemporaryFile(dir=output, prefix='.references-', suffix='.tmp', delete=False) as handle:
        temporary = Path(handle.name)
    try:
        combined.write_parquet(temporary, compression='zstd', statistics=True)
        if not combined.equals(pl.read_parquet(temporary)):
            raise ValueError('candidate readback differs')
        durable_replace(temporary, payload)
    finally:
        temporary.unlink(missing_ok=True)
    data_sha = sha256_file(payload)
    arrow = pq.read_table(payload)
    arrow_plain = arrow.drop(['latest_known_missing_source_day', 'known_missing_day_since_reference'])
    if (audit_rows(arrow_plain.to_pylist(), day, official_hashes, finmind_hashes) != independent
            or any(sha256_file(Path(p)) != h for p, h in pins.items())):
        raise ValueError('independent payload readback or source identity changed')
    result = {'contract': 'tw_option_preclock_full_union_selected_keys_v1',
              'observed_at_utc': datetime.now(timezone.utc).isoformat(),
              'decision_date': day.isoformat(), 'data_path': str(payload), 'data_sha256': data_sha,
              'rows': combined.height, 'clock_comparison': checks,
              'cross_source_strike_identity': strikes,
              'arrow_independent_readback': independent, 'known_provider_gap_days': [d.isoformat() for d in missing],
              'input_pins': pins, 'capacity_at_start': capacity, 'training_ready': False,
              'limitations': ['Only the preselected official-window contract keys are queried.',
                  'Known provider gap flags remain even when an official source is retrospectively available.',
                  'A product and month may represent multiple physical generations.',
                  'Source completeness, historic delivery, listing, fills and option accounting are not proven.']}
    atomic_write_json(output / 'manifest.json', result)
    atomic_write_json(report, result)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('union-manifest', 'union-run', 'union-audit', 'union-audit-run', 'window-manifest',
                 'official-manifest', 'output', 'report'):
        parser.add_argument('--' + name, type=Path, required=True)
    parser.add_argument('--date', type=date.fromisoformat, required=True)
    parser.add_argument('--max-wait-seconds', type=int, default=86400)
    args = parser.parse_args()
    if not 1 <= args.max_wait_seconds <= 7 * 86400 or args.output.exists() or args.report.exists():
        raise ValueError('a bounded wait and new output locations are required')
    paths = [Path(__file__), Path('scripts/build_tw_option_preclock_window_union.py'),
             Path('scripts/build_tw_option_cross_source_references.py'),
             Path('scripts/build_option_official_gap_overlay.py'),
             Path('stockagent/data/tw_option_source_union.py')]
    pins = {str(path.resolve()): sha256_file(path) for path in paths}
    deadline = time.monotonic() + args.max_wait_seconds
    print(json.dumps({'state': 'waiting_for_verified_full_union_and_audit',
                      'provider_calls_by_waiter': 0}), flush=True)
    while time.monotonic() < deadline:
        states = [json.loads(path.read_bytes()).get('state') for path in (args.union_run, args.union_audit_run)]
        if states == ['succeeded', 'succeeded']:
            if any(sha256_file(Path(p)) != h for p, h in pins.items()):
                raise ValueError('lookup implementation changed while waiting')
            result = build(args.union_manifest, args.window_manifest, args.official_manifest,
                           args.union_audit, args.date, args.output, args.report)
            print(json.dumps({'state': 'selected_keys_read_back', 'output': str(args.output / 'manifest.json'),
                              'report': str(args.report), 'clock_comparison': result['clock_comparison'],
                              'training_ready': False}, ensure_ascii=False), flush=True)
            return
        if any(state not in {'queued', 'running', 'succeeded'} for state in states):
            raise ValueError('parent union or independent audit ended unsuccessfully: ' + str(states))
        time.sleep(min(30, max(0, deadline - time.monotonic())))
    raise TimeoutError('source union and independent audit did not finish before the bounded wait')


if __name__ == '__main__':
    main()
