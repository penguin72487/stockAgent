#!/usr/bin/env python3
"""Index observed official option ticks by physical calendar date.

Official files are trading-date files. For a listed trading day this builder
requires that day and the next listed trading day's original files, retaining
their distinct identities. It does not prove historic data delivery, expiry,
execution or options accounting and does not replace any provider receipt.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import date, datetime, timezone
import hashlib
import json
from pathlib import Path
import tempfile

import polars as pl

from downloader.artifact_io import atomic_write_json, durable_replace, sha256_file
from scripts.audit_taifex_option_reference_oracle import native_reference
from scripts.build_taifex_option_finmind_reference_window import (
    CapacityNotAdmitted, candidate_policy, capacity_check,
)
from stockagent.data.tw_futures_official_ticks import pin_official_option_tick_inventory
from stockagent.data.tw_option_product_ticks import iter_official_option_trade_batches
from stockagent.data.tw_option_reference_candidates import compact_last_observations, reference_candidates
from stockagent.data.tw_option_reference_event_index import SOURCE_COLUMNS, compact_decision_events


CONTRACT = 'taifex_option_calendar_event_index_v1'


def append_chunk(index: pl.DataFrame | None, state: pl.DataFrame | None, rows: pl.DataFrame, *,
                 day: date, sessions: dict[str, tuple[int, int]]) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Reduce a bounded native batch while preserving whole-second alternatives."""
    idx_input = rows if index is None else pl.concat([index, rows])
    state_input = rows if state is None else pl.concat([state, rows])
    return (compact_decision_events(idx_input, day=day, session_windows=sessions),
            compact_last_observations(state_input))


def compare_native(index: pl.DataFrame, sources: list[dict], day: date, sessions: dict) -> tuple[dict, dict]:
    expected, counts = native_reference(sources, day, sessions)
    compared = {}
    for clock in ('08:45', '09:00'):
        actual = {}
        rows = reference_candidates(index, trading_date=day, decision_time=clock, session_windows=sessions)
        for row in rows.iter_rows(named=True):
            key = tuple(row[name] for name in ('product_raw', 'series_raw', 'strike_decimal', 'right_raw'))
            actual[key] = (row['reference_kind'], row['reference_price_decimal'],
                           row['trade_ts'].strftime('%Y%m%d%H%M%S'), tuple(row['source_hashes']))
        wanted = expected[clock]
        missing, extra = set(wanted) - set(actual), set(actual) - set(wanted)
        mismatch = [key for key in wanted.keys() & actual.keys() if wanted[key] != actual[key]]
        if missing or extra or mismatch:
            raise ValueError(f'{clock}: missing={len(missing)} extra={len(extra)} mismatched={len(mismatch)}')
        compared[clock] = {'contract_keys_compared': len(wanted), 'missing': 0, 'extra': 0,
                           'reference_kind_price_second_or_provenance_mismatch': 0}
    return compared, counts


def build(source_manifest: Path, policy: Path, day: date, output: Path) -> dict:
    if output.is_symlink():
        raise ValueError('a new regular research output is required')
    source_manifest, policy, output = (p.resolve() for p in (source_manifest, policy, output))
    workspace = source_manifest.parents[1]
    if output.exists() or not output.is_relative_to(workspace / 'data_tw_index_derivatives_ticks' / 'research'):
        raise ValueError('output must be a new official-source research directory')
    manifest_sha, policy_sha = sha256_file(source_manifest), sha256_file(policy)
    official = json.loads(source_manifest.read_bytes())
    dates = official['listing_page_dates']['options']
    if dates != sorted(set(dates)) or day.isoformat() not in dates:
        raise ValueError('target must be a listed official trading date')
    position = dates.index(day.isoformat())
    if position + 1 >= len(dates):
        raise ValueError('the next listed trading-day source is required')
    source_days = [day, date.fromisoformat(dates[position + 1])]
    sources = pin_official_option_tick_inventory(source_manifest, source_days)
    sessions, linked_pins = candidate_policy(policy)
    code_paths = [Path(__file__), Path('scripts/audit_taifex_option_reference_oracle.py'),
                  Path('stockagent/data/tw_option_product_ticks.py'),
                  Path('stockagent/data/tw_futures_official_ticks.py'),
                  Path('stockagent/data/tw_option_reference_candidates.py'),
                  Path('stockagent/data/tw_option_reference_event_index.py'),
                  Path('scripts/build_taifex_option_finmind_reference_window.py')]
    code_pins = {str(p.resolve()): sha256_file(p) for p in code_paths}
    capacity_start = capacity_check(output.parent)
    indexes, states, counts = {}, {}, []
    for source in sources:
        source_counts, product_counts = Counter(), Counter()
        for frame in iter_official_option_trade_batches(source):
            for observed_day, count in frame.group_by('event_date').len().iter_rows():
                source_counts[observed_day.isoformat()] += count
            chosen = frame.filter(pl.col('event_date') <= day)
            if chosen.is_empty():
                continue
            for product, count in chosen.group_by('product_raw').len().iter_rows():
                product_counts[product] += count
            unknown = set(product_counts) - set(sessions)
            if unknown:
                raise ValueError(f'official products missing from candidate policy: {sorted(unknown)}')
            for observed_day in chosen['event_date'].unique().sort().to_list():
                part = chosen.filter(pl.col('event_date') == observed_day).select(*SOURCE_COLUMNS)
                indexes[observed_day], states[observed_day] = append_chunk(
                    indexes.get(observed_day), states.get(observed_day), part,
                    day=observed_day, sessions=sessions)
        counts.append({'trading_file_date': source['date'], 'source_rows_by_calendar_date': dict(source_counts),
                       'selected_rows_by_product': dict(sorted(product_counts.items()))})
    if day not in indexes:
        raise ValueError('target calendar day has no retained official events')
    for observed_day in indexes:
        if not compact_last_observations(indexes[observed_day]).equals(states[observed_day]):
            raise ValueError(f'official index end state differs from source batches: {observed_day}')
    index = pl.concat([indexes[d].with_columns(pl.lit(d).cast(pl.Date).alias('source_date'))
                       for d in sorted(indexes)]).sort('source_date', 'product_raw', 'series_raw',
                                                       'strike_raw', 'right_raw', 'event_ts',
                                                       'price_raw', 'source_sha256')
    compared, native_counts = compare_native(index, sources, day, sessions)
    if (sha256_file(source_manifest) != manifest_sha or sha256_file(policy) != policy_sha
            or any(sha256_file(Path(p)) != h for p, h in linked_pins.items())
            or any(sha256_file(Path(p)) != h for p, h in code_pins.items())
            or any(sha256_file(Path(s['path'])) != s['sha256'] for s in sources)):
        raise ValueError('official source, policy or implementation changed during processing')
    capacity_publish = capacity_check(output.parent)
    output.mkdir(parents=True, exist_ok=False)
    payload = output / 'observed_events.parquet'
    with tempfile.NamedTemporaryFile(dir=output, prefix='.events-', suffix='.tmp', delete=False) as stream:
        tmp = Path(stream.name)
    try:
        index.write_parquet(tmp, compression='zstd', statistics=True)
        if not index.equals(pl.read_parquet(tmp)):
            raise ValueError('official index payload readback differs')
        durable_replace(tmp, payload)
    finally:
        tmp.unlink(missing_ok=True)
    identity = {'contract': CONTRACT, 'source_manifest_sha256': manifest_sha, 'policy_sha256': policy_sha,
                'sources': [s['sha256'] for s in sources], 'implementation_sha256': code_pins}
    fingerprint = hashlib.sha256(json.dumps(identity, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    receipt = {'contract': CONTRACT, 'generated_at_utc': datetime.now(timezone.utc).isoformat(),
               'event_calendar_date': day.isoformat(), 'data_fingerprint': fingerprint,
               'source_manifest_path': str(source_manifest), 'source_manifest_sha256': manifest_sha,
               'policy_path': str(policy), 'policy_sha256': policy_sha, 'policy_linked_pins': linked_pins,
               'implementation_sha256': code_pins, 'source_pins': sources, 'source_counts': counts,
               'index_rows_by_calendar_date': {d.isoformat(): indexes[d].height for d in sorted(indexes)},
               'native_sequential_oracle': {'counts': native_counts, 'clock_readback': compared},
               'data_path': str(payload), 'data_sha256': sha256_file(payload), 'rows': index.height,
               'capacity_at_start': capacity_start, 'capacity_before_publication': capacity_publish,
               'training_ready': False,
               'limitations': ['Prior calendar dates in these two files are only partial observed context.',
                               'Source coverage, listing, session rules and historic availability are not proven.',
                               'Official side rows and same-second alternatives do not establish executable fills.',
                               'This index is not yet merged into the frozen FinMind source plan.']}
    atomic_write_json(output / 'manifest.json', receipt)
    return receipt


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-manifest', type=Path, default=Path('data_tw_index_derivatives_ticks/manifest.json'))
    parser.add_argument('--policy', type=Path, required=True)
    parser.add_argument('--date', type=date.fromisoformat, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    try:
        result = build(args.source_manifest, args.policy, args.date, args.output)
    except CapacityNotAdmitted as error:
        print(json.dumps({'state': 'capacity_deferred', 'capacity': error.status}), flush=True)
        raise SystemExit(75) from error
    print(json.dumps({'manifest': str(args.output / 'manifest.json'), 'rows': result['rows'],
                      'clock_readback': result['native_sequential_oracle']['clock_readback'],
                      'training_ready': False}, ensure_ascii=False), flush=True)


if __name__ == '__main__':
    main()
