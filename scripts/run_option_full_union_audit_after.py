#!/usr/bin/env python3
"""Read back the completed source union against two pinned finite oracles."""
from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import json
from pathlib import Path
import time

import polars as pl

from downloader.artifact_io import atomic_write_json, sha256_file
from stockagent.data.tw_option_reference_candidates import reference_candidates
from stockagent.data.tw_option_source_union import lookup_pinned_union, open_pinned_union


KEYS = ['product_raw', 'series_raw', 'strike_decimal', 'right_raw']


def compare_clock(conn, doc, expected: pl.DataFrame, *, day: date, clock: str) -> dict:
    expected = expected.sort(*KEYS)
    keys = [tuple(row) for row in expected.select(*KEYS).iter_rows()]
    actual = lookup_pinned_union(conn, doc, keys, day=day, clock=clock)
    flagged = actual['known_missing_day_since_reference'].sum()
    original = actual.drop('latest_known_missing_source_day', 'known_missing_day_since_reference')
    if not original.equals(expected):
        raise ValueError(f'{day} {clock} full union differs from the selected finite native oracle')
    return {'contract_keys_compared': actual.height, 'reference_mismatches': 0,
            'known_provider_gap_flags_retained': flagged}


def audit(union_manifest: Path, baseline_manifest: Path, native_oracle: Path) -> dict:
    paths = [p.resolve() for p in (union_manifest, baseline_manifest, native_oracle)]
    union_manifest, baseline_manifest, native_oracle = paths
    hashes = {str(p): sha256_file(p) for p in paths}
    baseline = json.loads(baseline_manifest.read_bytes())
    oracle = json.loads(native_oracle.read_bytes())
    if (baseline.get('contract') != 'option_official_gap_overlay_research_v1'
            or baseline.get('training_ready') is not False or baseline.get('decision_date') != '2026-10-07'
            or oracle.get('contract') != 'finmind_option_event_index_native_oracle_v1'
            or oracle.get('training_ready') is not False or oracle.get('day') != '2026-10-28'):
        raise ValueError('pinned finite source oracles have incompatible scope')
    baseline_data = Path(baseline['data_path'])
    recent_manifest = Path(oracle['manifest_path'])
    if (sha256_file(baseline_data) != baseline['data_sha256']
            or sha256_file(recent_manifest) != oracle['manifest_sha256']):
        raise ValueError('finite oracle input changed')
    recent = json.loads(recent_manifest.read_bytes())
    recent_data = Path(recent['data_path'])
    if sha256_file(recent_data) != recent['data_sha256']:
        raise ValueError('recent compact source events changed')
    finite = pl.read_parquet(baseline_data)
    events = pl.read_parquet(recent_data)
    checks = {}
    with open_pinned_union(union_manifest) as (conn, doc):
        if (baseline['official_manifest_sha256'] != doc['overlay_sources'][0]['manifest_sha256']
                or oracle['manifest_sha256'] != doc['overlay_sources'][1]['manifest_sha256']):
            raise ValueError('oracle sources do not match the full union overlays')
        windows = {key: tuple(value) for key, value in doc['session_windows'].items()}
        for clock in ('08:45', '09:00'):
            expected = (finite.filter(pl.col('decision_time') == clock)
                        .drop('decision_time', 'decision_date'))
            checks['2026-10-07 ' + clock] = compare_clock(conn, doc, expected,
                                                           day=date(2026, 10, 7), clock=clock)
            expected_recent = reference_candidates(events, trading_date=date(2026, 10, 28),
                                                   decision_time=clock, session_windows=windows)
            native = oracle['clock_readback'][clock]
            if (expected_recent.height != native['contract_keys_compared']
                    or any(native[name] != 0 for name in ('missing', 'extra',
                           'reference_kind_price_second_or_provenance_mismatch'))):
                raise ValueError('recent source event result differs from its native oracle')
            checks['2026-10-28 ' + clock] = compare_clock(conn, doc, expected_recent,
                                                           day=date(2026, 10, 28), clock=clock)
    if (any(sha256_file(Path(path)) != digest for path, digest in hashes.items())
            or sha256_file(baseline_data) != baseline['data_sha256']
            or sha256_file(recent_data) != recent['data_sha256']):
        raise ValueError('source union or finite oracle changed during readback')
    return {'contract': 'tw_option_full_source_union_two_oracle_audit_v1',
            'observed_at_utc': datetime.now(timezone.utc).isoformat(), 'input_pins': hashes,
            'baseline_data_sha256': baseline['data_sha256'], 'recent_data_sha256': recent['data_sha256'],
            'clock_readback': checks, 'implementation_sha256': sha256_file(Path(__file__)),
            'training_ready': False,
            'limitations': ['Only keys already present in the two finite native oracles are compared.',
                'This does not prove whole-feed completeness, point-in-time delivery, eligibility or executions.',
                'Option accounting and training remain unverified.']}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('union-manifest', 'union-run', 'baseline-manifest', 'native-oracle', 'output'):
        parser.add_argument('--' + name, type=Path, required=True)
    parser.add_argument('--max-wait-seconds', type=int, default=86400)
    args = parser.parse_args()
    if not 1 <= args.max_wait_seconds <= 7 * 86400 or args.output.exists() or args.output.is_symlink():
        raise ValueError('a bounded wait and new audit evidence path are required')
    code_sha = sha256_file(Path(__file__))
    deadline = time.monotonic() + args.max_wait_seconds
    print(json.dumps({'state': 'waiting_for_completed_source_union', 'provider_calls_by_waiter': 0}), flush=True)
    while time.monotonic() < deadline:
        state = json.loads(args.union_run.read_bytes()).get('state')
        if state == 'succeeded':
            if sha256_file(Path(__file__)) != code_sha:
                raise ValueError('audit implementation changed while waiting')
            result = audit(args.union_manifest, args.baseline_manifest, args.native_oracle)
            atomic_write_json(args.output, result)
            print(json.dumps({'state': 'source_union_independently_read_back',
                              'output': str(args.output), 'clock_readback': result['clock_readback'],
                              'training_ready': False}, ensure_ascii=False), flush=True)
            return
        if state not in {'queued', 'running'}:
            raise ValueError('source union workflow ended without success: ' + str(state))
        time.sleep(min(30, max(0, deadline - time.monotonic())))
    raise TimeoutError('source union did not complete before finite audit wait expired')


if __name__ == '__main__':
    main()
