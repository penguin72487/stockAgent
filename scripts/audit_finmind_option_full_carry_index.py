#!/usr/bin/env python3
"""Independently read back a dated, source-only option lookup index."""
from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import json
from pathlib import Path
import sqlite3

import polars as pl

from downloader.artifact_io import atomic_write_json, sha256_file
from scripts.audit_finmind_option_event_index_cross_window import prefix
from scripts.build_finmind_option_full_carry_index import db_digest
from scripts.build_taifex_option_finmind_reference_window import candidate_policy
from stockagent.data.tw_option_full_carry_index import CONTRACT, lookup
from stockagent.data.tw_option_reference_candidates import reference_candidates
from stockagent.data.tw_option_reference_gap_guard import mark_known_missing_days


def audit(database: Path, plan_path: Path, index_root: Path, native_oracle: Path, day: date) -> dict:
    database, plan_path, index_root, native_oracle = (p.resolve() for p in
                                                       (database, plan_path, index_root, native_oracle))
    plan_sha = sha256_file(plan_path)
    plan = json.loads(plan_path.read_bytes())
    windows = prefix(plan, day)
    missing_source_days = [x for x in plan.get('not_planned_days', [])
                           if windows[0]['start'] <= x['date'] <= day.isoformat()]
    missing_dates = sorted({date.fromisoformat(x['date']) for x in missing_source_days})
    session_windows, _ = candidate_policy(Path(plan['policy_path']))
    oracle_sha = sha256_file(native_oracle)
    oracle = json.loads(native_oracle.read_bytes())
    if (oracle.get('contract') != 'finmind_option_event_index_cross_window_oracle_v1'
            or oracle.get('day') != day.isoformat() or oracle.get('plan_sha256') != plan_sha
            or len(oracle.get('partitions', [])) != len(windows) or oracle.get('training_ready') is not False):
        raise ValueError('independent native oracle identity differs')
    parts, parents = [], []
    with sqlite3.connect(database.as_uri() + '?mode=ro', uri=True) as conn:
        conn.execute('BEGIN')
        meta = conn.execute("SELECT value FROM metadata WHERE key='identity'").fetchone()
        if not meta or json.loads(meta[0]).get('contract') != CONTRACT or json.loads(meta[0]).get('plan_sha256') != plan_sha:
            raise ValueError('source index identity differs')
        for number, (window, pin) in enumerate(zip(windows, oracle['partitions'], strict=True)):
            parent = index_root / f"{window['start']}_{window['end']}"
            manifest = parent / 'manifest.json'
            doc = json.loads(manifest.read_bytes())
            stored = conn.execute('SELECT start,end,manifest_sha256,payload_sha256,rows,events_sha256 '
                                  'FROM windows WHERE idx=?', (number,)).fetchone()
            if (stored is None or stored[:2] != (window['start'], window['end'])
                    or sha256_file(manifest) != stored[2] or stored[2] != pin['manifest_sha256']
                    or sha256_file(Path(doc['data_path'])) != stored[3] or stored[3] != pin['data_sha256']
                    or db_digest(conn, number) != (stored[4], stored[5])):
                raise ValueError('indexed prefix differs from the native oracle source')
            parts.append(pl.read_parquet(doc['data_path']).filter(pl.col('source_date') <= day))
            parents.append({'start': stored[0], 'end': stored[1], 'manifest_sha256': stored[2],
                            'payload_sha256': stored[3], 'events': stored[4]})
        events = pl.concat(parts)
        keys = [tuple(row) for row in events.select('product_raw', 'series_raw', 'strike_raw', 'right_raw')
                .unique().sort('product_raw', 'series_raw', 'strike_raw', 'right_raw').iter_rows()]
        result = {}
        for clock in ('08:45', '09:00'):
            expected = reference_candidates(events, trading_date=day, decision_time=clock,
                                            session_windows=session_windows)
            actual = lookup(conn, keys, day=day, clock=clock, session_windows=session_windows)
            native = oracle['clock_readback'][clock]
            if (not actual.equals(expected) or native.get('contract_keys_compared') != expected.height
                    or native.get('missing') != 0 or native.get('extra') != 0
                    or native.get('reference_kind_price_second_or_provenance_mismatch') != 0):
                raise ValueError(f'{clock} lookup differs from event or independent native oracle')
            marked = mark_known_missing_days(actual, missing_days=missing_dates, trading_date=day,
                                             decision_time=clock)
            flagged = marked['known_missing_day_since_reference'].sum()
            independent_flagged = sum(
                any(row['trade_ts'].date() <= missing <= day for missing in missing_dates)
                for row in actual.iter_rows(named=True))
            if flagged != independent_flagged:
                raise ValueError(f'{clock} missing-source gate differs from independent readback')
            result[clock] = {'reference_keys': actual.height, 'independent_native_mismatches': 0,
                             'event_index_mismatches': 0,
                             'known_missing_day_since_reference': flagged,
                             'independent_gap_flag_mismatches': 0}
    if (sha256_file(plan_path) != plan_sha or sha256_file(native_oracle) != oracle_sha
            or any(sha256_file(index_root / f"{p['start']}_{p['end']}" / 'manifest.json')
                   != p['manifest_sha256'] for p in parents)):
        raise ValueError('source evidence changed during readback')
    return {'contract': 'finmind_option_full_carry_index_oracle_v1',
            'observed_at_utc': datetime.now(timezone.utc).isoformat(), 'day': day.isoformat(),
            'database': str(database), 'plan_path': str(plan_path), 'plan_sha256': plan_sha,
            'independent_native_oracle': str(native_oracle), 'independent_native_oracle_sha256': oracle_sha,
            'source_partitions': parents, 'requested_contract_keys': len(keys), 'clock_readback': result,
            'known_missing_source_days': missing_source_days,
            'implementation_sha256': sha256_file(Path(__file__)), 'training_ready': False,
            'limitations': ['Only retained FinMind observations are checked; source gaps may hide later trades.',
                            'Historical eligibility, actual execution, option accounting and training remain unverified.']}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--database', type=Path, required=True)
    parser.add_argument('--plan', type=Path, required=True)
    parser.add_argument('--index-root', type=Path, required=True)
    parser.add_argument('--native-oracle', type=Path, required=True)
    parser.add_argument('--date', type=date.fromisoformat, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists() or args.output.is_symlink():
        raise FileExistsError('preserve earlier audit evidence')
    result = audit(args.database, args.plan, args.index_root, args.native_oracle, args.date)
    atomic_write_json(args.output, result)
    print(json.dumps({'output': str(args.output), 'clock_readback': result['clock_readback'],
                      'training_ready': False}, ensure_ascii=False), flush=True)


if __name__ == '__main__':
    main()
