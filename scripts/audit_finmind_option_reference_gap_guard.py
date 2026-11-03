#!/usr/bin/env python3
"""Apply the known-missing-day guard to a verified option-event prefix.

This is source-gap research evidence, not an executable price or training
admission. A clean flag means only that the supplied plan lists no later gap.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import json
from pathlib import Path

import polars as pl

from downloader.artifact_io import atomic_write_json, sha256_file
from scripts.audit_finmind_option_event_index_cross_window import prefix
from scripts.build_taifex_option_finmind_reference_window import candidate_policy
from scripts.run_finmind_option_reference_event_history import verify
from stockagent.data.tw_option_reference_candidates import reference_candidates
from stockagent.data.tw_option_reference_gap_guard import mark_known_missing_days


def audit(root: Path, plan_path: Path, index_root: Path, day: date) -> dict:
    root, plan_path, index_root = (p.resolve() for p in (root, plan_path, index_root))
    plan_sha = sha256_file(plan_path)
    plan = json.loads(plan_path.read_bytes())
    windows = prefix(plan, day)
    missing_records = plan.get('not_planned_days', [])
    if any(row.get('planned') is not False for row in missing_records):
        raise ValueError('missing source days must not be planned days')
    missing = [date.fromisoformat(row['date']) for row in missing_records]
    if missing != sorted(set(missing)):
        raise ValueError('missing source days must be unique and ordered')
    policy = Path(plan['policy_path'])
    if sha256_file(policy) != plan['policy_sha256']:
        raise ValueError('candidate policy differs from the source plan')
    sessions, _ = candidate_policy(policy)
    frames, partitions = [], []
    for window in windows:
        directory = index_root / f"{window['start']}_{window['end']}"
        partition = verify(directory, plan_path, plan, window, root)
        doc = json.loads((directory / 'manifest.json').read_bytes())
        frames.append(pl.read_parquet(doc['data_path']).filter(pl.col('source_date') <= day))
        partitions.append({'start': window['start'], 'end': window['end'], **partition})
    indexed = pl.concat(frames)
    clocks = {}
    for clock in ('08:45', '09:00'):
        refs = reference_candidates(indexed, trading_date=day, decision_time=clock, session_windows=sessions)
        marked = mark_known_missing_days(refs, missing_days=missing, trading_date=day, decision_time=clock)
        flagged = marked.filter(pl.col('known_missing_day_since_reference'))
        by_product = (flagged.group_by('product_raw').len().sort('product_raw').to_dicts())
        clocks[clock] = {'observed_candidate_keys': marked.height, 'known_gap_affected_keys': flagged.height,
                         'known_gap_affected_by_product': by_product}
    if (sha256_file(plan_path) != plan_sha or sha256_file(policy) != plan['policy_sha256']
            or any(sha256_file(Path(p['manifest'])) != p['manifest_sha256']
                   or sha256_file(Path(p['manifest']).parent / 'observed_events.parquet') != p['data_sha256']
                   for p in partitions)):
        raise ValueError('source plan, policy or event index changed during gap analysis')
    implementation = [Path(__file__), Path('stockagent/data/tw_option_reference_gap_guard.py'),
                      Path('scripts/audit_finmind_option_event_index_cross_window.py'),
                      Path('stockagent/data/tw_option_reference_candidates.py')]
    return {'contract': 'finmind_option_known_source_gap_audit_v1',
            'generated_at_utc': datetime.now(timezone.utc).isoformat(), 'decision_date': day.isoformat(),
            'plan_path': str(plan_path), 'plan_sha256': plan_sha, 'partitions': partitions,
            'implementation_sha256': {str(p.resolve()): sha256_file(p) for p in implementation},
            'known_missing_source_days': [row for row in missing_records if row['date'] <= day.isoformat()],
            'clocks': clocks, 'training_ready': False,
            'limitations': ['Flags only gaps recorded by the supplied immutable source plan.',
                            'Known gaps may affect fewer contracts but are not assumed harmless.',
                            'An unflagged candidate is not proof of a complete feed or executable price.',
                            'Expiry, contract generation, source arrival and account rules remain unverified.']}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path('data_finmind/complement'))
    parser.add_argument('--plan', type=Path, required=True)
    parser.add_argument('--index-root', type=Path, required=True)
    parser.add_argument('--date', type=date.fromisoformat, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists() or args.output.is_symlink():
        raise FileExistsError('preserve earlier source-gap audit')
    result = audit(args.root, args.plan, args.index_root, args.date)
    atomic_write_json(args.output, result)
    print(json.dumps({'output': str(args.output), 'clocks': result['clocks']}))


if __name__ == '__main__':
    main()
