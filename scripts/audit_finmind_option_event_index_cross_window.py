#!/usr/bin/env python3
"""Compare a chronological event-index prefix with retained native trades.

The independent sequential oracle and index use the same vendor files. This
checks representation and cross-window carry, not source completeness,
contract lifecycle, executable prices or admission to model training.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import json
from pathlib import Path
import tempfile

import polars as pl

from downloader.artifact_io import atomic_write_json, sha256_file
from scripts.audit_finmind_option_event_index_oracle import audit as native_audit
from scripts.build_taifex_option_finmind_reference_window import candidate_policy
from scripts.run_finmind_option_reference_event_history import verify
from scripts.run_finmind_option_reference_history import window_dates
from stockagent.data.tw_option_reference_candidates import reference_candidates
from stockagent.data.tw_option_reference_event_index import CONTRACT


def prefix(plan: dict, day: date) -> list[dict]:
    """Take every planned window from the plan beginning through decision day."""
    if plan.get('contract') != 'finmind_option_reference_history_plan_v1' or plan.get('training_ready') is not False:
        raise ValueError('research-only pinned history plan required')
    result, prior_day, found = [], None, False
    for window in plan['windows']:
        days = window_dates(window)
        if prior_day is not None and days[0] <= prior_day:
            raise ValueError('source windows are not strictly ordered')
        prior_day = days[-1]
        if days[0] > day:
            break
        result.append(window)
        if day in days:
            found = True
            break
    if not found:
        raise ValueError('decision day must have its own planned retained source')
    return result


def audit(root: Path, plan_path: Path, index_root: Path, day: date) -> dict:
    root, plan_path, index_root = (p.resolve() for p in (root, plan_path, index_root))
    plan_sha = sha256_file(plan_path)
    plan = json.loads(plan_path.read_bytes())
    windows = prefix(plan, day)
    policy = Path(plan['policy_path'])
    if sha256_file(policy) != plan['policy_sha256']:
        raise ValueError('candidate policy differs from source plan')
    session_windows, _ = candidate_policy(policy)
    parts, sources, partitions = [], [], []
    for window in windows:
        output = index_root / f"{window['start']}_{window['end']}"
        verified = verify(output, plan_path, plan, window, root)
        doc = json.loads((output / 'manifest.json').read_bytes())
        frame = pl.read_parquet(doc['data_path']).filter(pl.col('source_date') <= day)
        parts.append(frame)
        sources.extend(s for s in doc['source_pins'] if s['day'] <= day.isoformat())
        partitions.append({'start': window['start'], 'end': window['end'], **verified})
    if not sources or [s['day'] for s in sources] != sorted({s['day'] for s in sources}):
        raise ValueError('native source dates must be unique and ordered')
    missing = [item for item in plan.get('not_planned_days', [])
               if windows[0]['start'] <= item['date'] <= day.isoformat()]
    index = pl.concat(parts)
    old_cutoff = date.fromisoformat(windows[0]['end'])
    older_observations = {}
    for clock in ('08:45', '09:00'):
        refs = reference_candidates(index, trading_date=day, decision_time=clock,
                                    session_windows=session_windows)
        older_observations[clock] = refs.filter(pl.col('trade_ts').dt.date() <= old_cutoff).height
    # Compose only a temporary readback fixture; the result binds the actual
    # durable partition manifests and native files, never this temporary path.
    with tempfile.TemporaryDirectory(prefix='finmind-cross-window-', dir='/var/tmp') as temp:
        directory = Path(temp)
        payload = directory / 'events.parquet'
        index.write_parquet(payload, compression='zstd')
        composite = directory / 'manifest.json'
        atomic_write_json(composite, {'contract': CONTRACT, 'training_ready': False,
                          'policy_path': str(policy), 'policy_sha256': plan['policy_sha256'],
                          'data_path': str(payload), 'data_sha256': sha256_file(payload),
                          'source_pins': sources, 'known_missing_source_days': missing})
        result = native_audit(composite, day)
        result['composite_manifest_sha256'] = result.pop('manifest_sha256')
        result.pop('manifest_path')
    if (sha256_file(plan_path) != plan_sha or sha256_file(policy) != plan['policy_sha256']
            or any(sha256_file(Path(p['manifest'])) != p['manifest_sha256']
                   or sha256_file(Path(p['manifest']).parent / 'observed_events.parquet') != p['data_sha256']
                   for p in partitions)):
        raise ValueError('plan, policy or event partition changed during cross-window oracle')
    result.update({'contract': 'finmind_option_event_index_cross_window_oracle_v1',
                   'observed_at_utc': datetime.now(timezone.utc).isoformat(),
                   'plan_path': str(plan_path), 'plan_sha256': plan_sha,
                   'partitions': partitions, 'native_source_dates': [s['day'] for s in sources],
                   'candidate_references_at_or_before_first_window_end': older_observations,
                   'first_window_end': old_cutoff.isoformat(),
                   'cross_window_implementation_sha256': sha256_file(Path(__file__)),
                   'limitations': result['limitations'] + [
                       'History before the first planned date is unknown.',
                       'A carried observation is not proof that an old contract remains tradable.',
                       'Known missing source days remain gaps, not zero-trading days.']})
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path('data_finmind/complement'))
    parser.add_argument('--plan', type=Path, required=True)
    parser.add_argument('--index-root', type=Path, required=True)
    parser.add_argument('--date', type=date.fromisoformat, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists() or args.output.is_symlink():
        raise FileExistsError('preserve earlier oracle output')
    result = audit(args.root, args.plan, args.index_root, args.date)
    atomic_write_json(args.output, result)
    print(json.dumps({'output': str(args.output), 'clock_readback': result['clock_readback'],
                      'older_candidates': result['candidate_references_at_or_before_first_window_end']}))


if __name__ == '__main__':
    main()
