#!/usr/bin/env python3
"""Rebuild one source-only option window with its preceding observed state.

The existing window builder and reducer remain authoritative. This adapter
replays one preceding, receipt-verified window solely to seed the next window;
it neither infers closed days nor treats source observations as executions.
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
    candidate_policy, capacity_check, pin_source,
)
from scripts.build_taifex_option_reference_source_window import derive_day
from scripts.run_finmind_option_reference_history import check_planned_receipts, verify_window
from stockagent.data.tw_option_finmind_reference import candidate_rows


CONTRACT = 'finmind_option_observed_reference_boundary_v1'
KEYS = ['decision_date', 'decision_time', 'product_raw', 'series_raw', 'strike_decimal', 'right_raw']
VALUES = ['reference_kind', 'reference_price_decimal', 'trade_ts', 'available_by', 'source_hashes']


def selected_windows(plan: dict, target_start: date) -> tuple[dict, dict, list[str]]:
    windows = plan.get('windows', [])
    matches = [i for i, window in enumerate(windows) if window.get('start') == target_start.isoformat()]
    if (plan.get('contract') != 'finmind_option_reference_history_plan_v1'
            or plan.get('training_ready') is not False or len(matches) != 1 or matches[0] == 0):
        raise ValueError('target requires an immediately preceding planned source window')
    previous, target = windows[matches[0] - 1:matches[0] + 1]
    previous_dates = [date.fromisoformat(x['date']) for x in previous['source_receipt_pins']]
    target_dates = [date.fromisoformat(x['date']) for x in target['source_receipt_pins']]
    if (not previous_dates or not target_dates or previous_dates != sorted(set(previous_dates))
            or target_dates != sorted(set(target_dates)) or previous_dates[-1] >= target_dates[0]
            or previous['start'] != previous_dates[0].isoformat()
            or previous['end'] != previous_dates[-1].isoformat()
            or target['start'] != target_dates[0].isoformat()
            or target['end'] != target_dates[-1].isoformat()):
        raise ValueError('window dates do not match ordered source identities')
    # A gap is retained as unknown even if it is absent from the positive-day
    # inventory. A known prior trade can still be carried, with its source time.
    known_missing = [x['date'] for x in plan.get('not_planned_days', [])
                     if previous_dates[-1] < date.fromisoformat(x['date']) < target_dates[0]]
    return previous, target, known_missing


def compare(old: pl.DataFrame, new: pl.DataFrame) -> dict:
    for frame in (old, new):
        if set(KEYS + VALUES) - set(frame.columns) or frame.select(KEYS).is_duplicated().any():
            raise ValueError('option reference keys must be unique and fully specified')
    joined = old.join(new, on=KEYS, how='left', suffix='_new', validate='1:1')
    absent = joined['reference_kind_new'].null_count()
    if absent:
        raise ValueError('seeded output removed previously observed candidate keys')
    unchanged = pl.all_horizontal(*(pl.col(field).eq_missing(pl.col(field + '_new')) for field in VALUES))
    open_kinds = ['observed_session_open_candidate', 'ambiguous_first_second']
    if joined.filter(pl.col('reference_kind').is_in(open_kinds) & ~unchanged).height:
        raise ValueError('prior-state seed changed an already observed current-session opening')
    return {'baseline_rows': old.height, 'seeded_rows': new.height,
            'new_candidate_keys': new.join(old.select(KEYS), on=KEYS, how='anti').height,
            'old_keys_changed': joined.filter(~unchanged).height,
            'old_session_open_keys_changed': 0}


def build(root: Path, plan_path: Path, history_root: Path, target_start: date, output: Path) -> dict:
    root, plan_path, history_root, output = (p.resolve() for p in (root, plan_path, history_root, output))
    workspace = root.parents[1]
    if (output.exists() or output.is_symlink()
            or not output.is_relative_to(workspace / 'data_finmind' / 'research')):
        raise ValueError('a new option research data directory is required')
    plan_sha = sha256_file(plan_path)
    plan = json.loads(plan_path.read_bytes())
    previous, target, known_missing = selected_windows(plan, target_start)
    policy_path = Path(plan['policy_path']).resolve()
    policy_sha = plan['policy_sha256']
    if sha256_file(policy_path) != policy_sha:
        raise ValueError('planned policy changed')
    windows, linked_pins = candidate_policy(policy_path)
    parents, pinned_sources = [], []
    for group in (previous, target):
        parent = history_root / f"{group['start']}_{group['end']}"
        check_planned_receipts(root, group)
        parent_check = verify_window(parent, group, policy_sha)
        parents.append({'window': [group['start'], group['end']], **parent_check})
        for planned in group['source_receipt_pins']:
            pin = pin_source(root, date.fromisoformat(planned['date']))
            if (pin['sha256'] != planned['parquet_sha256']
                    or pin['receipt_sha256'] != planned['receipt_sha256']):
                raise ValueError('native source changed from the original plan')
            pinned_sources.append(pin)
    implementation_paths = [Path(__file__).resolve(),
        Path('scripts/build_taifex_option_finmind_reference_window.py').resolve(),
        Path('scripts/build_taifex_option_reference_source_window.py').resolve(),
        Path('stockagent/data/tw_option_finmind_reference.py').resolve(),
        Path('stockagent/data/tw_option_reference_candidates.py').resolve(),
        Path('downloader/finmind_option_contract_keys.py').resolve()]
    implementation_pins = {str(path): sha256_file(path) for path in implementation_paths}
    initial_capacity = capacity_check(output.parent)
    state, outputs, counts = None, [], []
    for source in pinned_sources:
        day = date.fromisoformat(source['day'])
        current, stats = candidate_rows(pl.read_parquet(source['path']), day=day,
            source_sha256=source['sha256'], candidate_products=set(windows))
        if current.is_empty():
            raise ValueError('a planned source day has no valid candidate observations')
        result, state = derive_day(state, current, day, windows)
        if day >= target_start:
            outputs.append(result)
        counts.append({'day': day.isoformat(), 'seed_only': day < target_start,
                       'candidate_rows': stats['candidate_rows'], 'state_rows': state.height})
    result = pl.concat(outputs).sort(KEYS)
    if result.estimated_size() > 64 * 1024**2:
        raise ValueError('boundary output exceeds bounded window size')
    old = pl.read_parquet(Path(parents[1]['manifest']).parent / 'observed_references.parquet')
    comparison = compare(old, result)
    first_day = result.filter((pl.col('decision_date') == target_start) & (pl.col('decision_time') == '08:45'))
    old_first = old.filter((pl.col('decision_date') == target_start) & (pl.col('decision_time') == '08:45'))
    comparison.update(first_day_0845_before=old_first.height, first_day_0845_seeded=first_day.height)
    if (sha256_file(plan_path) != plan_sha or sha256_file(policy_path) != policy_sha
            or any(sha256_file(Path(path)) != digest for path, digest in linked_pins.items())
            or any(sha256_file(Path(path)) != digest for path, digest in implementation_pins.items())
            or any(sha256_file(Path(pin['path'])) != pin['sha256']
                   or sha256_file(Path(pin['receipt_path'])) != pin['receipt_sha256'] for pin in pinned_sources)
            or any(sha256_file(Path(parent['manifest'])) != parent['manifest_sha256'] for parent in parents)):
        raise ValueError('a source or implementation changed during the rebuild')
    publication_capacity = capacity_check(output.parent)
    output.mkdir(parents=True, exist_ok=False)
    payload = output / 'observed_references.parquet'
    with tempfile.NamedTemporaryFile(dir=output, prefix='.reference-', suffix='.tmp', delete=False) as handle:
        temporary = Path(handle.name)
    try:
        result.write_parquet(temporary, compression='zstd', statistics=True)
        if not result.equals(pl.read_parquet(temporary)):
            raise ValueError('independent seeded-reference readback differs')
        durable_replace(temporary, payload)
    finally:
        temporary.unlink(missing_ok=True)
    identity = {'contract': CONTRACT, 'plan_sha256': plan_sha, 'policy_sha256': policy_sha,
                'parent_manifest_sha256s': [x['manifest_sha256'] for x in parents],
                'source_sha256s': [x['sha256'] for x in pinned_sources],
                'implementation_sha256': implementation_pins}
    fingerprint = hashlib.sha256(json.dumps(identity, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    receipt = {'contract': CONTRACT, 'generated_at_utc': datetime.now(timezone.utc).isoformat(),
               'data_fingerprint': fingerprint, 'plan_path': str(plan_path), 'plan_sha256': plan_sha,
               'policy_path': str(policy_path), 'policy_sha256': policy_sha,
               'policy_linked_pins': linked_pins, 'parents': parents, 'source_pins': pinned_sources,
               'implementation_sha256': implementation_pins, 'source_counts': counts,
               'known_missing_official_positive_days_between_windows': known_missing,
               'data_path': str(payload), 'data_sha256': sha256_file(payload), 'rows': result.height,
               'comparison': comparison, 'capacity_at_start': initial_capacity,
               'capacity_before_publication': publication_capacity, 'training_ready': False,
               'limitations': [
                   'Only one preceding source window supplies carry; still older real observations are unknown.',
                   'Missing calendar/source days and unknown vendor completeness can hide a later actual trade.',
                   'A carried observation is a known source trade, not proof that no later trade occurred.',
                   'Source event timestamps do not prove historical receive time or point-in-time availability.',
                   'Retrospective session policy and keys do not prove listing, generation, expiry or eligibility.',
                   'No quote, execution, position, margin, settlement or training admission is implied.']}
    atomic_write_json(output / 'manifest.json', receipt)
    return receipt


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path('data_finmind/complement'))
    parser.add_argument('--plan', type=Path, required=True)
    parser.add_argument('--history-root', type=Path, required=True)
    parser.add_argument('--target-start', type=date.fromisoformat, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    receipt = build(args.root, args.plan, args.history_root, args.target_start, args.output)
    print(json.dumps({'output': str(args.output), **receipt['comparison'],
                      'training_ready': False}, ensure_ascii=False))


if __name__ == '__main__':
    main()
