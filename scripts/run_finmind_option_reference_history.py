#!/usr/bin/env python3
"""Process a pinned FinMind option history plan into bounded research windows.

This calls the existing source-window builder, not a provider. Every window
starts with unknown prior state, and no result is admitted to training.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta, timezone
import fcntl
import hashlib
import json
from pathlib import Path

import pyarrow.parquet as pq

from downloader.artifact_io import atomic_write_json, sha256_file
from scripts.build_taifex_option_finmind_reference_window import (
    CONTRACT as WINDOW_CONTRACT,
    CapacityNotAdmitted,
    build,
    capacity_check,
)
from scripts.plan_finmind_option_reference_history import CONTRACT as PLAN_CONTRACT


def window_dates(window: dict) -> list[date]:
    rows = window['source_receipt_pins']
    encoded = json.dumps(rows, sort_keys=True, separators=(',', ':')).encode()
    if hashlib.sha256(encoded).hexdigest() != window['source_group_fingerprint']:
        raise ValueError('window source group fingerprint differs')
    days = [date.fromisoformat(row['date']) for row in rows]
    if (not days or len(days) != window['source_days']
            or days[0].isoformat() != window['start'] or days[-1].isoformat() != window['end']
            or any(b - a != timedelta(days=1) for a, b in zip(days, days[1:]))
            or any(row.get('planned') is not True for row in rows)):
        raise ValueError('invalid window dates or source status')
    return days


def check_planned_receipts(root: Path, window: dict) -> None:
    for row in window['source_receipt_pins']:
        path = root / 'receipts' / 'TaiwanOptionTick' / 'all' / f"{row['date']}.json"
        if path.is_symlink() or not path.is_file() or sha256_file(path) != row['receipt_sha256']:
            raise ValueError(f"planned receipt changed or disappeared: {row['date']}")
        if json.loads(path.read_bytes()).get('sha256') != row['parquet_sha256']:
            raise ValueError(f"planned payload identity changed: {row['date']}")


def verify_window(output: Path, window: dict, policy_sha: str) -> dict:
    manifest_path = output / 'manifest.json'
    if output.is_symlink() or manifest_path.is_symlink() or not manifest_path.is_file():
        raise ValueError('existing output has no completed regular manifest')
    doc = json.loads(manifest_path.read_bytes())
    days = window_dates(window)
    if (doc.get('contract') != WINDOW_CONTRACT or doc.get('training_ready') is not False
            or doc.get('policy_sha256') != policy_sha
            or doc.get('source_dates') != [d.isoformat() for d in days]):
        raise ValueError('output contract, policy or source dates differ')
    pins = doc.get('source_pins', [])
    expected = window['source_receipt_pins']
    if (len(pins) != len(expected) or any(
        pin.get('day') != item['date'] or pin.get('receipt_sha256') != item['receipt_sha256']
        or pin.get('sha256') != item['parquet_sha256'] for pin, item in zip(pins, expected, strict=True)
    )):
        raise ValueError('output source pins differ from plan')
    for name, digest in doc.get('implementation_sha256', {}).items():
        if sha256_file(Path(name)) != digest:
            raise ValueError('output implementation is no longer the running version')
    if not doc.get('implementation_sha256'):
        raise ValueError('output lacks implementation pins')
    data_path = Path(doc['data_path'])
    data = data_path.resolve()
    if (data.parent != output.resolve() or not data.is_file() or data_path.is_symlink()
            or sha256_file(data) != doc['data_sha256']
            or pq.ParquetFile(data).metadata.num_rows != doc['rows']):
        raise ValueError('output payload or independent row readback differs')
    return {'rows': doc['rows'], 'manifest': str(manifest_path),
            'manifest_sha256': sha256_file(manifest_path), 'data_sha256': doc['data_sha256']}


def run(root: Path, plan_path: Path, output_root: Path, summary_path: Path,
        *, limit: int | None = None) -> dict:
    root, plan_path = root.resolve(), plan_path.resolve()
    output_root, summary_path = output_root.resolve(), summary_path.resolve()
    workspace = root.parents[1]
    if (not (output_root.is_relative_to(workspace / 'data_finmind' / 'research'))
            or not summary_path.is_relative_to(workspace / 'artifacts' / 'markets')
            or summary_path.exists() or summary_path.is_symlink()
            or (limit is not None and limit < 1)):
        raise ValueError('research output, new market summary and positive limit are required')
    plan_sha = sha256_file(plan_path)
    plan = json.loads(plan_path.read_bytes())
    if plan.get('contract') != PLAN_CONTRACT or plan.get('training_ready') is not False:
        raise ValueError('a pinned research-only history plan is required')
    policy_path = Path(plan['policy_path']).resolve()
    policy_sha = plan['policy_sha256']
    if sha256_file(policy_path) != policy_sha:
        raise ValueError('history policy changed')
    for pin in plan['source_inventory_pins']:
        if (sha256_file(Path(pin['summary_path'])) != pin['summary_sha256']
                or sha256_file(Path(pin['inventory_path'])) != pin['inventory_sha256']):
            raise ValueError('history inventory changed')
    windows = plan['windows'][:limit]
    all_days = [day for window in windows for day in window_dates(window)]
    if len(set(all_days)) != len(all_days) or all_days != sorted(all_days):
        raise ValueError('history windows overlap or are unordered')
    capacity_check(output_root)
    output_root.mkdir(parents=True, exist_ok=True)
    results = []
    deferred = False
    with (output_root / '.writer.lock').open('a+') as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        for index, window in enumerate(windows, 1):
            output = output_root / f"{window['start']}_{window['end']}"
            try:
                if sha256_file(plan_path) != plan_sha or sha256_file(policy_path) != policy_sha:
                    raise ValueError('plan or policy changed during processing')
                check_planned_receipts(root, window)
                if output.exists() or output.is_symlink():
                    state = 'reused_verified'
                else:
                    build(root, policy_path, window_dates(window), output)
                    state = 'built_verified'
                result = {'index': index, 'start': window['start'], 'end': window['end'],
                          'state': state, **verify_window(output, window, policy_sha)}
            except CapacityNotAdmitted as error:
                result = {'index': index, 'start': window['start'], 'end': window['end'],
                          'state': 'capacity_deferred', 'capacity': error.status}
                deferred = True
            except Exception as error:
                result = {'index': index, 'start': window['start'], 'end': window['end'],
                          'state': 'failed', 'error_type': type(error).__name__, 'error': str(error)}
            results.append(result)
            print(json.dumps(result, ensure_ascii=False), flush=True)
            if deferred:
                break
        fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
    summary = {'contract': 'finmind_option_reference_history_run_v1',
               'generated_at_utc': datetime.now(timezone.utc).isoformat(),
               'plan_path': str(plan_path), 'plan_sha256': plan_sha,
               'policy_path': str(policy_path), 'policy_sha256': policy_sha,
               'implementation_sha256': sha256_file(Path(__file__)),
               'total_plan_windows': len(plan['windows']), 'selected_windows': len(windows),
               'attempted_windows': len(results), 'results': results,
               'capacity_deferred': deferred, 'training_ready': False,
               'limitations': ['Local retained source processing only; no provider call.',
                   'State resets at every window and unknown source day.',
                   'Source completeness, historic delivery, listing and execution remain unverified.']}
    atomic_write_json(summary_path, summary)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path('data_finmind/complement'))
    parser.add_argument('--plan', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--summary', type=Path, required=True)
    parser.add_argument('--limit', type=int)
    args = parser.parse_args()
    try:
        summary = run(args.root, args.plan, args.output, args.summary, limit=args.limit)
    except CapacityNotAdmitted as error:
        print(json.dumps({'state': 'capacity_deferred', 'capacity': error.status}), flush=True)
        raise SystemExit(75) from error
    if summary['capacity_deferred']:
        raise SystemExit(75)
    if any(row['state'] == 'failed' for row in summary['results']):
        raise SystemExit(1)


if __name__ == '__main__':
    main()
