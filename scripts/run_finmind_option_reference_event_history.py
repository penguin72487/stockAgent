#!/usr/bin/env python3
"""Build the pinned history plan's compact option-event partitions.

The existing workflow supervisor owns process lifecycle; this is a source-only
dataset producer. It reuses the native source builder and never calls a vendor.
Each partition is independently verifiable and may be resumed without changing
its receipt or silently accepting changed inputs.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import fcntl
import json
from pathlib import Path

import pyarrow.parquet as pq

from downloader.artifact_io import atomic_write_json, sha256_file
from scripts.build_finmind_option_reference_event_index import build
from scripts.build_taifex_option_finmind_reference_window import CapacityNotAdmitted, capacity_check
from scripts.run_finmind_option_reference_history import check_planned_receipts, window_dates
from stockagent.data.tw_option_reference_event_index import CONTRACT


def verify(output: Path, plan_path: Path, plan: dict, window: dict, root: Path) -> dict:
    manifest = output / 'manifest.json'
    if output.is_symlink() or manifest.is_symlink() or not manifest.is_file():
        raise ValueError('event partition has no complete regular manifest')
    doc = json.loads(manifest.read_bytes())
    sources = doc.get('source_pins', [])
    expected = window['source_receipt_pins']
    if (doc.get('contract') != CONTRACT or doc.get('training_ready') is not False
            or doc.get('plan_sha256') != sha256_file(plan_path)
            or doc.get('policy_sha256') != plan['policy_sha256']
            or len(sources) != len(expected)
            or any(s.get('day') != e['date'] or s.get('sha256') != e['parquet_sha256']
                   or s.get('receipt_sha256') != e['receipt_sha256']
                   for s, e in zip(sources, expected, strict=True))):
        raise ValueError('event partition identity differs from its plan')
    check_planned_receipts(root, window)
    for source in sources:
        if (sha256_file(Path(source['path'])) != source['sha256']
                or sha256_file(Path(source['receipt_path'])) != source['receipt_sha256']):
            raise ValueError('event partition native source changed')
    for name, digest in (doc.get('implementation_sha256') or {}).items():
        if sha256_file(Path(name)) != digest:
            raise ValueError('event partition implementation changed')
    if not doc.get('implementation_sha256'):
        raise ValueError('event partition lacks implementation identity')
    for name, digest in doc.get('policy_linked_pins', {}).items():
        if sha256_file(Path(name)) != digest:
            raise ValueError('event partition policy evidence changed')
    path = Path(doc['data_path'])
    if (path.is_symlink() or path.resolve().parent != output.resolve() or not path.is_file()
            or sha256_file(path) != doc.get('data_sha256')
            or pq.ParquetFile(path).metadata.num_rows != doc.get('rows')):
        raise ValueError('event partition payload differs from receipt')
    return {'manifest': str(manifest), 'manifest_sha256': sha256_file(manifest),
            'data_sha256': doc['data_sha256'], 'rows': doc['rows']}


def run(root: Path, plan_path: Path, output_root: Path, summary_path: Path,
        *, limit: int | None = None) -> dict:
    if output_root.is_symlink() or summary_path.is_symlink():
        raise ValueError('new regular output locations are required')
    root, plan_path, output_root, summary_path = (p.resolve() for p in
                                                  (root, plan_path, output_root, summary_path))
    workspace = root.parents[1]
    if (not output_root.is_relative_to(workspace / 'data_finmind' / 'research')
            or not summary_path.is_relative_to(workspace / 'artifacts' / 'markets')
            or summary_path.exists() or (limit is not None and limit < 1)):
        raise ValueError('research output, new market summary and positive limit required')
    plan_sha = sha256_file(plan_path)
    plan = json.loads(plan_path.read_bytes())
    if (plan.get('contract') != 'finmind_option_reference_history_plan_v1'
            or plan.get('training_ready') is not False):
        raise ValueError('research-only pinned history plan required')
    policy = Path(plan['policy_path'])
    if sha256_file(policy) != plan['policy_sha256']:
        raise ValueError('plan policy changed')
    inventory_pins = {}
    for pin in plan['source_inventory_pins']:
        for field in ('summary', 'inventory'):
            path, digest = Path(pin[f'{field}_path']), pin[f'{field}_sha256']
            if sha256_file(path) != digest:
                raise ValueError('source inventory changed from the history plan')
            inventory_pins[str(path)] = digest
    windows = plan['windows'][:limit]
    days = [day for window in windows for day in window_dates(window)]
    if not days or days != sorted(set(days)):
        raise ValueError('history windows are empty, overlapping or unordered')
    capacity_check(output_root)
    output_root.mkdir(parents=True, exist_ok=True)
    results, deferred = [], False
    with (output_root / '.writer.lock').open('a+') as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        for index, window in enumerate(windows, 1):
            path = output_root / f"{window['start']}_{window['end']}"
            try:
                if sha256_file(plan_path) != plan_sha or sha256_file(policy) != plan['policy_sha256']:
                    raise ValueError('history plan or policy changed during processing')
                if path.exists() or path.is_symlink():
                    state = 'reused_verified'
                else:
                    build(root, plan_path, window_dates(window), path)
                    state = 'built_verified'
                result = {'index': index, 'start': window['start'], 'end': window['end'],
                          'state': state, **verify(path, plan_path, plan, window, root)}
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
    if any(sha256_file(Path(p)) != h for p, h in inventory_pins.items()):
        raise ValueError('source inventory changed during index generation')
    summary = {'contract': 'finmind_option_reference_event_history_v1',
               'generated_at_utc': datetime.now(timezone.utc).isoformat(),
               'plan_path': str(plan_path), 'plan_sha256': plan_sha,
               'source_inventory_pins': inventory_pins, 'implementation_sha256': sha256_file(Path(__file__)),
               'total_plan_windows': len(plan['windows']), 'selected_windows': len(windows),
               'attempted_windows': len(results), 'results': results, 'capacity_deferred': deferred,
               'known_missing_source_days': plan.get('not_planned_days', []), 'training_ready': False,
               'limitations': [
                   'A partition indexes retained observed trades only; missing days are not empty days.',
                   'Cross-window queries require all relevant partitions and the original source-gap plan.',
                   'Retrospective session windows, expiry and generation do not establish eligibility.',
                   'No executable quote, account, training data or strategy performance is implied.']}
    atomic_write_json(summary_path, summary)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path('data_finmind/complement'))
    parser.add_argument('--plan', type=Path, required=True)
    parser.add_argument('--output-root', type=Path, required=True)
    parser.add_argument('--summary', type=Path, required=True)
    parser.add_argument('--limit', type=int)
    args = parser.parse_args()
    try:
        result = run(args.root, args.plan, args.output_root, args.summary, limit=args.limit)
    except CapacityNotAdmitted as error:
        print(json.dumps({'state': 'capacity_deferred', 'capacity': error.status}), flush=True)
        raise SystemExit(75) from error
    if result['capacity_deferred']:
        raise SystemExit(75)
    if any(row['state'] == 'failed' for row in result['results']):
        raise SystemExit(1)


if __name__ == '__main__':
    main()
