#!/usr/bin/env python3
"""Resume receipt-verified, source-only option carry across history windows.

Uses the existing bounded boundary builder; no provider requests are made.
Incomplete parent windows remain pending and never become zero observations.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import fcntl
import json
from pathlib import Path
import time

import pyarrow.parquet as pq

from downloader.artifact_io import atomic_write_json, sha256_file
from scripts.build_finmind_option_reference_boundary import CONTRACT, build, selected_windows
from scripts.build_taifex_option_finmind_reference_window import CapacityNotAdmitted, capacity_check
from scripts.run_finmind_option_reference_history import check_planned_receipts, verify_window


def verify(output: Path, plan_path: Path, target: dict, root: Path) -> dict:
    manifest = output / 'manifest.json'
    if output.is_symlink() or manifest.is_symlink() or not manifest.is_file():
        raise ValueError('boundary output has no completed regular manifest')
    doc = json.loads(manifest.read_bytes())
    path = Path(doc.get('data_path', ''))
    if (doc.get('contract') != CONTRACT or doc.get('training_ready') is not False
            or doc.get('plan_sha256') != sha256_file(plan_path)
            or len(doc.get('parents', [])) != 2
            or doc['parents'][1]['window'] != [target['start'], target['end']]
            or path.is_symlink() or path.resolve().parent != output.resolve() or not path.is_file()
            or sha256_file(path) != doc.get('data_sha256')
            or pq.ParquetFile(path).metadata.num_rows != doc.get('rows')):
        raise ValueError('boundary output identity or payload changed')
    for name, digest in doc.get('implementation_sha256', {}).items():
        if sha256_file(Path(name)) != digest:
            raise ValueError('boundary implementation changed')
    if not doc.get('implementation_sha256'):
        raise ValueError('boundary output has no implementation identity')
    policy = Path(doc['policy_path'])
    if sha256_file(policy) != doc.get('policy_sha256'):
        raise ValueError('boundary policy changed')
    for name, digest in doc.get('policy_linked_pins', {}).items():
        if sha256_file(Path(name)) != digest:
            raise ValueError('boundary policy evidence changed')
    plan = json.loads(plan_path.read_bytes())
    previous, planned_target, _ = selected_windows(plan, date.fromisoformat(target['start']))
    if planned_target != target or doc['policy_sha256'] != plan['policy_sha256']:
        raise ValueError('boundary plan target or policy changed')
    for parent, group in zip(doc['parents'], (previous, target), strict=True):
        check_planned_receipts(root, group)
        checked = verify_window(Path(parent['manifest']).parent, group, doc['policy_sha256'])
        if (parent['window'] != [group['start'], group['end']]
                or checked['manifest_sha256'] != parent['manifest_sha256']):
            raise ValueError('boundary parent manifest changed')
    expected_sources = previous['source_receipt_pins'] + target['source_receipt_pins']
    if len(doc.get('source_pins', [])) != len(expected_sources):
        raise ValueError('boundary source pins are incomplete')
    for pin, expected in zip(doc['source_pins'], expected_sources, strict=True):
        if (pin['day'] != expected['date'] or pin['sha256'] != expected['parquet_sha256']
                or pin['receipt_sha256'] != expected['receipt_sha256']):
            raise ValueError('boundary source differs from the planned source')
        if (sha256_file(Path(pin['path'])) != pin['sha256']
                or sha256_file(Path(pin['receipt_path'])) != pin['receipt_sha256']):
            raise ValueError('boundary source changed')
    return {'manifest': str(manifest), 'manifest_sha256': sha256_file(manifest),
            'rows': doc['rows'], 'data_sha256': doc['data_sha256'], 'comparison': doc['comparison']}


def run(root: Path, plan_path: Path, history_root: Path, output_root: Path, summary_path: Path,
        *, max_wait_seconds: int, poll_seconds: int = 30) -> dict:
    root, plan_path, history_root, output_root, summary_path = (p.resolve() for p in
        (root, plan_path, history_root, output_root, summary_path))
    workspace = root.parents[1]
    if (not output_root.is_relative_to(workspace / 'data_finmind' / 'research')
            or not summary_path.is_relative_to(workspace / 'artifacts' / 'markets')
            or summary_path.exists() or summary_path.is_symlink()
            or not 1 <= max_wait_seconds <= 7 * 86400 or not 1 <= poll_seconds <= 300):
        raise ValueError('bounded wait, new market summary and research output are required')
    plan_sha = sha256_file(plan_path)
    plan = json.loads(plan_path.read_bytes())
    windows = plan.get('windows', [])
    if len(windows) < 2:
        raise ValueError('at least two planned source windows are required')
    for window in windows[1:]:
        selected_windows(plan, date.fromisoformat(window['start']))
    capacity_check(output_root)
    output_root.mkdir(parents=True, exist_ok=True)
    results: dict[str, dict] = {}
    deadline = time.monotonic() + max_wait_seconds
    deferred = False
    with (output_root / '.writer.lock').open('a+') as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        while len(results) < len(windows) - 1:
            if sha256_file(plan_path) != plan_sha:
                raise ValueError('history plan changed during boundary processing')
            changed = False
            for index, target in enumerate(windows[1:], 1):
                key = target['start']
                if key in results:
                    continue
                previous = windows[index - 1]
                parent_dirs = [history_root / f"{item['start']}_{item['end']}" for item in (previous, target)]
                if not all((parent / 'manifest.json').is_file() for parent in parent_dirs):
                    continue
                output = output_root / f"{target['start']}_{target['end']}"
                try:
                    if output.exists() or output.is_symlink():
                        state = 'reused_verified'
                    else:
                        build(root, plan_path, history_root, date.fromisoformat(key), output)
                        state = 'built_verified'
                    result = {'index': index, 'start': key, 'end': target['end'], 'state': state,
                              **verify(output, plan_path, target, root)}
                except CapacityNotAdmitted as error:
                    result = {'index': index, 'start': key, 'end': target['end'],
                              'state': 'capacity_deferred', 'capacity': error.status}
                    deferred = True
                except Exception as error:
                    result = {'index': index, 'start': key, 'end': target['end'], 'state': 'failed',
                              'error_type': type(error).__name__, 'error': str(error)}
                results[key] = result
                changed = True
                print(json.dumps(result, ensure_ascii=False), flush=True)
                if deferred:
                    break
            if deferred or len(results) == len(windows) - 1 or time.monotonic() >= deadline:
                break
            if not changed:
                print(json.dumps({'state': 'waiting_for_parent_windows',
                                  'completed_or_failed': len(results),
                                  'planned_boundaries': len(windows) - 1}), flush=True)
                time.sleep(min(poll_seconds, max(0, deadline - time.monotonic())))
        fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
    missing = [window['start'] for window in windows[1:] if window['start'] not in results]
    summary = {'contract': 'finmind_option_reference_boundaries_run_v1',
               'generated_at_utc': datetime.now(timezone.utc).isoformat(),
               'plan_path': str(plan_path), 'plan_sha256': plan_sha,
               'implementation_sha256': sha256_file(Path(__file__)),
               'planned_boundaries': len(windows) - 1, 'attempted_boundaries': len(results),
               'results': list(results.values()), 'pending_parent_starts': missing,
               'capacity_deferred': deferred, 'training_ready': False,
               'limitations': ['Local retained sources only; no provider calls.',
                   'Each target carries at most its immediately preceding source window.',
                   'Source gaps, historical availability, listing and execution remain unverified.']}
    atomic_write_json(summary_path, summary)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path('data_finmind/complement'))
    parser.add_argument('--plan', type=Path, required=True)
    parser.add_argument('--history-root', type=Path, required=True)
    parser.add_argument('--output-root', type=Path, required=True)
    parser.add_argument('--summary', type=Path, required=True)
    parser.add_argument('--max-wait-seconds', type=int, default=86400)
    parser.add_argument('--poll-seconds', type=int, default=30)
    args = parser.parse_args()
    try:
        result = run(args.root, args.plan, args.history_root, args.output_root, args.summary,
                     max_wait_seconds=args.max_wait_seconds, poll_seconds=args.poll_seconds)
    except CapacityNotAdmitted as error:
        print(json.dumps({'state': 'capacity_deferred', 'capacity': error.status}), flush=True)
        raise SystemExit(75) from error
    if result['capacity_deferred'] or result['pending_parent_starts']:
        raise SystemExit(75)
    if any(row['state'] == 'failed' for row in result['results']):
        raise SystemExit(1)


if __name__ == '__main__':
    main()
