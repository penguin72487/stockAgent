#!/usr/bin/env python3
"""Refresh all declared ablation report surfaces on each completed fold.

The training scheduler delegates report ownership to this observer. A view
manifest pins every legacy and current output family; mirrors copy the same
generated report rather than rendering a competing version.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
from downloader.artifact_io import atomic_write_bytes, atomic_write_json


def report_input_signature(root: Path) -> str:
    """Observe only case summaries and fold markers, not large NPZ payloads."""
    items = []
    for summary in sorted(root.glob('*/summary.json')):
        paths = [summary, *sorted(summary.parent.glob('fold_*/fold_complete.json'))]
        for path in paths:
            try:
                stat = path.stat()
            except OSError:
                # The next observation will retry a concurrently changing file.
                continue
            items.append((str(path.relative_to(root)), stat.st_size, stat.st_mtime_ns))
    return hashlib.sha256(json.dumps(items).encode()).hexdigest()


def _report_views(args):
    if args.report_spec is None:
        return [dict(name='test_year', mode='test_year', split='test', prefix='reverse_test',
                     output=args.output_dir, scope_label='First test year from each independent fold account')]
    payload = json.loads(args.report_spec.read_text())
    if payload.get('contract') != 'ablation_report_views_v1' or not payload.get('views'):
        raise ValueError('invalid ablation report view manifest')
    views, names, outputs = [], set(), set()
    for item in payload['views']:
        name, prefix = item['name'], item['prefix']
        if any(re.fullmatch(r'[a-z0-9][a-z0-9_-]*', value) is None for value in (name, prefix)):
            raise ValueError('unsafe report view name or prefix')
        mode, split = item['mode'], item.get('split', 'test')
        if mode not in {'test_year', 'completed', 'mirror'} or split not in {'val', 'test', 'deployment'}:
            raise ValueError('unsupported report view')
        if mode == 'test_year' and split != 'test':
            raise ValueError('test-year view requires test split')
        subdir = Path(item.get('output_subdir', ''))
        if subdir.is_absolute() or '..' in subdir.parts:
            raise ValueError('report output must remain inside the study')
        output = (args.root / subdir).resolve()
        if not output.is_relative_to(args.root.resolve()):
            raise ValueError('report output escapes the study')
        identity = output, prefix, mode == 'completed'
        if name in names or identity in outputs:
            raise ValueError('duplicate report name or output owner')
        if mode == 'mirror' and item.get('source_view') not in names:
            raise ValueError('mirror source must be a preceding report view')
        names.add(name)
        outputs.add(identity)
        views.append(dict(item, name=name, prefix=prefix, mode=mode, split=split, output=output))
    declared = {(view['output'], view['prefix'] + '_risk_return_by_fold')
                for view in views if view['mode'] == 'completed'}
    missing = [path.name for path in args.root.glob('*_risk_return_by_fold')
               if (args.root.resolve(), path.name) not in declared]
    if missing:
        raise ValueError('unowned existing risk-return report families: ' + ', '.join(missing))
    return views


def _refresh_view(args, view, env, completed_views):
    mode, output, prefix = view['mode'], view['output'], view['prefix']
    directory = output / (f'{prefix}_risk_return_by_fold' if mode == 'completed' else f'{prefix}_by_test_year')
    status_path = directory / ('plot_status.json' if mode == 'completed' else 'alignment_status.json')
    if mode == 'mirror':
        source, report = completed_views[view['source_view']]
        files = [*report['png_files'], 'test_year_fold_metrics.csv', 'strategy_zh_en.csv']
        for filename in files:
            relative = Path(filename)
            if relative.is_absolute() or '..' in relative.parts:
                raise ValueError('unsafe mirrored report member')
            path = source / relative
            if path.is_file():
                target = directory / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                atomic_write_bytes(target, path.read_bytes())
        atomic_write_json(status_path, report)
    else:
        command = [sys.executable, str(REPO_ROOT / 'scripts/plot_ablation_analysis.py'),
                   '--root', str(args.root), '--output-dir', str(output),
                   '--split', view['split'], '--prefix', prefix, '--comparison-mode', mode,
                   '--scope-label', view.get('scope_label', view['split']),
                   '--calendar-receipt', str(args.calendar_receipt)]
        result = subprocess.run(command, env=env, check=False)
        if result.returncode:
            raise subprocess.CalledProcessError(result.returncode, command)
        report = json.loads(status_path.read_text())
    if report.get('rendering_contract') != 'canonical_ablation_risk_return_v1':
        raise ValueError('unexpected risk-return rendering contract')
    completed_views[view['name']] = directory, report
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--output-dir', type=Path, required=True)
    parser.add_argument('--calendar-receipt', type=Path, required=True)
    parser.add_argument('--queue-status', type=Path, required=True)
    parser.add_argument('--report-spec', type=Path)
    parser.add_argument('--interval-seconds', type=float, default=10.)
    parser.add_argument('--once', action='store_true')
    args = parser.parse_args()
    if args.interval_seconds <= 0:
        raise ValueError('interval-seconds must be positive')
    args.output_dir.mkdir(parents=True, exist_ok=True)
    status_path = args.output_dir / 'live_report_watch_status.json'
    last_success = None
    while True:
        signature = report_input_signature(args.root)
        if args.report_spec is not None:
            signature = hashlib.sha256((signature + hashlib.sha256(args.report_spec.read_bytes()).hexdigest()).encode()).hexdigest()
        if signature != last_success:
            started = time.monotonic()
            env = dict(os.environ, OMP_NUM_THREADS='1', OPENBLAS_NUM_THREADS='1', MKL_NUM_THREADS='1')
            returncode, error, completed_views, view_receipts = 0, '', {}, {}
            for view in _report_views(args):
                try:
                    report = _refresh_view(args, view, env, completed_views)
                    view_receipts[view['name']] = dict(state='current', output=str(completed_views[view['name']][0]),
                        observations=report.get('completed_observations'), png_files=len(report.get('png_files', [])),
                        input_sha256=report.get('input_sha256'), rendering_contract=report['rendering_contract'])
                except (OSError, ValueError, KeyError, subprocess.CalledProcessError) as exc:
                    returncode = getattr(exc, 'returncode', 1)
                    error = str(exc)
                    view_receipts[view['name']] = dict(state='retry_pending', error=error)
            readback_signature = report_input_signature(args.root)
            if args.report_spec is not None:
                readback_signature = hashlib.sha256((readback_signature + hashlib.sha256(args.report_spec.read_bytes()).hexdigest()).encode()).hexdigest()
            report = next(iter(completed_views.values()))[1] if completed_views else {}
            state = ('current' if signature == readback_signature else 'refresh_pending') if returncode == 0 else 'retry_pending'
            receipt = {'state': state,
                'input_signature': signature, 'returncode': returncode, 'error': error,
                'readback_input_signature': readback_signature,
                'views': view_receipts,
                'included_observations': report.get('completed_observations'),
                'included_folds': [{'variant': row['variant'], 'fold_id': row['fold_id'],
                                    'test_year': row['test_year']} for row in report.get('records', [])],
                'rendering_contract': report.get('rendering_contract'),
                'duration_seconds': time.monotonic() - started, 'checked_at_unix_ns': time.time_ns(),
                'charts': str(args.output_dir / 'reverse_test_by_test_year')}
            atomic_write_json(status_path, receipt)
            with (args.output_dir / 'live_report_refreshes.jsonl').open('a', encoding='utf-8') as handle:
                handle.write(json.dumps(receipt) + '\n')
            print(json.dumps({'state': state, 'included_observations': receipt['included_observations'],
                              'returncode': returncode, 'input_signature': signature}), flush=True)
            if returncode == 0:
                last_success = signature
            if args.once:
                return returncode
        if args.queue_status.is_file():
            try:
                queue = json.loads(args.queue_status.read_text())
            except (OSError, json.JSONDecodeError):
                queue = {}
            if queue.get('phase') == 'queue_exited' and signature == last_success:
                return 0
        time.sleep(args.interval_seconds)


if __name__ == '__main__':
    raise SystemExit(main())
