#!/usr/bin/env python3
"""One-pass, read-only WSL allocated-space census; never a retirement proof.

Split selected large directories into disjoint arguments so completed parts
are visible while one du process retains a single hardlink accounting set.
The scan is live, not an atomic filesystem snapshot. Results and errors stay
in an operator-private output directory, not in a public service response.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import time


DEFAULT_SPLITS = (
    ".", "root", "root/stockAgent", "root/stockAgent/artifacts",
    "root/stockAgent/artifacts/cache", "root/stockAgent/artifacts/benchmarks",
    "root/stockAgent/artifacts/benchmarks/workflow_speed_optimization_20261021",
    "root/stockAgent/data_finmind", "root/stockAgent/data_finmind/complement",
    "root/stockAgent/data_finmind/complement/parquet",
    "root/stockAgent/data_finmind/complement/parquet/USStockPriceMinute",
    "root/stockAgent/data_finmind/complement/parquet/USStockPrice",
    "root/stockAgent/data_finmind/complement/parquet/UKStockPrice",
    "root/stockAgent/data_finmind/complement/parquet/TaiwanStockKBar",
    "root/.shioaji", "root/.shioaji/data", "root/.shioaji/data/kbars",
    "root/.shioaji/data/kbars/TSE", "root/.shioaji/data/kbars/OTC",
    "root/.shioaji/data/kbars/TAIFEX", "srv", "srv/stockagent-live",
    "srv/stockagent-packed-materialized", "var", "var/lib",
    "var/lib/stockagent",
)
CONTRACT = "stockagent.wsl_allocated_space_census.v1"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _identity(info: os.stat_result) -> dict:
    return {"device": info.st_dev, "inode": info.st_ino, "mode": info.st_mode,
            "links": info.st_nlink, "allocated_bytes": info.st_blocks * 512,
            "mtime_ns": info.st_mtime_ns, "ctime_ns": info.st_ctime_ns}


def _mount_targets() -> list[Path]:
    """Use mountinfo rather than following a bind mount on the same device."""
    targets = []
    with open('/proc/self/mountinfo', encoding='utf-8') as stream:
        for line in stream:
            parts = line.split()
            if len(parts) < 6 or '-' not in parts:
                raise ValueError('invalid mountinfo')
            name = re.sub(r'\\([0-7]{3})', lambda m: chr(int(m.group(1), 8)), parts[4])
            targets.append(Path(name))
    return targets


def _priority(path: Path, root: Path) -> tuple[int, str]:
    relative = path.relative_to(root).as_posix()
    if relative.startswith('root/stockAgent/artifacts/'):
        return 0, relative
    # The finite Financial-provider symbol trees can hold millions of tiny
    # files. Report other roots first, then emit each selected key subtree
    # individually while retaining the *same* du hardlink-accounting set.
    if relative.startswith('root/stockAgent/data_finmind/complement/parquet/'):
        return 3, relative
    if relative.startswith('root/stockAgent/'):
        return 1, relative
    if relative.startswith('srv/') or relative.startswith('var/lib/stockagent/'):
        return 2, relative
    if relative.startswith('root/.shioaji/data/kbars/'):
        return 4, relative
    return 3, relative


def make_plan(root: Path, splits=DEFAULT_SPLITS, mounts=None) -> dict:
    root = root.absolute()
    base = root.lstat()
    if not stat.S_ISDIR(base.st_mode) or root.is_symlink():
        raise ValueError('census root must be a real directory')
    split_set = {root / value for value in splits}
    split_set.add(root)
    mount_set = {Path(p) for p in (mounts if mounts is not None else _mount_targets())
                 if Path(p) != root and root in Path(p).parents}
    # A nested mount is excluded even if its device matches the WSL root.
    root_rows, expanded, excluded, errors = [], [], [], []

    def visit(path: Path) -> None:
        try:
            info = path.lstat()
        except OSError as error:
            errors.append({'path': str(path), 'error': type(error).__name__})
            return
        if path != root and (path in mount_set or info.st_dev != base.st_dev):
            excluded.append(str(path))
            return
        is_dir = stat.S_ISDIR(info.st_mode)
        if path not in split_set or not is_dir:
            root_rows.append({'path': str(path), 'directory': is_dir, **_identity(info)})
            return
        expanded.append({'path': str(path), **_identity(info)})
        try:
            with os.scandir(path) as entries:
                children = sorted((Path(entry.path) for entry in entries), key=os.fsencode)
        except OSError as error:
            errors.append({'path': str(path), 'error': type(error).__name__})
            return
        for child in children:
            visit(child)

    visit(root)
    root_rows.sort(key=lambda row: _priority(Path(row['path']), root))
    return {'contract': CONTRACT, 'root': str(root), 'root_device': base.st_dev,
            'created_at_utc': _now(), 'roots': root_rows, 'expanded_directories': expanded,
            'excluded_mounts_or_devices': excluded,
            'nested_mount_exclusions': [str(p) for p in sorted(mount_set, key=os.fsencode)],
            'planning_errors': errors, 'source_payload_reads': False,
            'authorizes_retirement': False}


def parse_records(raw: bytes, planned: set[str]) -> tuple[dict[str, int], bool]:
    """Ignore an incomplete final write while a running process owns the file."""
    records, complete = {}, raw.endswith(b'\0') or not raw
    for entry in raw.split(b'\0')[:-1]:
        amount, sep, name = entry.partition(b'\t')
        path = os.fsdecode(name)
        if not sep or not amount.isdigit() or path not in planned or path in records:
            raise ValueError('unexpected or duplicated du record')
        records[path] = int(amount)
    return records, complete


def summarize(plan: dict, raw: bytes, *, returncode: int | None = None) -> dict:
    root = Path(plan['root'])
    rows = plan['roots']
    expected = {row['path'] for row in rows}
    records, last_record_complete = parse_records(raw, expected)
    total_by_path = defaultdict(int)

    def add(path: Path, amount: int) -> None:
        while True:
            total_by_path[str(path)] += amount
            if path == root:
                break
            if root not in path.parents:
                raise ValueError('planned census path outside root')
            path = path.parent

    for row in plan['expanded_directories']:
        add(Path(row['path']), row['allocated_bytes'])
    for path, amount in records.items():
        add(Path(path), amount)
    missing = expected - records.keys()
    omitted_links, unresolved = [], []
    if returncode is not None:
        for row in rows:
            if row['path'] not in missing:
                continue
            try:
                now = Path(row['path']).lstat()
                same = (now.st_dev == row['device'] and now.st_ino == row['inode']
                        and now.st_nlink > 1 and not row['directory'])
            except OSError:
                same = False
            (omitted_links if same else unresolved).append(row['path'])
    changed_expanded = []
    if returncode is not None:
        for row in plan['expanded_directories']:
            try:
                now = Path(row['path']).lstat()
                if (now.st_dev, now.st_ino, now.st_mtime_ns, now.st_ctime_ns) != (
                        row['device'], row['inode'], row['mtime_ns'], row['ctime_ns']):
                    changed_expanded.append(row['path'])
            except OSError:
                changed_expanded.append(row['path'])
    state = 'running' if returncode is None else 'complete'
    if returncode is not None and (returncode != 0 or unresolved or changed_expanded
                                   or plan['planning_errors'] or not last_record_complete):
        state = 'partial'
    return {'contract': CONTRACT, 'state': state, 'root': str(root),
            'observed_at_utc': _now(), 'created_at_utc': plan['created_at_utc'],
            'returncode': returncode, 'planned_roots': len(rows),
            'completed_records': len(records), 'omitted_hardlink_operands': len(omitted_links),
            'unresolved_roots': unresolved, 'changed_expanded_directories': changed_expanded,
            'planning_errors': plan['planning_errors'],
            'included_allocated_bytes': total_by_path.get(str(root), 0),
            'metric': 'allocated_bytes_hardlinks_counted_once_in_one_du_process',
            'scope': 'live_wsl_filesystem_namespace_not_an_atomic_snapshot',
            'reclaimable_bytes': None, 'authorizes_retirement': False,
            'directory_totals': dict(total_by_path)}


def _write_new(path: Path, data: bytes) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, 'wb') as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())


def scan(root: Path, output: Path, splits=DEFAULT_SPLITS, mounts=None) -> int:
    output.mkdir(mode=0o700, parents=True, exist_ok=False)
    os.chmod(output, 0o700)
    plan = make_plan(root, splits, mounts)
    _write_new(output / 'plan.json', json.dumps(plan, ensure_ascii=True).encode() + b'\n')
    _write_new(output / 'roots.nul', b''.join(os.fsencode(row['path']) + b'\0' for row in plan['roots']))
    command = ['nice', '-n', '19', 'ionice', '-c', '3', 'du', '-x', '-B1', '-s', '--null']
    command.extend('--exclude=' + p for p in plan['nested_mount_exclusions'])
    command.append('--files0-from=' + str(output / 'roots.nul'))
    print(json.dumps({'event': 'census-started', 'planned_roots': len(plan['roots']),
                      'planning_errors': len(plan['planning_errors']), 'at_utc': _now()}), flush=True)
    start = time.monotonic()
    with (output / 'records.nul').open('xb', buffering=0) as out, (output / 'errors.log').open('xb', buffering=0) as err:
        os.chmod(output / 'records.nul', 0o600)
        os.chmod(output / 'errors.log', 0o600)
        result = subprocess.run(command, stdout=out, stderr=err, check=False)
        os.fsync(out.fileno())
        os.fsync(err.fileno())
    report = summarize(plan, (output / 'records.nul').read_bytes(), returncode=result.returncode)
    report['elapsed_seconds'] = time.monotonic() - start
    _write_new(output / 'result.json', json.dumps(report, ensure_ascii=True).encode() + b'\n')
    print(json.dumps({'event': 'census-finished', 'state': report['state'],
                      'included_allocated_bytes': report['included_allocated_bytes'],
                      'completed_records': report['completed_records'],
                      'planned_roots': report['planned_roots']}), flush=True)
    return 0 if report['state'] == 'complete' else 75


def progress(output: Path, limit: int, *, under: Path | None = None,
             max_depth: int = 3) -> dict:
    result_path = output / 'result.json'
    plan = json.loads((output / 'plan.json').read_text())
    try:
        raw = (output / 'records.nul').read_bytes()
    except FileNotFoundError:
        raw = b''
    if result_path.exists():
        report = json.loads(result_path.read_text())
    else:
        report = summarize(plan, raw)
    root = Path(report['root'])
    target = root if under is None else under.absolute()
    if target != root and root not in target.parents:
        raise ValueError('selected path is outside the census root')
    records, _ = parse_records(raw, {row['path'] for row in plan['roots']})
    planned_counts, recorded_counts, accounted_counts = defaultdict(int), defaultdict(int), defaultdict(int)
    # GNU du can suppress a later top-level hardlink operand. The final scan
    # reconciles such omissions by inode; while running, never guess that a
    # not-yet-emitted operand has already been accounted for.
    accounted = set(records)
    if report['returncode'] is not None:
        unresolved = set(report['unresolved_roots'])
        accounted.update(row['path'] for row in plan['roots']
                         if row['path'] not in records and row['path'] not in unresolved)
    for row in plan['roots']:
        path = Path(row['path'])
        while True:
            planned_counts[str(path)] += 1
            recorded_counts[str(path)] += int(row['path'] in records)
            accounted_counts[str(path)] += int(row['path'] in accounted)
            if path == root:
                break
            path = path.parent
    paths = []
    totals = report.pop('directory_totals')
    if str(target) not in planned_counts and str(target) not in totals:
        raise ValueError('selected path is not present in the frozen census plan')
    target_planned = planned_counts[str(target)]
    target_recorded = recorded_counts[str(target)]
    report['selected_subtree_observation'] = {
        'observed_allocated_bytes': totals.get(str(target)),
        'planned_roots': target_planned, 'recorded_roots': target_recorded,
        'all_planned_operands_recorded': target_planned == target_recorded,
        'accounted_roots': accounted_counts[str(target)],
        'all_planned_operands_accounted': target_planned == accounted_counts[str(target)],
    }
    incomplete_children = []
    for name, planned in planned_counts.items():
        path = Path(name)
        if path != target and path.parent == target and planned != accounted_counts[name]:
            incomplete_children.append({'path': name, 'planned_roots': planned,
                                        'recorded_roots': recorded_counts[name]})
    for name, size in totals.items():
        path = Path(name)
        if path == target or target not in path.parents:
            continue
        depth = len(path.relative_to(target).parts)
        if depth <= max_depth:
            planned, recorded = planned_counts[name], recorded_counts[name]
            paths.append({'path': name, 'allocated_bytes': size, 'depth': depth,
                          'planned_roots': planned, 'recorded_roots': recorded,
                          'all_planned_operands_recorded': planned == recorded,
                          'accounted_roots': accounted_counts[name],
                          'all_planned_operands_accounted': planned == accounted_counts[name]})
    report['selected_subtree'] = str(target)
    report['largest_observed_paths'] = sorted(paths, key=lambda row: (-row['allocated_bytes'], row['path']))[:limit]
    report['incomplete_direct_children_count'] = len(incomplete_children)
    report['incomplete_direct_children'] = sorted(incomplete_children, key=lambda row: row['path'])[:limit]
    report['incomplete_direct_children_truncated'] = len(incomplete_children) > limit
    report['partial_scan_is_not_a_point_in_time_total'] = True
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    run = sub.add_parser('scan')
    run.add_argument('--root', type=Path, default=Path('/'))
    run.add_argument('--output', type=Path, required=True)
    show = sub.add_parser('progress')
    show.add_argument('--output', type=Path, required=True)
    show.add_argument('--limit', type=int, default=30)
    show.add_argument('--under', type=Path, help='Show observed descendants of this path')
    show.add_argument('--max-depth', type=int, default=3,
                      help='Limit displayed path depth relative to --under (default 3)')
    args = parser.parse_args()
    if args.command == 'scan':
        return scan(args.root, args.output)
    if not 1 <= args.limit <= 500:
        parser.error('--limit must be between 1 and 500')
    if not 1 <= args.max_depth <= 20:
        parser.error('--max-depth must be between 1 and 20')
    print(json.dumps(progress(args.output, args.limit, under=args.under,
                              max_depth=args.max_depth), ensure_ascii=False, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
