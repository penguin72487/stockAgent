#!/usr/bin/env python3
"""Render the existing WSL census without scanning or authorizing retirement."""
from __future__ import annotations

import argparse
from collections import defaultdict
import csv
import io
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import audit_wsl_storage_usage as census


def _replace_generated(path: Path, content: str) -> None:
    if path.is_symlink():
        raise ValueError('generated report target is redirected')
    fd, name = tempfile.mkstemp(prefix='.' + path.name + '.', dir=path.parent)
    try:
        with os.fdopen(fd, 'w', encoding='utf-8', newline='') as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(name, path)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def _view(plan: dict, raw: bytes, final: dict | None) -> tuple[dict, list[dict]]:
    report = final if final is not None else census.summarize(plan, raw)
    root = Path(plan['root'])
    records, _ = census.parse_records(raw, {row['path'] for row in plan['roots']})
    accounted = set(records)
    if report['returncode'] is not None:
        unresolved = set(report['unresolved_roots'])
        accounted.update(row['path'] for row in plan['roots']
                         if row['path'] not in records and row['path'] not in unresolved)
    counts = defaultdict(lambda: [0, 0, 0])
    for row in plan['roots']:
        path = Path(row['path'])
        while True:
            values = counts[str(path)]
            values[0] += 1
            values[1] += int(row['path'] in records)
            values[2] += int(row['path'] in accounted)
            if path == root:
                break
            path = path.parent
    totals = dict(report['directory_totals'])
    for item in plan['roots']:
        path = item['path']
        if path not in totals:
            # A finished du may intentionally omit an already-counted
            # hardlink, but a not-yet-visited operand is unknown, not zero.
            totals[path] = 0 if path in accounted else None
    rows = []
    for path, amount in totals.items():
        planned, recorded, accounted_count = counts[path]
        rows.append({'path': path, 'allocated_bytes': amount, 'planned_roots': planned,
                     'recorded_roots': recorded, 'accounted_roots': accounted_count,
                     'all_planned_roots_accounted': planned == accounted_count})
    return report, sorted(rows, key=lambda row: (row['allocated_bytes'] is None,
                                                -(row['allocated_bytes'] or 0), row['path']))


def _children(rows: list[dict], parent: Path) -> list[dict]:
    return [row for row in rows if Path(row['path']) != parent and Path(row['path']).parent == parent]


def render(output: Path, *, observed_run_state: str = 'not_observed') -> dict:
    plan = json.loads((output / 'plan.json').read_bytes())
    raw = (output / 'records.nul').read_bytes()
    final_path = output / 'result.json'
    try:
        final = json.loads(final_path.read_bytes()) if final_path.exists() else None
    except json.JSONDecodeError:
        # The scan creates its result before writing is complete. Keep the
        # honest running view until the supervisor reports a final state.
        final = None
    report, rows = _view(plan, raw, final)
    stamp = datetime.now(timezone.utc).isoformat()
    by_path = {row['path']: row for row in rows}
    csv_io = io.StringIO(newline='')
    columns = ['path', 'allocated_bytes', 'planned_roots', 'recorded_roots',
               'accounted_roots', 'all_planned_roots_accounted']
    writer = csv.DictWriter(csv_io, fieldnames=columns)
    writer.writeheader()
    writer.writerows(rows)
    lines = ['# WSL 線上空間清點', '', f'- 報表產生時間：{stamp}（UTC）',
             f"- 凍結掃描計畫時間：{plan['created_at_utc']}",
             f"- 掃描資料狀態：{report['state']}；監督程序狀態：{observed_run_state}",
             f"- 已輸出紀錄：{report['completed_records']}；計畫項目：{report['planned_roots']}。"
             '大子樹可能耗時不同，這不是完成百分比或 ETA。',
             '- 數值為同一 `du` 的 allocated bytes；同一 inode 只計一次。資料持續變動，'
             '不是原子快照，也不代表可刪除空間。未走訪的路徑顯示「尚未量測」，不是零。',
             '- 只列 WSL 同裝置命名空間；其他掛載點、Windows 磁碟和未遍歷範圍不包含在總額中。',
             '- 子目錄與父目錄包含關係，不能將不同層數相加。硬連結歸屬於先掃描到的路徑。',
             '- `已核對` 僅指凍結計畫的路徑已有 du 紀錄或完成後已核對硬連結省略；'
             '不代表來源、冷還原或可退休性已驗證。',
             f"- 掃描錯誤／變更目錄：{len(report['planning_errors'])}／"
             f"{len(report['unresolved_roots'])}／{len(report['changed_expanded_directories'])}",
             '', '完整已觀察路徑與 bytes 見同目錄的 `path_usage.csv`。']
    for title, parent in [('WSL 根目錄', Path(plan['root'])),
                          ('專案主要資料', Path('/root/stockAgent')),
                          ('專案 artifacts', Path('/root/stockAgent/artifacts')),
                          ('服務目錄', Path('/srv')), ('系統狀態目錄', Path('/var'))]:
        if str(parent) not in by_path:
            continue
        children = _children(rows, parent)
        lines.extend(['', f'## {title}', '', '| 路徑 | 已觀察 GiB | 計畫項目已核對 |',
                      '| --- | ---: | ---: |'])
        for row in children:
            name = row['path'].replace('`', "'").replace('|', '\\|').replace('\n', ' ')
            amount = ('尚未量測' if row['allocated_bytes'] is None
                      else f"{row['allocated_bytes'] / 1024**3:.2f}")
            if row['allocated_bytes'] is not None and not row['all_planned_roots_accounted']:
                amount += '（局部）'
            lines.append(f"| `{name}` | {amount} | "
                         f"{row['accounted_roots']}/{row['planned_roots']} |")
    _replace_generated(output / 'path_usage.csv', csv_io.getvalue())
    _replace_generated(output / 'inventory_summary.md', '\n'.join(lines) + '\n')
    return {'state': report['state'], 'run_state': observed_run_state,
            'recorded': report['completed_records'], 'paths': len(rows),
            'summary': str(output / 'inventory_summary.md'), 'csv': str(output / 'path_usage.csv')}


def _run_state(run_id: str, output: Path) -> str:
    if re.fullmatch(r'[A-Za-z0-9_-]+', run_id) is None:
        raise ValueError('invalid census run id')
    result = subprocess.run(['stockagent-agent', 'run-status', run_id], capture_output=True,
                            text=True, check=True, timeout=15)
    run = json.loads(result.stdout)
    args = run.get('argv')
    if (run.get('run_id') != run_id or run.get('repo_root') != str(ROOT)
            or not isinstance(args, list) or 'scripts/audit_wsl_storage_usage.py' not in args
            or 'scan' not in args or '--output' not in args
            or args.index('--output') + 1 >= len(args) or args[args.index('--output') + 1] != str(output)):
        raise ValueError('supervisor does not identify this exact census output')
    return str(run['state'])


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True, help='Existing private census directory')
    parser.add_argument('--watch-run', help='Refresh on new records until this exact supervised scan ends')
    parser.add_argument('--interval-seconds', type=int, default=60, choices=range(30, 601))
    args = parser.parse_args()
    output = args.output.absolute()
    if not args.watch_run:
        print(json.dumps(render(output), ensure_ascii=False))
        return 0
    previous = None
    while True:
        try:
            state = _run_state(args.watch_run, output)
        except (OSError, subprocess.SubprocessError, ValueError, KeyError) as error:
            print(json.dumps({'event': 'supervisor_unobservable', 'error': str(error)}), flush=True)
            time.sleep(args.interval_seconds)
            continue
        record_file = output / 'records.nul'
        fingerprint = (state, record_file.stat().st_size, (output / 'result.json').exists())
        if fingerprint != previous:
            result = render(output, observed_run_state=state)
            print(json.dumps({'event': 'report_refreshed', **result}, ensure_ascii=False), flush=True)
            previous = fingerprint
        if state not in {'queued', 'launch_unverified', 'running'}:
            return 0 if state == 'succeeded' and result['state'] == 'complete' else 75
        time.sleep(args.interval_seconds)


if __name__ == '__main__':
    raise SystemExit(main())
