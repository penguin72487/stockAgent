#!/usr/bin/env python3
"""Inventory source roots without walking or changing their payloads.

The report is a placement observation, not a complete byte census, migration
authorization, active-reference proof, or D/NAS recovery acceptance.
"""
from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re

from downloader.artifact_io import atomic_write_json, atomic_write_text
from stockagent.data_sync import workspace_volume as volume

ROOT = Path(__file__).resolve().parents[1]


def _mount_unescape(value: str) -> str:
    return re.sub(r'\\([0-7]{3})', lambda match: chr(int(match.group(1), 8)), value)


def mount_rows(raw: str) -> list[tuple[Path, str]]:
    result = []
    for line in raw.splitlines():
        fields = line.split()
        if len(fields) < 10 or '-' not in fields:
            raise ValueError('incomplete mount observation')
        result.append((Path(_mount_unescape(fields[4])), fields[2]))
    return result


def old_sizes(path: Path) -> dict[str, dict]:
    if not path.exists():
        return {}
    if path.is_symlink() or path.stat().st_size > 16 * 1024**2:
        raise ValueError('old census is redirected or too large')
    with path.open(newline='', encoding='utf-8') as stream:
        return {row['path']: {'allocated_bytes': int(row['allocated_bytes']),
                              'state': row.get('census_state')}
                for row in csv.DictReader(stream)}


def _device(path: Path) -> int:
    return path.stat().st_dev


def inventory(repo: Path, workspace: Path, catalog: dict, mounts: list[tuple[Path, str]],
              previous_sizes: dict[str, dict], previous_observed_at: str | None = None) -> dict:
    if not isinstance(catalog, dict) or not isinstance(catalog.get('datasets'), list):
        raise ValueError('invalid packed dataset catalog')
    with os.scandir(repo) as stream:
        roots = {entry.name for entry in stream
                 if entry.name.startswith('data_') or entry.name.startswith('.data_')}
    roots.update({'artifacts', 'service_data'})
    entries = {}
    for item in catalog['datasets']:
        if not isinstance(item, dict) or not isinstance(item.get('source'), str):
            continue
        source = Path(item['source'])
        if source.is_absolute() or '..' in source.parts or not source.parts:
            continue
        if source.parts[0].startswith('data_'):
            roots.add(source.parts[0])
        entries.setdefault(source.parts[0], []).append({
            'dataset': item.get('dataset'), 'source': str(source), 'role': item.get('role'),
            'publish': item.get('publish') is True,
            'excluded_subtrees': item.get('excluded_subtrees', [])})
    c_device, e_device = _device(repo), _device(workspace)
    if c_device == e_device:
        raise ValueError('E workspace is not a distinct enrolled filesystem')
    rows = []
    paths = [(repo / name, name) for name in sorted(roots)]
    paths.extend((path, None) for path in (Path('/root/.shioaji/data'), Path('/root/finlab_db'),
                                           Path('/var/lib/stockagent'), Path('/srv/stockagent-live')))
    for path, name in paths:
        try:
            root = path.lstat()
            device = _device(path)
            placement = 'E' if device == e_device else 'C' if device == c_device else 'other'
            nested = [{'path': str(mount), 'device': major_minor} for mount, major_minor in mounts
                      if mount != path and mount.is_relative_to(path)]
            state = 'observed'
        except FileNotFoundError:
            root, placement, nested, state = None, 'unknown', [], 'missing'
        rows.append({'path': str(path), 'state': state, 'root_placement': placement,
                     'root_is_symlink': path.is_symlink() if root else False,
                     'nested_mounts': nested, 'catalog_datasets': entries.get(name, []),
                     'old_allocated_observation': previous_sizes.get(str(path)),
                     'current_recursive_size_bytes': None, 'process_references_checked': False,
                     'source_retirement_authorized': False})
    return {'contract': 'stockagent.workspace_source_placement_inventory.v1',
            'observed_at_utc': datetime.now(timezone.utc).isoformat(),
            'scope': 'checkout data and provider recovery roots, service_data, artifacts, selected SDK and service roots',
            'filesystem_root_counts': {key: sum(row['root_placement'] == key for row in rows)
                                       for key in ('C', 'E', 'other', 'unknown')},
            'catalog_dataset_count': len(catalog['datasets']), 'roots': rows,
            'old_size_observed_at_utc': previous_observed_at,
            'old_sizes_are_nonatomic_and_not_additive': True,
            'current_recursive_size_measured': False, 'D_backup_acceptance_claimed': False,
            'source_files_deleted': 0}


def render(value: dict) -> str:
    lines = ['# C／E 資料根位置清冊', '',
             f"觀測時間：{value['observed_at_utc']}；這是頂層 mount 與 catalog，非遞迴大小或刪除驗收。",
             f"舊大小觀測時間：{value['old_size_observed_at_utc'] or '未知'}；舊清冊非原子，父子及 hardlink 不可相加。",
             'nested mount 需另外判斷。', '',
             '| 資料根 | 根位置 | 巢狀掛載 | catalog | 舊配置 GiB |', '| --- | --- | ---: | ---: | ---: |']
    for row in value['roots']:
        old = row['old_allocated_observation']
        size = f"{old['allocated_bytes'] / 1024**3:.2f}" if old else '未量測'
        lines.append(f"| `{row['path']}` | {row['root_placement']} | {len(row['nested_mounts'])} | "
                     f"{len(row['catalog_datasets'])} | {size} |")
    return '\n'.join(lines) + '\n'


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir', type=Path, required=True)
    args = parser.parse_args()
    volume.admit(64 * 1024**2)
    workspace = Path(volume.configuration()['mount_point'])
    output = args.output_dir.absolute()
    if (not output.is_relative_to(workspace / 'verify') or output == workspace / 'verify'
            or output.is_symlink() or output.parent.resolve(strict=True) != output.parent):
        raise ValueError('inventory output must be a private, unredirected E verification directory')
    with Path('/proc/self/mountinfo').open('rb') as stream:
        raw = stream.read(4 * 1024**2 + 1)
    if len(raw) > 4 * 1024**2:
        raise ValueError('mount inventory exceeded its bound')
    catalog = json.loads((ROOT / 'configs/data_sync/packed_datasets.json').read_bytes())
    old_root = workspace / 'verify/wsl-layout-inventory-20261104'
    previous = old_sizes(old_root / 'path_usage.csv')
    try:
        old_status = json.loads((old_root / 'status.json').read_bytes())
        old_observed = old_status.get('observed_at_utc')
    except (OSError, ValueError, AttributeError):
        old_observed = None
    result = inventory(ROOT, workspace, catalog, mount_rows(raw.decode('utf-8')), previous, old_observed)
    output.mkdir(mode=0o700, parents=False, exist_ok=True)
    os.umask(0o077)
    atomic_write_json(output / 'source_placement.json', result)
    atomic_write_text(output / 'source_placement.md', render(result))
    print(json.dumps({'state': 'observed', 'observed_at_utc': result['observed_at_utc'],
                      'roots': len(result['roots']), 'root_placement_counts': result['filesystem_root_counts'],
                      'catalog_datasets': result['catalog_dataset_count'], 'source_files_deleted': 0},
                     ensure_ascii=False))


if __name__ == '__main__':
    main()
