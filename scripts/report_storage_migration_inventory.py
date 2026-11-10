#!/usr/bin/env python3
"""Create a local, evidence-scoped inventory of storage migration progress.

This reads existing metadata and the local monitor only. It does not scan D
payloads, acquire storage-owner locks, change a head, or authorize retirement.
The older backup catalog and newer fixed-generation receipts are deliberately
reported as separate observations rather than combined into a completion rate.
"""
from __future__ import annotations

import argparse
from collections import Counter
import csv
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import stat
import sys
import tempfile
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import ijson

from stockagent.data_sync.d_backup_inventory import GROUPS, group_for
from stockagent.data_sync.backup_stream import _private_catalog_generation
from scripts.retire_staged_cold_objects import _same_catalog_generation

HEX = re.compile(r'[0-9a-f]{64}\Z')
CATALOG = Path('/var/lib/stockagent/backup-stream/catalog.json')
CATALOG_STATUS = CATALOG.with_name('catalog-observation.json')
REPACK = Path('/srv/stockagent-workspace/migration/source-local-exact-repack')
LIFECYCLE = Path('/var/lib/stockagent/lakehouse-control/dataset-versions')
URL = 'http://127.0.0.1:8771/data-monitor/api/storage'
LIMIT = 1_000_000


def document(path: Path, maximum=8 * 1024**2):
    if path.is_symlink():
        raise ValueError('redirected metadata')
    with path.open('rb') as stream:
        raw = stream.read(maximum + 1)
    if len(raw) > maximum:
        raise ValueError('metadata too large')
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise ValueError('metadata is not an object')
    return value


def catalog_inventory(catalog: Path, status_path: Path):
    """Associate fixed catalog object bytes to datasets with bounded bitsets.

    Shared objects count once *within* a dataset, but appear for every dataset
    that references them. The sum of dataset rows is not physical usage.
    """
    before_status = document(status_path)
    expected = before_status.get('catalog_identity_sha256')
    generation_at_start = _private_catalog_generation(catalog)
    if (before_status.get('contract') != 'stockagent.backup_catalog_observation.v1'
            or not isinstance(expected, str) or not HEX.fullmatch(expected)
            or not _same_catalog_generation(catalog, before_status.get('catalog_generation'), generation_at_start)):
        raise ValueError('missing catalog identity')
    names, stats, all_refs, current_refs = {}, [], {}, {}
    release, release_refs, file_row = {}, set(), {}
    total_files = total_bytes = 0
    seen_files = set()
    identity = contract = None
    fd = os.open(catalog, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    with os.fdopen(fd, 'rb') as stream:
        before = os.fstat(stream.fileno())
        if not stat.S_ISREG(before.st_mode):
            raise ValueError('catalog is not a regular file')
        for prefix, event, value in ijson.parse(stream):
            if prefix == 'releases.item' and event == 'start_map':
                release, release_refs = {}, set()
            elif prefix in {'releases.item.dataset', 'releases.item.current_head'} and event in {'string', 'boolean'}:
                release[prefix.rsplit('.', 1)[-1]] = value
            elif prefix == 'releases.item.required_file_keys.item' and event == 'string':
                if len(value) > 8192 or len(release_refs) >= 500_000:
                    raise ValueError('catalog release references exceed bounds')
                release_refs.add(value)
            elif prefix == 'releases.item' and event == 'end_map':
                name, current = release.get('dataset'), release.get('current_head')
                if not isinstance(name, str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,127}', name) or type(current) is not bool:
                    raise ValueError('invalid catalog release')
                if name not in names:
                    if len(names) >= 20_000:
                        raise ValueError('too many catalog datasets')
                    names[name] = len(stats)
                    stats.append({'dataset': name, 'catalog_versions': 0, 'catalog_current_versions': 0,
                                  'catalog_referenced_bytes': 0, 'catalog_current_bytes': 0,
                                  'catalog_referenced_files': 0, 'catalog_current_files': 0})
                idx = names[name]
                stats[idx]['catalog_versions'] += 1
                stats[idx]['catalog_current_versions'] += int(current)
                mask = 1 << idx
                for key in release_refs:
                    all_refs[key] = all_refs.get(key, 0) | mask
                    if current:
                        current_refs[key] = current_refs.get(key, 0) | mask
                if len(all_refs) > LIMIT:
                    raise ValueError('too many catalog reference keys')
            elif prefix == 'files.item' and event == 'start_map':
                file_row = {}
            elif prefix in {'files.item.relative', 'files.item.sha256', 'files.item.bytes'} and event in {'string', 'number'}:
                file_row[prefix.rsplit('.', 1)[-1]] = value
            elif prefix == 'files.item' and event == 'end_map':
                relative, digest, size = (file_row.get(k) for k in ('relative', 'sha256', 'bytes'))
                if (not isinstance(relative, str) or len(relative) > 8192 or not isinstance(digest, str)
                        or not HEX.fullmatch(digest) or type(size) is not int or size < 0):
                    raise ValueError('invalid catalog file')
                total_files += 1
                total_bytes += size
                key = relative + '@' + digest
                if key in seen_files or len(seen_files) >= LIMIT:
                    raise ValueError('duplicate or excessive catalog files')
                seen_files.add(key)
                for refs, byte_key, count_key in (
                    (all_refs, 'catalog_referenced_bytes', 'catalog_referenced_files'),
                    (current_refs, 'catalog_current_bytes', 'catalog_current_files'),
                ):
                    mask = refs.pop(key, 0)
                    while mask:
                        bit = mask & -mask
                        row = stats[bit.bit_length() - 1]
                        row[byte_key] += size
                        row[count_key] += 1
                        mask ^= bit
            elif prefix == 'identity_sha256' and event == 'string':
                identity = value
            elif prefix == 'contract' and event == 'string':
                contract = value
        after = os.fstat(stream.fileno())
    generation = lambda x: (x.st_dev, x.st_ino, x.st_size, x.st_mtime_ns, x.st_ctime_ns)
    after_status = document(status_path)
    if (generation(before) != generation(after) or generation(before) != generation(catalog.stat(follow_symlinks=False))
            or identity != expected or after_status.get('catalog_identity_sha256') != expected
            or not _same_catalog_generation(catalog, after_status.get('catalog_generation'),
                                            _private_catalog_generation(catalog))
            or contract != 'retained_packed_backup_catalog_v1'
            or (type(before_status.get('available_file_count')) is int
                and total_files != before_status['available_file_count'])
            or (type(before_status.get('available_bytes')) is int
                and total_bytes != before_status['available_bytes'])):
        raise ValueError('catalog identity, generation, or totals changed')
    return {x['dataset']: x for x in stats}, {
        'catalog_identity_sha256': identity,
        'catalog_observed_at_utc': before_status.get('persisted_observed_at_utc'),
        'catalog_files': total_files, 'catalog_total_bytes': total_bytes,
        'unresolved_references': len(all_refs), 'unresolved_current_references': len(current_refs),
    }


def repack_receipts(root: Path):
    rows, errors = [], []
    with os.scandir(root) as directories:
        for directory in directories:
            if not directory.is_dir(follow_symlinks=False):
                continue
            with os.scandir(directory.path) as entries:
                for entry in entries:
                    if (not entry.is_file(follow_symlinks=False) or not entry.name.endswith('.json')
                            or not HEX.fullmatch(entry.name[:-5])):
                        continue
                    try:
                        row = document(Path(entry.path), maximum=2 * 1024**2)
                        if (row.get('contract') != 'stockagent.source_local_exact_repack.v1'
                                or row.get('state') != 'new_D_head_committed_from_exact_E_source'
                                or row.get('dataset') != directory.name
                                or row.get('new_manifest_sha256') != entry.name[:-5]):
                            raise ValueError('receipt identity mismatch')
                        rows.append(row)
                    except (OSError, ValueError, TypeError) as error:
                        errors.append({'dataset': directory.name, 'error_type': type(error).__name__})
    return rows, errors


def write_csv(path: Path, fields: list[str], rows):
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8-sig', newline='',
                                         dir=path.parent, prefix='.'+path.name+'.', delete=False) as stream:
            temporary = Path(stream.name)
            writer = csv.DictWriter(stream, fieldnames=fields, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(rows)
        os.replace(temporary, path)
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink()


def category(name: str) -> str:
    return group_for(name[6:] if name.startswith('local-') else name)


def write_localized_tables(output: Path):
    """Produce human-readable, separately scoped views of the same snapshot."""
    with (output/'datasets.csv').open(encoding='utf-8-sig', newline='') as stream:
        data = list(csv.DictReader(stream))
    groups = dict(GROUPS)
    queue_labels = {'published':'此批已發布', 'running':'處理中', 'waiting':'等待', 'retrying':'暫緩重試'}
    rows = []
    for item in data:
        receipt = item['fixed_repack_receipt'] == 'True'
        state = queue_labels.get(item['queue_state'], '固定世代已發布' if receipt else '尚未確認新封裝')
        rows.append({'資料集':item['dataset'], '分類':groups.get(item['category'], item['category']),
            '新封裝狀態':state, '有固定發布收據':'有' if receipt else '未找到',
            '最近封裝來源邏輯位元組':item['repack_source_logical_bytes'], '最近封裝檔案數':item['repack_files'],
            '舊D目錄當時現行版本引用位元組':item['catalog_current_bytes'],
            '舊D目錄全部版本引用位元組':item['catalog_referenced_bytes'],
            '舊D目錄時間UTC':item['catalog_observed_at_utc'], '佇列來源邏輯位元組':item['queue_logical_bytes'],
            '佇列原因':item['queue_reason'], '直接舊版回收狀態':item['direct_retirement_state'],
            '原版本工作者狀態':item['lifecycle_state'], '固定版本SHA256':item['repack_new_manifest_sha256']})
    fields = list(rows[0]) if rows else ['資料集']
    write_csv(output/'資料集清冊.csv', fields, rows)
    write_csv(output/'尚未確認新封裝.csv', fields, [x for x in rows if x['有固定發布收據'] == '未找到'])
    write_csv(output/'待處理佇列.csv', fields, [x for x in rows if x['新封裝狀態'] in {'等待', '處理中', '暫緩重試'}])
    live, marker = output/'wsl_live_candidates_scan.partial', output/'wsl_live_candidates_scan.status.json'
    if not live.is_file() or not marker.is_file():
        return
    result = document(marker)
    if result.get('exit_code') != 0:
        return
    live_rows = []
    for line in live.read_text().splitlines():
        size, separator, name = line.partition('\t')
        if not separator or not size.isdigit() or not name.startswith('/root/stockAgent/service_data/live/'):
            continue
        name = Path(name).name
        decision = '待逐項核對；不因名稱決定可刪'
        if name == 'tw_day_trade_simulation':
            decision = '現行當沖帳本；保留'
        elif name in {'tw_day_trade_ytd_five_modes_sources_complete_20261018',
                       'tw_day_trade_unit_mgda.addition.zuigt766'}:
            decision = '2026-11-04 文件記錄為策略切換 rollback；保留待核對'
        elif 'factorized' in name:
            decision = '與其他視窗處理範圍相關；本輪不動'
        live_rows.append({'目錄':name, 'C槽配置位元組':size, '目前判讀':decision,
                          '來源掃描完成時間epoch':result.get('finished_at_epoch')})
    live_rows.sort(key=lambda row: int(row['C槽配置位元組']), reverse=True)
    write_csv(output/'C當沖服務目錄.csv', ['目錄', 'C槽配置位元組', '目前判讀', '來源掃描完成時間epoch'], live_rows)


def write_overview(output: Path):
    summary = document(output/'summary.json')
    with (output/'datasets.csv').open(encoding='utf-8-sig', newline='') as stream:
        rows = list(csv.DictReader(stream))
    with (output/'volumes.csv').open(encoding='utf-8-sig', newline='') as stream:
        volumes = list(csv.DictReader(stream))
    gib = lambda value: f'{int(value)/1024**3:,.2f}' if value not in (None, '') else '未知'
    categories = dict(GROUPS)
    missing = Counter(x['category'] for x in rows if x['fixed_repack_receipt'] != 'True')
    queue = [x for x in rows if x['queue_state'] in {'waiting', 'running', 'retrying'}]
    queue.sort(key=lambda x: int(x['queue_logical_bytes'] or 0), reverse=True)
    lines = [
        '# 儲存遷移清冊', '',
        f"產生時間：{summary['generated_at_utc']}；面板資料時間：{summary['monitor_generated_at_utc']}。",
        f"D 舊目錄快照時間：{summary['catalog']['catalog_observed_at_utc']}，並非目前完整冷庫清冊。", '',
        f"此報告合併 {summary['dataset_count_union']} 個有名稱項目；其中 "
        f"{summary['datasets_with_fixed_repack_receipt']} 個至少有一份新 D 固定版本發布收據。",
        f"其餘 {sum(missing.values())} 個沒有這類收據，**不等於**確定未遷移。", '',
        '複製、原路徑交接、D 發布、舊檔回收、獨立還原及 NAS 驗收是不同階段。'
        '本文的發布收據未重新讀取 D 資料，也不等於最新 head 一定未變。', '',
        '同一來源、資料集、歷史版本可能共用封包；資料集的來源邏輯大小及 D 物件引用大小不可相加為實體空間。', '',
        '## 容量快照', '', '| 節點／卷 | 已用 GiB | 可用 GiB | 容量 GiB | 資料時間 UTC |',
        '| --- | ---: | ---: | ---: | --- |',
    ]
    for v in volumes:
        lines.append(f"| {v['label']} | {gib(v['used_bytes'])} | {gib(v['available_bytes'])} | "
                     f"{gib(v['total_bytes'])} | {v['observed_at_utc']} |")
    lines += ['', 'WSL 位於 Windows C 內，E Linux 工作卷位於 Windows E 內；容量不能相加。', '',
        '## 當時仍在佇列的項目', '',
        '大小是該固定來源的邏輯大小，並非已占用的壓縮空間或仍須搬移的位元組。', '',
        '| 資料集 | 來源 GiB | 佇列狀態 | 原因 |', '| --- | ---: | --- | --- |']
    for row in queue:
        lines.append(f"| {row['dataset']} | {gib(row['queue_logical_bytes'])} | "
                     f"{row['queue_state']} | {row['queue_reason'] or '—'} |")
    lines += ['', '## 尚無固定封裝收據', '',
              '這是收據來源的查找結果，還需核對其他發布方式、最新 head 及重疊來源。', '']
    for key, label in GROUPS:
        if missing[key]:
            lines.append(f'- {label}：{missing[key]} 項')
    lines += ['', '## 明細欄位', '',
        '- [逐資料集清冊](datasets.csv)：每列一個已知名稱；`catalog_*` 欄位來自上述舊 D 目錄，'
        '`repack_*` 是該資料集最近一張固定發布收據，`queue_*` 是當時佇列，`lifecycle_*` 是原版本工作者記錄。',
        '- [全部固定發布收據](repack_receipts.csv)：每列一個固定版本，不代表每個版本都是目前 head。',
        '- [各階段及阻礙](operation_stages.csv)：來源複製、路徑交接、發布與回收分開。',
        '- [各磁碟容量](volumes.csv)、[現有分類樹](volume_classification.csv)：保留各自觀測時間與部分掃描標記。',
        '- `wsl_*_scan.partial` 即使檔名含 partial，也可能有完成的單一子目錄；應以同名 status 的 exit code 判斷整次掃描是否完成。',
        '- 無法歸屬的 E 舊物件需另依當前 attribution 收據及使用者決定處理。', '']
    if (output/'local_measurements.md').is_file():
        lines.extend(['[本機已完成的目錄量測及使用者決定](local_measurements.md)', ''])
    if all((output/name).is_file() for name in ('資料集清冊.csv', '尚未確認新封裝.csv',
                                                '待處理佇列.csv', 'C當沖服務目錄.csv')):
        lines.extend(['[中文逐資料集清冊](資料集清冊.csv) · [尚未確認新封裝](尚未確認新封裝.csv) · '
                      '[待處理佇列](待處理佇列.csv) · [C 當沖服務目錄](C當沖服務目錄.csv)', ''])
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', dir=output,
                                         prefix='.README.', delete=False) as stream:
            temporary = Path(stream.name)
            stream.write('\n'.join(lines))
        os.replace(temporary, output/'README.md')
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink()


def _prepare_output(output: Path):
    allowed = (ROOT/'artifacts/data_quality').resolve()
    resolved = output.resolve(strict=False)
    if resolved == allowed or not resolved.is_relative_to(allowed):
        raise ValueError('report output must be inside artifacts/data_quality')
    output.mkdir(mode=0o700, parents=True, exist_ok=True)
    if output.is_symlink() or not output.is_dir() or output.resolve() != resolved:
        raise ValueError('output is not a directory')
    os.chmod(output, 0o700)
    os.umask(0o077)


def write_live_layout_report(output: Path):
    """Fresh manifest references, independent of the older backup catalog."""
    from stockagent.data_sync import d_pack_layout_inventory as layout
    from stockagent.data_sync.cold_primary import _check_d_primary_mount
    from stockagent.data_sync.desync_snapshots import atomic_write_json
    _prepare_output(output)
    cold = Path('/srv/stockagent-packed')
    _check_d_primary_mount(cold)
    started = time.time()
    last = 0.0
    def progress(event):
        nonlocal last
        if time.monotonic()-last < 5 and event['datasets_completed'] != event['datasets_total']:
            return
        last = time.monotonic()
        atomic_write_json(output/'layout_progress.json', {'contract': layout.CONTRACT+'.progress',
            'state': 'scanning', 'observed_at_epoch': time.time(), 'progress': event}, durable=True)
    result = layout.collect(cold, REPACK.parents[1], progress=progress)
    _check_d_primary_mount(cold)
    summary = {key: value for key, value in result.items() if key not in {'manifests', 'objects'}}
    summary['elapsed_seconds'] = round(time.time()-started, 3)
    atomic_write_json(output/'layout_summary.json', summary, durable=True)
    write_csv(output/'layout_manifests.csv', ['authority', 'dataset', 'snapshot_id', 'manifest_sha256',
        'current', 'layout', 'logical_bytes', 'files', 'encoded_reference_bytes', 'object_count'], result['manifests'])
    write_csv(output/'layout_objects.csv', ['authority', 'relative', 'sha256', 'bytes', 'layout_group',
        'current_reference', 'layouts', 'datasets'], result['objects'])
    atomic_write_json(output/'layout_progress.json', {'contract': layout.CONTRACT+'.progress',
        'state': 'completed' if result['complete'] else 'partial', 'observed_at_epoch': time.time(),
        'summary': {key: summary[key] for key in ('dataset_count', 'manifest_count', 'groups', 'errors')}}, durable=True)
    return summary


def write_presence_report(output: Path):
    from stockagent.data_sync import d_pack_layout_inventory as layout
    from stockagent.data_sync.cold_primary import _check_d_primary_mount
    from stockagent.data_sync.desync_snapshots import atomic_write_json
    _prepare_output(output)
    cold = Path('/srv/stockagent-packed')
    _check_d_primary_mount(cold)
    with (output/'layout_objects.csv').open(encoding='utf-8-sig', newline='') as stream:
        rows = []
        for row in csv.DictReader(stream):
            if len(rows) >= LIMIT:
                raise ValueError('presence input exceeds object bound')
            rows.append({**row, 'bytes': int(row['bytes'])})
    last = 0.0
    def progress(event):
        nonlocal last
        if time.monotonic()-last < 5 and event['objects_completed'] != event['objects_total']:
            return
        last = time.monotonic()
        atomic_write_json(output/'layout_presence_progress.json', {
            'contract': layout.CONTRACT+'.presence.progress', 'state': 'scanning',
            'observed_at_epoch': time.time(), 'progress': event}, durable=True)
    result = layout.object_presence(cold, rows, progress=progress)
    _check_d_primary_mount(cold)
    summary = {key: value for key, value in result.items() if key != 'objects'}
    write_csv(output/'layout_presence_objects.csv', ['authority', 'relative', 'sha256', 'bytes',
        'layout_group', 'state', 'observed_bytes', 'allocated_bytes', 'hardlinks', 'error_type'], result['objects'])
    atomic_write_json(output/'layout_presence_summary.json', summary, durable=True)
    atomic_write_json(output/'layout_presence_progress.json', {
        'contract': layout.CONTRACT+'.presence.progress', 'state': 'completed',
        'observed_at_epoch': time.time(), 'summary': summary}, durable=True)
    return summary


def write_report(output: Path):
    _prepare_output(output)
    with urllib.request.urlopen(URL, timeout=20) as response:
        monitor = json.load(response)
    if monitor.get('contract') != 'stockagent.storage_monitor.v1' or monitor.get('read_only') is not True:
        raise ValueError('local monitor contract differs')
    queue = document(REPACK/'queue-status.json')
    if queue.get('contract') != 'stockagent.source_local_repack_queue.v1.status':
        raise ValueError('repack queue contract differs')
    current = document(LIFECYCLE/'current.json')
    operation = Path(current.get('operation_directory', ''))
    if operation.parent != LIFECYCLE/'operations' or operation.is_symlink():
        raise ValueError('lifecycle operation path differs')
    lifecycle = document(operation/'progress.json')
    if lifecycle.get('contract') != current.get('contract') or lifecycle.get('plan_sha256') != current.get('plan_sha256'):
        raise ValueError('lifecycle generation differs')
    datasets, catalog_info = catalog_inventory(CATALOG, CATALOG_STATUS)
    receipts, receipt_errors = repack_receipts(REPACK)
    latest = {}
    for receipt in receipts:
        name, stamp = receipt['dataset'], receipt.get('observed_at_epoch', 0)
        if type(stamp) not in (int, float):
            continue
        if name not in latest or stamp > latest[name].get('observed_at_epoch', 0):
            latest[name] = receipt
    queue_rows = {r['dataset']: r for r in queue.get('entries', []) if isinstance(r, dict) and isinstance(r.get('dataset'), str)}
    lifecycle_rows = lifecycle.get('datasets', {})
    if not isinstance(lifecycle_rows, dict):
        raise ValueError('lifecycle datasets unavailable')
    names = set(datasets) | set(latest) | set(queue_rows) | set(lifecycle_rows)
    combined = []
    for name in sorted(names, key=lambda n: (tuple(x[0] for x in GROUPS).index(category(n)), n)):
        catalog = datasets.get(name, {})
        rec = latest.get(name, {})
        q = queue_rows.get(name, {})
        life = lifecycle_rows.get(name, {})
        retired = rec.get('retirement') if isinstance(rec.get('retirement'), dict) else {}
        combined.append({'dataset': name, 'category': category(name),
            **{k: catalog.get(k) for k in ('catalog_versions', 'catalog_current_versions',
                'catalog_referenced_files', 'catalog_referenced_bytes', 'catalog_current_files', 'catalog_current_bytes')},
            'catalog_observed_at_utc': catalog_info['catalog_observed_at_utc'] if catalog else None,
            'fixed_repack_receipt': bool(rec), 'repack_observed_epoch': rec.get('observed_at_epoch'),
            'repack_new_manifest_sha256': rec.get('new_manifest_sha256'),
            'repack_source_logical_bytes': rec.get('logical_bytes'), 'repack_files': rec.get('files'),
            'repack_source_mode': rec.get('source_mode'),
            'direct_retirement_state': retired.get('state'),
            'direct_retirement_estimated_allocated_bytes': retired.get('estimated_allocated_bytes_reclaimed'),
            'queue_state': q.get('state'), 'queue_reason': q.get('reason'),
            'queue_logical_bytes': q.get('logical_bytes'), 'queue_files': q.get('files'),
            'lifecycle_state': life.get('state'), 'lifecycle_logical_bytes': life.get('logical_bytes'),
            'lifecycle_manifest_sha256': life.get('manifest_sha256'),
            'lifecycle_domain': life.get('domain')})
    fields = ['dataset', 'category', 'catalog_versions', 'catalog_current_versions', 'catalog_referenced_files',
              'catalog_referenced_bytes', 'catalog_current_files', 'catalog_current_bytes', 'catalog_observed_at_utc',
              'fixed_repack_receipt', 'repack_observed_epoch', 'repack_new_manifest_sha256',
              'repack_source_logical_bytes', 'repack_files', 'repack_source_mode', 'direct_retirement_state',
              'direct_retirement_estimated_allocated_bytes', 'queue_state', 'queue_reason', 'queue_logical_bytes',
              'queue_files', 'lifecycle_state', 'lifecycle_logical_bytes', 'lifecycle_manifest_sha256', 'lifecycle_domain']
    write_csv(output/'datasets.csv', fields, combined)
    receipt_rows = []
    for rec in sorted(receipts, key=lambda x: (x['dataset'], x.get('observed_at_epoch', 0))):
        retirement = rec.get('retirement') if isinstance(rec.get('retirement'), dict) else {}
        receipt_rows.append({'dataset': rec['dataset'], 'observed_at_epoch': rec.get('observed_at_epoch'),
            'old_manifest_sha256': rec.get('old_manifest_sha256'), 'new_manifest_sha256': rec.get('new_manifest_sha256'),
            'files': rec.get('files'), 'source_logical_bytes': rec.get('logical_bytes'),
            'source_mode': rec.get('source_mode'), 'private': rec.get('private'),
            'direct_retirement_state': retirement.get('state'),
            'direct_retirement_estimated_allocated_bytes': retirement.get('estimated_allocated_bytes_reclaimed'),
            'independent_D_readback': rec.get('independent_D_readback'), 'NAS_accepted': rec.get('NAS_accepted')})
    write_csv(output/'repack_receipts.csv', list(receipt_rows[0]) if receipt_rows else ['dataset'], receipt_rows)
    tasks = []
    for task in monitor.get('operations', {}).get('tasks', []):
        progress = task.get('progress') if isinstance(task.get('progress'), dict) else {}
        tasks.append({'kind': task.get('kind'), 'label': task.get('label'), 'state': task.get('state'),
            'phase': task.get('phase'), 'reason': task.get('reason'), 'scope': task.get('scope'),
            'completed_bytes': progress.get('completed_bytes'), 'total_bytes': progress.get('total_bytes'),
            'completed_items': progress.get('completed_items'), 'total_items': progress.get('total_items'),
            'observed_at_utc': task.get('observed_at_utc'), 'stale': task.get('stale')})
    task_fields = ['kind', 'label', 'state', 'phase', 'reason', 'scope', 'completed_bytes', 'total_bytes',
                   'completed_items', 'total_items', 'observed_at_utc', 'stale']
    write_csv(output/'operation_stages.csv', task_fields, tasks)
    volume_rows = []
    def walk(node_id, volume_id, classification, item, prefix=''):
        label = str(item.get('label', ''))
        path = prefix+'/'+label
        volume_rows.append({'node': node_id, 'volume': volume_id, 'classification_state': classification.get('state'),
            'observed_at_utc': classification.get('observed_at_utc'), 'path_label': path,
            'kind': item.get('kind'), 'allocated_bytes': item.get('allocated_bytes'),
            'lower_bound': item.get('allocated_bytes_is_lower_bound')})
        for child in item.get('children', []):
            walk(node_id, volume_id, classification, child, path)
    capacities = []
    for node in monitor.get('nodes', []):
        for volume in node.get('volumes', []):
            c, cl = volume.get('capacity', {}), volume.get('classification', {})
            capacities.append({'node': node.get('id'), 'volume': volume.get('id'), 'label': volume.get('label'),
                'contained_in': volume.get('contained_in'), 'capacity_state': c.get('state'),
                'total_bytes': c.get('total_bytes'), 'used_bytes': c.get('used_bytes'),
                'available_bytes': c.get('available_bytes'), 'observed_at_utc': c.get('observed_at_utc'),
                'classification_state': cl.get('state'), 'classification_observed_at_utc': cl.get('observed_at_utc'),
                'classification_deferred_reason': cl.get('deferred_reason')})
            for item in cl.get('tree', []):
                walk(node.get('id'), volume.get('id'), cl, item)
    write_csv(output/'volumes.csv', list(capacities[0]) if capacities else ['volume'], capacities)
    write_csv(output/'volume_classification.csv', ['node', 'volume', 'classification_state', 'observed_at_utc',
              'path_label', 'kind', 'allocated_bytes', 'lower_bound'], volume_rows)
    summary = {'generated_at_utc': datetime.now(timezone.utc).isoformat(),
        'monitor_generated_at_utc': monitor.get('generated_at_utc'), 'catalog': catalog_info,
        'dataset_count_union': len(names), 'catalog_dataset_count': len(datasets),
        'datasets_with_fixed_repack_receipt': len(latest), 'repack_receipt_count': len(receipts),
        'repack_receipt_errors': receipt_errors, 'queue_observed_at_epoch': queue.get('observed_at_epoch'),
        'queue_states': dict(Counter(x.get('state') for x in queue_rows.values())),
        'lifecycle_dataset_states': dict(Counter(x.get('state') for x in lifecycle_rows.values())),
        'task_count': len(tasks), 'capacity_and_classification_count': len(capacities),
        'scopes': ['fixed repack receipt means a release was published, not necessarily current head',
                   'old catalog object counts are as of catalog_observed_at_utc',
                   'dataset rows can share source files and encoded objects; do not sum their bytes',
                   'capacity, file copy, handoff, publication, retirement and NAS acceptance are separate']}
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', dir=output,
                                         prefix='.summary.', delete=False) as stream:
            temporary = Path(stream.name)
            stream.write(json.dumps(summary, ensure_ascii=False, indent=2)+'\n')
        os.replace(temporary, output/'summary.json')
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink()
    write_localized_tables(output)
    write_overview(output)
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument('--layout-only', action='store_true', help='Read live public/private manifest references, no payload scan')
    mode.add_argument('--presence-only', action='store_true', help='Observe names from the existing layout CSV without reading payloads')
    args = parser.parse_args()
    result = (write_presence_report(args.output.absolute()) if args.presence_only
              else write_live_layout_report(args.output.absolute()) if args.layout_only
              else write_report(args.output.absolute()))
    print(json.dumps(result, ensure_ascii=False))


if __name__ == '__main__':
    main()
