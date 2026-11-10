#!/usr/bin/env python3
"""Compare the completed fixed futures subtree with one fixed D inventory.

The comparison reads manifest identities only; it never authorizes retirement
or substitutes for independently reconstructing D or accepting NAS delivery.
"""
from __future__ import annotations

import fcntl
import hashlib
import json
import os
from datetime import datetime, time as wall_time
from pathlib import Path
import re
import stat
import sys
import time
from zoneinfo import ZoneInfo

from downloader.artifact_io import atomic_write_bytes, atomic_write_json
from scripts import audit_e_staging_against_cold as audit, run_workspace_source_staging as queue
from stockagent.data_sync import workspace_volume as volume

NAME = 'tw-futures-prepared-e-primary-20261104-v1'
CONTRACT = 'stockagent.futures_e_cold_inventory_comparison.v1'


def valid_result(result, proof, report_sha) -> bool:
    counts = result.get('counts') if isinstance(result, dict) else None
    keys = ('exact_files', 'exact_bytes', 'changed_files', 'changed_bytes',
            'e_only_files', 'e_only_bytes', 'd_only_files', 'd_non_file_entries')
    return bool(isinstance(counts, dict) and all(type(counts.get(k)) is int and counts[k] >= 0 for k in keys)
        and result.get('contract') == CONTRACT and result.get('source') == NAME
        and result.get('source_copy_report_sha256') == report_sha
        and result.get('copy_result_sha256') == proof.get('relocation_evidence_sha256')
        and result.get('state') == 'inventory_compared'
        and isinstance(result.get('D_snapshot_id'), str) and bool(result['D_snapshot_id'])
        and isinstance(result.get('D_inventory_sha256'), str)
        and re.fullmatch(r'[0-9a-f]{64}', result['D_inventory_sha256']) is not None
        and (result.get('D_manifest_sha256') is None
             or re.fullmatch(r'[0-9a-f]{64}', str(result['D_manifest_sha256'])) is not None)
        and result.get('D_inventory_source_prefix') == 'training_prepared'
        and sum(counts[k] for k in ('exact_files', 'changed_files', 'e_only_files')) == proof.get('files')
        and sum(counts[k] for k in ('exact_bytes', 'changed_bytes', 'e_only_bytes')) == proof.get('logical_bytes')
        and result.get('D_independent_restore_verified') is False and result.get('NAS_acceptance') is False
        and type(result.get('source_files_deleted')) is int and result['source_files_deleted'] == 0
        and result.get('retirement_authorized') is False)


def current_head_identity() -> dict:
    """Observe only the current D head and manifest metadata, not payloads."""
    audit._check_d_primary_mount(audit.COLD)
    snapshot = audit.resolve_latest_packed(audit.COLD, 'tw-index-futures', require_objects=False)
    result = {'D_snapshot_id': snapshot.manifest['snapshot_id'],
              'D_manifest_sha256': snapshot.manifest_sha256,
              'D_inventory_sha256': snapshot.manifest['archive']['inventory']['sha256']}
    source = snapshot.manifest.get('source', {})
    counts = [source.get(key) for key in ('files', 'directories', 'symlinks')]
    if all(type(value) is int and value >= 0 for value in counts):
        result['D_entries_total'] = sum(counts)
    return result


def matches_current(result, current) -> bool:
    return (isinstance(result, dict) and isinstance(current, dict)
            and all(isinstance(current.get(key), str) and result.get(key) == current[key]
                    for key in ('D_snapshot_id', 'D_manifest_sha256', 'D_inventory_sha256')))


def links_compared(result, proof) -> bool:
    """An old regular-file-only receipt is history when this copy has links."""
    expected = proof.get('symlink_names_preserved')
    if expected is None:
        return True
    if type(expected) is not int or expected < 0 or not isinstance(result, dict):
        return False
    if expected == 0:
        return True
    counts = result.get('counts', {})
    keys = ('exact_symlinks', 'changed_symlinks', 'e_only_symlinks', 'd_only_symlinks')
    return (isinstance(counts, dict) and all(type(counts.get(k)) is int and counts[k] >= 0 for k in keys)
            and sum(counts[k] for k in keys[:3]) == expected)


def _preserve_history(verify: Path, raw: bytes) -> Path:
    """Keep exact prior comparison bytes when the moving D head advances."""
    history = verify/'futures-e-cold-inventory-history-v1'
    history.mkdir(mode=0o700, exist_ok=True)
    if history.is_symlink() or history.resolve(strict=True) != history:
        raise RuntimeError('historical comparison directory is redirected')
    digest = hashlib.sha256(raw).hexdigest()
    path = history/(digest+'.json')
    if path.is_symlink():
        raise RuntimeError('historical comparison receipt is redirected')
    if path.exists():
        if not path.is_file() or path.stat().st_size > 128*1024 or path.read_bytes() != raw:
            raise RuntimeError('historical comparison receipt identity differs')
    else:
        atomic_write_bytes(path, raw, durable=True, mode=0o600)
    return path


def _trading_allowed(*, for_start: bool) -> bool:
    from scripts.check_outside_tw_opening_resource_window import evaluate
    return bool(evaluate(datetime.now(ZoneInfo('Asia/Taipei')),
        minimum_runway_minutes=60 if for_start else 0, protected_until=wall_time(13, 30),
        official_calendar_root=queue.ROOT/'data_tw_public')['allowed'])


def main() -> int:
    control = volume.ENROLLMENT.parent / 'source-cold-comparison-v1'
    control.mkdir(mode=0o700, parents=True, exist_ok=True)
    if control.is_symlink() or control.resolve(strict=True) != control:
        raise RuntimeError('cold inventory comparison owner is redirected')
    fd = os.open(control / 'owner.lock', os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, 'r+b') as owner:
        if not stat.S_ISREG(os.fstat(owner.fileno()).st_mode):
            raise RuntimeError('cold inventory comparison lock is not a regular file')
        try:
            fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print(json.dumps({'state': 'waiting', 'reason': 'comparison_owner_busy'}))
            return 75
        return check()


def check() -> int:
    volume.check()
    workspace = Path(volume.configuration()['mount_point'])
    items = [item for item in queue.load_sources() if item['name'] == NAME]
    if len(items) != 1:
        raise RuntimeError('fixed futures source enrollment differs')
    item = items[0]
    _, _, _, report_path = queue.paths(item, workspace)
    migration, verify = workspace / 'migration', workspace / 'verify'
    if any(p.is_symlink() or p.resolve(strict=True) != p for p in (migration, verify)):
        raise RuntimeError('cold comparison evidence root is redirected')
    status_path = migration / 'futures-e-cold-inventory-status.json'
    result_path = verify / 'futures-e-cold-inventory-comparison-v1.json'
    def status(state, **fields):
        atomic_write_json(status_path, {'contract': CONTRACT, 'state': state,
            'observed_at_epoch': time.time(), **fields}, durable=state != 'comparing')
    try:
        proof = queue.verified_report(item, workspace)
        if proof is None:
            status('waiting_for_copy')
            return 75
        report_sha = hashlib.sha256(report_path.read_bytes()).hexdigest()
        if result_path.is_symlink():
            raise RuntimeError('fixed cold comparison receipt is redirected')
        result = None
        if result_path.exists():
            if result_path.stat().st_size > 128 * 1024:
                raise RuntimeError('fixed cold comparison receipt is oversized')
            previous_raw = result_path.read_bytes()
            result = json.loads(previous_raw)
            if not valid_result(result, proof, report_sha):
                raise RuntimeError('fixed cold comparison receipt differs')
        current = current_head_identity()
        if result is not None and matches_current(result, current) and links_compared(result, proof):
            status('inventory_compared', counts=result['counts'], **current)
            return 0
        if result is not None:
            _preserve_history(verify, previous_raw)
        if not _trading_allowed(for_start=True):
            status('waiting_trading', **current)
            return 75
        status('comparing', **current)
        last_guard = 0.0
        def guard():
            nonlocal last_guard
            now = time.monotonic()
            if now >= last_guard:
                if not _trading_allowed(for_start=False):
                    raise BlockingIOError('tw_day_trade_priority_window')
                last_guard = now+5
        def progress(value):
            status('comparing', progress=value, **current)
        try:
            observed = audit.audit(item, workspace, progress=progress, guard=guard, expected_head=current)
        except audit.SnapshotError as error:
            if str(error) != 'D head changed before the inventory comparison began':
                raise
            status('waiting_head_changed', **current_head_identity())
            return 75
        if observed.get('state') != 'inventory_compared':
            raise RuntimeError('fixed D inventory is unavailable for comparison')
        result = {'contract': CONTRACT, 'source': NAME, 'state': 'inventory_compared',
                  'source_copy_report_sha256': report_sha,
                  'copy_result_sha256': proof['relocation_evidence_sha256'],
                  'D_snapshot_id': observed['D_snapshot_id'],
                  'D_manifest_sha256': observed['D_manifest_sha256'],
                  'D_inventory_sha256': observed['D_inventory_sha256'],
                  'D_inventory_source_prefix': observed['D_inventory_source_prefix'],
                  'counts': observed['counts'], 'observed_at_epoch': time.time(),
                  'D_independent_restore_verified': False, 'NAS_acceptance': False,
                  'source_files_deleted': 0, 'retirement_authorized': False}
        if not valid_result(result, proof, report_sha) or not links_compared(result, proof):
            raise RuntimeError('fixed D inventory comparison does not conserve its source')
        latest = current_head_identity()
        if not matches_current(result, latest):
            _preserve_history(verify, (json.dumps(result, sort_keys=True, ensure_ascii=False)+'\n').encode())
            status('waiting_head_changed', **latest)
            return 75
        atomic_write_json(result_path, result, durable=True)
        status('inventory_compared', counts=result['counts'], **latest)
        print(json.dumps({'state': result['state'], 'counts': result['counts']}))
        return 0
    except BlockingIOError:
        status('waiting_trading')
        return 75
    except Exception as error:
        status('blocked', error_type=type(error).__name__)
        print(json.dumps({'state': 'blocked', 'error_type': type(error).__name__}), file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
