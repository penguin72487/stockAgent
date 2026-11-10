#!/usr/bin/env python3
"""Compare changed Binance archive members without restoring a second tree."""
from __future__ import annotations

import gzip
import hashlib
import io
import json
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import plan_handoff_index_integration as planner  # noqa: E402
from stockagent.data_sync import (binance_generation_comparison as comparison, packed_member_stream as reader,
                                  packed_snapshots as packed, stream_repack,
                                  workspace_source_handoff as handoff, workspace_volume)  # noqa: E402
from stockagent.data_sync.completed_forward_capacity import reserve_capacity  # noqa: E402
from stockagent.data_sync.fixed_snapshot_spool import FixedSnapshotSpool, populate_pair  # noqa: E402
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json  # noqa: E402
from stockagent.data_sync.dataset_migration import private_history_root  # noqa: E402
from stockagent.data_sync.dataset_version_layout import version_policy  # noqa: E402

DATASET = 'binance-public-archive'
CONTRACT = 'stockagent.binance_archive_generation_audit.v1'


def compare_fixed_members(spool, records, *, cold, current, private_root, private,
                          report, guard, save_status, stream=reader.stream_members):
    """Read both fixed versions in pack order, then compare outside D locks."""
    def progress(event):
        report.update(event)
        save_status()
    proofs = populate_pair(spool, records, sides=(('private', private_root, private), ('public', cold, current)),
                           stream=stream, guard=guard, progress=progress,
                           maximum_file_bytes=64*1024**2, maximum_side_bytes=4*1024**3)
    report.update(stage='comparing_fixed_members', stage_files_total=len(records), stage_files_completed=0)
    save_status(True)
    for name in sorted(records):
        guard()
        original, newer = spool.get('public', name), spool.get('private', name)
        result = comparison.compare_parquet(name, original, newer)
        category = result['category']
        totals = report['categories'].setdefault(category, {key: 0 for key in result if key != 'category'})
        totals['files'] = totals.get('files', 0)+1
        for key, value in result.items():
            if key != 'category':
                totals[key] += value
        if result['old_only_rows'] or result['overlap_rows_with_price_change']:
            report['differences'].append({'path': name, **result})
        report['files_completed'] += 1
        report['stage_files_completed'] = report['files_completed']
        report['logical_bytes_completed'] += len(original)
        save_status()
    return proofs


def audit() -> dict:
    config = workspace_volume.configuration()
    workspace_volume.check(c=config)
    cold = planner.COLD
    planner._check_d_primary_mount(cold)
    root = workspace_volume.path('migration', create=True)/'handoff-index-archive-v1'/DATASET
    if root.is_symlink() or root.resolve(strict=True) != root:
        raise SnapshotError('Binance source comparison root is redirected')
    plans = list(root.glob('*/integration-diff-v1/diff.json'))
    if len(plans) != 1:
        raise SnapshotError('Binance comparison requires one fixed source generation')
    plan_path = plans[0]
    plan = json.loads(reader.read_stable_bytes(plan_path, maximum=1024**2))
    if (plan.get('contract') != 'stockagent.index_generation_diff.v1' or plan.get('dataset') != DATASET
            or plan.get('identity_sha256') != stream_repack._identity(plan)):
        raise SnapshotError('Binance comparison plan identity differs')
    selected = packed._safe_relative_path(plan['worklist']['name'], 'Binance comparison worklist')
    if len(selected.parts) != 1:
        raise SnapshotError('Binance worklist name is not local')
    worklist = reader.read_stable_bytes(plan_path.parent/selected, maximum=8*1024**2)
    if hashlib.sha256(worklist).hexdigest() != plan['worklist']['sha256']:
        raise SnapshotError('Binance comparison worklist SHA differs')
    records = {}
    with gzip.open(io.BytesIO(worklist), 'rt', encoding='utf-8') as source:
        for line in source:
            row = json.loads(line)
            if row.get('decision') == 'conflict' and row.get('category') == 'parquet':
                name = row['path']
                if name in records or len(records) >= 4096:
                    raise SnapshotError('Binance comparison has duplicate or too many members')
                records[name] = row
    if len(records) != plan['conflict_categories'].get('parquet'):
        raise SnapshotError('Binance comparison member count differs')
    excludes, catalog_sha = planner.policy(DATASET)
    if list(excludes) != plan['excluded_subtrees']:
        raise SnapshotError('Binance publication scope changed')
    current = packed.resolve_latest_packed(cold, DATASET, require_objects=False)
    if current.manifest_sha256 != plan['public_manifest_sha256']:
        raise SnapshotError('Binance public head changed before comparison')
    private_root = private_history_root(cold, version_policy(ROOT), initialize=False)
    private = packed.resolve_packed_snapshot_id(private_root, DATASET, plan['private_snapshot_id'],
                                                 require_objects=False)
    if private.manifest_sha256 != plan['private_manifest_sha256']:
        raise SnapshotError('Binance private fixed generation changed before comparison')
    output = root/('row-audit-'+plan['identity_sha256']+'.json')
    if output.is_symlink():
        raise SnapshotError('Binance audit output is redirected')
    if output.is_file():
        existing = json.loads(reader.read_stable_bytes(output, maximum=4*1024**2))
        if (existing.get('contract') == CONTRACT and existing.get('state') == 'compared'
                and existing.get('comparison_identity_sha256') == plan['identity_sha256']):
            return existing
        raise SnapshotError('Binance audit receipt already exists with different identity')
    status = root/'row-audit-status.json'
    report = {'contract': CONTRACT, 'dataset': DATASET, 'state': 'running',
        'comparison_identity_sha256': plan['identity_sha256'],
        'public_manifest_sha256': plan['public_manifest_sha256'],
        'private_manifest_sha256': plan['private_manifest_sha256'], 'catalog_sha256': catalog_sha,
        'files_total': len(records), 'files_completed': 0,
        'logical_bytes_total': sum(row['public']['size'] for row in records.values()),
        'logical_bytes_completed': 0, 'categories': {}, 'differences': [],
        'source_mutated': False, 'independent_D_readback': False, 'NAS_accepted': False,
        'observed_at_epoch': time.time()}
    last = 0.0

    def save_status(force=False):
        nonlocal last
        if force or time.monotonic()-last >= 5:
            last = time.monotonic()
            report['observed_at_epoch'] = time.time()
            atomic_write_json(status, {key: value for key, value in report.items() if key != 'differences'})

    try:
        next_check = 0.0
        def guard():
            nonlocal next_check
            now = time.monotonic()
            if now >= next_check:
                if not handoff.binance_handoff_window_allowed(for_start=False):
                    raise BlockingIOError('tw_day_trade_priority_window')
                next_check = now+30.0
        guard()
        scratch = root/('row-audit-spool-'+plan['identity_sha256']+'.sqlite')
        controls = Path(config['mount_point'])/'migration/provider-state-temporary-20261103/capacity-leases'
        controls.mkdir(mode=0o700, parents=True, exist_ok=True)
        if controls.is_symlink() or controls.resolve(strict=True) != controls:
            raise SnapshotError('Binance comparison capacity control redirected')
        total = sum(row[side]['size'] for row in records.values() for side in ('private', 'public'))
        largest = max(row[side]['size'] for row in records.values() for side in ('private', 'public'))
        # SQLite payloads, one bounded WAL batch, indexes and allocation overhead.
        needed = total + max(256*1024**2, 16*largest) + total//10 + 128*1024**2
        existing = scratch.stat().st_blocks*512 if scratch.exists() and not scratch.is_symlink() else 0
        growth = max(1024**2, needed-existing)
        with packed._try_publication_lock(root/'row-audit-owner.lock'):
            with reserve_capacity(controls, key=plan['identity_sha256'], maximum_growth_bytes=growth,
                    capacity_bytes=2**63-1, minimum_free_bytes=config['minimum_free_bytes']) as lease:
                workspace_volume.admit(growth+lease['other_reserved_growth_bytes'], c=config)
                spool = FixedSnapshotSpool(scratch, {'comparison_identity_sha256': plan['identity_sha256'],
                    'public_manifest_sha256': plan['public_manifest_sha256'],
                    'private_manifest_sha256': plan['private_manifest_sha256']}, records,
                    maximum_file_bytes=64*1024**2, maximum_side_bytes=4*1024**3)
                try:
                    proofs = compare_fixed_members(spool, records, cold=cold, current=current,
                        private_root=private_root, private=private, report=report, guard=guard, save_status=save_status)
                    with packed._try_publication_lock(cold/'.local-state/locks/publish-retention-global.lock', shared=True), \
                         packed._try_publication_lock(private_root/'.local-state/locks/publish-retention-global.lock', shared=True):
                        guard()
                        workspace_volume.check(c=config)
                        planner._check_d_primary_mount(cold)
                        fixed_private = packed.resolve_packed_snapshot_id(private_root, DATASET,
                            plan['private_snapshot_id'], require_objects=False)
                        if (packed.resolve_latest_packed(cold, DATASET, require_objects=False).manifest_sha256
                                != plan['public_manifest_sha256'] or fixed_private.manifest_sha256
                                != plan['private_manifest_sha256'] or planner.policy(DATASET) != (excludes, catalog_sha)):
                            raise SnapshotError('Binance fixed source or publication generation changed during comparison')
                        spool.remove()
                        report.update(state='compared', stage='completed', stream_scope=proofs['public'],
                                      private_stream_scope=proofs['private'],
                                      source_mode='fixed_public_and_private_D_generations',
                                      scratch_removed=True, observed_at_epoch=time.time())
                        atomic_write_json(output, report, durable=True)
                finally:
                    spool.close()
        save_status(True)
        return report
    except BaseException as error:
        report.update(state='blocked', error_type=type(error).__name__, observed_at_epoch=time.time())
        save_status(True)
        raise


if __name__ == '__main__':
    try:
        value = audit()
        print(json.dumps({key: value for key, value in value.items() if key != 'differences'}, ensure_ascii=False))
    except (OSError, ValueError, SnapshotError) as error:
        print(f'{type(error).__name__}: {error}', file=sys.stderr)
        raise SystemExit(1)
