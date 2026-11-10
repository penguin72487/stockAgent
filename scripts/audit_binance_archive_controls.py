#!/usr/bin/env python3
"""Compare changed Binance control members using the existing immutable source."""
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
from stockagent.data_sync import (binance_state_comparison, packed_member_stream as reader,
                                  packed_snapshots as packed, stream_repack,
                                  workspace_source_handoff as handoff, workspace_volume)  # noqa: E402
from stockagent.data_sync.completed_forward_capacity import reserve_capacity  # noqa: E402
from stockagent.data_sync.fixed_snapshot_spool import FixedSnapshotSpool, populate_pair  # noqa: E402
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json  # noqa: E402
from stockagent.data_sync.dataset_migration import private_history_root  # noqa: E402
from stockagent.data_sync.dataset_version_layout import version_policy  # noqa: E402

DATASET = 'binance-public-archive'
CONTRACT = 'stockagent.binance_archive_control_audit.v1'
CONTROL_NAMES = frozenset({'capacity_receipt.json', 'download_summary.json', 'plan_summary.json',
                           'progress.json', 'state/archive_objects.sqlite3'})


def compare_fixed_controls(spool, selected, *, cold, current, private_root, private, result, guard,
                           save_status, stream=reader.stream_members):
    proofs = populate_pair(spool, selected, sides=(('private', private_root, private), ('public', cold, current)),
                           stream=stream, guard=guard, progress=lambda event: (result.update(event), save_status()),
                           maximum_file_bytes=256*1024**2, maximum_side_bytes=512*1024**2)
    result.update(stage='comparing_fixed_members', stage_files_total=len(selected), stage_files_completed=0)
    save_status(True)
    for member in sorted(selected):
        guard()
        old, new = spool.get('public', member), spool.get('private', member)
        if member.endswith('.sqlite3'):
            result['state_comparison'] = binance_state_comparison.compare_databases(old, new)
        else:
            a, b = json.loads(old), json.loads(new)
            if not isinstance(a, dict) or not isinstance(b, dict):
                raise SnapshotError('Binance JSON control is not a mapping')
            result['changed_json_fields'][member] = sorted(k for k in set(a)|set(b) if a.get(k) != b.get(k))
        result['files_completed'] += 1
        result['stage_files_completed'] = result['files_completed']
        save_status()
    return proofs


def audit() -> dict:
    config = workspace_volume.configuration()
    workspace_volume.check(c=config)
    cold = planner.COLD
    planner._check_d_primary_mount(cold)
    root = workspace_volume.path('migration', create=True)/'handoff-index-archive-v1'/DATASET
    if root.is_symlink() or root.resolve(strict=True) != root:
        raise SnapshotError('Binance control audit root is redirected')
    plans = list(root.glob('*/integration-diff-v1/diff.json'))
    if len(plans) != 1:
        raise SnapshotError('Binance control audit requires one fixed comparison')
    path = plans[0]
    plan = json.loads(reader.read_stable_bytes(path, maximum=1024**2))
    if (plan.get('contract') != 'stockagent.index_generation_diff.v1' or plan.get('dataset') != DATASET
            or plan.get('identity_sha256') != stream_repack._identity(plan)):
        raise SnapshotError('Binance control comparison identity differs')
    name = packed._safe_relative_path(plan['worklist']['name'], 'Binance worklist')
    if len(name.parts) != 1:
        raise SnapshotError('Binance control worklist is not local')
    compressed = reader.read_stable_bytes(path.parent/name, maximum=8*1024**2)
    if hashlib.sha256(compressed).hexdigest() != plan['worklist']['sha256']:
        raise SnapshotError('Binance control worklist SHA differs')
    with gzip.open(io.BytesIO(compressed), 'rt', encoding='utf-8') as stream:
        selected = {row['path']: row for line in stream if (row := json.loads(line)).get('decision') == 'conflict'
                    and row.get('category') != 'parquet'}
    if set(selected) != CONTROL_NAMES:
        raise SnapshotError('Binance changed control namespace needs explicit review')
    excludes, catalog_sha = planner.policy(DATASET)
    if list(excludes) != plan['excluded_subtrees']:
        raise SnapshotError('Binance publication scope changed')
    current = packed.resolve_latest_packed(cold, DATASET, require_objects=False)
    if current.manifest_sha256 != plan['public_manifest_sha256']:
        raise SnapshotError('Binance public generation changed')
    private_root = private_history_root(cold, version_policy(ROOT), initialize=False)
    private = packed.resolve_packed_snapshot_id(private_root, DATASET, plan['private_snapshot_id'],
                                                require_objects=False)
    if private.manifest_sha256 != plan['private_manifest_sha256']:
        raise SnapshotError('Binance private fixed generation changed')
    output = root/('control-audit-'+plan['identity_sha256']+'.json')
    if output.is_symlink():
        raise SnapshotError('Binance control audit receipt is redirected')
    if output.is_file():
        existing = json.loads(reader.read_stable_bytes(output, maximum=1024**2))
        if (existing.get('contract') == CONTRACT and existing.get('state') == 'compared'
                and existing.get('comparison_identity_sha256') == plan['identity_sha256']):
            return existing
        raise SnapshotError('existing Binance control audit receipt differs')
    result = {'contract': CONTRACT, 'dataset': DATASET, 'state': 'running',
        'comparison_identity_sha256': plan['identity_sha256'],
        'public_manifest_sha256': plan['public_manifest_sha256'],
        'private_manifest_sha256': plan['private_manifest_sha256'], 'catalog_sha256': catalog_sha,
        'files_completed': 0, 'files_total': len(selected), 'changed_json_fields': {},
        'source_mutated': False, 'independent_D_readback': False, 'NAS_accepted': False}
    status, last = root/'control-audit-status.json', 0.0
    def save_status(force=False):
        nonlocal last
        if force or time.monotonic()-last >= 5:
            last = time.monotonic()
            result['observed_at_epoch'] = time.time()
            atomic_write_json(status, result)
    try:
        next_check = 0.0
        def guard():
            nonlocal next_check
            if time.monotonic() >= next_check:
                if not handoff.binance_handoff_window_allowed(for_start=False):
                    raise BlockingIOError('tw_day_trade_priority_window')
                next_check = time.monotonic()+30.0
        guard()
        scratch = root/('control-audit-spool-'+plan['identity_sha256']+'.sqlite')
        controls = Path(config['mount_point'])/'migration/provider-state-temporary-20261103/capacity-leases'
        controls.mkdir(mode=0o700, parents=True, exist_ok=True)
        if controls.is_symlink() or controls.resolve(strict=True) != controls:
            raise SnapshotError('Binance control comparison capacity control redirected')
        total = sum(row[side]['size'] for row in selected.values() for side in ('private', 'public'))
        largest = max(row[side]['size'] for row in selected.values() for side in ('private', 'public'))
        needed = total + max(256*1024**2, 16*largest) + total//10 + 128*1024**2
        existing = scratch.stat().st_blocks*512 if scratch.exists() and not scratch.is_symlink() else 0
        growth = max(1024**2, needed-existing)
        with packed._try_publication_lock(root/'control-audit-owner.lock'):
            with reserve_capacity(controls, key=hashlib.sha256((plan['identity_sha256']+'-controls').encode()).hexdigest(),
                    maximum_growth_bytes=growth,
                    capacity_bytes=2**63-1, minimum_free_bytes=config['minimum_free_bytes']) as lease:
                workspace_volume.admit(growth+lease['other_reserved_growth_bytes'], c=config)
                spool = FixedSnapshotSpool(scratch, {'comparison_identity_sha256': plan['identity_sha256'],
                    'public_manifest_sha256': plan['public_manifest_sha256'],
                    'private_manifest_sha256': plan['private_manifest_sha256']}, selected,
                    maximum_file_bytes=256*1024**2, maximum_side_bytes=512*1024**2)
                try:
                    proofs = compare_fixed_controls(spool, selected, cold=cold, current=current,
                        private_root=private_root, private=private, result=result, guard=guard, save_status=save_status)
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
                            raise SnapshotError('Binance fixed source or publication generation changed during control comparison')
                        spool.remove()
                        result.update(state='compared', stage='completed', stream_scope=proofs['public'],
                            private_stream_scope=proofs['private'], source_mode='fixed_public_and_private_D_generations',
                            scratch_removed=True, observed_at_epoch=time.time())
                        atomic_write_json(output, result, durable=True)
                finally:
                    spool.close()
        save_status(True)
        return result
    except BaseException as error:
        result.update(state='blocked', error_type=type(error).__name__, observed_at_epoch=time.time())
        save_status(True)
        raise


if __name__ == '__main__':
    try:
        print(json.dumps(audit(), ensure_ascii=False))
    except (OSError, ValueError, SnapshotError) as error:
        print(f'{type(error).__name__}: {error}', file=sys.stderr)
        raise SystemExit(1)
