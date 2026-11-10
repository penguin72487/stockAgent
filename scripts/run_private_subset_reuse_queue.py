#!/usr/bin/env python3
"""Automatically share fixed same-root private packs through existing owners."""
from __future__ import annotations

from contextlib import ExitStack
import fcntl
import json
import os
from pathlib import Path
import shutil
import stat
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.run_bounded_storage_maintenance import TradingPriorityGuard
from scripts.run_source_local_repack_queue import source_layout_confirmed
from scripts.run_stream_packed_repack import cleanup_completed_stage, private_publisher
from stockagent.data_sync import dataset_migration as migration, packed_snapshots as p
from stockagent.data_sync import private_subset_reuse as subset, retirement_turn, stream_repack as s, workspace_volume
from stockagent.data_sync.cold_primary import _check_d_primary_mount
from stockagent.data_sync.completed_forward_capacity import reserve_capacity
from stockagent.data_sync.dataset_version_layout import version_policy
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json
from stockagent.data_sync.packed_member_stream import read_stable_bytes

PRIMARY = Path('/srv/stockagent-packed')
LIFECYCLE = Path('/var/lib/stockagent/lakehouse-control/dataset-versions')
CONTRACT = 'stockagent.private_same_root_subset_queue.v1'
D_RESERVE = 32 * 1024**3


def _install_owner_busy(workspace: Path) -> bool:
    """Scheduling hint for a sealed E inventory, never an admission grant."""
    path = workspace/'migration/cold-install-write-owner.lock'
    try:
        fd = os.open(path, os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC)
    except FileNotFoundError:
        return False
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or not os.path.samestat(info, path.lstat()):
            raise SnapshotError('D install owner lock changed')
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return True
        return False
    finally:
        os.close(fd)


def _current_operation(root: Path):
    current = subset._stable_document(root/'current.json', limit=1024*1024)
    directory = Path(current.get('operation_directory', '')).absolute()
    expected_parent = root/'operations'
    if (current.get('contract') != migration.CONTRACT or directory.parent != expected_parent
            or directory.is_symlink() or directory.resolve(strict=True) != directory):
        raise SnapshotError('original lifecycle operation identity differs')
    progress = subset._stable_document(directory/'progress.json')
    if (progress.get('contract') != migration.CONTRACT
            or progress.get('plan_sha256') != current.get('plan_sha256')
            or not isinstance(progress.get('datasets'), dict)):
        raise SnapshotError('original lifecycle progress identity differs')
    return directory, progress


def _select(cold: Path, workspace: Path, target: str, donor: str, progress: dict):
    row = progress['datasets'].get(target)
    if (not isinstance(row, dict) or row.get('domain') != 'host_private'
            or row.get('state') not in {'verified', 'recovery_deferred_user_migration'}):
        return None, 'waiting_lifecycle'
    old_sha = p._validate_hash(row.get('manifest_sha256'), 'lifecycle original SHA')
    old_id = p.validate_slug(row.get('snapshot_id'), 'lifecycle original snapshot')
    latest = p.resolve_latest_packed(cold, target, require_objects=False)
    private_publisher(cold, target, latest)
    # An already published subset has its own canonical receipt and is not
    # re-enrolled every minute, even after the lifecycle rebinds its head.
    receipt_dir = workspace/'migration/source-local-exact-repack'/target
    current_receipt = receipt_dir/(latest.manifest_sha256+'.json')
    if current_receipt.is_file() and not current_receipt.is_symlink():
        receipt = subset._stable_document(current_receipt, limit=1024*1024)
        if (receipt.get('source_mode') == subset.SOURCE_MODE
                and receipt.get('new_manifest_sha256') == latest.manifest_sha256
                and receipt.get('private') is True):
            return {'receipt':receipt}, 'published'
    stage = workspace/'migration/stream-source-repack-v1'/target/old_sha
    if latest.manifest_sha256 != old_sha:
        # Only our exact post-head crash may use an older lifecycle row.
        try:
            intent = subset._stable_document(stage/'intent.json', limit=16384)
            journal = subset._stable_document(stage/'install.json', limit=32*1024**2)
            if (intent.get('subset_donor') != donor
                    or journal.get('new_manifest_sha256') != latest.manifest_sha256
                    or journal.get('old_snapshot_id') != old_id):
                return None, 'waiting_lifecycle_refresh'
            donor_sha = p._validate_hash(intent.get('subset_donor_manifest_sha256'), 'saved donor SHA')
        except (OSError, ValueError, TypeError, SnapshotError):
            return None, 'waiting_lifecycle_refresh'
    else:
        if latest.manifest['snapshot_id'] != old_id:
            raise SnapshotError('lifecycle original snapshot identity differs')
        fixed_donor = p.resolve_latest_packed(cold, donor, require_objects=False)
        private_publisher(cold, donor, fixed_donor)
        donor_sha = fixed_donor.manifest_sha256
        if (stage/'install.json').is_file() and not (stage/'install.json').is_symlink():
            # A D install must resume its exact donor even if a later donor
            # head appears; a pre-install E candidate may be superseded.
            intent = subset._stable_document(stage/'intent.json', limit=16384)
            install = subset._stable_document(stage/'install.json')
            if (intent.get('contract') != s.CONTRACT+'.intent' or intent.get('dataset') != target
                    or intent.get('old_manifest_sha256') != old_sha or intent.get('subset_donor') != donor
                    or install.get('contract') != s.CONTRACT+'.install' or install.get('dataset') != target
                    or install.get('old_manifest_sha256') != old_sha):
                raise SnapshotError('existing same-root D install identity differs')
            donor_sha = p._validate_hash(intent.get('subset_donor_manifest_sha256'), 'installed donor SHA')
        if subset.incompatible(workspace, target, old_sha, donor_sha):
            return None, 'donor_not_covering_target'
    return {'dataset':target, 'donor':donor, 'old_sha':old_sha, 'donor_sha':donor_sha,
            'stage':stage, 'latest':latest}, None


def run_one(cold: Path, workspace: Path, config: dict, selected: dict, *, guard, report):
    target, donor = selected['dataset'], selected['donor']
    old_sha, donor_sha, stage = selected['old_sha'], selected['donor_sha'], selected['stage']
    node = private_publisher(cold, target, selected['latest'])
    fixed_donor = p.resolve_latest_packed(cold, donor, require_objects=False)
    if fixed_donor.manifest_sha256 != donor_sha:
        candidate = subset._stable_document(stage/'subset-prepared.json')
        donor_id = p.validate_slug(candidate.get('donor_snapshot_id'), 'fixed donor snapshot')
        fixed_donor = p.resolve_packed_snapshot_id(cold, donor, donor_id, require_objects=False)
        if fixed_donor.manifest_sha256 != donor_sha:
            raise SnapshotError('installed same-root donor identity differs')
    # Budget metadata scratch only. The donor's large immutable payload is
    # shared by reference; it is not copied or unpacked by this operation.
    source = selected['latest'].manifest['archive']['inventory']['bytes']
    other = fixed_donor.manifest['archive']['inventory']['bytes']
    budget = max(source, other)*3 + 128*1024**2
    controls = workspace/'migration/provider-state-temporary-20261103/capacity-leases'
    turn_root = workspace/'migration/source-local-exact-repack'
    install_turn = ExitStack()
    def donor_check(authority, name, manifest):
        fixed = p.resolve_packed_snapshot_id(authority, name, manifest['snapshot_id'], require_objects=False)
        if fixed.manifest_sha256 != donor_sha or fixed.manifest != manifest:
            raise SnapshotError('same-root subset donor changed')
        return subset.retained_handoff_donor_proof(authority, workspace, fixed,
            layout_confirmed=source_layout_confirmed(authority, name, manifest))
    def admit_install(prepared, _journal):
        guard(force=True)
        install_turn.enter_context(retirement_turn.turn(turn_root, retirement=False))
        if shutil.disk_usage(PRIMARY).free < prepared['inventory']['bytes']+D_RESERVE:
            raise SnapshotError('D inventory installation reserve unavailable')
    def admit_next(item):
        guard(force=True)
        if shutil.disk_usage(PRIMARY).free < item['bytes']+D_RESERVE:
            raise SnapshotError('D inventory installation reserve unavailable')
    with install_turn, reserve_capacity(controls, key=old_sha, maximum_growth_bytes=budget,
                                        capacity_bytes=2**63-1, minimum_free_bytes=config['minimum_free_bytes']) as lease:
        workspace_volume.admit(budget+lease['other_reserved_growth_bytes'], c=config)
        result = s.repack(cold, stage, target, old_sha, node_id=node, guard=guard, progress=report,
            admit_install=admit_install, admit_next=admit_next,
            install_lock=workspace/'migration/cold-install-write-owner.lock',
            materialized_pin_root=Path('/srv/stockagent-packed-materialized'),
            subset_donor=(donor, donor_sha), subset_layout_check=donor_check,
            before_head_commit=lambda _prepared: guard(force=True))
    receipt = {'contract':'stockagent.source_local_exact_repack.v1',
        'state':'new_D_head_committed_from_exact_E_source', 'dataset':target,
        'old_manifest_sha256':old_sha, 'new_snapshot_id':result['snapshot_id'],
        'new_manifest_sha256':result['new_manifest_sha256'], 'files':result['files'],
        'logical_bytes':result['logical_bytes'], 'new_object_count':1,
        'referenced_object_count':result['objects'],
        'pack_layout':p.SOURCE_DATASET_PACK_LAYOUT, 'old_release_removed':False,
        'independent_D_readback':False, 'NAS_accepted':False, 'private':True,
        'source_mode':subset.SOURCE_MODE, 'packed_format':subset.FORMAT,
        'source_inventory_sha_compared':True, 'source_member_sha_streamed':False,
        'donor_dataset':donor, 'donor_manifest_sha256':donor_sha,
        'observed_at_epoch':time.time()}
    receipt['E_stage_cleanup'] = cleanup_completed_stage(cold, stage, receipt, guard=guard)
    destination = workspace/'migration/source-local-exact-repack'/target/(result['new_manifest_sha256']+'.json')
    destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    if os.path.lexists(destination):
        raise SnapshotError('same-root subset receipt already exists; preserve canonical owner state')
    atomic_write_json(destination, receipt, durable=True)
    return receipt


def main():
    config = workspace_volume.configuration()
    workspace = Path(config['mount_point'])
    workspace_volume.check(c=config)
    _check_d_primary_mount(PRIMARY)
    cold = migration.private_history_root(PRIMARY, version_policy(ROOT), initialize=False)
    status_path = workspace/'migration/source-local-exact-repack/private-subset-status.json'
    status = {'contract':CONTRACT, 'observed_at_epoch':time.time(),
              'entries':[{'dataset':name, 'state':'waiting', 'reason':'pending_turn'}
                         for name in subset.REVIEWED_PAIRS],
              'independent_D_readback':False, 'NAS_accepted':False}
    def save():
        status['observed_at_epoch'] = time.time()
        atomic_write_json(status_path, status, durable=True)
    priority = TradingPriorityGuard()
    checked = [0.0]
    def guard(*, force=False):
        priority.check()
        if force or time.monotonic()-checked[0] >= 10:
            workspace_volume.check(c=config)
            _check_d_primary_mount(PRIMARY)
            if migration.private_history_root(PRIMARY, version_policy(ROOT), initialize=False) != cold:
                raise SnapshotError('private D authority changed')
            checked[0] = time.monotonic()
    with ExitStack() as owners:
        try:
            owners.enter_context(migration._operation_owner(LIFECYCLE))
            directory, _ = _current_operation(LIFECYCLE)
            owners.enter_context(migration._operation_owner(directory))
        except p.PackedPublicationBusy:
            status['entries'] = [{'dataset':name, 'state':'waiting', 'reason':'original_owner_busy'}
                                 for name in subset.REVIEWED_PAIRS]
            save()
            return 0
        directory2, progress = _current_operation(LIFECYCLE)
        if directory2 != directory:
            raise SnapshotError('original lifecycle operation changed')
        for (target, donor), row in zip(subset.REVIEWED_PAIRS.items(), status['entries']):
            save()
            try:
                selected, reason = _select(cold, workspace, target, donor, progress)
                if reason == 'published':
                    recorded = selected['receipt']
                    row.update(state='published', reason=None, files=recorded.get('files'),
                               logical_bytes=recorded.get('logical_bytes'), retirement='queued',
                               generation=recorded.get('old_manifest_sha256'),
                               new_manifest_sha256=recorded.get('new_manifest_sha256'))
                    save()
                    continue
                if selected is None:
                    row.update(state='blocked' if reason == 'donor_not_covering_target' else 'waiting', reason=reason)
                    save()
                    continue
                prepared_path, intent_path = selected['stage']/'prepared.json', selected['stage']/'intent.json'
                same_donor = False
                if prepared_path.is_file() and intent_path.is_file() and not intent_path.is_symlink():
                    same_donor = subset._stable_document(intent_path, limit=16384).get(
                        'subset_donor_manifest_sha256') == selected['donor_sha']
                if (same_donor
                        and selected['latest'].manifest_sha256 == selected['old_sha']
                        and _install_owner_busy(workspace)):
                    row.update(state='waiting', reason='original_owner_busy', generation=selected['old_sha'])
                    save()
                    continue
                row.update(state='running', stage='preparing_fixed_generations', reason=None)
                row['generation'] = selected['old_sha']
                save()
                last = [0.0]
                def report(event):
                    if time.monotonic()-last[0] >= 3:
                        last[0] = time.monotonic()
                        row['progress'] = event
                        save()
                guard()
                receipt = run_one(cold, workspace, config, selected, guard=guard, report=report)
                row.update(state='published', reason=None, files=receipt['files'],
                           logical_bytes=receipt['logical_bytes'], retirement='queued',
                           new_manifest_sha256=receipt['new_manifest_sha256'])
                row.pop('progress', None)
                save()
                break  # Give the existing retirement owner a turn.
            except p.PackedPublicationBusy:
                row.update(state='waiting', reason='original_owner_busy')
            except (OSError, ValueError, TypeError, KeyError, SnapshotError) as error:
                text = str(error)
                reason = ('donor_generation_unproven' if 'donor installed object generation changed' in text else
                          'capacity_or_volume' if 'reserve' in text or 'workspace' in text else
                          'target_or_donor_changed' if 'changed' in text or 'differs' in text else
                          'comparison_or_source_error')
                print(f'{target}: {type(error).__name__}: {text}', file=sys.stderr, flush=True)
                row.update(state='blocked', reason=reason, error_type=type(error).__name__)
            save()
    save()
    return 0


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except (OSError, ValueError, TypeError, KeyError, SnapshotError) as error:
        print(type(error).__name__+': '+str(error), file=sys.stderr)
        raise SystemExit(1)
