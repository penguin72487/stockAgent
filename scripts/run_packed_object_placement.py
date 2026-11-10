#!/usr/bin/env python3
"""Advance one source-bound D -> E service-archive wave under original owners."""
from __future__ import annotations

import argparse
from contextlib import ExitStack, contextmanager
import json
import hashlib
import os
import re
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.run_bounded_storage_maintenance import TradingPriorityGuard
from stockagent.data_sync import packed_object_placement as placement, packed_snapshots as p, workspace_volume, retirement_turn
from stockagent.data_sync.cold_primary import _check_d_primary_mount
from stockagent.data_sync.completed_forward_capacity import reserve_capacity
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json
from stockagent.data_sync.materialized_cache import process_references_many


def preserve_wave_plan(locations, dataset, event):
    """Keep the fixed restore map beside its E packages, before any D unlink.

    A lost C control disk must not turn self-contained E archives into
    unidentified bytes. This is a small immutable control copy, not another
    payload pass or an independent recovery claim.
    """
    from scripts.retire_staged_cold_objects import _identity
    wave = event.get('wave_identity')
    if not isinstance(wave, str) or re.fullmatch('[a-f0-9]{64}', wave) is None:
        raise SnapshotError('archive progress has no fixed wave identity')
    plan = placement.regular_document(locations.state/'waves'/dataset/(wave+'.json'), limit=2*1024**2)
    if (plan.get('identity_sha256') != wave or _identity(plan) != wave
            or plan.get('dataset') != dataset or plan.get('candidate_scope') != placement.CONTRACT):
        raise SnapshotError('archive recovery plan differs from its fixed wave')
    parent = locations.workspace/'archives'/dataset/plan['source_manifest_sha256']
    parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    if parent.resolve(strict=True) != parent:
        raise SnapshotError('archive recovery map directory redirected')
    target = parent/(wave+'.plan.json')
    if os.path.lexists(target):
        if placement.regular_document(target, limit=2*1024**2) != plan:
            raise SnapshotError('archive recovery map differs from the original')
    else:
        atomic_write_json(target, plan, durable=True)


def remaining_wave_growth(locations, cold, plan):
    """Credit only the existing copier's validated, durable E generations."""
    from scripts import stage_d_cold_relocation as staging
    from scripts.retire_staged_cold_objects import closed_E_handoff
    target = locations.workspace/'archives'/plan['dataset']/plan['source_manifest_sha256']/plan['identity_sha256']
    copied = []
    if (target/'write-complete.json').exists():
        result = staging.recover_fast_result(plan, destination=target)
        copied = closed_E_handoff(plan, result, target)
    elif (target/'write-progress.json').exists():
        copied = staging._resume_records(plan, cold=cold, destination=target)
    return max(0, plan['bytes']-sum(row['bytes'] for row in copied))


def select_archive_generation(cold, dataset, latest, enrolled, *, guard=lambda: None):
    """Move retained old encodings first; preserve every logical manifest.

    Ordinary retirement independently removes unreferenced generations. This
    copier may also stage research-pinned history because the content identity
    and logical path remain unchanged. Missing historical objects are not a
    reason to stop another generation from making progress.
    """
    paths = sorted((cold/'manifests'/dataset).glob('*.json'))
    if len(paths) > 10000:
        raise SnapshotError('archive history selection exceeds its metadata bound')
    for path in paths:
        guard()
        if path.stem == latest.manifest['snapshot_id']:
            continue
        release = p.resolve_packed_snapshot_id(cold, dataset, path.stem, require_objects=False)
        if release.manifest['archive'].get('new_pack_layout') == 'source-dataset-v1':
            continue
        for number, item in enumerate(release.manifest['archive']['objects']):
            if number % 256 == 0:
                guard()
            if item['relpath'] in enrolled:
                continue
            try:
                info = (cold/item['relpath']).lstat()
            except FileNotFoundError:
                continue
            if info.st_nlink == 1 and info.st_size == item['bytes']:
                return release
    return latest


def run(*, apply=False, selected_dataset=None) -> dict:
    policy = placement.load_policy()
    if policy is None:
        return {'contract': placement.CONTRACT+'.queue', 'state': 'not_enrolled'}
    workspace = Path(policy['workspace_root'])
    public = Path(policy['cold_root'])
    cold = public/'.local-state/private-history'
    state = Path(policy['state_root'])
    locations = placement.Locations(state, workspace, public)
    if apply:
        try:
            placement.require_reader_rollout(state)
        except (OSError, ValueError, SnapshotError) as error:
            value = {'contract': placement.CONTRACT+'.queue', 'state': 'waiting_reader_rollout',
                     'reason': str(error)[:240], 'observed_at_epoch': time.time(), 'source_deleted': False}
            state.mkdir(mode=0o700, parents=True, exist_ok=True)
            atomic_write_json(state/'status.json', value, durable=True)
            return value
    priority = TradingPriorityGuard()
    checked = 0.0
    live_status = None
    status_observed = 0.0
    def guard():
        nonlocal checked, status_observed
        priority.check()
        if time.monotonic()-checked >= 2:
            if apply:
                placement.require_reader_rollout(state)
            workspace_volume.check()
            _check_d_primary_mount(public)
            checked = time.monotonic()
        if live_status is not None and time.monotonic()-status_observed >= 15:
            # Metadata selection can involve slow HDD lookups before the
            # copier emits its first byte. Observe that real work without
            # inventing a byte counter or keeping the previous wait reason.
            live_status['observed_at_epoch'] = time.time()
            atomic_write_json(state/'status.json', live_status, durable=False)
            status_observed = time.monotonic()
    guard()
    names = policy['warm_private_datasets']
    if selected_dataset is not None:
        if selected_dataset not in names:
            raise SnapshotError('dataset is not enrolled for the E archive tier')
        names = [selected_dataset]
    result = {'contract': placement.CONTRACT+'.queue', 'state': 'checking',
              'observed_at_epoch': time.time(), 'entries': [], 'source_values_changed': False,
              'remote_execution': False, 'NAS_accepted': False, 'independent_readback': False}
    state.mkdir(mode=0o700, parents=True, exist_ok=True)
    turn_root = workspace/'migration/source-local-exact-repack'
    turn_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    status = state/'status.json'
    with p._try_publication_lock(state/'queue-owner.lock'):
        previous = placement.regular_document(status, limit=1024**2) if status.exists() else {}
        cursor = previous.get('last_attempted_dataset')
        if cursor in names:
            index = names.index(cursor)+1
            names = names[index:]+names[:index]
        result['last_attempted_dataset'] = cursor
        live_status = result
        for name in names:
            entry = {'dataset': name, 'destination_tier': 'E', 'state': 'checking'}
            result['entries'].append(entry)
            result.update(state='checking', observed_at_epoch=time.time())
            atomic_write_json(status, result, durable=False)
            status_observed = time.monotonic()
            try:
                with ExitStack() as owners:
                    # A continuous reader is also a GC blocker. Join the
                    # original fair turn before publication leases so an
                    # already-reserved retirement gets the next wave gap.
                    # An admitted immutable wave still finishes unchanged.
                    owners.enter_context(retirement_turn.turn(turn_root, retirement=False))
                    # The same authority, publication, backup and transport
                    # owners fence placement. No lock inode is removed.
                    shared_leases = ExitStack()
                    owners.callback(shared_leases.close)
                    global_paths = [public/'.local-state/locks/publish-retention-global.lock',
                                    cold/'.local-state/locks/publish-retention-global.lock']
                    def acquire_shared():
                        for lock in global_paths:
                            shared_leases.enter_context(p._try_publication_lock(lock, shared=True))
                    acquire_shared()
                    owners.enter_context(p._try_publication_lock(cold/'.local-state/locks'/f'publish-{name}.lock'))
                    @contextmanager
                    def handoff_owner():
                        # Keep both global READ leases: they fence global GC.
                        # The exact dataset lease is already exclusive. Only
                        # backup/replication readers need a short exclusive
                        # handoff; unrelated long-running publishers must not
                        # force every dataset's read lease to drain first.
                        with ExitStack() as boundary:
                            for lock in [Path('/var/lib/stockagent/backup-stream/cycle.lock'),
                                    Path('/var/lib/stockagent/lakehouse-control/source-replication-owner.lock')]:
                                boundary.enter_context(p._try_publication_lock(lock))
                            yield
                    guard()
                    latest = p.resolve_latest_packed(cold, name, require_objects=False)
                    wanted, count, shared_names, placed = [], 0, 0, 0
                    placed_bytes = shared_bytes = remaining_bytes = remaining_objects = 0
                    oversized = 0
                    reader = locations._reader()
                    enrolled = {} if reader is None else {row['relative']: dict(row) for row in reader.execute(
                        "SELECT * FROM objects WHERE authority='private' AND state!='E_retired'")}
                    current = select_archive_generation(cold, name, latest, enrolled, guard=guard)
                    missing = 0
                    for number, item in enumerate(sorted(current.manifest['archive']['objects'], key=lambda row: row['relpath'])):
                        if number % 256 == 0:
                            guard()
                        known = enrolled.get(item['relpath'])
                        if known is not None:
                            # This is scheduling telemetry, not a read proof.
                            # Actual readers and the handoff check the exact E
                            # generation; do not restat every moved object here.
                            if known['sha256'] != item['sha256'] or known['bytes'] != item['bytes']:
                                raise SnapshotError('current object differs from enrolled placement')
                            placed += 1
                            placed_bytes += item['bytes']
                            continue
                        remaining_objects += 1
                        remaining_bytes += item['bytes']
                        if item['bytes'] > policy['maximum_wave_bytes']:
                            oversized += 1
                            continue
                        if count+item['bytes'] > policy['maximum_wave_bytes'] or len(wanted) >= 512:
                            continue
                        path = cold/item['relpath']
                        try:
                            before = path.lstat()
                        except FileNotFoundError:
                            if current.manifest_sha256 == latest.manifest_sha256:
                                raise
                            missing += 1
                            continue
                        if before.st_nlink != 1:
                            shared_names += 1
                            shared_bytes += item['bytes']
                            continue
                        if before.st_size != item['bytes'] or path.is_symlink():
                            raise SnapshotError('current source descriptor changed')
                        wanted.append(dict(item))
                        count += item['bytes']
                    pending = locations.pending(name)
                    pending_plans = locations.state/'waves'/name
                    interrupted = []
                    for plan_path in sorted(pending_plans.glob('*.json')):
                        if not plan_path.with_suffix('.done').exists():
                            interrupted.append(placement.regular_document(plan_path, limit=2*1024**2))
                    entry.update(manifest_sha256=current.manifest_sha256, objects_in_wave=len(wanted),
                                 bytes_in_wave=count, already_placed_objects=placed,
                                 already_placed_bytes=placed_bytes,
                                 current_head_objects=len(current.manifest['archive']['objects']),
                                 current_head_bytes=sum(item['bytes'] for item in current.manifest['archive']['objects']),
                                 remaining_objects=remaining_objects, remaining_bytes=remaining_bytes,
                                 shared_D_names_retained=shared_names, shared_D_bytes_retained=shared_bytes,
                                 oversized_objects_retained=oversized, pending_handoffs=len(pending),
                                 selected_historical=current.manifest_sha256 != latest.manifest_sha256,
                                 historical_missing_objects=missing,
                                 state='planned' if wanted or pending or interrupted else 'current_head_placed_or_shared')
                    if not apply or not (wanted or pending or interrupted):
                        continue
                    if interrupted:
                        # A prior fixed generation is resumed before this head.
                        count = interrupted[0]['bytes']
                        entry.update(bytes_in_wave=count, objects_in_wave=interrupted[0]['files'])
                        preserve_wave_plan(locations, name, {'wave_identity': interrupted[0]['identity_sha256']})
                    growth = count+16*1024**2
                    if interrupted:
                        growth = remaining_wave_growth(locations, cold, interrupted[0])+16*1024**2
                    def admit():
                        lease_key = hashlib.sha256((placement.CONTRACT+name+current.manifest_sha256).encode()).hexdigest()
                        with reserve_capacity(workspace/'migration/provider-state-temporary-20261103/capacity-leases', key=lease_key,
                                maximum_growth_bytes=growth, capacity_bytes=2**63-1,
                                minimum_free_bytes=policy['reserve_bytes']) as lease:
                            workspace_volume.admit(growth+lease['other_reserved_growth_bytes']+
                                max(0, policy['reserve_bytes']-workspace_volume.configuration()['minimum_free_bytes']))
                            def progress(event):
                                guard()
                                preserve_wave_plan(locations, name, event)
                                entry.update(state='running', progress=event)
                                result.update(state='running', observed_at_epoch=time.time())
                                atomic_write_json(status, result, durable=True)
                            return placement.relocate(locations, cold, name, current.manifest_sha256, wanted,
                                guard=guard, process_check=lambda paths: process_references_many(
                                    paths, immutable_regular_paths=True), progress=progress, handoff_owner=handoff_owner)
                    outcome = admit()
                    entry.update(state='wave_completed', completed_files=outcome['wave_files'],
                                 completed_bytes=outcome['wave_bytes'], progress=outcome.get('progress', entry.get('progress')))
                    result.update(state='wave_completed', totals=locations.totals(), observed_at_epoch=time.time())
                    result['last_attempted_dataset'] = name
                    atomic_write_json(status, result, durable=True)
                    return result  # One bounded wave, then release every original owner.
            except (OSError, ValueError, RuntimeError, SnapshotError) as error:
                entry.update(state='waiting' if isinstance(error, p.PackedPublicationBusy) else 'blocked',
                             reason=str(error)[:240], error_type=type(error).__name__)
                if isinstance(error, p.PackedPublicationBusy) and any(
                        key in str(error) for key in (retirement_turn.LOCK_NAME, retirement_turn.INTENT_LOCK_NAME)):
                    entry['wait_kind'] = 'retirement_turn'
                if entry.get('progress'):
                    # One byte-copy wave per invocation even when its short
                    # handoff is busy. Resume it later without copying again.
                    result.update(state='waiting_or_current', observed_at_epoch=time.time(),
                                  last_attempted_dataset=name, totals=locations.totals())
                    atomic_write_json(status, result, durable=True)
                    return result
                # A busy dataset never blocks another independently eligible cohort.
                continue
        result.update(state='planned' if not apply else 'waiting_or_current',
                      observed_at_epoch=time.time(), totals=locations.totals())
        atomic_write_json(status, result, durable=True)
        return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--dataset')
    args = parser.parse_args()
    os.umask(0o077)
    value = run(apply=args.apply, selected_dataset=args.dataset)
    print(json.dumps(value, ensure_ascii=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
