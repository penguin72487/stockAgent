#!/usr/bin/env python3
"""Resume an exact user-approved list of disposable Penguin performance copies.

Reuse the canonical discard transaction and shared artifact mutation owner.
No data discovery, cold/NAS deletion, forced lock removal or preservation claim.
"""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import stat
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from stockagent.data_sync import artifact_discard as discard
from stockagent.data_sync.artifact_discard import write_discard_receipt as atomic_write_json
from downloader.artifact_io import atomic_write_bytes
from stockagent.data_sync.testing_disposition import (D_WORKFLOW_TESTING_CONTRACT, LOCAL_COPY_CONTRACTS,
                                                     load_policy, real, target_path)
from scripts.retire_local_offline_artifacts import COMMON_OWNER

CONTRACT = 'stockagent.local_testing_cleanup.v1'
IMPLEMENTATION_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
EXECUTION_SHA256 = hashlib.sha256((IMPLEMENTATION_SHA256 + hashlib.sha256(
    Path(discard.__file__).read_bytes()).hexdigest()).encode('ascii')).hexdigest()
COMPARISON_RECEIPTS = (
    'optimization_acceptance_readback.json', 'source_disposition.json', 'acceptance.json',
    'comparison.json', 'summary.json', 'summary.csv', 'abba-receipt.json', 'receipt.json',
    'owner_final_acceptance.json', 'owner_snapshot_acceptance.json', 'full_sql_complete_pair.json',
    'fixture.json', 'immutable-inputs.json', 'current-all-file-actual-open-count-control.json', 'collection.json',
)


def retain_small_receipts(source: Path, destination: Path) -> None:
    """Keep bounded conclusions and failures, without retaining test payloads."""
    if not source.is_dir():
        return
    destination.mkdir(parents=True, mode=0o700, exist_ok=True)
    records = []
    for name in COMPARISON_RECEIPTS:
        path = source / name
        if not path.exists() or path.is_symlink():
            continue
        if not stat.S_ISREG(path.lstat().st_mode):
            continue
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC)
        with os.fdopen(descriptor, 'rb') as handle:
            if not stat.S_ISREG(os.fstat(handle.fileno()).st_mode):
                raise ValueError('comparison receipt became a special entry')
            before = discard.signature(os.fstat(handle.fileno()))
            if before[2] > 1024**2:
                records.append(dict(name=name, bytes=before[2], copied=False, reason='bounded_receipts_only'))
                continue
            raw = handle.read(1024**2+1)
            if (len(raw) != before[2] or discard.signature(os.fstat(handle.fileno())) != before
                    or discard.signature(path.lstat()) != before):
                raise ValueError('comparison receipt changed before disposable-copy deletion')
        atomic_write_bytes(destination / name, raw)
        records.append(dict(name=name, bytes=len(raw), copied=True, sha256=hashlib.sha256(raw).hexdigest()))
    atomic_write_json(destination / 'retained-receipts.json', dict(receipts=records,
        full_measurement_payload_preserved=False, recovery_claimed=False))


def cycle(policy_path: Path, state: Path, *, apply: bool, repo: Path = ROOT) -> dict:
    policy = load_policy(policy_path)
    if policy['contract'] not in LOCAL_COPY_CONTRACTS:
        raise ValueError('only an explicit local performance-copy disposition is allowed')
    state = real(state.absolute())
    state.mkdir(parents=True, mode=0o700, exist_ok=True)
    with real(state / 'owner.lock').open('a') as owner:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        status_path = state / 'status.json'
        previous = json.loads(status_path.read_bytes()) if status_path.exists() else {}
        if previous and previous.get('policy_identity_sha256') != policy['identity_sha256']:
            raise ValueError('retain the previous cleanup generation under a different state root')
        prior = {row['target_key']: row for row in previous.get('results', [])}
        started = time.monotonic()
        options = dict(repo_root=repo, engineering_root=repo.parent, state_root=state / 'transactions',
                       materialized_root=Path('/srv/stockagent-packed-materialized'),
                       cold_root=Path('/srv/stockagent-packed'))
        selected = []
        for target in policy['targets']:
            source = target_path(policy, target, repo_root=repo, engineering_root=repo.parent)
            if source == state or source in state.parents or state in source.parents:
                raise ValueError('cleanup evidence overlaps its target')
            selected.append((target, source, discard.identity(target)))
        results = []

        def publish(phase, current=None):
            merged = {**prior, **{row['target_key']: row for row in results}}
            observed_results = [merged[key] for _target, _source, key in selected if key in merged]
            terminal = {'discarded-without-preservation', 'already-absent'}
            value = dict(contract=CONTRACT, policy_identity_sha256=policy['identity_sha256'],
                state=phase, observed_at_epoch=time.time(), current_target=current,
                targets_total=len(selected), targets_completed=sum(r['state'] in terminal for r in observed_results),
                targets_blocked=sum(r['state'] == 'protected' for r in observed_results),
                targets_failed=sum(r['state'] in {'retry_pending', 'retry_exhausted'} for r in observed_results),
                reclaimed_allocated_bytes=sum(r.get('reclaimed_allocated_bytes', 0) for r in observed_results),
                discarded_logical_bytes=sum(r.get('logical_bytes', 0) for r in observed_results if r.get('deleted')),
                results=observed_results, cycle_seconds=time.monotonic()-started, apply=apply,
                implementation_sha256=IMPLEMENTATION_SHA256,
                execution_sha256=EXECUTION_SHA256,
                preservation_waived_by_user=True, recovery_claimed=False,
                cold_objects_deleted=False, nas_objects_deleted=False)
            atomic_write_json(status_path, value)
            return value

        publish('checking_exact_targets')
        for target, source, key in selected:
            old = prior.get(key, {})
            if old.get('state') in {'discarded-without-preservation', 'already-absent'}:
                # A recreated producer output is a new generation, never an
                # authorization to erase the same pathname a second time.
                if source.exists() or source.is_symlink():
                    results.append(dict(target_key=key, state='protected', reason='target_recreated',
                                        relative=target['relative'], deleted=False,
                                        reclaimed_allocated_bytes=old.get('reclaimed_allocated_bytes', 0)))
                else:
                    results.append(old)
                continue
            resumed_error_receipt = None
            if old.get('error_attempts', 0) >= 3:
                old_code = old.get('execution_sha256', previous.get('execution_sha256'))
                changed = (old_code != EXECUTION_SHA256 if old_code else
                           previous.get('implementation_sha256') != IMPLEMENTATION_SHA256)
                if not changed:
                    results.append(old)
                    continue
                # A real repair may retry; repeated minute checks on the same
                # failed implementation remain bounded. Keep the original
                # exhausted evidence instead of clearing history in place.
                history = state / 'retry-history' / (key + '-' + discard.identity(old) + '.json')
                history.parent.mkdir(mode=0o700, exist_ok=True)
                atomic_write_json(history, dict(previous_error=old,
                    previous_implementation_sha256=previous.get('implementation_sha256'),
                    resumed_execution_sha256=EXECUTION_SHA256))
                resumed_error_receipt = history.relative_to(state).as_posix()
                old = {**old, 'error_attempts': 0}
            publish('checking_consumers', key)
            try:
                journal = options['state_root'] / (key + '.json')
                if apply and journal.exists():
                    with real(COMMON_OWNER).open('a') as mutation_owner:
                        fcntl.flock(mutation_owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
                        discard.restore_unchanged_discard_quarantine(policy, target, **options)
                    terminal = json.loads(journal.read_bytes())
                    result = terminal.get('result') if terminal.get('state') == 'discarded' else None
                    if (policy['contract'] in LOCAL_COPY_CONTRACTS and isinstance(result, dict)
                            and result.get('target') == target and result.get('deleted') is True
                            and result.get('policy_identity_sha256') == policy['identity_sha256']
                            and type(result.get('reclaimed_allocated_bytes')) is int
                            and result['reclaimed_allocated_bytes'] >= 0):
                        if source.exists() or source.is_symlink():
                            results.append(dict(target_key=key, state='protected', reason='target_recreated',
                                                relative=target['relative'], deleted=False))
                        else:
                            results.append(dict(result, target_key=key, relative=target['relative']))
                        publish('checking_exact_targets')
                        continue
                plan = discard.discard_target(policy, target, **options)
                if plan.get('blockers'):
                    result = dict(state='protected', reason='current_consumer_or_type_gate',
                                  blockers=plan['blockers'], deleted=False)
                elif apply and plan['state'] != 'already-absent':
                    atomic_write_json(state / ('dry-' + key + '.json'), plan)
                    publish('waiting_artifact_owner', key)
                    with real(COMMON_OWNER).open('a') as mutation_owner:
                        fcntl.flock(mutation_owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
                        if load_policy(policy_path) != policy:
                            raise ValueError('user disposition changed before mutation')
                        retain_small_receipts(source, state / 'retained-evidence' / key)
                        publish('discarding_exact_target', key)
                        result = discard.discard_target(policy, target, **options, apply=True,
                                                       expected_plan_identity=plan['identity_sha256'])
                else:
                    result = {k: v for k, v in plan.items() if k not in {'observed', 'source'}}
                results.append(dict(result, target_key=key, relative=target['relative']))
            except BlockingIOError:
                results.append(dict(target_key=key, state='protected', reason='canonical_owner_busy',
                                    relative=target['relative'], deleted=False))
            except (OSError, ValueError, discard.SnapshotError) as error:
                attempts = old.get('error_attempts', 0) + 1
                results.append(dict(target_key=key, state='retry_exhausted' if attempts >= 3 else 'retry_pending',
                    error_attempts=attempts, error_type=type(error).__name__, detail=str(error)[:512],
                    execution_sha256=EXECUTION_SHA256,
                    relative=target['relative'], deleted=False, reclaimed_allocated_bytes=0))
            if resumed_error_receipt:
                results[-1]['resumed_error_receipt'] = resumed_error_receipt
            publish('checking_exact_targets')
        done = all(row['state'] in {'discarded-without-preservation', 'already-absent'} for row in results)
        return publish('completed' if done else 'waiting_protected_targets' if apply else 'dry_run_complete')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--policy', type=Path, required=True)
    parser.add_argument('--state', type=Path, required=True)
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    os.umask(0o077)
    try:
        result = cycle(args.policy.absolute(), args.state.absolute(), apply=args.apply)
    except BlockingIOError:
        print(json.dumps({'state': 'existing_cleanup_owner_busy', 'source_changed': False}))
        return 75
    print(json.dumps({k: v for k, v in result.items() if k not in {'results', 'current_target'}}))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
