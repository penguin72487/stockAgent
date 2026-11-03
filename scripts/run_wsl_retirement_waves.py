#!/usr/bin/env python3
"""Schedule frozen, disjoint offline cohorts through the existing retirement owner.

Default is read-only planning.  --apply only invokes the canonical owner;
it never archives, publishes or unlinks data itself.  Each wave must pass a
fresh preflight. Independent cohorts may explicitly continue past a deferred
wave; the overall run still fails until every selected cohort is accepted.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import plan_wsl_retirement_waves as planner

SCHEDULER_LOCK = Path('/run/lock/stockagent-wsl-retirement-wave-scheduler.lock')
CONTRACT = 'stockagent.wsl_retirement_wave_execution.v1'


class SchedulerBusy(Exception):
    """A different cooperating wave owns the scheduler, before any work starts."""


def _own_agent_run_id() -> str | None:
    """Identify only this process's actual agent cgroup, not an argv claim."""
    try:
        lines = Path('/proc/self/cgroup').read_text().splitlines()
    except OSError:
        return None
    matches = {match.group(1) for line in lines for match in
               re.finditer(r'/stockagent-agent-([A-Za-z0-9_-]+)\.service(?:/|$)', line)}
    return next(iter(matches)) if len(matches) == 1 else None


def _inspect(pairs: list[tuple[Path, Path]], parallelism: int, *, fixed_d_stage_only: bool = False,
             d_stage_recovery_only: bool = False, use_backup_reserve: bool = False) -> dict:
    if type(use_backup_reserve) is not bool or (use_backup_reserve and not (
            fixed_d_stage_only or d_stage_recovery_only)):
        raise ValueError('backup reserve use requires a guarded-D recovery mode')
    active = planner.observe_active_supervisors()
    own = _own_agent_run_id()
    if active is not None and own is not None:
        active = [run for run in active if run['run_id'] != own]
    options = {'fixed_d_stage_only': True} if fixed_d_stage_only else {}
    if d_stage_recovery_only:
        options['d_stage_recovery_only'] = True
    cohorts = [planner.inspect_cohort(inventory, receipt, active, **options) for inventory, receipt in pairs]
    if use_backup_reserve:
        for cohort in cohorts:
            cohort['canonical_argv'].append('--use-backup-reserve')
    return planner.build_plan(cohorts, parallelism, planner.capacity_observation(),
                              **({'use_backup_reserve': True} if use_backup_reserve else {}))


@contextmanager
def _scheduler_lock(*, shared: bool = False):
    # This only serializes cooperating wave schedulers. The original owner
    # retains every publication, recovery, consumer and deletion lock.
    fd = os.open(SCHEDULER_LOCK, os.O_RDWR | os.O_CREAT | os.O_CLOEXEC | os.O_NOFOLLOW, 0o600)
    try:
        st = os.fstat(fd)
        if not stat.S_ISREG(st.st_mode) or st.st_uid != os.geteuid():
            raise ValueError('wave scheduler lock is not a local owned regular file')
        try:
            fcntl.flock(fd, (fcntl.LOCK_SH if shared else fcntl.LOCK_EX) | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise SchedulerBusy('another wave owns the scheduler') from error
        yield
    finally:
        os.close(fd)


def _scheduler_read_lock():
    """Share only an already-accepted, read-only proof check with a census."""
    return _scheduler_lock(shared=True)


def _accepted(cohort: dict, *, expected_manifest_sha256: str | None = None) -> dict:
    """Check the original owner's durable completion, not just child exit 0."""
    if expected_manifest_sha256 is not None and (not isinstance(expected_manifest_sha256, str)
            or not re.fullmatch(r'[0-9a-f]{64}', expected_manifest_sha256)):
        raise ValueError('invalid enrolled cold manifest')
    inventory = Path(cohort['inventory'])
    if hashlib.sha256(inventory.read_bytes()).hexdigest() != cohort['inventory_sha256']:
        raise ValueError('frozen inventory changed while the owner was running')
    receipt = Path(cohort['receipt_directory'])
    progress_file = receipt / 'progress.json'
    if progress_file.is_symlink():
        raise ValueError('owner progress is redirected')
    progress = json.loads(progress_file.read_bytes())
    if (progress.get('contract') != planner.owner.MANUAL_WSL_CAPTURE_CONTRACT
            or progress.get('inventory_sha256') != cohort['inventory_sha256']
            or progress.get('all_selected_retired') is not True):
        raise ValueError('owner progress does not prove this frozen cohort retired')
    sources = {source['relative_root']: source['path'] for source in cohort['sources']}
    items = progress.get('items')
    if (not isinstance(items, list) or len(items) != len(sources)
            or len(sources) != len(cohort['sources'])):
        raise ValueError('owner result count differs from the fixed cohort')
    seen, reclaimed = set(), 0
    for item in items:
        if not isinstance(item, dict):
            raise ValueError('invalid owner item')
        relative, dataset = item.get('relative_root'), item.get('dataset')
        if (relative not in sources or relative in seen or item.get('retired') is not True
                or not isinstance(dataset, str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]*', dataset)
                or not isinstance(item.get('snapshot_id'), str) or not item['snapshot_id']
                or not isinstance(item.get('manifest_sha256'), str)
                or not re.fullmatch(r'[0-9a-f]{64}', item['manifest_sha256'])
                or type(item.get('reclaimed_allocated_bytes')) is not int
                or item['reclaimed_allocated_bytes'] < 0):
            raise ValueError('owner item lacks an exact accepted cold retirement')
        if expected_manifest_sha256 is not None and item['manifest_sha256'] != expected_manifest_sha256:
            raise ValueError('owner accepted another enrolled cold manifest')
        acceptance = receipt / dataset / 'acceptance.json'
        if acceptance.parent.is_symlink() or acceptance.is_symlink():
            raise ValueError('owner acceptance is redirected')
        accepted = json.loads(acceptance.read_bytes())
        for key in ('dataset', 'relative_root', 'retired', 'snapshot_id',
                    'manifest_sha256', 'reclaimed_allocated_bytes'):
            if accepted.get(key) != item.get(key):
                raise ValueError('owner progress and durable item acceptance differ')
        if os.path.lexists(sources[relative]):
            raise ValueError('retired source is present or has reappeared')
        seen.add(relative)
        reclaimed += item['reclaimed_allocated_bytes']
    if seen != set(sources) or progress.get('reclaimed_allocated_bytes') != reclaimed:
        raise ValueError('owner reclaimed-byte or source coverage does not reconcile')
    proof = {'inventory_sha256': cohort['inventory_sha256'], 'items': len(items),
             'reclaimed_allocated_bytes': reclaimed, 'receipt_directory': str(receipt)}
    if expected_manifest_sha256 is not None:
        proof['manifest_sha256'] = expected_manifest_sha256
    return proof


def _run_one(cohort: dict) -> dict:
    command = [*cohort['canonical_argv'], '--expected-inventory-sha256', cohort['inventory_sha256']]
    try:
        result = subprocess.run(command, cwd=ROOT, stdin=subprocess.DEVNULL, check=False)
        if result.returncode != 0:
            return {'state': 'owner_incomplete', 'returncode': result.returncode,
                    'inventory': cohort['inventory']}
        return {'state': 'accepted', **_accepted(cohort)}
    except (OSError, ValueError, json.JSONDecodeError) as error:
        return {'state': 'not_accepted', 'inventory': cohort['inventory'],
                'error': f'{type(error).__name__}: {error}'}


def _emit(event: str, **fields) -> None:
    print(json.dumps({'contract': CONTRACT, 'event': event, **fields}, ensure_ascii=False), flush=True)


def _initial_admission_blocked(plan: dict, continue_independent_waves: bool) -> bool:
    # A cohort-local deferral cannot authorize that cohort, but does not make
    # a disjoint source unsafe. Lost global owner visibility or overlaps do.
    if plan['overlap_conflicts'] or any(
            'active_agent_supervisors_unobservable' in c['preflight_deferrals'] for c in plan['cohorts']):
        return True
    return (not continue_independent_waves
            and (any(c['preflight_deferrals'] for c in plan['cohorts']) or not plan['waves']))


def wait_for_capacity(pairs: list[tuple[Path, Path]], parallelism: int, *, poll_seconds: int = 60,
                      fixed_d_stage_only: bool = False, handoff_identity: list | None = None,
                      expected_identity: list | None = None, d_stage_recovery_only: bool = False,
                      continue_independent_waves: bool = False, use_backup_reserve: bool = False) -> int:
    """Wait without holding the scheduler or any retirement owner lock.

    Capacity is re-observed, not reserved. The actual executor must still
    acquire the scheduler lock and repeat every preflight immediately before
    starting an owner. A changed inventory or non-capacity blocker is not an
    invitation to retry an unknown transaction.
    """
    if type(poll_seconds) is not int or not 1 <= poll_seconds <= 60:
        raise ValueError('capacity poll interval must be between 1 and 60 seconds')
    expected = (None if expected_identity is None else
                ([item[0] for item in expected_identity], [item[1] for item in expected_identity]))
    while True:
        options = {'fixed_d_stage_only': True} if fixed_d_stage_only else {}
        if d_stage_recovery_only:
            options['d_stage_recovery_only'] = True
        if use_backup_reserve:
            options['use_backup_reserve'] = True
        current = _inspect(pairs, parallelism, **options)
        digests = [cohort['inventory_sha256'] for cohort in current['cohorts']]
        publications = [cohort.get('fixed_d_publication') for cohort in current['cohorts']]
        identity = (digests, publications)
        if expected is None:
            expected = identity
        if (_initial_admission_blocked(current, continue_independent_waves) or identity != expected):
            _emit('deferred', reason='waiting_preflight_or_inventory_changed')
            return 75
        blocked = sorted({reason for wave in current['waves'] for reason in wave['capacity_deferrals']})
        if not blocked:
            if handoff_identity is not None:
                handoff_identity[:] = list(zip(digests, publications))
            _emit('capacity_available', inventory_sha256=digests, fixed_publications=publications)
            return 0
        _emit('waiting_for_capacity', blocked_volumes=blocked,
              observed_free_bytes=current.get('observed_free_bytes'),
              required_bytes_heuristic=[wave.get('required_bytes_heuristic') for wave in current['waves']],
              poll_seconds=poll_seconds)
        time.sleep(poll_seconds)


def _predecessor_unit_state(unit: str) -> str:
    result = subprocess.run(['systemctl', 'show', unit, '--property=ActiveState', '--value'],
                            capture_output=True, text=True, check=True, timeout=10)
    state = result.stdout.strip()
    if state not in {'active', 'activating', 'deactivating', 'reloading', 'inactive', 'failed'}:
        raise ValueError('predecessor service state is unobservable')
    return state


def wait_for_predecessor(pair: tuple[Path, Path], run_id: str, *, poll_seconds: int = 60) -> tuple[str, list]:
    """Wait for one exact existing owner; only retry a known failed audit once.

    A finished service alone is never a completion proof. Unknown failures,
    changed inventories, replacement owners and changed cold releases defer.
    """
    if not re.fullmatch(r'[A-Za-z0-9_-]+', run_id) or not 1 <= poll_seconds <= 60:
        raise ValueError('invalid predecessor run or polling interval')
    inventory, receipt = pair
    run_path = ROOT / 'artifacts/operations/agent-workflow/runs' / run_id / 'run.json'
    if run_path.is_symlink() or not run_path.is_file():
        raise ValueError('predecessor receipt unavailable')
    first = json.loads(run_path.read_bytes())
    unit = f'stockagent-agent-{run_id}.service'
    argv = first.get('argv')
    if (first.get('run_id') != run_id or first.get('repo_root') != str(ROOT)
            or first.get('host') != 'penguin' or first.get('unit') != unit
            or not isinstance(argv, list) or len(argv) != 9
            or argv[:5] != ['/bin/bash', 'scripts/run_authority_storage_operation.sh',
                            'scripts/retire_local_offline_artifacts.py', 'apply', '--inventory']
            or (ROOT / argv[5]).absolute() != inventory.absolute()
            or argv[6] != '--receipt-dir' or (ROOT / argv[7]).absolute() != receipt.absolute()
            or argv[8] != '--wait-for-owner'):
        raise ValueError('predecessor is not the exact known frozen owner')
    pinned = planner.inspect_cohort(inventory, receipt, [], fixed_d_stage_only=True)
    publication = pinned.get('fixed_d_publication')
    if not publication or len(pinned.get('sources', [])) != 1:
        raise ValueError('predecessor fixed D release is unavailable')
    identity = [(pinned['inventory_sha256'], publication)]
    immutable_keys = ('run_id', 'repo_root', 'host', 'unit', 'argv', 'queued_at_utc', 'process', 'child_process')
    while True:
        current = json.loads(run_path.read_bytes())
        if any(current.get(key) != first.get(key) for key in immutable_keys):
            raise ValueError('predecessor process or run identity changed')
        state = _predecessor_unit_state(unit)
        if state in {'active', 'activating', 'deactivating', 'reloading'}:
            _emit('waiting_for_predecessor', run_id=run_id, service_state=state, poll_seconds=poll_seconds)
            time.sleep(poll_seconds)
            continue
        if current.get('state') == 'succeeded' and current.get('exit_code') == 0:
            _emit('predecessor_accepted', run_id=run_id, result=_accepted(pinned))
            return 'accepted', identity
        if current.get('state') != 'failed' or current.get('exit_code') != 75:
            raise ValueError('predecessor did not finish with a recognized recoverable failure')
        progress = json.loads((receipt / 'progress.json').read_bytes())
        started_at = datetime.fromisoformat(first['started_at_utc'])
        if started_at.tzinfo is None:
            raise ValueError('predecessor start time has no timezone')
        started = started_at.timestamp()
        items = progress.get('items')
        if (progress.get('contract') != planner.owner.MANUAL_WSL_CAPTURE_CONTRACT
                or progress.get('inventory_sha256') != identity[0][0]
                or progress.get('all_selected_retired') is not False
                or type(progress.get('checked_at_epoch')) not in (int, float)
                or progress['checked_at_epoch'] < started
                or not isinstance(items, list) or len(items) != 1 or not isinstance(items[0], dict)
                or items[0].get('relative_root') != pinned['sources'][0]['relative_root']
                or items[0].get('retired') is not False
                or items[0].get('error') != 'owned legacy full verification expired or its generation changed'):
            raise ValueError('predecessor failure is not the current exact audit-expiry/generation outcome')
        again = planner.inspect_cohort(inventory, receipt, [], fixed_d_stage_only=True)
        if ([(again['inventory_sha256'], again.get('fixed_d_publication'))] != identity
                or not all(source['present'] for source in again['sources'])):
            raise ValueError('fixed inventory, source or D release changed during predecessor')
        _emit('predecessor_known_failure_retry', run_id=run_id, inventory_sha256=identity[0][0])
        return 'retry', identity


def execute(pairs: list[tuple[Path, Path]], parallelism: int, *, fixed_d_stage_only: bool = False,
            expected_handoff_identity: list | None = None, d_stage_recovery_only: bool = False,
            continue_independent_waves: bool = False, use_backup_reserve: bool = False) -> int:
    if os.readlink('/proc/self/ns/mnt') != os.readlink('/proc/1/ns/mnt'):
        raise ValueError('wave scheduler must observe the authority mount namespace')
    with _scheduler_lock():
        options = {'fixed_d_stage_only': True} if fixed_d_stage_only else {}
        if d_stage_recovery_only:
            options['d_stage_recovery_only'] = True
        if use_backup_reserve:
            options['use_backup_reserve'] = True
        initial = _inspect(pairs, parallelism, **options)
        _emit('initial_plan', plan=initial)
        if _initial_admission_blocked(initial, continue_independent_waves):
            _emit('deferred', reason='cohort_preflight_or_overlap')
            return 75
        expected = [(cohort['inventory_sha256'], cohort.get('fixed_d_publication'))
                    for cohort in initial['cohorts']]
        if expected_handoff_identity is not None and expected != expected_handoff_identity:
            _emit('deferred', reason='capacity_wait_identity_changed_before_execution')
            return 75
        deferred_waves, deferred_cohorts, already_accepted = [], [], []
        for index, cohort in enumerate(initial['cohorts']):
            if not cohort['preflight_deferrals']:
                continue
            accepted = False
            if (cohort.get('historical_progress') == 'retired_and_source_absent'
                    and cohort['preflight_deferrals'] == ['one_or_more_sources_absent_or_already_retired']):
                try:
                    result = _accepted(cohort)
                    _emit('cohort_already_accepted', cohort=index, result=result)
                    already_accepted.append(cohort)
                    accepted = True
                except (OSError, ValueError, json.JSONDecodeError) as error:
                    _emit('prior_acceptance_rejected', cohort=index, reason=str(error))
            if not accepted:
                deferred_cohorts.append(index)
                _emit('cohort_deferred', cohort=index, reasons=cohort['preflight_deferrals'])
        for wave_index, wave in enumerate(initial['waves']):
            indexes = wave['cohorts']
            current = _inspect([pairs[i] for i in indexes], len(indexes), **options)
            _emit('wave_preflight', wave=wave_index, plan=current)
            if (current['overlap_conflicts'] or any(c['preflight_deferrals'] for c in current['cohorts'])
                    or len(current['waves']) != 1 or current['waves'][0]['capacity_deferrals']
                    or any((c['inventory_sha256'], c.get('fixed_d_publication')) != expected[i]
                           for c, i in zip(current['cohorts'], indexes))):
                _emit('deferred', wave=wave_index, reason='fresh_preflight_or_capacity')
                if not continue_independent_waves:
                    return 75
                deferred_waves.append(wave_index)
                continue
            _emit('wave_start', wave=wave_index, cohorts=indexes)
            with ThreadPoolExecutor(max_workers=len(indexes)) as executor:
                results = list(executor.map(_run_one, current['cohorts']))
            _emit('wave_result', wave=wave_index, results=results)
            if any(result['state'] != 'accepted' for result in results):
                _emit('deferred', wave=wave_index, reason='owner_or_acceptance_incomplete')
                if not continue_independent_waves:
                    return 75
                deferred_waves.append(wave_index)
        for cohort in already_accepted:
            try:
                _accepted(cohort)
            except (OSError, ValueError, json.JSONDecodeError) as error:
                deferred_cohorts.append(initial['cohorts'].index(cohort))
                _emit('prior_acceptance_rejected', inventory=cohort['inventory'], reason=str(error))
        if deferred_waves or deferred_cohorts:
            _emit('incomplete', waves=len(initial['waves']), deferred_waves=deferred_waves,
                  deferred_cohorts=deferred_cohorts,
                  reason='independent_waves_finished_with_unaccepted_cohorts')
            return 75
        _emit('complete', waves=len(initial['waves']))
        return 0


def wait_and_execute(pairs: list[tuple[Path, Path]], parallelism: int, *,
                     fixed_d_stage_only: bool = False, expected_identity: list | None = None,
                     wait_for_scheduler: bool = False, poll_seconds: int = 60,
                     d_stage_recovery_only: bool = False,
                     continue_independent_waves: bool = False, use_backup_reserve: bool = False) -> int:
    """Queue behind the scheduler without holding a lock or trusting stale capacity.

    Only scheduler contention is retried. Unknown owner, source, publication,
    inventory or capacity outcomes still follow the existing fail-closed rules.
    """
    if type(poll_seconds) is not int or not 1 <= poll_seconds <= 60:
        raise ValueError('scheduler poll interval must be between 1 and 60 seconds')
    options = {'fixed_d_stage_only': True} if fixed_d_stage_only else {}
    if d_stage_recovery_only:
        options['d_stage_recovery_only'] = True
    if use_backup_reserve:
        options['use_backup_reserve'] = True
    pinned = expected_identity
    while True:
        if wait_for_scheduler:
            try:
                # Observe only; never hold this lock while waiting for capacity
                # or another owner. A final execution still acquires it again.
                with _scheduler_lock():
                    pass
            except SchedulerBusy:
                if pinned is None:
                    queued = _inspect(pairs, parallelism, **options)
                    pinned = [(c['inventory_sha256'], c.get('fixed_d_publication'))
                              for c in queued['cohorts']]
                    if fixed_d_stage_only and any(publication is None for _, publication in pinned):
                        _emit('deferred', reason='queued_fixed_publication_unavailable')
                        return 75
                _emit('waiting_for_scheduler', poll_seconds=poll_seconds)
                time.sleep(poll_seconds)
                continue
        handoff: list = []
        waiting_options = dict(options)
        if continue_independent_waves:
            waiting_options['continue_independent_waves'] = True
        if wait_for_capacity(pairs, parallelism, handoff_identity=handoff,
                             expected_identity=pinned, **waiting_options) != 0:
            return 75
        if pinned is None:
            pinned = handoff
        try:
            execution_options = dict(options)
            if continue_independent_waves:
                execution_options['continue_independent_waves'] = True
            return execute(pairs, parallelism, expected_handoff_identity=pinned, **execution_options)
        except SchedulerBusy:
            if not wait_for_scheduler:
                raise
            _emit('waiting_for_scheduler', poll_seconds=poll_seconds)
            time.sleep(poll_seconds)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cohort', action='append', nargs=2,
                        metavar=('INVENTORY', 'RECEIPT'))
    parser.add_argument('--queue-config', type=Path,
                        help='Resume the explicitly enrolled persistent queue (no candidate discovery)')
    parser.add_argument('--parallelism', type=int, default=1, choices=range(1, 9))
    parser.add_argument('--apply', action='store_true', help='Invoke existing owner after fresh safety gates')
    parser.add_argument('--wait-for-capacity', action='store_true',
                        help='Wait for observed capacity before the unchanged execution preflight')
    parser.add_argument('--wait-for-scheduler', action='store_true',
                        help='Queue behind another wave and recheck capacity and identity before launch')
    parser.add_argument('--fixed-d-stage-only', action='store_true',
                        help='Use only an existing pinned D stage; defer instead of recreating source-sized workspaces')
    parser.add_argument('--d-stage-recovery-only', action='store_true',
                        help='Prepare on guarded D and require its encoding throughout recovery and retirement')
    parser.add_argument('--use-backup-reserve', action='store_true',
                        help='Use the pool earmarked for this guarded-D backup/recovery wave')
    parser.add_argument('--wait-for-prior-run',
                        help='After this exact supervised owner exits, accept it or retry one known audit failure')
    parser.add_argument('--continue-independent-waves', action='store_true',
                        help='Continue disjoint waves after a deferral; retain failures and exit incomplete')
    args = parser.parse_args()
    if args.queue_config:
        if (args.cohort or args.wait_for_capacity or args.wait_for_scheduler or args.wait_for_prior_run
                or args.fixed_d_stage_only or args.d_stage_recovery_only or args.continue_independent_waves
                or args.parallelism != 1 or args.use_backup_reserve):
            parser.error('--queue-config uses only its enrolled policy and --apply')
        from stockagent.data_sync.retirement_queue import cycle
        result = cycle(args.queue_config.absolute(), root=ROOT, runner=sys.modules[__name__], apply=args.apply)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    if not args.cohort:
        parser.error('provide --cohort or --queue-config')
    if args.fixed_d_stage_only and args.d_stage_recovery_only:
        parser.error('choose exactly one D resource mode')
    if args.use_backup_reserve and not (args.fixed_d_stage_only or args.d_stage_recovery_only):
        parser.error('--use-backup-reserve requires a guarded-D recovery mode')
    pairs = [(Path(inventory).absolute(), Path(receipt).absolute()) for inventory, receipt in args.cohort]
    if not args.apply:
        if (args.wait_for_capacity or args.wait_for_prior_run or args.wait_for_scheduler
                or args.continue_independent_waves):
            parser.error('waiting requires --apply')
        options = {'fixed_d_stage_only': True} if args.fixed_d_stage_only else {}
        if args.d_stage_recovery_only:
            options['d_stage_recovery_only'] = True
        if args.use_backup_reserve:
            options['use_backup_reserve'] = True
        print(json.dumps(_inspect(pairs, args.parallelism, **options), ensure_ascii=False, indent=2))
        return 0
    try:
        options = {'fixed_d_stage_only': True} if args.fixed_d_stage_only else {}
        if args.d_stage_recovery_only:
            options['d_stage_recovery_only'] = True
        if args.use_backup_reserve:
            options['use_backup_reserve'] = True
        if args.continue_independent_waves:
            options['continue_independent_waves'] = True
        predecessor_identity = None
        if args.wait_for_prior_run:
            if not args.wait_for_capacity or not args.fixed_d_stage_only or len(pairs) != 1:
                parser.error('--wait-for-prior-run requires one fixed-D cohort and capacity waiting')
            outcome, predecessor_identity = wait_for_predecessor(pairs[0], args.wait_for_prior_run)
            if outcome == 'accepted':
                return 0
        if args.wait_for_capacity:
            if os.readlink('/proc/self/ns/mnt') != os.readlink('/proc/1/ns/mnt'):
                raise ValueError('wave scheduler must observe the authority mount namespace')
            return wait_and_execute(pairs, args.parallelism, expected_identity=predecessor_identity,
                                    wait_for_scheduler=args.wait_for_scheduler, **options)
        if args.wait_for_scheduler:
            parser.error('--wait-for-scheduler requires --wait-for-capacity')
        return execute(pairs, args.parallelism, **options)
    except (SchedulerBusy, OSError, ValueError, KeyError, TypeError,
            subprocess.SubprocessError, json.JSONDecodeError) as error:
        _emit('deferred', reason=f'{type(error).__name__}: {error}')
        return 75


if __name__ == '__main__':
    raise SystemExit(main())
