"""Persistent dispatch for approved frozen WSL cohorts, using the original owner.

This queue never discovers deletion candidates, archives bytes or unlinks files.
The wave runner retains capacity, overlap, recovery and retirement admission.
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import time
import uuid

from stockagent.data_sync.desync_snapshots import atomic_write_bytes, write_immutable_json

CONTRACT = 'stockagent.frozen_retirement_queue.v1'
HASH = re.compile(r'[0-9a-f]{64}')
MODES = {'fixed_d_stage_only', 'd_stage_recovery_only'}


def _execution_generation() -> str:
    """A repaired owner can resume exhausted work without editing private state."""
    root = Path(__file__).resolve().parents[2]
    digest = hashlib.sha256()
    for relative in ('scripts/run_wsl_retirement_waves.py', 'scripts/retire_local_offline_artifacts.py',
                     'scripts/plan_wsl_retirement_waves.py', 'stockagent/data_sync/retirement_queue.py',
                     'stockagent/data_sync/legacy_artifact_retirement.py',
                     'stockagent/data_sync/legacy_artifact_archive.py',
                     'stockagent/data_sync/artifact_maintenance.py',
                     'stockagent/data_sync/packed_backup.py', 'stockagent/data_sync/packed_snapshots.py'):
        digest.update(relative.encode() + b'\0' + hashlib.sha256((root / relative).read_bytes()).digest())
    return digest.hexdigest()


def _regular_json(path: Path) -> dict:
    if path.resolve(strict=False) != path.absolute() or path.is_symlink():
        raise ValueError('queue control path is redirected')
    info = path.stat()
    if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid():
        raise ValueError('queue control is not an owned regular file')
    data = json.loads(path.read_bytes())
    if not isinstance(data, dict):
        raise ValueError('queue control must be an object')
    return data


def _local_path(root: Path, value: str, scope: str) -> Path:
    if not isinstance(value, str) or not value:
        raise ValueError('queue path is missing')
    path = root / value
    base = root / scope
    if (Path(value).is_absolute() or '..' in Path(value).parts or path == base
            or not path.is_relative_to(base) or path.resolve(strict=False) != path):
        raise ValueError('queue path is outside its local scope')
    return path


def load_configuration(path: Path, root: Path) -> dict:
    data = _regular_json(path)
    if (data.get('contract') != CONTRACT or data.get('schema_version') != 1
            or data.get('authority_node_id') != 'penguin' or data.get('enabled') is not True
            or not isinstance(data.get('user_instruction'), str) or not data['user_instruction'].strip()):
        raise ValueError('automatic retirement lacks its explicit enrolled policy')
    reserve_policy = data.get('capacity_reserve_policy', 'retain_reserve')
    if reserve_policy not in ('retain_reserve', 'use_backup_reserve'):
        raise ValueError('invalid queue capacity reserve policy')
    if reserve_policy == 'use_backup_reserve' and (
            not isinstance(data.get('backup_reserve_instruction'), str)
            or not data['backup_reserve_instruction'].strip()):
        raise ValueError('backup reserve use lacks its explicit purpose instruction')
    for name, low, high in [('parallelism', 1, 8), ('maximum_error_attempts', 1, 10)]:
        if type(data.get(name)) is not int or not low <= data[name] <= high:
            raise ValueError('invalid queue ' + name)
    delays = data.get('retry_delays_seconds')
    if (not isinstance(delays, list) or not delays
            or any(type(v) is not int or not 30 <= v <= 86400 for v in delays)):
        raise ValueError('queue retries must be bounded and nonzero')
    rows = data.get('cohorts')
    if not isinstance(rows, list) or not rows or len(rows) > 128:
        raise ValueError('queue requires a bounded explicit cohort list')
    ids, paths, receipts = set(), set(), set()
    for row in rows:
        if (not isinstance(row, dict) or not isinstance(row.get('id'), str)
                or not re.fullmatch(r'[a-z0-9][a-z0-9_-]{0,79}', row['id'])
                or row.get('resource_mode') not in MODES
                or not isinstance(row.get('inventory_sha256'), str)
                or not HASH.fullmatch(row['inventory_sha256'])):
            raise ValueError('invalid frozen cohort enrollment')
        manifest = row.get('manifest_sha256')
        if manifest is not None and (not isinstance(manifest, str) or not HASH.fullmatch(manifest)):
            raise ValueError('invalid enrolled cold manifest')
        inventory = _local_path(root, row.get('inventory'), 'artifacts/operations')
        receipt = _local_path(root, row.get('receipt_directory'), 'artifacts/operations')
        if row['id'] in ids or inventory in paths or any(
                receipt == other or receipt in other.parents or other in receipt.parents for other in receipts):
            raise ValueError('queue cohort identities/receipts overlap')
        ids.add(row['id']); paths.add(inventory); receipts.add(receipt)
    _local_path(root, data.get('state_root'), 'service_data')
    return data


@contextmanager
def _controller(state_root: Path):
    state_root.mkdir(parents=True, exist_ok=True, mode=0o700)
    if state_root.resolve() != state_root or state_root.stat().st_uid != os.geteuid():
        raise ValueError('queue state root is redirected or unowned')
    fd = os.open(state_root / 'controller.lock', os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid():
            raise ValueError('queue controller lock is invalid')
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield
    finally:
        os.close(fd)


def _load_state(path: Path) -> dict:
    if path.is_symlink():
        raise ValueError('preserve redirected queue state')
    if not path.exists():
        return {'contract': CONTRACT, 'cohorts': {}}
    state = _regular_json(path)
    if (state.get('contract') != CONTRACT or not isinstance(state.get('cohorts'), dict)
            or any(not isinstance(row, dict) for row in state['cohorts'].values())):
        raise ValueError('preserve invalid previous queue state')
    return state


def cycle(configuration: Path, *, root: Path, runner, apply: bool = False) -> dict:
    """A timer turn; busy/capacity waits do not consume failure retries."""
    config = load_configuration(configuration, root)
    state_root = _local_path(root, config['state_root'], 'service_data')
    if not apply:
        return _cycle(config, root, runner, state_root, apply=False)
    try:
        with _controller(state_root):
            return _cycle(config, root, runner, state_root, apply=True)
    except BlockingIOError:
        return {'contract': CONTRACT, 'state': 'waiting_existing_queue_controller',
                'all_enrolled_accepted': False}


def _cycle(config: dict, root: Path, runner, state_root: Path, *, apply: bool) -> dict:
    path = state_root / 'status.json'
    state = _load_state(path)
    rows = config['cohorts']
    verified_now = set()
    state.update(state='inspecting', all_enrolled_accepted=False,
                 enrolled_cohort_ids=[row['id'] for row in rows])
    reserve_options = ({'use_backup_reserve': True}
                       if config.get('capacity_reserve_policy') == 'use_backup_reserve' else {})
    state['capacity_reserve_policy'] = config.get('capacity_reserve_policy', 'retain_reserve')
    for row in rows:
        enrollment = hashlib.sha256(json.dumps(row, sort_keys=True).encode()).hexdigest()
        prior = state['cohorts'].get(row['id'], {})
        if prior.get('enrollment_sha256') != enrollment:
            state['cohorts'][row['id']] = {'enrollment_sha256': enrollment,
                'inventory_sha256': row['inventory_sha256'], 'inventory': row['inventory'],
                'receipt_directory': row['receipt_directory'], 'resource_mode': row['resource_mode'],
                'state': 'enrolled', 'error_attempts': 0}

    def save():
        state['observed_at_utc'] = datetime.now(timezone.utc).isoformat()
        state['all_enrolled_accepted'] = all(
            row['id'] in verified_now and state['cohorts'][row['id']]['state'] == 'accepted' for row in rows)
        if state['all_enrolled_accepted'] and not state['state'].startswith('blocked_'):
            state['state'] = 'all_enrolled_accepted'
        if apply:
            atomic_write_bytes(path, (json.dumps(state, sort_keys=True, indent=2) + '\n').encode(), mode=0o600)
        return state

    # A completed queue only rechecks fixed receipts and source absence. Share
    # the census's read lease, while still excluding a real exclusive wave;
    # otherwise the minute timer and the large read-only census can repeatedly
    # preempt one another even though no retirement is runnable.
    previous_accepted = all(
        state['cohorts'][row['id']].get('state') == 'accepted'
        and isinstance(state['cohorts'][row['id']].get('acceptance'), dict)
        and state['cohorts'][row['id']]['acceptance']
        for row in rows)
    if previous_accepted:
        try:
            observed_sources, proofs = [], {}
            with runner._scheduler_read_lock():
                for row in rows:
                    inventory, receipt = root/row['inventory'], root/row['receipt_directory']
                    _regular_json(inventory)
                    if hashlib.sha256(inventory.read_bytes()).hexdigest() != row['inventory_sha256']:
                        raise ValueError('enrolled inventory SHA-256 changed')
                    plan = runner._inspect([(inventory, receipt)], 1,
                        **{row['resource_mode']: True}, **reserve_options)
                    cohort = plan['cohorts'][0]
                    if cohort['inventory_sha256'] != row['inventory_sha256']:
                        raise ValueError('inventory changed during read-only acceptance')
                    for source in cohort['sources']:
                        current = Path(source['resolved_path'])
                        if any(current == other or current in other.parents or other in current.parents
                               for other in observed_sources):
                            raise ValueError('overlapping accepted sources')
                        observed_sources.append(current)
                    proofs[row['id']] = runner._accepted(cohort, expected_manifest_sha256=row.get('manifest_sha256'))
            for row in rows:
                state['cohorts'][row['id']].update(state='accepted', acceptance=proofs[row['id']],
                                                    reasons=[], next_attempt_epoch=None)
                verified_now.add(row['id'])
            return save()
        except runner.SchedulerBusy:
            state['state'] = 'waiting_existing_wave'
            return save()
        except (OSError, ValueError, KeyError, TypeError, RuntimeError):
            # A changed proof/source goes through the original exclusive
            # preflight; no execution is permitted under the shared lease.
            pass

    # Avoid D metadata reads while an existing wave owns this same scheduler.
    # The executor reacquires it and repeats all preflights before allocating.
    try:
        with runner._scheduler_lock():
            pass
    except runner.SchedulerBusy:
        state['state'] = 'waiting_existing_wave'
        return save()

    eligible = {mode: [] for mode in MODES}
    generation = _execution_generation()
    all_sources = []
    overlap = False
    for row in rows:
        key = row['id']
        prior = state['cohorts'][key]
        pair = (root / row['inventory'], root / row['receipt_directory'])
        try:
            # Pin bytes before asking the original planner; it does not mutate.
            _regular_json(pair[0])
            if hashlib.sha256(pair[0].read_bytes()).hexdigest() != row['inventory_sha256']:
                raise ValueError('enrolled inventory SHA-256 changed')
            options = {row['resource_mode']: True, **reserve_options}
            plan = runner._inspect([pair], 1, **options)
            cohort = plan['cohorts'][0]
            if cohort['inventory_sha256'] != row['inventory_sha256']:
                raise ValueError('inventory changed during queue preflight')
            prior['source_summary'] = {name: cohort.get(name) for name in (
                'logical_bytes_frozen', 'allocated_unique_file_bytes_frozen', 'files_frozen')}
            for source in cohort['sources']:
                current = Path(source['resolved_path'])
                if any(current == other or current in other.parents or other in current.parents
                       for other in all_sources):
                    overlap = True
                all_sources.append(current)
            if cohort.get('historical_progress') == 'retired_and_source_absent' or prior.get('acceptance'):
                # Completed stages have been retired. Their original matching
                # item acceptance and source absence bind the enrolled release;
                # requiring its former temporary stage would block forever.
                proof = runner._accepted(cohort, expected_manifest_sha256=row.get('manifest_sha256'))
                prior.update(state='accepted', acceptance=proof, reasons=[], next_attempt_epoch=None)
                verified_now.add(key)
                continue
            publication = cohort.get('fixed_d_publication')
            if row.get('manifest_sha256') and (
                    not publication or publication['manifest_sha256'] != row['manifest_sha256']):
                raise ValueError('enrolled cold manifest differs')
            if cohort['preflight_deferrals']:
                prior.update(state='waiting_preflight', reasons=cohort['preflight_deferrals'])
                continue
            capacity = sorted({reason for wave in plan['waves'] for reason in wave['capacity_deferrals']})
            prior['capacity_plan'] = [{name: wave[name] for name in (
                'required_bytes_heuristic', 'workspace_bytes_heuristic', 'retained_reserve_bytes')
                if name in wave} for wave in plan['waves']]
            if capacity:
                prior.update(state='waiting_capacity', reasons=capacity)
                continue
            if prior.get('failed_execution_generation') not in (None, generation):
                prior.update(previous_failed_execution_generation=prior['failed_execution_generation'],
                             error_attempts=0, next_attempt_epoch=None)
            if prior.get('error_attempts', 0) >= config['maximum_error_attempts']:
                prior.update(state='needs_attention', reasons=['owner_retry_limit_reached'])
                continue
            if prior.get('next_attempt_epoch', 0) and prior['next_attempt_epoch'] > time.time():
                prior['state'] = 'waiting_retry'
                continue
            prior.update(state='ready', reasons=[])
            eligible[row['resource_mode']].append((row, pair, cohort))
        except (OSError, ValueError, KeyError, TypeError, RuntimeError) as error:
            prior.update(state='blocked_identity_or_proof', reasons=[f'{type(error).__name__}: {error}'],
                         next_attempt_epoch=None)
    if overlap:
        state.update(state='blocked_overlapping_sources', all_enrolled_accepted=False)
        return save()
    if not apply:
        state['state'] = 'planned'
        return save()
    for mode in ('fixed_d_stage_only', 'd_stage_recovery_only'):
        group = eligible[mode]
        if not group:
            continue
        pairs = [pair for _, pair, _ in group]
        identity = [(cohort['inventory_sha256'], cohort.get('fixed_d_publication')) for _, _, cohort in group]
        for row, _, _ in group:
            state['cohorts'][row['id']].update(state='running', attempt_started_at_utc=datetime.now(timezone.utc).isoformat())
        state['state'] = 'dispatching_original_owner'
        save()
        execution_error = None
        try:
            code = runner.execute(pairs, config['parallelism'], expected_handoff_identity=identity,
                                  continue_independent_waves=True, **{mode: True}, **reserve_options)
        except runner.SchedulerBusy:
            code = None
        except (OSError, ValueError, KeyError, TypeError, RuntimeError) as error:
            code, execution_error = 75, f'{type(error).__name__}: {error}'
        for row, pair, _ in group:
            prior = state['cohorts'][row['id']]
            if code is None:
                prior.update(state='waiting_existing_wave', reasons=['scheduler_busy'])
                continue
            current = None
            try:
                current = runner._inspect([pair], 1, **{mode: True}, **reserve_options)
                cohort = current['cohorts'][0]
                if cohort['inventory_sha256'] != row['inventory_sha256']:
                    raise ValueError('inventory changed during original owner execution')
                proof = runner._accepted(cohort, expected_manifest_sha256=row.get('manifest_sha256'))
                prior.update(state='accepted', acceptance=proof, reasons=[], next_attempt_epoch=None)
                verified_now.add(row['id'])
                continue
            except (OSError, ValueError, KeyError, TypeError, RuntimeError) as error:
                reason = f'{type(error).__name__}: {error}'
            if current is not None:
                if cohort['inventory_sha256'] != row['inventory_sha256']:
                    prior.update(state='blocked_identity_or_proof', reasons=[reason], next_attempt_epoch=None)
                    continue
                waits = cohort['preflight_deferrals'] + sorted({
                    reason for wave in current['waves'] for reason in wave['capacity_deferrals']})
                if waits and 'one_or_more_sources_absent_or_already_retired' not in waits:
                    prior.update(state='waiting_preflight', reasons=waits)
                    continue
            prior['error_attempts'] = prior.get('error_attempts', 0) + 1
            delays = config['retry_delays_seconds']
            prior.update(state='needs_attention' if prior['error_attempts'] >= config['maximum_error_attempts']
                         else 'waiting_retry', reasons=[execution_error or reason], last_owner_returncode=code,
                         failed_execution_generation=generation,
                         next_attempt_epoch=time.time() + delays[min(prior['error_attempts'] - 1, len(delays) - 1)])
            history = state_root / 'failed-attempts'
            history.mkdir(exist_ok=True, mode=0o700)
            if history.resolve() != history:
                raise ValueError('queue failure history is redirected')
            failure = history / (row['id'] + '-' + uuid.uuid4().hex + '.json')
            write_immutable_json(failure, {'contract': CONTRACT, 'cohort_id': row['id'],
                'observed_at_utc': datetime.now(timezone.utc).isoformat(),
                'inventory_sha256': row['inventory_sha256'], 'failure': dict(prior),
                'acceptance_claimed': False})
            prior['last_failed_attempt_receipt'] = str(failure.relative_to(root))
        save()
    state['state'] = 'waiting_remaining_cohorts'
    return save()
