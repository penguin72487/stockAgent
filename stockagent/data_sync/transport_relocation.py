"""Original sender's D-to-E placement transaction; envelope identities survive.

All writers use their existing owner locks. Only the enrolled Syncthing folder
is paused. Full copies and source generations precede alias handoff; retirement
of the old transport names rechecks E bytes and process references. No cold,
source-observation or NAS object is deleted by this operation.
"""
from contextlib import ExitStack, contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import subprocess
import time
import uuid

from downloader.artifact_io import atomic_write_json
from stockagent.data_sync import transport_placement as placement
from stockagent.data_sync.transport_namespace_handoff import exited_only
from stockagent.data_sync import workspace_relocation as files
from stockagent.data_sync import workspace_volume as volume


class SenderBoundaryBusy(BlockingIOError):
    """Retain the queued operation through the original timer's exception path."""

    def __init__(self, purpose):
        self.purpose = purpose
        super().__init__('original sender boundary busy; return to the timer for reevaluation')


@contextmanager
def sender_boundary(evidence, config, *, wait=False, recheck_seconds=60,
                    purpose='namespace_handoff'):
    """Queue at the original boundaries without retaining a partial lock set.

    A once-a-minute nonblocking attempt can repeatedly miss the short idle
    interval of a long capture. Wait cooperatively; an active publisher is
    never stopped, and observer heartbeats remain distinct from acceptance.
    Yield back to the original timer periodically so a now-qualified worker
    handoff is not starved behind this independent sender/GC boundary.
    """
    if type(recheck_seconds) not in (int, float) or not 0 < recheck_seconds <= 600:
        raise ValueError('sender boundary requires a bounded recheck interval')
    if not isinstance(purpose, str) or purpose not in {'namespace_handoff', 'old_D_transport_retirement'}:
        raise ValueError('sender boundary purpose is not enrolled')
    deadline = time.monotonic() + recheck_seconds if wait else None
    observed = None
    while True:
        stack = ExitStack()
        handles = []
        try:
            state = Path(config['state_root'])
            for path in (state/'cycle.lock', state/'owner.lock',
                         Path(config['immutable_archive_state_root'])/'source-replication-owner.lock'):
                if path.is_symlink():
                    raise ValueError('sender writer lock is redirected')
                lock = stack.enter_context(path.open('a'))
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                handles.append(lock)
        except BlockingIOError:
            stack.close()
            if not wait:
                raise
            now = time.monotonic()
            expired = now >= deadline
            if observed is None or now - observed >= 30 or expired:
                atomic_write_json(evidence/'attempt.json', {
                    'state': 'waiting_for_original_owner_or_reader', 'retryable': True,
                    'observed_at_epoch': time.time(), 'reason_code': 'original_owner',
                    'purpose': purpose})
                observed = now
            if expired:
                raise SenderBoundaryBusy(purpose)
            time.sleep(0.25)
            continue
        except BaseException:
            stack.close()
            raise
        with stack:
            yield tuple(handles)
        return


def stale_transport_namespaces(proc=Path('/proc')):
    """Count distinct live mount namespaces still exposing the old D bind.

    A consumer may inherit a mount namespace before the PID 1 cutover. File
    descriptor scans alone cannot establish that this mount has been replaced.
    Disappearing processes are normal; loss of visibility of a live process is
    retained as an incomplete observation.
    """
    seen = set()
    stale = []
    reachable = 0
    incomplete = False
    for process in proc.iterdir():
        if not process.name.isdigit():
            continue
        try:
            namespace = os.readlink(process / 'ns/mnt')
            if namespace in seen:
                continue
            mountinfo = (process / 'mountinfo').read_text()
        except OSError:
            if process.exists() and not exited_only(process) and process.exists():
                incomplete = True
            continue
        seen.add(namespace)
        for line in mountinfo.splitlines():
            try:
                before, after = line.split(' - ', 1)
                fields, source = before.split(), after.split()
                if (fields[3] in {'/' + placement.D_BACKING.name,
                                          '/' + placement.D_BACKING.name + '.relocated-to-E-v1'}
                        and source[0] == '9p' and source[1].upper().startswith('D:')):
                    stale.append(namespace)
                    try:
                        # A consumer may retain the same old root through a
                        # different bind name. mountinfo escapes spaces and
                        # other path bytes as octal sequences.
                        mounted_at = re.sub(r'\\([0-7]{3})', lambda m: chr(int(m.group(1), 8)), fields[4])
                        (process / 'root' / mounted_at.lstrip('/')).stat()
                        reachable += 1
                    except FileNotFoundError:
                        pass
                    except OSError:
                        incomplete = True
                    break
            except (ValueError, IndexError):
                incomplete = True
    return {'count': len(stale), 'reachable': reachable, 'inspection_complete': not incomplete}


def syncthing_alias_matches(destination):
    """Observe the sender's own namespace, not only the supervising process."""
    try:
        result = subprocess.run(['systemctl', 'show', 'syncthing@root.service',
                                 '--property=MainPID', '--value'],
                                capture_output=True, text=True, check=True, timeout=5)
        pid = int(result.stdout.strip())
        if pid <= 0:
            return False
        actual = Path('/proc') / str(pid) / 'root' / str(placement.CANONICAL).lstrip('/')
        return destination.samefile(actual)
    except (OSError, ValueError, subprocess.SubprocessError):
        return False


def complete_namespace_observation():
    """Reobserve a process-exit race; never turn an incomplete pass into proof."""
    for attempt in range(3):
        observed = stale_transport_namespaces()
        if observed.get('inspection_complete'):
            return observed
        if attempt < 2:
            time.sleep(0.05)
    return observed  # A persistently unreadable live namespace still blocks.


def previous_completion_reconciliation(evidence, phase, destination):
    """Recheck an old handoff without promoting a copy receipt to retirement.

    In particular, WSL can expose D through multiple 9p mount caches. A
    negative lookup on the canonical mount is not proof of physical absence.
    This observer never unlinks, changes a mount or rewrites the old handoff.
    """
    old = placement.D_BACKING
    old_names = (old, old.with_name(old.name + '.relocated-to-E-v1'),
                 Path('/mnt/d') / old.name,
                 Path('/mnt/d') / (old.name + '.relocated-to-E-v1'))
    present, errors = 0, 0
    for name in old_names:
        try:
            name.lstat()
            present += 1
        except FileNotFoundError:
            pass
        except OSError:
            errors += 1
    namespaces = complete_namespace_observation()
    consumer = syncthing_alias_matches(destination)
    if not consumer:
        reason = 'transport_consumer_alias'
    elif errors or not namespaces['inspection_complete']:
        reason = 'transport_namespace_unavailable'
    elif namespaces['count']:
        reason = 'transport_stale_mount_namespace'
    else:
        reason = 'transport_retirement_unverified'
    value = {'contract': 'stockagent.transport_relocation_reconciliation.v1',
             'state': 'waiting_verified_old_D_retirement', 'reason_code': reason,
             'observed_at_epoch': time.time(), 'syncthing_alias_verified': consumer,
             'old_D_candidate_lookups_present': present,
             'old_D_candidate_lookup_errors': errors,
             'stale_mount_namespaces': namespaces['count'],
             'reachable_stale_mount_namespaces': namespaces.get('reachable'),
             'namespace_inspection_complete': namespaces['inspection_complete'],
             'old_handoff_state': phase.get('state'), 'old_handoff_preserved': True,
             'source_observations_deleted': 0, 'cold_objects_deleted': 0,
             'nas_objects_deleted': 0}
    atomic_write_json(evidence / 'reconciliation.json', value)
    return value


def finish_worker_handoff(evidence, *, restart_active_owner=False):
    """Reuse the canonical drained rollout after placement, once per E cutover.

    The timer uses the default drained handoff. An explicitly authorized active
    restart uses the same code-only transaction, preserving policy and journals.
    """
    from scripts.manage_lakehouse import worker_code_identity
    if type(restart_active_owner) is not bool:
        raise ValueError('active restart requires an explicit boolean selection')
    path = evidence / 'worker-reload.json'
    previous = json.loads(path.read_bytes()) if path.exists() else {}
    candidate = worker_code_identity()
    if (previous.get('state') == 'accepted'
            and previous.get('loaded_code_identity_sha256') == candidate):
        return previous
    phase = json.loads((evidence / 'handoff.json').read_bytes())
    if phase.get('state') != 'completed':
        return {'state': 'waiting_for_transport_placement'}
    placement.check_transport(physical=True)
    failures = previous.get('failures', 0)
    if failures >= 3 and previous.get('candidate_code_identity_sha256') == candidate:
        return previous
    if previous.get('candidate_code_identity_sha256') != candidate:
        failures = 0
    output = evidence / ('worker-reload-' + uuid.uuid4().hex + '.json')
    atomic_write_json(path, {'contract': 'stockagent.workspace_worker_handoff.v1',
        'state': 'waiting_for_original_owner', 'failures': failures,
        'candidate_code_identity_sha256': candidate, 'observed_at_epoch': time.time(),
        'authorized_active_owner_restart': restart_active_owner,
        'publication_progress_renewed': False, 'original_active_owner_forced': False})
    command = ['bash', str(volume.ROOT / 'scripts/run_lakehouse_control.sh'), 'python',
               str(volume.ROOT / 'scripts/rollout_lakehouse_source_policy.py'),
               '--reload-code-only', '--deadline-seconds', '600',
               '--handoff-status', str(path), '--output', str(output)]
    if restart_active_owner:
        command.append('--restart-active-owner')
    # The canonical rollout bounds the owner wait and owns stop/start recovery.
    # Do not externally kill it between stopping and re-admitting its worker.
    result = subprocess.run(command, cwd=volume.ROOT, capture_output=True)
    if result.returncode == 0 and output.exists():
        accepted = json.loads(output.read_bytes())
        if (accepted.get('state') != 'accepted'
                or accepted.get('loaded_code_identity_sha256') != candidate
                or bool(accepted.get('authorized_active_owner_restart', False)) != restart_active_owner
                or not accepted.get('workflow_chains_preserved')
                or not accepted.get('postgres_and_temporal_server_not_restarted')
                or not accepted.get('publication_owner_boundary_verified')):
            raise ValueError('canonical worker rollout did not prove the exact E cutover')
        accepted['failures'] = failures
        atomic_write_json(path, accepted)
        return accepted
    # Preserve the exact failure privately; only a bounded reason is published.
    (evidence / (output.stem + '.log')).write_bytes(result.stdout + b'\n' + result.stderr)
    busy = b'PublicationOwnerBusy: publication owner has not reached its handoff boundary' in result.stderr
    value = {'contract': 'stockagent.workspace_worker_handoff.v1',
             'state': 'waiting_for_original_owner' if busy else 'worker_reload_failed',
             'failures': failures if busy else failures + 1,
             'candidate_code_identity_sha256': candidate,
             'observed_at_epoch': time.time(), 'original_active_owner_forced': False}
    atomic_write_json(path, value)
    return value


def _namespace_handoff_plans_complete(evidence):
    boot = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
    plans = evidence / 'namespace-handoff' / boot
    if plans.resolve() != plans:
        raise ValueError('namespace handoff evidence is redirected')
    return all(not p.is_symlink() and json.loads(p.read_bytes()).get('phase') == 'alias_verified'
               for p in plans.glob('*.json'))


def advance(evidence):
    """Do not put an already-admitted worker reload behind transport GC locks.

    The original rollout owns its separate complete publication boundary. Old
    D names can remain held while that worker adopts E scratch placement. No
    sender alias is changed here, and every namespace intent must be complete.
    """
    phase_path = evidence / 'handoff.json'
    phase = json.loads(phase_path.read_bytes()) if phase_path.exists() else {}
    rollout = None
    if phase.get('state') == 'completed':
        placement.check_transport(physical=True)
        observed = complete_namespace_observation()
        complete = _namespace_handoff_plans_complete(evidence)
        if (complete and observed.get('inspection_complete') and observed.get('count') == 0
                and observed.get('reachable') == 0
                and syncthing_alias_matches(volume.path('transport') / 'lab203')):
            rollout = finish_worker_handoff(evidence)
            if rollout.get('state') == 'waiting_for_original_owner':
                return {'state': 'waiting_worker_handoff', 'worker_rollout': rollout,
                        'source_observations_deleted': 0, 'cold_objects_deleted': 0, 'nas_objects_deleted': 0}
    result = run(evidence)
    if (result.get('state') == 'completed' or result.get('syncthing_alias_verified') is True
            and result.get('namespace_inspection_complete') is True and result.get('stale_mount_namespaces') == 0):
        result = {**result, 'worker_rollout': rollout or finish_worker_handoff(evidence)}
    return result


def run(evidence):
    from scripts.configure_artifact_ingress_syncthing import credentials, request
    from stockagent.data_sync.backup_transport_cache import process_roots
    from stockagent.data_sync.materialized_cache import process_references_many
    config_path = placement.CONFIG.parent / 'backup_stream.json'
    c = json.loads(config_path.read_bytes())
    source = placement.D_BACKING
    destination = volume.path('transport', create=True) / 'lab203'
    state = Path(c['state_root'])
    evidence.mkdir(mode=0o700, parents=True, exist_ok=True)
    phase_path = evidence / 'handoff.json'
    phase = json.loads(phase_path.read_bytes()) if phase_path.exists() else {'state': 'preflight'}
    if phase.get('state') in {'completed', 'retirement_unverified'}:
        placement.check_transport(physical=True)
        if phase.get('state') == 'completed' and all(
                phase.get(k) is True for k in ('old_D_copy_retirement_verified',
                                               'old_D_source_absent_confirmed',
                                               'consumer_aliases_verified')):
            return phase
        result = previous_completion_reconciliation(evidence, phase, destination)
        # The comparison is read-only and does not acquire the sender's
        # publication locks. Persist checkpoints so a service restart resumes.
        from stockagent.data_sync.transport_relocation_audit import compare_fixed_copy
        comparison = compare_fixed_copy(evidence, destination)
        from stockagent.data_sync.transport_relocation_delta import preserve_differences
        native_old_root = Path('/mnt/d') / (source.name + '.relocated-to-E-v1')
        delta_root = volume.path('migration', create=True) / 'transport-delta-v1'
        delta = preserve_differences(evidence, native_old_root, delta_root, admit=volume.admit)
        from stockagent.data_sync.transport_relocation_retirement import (
            load_archived_controls_policy, observe_unchanged_held_copy, rehearse_delta_restore, retire_fixed_copy,
        )
        from stockagent.data_sync import transport_restore_held_partials as partial_restore
        restored = rehearse_delta_restore(evidence, volume.path('verify', create=True) /
                                         'transport-delta-restore-v1', admit=volume.admit)
        policy = load_archived_controls_policy(
            Path(__file__).resolve().parents[2]/'configs/data_sync/transport_relocation_archived_controls.json',
            comparison, evidence)
        result.update(comparison=comparison, delta=delta, delta_member_restore=restored)
        retirement_path = evidence / 'retirement-receipt.json'
        if retirement_path.exists():
            retired = json.loads(retirement_path.read_bytes())
            if (retired.get('state') != 'registered_files_retired'
                    or retired.get('comparison_identity_sha256') != comparison['identity_sha256']
                    or retired.get('files_completed') != comparison['files_total']
                    or not retired.get('old_root_and_empty_directories_retained')):
                raise ValueError('fixed D transport retirement receipt differs')
            result['retirement'] = retired
        elif (result.get('namespace_inspection_complete') and result.get('syncthing_alias_verified')):
            pending_partial_restore = partial_restore.needs_restore(evidence, destination, comparison)
            if pending_partial_restore:
                from stockagent.data_sync.workspace_source_handoff import binance_handoff_window_allowed
                if not binance_handoff_window_allowed(for_start=True):
                    result['partial_name_restore'] = {'state': 'waiting_trading_priority',
                        'source_files_deleted': 0, 'retirement_authorized': False}
                    return result
            if (not pending_partial_restore and result.get('stale_mount_namespaces') == 0
                    and _namespace_handoff_plans_complete(evidence)):
                # If every previously held E name is still missing/different,
                # there is no mutation to perform. Observe without waiting on
                # the busy sender; a newly usable member must enter the full
                # original lock/namespace/recovery/unlink path below.
                waiting = (observe_unchanged_held_copy(evidence, native_old_root, destination, archived_fallback=policy)
                           if policy else observe_unchanged_held_copy(evidence, native_old_root, destination))
                if waiting is not None:
                    result['retirement'] = waiting
                    result['held_queue_observation_without_sender_lock'] = True
                    return result
            # This resumes the original placement transaction with the same
            # writer locks. A mount, reader or busy owner always defers it.
            with sender_boundary(evidence, c, wait=c.get('transport_namespace_handoff_contract')
                                 == 'stockagent.transport_namespace_handoff.v1',
                                 # A fixed preparation otherwise waits one minute,
                                 # sleeps one minute, and can repeatedly miss
                                 # the sender's 30-second inter-cycle window.
                                 # This wait holds no partial lock set.
                                 recheck_seconds=600 if pending_partial_restore else 60,
                                 purpose='namespace_handoff' if result.get('stale_mount_namespaces')
                                     else 'old_D_transport_retirement') as sender_locks:
                if pending_partial_restore and not binance_handoff_window_allowed(for_start=True):
                    # The trading priority window can begin while waiting.
                    result['partial_name_restore'] = {'state': 'waiting_trading_priority',
                        'source_files_deleted': 0, 'retirement_authorized': False}
                    return result
                handoff = None
                if c.get('transport_namespace_handoff_contract') == 'stockagent.transport_namespace_handoff.v1':
                    from stockagent.data_sync.transport_namespace_handoff import run as handoff_namespaces
                    handoff = handoff_namespaces(evidence, destination)
                    # Keep the old handoff receipt immutable. A fresh global
                    # observation, not this helper's exit, gates retirement.
                    result.update(previous_completion_reconciliation(evidence, phase, destination))
                    result['namespace_handoff'] = handoff
                if (result.get('namespace_inspection_complete')
                        and result.get('stale_mount_namespaces') == 0
                        and (handoff is None or handoff.get('waiting') == 0)
                        and result.get('syncthing_alias_verified')):
                    if pending_partial_restore:
                        result['partial_name_restore'] = partial_restore.restore(
                            evidence, destination, c, sender_locks, admit=volume.admit)
                    result['retirement'] = (retire_fixed_copy(evidence, native_old_root, destination,
                                                            archived_fallback=policy)
                                            if policy else retire_fixed_copy(evidence, native_old_root, destination))
        return result
    with ExitStack() as stack:
        for lock_path in (state/'cycle.lock', state/'owner.lock',
                          Path(c['immutable_archive_state_root'])/'source-replication-owner.lock'):
            lock = stack.enter_context(lock_path.open('a'))
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        volume.admit()
        base, key = credentials()
        folder_id = c['syncthing_folder_id']
        endpoint = '/rest/config/folders/' + folder_id
        folder = request(base, key, endpoint)
        if (folder['path'] != str(placement.CANONICAL) or folder['type'] != 'sendonly'
                or {x['deviceID'] for x in folder['devices']} != {c['producer_device_id'],c['receiver_device_id']}):
            raise ValueError('original sender enrollment changed')
        if 'prior_folder_paused' not in phase:
            phase['prior_folder_paused'] = folder['paused']
            atomic_write_json(phase_path, phase)
        request(base,key,endpoint,method='PATCH',payload={'paused':True})
        if not placement.on_e():
            # Verify that the active D alias is the source being captured.
            placement.check_transport()
            db = files.index(source,destination,evidence)
            intent = json.loads((evidence/'intent.json').read_bytes())
            db.close()
            volume.admit(intent['logical_bytes'])
            result = files.copy(source,destination,evidence,native_read=True)
            phase.update(state='copy_verified',copy=result,old_marker_sha256=hashlib.sha256(placement.placement()[1]).hexdigest())
            atomic_write_json(phase_path,phase)
            # Readers must release the previous transport before alias cutover.
            aliases = process_roots(placement.CANONICAL,placement.CANONICAL)
            refs = process_references_many(aliases)
            if refs:
                raise BlockingIOError('old transport is still referenced; wait at original owner boundary')
            new = {**placement.placement()[0], 'schema_version':2,'physical_volume':'E:',
                   'backing':'enrolled_ext4_workspace','relative_root':'transport/lab203'}
            raw = (json.dumps(new,ensure_ascii=False,indent=2)+'\n').encode()
            from stockagent.data_sync.desync_snapshots import atomic_write_bytes
            atomic_write_bytes(destination/'.transport-owner.json',raw,mode=0o600)
            atomic_write_bytes(placement.CONFIG,raw,mode=0o644)
            current = json.loads(config_path.read_bytes())
            current['required_mounts'][str(placement.CANONICAL)] = 'enrolled-E-ext4'
            atomic_write_json(config_path,current)
            phase['state']='configured_E';atomic_write_json(phase_path,phase)
        if not destination.samefile(placement.CANONICAL):
            if not source.samefile(placement.CANONICAL):
                raise ValueError('unexpected sender alias during handoff')
            if process_references_many(process_roots(placement.CANONICAL,placement.CANONICAL)):
                raise BlockingIOError('previous transport still referenced')
            subprocess.run(['umount',str(placement.CANONICAL)],check=True)
            subprocess.run(['mount','--bind',str(destination),str(placement.CANONICAL)],check=True)
        placement.check_transport(physical=True)
        phase['state']='E_alias_verified';atomic_write_json(phase_path,phase)
        # Keep all existing mount and Syncthing owners; add only boot ordering.
        dropin = Path('/etc/systemd/system/stockagent-backup-transport-mount.service.d/e-workspace.conf')
        dropin.parent.mkdir(parents=True,exist_ok=True)
        dropin.write_text('[Unit]\nRequires=stockagent-e-workspace-mount.service\nAfter=stockagent-e-workspace-mount.service\n')
        subprocess.run(['systemctl','daemon-reload'],check=True)
        if not phase['prior_folder_paused']:
            request(base,key,endpoint,method='PATCH',payload={'paused':False})
        # Old physical D names are no longer used by the sender. Lock+alias
        # fencing and fresh exact E readback authorize only these copy names.
        db=sqlite3.connect(evidence/'files.sqlite')
        quarantine=source.with_name(source.name+'.relocated-to-E-v1')
        # PID 1's bind alone does not prove every existing reader has cut over.
        namespaces = stale_transport_namespaces()
        if (not syncthing_alias_matches(destination) or not namespaces['inspection_complete']
                or namespaces['count']):
            db.close()
            raise BlockingIOError('old transport mount namespace or sender alias still needs handoff')
        if not source.exists() and not quarantine.exists():
            db.close()
            raise BlockingIOError('old D transport absence lacks a retirement journal')
        if source.exists():
            inode_keys={(row[0],row[1]) for (s,) in db.execute('SELECT sig FROM files')
                        for row in [json.loads(s)]}
            aliases=process_roots(Path('/srv/stockagent-d-volume'),source)
            if process_references_many(aliases,inode_keys=inode_keys):
                raise BlockingIOError('verified old D copy still has process references')
            if quarantine.exists():
                raise ValueError('both old and quarantined transport roots exist')
            phase.update(state='quarantine_intent',quarantine=str(quarantine))
            atomic_write_json(phase_path,phase)
            os.rename(source,quarantine)
            phase['state']='quarantined';atomic_write_json(phase_path,phase)
        retired=0
        if quarantine.exists():
            keys={(s[0],s[1]) for (raw,) in db.execute('SELECT sig FROM files') for s in [json.loads(raw)]}
            if process_references_many(process_roots(Path('/srv/stockagent-d-volume'),quarantine),inode_keys=keys):
                raise BlockingIOError('quarantined transport still has a live reference')
            free_before = os.statvfs(quarantine).f_bavail*os.statvfs(quarantine).f_frsize
            for relative,sig,sha in db.execute("SELECT path,sig,sha FROM files WHERE kind='file'"):
                origin=quarantine/relative
                if not origin.exists():
                    continue  # Previously retired exact name; E remains checked.
                if files.signature(origin)!=json.loads(sig):
                    raise ValueError('quarantined D member changed; preserve remaining sources')
                target=destination/relative
                if relative=='.transport-owner.json':
                    if target.read_bytes()!=placement.placement()[1]:
                        raise ValueError('new E transport marker differs')
                elif files.digest(target)!=sha:
                    raise ValueError('E destination changed before old-copy retirement')
                origin.unlink()
                retired+=1
                if retired%512==0:
                    phase.update(state='retiring_verified_D_copy',old_names_retired=retired,observed_at_epoch=time.time())
                    atomic_write_json(phase_path,phase)
            for relative, in db.execute("SELECT path FROM files WHERE kind='directory' ORDER BY length(path) DESC"):
                p=quarantine/relative
                if p.exists():p.rmdir()
            quarantine.rmdir()
            free_after=os.statvfs(source.parent).f_bavail*os.statvfs(source.parent).f_frsize
            phase['D_available_change_during_retirement_bytes']=free_after-free_before
        db.close()
        # A fully counted uninterrupted pass is required before recording a
        # retirement receipt. Missing names after an interrupted pass need a
        # separate durable per-name journal; guessing would fabricate proof.
        expected_files = json.loads((evidence/'intent.json').read_bytes())['files']
        native = Path('/mnt/d') / source.name
        native_quarantine = native.with_name(native.name + '.relocated-to-E-v1')
        namespaces = stale_transport_namespaces()
        if (retired != expected_files or source.exists() or quarantine.exists()
                or native.exists() or native_quarantine.exists()
                or not namespaces['inspection_complete'] or namespaces['count']
                or not syncthing_alias_matches(destination)):
            phase.update(state='retirement_unverified', observed_at_epoch=time.time())
            atomic_write_json(phase_path,phase)
            atomic_write_json(evidence/'progress.json',phase)
            return phase
        phase.update(state='completed',physical_volume='E:',cold_physical_volume='D:',
                     source_observations_deleted=0,cold_objects_deleted=0,nas_objects_deleted=0,
                     original_sender_path_preserved=True, old_D_copy_retirement_verified=True,
                     old_D_source_absent_confirmed=True, consumer_aliases_verified=True,
                     observed_at_epoch=time.time())
        atomic_write_json(phase_path,phase)
        atomic_write_json(evidence/'progress.json',phase)
        return phase
