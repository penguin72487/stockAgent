"""Personal NAS backup of the existing private immutable authority.

This is another source scope of BackupStream, using its owner, ledger, bounded
export, receiver and verified transport retirement. Shared publication and the
original local-only marker keep their original meanings and bytes.
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
import json
import os
from pathlib import Path
import stat
import time
import uuid

from scripts.backup_delivery_receipt import read_json, validate_readiness
from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync.offhost_backup import _regular, export_incremental_delivery, private_json
from stockagent.runtime_identity import identity_sha256

CONTRACT = 'stockagent.private_cold_NAS_enrollment.v1'
NAMESPACE = 'private-history/'
KIND = 'private_immutable_cold_backup'
WAVE_BYTES = 512 * 1024**2


@contextmanager
def publication_owner(queue, lock: Path, *, wait_for_owner: bool = False):
    """Report a real owner wait without repeatedly reading the fixed catalog.

    The caller already owns private-capture.lock, so another private turn
    cannot replace this small status receipt while it is being updated.
    """
    with lock.open('a') as owner:
        def waiting():
            path = queue.state / 'private-status.json'
            prior = json.loads(_regular(path).read_bytes()) if path.exists() else {}
            private_json(path, {**prior, 'contract': CONTRACT,
                'state': 'private_backup_waiting_publication_owner',
                'observed_at_utc': datetime.now(timezone.utc).isoformat(),
                'all_project_sources_NAS_verified': False, 'source_files_deleted': 0}, compact=True)
        if wait_for_owner:
            waiting()
        try:
            # An explicit supervised waiter joins the kernel queue. Polling
            # every 30 s could miss every 50 ms inter-wave handoff forever.
            fcntl.flock(owner, fcntl.LOCK_EX | (0 if wait_for_owner else fcntl.LOCK_NB))
        except BlockingIOError:
            waiting()
            raise
        yield owner


def validate_policy(policy: dict) -> None:
    expected = {'contract': CONTRACT, 'enabled': True,
                'source_relative_root': '.local-state/private-history',
                'private_backup_authorized': True, 'public_publish_allowed': False,
                'source_cleanup_authorized': False,
                'route': 'existing_paired_encrypted_Restic_relay',
                'scope': 'all_retained_private_immutable_manifests'}
    if (any(type(policy.get(k)) is not type(v) or policy[k] != v for k, v in expected.items())
            or not isinstance(policy.get('user_instruction'), str) or not policy['user_instruction'].strip()
            or not isinstance(policy.get('user_instruction_date'), str)):
        raise SnapshotError('private NAS scope needs explicit personal backup authorization')
    try:
        datetime.strptime(policy['user_instruction_date'], '%Y-%m-%d')
    except ValueError:
        raise SnapshotError('private NAS authorization date is invalid') from None


def validate_private_root(root: Path, policy: dict) -> None:
    validate_policy(policy)
    if (root.name != 'private-history' or root.parent.name != '.local-state'
            or any(p.is_symlink() for p in (root, *root.parents)) or root.resolve() != root):
        raise SnapshotError('private NAS source is outside the original private authority')
    info = root.stat()
    marker = json.loads(_regular(root / 'private-history.json').read_bytes())
    expected = {'contract': 'all_catalog_incremental_migration_v1',
                'parent_authority': str(root.parent.parent), 'shared_export': False,
                'nas_export': False, 'source_retirement': False}
    if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid()
            or stat.S_IMODE(info.st_mode) != 0o700 or marker != expected):
        raise SnapshotError('original private marker/owner/permissions changed')


def namespace_catalog(catalog: dict) -> dict:
    """Keep private and public object/head names distinct in the one ledger."""
    body = {k: v for k, v in catalog.items() if k not in ('identity_sha256', 'observed_at_utc')}
    body['files'] = [{**r, 'relative': NAMESPACE + r['relative']} for r in catalog['files']]
    body['releases'] = [{**r, 'required_file_keys': [NAMESPACE + k for k in r['required_file_keys']]}
                        for r in catalog['releases']]
    body['source_catalog_identity_sha256'] = catalog['identity_sha256']
    body['backup_namespace'] = NAMESPACE
    from stockagent.data_sync.backup_stream import catalog_identity
    return {**body, 'identity_sha256': catalog_identity(body, omit_file_signatures=True),
            'observed_at_utc': catalog['observed_at_utc']}


def cycle(queue, *, publish: bool = True, wait_for_owner: bool = False) -> dict:
    """One bounded turn; full metadata capture never holds the publication lock."""
    from stockagent.data_sync.backup_stream import capture_catalog, coverage, file_key, select_wave
    if type(wait_for_owner) is not bool:
        raise ValueError('private owner waiting must be an explicit boolean')
    policy = queue.config.get('private_history_backup', {})
    if not policy.get('enabled'):
        return {'state': 'private_backup_not_enrolled', 'all_project_sources_NAS_verified': False}
    validate_policy(policy)
    root = queue.cold / policy['source_relative_root']
    queue.storage_guard()
    validate_private_root(root, policy)
    # This guard prevents duplicate metadata captures, not another publication
    # owner. Every admission, dispatch and ledger change uses owner.lock below.
    capture_lock = queue.state / 'private-capture.lock'
    if capture_lock.is_symlink():
        raise SnapshotError('private capture lock is redirected')
    with capture_lock.open('a') as capture_owner:
        fcntl.flock(capture_owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        plan_path = queue.state / 'private-catalog.json'
        lock = queue.state / 'owner.lock'
        if lock.is_symlink():
            raise SnapshotError('original backup owner is redirected')
        if plan_path.exists() and not wait_for_owner:
            # A busy publisher cannot accept another wave. Avoid repeatedly
            # parsing a large cached catalog while waiting for that owner.
            # A supervised kernel waiter instead parses once outside the
            # owner and queues only for the real transaction below.
            with publication_owner(queue, lock, wait_for_owner=wait_for_owner):
                pass
        policy_id = identity_sha256(policy)
        catalog = json.loads(_regular(plan_path).read_bytes()) if plan_path.exists() else None
        if catalog is not None and catalog.get('private_backup_policy_identity_sha256') != policy_id:
            raise SnapshotError('retain the private catalog made under another authorization')
        if catalog is not None:
            from stockagent.data_sync.backup_stream import catalog_identity
            body = {k: v for k, v in catalog.items() if k not in ('identity_sha256', 'observed_at_utc')}
            if (catalog.get('backup_namespace') != NAMESPACE
                    or catalog_identity(body, omit_file_signatures=True) != catalog.get('identity_sha256')):
                raise SnapshotError('private fixed catalog identity changed')
            prior_path = queue.state / 'private-status.json'
            prior = json.loads(_regular(prior_path).read_bytes()) if prior_path.exists() else {}
            if (prior.get('catalog_identity_sha256') == catalog['identity_sha256']
                    and prior.get('registered_private_closure_NAS_verified') is True):
                # The previous fixed closure is fully acknowledged. Observe
                # new immutable versions with the same canonical collector.
                catalog = None
        if catalog is None:
            private_json(queue.state / 'private-status.json', {
                'contract': CONTRACT, 'state': 'capturing_private_cold_metadata',
                'observed_at_utc': datetime.now(timezone.utc).isoformat(),
                'registered_private_closure_NAS_verified': False,
                'all_project_sources_NAS_verified': False, 'source_files_deleted': 0})
            catalog = namespace_catalog(capture_catalog(root, Path(queue.config['publication_catalog']),
                private_backup_policy=policy))
            private_json(plan_path, catalog, compact=True)
            private_json(queue.state / 'private-status.json', {
                'contract': CONTRACT, 'state': 'private_catalog_captured',
                'observed_at_utc': datetime.now(timezone.utc).isoformat(),
                'catalog_identity_sha256': catalog['identity_sha256'],
                'available_file_count': len(catalog['files']),
                'available_bytes': sum(row['bytes'] for row in catalog['files']),
                'nas_covered_file_count': None, 'nas_covered_bytes': None,
                'pending_private_bytes': None,
                'registered_private_closure_NAS_verified': False,
                'canonical_private_reconstruction_verified': False,
                'all_project_sources_NAS_verified': False, 'source_files_deleted': 0}, compact=True)
        notifications = []
        with publication_owner(queue, lock, wait_for_owner=wait_for_owner):
            private_json(queue.state / 'private-status.json', {
                'contract': CONTRACT, 'state': 'private_original_owner_acquired',
                'catalog_identity_sha256': catalog['identity_sha256'],
                'available_file_count': len(catalog['files']),
                'available_bytes': sum(row['bytes'] for row in catalog['files']),
                'observed_at_utc': datetime.now(timezone.utc).isoformat(),
                'registered_private_closure_NAS_verified': False,
                'all_project_sources_NAS_verified': False, 'source_files_deleted': 0}, compact=True)
            queue.storage_guard()
            validate_private_root(root, policy)
            ledger = queue.load_ledger()
            queue.recover_publications(ledger)
            errors = queue.ingest(ledger)
            result = {'contract': CONTRACT, 'catalog_identity_sha256': catalog['identity_sha256'],
                      'catalog_observed_at_utc': catalog['observed_at_utc'], 'state': 'inspected',
                      'published_waves': [], 'receipt_errors': errors,
                      'unpublished_sources_included': False, 'all_project_sources_NAS_verified': False,
                      'canonical_private_reconstruction_verified': False,
                      'source_files_deleted': 0, 'shared_publication_changed': False}
            retries = ledger.setdefault('private_export_retries', {})
            if publish and not errors and queue.config.get('cold_transport_cache', {}).get('enabled'):
                result['transport_cache'] = queue.retire_transport_cache(ledger)
            usage = None
            for _ in range(queue.pipeline.get('maximum_waves_per_cycle', 1) if publish and not errors else 0):
                errors.extend(queue.ingest(ledger))
                if errors:
                    result['state'] = 'private_backup_waiting_valid_receipts'
                    break
                covered = coverage(catalog, ledger)
                sent = covered['published_file_keys'] | covered['accepted_file_keys']
                deferred = {k for k, v in retries.items() if v['next_attempt_epoch'] > time.time()}
                if not any(file_key(r) not in sent | deferred for r in catalog['files']):
                    result['state'] = ('private_backup_waiting_export_retry' if deferred
                                       else 'private_files_published_waiting_NAS_ACK')
                    break  # No allocation: no full transport census/capacity query.
                ready = read_json(queue.receipts / 'readiness.json')
                validate_readiness(ready, queue.config)
                reserve = max(queue.config['reserve_bytes'], ready['reserve_bytes'])
                if usage is None:
                    usage = queue.transport_usage()
                limit = min(queue.config['maximum_batch_bytes'], ready['maximum_batch_bytes'],
                            queue.staging_room(usage),
                            queue.config['maximum_pending_bytes'] - covered['pending_bytes'],
                            ready['ingress_free_bytes'] - reserve - covered['pending_bytes'],
                            ready['nas_free_bytes'] - reserve - covered['pending_bytes'])
                if limit <= 1024**2:
                    result['state'] = 'private_backup_waiting_capacity'
                    break
                selected = select_wave(catalog, sent | deferred, maximum_bytes=min(limit, WAVE_BYTES),
                                       maximum_files=queue.config['maximum_batch_files'])
                if not selected:
                    # A latency target must never become a hard size limit for
                    # a valid older pack. Admit it within the original quota.
                    selected = select_wave(catalog, sent | deferred, maximum_bytes=limit,
                                           maximum_files=queue.config['maximum_batch_files'])
                if not selected:
                    unsent = any(file_key(r) not in sent for r in catalog['files'])
                    result['state'] = ('private_backup_waiting_export_retry' if unsent and deferred
                                       else 'private_object_exceeds_available_wave_budget' if unsent
                                       else 'private_files_published_waiting_NAS_ACK')
                    break
                original = [{**r, 'relative': r['relative'][len(NAMESPACE):]} for r in selected]
                staging = queue.transport / '.staging' / ('private-cold-' + uuid.uuid4().hex)
                result.update(state='copying_and_independently_verifying_private_wave',
                              current_wave_files=len(selected),
                              current_wave_bytes=sum(row['bytes'] for row in selected),
                              available_file_count=len(catalog['files']),
                              available_bytes=sum(row['bytes'] for row in catalog['files']),
                              observed_at_utc=datetime.now(timezone.utc).isoformat())
                private_json(queue.state / 'private-status.json', result, compact=True)
                try:
                    exported = export_incremental_delivery(root, original, staging,
                        catalog_identity=catalog['identity_sha256'], namespace='private-history',
                        copy_workers=queue.pipeline.get('copy_workers', 1),
                        verify_workers=queue.pipeline.get('verify_workers', 1), native_windows_io=True)
                    key = exported['envelope_identity_sha256']
                    queue.publish_closed(ledger, selected, staging, key, catalog['identity_sha256'], KIND,
                                         native_windows_io=True)
                    queue._save_ledger(ledger)
                    # The same owner still excludes all allocating publishers.
                    # Account for the complete new wave, including controls;
                    # transport GC can only reduce this conservative bound.
                    # Physical free space is still measured at every admission.
                    usage = {**usage, 'retained_transport_bytes': usage['retained_transport_bytes']
                             + ledger['deliveries'][key]['dispatch']['complete_bytes']}
                    for row in selected:
                        retries.pop(file_key(row), None)
                    notifications.append({'deliveries': {key: ledger['deliveries'][key]}})
                    result['published_waves'].append(key)
                    result['state'] = 'private_files_published_waiting_NAS_ACK'
                    result['observed_at_utc'] = datetime.now(timezone.utc).isoformat()
                    private_json(queue.state / 'private-status.json', result, compact=True)
                except (OSError, ValueError, RuntimeError) as error:
                    usage = None  # Unknown partial allocations need a new full census.
                    # Preserve the exact failed stage. Do not let one bad
                    # private wave block the normal public metadata controller.
                    result.update(state='private_export_retry_required', error_type=type(error).__name__,
                                  error_reason=str(error)[:512], retained_staging=str(staging))
                    for row in selected:
                        file = file_key(row)
                        attempt = retries.get(file, {}).get('attempts', 0) + 1
                        delays = queue.pipeline.get('retry_delays_seconds', [30, 120, 600])
                        retries[file] = {'attempts': attempt, 'error_type': type(error).__name__,
                                        'next_attempt_epoch': time.time() + delays[min(attempt - 1, len(delays) - 1)],
                                        'retained_staging': str(staging)}
                    queue._save_ledger(ledger)
                    continue
            queue._save_ledger(ledger)
            covered = coverage(catalog, ledger)
            for k in ('available_file_count', 'available_bytes', 'machine_acknowledged_files',
                      'nas_covered_file_count', 'nas_covered_bytes', 'current_release_count',
                      'current_releases_nas_file_covered', 'retained_release_count',
                      'retained_releases_nas_file_covered', 'metadata_error_count', 'missing_object_count'):
                result[k] = covered[k]
            result['pending_private_bytes'] = covered['available_bytes'] - covered['nas_covered_bytes']
            result['deferred_private_files'] = sum(v['next_attempt_epoch'] > time.time() for v in retries.values())
            result['registered_private_closure_NAS_verified'] = (
                covered['machine_acknowledged_files'] == covered['available_file_count']
                and not catalog['missing_objects'] and not catalog['metadata_errors'])
            if result['registered_private_closure_NAS_verified']:
                result['state'] = 'registered_private_files_NAS_ACK_verified'
            result['observed_at_utc'] = datetime.now(timezone.utc).isoformat()
            private_json(queue.state / 'private-status.json', result, compact=True)
        # The original durable dispatches/ledger are the notification journal;
        # normal notify-transport also retries them after interruption.
        for notification in notifications:
            queue.scan_transport(notification)
        return result


def source_scope_inventory(repository: Path, configuration: dict) -> dict:
    """Bounded metadata census; never call a provider, walk payloads or infer ACKs."""
    cold = Path(configuration['cold_root'])
    entries = json.loads(Path(configuration['publication_catalog']).read_bytes())['datasets']
    shared = {p.name for p in (cold / 'heads').iterdir() if p.is_dir()}
    private_root = cold / '.local-state/private-history'
    private = ({p.name for p in (private_root / 'heads').iterdir() if p.is_dir()}
               if (private_root / 'heads').is_dir() else set())
    current = Path(configuration.get('immutable_archive_state_root',
                   str(Path(configuration['state_root']) / 'not-enrolled'))) / 'dataset-versions/current.json'
    migration = {}
    if current.exists():
        current_state = json.loads(_regular(current).read_bytes())
        progress = Path(current_state['operation_directory']) / 'progress.json'
        if progress.exists():
            migration = json.loads(_regular(progress).read_bytes()).get('datasets', {})
    listed = []
    roots = set()
    entries = list(entries)
    known = {entry['dataset'] for entry in entries}
    entries.extend({'dataset': name, 'source': value['source'],
                    'publish': value.get('domain') == 'shared', 'scope_origin': 'canonical_migration'}
                   for name, value in migration.items() if name not in known and 'source' in value)
    for entry in entries:
        source = Path(entry['source'])
        if not source.is_absolute():
            source = repository / source
        roots.add(str(source))
        state = migration.get(entry['dataset'], {})
        listed.append({'dataset': entry['dataset'], 'source': str(source),
            'scope_origin': entry.get('scope_origin', 'publication_catalog'),
            'source_exists': source.exists(), 'source_is_symlink': source.is_symlink(),
            'shared_publication_enabled': entry.get('publish') is True,
            'shared_cold_head_observed': entry['dataset'] in shared,
            'private_cold_head_observed': entry['dataset'] in private,
            'migration_state': state.get('state', 'not_in_current_migration_receipt'),
            'source_currentness_and_NAS_recovery_verified': False})
    unregistered = [str(p) for p in repository.iterdir()
                    if p.name.startswith('data_') and p.is_dir() and str(p) not in roots
                    and not any(Path(r).is_relative_to(p) for r in roots)
                    and not any(p.resolve() == Path(r).resolve() for r in roots)]
    private_status = Path(configuration['state_root']) / 'private-status.json'
    public_status = Path(configuration['state_root']) / 'status.json'
    return {'contract': 'stockagent.project_NAS_source_scope_inventory.v1',
        'observed_at_utc': datetime.now(timezone.utc).isoformat(), 'catalog_sources': listed,
        'registered_source_count': len(listed), 'private_head_datasets': sorted(private),
        'private_head_dataset_count': len(private), 'shared_head_dataset_count': len(shared),
        'unregistered_data_roots': sorted(unregistered),
        'private_enrollment': configuration.get('private_history_backup', {}),
        'private_NAS_status': json.loads(_regular(private_status).read_bytes()) if private_status.exists() else None,
        'public_cold_NAS_status': json.loads(_regular(public_status).read_bytes()) if public_status.exists() else None,
        'all_project_sources_NAS_verified': False,
        'metadata_only': True, 'source_files_deleted': 0,
        'other_scopes_requiring_separate_proofs': [
            'unpublished/mutable source tails via canonical dataset-version lifecycle',
            'artifacts, active service projections and live ledgers via original archive/control owners',
            'code/config/SQL auxiliary backup and independently restored current runtime',
            'private fixed news generation via its existing exact delivery plan'],
        'scope_note': 'Cold head presence is not current-source parity or NAS recovery; no recursive payload census.'}
