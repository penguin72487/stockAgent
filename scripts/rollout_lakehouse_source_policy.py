#!/usr/bin/env python3
"""Apply the local source policy at the existing publication-owner boundary."""
import argparse
import asyncio
from contextlib import ExitStack, contextmanager
from datetime import datetime, timedelta, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from downloader.artifact_io import atomic_write_json
from stockagent.control.lakehouse import configuration, guard, source_wave_policy
from scripts.manage_lakehouse import worker_code_identity

OWNER = 'stockagent-storage-lifecycle.service'
IDS = ('stockagent-storage-lifecycle-v1', 'stockagent-source-replication-v1')
OPTIONAL_IDS = ('stockagent-dataset-versions-v1',)
PRIVATE_CONFIGURATION = Path('/etc/stockagent/lakehouse-control.json')
RECEIVER_READINESS = Path('/srv/stockagent-backup-receipts-lab203/readiness.json')
CATALOG_RELATIVE = 'configs/data_sync/packed_datasets.json'
ROOT_BACKUP_ROOT = 'artifacts/operations/root-backup-consolidation-20261028'
ROOT_BACKUP_DATASET = 'host-private-root-backups-20261028'
ROOT_BACKUP_NAMES = frozenset({
    'StockAgent', 'stockagent-sync-backups', 'stockAgent-git-recovery-20261015.j0E2PL',
    'stockAgent-github-sync-20261015.LTXWb1', 'stockAgent-two-node-sync-20261015.Luk1nu',
    'stockAgent.git-recovery-backup-20260620-restore', 'syncthing-merge-backups',
})


def prepare_root_backup_enrollment(repo):
    """Prepare the exact already-moved backup tree for private versioning."""
    from stockagent.data_sync import retained_source as retained
    from stockagent.data_sync.packed_snapshots import _collect_entries
    from scripts.publish_data_releases import _load_catalog
    repo = Path(repo).absolute()
    state = repo / ROOT_BACKUP_ROOT
    source = state / 'data'
    if (source.resolve(strict=True) != source or state.stat().st_mode & 0o077
            or source.stat().st_mode & 0o077 or not source.is_dir()
            or state.stat().st_uid != os.geteuid() or source.stat().st_uid != os.geteuid()):
        raise ValueError('private root backup source is redirected or not private')
    if {path.name for path in source.iterdir()} != ROOT_BACKUP_NAMES:
        raise ValueError('root backup source differs from its seven moved directories')
    receipt_hashes = {}
    expected_files = 0
    for name in sorted(ROOT_BACKUP_NAMES):
        path = state / (name + '.receipt.json')
        raw, _ = retained._read_binding(path)
        receipt = json.loads(raw)
        target = source / name
        observed = target.lstat()
        if (receipt.get('contract') != 'root-backup-atomic-relocation-v1'
                or receipt.get('state') != 'verified' or receipt.get('payload_deleted') is not False
                or receipt.get('move', {}).get('target') != str(target)
                or receipt['move'].get('directory_inode') != observed.st_ino
                or receipt['move'].get('device') != observed.st_dev
                or target.is_symlink() or not target.is_dir()
                or type(receipt.get('file_count')) is not int or receipt['file_count'] < 0):
            raise ValueError('root backup relocation proof differs from its source')
        expected_files += receipt['file_count']
        receipt_hashes[name] = hashlib.sha256(raw).hexdigest()
    _, census = _collect_entries(source, retain_entries=False)
    if census['symlinks'] or census['files'] != expected_files:
        raise ValueError('root backup file census differs from relocation proofs')
    catalog_path = repo / CATALOG_RELATIVE
    raw, _ = retained._read_binding(catalog_path)
    catalog = json.loads(raw)
    entries = _load_catalog(catalog_path, include_families=True)
    if any(row['dataset'] == ROOT_BACKUP_DATASET for row in entries):
        raise ValueError('root backup dataset is already enrolled; reconcile its existing owner')
    entry = {'dataset': ROOT_BACKUP_DATASET, 'source': ROOT_BACKUP_ROOT + '/data',
        'role': 'private-owner-root-backup-originals', 'publish': False,
        'reader_contract': 'packed-v2-chunks', 'file_chunk_bytes': 8 * 1024**2,
        'pack_buckets': 64, 'loose_threshold_mib': 8, 'active_process_substrings': [],
        'note': 'Private relocated owner backups; personal NAS backup uses the existing paired private-history owner. No public export or source retirement is granted.'}
    catalog['datasets'].append(entry)
    updated = (json.dumps(catalog, ensure_ascii=False, indent=2) + '\n').encode()
    return {'source': source, 'state': state, 'census': census, 'receipt_hashes': receipt_hashes,
        'catalog_path': catalog_path, 'catalog_original': raw, 'catalog_updated': updated,
        'catalog_mode': catalog_path.stat().st_mode & 0o777, 'entry': entry}


def commit_root_backup_enrollment(prepared, intent_path):
    """Enroll at the drained canonical owner boundary; do not delete payloads."""
    from stockagent.data_sync import retained_source as retained
    from stockagent.data_sync.desync_snapshots import atomic_write_bytes
    from stockagent.data_sync.packed_snapshots import _collect_entries
    before, _ = retained._read_binding(prepared['catalog_path'])
    if before != prepared['catalog_original']:
        raise ValueError('source catalog changed before root backup enrollment')
    for name, expected in prepared['receipt_hashes'].items():
        raw, _ = retained._read_binding(prepared['state'] / (name + '.receipt.json'))
        if hashlib.sha256(raw).hexdigest() != expected:
            raise ValueError('root backup relocation proof changed before enrollment')
    _, current = _collect_entries(prepared['source'], retain_entries=False)
    if current != prepared['census']:
        raise ValueError('root backup source changed while waiting for the owner')
    if intent_path.exists() or intent_path.is_symlink():
        raise ValueError('retain the original enrollment intent; use a new receipt')
    intent = {'contract': 'private_root_backup_catalog_enrollment_v1', 'state': 'prepared',
        'entry': prepared['entry'], 'source_census': current, 'relocation_receipt_sha256': prepared['receipt_hashes'],
        'catalog_before_sha256': hashlib.sha256(before).hexdigest(),
        'catalog_after_sha256': hashlib.sha256(prepared['catalog_updated']).hexdigest(),
        'source_files_deleted': 0, 'cold_reconstruction_verified': False, 'NAS_restore_ACK_verified': False}
    atomic_write_json(intent_path, intent, durable=True)
    atomic_write_bytes(prepared['catalog_path'], prepared['catalog_updated'], mode=prepared['catalog_mode'])
    after, _ = retained._read_binding(prepared['catalog_path'])
    if after != prepared['catalog_updated']:
        raise ValueError('private root backup catalog readback differs')
    return {**intent, 'state': 'catalog_enrolled_not_yet_cold_or_NAS_verified',
            'intent_receipt': str(intent_path)}


def prepare_retained_cutover(repo, c, candidate_path):
    """Prepare exact metadata; neither activate the source nor stop its owner."""
    from stockagent.data_sync import retained_source as retained
    from stockagent.data_sync.dataset_migration import private_history_root
    from stockagent.data_sync.dataset_version_layout import version_policy
    from scripts.publish_data_releases import _load_catalog, _source_path
    candidate_raw, _ = retained._read_binding(candidate_path.absolute())
    document = json.loads(candidate_raw)
    binding = document['binding']
    catalog_path = repo / CATALOG_RELATIVE
    catalog_raw, _ = retained._read_binding(catalog_path)
    catalog = json.loads(catalog_raw)
    entries = _load_catalog(catalog_path)
    selected = [row for row in entries if row['dataset'] == binding['dataset']]
    if len(selected) != 1:
        raise ValueError('retained cutover requires one existing canonical source')
    original = selected[0]
    source = _source_path(original).absolute()
    cold = private_history_root(Path(c['cold_root']), version_policy(repo), initialize=False)
    if (binding.get('contract') != retained.CONTRACT or binding.get('source') != str(source)
            or binding.get('cold_root') != str(cold)
            or binding.get('retirement_authorized_by_binding') is not False):
        raise ValueError('retained cutover candidate differs from its original private source')
    if (source / retained.BINDING).exists() or (source / retained.BINDING).is_symlink():
        raise ValueError('retained source is already installed; reconcile its original cutover receipt')
    updated = {**original, 'retained_source_contract': retained.CONTRACT,
        'source_coordination_lock_scope': 'source_parent',
        'source_coordination_lock': f'{source.name}/{retained.LOCK}',
        'source_coordination_lock_mode': 'exclusive'}
    retained.validate_catalog(updated)
    catalog['datasets'] = [updated if row['dataset'] == binding['dataset'] else row
                           for row in catalog['datasets']]
    new_raw = (json.dumps(catalog, ensure_ascii=False, indent=2) + '\n').encode()
    return {'source': source, 'cold': cold, 'candidate': binding,
        'candidate_sha256': hashlib.sha256(candidate_raw).hexdigest(),
        'catalog_path': catalog_path, 'catalog_original': catalog_raw, 'catalog_updated': new_raw,
        'catalog_mode': catalog_path.stat().st_mode & 0o777,
        'materialized': Path(binding['workspace_raw_verification']['fixed_materialization']),
        'collector_lock': source / retained.LOCK}


def commit_retained_cutover(prepared, descriptor, intent_path):
    """Install at the drained publication and original collector boundary.

    Write the supporting catalog first. An interrupted binding write leaves
    every original in place and a publisher that can still capture the full
    workspace. No source retirement is authorized by this metadata transition.
    """
    from stockagent.data_sync import retained_source as retained
    from stockagent.data_sync.desync_snapshots import atomic_write_bytes
    source, cold, candidate = (prepared[key] for key in ('source', 'cold', 'candidate'))
    retained.require_collector_owner(source, descriptor)
    before, _ = retained._read_binding(prepared['catalog_path'])
    if before != prepared['catalog_original']:
        raise ValueError('source catalog changed before the drained cutover')
    fixed = retained.packed.resolve_packed_snapshot_id(cold, candidate['dataset'], candidate['snapshot_id'])
    if fixed.manifest_sha256 != candidate['manifest_sha256']:
        raise ValueError('retained cutover fixed manifest changed')
    binding = retained.prepare_retained_source_binding(source, cold, retained.DEFAULT_PIN_ROOT, fixed,
        source_lock_fd=descriptor, fixed_materialized_path=prepared['materialized'])
    for key in ('contract', 'source', 'cold_root', 'dataset', 'snapshot_id', 'manifest_sha256',
                'inventory_sha256', 'raw_files', 'retirement_authorized_by_binding'):
        if binding[key] != candidate[key]:
            raise ValueError('fresh retained verification differs from the reviewed candidate')
    if retained._read_binding(prepared['catalog_path'])[0] != before:
        raise ValueError('source catalog changed during full cutover recovery')
    if intent_path.exists() or intent_path.is_symlink():
        raise ValueError('retain the original cutover intent; use a new receipt')
    atomic_write_json(intent_path, {'contract': 'retained_source_owner_cutover_v1', 'state': 'prepared',
        'dataset': candidate['dataset'], 'snapshot_id': fixed.manifest['snapshot_id'],
        'manifest_sha256': fixed.manifest_sha256, 'candidate_sha256': prepared['candidate_sha256'],
        'catalog_before_sha256': hashlib.sha256(before).hexdigest(),
        'catalog_after_sha256': hashlib.sha256(prepared['catalog_updated']).hexdigest(),
        'catalog_before': json.loads(before), 'binding': binding,
        'source_files_deleted': 0, 'retirement_authorized': False}, durable=True)
    atomic_write_bytes(prepared['catalog_path'], prepared['catalog_updated'], mode=prepared['catalog_mode'])
    atomic_write_json(source / retained.BINDING, binding, durable=True)
    os.chmod(source / retained.BINDING, 0o600)
    state = retained.load_retained_source(source, candidate['dataset'], cold=cold,
                                          pin_root=retained.DEFAULT_PIN_ROOT)
    state.check_generation()
    if state.request.release.manifest_sha256 != fixed.manifest_sha256:
        raise ValueError('installed retained source did not independently read its reviewed fixed release')
    return {'contract': 'retained_source_owner_cutover_v1', 'state': 'installed_and_fixed_raw_readback_verified',
        'dataset': candidate['dataset'], 'snapshot_id': fixed.manifest['snapshot_id'],
        'manifest_sha256': fixed.manifest_sha256, 'binding_sha256': state.binding_sha256,
        'raw_files_verified': len(binding['raw_files']),
        'encoded_raw_bytes_verified': sum(row['bytes'] for row in binding['raw_files'].values()),
        'intent_receipt': str(intent_path), 'source_files_deleted': 0,
        'production_incremental_round_verified': False, 'retirement_authorized': False}


def pid(unit):
    return subprocess.check_output(['systemctl','show',unit,'-p','MainPID','--value'],text=True).strip()


def migration_checkpoint(state_root):
    """Capture fixed accepted bindings, not just a mutable completion count."""
    root = Path(state_root) / 'dataset-versions'
    current = root / 'current.json'
    if not current.exists():
        return None
    selected = json.loads(current.read_bytes())
    operation = Path(selected['operation_directory'])
    if operation.resolve() != operation or not operation.is_relative_to(root / 'operations'):
        raise ValueError('migration checkpoint is outside its original owner')
    raw = (operation / 'progress.json').read_bytes()
    progress = json.loads(raw)
    if progress['plan_sha256'] != selected['plan_sha256']:
        raise ValueError('migration plan identity changed during rollout preflight')
    return {'operation_directory': str(operation), 'plan_sha256': progress['plan_sha256'],
            'progress_before_sha256': hashlib.sha256(raw).hexdigest(),
            'runtime_owner_before': progress.get('runtime_owner'),
            'inflight_attempts': {name: {key: row[key] for key in
                ('state', 'attempt_id', 'started_at_utc', 'snapshot_id') if key in row}
                for name, row in progress['datasets'].items() if row['state'] in ('processing', 'recovery')},
            'verified_releases': {name: {key: row[key] for key in ('snapshot_id', 'manifest_sha256')}
                                  for name, row in progress['datasets'].items()
                                  if row['state'] == 'verified'}}


def wait_for_committed_publication(state_root, dataset, deadline, candidate, output):
    """Preserve a long publisher; restart only after its fixed head is durable.

    This is not full recovery acceptance. The original journal must expose a
    snapshot and manifest which resolve to that exact closed D version. Any
    operation/code drift fails before systemd is touched.
    """
    from stockagent.data_sync.packed_snapshots import resolve_packed_snapshot_id
    initial = migration_checkpoint(state_root)
    if initial is None:
        raise ValueError('no original migration operation to hand off')
    directory = Path(initial['operation_directory'])
    while True:
        if worker_code_identity() != candidate:
            raise ValueError('worker candidate changed while waiting for source publication')
        current = migration_checkpoint(state_root)
        if current['operation_directory'] != str(directory) or current['plan_sha256'] != initial['plan_sha256']:
            raise ValueError('original migration operation changed before publication handoff')
        progress = json.loads((directory/'progress.json').read_bytes())
        row = progress['datasets'].get(dataset)
        if row is None:
            raise ValueError('handoff dataset is not in the original operation')
        snapshot, digest = row.get('snapshot_id'), row.get('manifest_sha256')
        if snapshot and digest:
            for cold in (Path('/srv/stockagent-packed/.local-state/private-history'), Path('/srv/stockagent-packed')):
                try:
                    fixed = resolve_packed_snapshot_id(cold, dataset, snapshot, require_objects=False)
                except (OSError, ValueError, RuntimeError):
                    continue
                if fixed.manifest_sha256 == digest:
                    return {'dataset': dataset, 'snapshot_id': snapshot, 'manifest_sha256': digest,
                            'publication_only_not_recovery_acceptance': True}
        if time.monotonic() >= deadline:
            raise TimeoutError('fixed source publication has not reached the restart boundary')
        atomic_write_json(output, {'state': 'waiting_for_fixed_source_publication',
            'observed_at_utc': datetime.now(timezone.utc).isoformat(), 'dataset': dataset,
            'candidate_code_identity_sha256': candidate, 'plan_sha256': initial['plan_sha256'],
            'source_retirement': False}, durable=False)
        time.sleep(5)


def verify_migration_checkpoint(checkpoint):
    if checkpoint is None:
        return 0
    progress = json.loads((Path(checkpoint['operation_directory']) / 'progress.json').read_bytes())
    if progress['plan_sha256'] != checkpoint['plan_sha256']:
        raise ValueError('original migration plan was replaced during rollout')
    for name, binding in checkpoint['verified_releases'].items():
        row = progress['datasets'].get(name, {})
        if row.get('state') != 'verified' or any(row.get(key) != value for key, value in binding.items()):
            raise ValueError('previously verified migration release was lost or rebound')
    return len(checkpoint['verified_releases'])


async def workflows(c):
    from temporalio.client import Client
    from temporalio.service import RPCError, RPCStatusCode
    client = await Client.connect(c['temporal_endpoint'],namespace=c['temporal_namespace'])
    result = {}
    for name in (*IDS, *OPTIONAL_IDS):
        try:
            d = await client.get_workflow_handle(name).describe()
        except RPCError as error:
            if name in OPTIONAL_IDS and error.status == RPCStatusCode.NOT_FOUND:
                continue
            raise
        result[name] = {'run_id':d.run_id,'first_run_id':d.raw_description.workflow_execution_info.first_run_id,
                        'status':d.status.name}
    return result


class PublicationOwnerBusy(TimeoutError):
    """Only an unacquired original publication boundary is a normal wait."""


async def started_activities(c):
    """Read the three canonical workflow activities without changing them."""
    from temporalio.api.enums.v1 import PendingActivityState
    from temporalio.client import Client
    from temporalio.service import RPCError, RPCStatusCode
    client = await Client.connect(c['temporal_endpoint'], namespace=c['temporal_namespace'])
    active = []
    for name in (*IDS, *OPTIONAL_IDS):
        try:
            description = await client.get_workflow_handle(name).describe(rpc_timeout=timedelta(seconds=10))
        except RPCError as error:
            if name in OPTIONAL_IDS and error.status == RPCStatusCode.NOT_FOUND:
                continue
            raise
        if description.status.name != 'RUNNING':
            raise ValueError('canonical workflow is not running during the handoff check')
        for item in description.raw_description.pending_activities:
            if not item.activity_type.name or not item.activity_id:
                raise ValueError('pending storage activity identity is incomplete')
            if item.state in {PendingActivityState.PENDING_ACTIVITY_STATE_STARTED,
                              PendingActivityState.PENDING_ACTIVITY_STATE_CANCEL_REQUESTED}:
                active.append((name, item.activity_type.name, item.activity_id))
            elif item.state != PendingActivityState.PENDING_ACTIVITY_STATE_SCHEDULED:
                raise ValueError('pending storage activity state is unknown')
    return active


def require_drained_activities(checkpoint, active):
    if (checkpoint and checkpoint.get('inflight_attempts')) or active:
        raise PublicationOwnerBusy('publication owner has not reached its handoff boundary: active activity or recovery')


@contextmanager
def publication_boundary(locks, deadline, *, on_wait=None):
    """Acquire the complete boundary without parking unrelated owners.

    If a later lock is busy, release all earlier locks before waiting on that
    one inode. Retry the complete set after its kernel wait finishes. Never
    retain the source/version owners throughout a long catalog export.
    """
    while True:
        blocked = None
        try:
            with ExitStack() as stack:
                if time.monotonic() >= deadline:
                    raise PublicationOwnerBusy('publication owner has not reached its handoff boundary')
                descriptors = {}
                for lock in locks:
                    if lock.is_symlink() or lock.parent.resolve() != lock.parent:
                        raise ValueError('storage owner lock is redirected')
                    handle = stack.enter_context(lock.open('a'))
                    try:
                        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    except BlockingIOError:
                        blocked = lock
                        raise
                    descriptors[lock] = handle.fileno()
                yield descriptors
                return
        except BlockingIOError:
            if blocked is None:
                raise  # A body error must not repeat a service-control action.
        # ExitStack has now released every partial acquisition. Wait only for
        # the busy lock, then close that descriptor before retrying all locks.
        if blocked.is_symlink() or blocked.parent.resolve() != blocked.parent:
            raise ValueError('storage owner lock is redirected')
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise PublicationOwnerBusy('publication owner has not reached its handoff boundary')
        if on_wait is not None:
            on_wait(blocked)
        interval = min(30, remaining)
        with blocked.open('a') as handle:
            result = subprocess.run(['flock', '--exclusive', '--wait', str(interval), str(handle.fileno())],
                                    pass_fds=(handle.fileno(),), capture_output=True, timeout=interval + 5)
            if result.returncode == 1:
                continue
            if result.returncode:
                raise RuntimeError('canonical owner acquisition failed')


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--deadline-seconds',type=int,default=600,
                        help='wait at most 1..86400 seconds for the existing owner boundary')
    parser.add_argument('--reload-code-only',action='store_true',help='reload the existing owner without changing source capacity policy')
    parser.add_argument('--handoff-status', type=Path,
                        help='refresh the original enrolled placement wait, separately from publication progress')
    parser.add_argument('--restart-active-owner',action='store_true',
                        help='explicitly authorized code-only restart; retain journals and requeue interrupted work')
    parser.add_argument('--after-publication-dataset',
                        help='wait for this original fixed source to commit before the authorized restart')
    parser.add_argument('--retained-source-candidate', type=Path,
                        help='adopt a reviewed fixed FRED binding after draining the original owners; retain all originals')
    parser.add_argument('--enroll-private-root-backups', action='store_true',
                        help='enroll only the seven verified relocated backup directories in private versioning')
    args = parser.parse_args(argv)
    before_path = args.output.with_name(args.output.stem + '-before.json')
    if (args.output.exists() or args.output.is_symlink() or before_path.exists() or before_path.is_symlink()
            or not 1 <= args.deadline_seconds <= 86400):
        parser.error('use a new receipt and a bounded handoff deadline')
    if args.restart_active_owner and not args.reload_code_only:
        parser.error('active-owner restart is restricted to an explicitly requested code-only reload')
    if args.after_publication_dataset and not (args.reload_code_only and args.restart_active_owner):
        parser.error('publication-boundary wait requires the original authorized code-only restart')
    if args.retained_source_candidate and not args.reload_code_only:
        parser.error('retained enrollment requires code-only reload and the original publication/collector boundary')
    if args.enroll_private_root_backups and (not args.reload_code_only or args.retained_source_candidate
                                             or args.restart_active_owner):
        parser.error('root backup enrollment requires a drained code-only reload without another cutover')
    original_code_identity=worker_code_identity()
    c=configuration();guard(c)
    if args.after_publication_dataset:
        wait_for_committed_publication(c['state_root'], args.after_publication_dataset,
            time.monotonic()+args.deadline_seconds, original_code_identity,
            args.output.with_name(args.output.stem+'-waiting.json'))
    cutover = prepare_retained_cutover(ROOT, c, args.retained_source_candidate) if args.retained_source_candidate else None
    root_backups = prepare_root_backup_enrollment(ROOT) if args.enroll_private_root_backups else None
    change = cutover or root_backups
    candidate_identity = (worker_code_identity(source_overrides={CATALOG_RELATIVE: change['catalog_updated']})
                          if change else original_code_identity)
    wait_status = None
    if args.handoff_status is not None:
        path = args.handoff_status
        if (not args.reload_code_only or path.is_symlink() or path.resolve() != path
                or path.parent != args.output.parent or path.name != 'worker-reload.json'):
            raise ValueError('worker handoff observation is outside its original placement owner')
        wait_status = json.loads(path.read_bytes())
        if (wait_status.get('contract') != 'stockagent.workspace_worker_handoff.v1'
                or wait_status.get('state') != 'waiting_for_original_owner'
                or wait_status.get('candidate_code_identity_sha256') != candidate_identity):
            raise ValueError('worker handoff observation belongs to another candidate')

    def observe_boundary_wait(lock):
        if wait_status is not None:
            atomic_write_json(args.handoff_status, {**wait_status,
                'observed_at_epoch': time.time(), 'waiting_owner_lock': str(lock),
                'publication_progress_renewed': False, 'original_active_owner_forced': args.restart_active_owner})
    policy=(c['source_replication'] if args.reload_code_only else
            json.loads((ROOT/'configs/data_sync/lake_source_replication.json').read_bytes()))
    source_wave_policy({**c,'source_replication':policy})
    ready=json.loads(RECEIVER_READINESS.read_bytes())
    if any(ready[k] != c[k] for k in ('producer_device_id','receiver_device_id')):
        raise ValueError('source policy capacity belongs to another paired receiver')
    reserve=64*1024**3
    if not args.reload_code_only and policy['maximum_retained_transport_bytes'] > ready['ingress_free_bytes']-reserve:
        raise ValueError('requested spool exceeds the reported receiver capacity reserve')
    private=PRIVATE_CONFIGURATION
    original=private.read_bytes()
    before=asyncio.run(workflows(c))
    pg_pid=pid('postgresql@18-main.service');server_pid=pid('stockagent-temporal.service')
    previous_pid=pid(OWNER)
    checkpoint=migration_checkpoint(c['state_root'])
    deadline=time.monotonic()+args.deadline_seconds
    stopped=False
    try:
        if args.restart_active_owner:
            # The operator explicitly allowed interruption. Persist intent
            # before stopping, keep PG/Temporal alive, and let systemd drain
            # then bound the old worker's lifetime. Never remove a lock or
            # unsealed staging directory. Its original owners reconcile them.
            atomic_write_json(before_path, {'state': 'authorized_restart_planned',
                'observed_at_utc': datetime.now(timezone.utc).isoformat(),
                'previous_pid': previous_pid, 'before': before,
                'candidate_code_identity_sha256': candidate_identity,
                'migration_checkpoint': checkpoint,
                'private_configuration_before_sha256': hashlib.sha256(original).hexdigest()}, durable=True)
            if worker_code_identity() != original_code_identity:
                raise ValueError('worker candidate changed before the authorized restart')
            stopped=True  # Even a stop timeout must leave finally able to restart.
            subprocess.run(['systemctl','stop',OWNER],check=True,timeout=110)
            if pid(OWNER) != '0':
                raise ValueError('original worker did not stop; retain all owners and journals')
        version_owner = Path(c['state_root'])/'dataset-versions'
        # The activity lease is acquired before any owner lock. Once loaded,
        # new activities cannot start their I/O while rollout owns it.
        locks = [Path(c['state_root'])/'activity-lease.lock']
        locks += ([version_owner/'owner.lock'] if version_owner.exists() else [])
        locks += [Path(c['state_root'])/name for name in
                  ('source-replication-owner.lock','catalog-owner.lock')]
        if cutover:
            locks.append(cutover['collector_lock'])
        cutover_result = None
        backup_result = None
        with publication_boundary(locks, deadline, on_wait=observe_boundary_wait) as descriptors:
            # Acquire every original boundary before stopping the worker.
            # A busy catalog cannot reserve the other owners while we wait.
            if worker_code_identity()!=original_code_identity:
                raise ValueError('worker candidate changed while waiting for the owner boundary')
            if not stopped:
                # Older workers do not yet use activity-lease.lock. Observe
                # persisted in-flight work and Temporal STARTED activities as
                # an additional fence before the first upgrade.
                require_drained_activities(migration_checkpoint(c['state_root']), asyncio.run(started_activities(c)))
                stopped=True
                subprocess.run(['systemctl','stop',OWNER],check=True,timeout=110)
            if worker_code_identity()!=original_code_identity:
                raise ValueError('worker candidate changed while stopping the old owner')
            verify_migration_checkpoint(checkpoint)
            if private.read_bytes() != original:
                raise ValueError('private policy changed during the owner handoff')
            rollback=Path(c['state_root'])/'policy-rollbacks'
            rollback.mkdir(mode=0o700,exist_ok=True)
            if rollback.is_symlink() or rollback.stat().st_mode & 0o077:
                raise ValueError('policy rollback directory is not private')
            digest=hashlib.sha256(original).hexdigest()
            backup=rollback/(digest+'.json')
            if not backup.exists():
                fd=os.open(backup,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
                with os.fdopen(fd,'wb') as stream:
                    stream.write(original);stream.flush();os.fsync(stream.fileno())
            elif backup.read_bytes()!=original:
                raise ValueError('private rollback identity conflicts')
            if not args.reload_code_only:
                updated=json.loads(original);updated['source_replication']=policy
                atomic_write_json(private,updated,durable=True)
                os.chmod(private,0o600)
            if cutover:
                cutover_result = commit_retained_cutover(cutover, descriptors[cutover['collector_lock']],
                    args.output.with_name(args.output.stem + '-source-cutover-intent.json'))
                if worker_code_identity() != candidate_identity:
                    raise ValueError('installed source catalog differs from the qualified worker closure')
            if root_backups:
                backup_result = commit_root_backup_enrollment(root_backups,
                    args.output.with_name(args.output.stem + '-root-backups-intent.json'))
                if worker_code_identity() != candidate_identity:
                    raise ValueError('installed private backup catalog differs from the qualified worker closure')
        subprocess.run(['systemctl','start',OWNER],check=True,timeout=30)
        started=time.monotonic()
        while True:
            worker=json.loads((Path(c['state_root'])/'worker-ready.json').read_bytes())
            if worker_code_identity()!=candidate_identity:
                raise ValueError('worker candidate changed during the owner handoff')
            if worker['code_identity_sha256']==candidate_identity and pid(OWNER) != previous_pid:
                break
            if time.monotonic()-started>60:
                raise TimeoutError('new worker has not proved its loaded code identity')
            time.sleep(1)
        after=asyncio.run(workflows(configuration()))
        if any(k not in after or after[k]['status']!='RUNNING' or after[k]['first_run_id']!=before[k]['first_run_id'] for k in before):
            raise ValueError('persistent workflow chain did not survive the owner handoff')
        if pid('postgresql@18-main.service')!=pg_pid or pid('stockagent-temporal.service')!=server_pid:
            raise ValueError('authority PG or Temporal server changed unexpectedly')
        preserved=verify_migration_checkpoint(checkpoint)
        result={'state':'accepted','observed_at_utc':datetime.now(timezone.utc).isoformat(),
                'reload_code_only':args.reload_code_only,
                'authorized_active_owner_restart':args.restart_active_owner,
                'interrupted_work_recovery_owner':'original_durable_journal_and_temporal_heartbeat',
                'verified_migration_releases_preserved':preserved,
                'publication_owner_boundary_verified':True,'previous_pid':previous_pid,'current_pid':pid(OWNER),
                'loaded_code_identity_sha256':worker['code_identity_sha256'],'source_policy':policy,
                'candidate_code_identity_sha256':candidate_identity,
                'private_configuration_before_sha256':hashlib.sha256(original).hexdigest(),
                'private_configuration_after_sha256':hashlib.sha256(private.read_bytes()).hexdigest(),
                'receiver_ingress_free_bytes':ready['ingress_free_bytes'],'before':before,'after':after,
                'workflow_chains_preserved':True,'postgres_and_temporal_server_not_restarted':True}
        if cutover_result:
            result['source_cutover'] = cutover_result
        if backup_result:
            result['root_backup_enrollment'] = backup_result
        atomic_write_json(args.output,result,durable=True)
        print(json.dumps({'state':result['state'],'current_pid':result['current_pid'],
                          'loaded_code_identity_sha256':result['loaded_code_identity_sha256']}))
    finally:
        if stopped:
            subprocess.run(['systemctl','start',OWNER],check=True,timeout=30)


if __name__=='__main__':
    main()
