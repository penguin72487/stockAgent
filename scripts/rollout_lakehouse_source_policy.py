#!/usr/bin/env python3
"""Apply the local source policy at the existing publication-owner boundary."""
import argparse
import asyncio
from contextlib import ExitStack
from datetime import datetime, timezone
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


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--deadline-seconds',type=int,default=600,
                        help='wait at most 1..86400 seconds for the existing owner boundary')
    parser.add_argument('--reload-code-only',action='store_true',help='reload the existing owner without changing source capacity policy')
    parser.add_argument('--restart-active-owner',action='store_true',
                        help='explicitly authorized code-only restart; retain journals and requeue interrupted work')
    args = parser.parse_args(argv)
    before_path = args.output.with_name(args.output.stem + '-before.json')
    if (args.output.exists() or args.output.is_symlink() or before_path.exists() or before_path.is_symlink()
            or not 1 <= args.deadline_seconds <= 86400):
        parser.error('use a new receipt and a bounded handoff deadline')
    if args.restart_active_owner and not args.reload_code_only:
        parser.error('active-owner restart is restricted to an explicitly requested code-only reload')
    candidate_identity=worker_code_identity()
    c=configuration();guard(c)
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
            if worker_code_identity() != candidate_identity:
                raise ValueError('worker candidate changed before the authorized restart')
            stopped=True  # Even a stop timeout must leave finally able to restart.
            subprocess.run(['systemctl','stop',OWNER],check=True,timeout=110)
            if pid(OWNER) != '0':
                raise ValueError('original worker did not stop; retain all owners and journals')
        while True:
            try:
                with ExitStack() as stack:
                    # Stop the optional version activity at its own durable
                    # source boundary BEFORE reserving NAS/catalog controls.
                    # Default: never stop a worker writing a private head.
                    # Authorized restart: acquire these same inode locks only
                    # AFTER the previous worker has fully stopped.
                    version_owner = Path(c['state_root'])/'dataset-versions'
                    locks = ([version_owner/'owner.lock'] if version_owner.exists() else [])
                    locks += [Path(c['state_root'])/name for name in
                              ('source-replication-owner.lock','catalog-owner.lock')]
                    for lock in locks:
                        if lock.is_symlink() or lock.parent.resolve() != lock.parent:
                            raise ValueError('storage owner lock is redirected')
                        f=stack.enter_context(lock.open('a'))
                        remaining=max(1,int(deadline-time.monotonic()))
                        acquired=subprocess.run(['flock','--exclusive','--wait',str(remaining),str(f.fileno())],
                                                pass_fds=(f.fileno(),),capture_output=True,timeout=remaining+5)
                        if acquired.returncode==1:
                            raise TimeoutError('publication owner has not reached its handoff boundary')
                        if acquired.returncode:
                            raise RuntimeError('canonical owner acquisition failed')
                    if worker_code_identity()!=candidate_identity:
                        raise ValueError('worker candidate changed while waiting for the owner boundary')
                    if not stopped:
                        stopped=True
                        subprocess.run(['systemctl','stop',OWNER],check=True,timeout=110)
                    if worker_code_identity()!=candidate_identity:
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
                break
            except BlockingIOError:
                if time.monotonic()>=deadline:
                    raise TimeoutError('publication owner has not reached its handoff boundary')
                time.sleep(5)
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
        atomic_write_json(args.output,result,durable=True)
        print(json.dumps({'state':result['state'],'current_pid':result['current_pid'],
                          'loaded_code_identity_sha256':result['loaded_code_identity_sha256']}))
    finally:
        if stopped:
            subprocess.run(['systemctl','start',OWNER],check=True,timeout=30)


if __name__=='__main__':
    main()
