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


def pid(unit):
    return subprocess.check_output(['systemctl','show',unit,'-p','MainPID','--value'],text=True).strip()


async def workflows(c):
    from temporalio.client import Client
    client = await Client.connect(c['temporal_endpoint'],namespace=c['temporal_namespace'])
    result = {}
    for name in IDS:
        d = await client.get_workflow_handle(name).describe()
        result[name] = {'run_id':d.run_id,'first_run_id':d.raw_description.workflow_execution_info.first_run_id,
                        'status':d.status.name}
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--deadline-seconds',type=int,default=600)
    args = parser.parse_args()
    if args.output.exists() or not 1 <= args.deadline_seconds <= 600:
        parser.error('use a new receipt and a bounded handoff deadline')
    c=configuration();guard(c)
    policy=json.loads((ROOT/'configs/data_sync/lake_source_replication.json').read_bytes())
    source_wave_policy({**c,'source_replication':policy})
    ready=json.loads(Path('/srv/stockagent-backup-receipts-lab203/readiness.json').read_bytes())
    if any(ready[k] != c[k] for k in ('producer_device_id','receiver_device_id')):
        raise ValueError('source policy capacity belongs to another paired receiver')
    reserve=64*1024**3
    if policy['maximum_retained_transport_bytes'] > ready['ingress_free_bytes']-reserve:
        raise ValueError('requested spool exceeds the reported receiver capacity reserve')
    private=Path('/etc/stockagent/lakehouse-control.json')
    original=private.read_bytes()
    before=asyncio.run(workflows(c))
    pg_pid=pid('postgresql@18-main.service');server_pid=pid('stockagent-temporal.service')
    previous_pid=pid(OWNER)
    deadline=time.monotonic()+args.deadline_seconds
    stopped=False
    try:
        while True:
            try:
                with ExitStack() as stack:
                    for name in ('source-replication-owner.lock','catalog-owner.lock'):
                        f=stack.enter_context((Path(c['state_root'])/name).open('a'))
                        fcntl.flock(f,fcntl.LOCK_EX|fcntl.LOCK_NB)
                    subprocess.run(['systemctl','stop',OWNER],check=True,timeout=110)
                    stopped=True
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
            if worker['code_identity_sha256']==worker_code_identity() and pid(OWNER) != previous_pid:
                break
            if time.monotonic()-started>60:
                raise TimeoutError('new worker has not proved its loaded code identity')
            time.sleep(1)
        after=asyncio.run(workflows(configuration()))
        if any(after[k]['status']!='RUNNING' or after[k]['first_run_id']!=before[k]['first_run_id'] for k in IDS):
            raise ValueError('persistent workflow chain did not survive the owner handoff')
        if pid('postgresql@18-main.service')!=pg_pid or pid('stockagent-temporal.service')!=server_pid:
            raise ValueError('authority PG or Temporal server changed unexpectedly')
        result={'state':'accepted','observed_at_utc':datetime.now(timezone.utc).isoformat(),
                'publication_owner_boundary_verified':True,'previous_pid':previous_pid,'current_pid':pid(OWNER),
                'loaded_code_identity_sha256':worker['code_identity_sha256'],'source_policy':policy,
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
