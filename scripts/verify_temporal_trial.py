#!/usr/bin/env python3
"""Real Temporal timer/retry/worker-crash/server-restart trial under systemd.

Uses the pinned CLI's SQLite dev server on loopback and the canonical immutable
code verifier. Dev-server durability is not a production HA deployment proof.
"""
from __future__ import annotations

import argparse
import asyncio
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from downloader.artifact_io import atomic_write_json
from stockagent.runtime_identity import runtime_identity, verify_source_release


def command(argv):
    result = subprocess.run(argv,capture_output=True,text=True,timeout=20)
    if result.returncode:
        raise RuntimeError('trial supervisor operation failed: '+argv[0]+': '+result.stderr[-400:])
    return result.stdout


async def worker(config: Path):
    from temporalio.client import Client
    from temporalio.worker import Worker
    from stockagent.control.temporal_trial import ReleaseVerificationWorkflow,verify_recorded_release
    values = json.loads(config.read_bytes())
    client = await Client.connect(values['endpoint'])
    async with Worker(client,task_queue=values['queue'],workflows=[ReleaseVerificationWorkflow],
                      activities=[verify_recorded_release]):
        Path(values['ready']).write_text('ready\n')
        await asyncio.Event().wait()


async def connect(endpoint: str, timeout=60):
    from temporalio.client import Client
    deadline=time.monotonic()+timeout
    while time.monotonic()<deadline:
        try:
            client=await Client.connect(endpoint)
            await client.service_client.check_health()
            return client
        except Exception:
            await asyncio.sleep(.2)
    raise RuntimeError('owned Temporal dev server did not become ready')


async def reach_state(handle,state: str,process,timeout=60):
    from stockagent.control.temporal_trial import ReleaseVerificationWorkflow
    deadline=time.monotonic()+timeout
    while time.monotonic()<deadline:
        if process.poll() is not None:
            raise RuntimeError('owned Temporal worker exited; inspect its private log')
        try:
            if await asyncio.wait_for(handle.query(ReleaseVerificationWorkflow.state),3)==state:
                return
        except (asyncio.TimeoutError,Exception):
            pass
        await asyncio.sleep(.2)
    raise RuntimeError('workflow did not reach '+state)


async def trial(args):
    from temporalio.client import Client
    from stockagent.control.temporal_trial import ReleaseVerificationWorkflow
    started=time.perf_counter()
    output=args.output.absolute();output.mkdir(parents=True,mode=0o700,exist_ok=False)
    cli=args.cli.resolve(strict=True)
    release=verify_source_release(args.receipt,args.source_root)
    identity=runtime_identity()
    with socket.socket() as sock:
        sock.bind(('127.0.0.1',0));port=sock.getsockname()[1]
    endpoint='127.0.0.1:'+str(port)
    suffix=uuid.uuid4().hex[:12];unit='stockagent-temporal-trial-'+suffix
    queue='release-verifier-'+suffix
    trace=output/'activity-receipts';trace.mkdir(mode=0o700)
    events=[];process=None;logs=[]
    def event(name,**values):
        events.append({'event':name,'elapsed_seconds':time.perf_counter()-started,**values})
        atomic_write_json(output/'events.json',events)
        print(json.dumps(events[-1]),flush=True)
    def start_worker(number):
        config=output/f'worker-{number}.json'
        ready=output/f'worker-{number}.ready'
        atomic_write_json(config,{'endpoint':endpoint,'queue':queue,'ready':str(ready)})
        log=(output/f'worker-{number}.log').open('wb');logs.append(log)
        return subprocess.Popen([sys.executable,str(Path(__file__).resolve()),'--worker',str(config)],
                                 stdout=log,stderr=subprocess.STDOUT)
    try:
        command(['systemd-run','--unit',unit,'--property=Slice=stockagent-control.slice',
                 '--property=RuntimeMaxSec=900','--property=Nice=10','--property=CPUQuota=200%',
                 '--property=MemoryMax=512M','--property=TasksMax=128',
                 '--property=Restart=on-failure','--property=RestartSec=1',
                 '--',str(cli),'server','start-dev','--db-filename',str(output/'temporal.sqlite'),
                 '--headless','--ip','127.0.0.1','--port',str(port),'--log-level','warn'])
        client=await connect(endpoint)
        before=command(['systemctl','show',unit,'-p','MainPID','-p','NRestarts','-p','MemoryCurrent'])
        event('server_ready',unit=unit,supervisor=before)
        process=start_worker(1)
        request={'receipt':str(args.receipt),'source_root':str(args.source_root),'trace_root':str(trace),
                 **{key:release[key] for key in ('receipt_sha256','source_sha256')}}
        handle=await client.start_workflow(ReleaseVerificationWorkflow.run,request,
                                          id='release-trial-'+suffix,task_queue=queue)
        await reach_state(handle,'waiting_for_resume',process)
        first_receipts={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in trace.iterdir()}
        event('first_stage_and_durable_timer_completed',activity_receipts=first_receipts)
        process.kill();process.wait(timeout=10)
        event('worker_killed',exit_code=process.returncode)
        command(['systemctl','kill','--kill-whom=main','--signal=KILL',unit])
        client=await connect(endpoint)
        after=command(['systemctl','show',unit,'-p','MainPID','-p','NRestarts','-p','MemoryCurrent'])
        before_props=dict(line.split('=',1) for line in before.splitlines())
        after_props=dict(line.split('=',1) for line in after.splitlines())
        if int(after_props['NRestarts'])<1 or after_props['MainPID']==before_props['MainPID']:
            raise ValueError('systemd did not actually restart the crashed dev server')
        event('systemd_server_crash_recovered',supervisor=after)
        process=start_worker(2)
        handle=client.get_workflow_handle(handle.id)
        await reach_state(handle,'waiting_for_resume',process)
        if first_receipts!={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in trace.iterdir()}:
            raise ValueError('replay re-executed an already completed activity')
        event('new_worker_replayed_completed_history')
        await handle.signal(ReleaseVerificationWorkflow.resume)
        result=await asyncio.wait_for(handle.result(),60)
        if result['first']!=release or result['second']!=release:
            raise ValueError('durable workflow result differs from canonical source proof')
        history=await handle.fetch_history()
        (output/'workflow-history.json').write_text(history.to_json())
        receipt_rows=[json.loads(p.read_bytes()) for p in sorted(trace.glob('*.json'))]
        states=[(p['stage'],p['attempt'],p['state']) for p in receipt_rows]
        if states!=[('first',1,'injected_transient_failure'),('first',2,'succeeded'),('second',1,'succeeded')]:
            raise ValueError('retry or restored-stage activity coverage differs')
        proof={'state':'accepted','runtime':identity,'cli_sha256':hashlib.sha256(cli.read_bytes()).hexdigest(),
               'source_proof':release,'workflow_id':handle.id,'history_events':len(history.events),
               'history_sha256':hashlib.sha256((output/'workflow-history.json').read_bytes()).hexdigest(),
               'activity_receipts':receipt_rows,'real_worker_crash_recovered':True,
               'real_server_crash_recovered_by_systemd':True,'durable_timer_verified':True,
               'completed_activity_not_reexecuted_on_replay':True,'transient_retry_verified':True,
               'complete_wall_seconds':time.perf_counter()-started,
               'scope':'one-node dev-server SQLite durability and actual verifier workflow; production HA, remote worker, provider/GPU/order side effects not migrated'}
        atomic_write_json(output/'acceptance.json',proof)
        event('accepted',history_events=len(history.events))
        return proof
    finally:
        if process is not None and process.poll() is None:
            process.terminate();process.wait(timeout=10)
        for log in logs:log.close()
        command(['systemctl','stop',unit])
        journal=command(['journalctl','-u',unit,'--no-pager','-n','100'])
        (output/'server-journal.log').write_text(journal)
        atomic_write_json(output/'cleanup.json',{'owned_server_unit':unit,'owned_server_stopped':True,
                                                'worker_stopped':True,'database_history_preserved':True})


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cli',type=Path)
    parser.add_argument('--receipt',type=Path)
    parser.add_argument('--source-root',type=Path)
    parser.add_argument('--output',type=Path)
    parser.add_argument('--worker',type=Path)
    args=parser.parse_args()
    if args.worker:
        asyncio.run(worker(args.worker));return
    if not all((args.cli,args.receipt,args.source_root,args.output)):
        parser.error('cli, receipt, source-root and output are required')
    args.receipt=args.receipt.resolve(strict=True);args.source_root=args.source_root.resolve(strict=True)
    try:
        asyncio.run(trial(args))
    except Exception as error:
        if args.output.is_dir():
            atomic_write_json(args.output/'failure.json',{'state':'failed','error_type':type(error).__name__,
                                                        'reason':str(error)[:1024]})
        raise


if __name__=='__main__':
    main()
