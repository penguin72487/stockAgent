#!/usr/bin/env python3
"""Real WSL/Windows transport fault injection; never connect TEJ or Excel."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess
import sys
import uuid

ROOT=Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:sys.path.insert(0,str(ROOT))

from downloader.artifact_io import atomic_write_json
from downloader.tej_history import DesktopBridge
from downloader import tej_windows_transport as transport


def run(root: Path, output: Path) -> dict:
    root,output=root.resolve(),output.resolve()
    if root.exists() or output.exists():raise FileExistsError('New isolated fixture root/output required')
    root.mkdir(mode=0o700,parents=True)
    checks=[]
    for mode in ('failed_first_relay','all_relays_failed','admitted_error','admitted_timeout'):
        request=root/'requests'/('e'*24+'-'+uuid.uuid4().hex+'.json')
        atomic_write_json(request,{'action':'owned_transport_fixture','mode':mode,'provider_queries_sent':0})
        marker=root/(mode+'.executed')
        command="[IO.File]::WriteAllText("+DesktopBridge.quote(DesktopBridge.windows_path(marker))+",'fixture executed once')"
        if mode=='admitted_error':command='exit 88'
        if mode=='admitted_timeout':command='Start-Sleep -Seconds 4;exit 0'
        original=transport.subprocess.Popen;launches=[];outcome=None
        def launch(argv, **kwargs):
            if argv[0]==transport.POWERSHELL:
                fail=mode=='all_relays_failed' or mode=='failed_first_relay' and not launches
                # WSL itself may fall back from an invalid WSL_INTEROP value.
                # A real worker exiting before ACK is a deterministic failure
                # at the exact same admission boundary, with no source action.
                if fail:argv=[*argv[:-1], 'exit 75']
                launches.append({'worker_exit_before_ready_injected':fail})
            return original(argv,**kwargs)
        transport.subprocess.Popen=launch
        try:
            try:
                result=transport.run_guarded_windows(command,request=request,windows_path=DesktopBridge.windows_path,
                    readiness_seconds=1 if mode=='admitted_timeout' else 5,
                    timeout=2 if mode=='admitted_timeout' else 30)
                outcome='completed' if result.returncode==0 else 'admitted_unknown'
            except transport.UnpermittedWindowsLaunch as exc:
                transport.validate_unpermitted(root,request,exc.evidence);outcome='proven_unpermitted'
            except subprocess.TimeoutExpired:outcome='admitted_timeout'
        finally:transport.subprocess.Popen=original
        permits=list((root/'launches').glob(request.stem+'-*.permit'))
        if mode=='failed_first_relay':
            accepted=len(launches)==2 and len(permits)==1 and marker.is_file() and outcome=='completed'
        elif mode=='all_relays_failed':
            accepted=1<=len(launches)<=2 and not permits and not marker.exists() and outcome=='proven_unpermitted'
        else:
            accepted=len(launches)==1 and len(permits)==1 and outcome==('admitted_timeout' if mode=='admitted_timeout' else 'admitted_unknown')
        checks.append({'name':mode,'accepted':accepted,'windows_launches':len(launches),
                       'durable_permits':len(permits),'outcome':outcome})
    receipt={'contract':'real_windows_no_provider_admission_fault_injection_v1',
        'accepted':all(check['accepted'] for check in checks),'checks':checks,'provider_queries_sent':0,
        'tej_or_excel_connected':False,'credentials_read':False,'source_values_exposed':False}
    atomic_write_json(output,receipt)
    if not receipt['accepted']:raise RuntimeError('Transport fixture incomplete; bounded evidence retained')
    return receipt


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args(argv)
    print(json.dumps(run(args.root,args.output)))


if __name__=='__main__':main()
