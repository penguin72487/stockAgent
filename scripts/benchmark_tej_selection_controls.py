#!/usr/bin/env python3
"""Bounded canonical ownership/lock launcher for no-query operation profiling."""
from __future__ import annotations

from contextlib import closing
import argparse
import json
from pathlib import Path
import subprocess
import sys
import uuid

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0,str(ROOT))

from downloader.artifact_io import atomic_write_bytes, atomic_write_json
from downloader.dataset_lock import exclusive_dataset_lock
from downloader.tej_history import DesktopBridge, connect, task_request
from scripts.benchmark_tej_preview_readback import completed_source


def run(root: Path, baseline: Path, output: Path, task_id: str | None) -> dict:
    root, baseline, output = root.resolve(), baseline.resolve(), output.resolve()
    if output.exists() or not baseline.is_file() or baseline.stat().st_size > 1024**2:
        raise ValueError('New output and bounded preserved original bridge required')
    with exclusive_dataset_lock(root/'.download.lock',provider='tej_no_query_control_benchmark',timeout_seconds=0):
        with closing(connect(root)) as con:
            row=(con.execute('SELECT * FROM tasks WHERE task_id=?',(task_id,)).fetchone() if task_id else
                 con.execute("SELECT * FROM tasks WHERE kind='download' AND state='complete' ORDER BY completed_at_utc DESC LIMIT 1").fetchone())
            if row is None:raise ValueError('Exact completed original source required')
            task = dict(row);task_id=task['task_id']
        prepared, _ = completed_source(root, task_id, allow_empty=True)
        session = root/'desktop_session.json'
        request = {**task_request(root,task), 'action':'inspect_query_runtime'}
        inspected, _, _ = DesktopBridge(ROOT,json.loads(session.read_text())).execute(
            root,{**task, 'request_json':json.dumps(request)})
        if (inspected.get('binding_matches_failed_plan') is not True or inspected.get('source_binding_unchanged') is not True
                or inspected.get('market_data_query_submitted') is not False):
            raise ValueError('Original completed source binding must remain stable')
        output.parent.mkdir(mode=0o700,parents=True,exist_ok=True)
        quote, windows_path = DesktopBridge.quote, DesktopBridge.windows_path
        command = ("$ErrorActionPreference='Stop';$s="+quote(windows_path(ROOT/'scripts/benchmark_tej_selection_controls.ps1'))+
            ";$d=Join-Path ([Environment]::GetFolderPath('LocalApplicationData')) ('StockAgent\\TEJSmartWizard\\controls-'+[Guid]::NewGuid().ToString('N'));"
            "[void](New-Item -ItemType Directory -Path $d);$p=Join-Path $d 'controls.ps1';Copy-Item -LiteralPath $s -Destination $p;"
            "if((Get-FileHash -LiteralPath $s).Hash -cne (Get-FileHash -LiteralPath $p).Hash){throw 'Script copy mismatch'};& $p "+
            ' '.join('-'+key+' '+quote(windows_path(path)) for key,path in {
                'BridgeScript':ROOT/'scripts/tej_smart_wizard_bridge.ps1','BaselineScript':baseline,
                'Session':session,'Prepared':prepared,'Output':output}.items()))
        from downloader.tej_windows_transport import run_guarded_windows
        from downloader.tej_startup import interactive_transport
        relay=interactive_transport(root)
        launch=root/'requests'/('f'*24+'-'+uuid.uuid4().hex+'.json')
        atomic_write_json(launch, {'action':'readonly_same_owned_control_benchmark','provider_queries_sent':0})
        result=run_guarded_windows(command,request=launch,windows_path=windows_path,timeout=120,
            windows_session_id=relay['windows_session_id'],interop_socket=relay['interop_socket'])
        if result.returncode or not output.is_file():
            atomic_write_bytes(root/'diagnostics'/('control-benchmark-'+output.stem+'.txt'),result.stderr[:32768])
            raise RuntimeError('Read-only control benchmark failed; private evidence retained')
        receipt = json.loads(output.read_text(encoding='utf-8-sig'))
        if (receipt.get('accepted') is not True or receipt.get('provider_queries_sent') != 0
                or receipt.get('ui_operations_sent') != 0 or receipt.get('source_values_exposed') is not False
                or receipt.get('queue_modified') is not False):
            raise ValueError('Incomplete source operation benchmark acceptance')
        return receipt


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',type=Path,default=ROOT/'data_tej')
    parser.add_argument('--baseline',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    binding=parser.add_mutually_exclusive_group(required=True)
    binding.add_argument('--task-id')
    binding.add_argument('--last-completed-source',action='store_true',help='Bind the last exact receipt under lock; never change UI source')
    args=parser.parse_args(argv)
    receipt=run(args.root,args.baseline,args.output,args.task_id)
    print(json.dumps({key:receipt[key] for key in ('accepted','samples','checks','provider_queries_sent','queue_modified')}))


if __name__ == '__main__':main()
