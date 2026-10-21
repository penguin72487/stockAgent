"""Read-only production acceptance for the canonical TEJ recovery deployment."""
from __future__ import annotations

import argparse
from contextlib import closing
from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
from urllib.request import urlopen

REPO = Path(__file__).resolve().parents[1]
OWNER_STATUS_URL = 'http://127.0.0.1:8771/tej/api/status'
sys.path.insert(0,str(REPO))
from downloader.artifact_io import atomic_write_json
from downloader.tej_history import SOURCE_SCOPE_CONTRACT
from downloader.tej_metadata_recovery import (
    RECONCILIATION_CONTRACT, failed_plan_proof, uncommitted_plan_failure_proof,
)
from downloader.tej_startup import interactive_transport
from downloader.tej_operation_clock import CONTRACT as OPERATION_CLOCK_CONTRACT
from downloader.tej_desktop_attempts import (
    ANNOTATION_RECONCILIATION_CONTRACT, deferred_unsent_annotation_proof,
    interrupted_stage_write_proof,
)
from scripts.audit_tej_history import audit


def validate_boot_tasks(windows: dict, launcher_sha256: str) -> None:
    """Registration and current relay proof are NOT a physical boot test."""
    expected = {'StockAgent TEJ WSL Startup':('S4U',True,{'MSFT_TaskBootTrigger'}),
                'StockAgent TEJ Desktop Recovery':('Interactive',False,{'MSFT_TaskLogonTrigger','MSFT_TaskTimeTrigger'})}
    tasks = windows.get('tasks')
    if not isinstance(tasks,list) or len(tasks) != len(expected):
        raise ValueError('Both exact boot and interactive task readbacks required')
    seen = set()
    for task in tasks:
        name = task.get('name')
        if name not in expected or name in seen:
            raise ValueError('Unique exact TEJ startup task required')
        seen.add(name)
        logon,service_only,triggers = expected[name]
        actual = task.get('triggers',[])
        if (task.get('enabled') is not True or task.get('logon_type') != logon
                or task.get('service_only') is not service_only
                or task.get('launcher_sha256') != launcher_sha256
                or task.get('executor_verified') is not True
                or task.get('multiple_instances') != 'IgnoreNew'
                or task.get('restart_count') != 3 or task.get('restart_interval') != 'PT1M'
                or len(actual) != len(triggers) or {t.get('type') for t in actual} != triggers
                or any(t.get('enabled') is not True for t in actual)
                or any(t.get('interval') != 'PT1M' for t in actual if t.get('type') == 'MSFT_TaskTimeTrigger')):
            raise ValueError('Installed TEJ startup policy or pinned launcher differs')


def boot_readback(root: Path) -> dict:
    """No GUI, restart, credential or provider API access; bounded OS readback."""
    import uuid
    from downloader.tej_history import DesktopBridge
    from downloader.tej_windows_transport import run_guarded_windows
    from downloader.tej_operation_clock import source_operation_clock
    relay = interactive_transport(root)
    launcher_sha = digest(REPO/'scripts/start_windows_tej.ps1')
    request = root/'requests'/('0'*24+'-'+uuid.uuid4().hex+'.json')
    atomic_write_json(request,{'action':'readonly_tej_boot_registration_readback','provider_queries_sent':0})
    command = r"""$ErrorActionPreference='Stop';[Console]::OutputEncoding=[Text.UTF8Encoding]::new($false);
$tasks=@(foreach($name in @('StockAgent TEJ WSL Startup','StockAgent TEJ Desktop Recovery')) {
 $t=Get-ScheduledTask -TaskName $name -TaskPath '\' -ErrorAction Stop;
 $info=Get-ScheduledTaskInfo -TaskName $name -TaskPath '\' -ErrorAction Stop;
 if(@($t.Actions).Count -ne 1){throw 'Exact single startup action required'};
 $taskArguments=[string]$t.Actions[0].Arguments;
 $m=[regex]::Match($taskArguments,'(?:^| )-File "([^"]+)"');
 if(-not $m.Success){throw 'Pinned startup launcher required'};
 $p=[IO.Path]::GetFullPath($m.Groups[1].Value);
 $base=Join-Path ([Environment]::GetFolderPath('LocalApplicationData')) 'StockAgent\TEJStartup';
 if([IO.Path]::GetDirectoryName($p) -cne $base -or
    [IO.Path]::GetFileName($p) -cnotmatch '^start-[0-9a-f]{64}\.ps1$'){throw 'Owned startup launcher path required'};
 @{name=$name;state=[string]$t.State;enabled=[bool]$t.Settings.Enabled;
   logon_type=[string]$t.Principal.LogonType;last_run_utc=$info.LastRunTime.ToUniversalTime().ToString('o');
   last_task_result=[long]$info.LastTaskResult;restart_count=[int]$t.Settings.RestartCount;
   restart_interval=[string]$t.Settings.RestartInterval;multiple_instances=[string]$t.Settings.MultipleInstances;
   executor_verified=([IO.Path]::GetFullPath($t.Actions[0].Execute) -ieq (Join-Path $env:SystemRoot 'System32\WindowsPowerShell\v1.0\powershell.exe'));
   launcher_sha256=(Get-FileHash -LiteralPath $p).Hash.ToLowerInvariant();service_only=($taskArguments -cmatch ' -StartServiceOnly$');
   triggers=@($t.Triggers|ForEach-Object{@{type=$_.CimClass.CimClassName;enabled=[bool]$_.Enabled;interval=[string]$_.Repetition.Interval}})}
});
@{observed_at_utc=[DateTime]::UtcNow.ToString('o');windows_boot_at_utc=(Get-CimInstance Win32_OperatingSystem).LastBootUpTime.ToUniversalTime().ToString('o');
 windows_session_id=[Diagnostics.Process]::GetCurrentProcess().SessionId;tasks=$tasks}|ConvertTo-Json -Depth 6 -Compress
"""
    started = datetime.now(UTC)
    result = run_guarded_windows(command,request=request,windows_path=DesktopBridge.windows_path,timeout=45,
        windows_session_id=relay['windows_session_id'],interop_socket=relay['interop_socket'])
    if result.returncode or len(result.stdout) > 16384:
        raise ValueError('Bounded Windows boot task readback failed')
    windows = json.loads(result.stdout.decode('utf-8-sig'))
    source_operation_clock(windows.get('observed_at_utc'),started,datetime.now(UTC))
    validate_boot_tasks(windows,launcher_sha)
    if windows.get('windows_session_id') != relay['windows_session_id']:
        raise ValueError('Original interactive relay session changed')
    interactive_transport(root)
    services = {}
    for name in ('stockagent-tej-history.service','stockagent-tej-api-trial.timer'):
        text = subprocess.check_output(['systemctl','show',name,'-p','ActiveState','-p','SubState','-p','UnitFileState'],text=True,timeout=10)
        fields = dict(line.split('=',1) for line in text.splitlines() if '=' in line)
        if fields.get('ActiveState') != 'active' or fields.get('UnitFileState') != 'enabled':
            raise ValueError('Enabled active canonical boot owners required')
        services[name] = fields
    return {'contract':'tej_current_boot_registration_and_live_relay_readback_v1',
        'observed_at_utc':datetime.now(UTC).isoformat(),'accepted':True,'mode':'boot_registration_readback',
        'provider_queries_sent':0,'desktop_actions_sent':0,'physical_reboot_performed':False,
        'current_code_cold_boot_verified':False,'linux_boot_id':Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
        'windows':windows,'services':services,'interactive_relay_live':True,
        'requires_windows_user_logon':True,'autologin_changed':False}


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def verified_value_reference(path: Path, current_code: dict) -> dict:
    """A valid empty result proves progress, not nonempty value equivalence."""
    reference=json.loads(path.read_text())
    if (reference.get('accepted') is not True or type(reference.get('source_rows')) is not int or reference['source_rows']<=0
            or type(reference.get('original_cells_checked')) is not int or reference['original_cells_checked']<=0
            or any(reference.get('code_sha256',{}).get(key)!=current_code.get(key) for key in (
                'scripts/tej_smart_wizard_bridge.ps1','downloader/tej_history.py'))):
        raise ValueError('Prior nonempty original-value audit must match unchanged capture and ingest code')
    return {'acceptance_path':str(path),'acceptance_sha256':digest(path),
            'source_rows':reference['source_rows'],'original_cells_checked':reference['original_cells_checked'],
            'basis':'prior_nonempty_audit_of_unchanged_capture_and_ingest_not_current_empty_values'}


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',type=Path,default=REPO/'data_tej')
    parser.add_argument('--output',type=Path,required=True)
    mode=parser.add_mutually_exclusive_group()
    mode.add_argument('--since',help='Require eight new full-lifecycle downloads STARTED after this UTC clock')
    mode.add_argument('--values-reference-task',help='Independently audit one retained nonempty receipt with the current validation code; not a new download')
    mode.add_argument('--boot-readback',action='store_true',help='Verify exact installed boot/logon tasks and live relay; not a physical reboot')
    parser.add_argument('--baseline',type=Path,help='Preserved original source failure evidence to verify unchanged')
    parser.add_argument('--reference-values-acceptance',type=Path,
        help='For an entirely explicit-empty cohort, retain a prior nonempty value audit with IDENTICAL capture/ingest code')
    args=parser.parse_args(argv)
    output=args.output.resolve(); root=args.root.resolve()
    if output.exists():
        raise FileExistsError('Preserve original acceptance evidence')
    if args.boot_readback:
        if args.baseline is not None or args.reference_values_acceptance is not None:
            raise ValueError('Boot registration readback is separate from acquisition acceptance')
        result = boot_readback(root)
        atomic_write_json(output,result,durable=True)
        print(json.dumps({k:result[k] for k in ('mode','accepted','current_code_cold_boot_verified')}))
        return 0
    with closing(sqlite3.connect(f'file:{root/"queue.sqlite3"}?mode=ro',uri=True,timeout=2)) as con:
        con.row_factory=sqlite3.Row
        blocked=[dict(r) for r in con.execute("SELECT * FROM tasks WHERE state='blocked'")]
        unfinished=[dict(r) for r in con.execute("SELECT * FROM tasks WHERE state='running' AND kind='discover'")]
        recent=[dict(r) for r in con.execute("SELECT * FROM tasks WHERE state='complete' AND kind='download' "
            "AND scope_contract=? AND timing_basis='fresh_end_to_end' ORDER BY completed_at_utc DESC LIMIT 128",(SOURCE_SCOPE_CONTRACT,))]
        retries=[dict(r) for r in con.execute('SELECT table_id,task_id,error_code,consecutive_failures,next_attempt_at_utc FROM desktop_retry_windows')]
    protected={}; metadata=[]; prequery=[]; interrupted=[]
    for task in [*blocked, *unfinished]:
        if task['kind']!='discover':
            continue
        try:
            proof=(uncommitted_plan_failure_proof(root,task) if task['state']=='running'
                   else failed_plan_proof(root,task))
            original=proof['prepared']; name=original.stem
            paths=[original,root/'raw'/(original.name+'.outcome.json'),root/'launches'/(name+'.bridge.json')]
            if 'failure_diagnostic_path' in proof:
                paths.append(root/proof['failure_diagnostic_path'])
            protected.update({str(p.relative_to(root)):digest(p) for p in paths})
            metadata.append({'task_id':task['task_id'],'negative_outcome_verified':True,
                             'attempted_at_utc':task['attempted_at_utc'],'error_code':task['last_error_code'],
                             'failure_commit_unfinished':task['state']=='running'})
        except (ValueError,OSError,KeyError,TypeError):
            metadata.append({'task_id':task['task_id'],'negative_outcome_verified':False,
                             'attempted_at_utc':task['attempted_at_utc'],'error_code':task['last_error_code']})
    with closing(sqlite3.connect(f'file:{root/"queue.sqlite3"}?mode=ro',uri=True,timeout=2)) as con:
        con.row_factory=sqlite3.Row
        for task in blocked:
            if task['kind'] != 'download':
                continue
            if task['last_error_code'] == 'unknown_outcome_no_auto_retry':
                from downloader.tej_history import task_request
                try:
                    attempt=task['active_attempt_id'];prepared=root/'requests'/(attempt+'.json')
                    proof=interrupted_stage_write_proof(root,task,task_request(root,task),prepared)
                    manifest=root/'launches'/(attempt+'.bridge.json')
                    paths=[prepared,manifest,root/proof['original_stage_path'],root/proof['original_diagnostic_path']]
                    progress=root/'progress'/(attempt+'.json.progress.json')
                    if progress.is_file():paths.append(progress)
                    protected.update({str(p.relative_to(root)):digest(p) for p in paths})
                    interrupted.append({'task_id':task['task_id'],'query_attempt_id':attempt,
                        'original_outcome':'unknown_retained_not_claimed_unsent',**proof})
                except (OSError,ValueError,KeyError,TypeError):
                    pass
            try:
                proof=deferred_unsent_annotation_proof(root,con,task)
                if proof is None:
                    continue
                paths=[proof['prepared_path'],proof['outcome_path'],proof['annotation_path'],proof['bridge_manifest_path']]
                diagnostic=root/'diagnostics'/(proof['attempt_id']+'.txt')
                if diagnostic.is_file():
                    paths.append(diagnostic)
                protected.update({str(p.relative_to(root)):digest(p) for p in paths})
                prequery.append({'task_id':task['task_id'],'negative_outcome_verified':True,
                    'attempt_annotation_unfinished':True,'original_finished_at_utc':proof['finished_at_utc'],
                    'error_code':task['last_error_code']})
            except (ValueError,OSError,KeyError,TypeError):
                prequery.append({'task_id':task['task_id'],'negative_outcome_verified':False,
                    'error_code':task['last_error_code']})
    result={'contract':'tej_source_recovery_production_acceptance_v2','observed_at_utc':datetime.now(UTC).isoformat(),
            'mode':'snapshot' if not args.since else 'production_acceptance',
            'failure_commit_reconciliation_contract':RECONCILIATION_CONTRACT,
            'operation_clock_contract':OPERATION_CLOCK_CONTRACT,
            'attempt_annotation_reconciliation_contract':ANNOTATION_RECONCILIATION_CONTRACT,
            'original_failure_artifacts':protected,'metadata_failures':metadata,'prequery_failures':prequery,
            'interrupted_stage_writes':interrupted,
            'blocked_tasks':len(blocked),'retry_windows':retries,'provider_queries_sent':0,
            'all_history_complete':False,'cold_boot_verified':False,
            'code_sha256':{str(p.relative_to(REPO)):digest(p) for p in (
                REPO/'downloader/tej_history.py',REPO/'downloader/tej_scheduler.py',
                REPO/'downloader/tej_metadata_recovery.py',REPO/'scripts/tej_smart_wizard_bridge.ps1',
                REPO/'downloader/tej_operation_clock.py',REPO/'downloader/tej_desktop_attempts.py',
                REPO/'downloader/tej_planning.py',REPO/'downloader/tej_startup.py',
                REPO/'scripts/start_tej_desktop.ps1',
                REPO/'configs/tej_history.json')},
            'latest_download':{k:recent[0][k] for k in ('task_id','completed_at_utc','actual_rows')} if recent else None}
    if args.values_reference_task:
        if args.baseline is not None or args.reference_values_acceptance is not None:
            raise ValueError('Retained value reference is a separate finite local audit')
        audited=audit(root,output.parent/(output.stem+'-receipts'),
                      task_ids=[args.values_reference_task],verify_original_cells=True)
        if not audited['accepted'] or not audited['original_source_cells_verified']:
            raise ValueError('Retained reference requires nonempty source-cell verification')
        result.update(mode='retained_nonempty_value_reference',accepted=True,
            reference_scope='existing_receipt_revalidated_with_current_code_not_a_new_download',
            reference_task_id=args.values_reference_task,receipts_audited=1,
            source_rows=audited['exported_rows'],original_cells_checked=audited['original_source_cells_checked'],
            source_non_null_cells=audited['exported_non_null_cells'])
    if args.since:
        since=datetime.fromisoformat(args.since.replace('Z','+00:00'))
        if since.tzinfo is None:
            raise ValueError('Aware deployment clock required')
        selected=[t for t in recent if datetime.fromisoformat(t['attempted_at_utc'])>=since][:8]
        if len(selected)!=8:
            raise ValueError('Await eight new receipts')
        empty_cohort=not any(t['actual_rows']>0 for t in selected)
        if empty_cohort:
            if args.reference_values_acceptance is None:
                raise ValueError('Nonempty value audit required; explicit-empty progress is separate evidence')
            reference=verified_value_reference(args.reference_values_acceptance,result['code_sha256'])
            for task in selected:
                receipt=json.loads((root/'receipts'/(task['task_id']+'.json')).read_text())
                raw=(root/receipt['raw_path']).resolve()
                if raw.parent!=(root/'raw').resolve() or json.loads(raw.read_text(encoding='utf-8-sig')).get('source_outcome')!='explicit_empty_scope':
                    raise ValueError('Each current zero-row response must be explicitly source-confirmed empty')
            result['nonempty_value_reference']=reference
        result['current_cohort_explicit_empty']=empty_cohort
        if args.baseline is None:
            raise ValueError('Preserved failure baseline required')
        baseline=json.loads(args.baseline.read_text())
        if not baseline['original_failure_artifacts']:
            raise ValueError('Original failed source proof required in baseline')
        for relative,expected in baseline['original_failure_artifacts'].items():
            path=root/relative
            if path.resolve().parent not in {(root/name).resolve() for name in ('requests','raw','launches','diagnostics','progress','orphaned_attempts')} or digest(path)!=expected:
                raise ValueError('Original failure evidence changed')
        audited=audit(root,output.parent/(output.stem+'-receipts'),task_ids=[t['task_id'] for t in selected],verify_original_cells=True)
        if not audited['accepted'] or not empty_cohort and not audited['original_source_cells_verified']:
            raise ValueError('Canonical receipt and original-source-cell audit failed')
        if empty_cohort and audited['original_source_cells_checked']!=0:
            raise ValueError('Explicit-empty cohort cannot claim nonempty stored source values')
        # Acquisition telemetry is owner-only. The public 8770 gateway may
        # correctly deny this route; that is not a failed TEJ collector.
        with urlopen(OWNER_STATUS_URL,timeout=15) as response:
            body=response.read(4*1024**2+1)
        if len(body)>4*1024**2:
            raise ValueError('Bounded owner-only status required')
        status=json.loads(body)
        if (status.get('schema_version')!=16 or status.get('read_only') is not True
                or status.get('raw_values_exposed') is not False or status['scheduler'].get('alive') is not True
                or status['scheduler'].get('paused_reason') is not None
                or status['scheduler'].get('metadata_recovery',{}).get('enabled') is not True):
            raise ValueError('Live safe dashboard progress and installed recovery policy required')
        interactive_transport(root)  # Independent birth-pinned Windows relay check.
        service=subprocess.check_output(['systemctl','show','stockagent-tej-history.service','-p','ActiveState','-p','SubState'],text=True,timeout=10)
        if 'ActiveState=active' not in service or 'SubState=running' not in service:
            raise ValueError('Canonical service not running')
        result.update(accepted=True,since_utc=since.isoformat(),baseline_sha256=digest(args.baseline),
            original_failure_evidence_preserved=True,receipts_audited=8,
            source_rows=audited['exported_rows'],source_non_null_cells=audited['exported_non_null_cells'],
            original_cells_checked=audited['original_source_cells_checked'],
            dashboard_state=status['state'],scheduler_state=status['scheduler']['state'])
    atomic_write_json(output,result,durable=True)
    print(json.dumps({k:result[k] for k in ('mode','observed_at_utc','blocked_tasks')}
                     | ({k:result[k] for k in ('accepted','receipts_audited','source_rows','original_cells_checked')}
                        if args.since or args.values_reference_task else {})))
    return 0


if __name__=='__main__':
    raise SystemExit(main())
