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
sys.path.insert(0,str(REPO))
from downloader.artifact_io import atomic_write_json
from downloader.tej_history import SOURCE_SCOPE_CONTRACT
from downloader.tej_metadata_recovery import (
    RECONCILIATION_CONTRACT, failed_plan_proof, uncommitted_plan_failure_proof,
)
from downloader.tej_startup import interactive_transport
from downloader.tej_operation_clock import CONTRACT as OPERATION_CLOCK_CONTRACT
from scripts.audit_tej_history import audit


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
    parser.add_argument('--baseline',type=Path,help='Preserved original metadata failure evidence to verify unchanged')
    parser.add_argument('--reference-values-acceptance',type=Path,
        help='For an entirely explicit-empty cohort, retain a prior nonempty value audit with IDENTICAL capture/ingest code')
    args=parser.parse_args(argv)
    output=args.output.resolve(); root=args.root.resolve()
    if output.exists():
        raise FileExistsError('Preserve original acceptance evidence')
    with closing(sqlite3.connect(f'file:{root/"queue.sqlite3"}?mode=ro',uri=True,timeout=2)) as con:
        con.row_factory=sqlite3.Row
        blocked=[dict(r) for r in con.execute("SELECT * FROM tasks WHERE state='blocked'")]
        unfinished=[dict(r) for r in con.execute("SELECT * FROM tasks WHERE state='running' AND kind='discover'")]
        recent=[dict(r) for r in con.execute("SELECT * FROM tasks WHERE state='complete' AND kind='download' "
            "AND scope_contract=? AND timing_basis='fresh_end_to_end' ORDER BY completed_at_utc DESC LIMIT 128",(SOURCE_SCOPE_CONTRACT,))]
        retries=[dict(r) for r in con.execute('SELECT table_id,task_id,error_code,consecutive_failures,next_attempt_at_utc FROM desktop_retry_windows')]
    protected={}; metadata=[]
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
    result={'contract':'tej_metadata_recovery_production_acceptance_v1','observed_at_utc':datetime.now(UTC).isoformat(),
            'mode':'snapshot' if not args.since else 'production_acceptance',
            'failure_commit_reconciliation_contract':RECONCILIATION_CONTRACT,
            'operation_clock_contract':OPERATION_CLOCK_CONTRACT,
            'original_failure_artifacts':protected,'metadata_failures':metadata,
            'blocked_tasks':len(blocked),'retry_windows':retries,'provider_queries_sent':0,
            'all_history_complete':False,'cold_boot_verified':False,
            'code_sha256':{str(p.relative_to(REPO)):digest(p) for p in (
                REPO/'downloader/tej_history.py',REPO/'downloader/tej_scheduler.py',
                REPO/'downloader/tej_metadata_recovery.py',REPO/'scripts/tej_smart_wizard_bridge.ps1',
                REPO/'downloader/tej_operation_clock.py',REPO/'downloader/tej_desktop_attempts.py',
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
            raise ValueError('Original failed metadata proof required in baseline')
        for relative,expected in baseline['original_failure_artifacts'].items():
            path=root/relative
            if path.resolve().parent not in {(root/name).resolve() for name in ('requests','raw','launches','diagnostics')} or digest(path)!=expected:
                raise ValueError('Original failure evidence changed')
        audited=audit(root,output.parent/(output.stem+'-receipts'),task_ids=[t['task_id'] for t in selected],verify_original_cells=True)
        if not audited['accepted'] or not empty_cohort and not audited['original_source_cells_verified']:
            raise ValueError('Canonical receipt and original-source-cell audit failed')
        if empty_cohort and audited['original_source_cells_checked']!=0:
            raise ValueError('Explicit-empty cohort cannot claim nonempty stored source values')
        with urlopen('http://127.0.0.1:8770/tej/api/status',timeout=15) as response:
            body=response.read(4*1024**2+1)
        if len(body)>4*1024**2:
            raise ValueError('Bounded public status required')
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
