"""Replan a proved oversized Preview, never retry an unknown source action.

The native record key makes company/period pairs an inadequate row estimate.
An overflow descriptor contains no adopted values. It must bind to the exact
fresh attempt before the existing lazy plan can be replaced transactionally.
Original attempts, requests, plans and traffic evidence remain private/intact.
"""
from contextlib import closing
from datetime import UTC, datetime
import hashlib
import json
import math
from pathlib import Path
import re

from downloader.artifact_io import atomic_write_json
from downloader.tej_planning import CAPACITY_CONTRACT, CONTRACT, build_plan, next_request

OUTCOME = 'verified_preview_capacity_exceeded'
REVIEW_STATE = 'source_capacity_requires_review'
SUPERSEDED = 'superseded_capacity_v1'


def _digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def validate_capacity_response(root: Path, task: dict, request: dict,
                               payload: dict, output: Path) -> dict:
    from downloader.tej_desktop_attempts import query_stage, validate_active_attempt
    from downloader.tej_history import PREVIEW_SUBMISSION_CONTRACT, validate_source_scope
    from downloader.tej_key_layout import KEY3_CONTRACT

    output = output.resolve()
    if (output.parent != (root/'raw').resolve() or not output.name.startswith(task['task_id']+'-')
            or output.suffix != '.json' or not output.is_file() or output.stat().st_size > 2*1024**2
            or json.loads(output.read_text(encoding='utf-8-sig')) != payload):
        raise ValueError('Exact private capacity descriptor required')
    validate_source_scope(request,payload,'download')
    if (task['kind'] != 'download' or not task.get('active_attempt_id')
            or payload.get('task_id') != task['task_id'] or payload.get('contract_version') != 4
            or payload.get('source_outcome') != OUTCOME or payload.get('capacity_contract') != CAPACITY_CONTRACT
            or request.get('source_key_mode') != 3 or request.get('key_layout_contract') != KEY3_CONTRACT
            or any(payload.get(k) != request.get(k) for k in
                   ('source_key_mode','key_layout_contract','company_labels','date_labels'))
            or any(payload.get(k) is not False for k in
                   ('market_data_query_repeated','source_rows_adopted','query_comments_read'))
            or any(k in payload for k in ('cells','rows','bounded_preview','source_record_keys'))):
        raise ValueError('Unverified native-record overflow scope')
    attempt = task['active_attempt_id']
    prepared = root/'requests'/(attempt+'.json')
    stage, stage_path = query_stage(root,task,request,prepared)
    validate_active_attempt(root,task,payload)
    signature = payload.get('preview_signature')
    before = stage.get('before_preview_signatures')
    if (payload.get('preview_submission_contract') != PREVIEW_SUBMISSION_CONTRACT
            or not isinstance(signature,str) or not re.fullmatch('[0-9a-fA-F]{64}',signature)
            or not isinstance(before,list) or signature in before):
        raise ValueError('Fresh exact-attempt capacity transition required')
    lower, upper, columns = (payload.get(k) for k in
                            ('native_rows_lower_bound','native_rows_upper_bound','native_columns'))
    max_rows,max_cells = request.get('max_rows'),request.get('max_cells')
    if (any(type(n) is not int or not 1 <= n <= 1000000 for n in (lower,upper,columns,max_rows,max_cells))
            or lower > upper or columns != len(request['fields'])+3
            or payload.get('requested_capacity') != {'max_rows':max_rows,'max_cells':max_cells}
            # At most one header/new-row each. Cells include the header, not
            # the optional new-row affordance. Counts are never observations.
            or not (lower > max_rows+2 or (lower-1)*columns > max_cells)):
        raise ValueError('Native capacity bound was not proved exceeded')
    if (not request['company_labels'] or not request['date_labels']
            or any(len(v)!=len(set(v)) for v in (request['company_labels'],request['date_labels']))):
        raise ValueError('Unique nonempty capacity query axes required')
    return {'prepared_request_path':str(prepared.relative_to(root)),
            'prepared_request_sha256':_digest(prepared),
            'query_stage_path':str(stage_path.relative_to(root)),
            'query_stage_sha256':_digest(stage_path),
            'capacity_response_path':str(output.relative_to(root.resolve())),
            'capacity_response_sha256':_digest(output)}


def replan_capacity(root: Path, task_id: str, payload: dict, output: Path,
                    *, event_id: str | None = None) -> dict:
    """Caller holds .download.lock. This function sends ZERO provider queries.

    Only an exact proved overflow is superseded, not claimed complete/unsent.
    Later smaller queries incur normal recorded source use. A density hint is
    a geometry estimate with headroom, never a guarantee or observed row count.
    An atomic company/period overflow stays durably blocked for review.
    """
    from downloader.tej_history import SOURCE_SCOPE_CONTRACT, connect, expand_request, stable_id
    from downloader.tej_key_layout import KEY3_CONTRACT, key_count, verified_empty_scope

    observed = datetime.now(UTC).isoformat()
    with closing(connect(root)) as con,con:
        row = con.execute('SELECT * FROM tasks WHERE task_id=?',(task_id,)).fetchone()
        if row is None:
            raise ValueError('Unknown overflow task')
        task = dict(row)
        definition = dict(con.execute('SELECT * FROM tables WHERE table_id=?',(task['table_id'],)).fetchone())
        request = expand_request(json.loads(task['request_json']),definition,task['table_id'])
        proof = validate_capacity_response(root,task,request,payload,output)
        plan_row = con.execute('SELECT * FROM download_plans WHERE table_id=?',(task['table_id'],)).fetchone()
        if plan_row is None or plan_row['contract'] != CONTRACT:
            raise ValueError('Existing canonical lazy plan required')
        former_row = dict(plan_row)
        former = json.loads(plan_row['plan_json'])
        next_request(former,0)  # Validate immutable original fingerprint.
        audit_path = root/'capacity_replanning'/(task['active_attempt_id']+'.json')
        if task['state'] in (SUPERSEDED,'blocked') and task['last_error_code'] in (OUTCOME,REVIEW_STATE) and audit_path.is_file():
            audit = json.loads(audit_path.read_text())
            if (audit.get('evidence') != proof or audit.get('task_id') != task_id
                    or audit.get('new_plan_fingerprint') != former['fingerprint']):
                raise ValueError('Committed capacity reconciliation differs from current evidence/plan')
            if audit['state'] != 'committed':
                audit['state'] = 'committed'
                atomic_write_json(audit_path,audit,durable=True)
            return {**audit['result'],'already_reconciled':True}
        if (task['state'] not in ('running','blocked') or task['scope_contract'] != SOURCE_SCOPE_CONTRACT
                or task['state']=='blocked' and task['last_error_code'] not in ('unknown_outcome_no_auto_retry','source_validation_failed')
                or con.execute("SELECT 1 FROM tasks WHERE task_id!=? AND (state='running' OR "
                               "(state='blocked' AND last_error_code='unknown_outcome_no_auto_retry')) LIMIT 1",(task_id,)).fetchone()
                or con.execute("SELECT 1 FROM meta WHERE key IN ('desktop_interface_recovery_required','source_period_replan_required')").fetchone()):
            raise ValueError('Unresolved foreign source action or capacity task changed')
        config = json.loads(con.execute("SELECT value FROM meta WHERE key='config'").fetchone()[0])
        runtime = con.execute("SELECT value FROM meta WHERE key='runtime_policy'").fetchone()
        enabled = runtime is not None and json.loads(runtime[0]).get('auto_preview_capacity_replanning') is True
        if (former['request'].get('source_key_mode')!=3 or former['request'].get('key_layout_contract')!=KEY3_CONTRACT
                or request.get('field_partition',{}).get('plan_fingerprint')!=former['fingerprint']
                or stable_id([task['table_id'],request])!=task_id
                or not set(request['company_labels']) <= set(former['companies'])
                or not set(request['date_labels']) <= set(former['dates'])
                or request['max_rows']!=former['request']['max_rows']
                or request['max_cells']!=former['request']['max_cells']):
            raise ValueError('Overflow is outside the exact original plan/capacity')
        pending = list(con.execute("SELECT * FROM tasks WHERE table_id=? AND kind='download' AND "
                                   "state='pending' AND scope_contract=?",(task['table_id'],SOURCE_SCOPE_CONTRACT)))
        pending_requests = [expand_request(json.loads(r['request_json']),definition,task['table_id']) for r in pending]
        if any(req.get('field_partition',{}).get('plan_fingerprint')!=former['fingerprint']
               or stable_id([task['table_id'],req])!=row['task_id'] for row,req in zip(pending,pending_requests)):
            raise ValueError('Pending native scope outside the exact original plan')
        completed = list(con.execute("SELECT * FROM tasks WHERE table_id=? AND kind='download' AND "
                                     "state='complete' AND scope_contract=?",(task['table_id'],SOURCE_SCOPE_CONTRACT)))
        if task['actual_rows'] is not None or task['receipt_path'] is not None or task['completed_at_utc'] is not None:
            raise ValueError('Capacity response cannot supersede adopted source observations')
        pairs = len(request['company_labels'])*len(request['date_labels'])
        hint = max(former['request'].get('native_record_density_hint',1)*2,
                   math.ceil(2*payload['native_rows_upper_bound']/pairs))
        review = not enabled or pairs == 1
        plan = former
        inherited_empty_proofs = []
        if not review:
            base = {**former['request'],'native_record_density_hint':hint,
                    'record_capacity_contract':CAPACITY_CONTRACT}
            # A hint cannot justify exceeding local bounds, even for one pair.
            columns = min(30,len(base['fields'])+3)
            hint_cap = min(base['max_rows'],base['max_cells']//columns-1)
            hint = min(hint,hint_cap)
            if hint <= former['request'].get('native_record_density_hint',1):
                review = True
            else:
                base['native_record_density_hint'] = hint
                done = []
                for row in completed:
                    completed_request = expand_request(json.loads(row['request_json']),definition,task['table_id'])
                    if key_count(completed_request)!=3:
                        completed_request,empty_proof = verified_empty_scope(root,dict(row))
                        inherited_empty_proofs.append(empty_proof)
                        completed_request = {**completed_request,'source_key_mode':3,'key_layout_contract':KEY3_CONTRACT}
                    done.append(completed_request)
                plan = build_plan(base,former['companies'],former['dates'],config,done)
                weights = sum(r['work_expected_rows'] if r['work_expected_rows'] is not None else r['expected_rows'] for r in completed)
                if (plan['total_work_rows']!=former['total_work_rows']
                        or plan['completed_work_rows']!=weights
                        or plan['remaining_work_rows']!=plan['total_work_rows']-weights):
                    raise ValueError('Capacity replan changed or overlaps completed source coverage')
        result = {'state':REVIEW_STATE if review else 'source_capacity_replanned',
                  'provider_queries_sent':0,'data_query_repeated':False,
                  'source_rows_adopted':False,'original_attempt_retained':True,
                  'completed_scopes_preserved':True,'native_record_density_hint':hint,
                  'remaining_queries':plan['total_queries']-plan_row['next_query'] if review else plan['total_queries'],
                  'audit':str(audit_path.relative_to(root))}
        backup_path = audit_path.with_suffix('.original_plan.json')
        if not backup_path.exists():
            atomic_write_json(backup_path,former_row,durable=True)
        elif json.loads(backup_path.read_text())!=former_row:
            raise ValueError('Original capacity plan backup differs')
        audit = {'contract':CAPACITY_CONTRACT,'state':'prepared','task_id':task_id,
                 'query_attempt_id':task['active_attempt_id'],'observed_at_utc':observed,
                 'evidence':proof,'original_plan_backup':str(backup_path.relative_to(root)),
                 'original_plan_backup_sha256':_digest(backup_path),
                 'old_plan_fingerprint':former['fingerprint'],'new_plan_fingerprint':plan['fingerprint'],
                 'completed_work_rows_preserved':plan['completed_work_rows'],
                 'inherited_empty_key_layout_coverage':inherited_empty_proofs,
                 'original_outcome':'proved_overflow_not_complete_not_claimed_unsent',
                 'replacement_queries_use_normal_provider_quota':True,
                 'native_dimensions_are_not_observed_record_counts':True,'result':result}
        atomic_write_json(audit_path,audit,durable=True)
        if not review:
            con.executemany("UPDATE tasks SET state=? WHERE task_id=? AND state='pending'",
                            [(SUPERSEDED,r['task_id']) for r in pending])
            con.execute('UPDATE download_plans SET plan_json=?,next_query=0,total_queries=? WHERE table_id=?',
                        (json.dumps(plan,ensure_ascii=False,separators=(',',':')),plan['total_queries'],task['table_id']))
        con.execute('UPDATE tasks SET state=?,last_error_code=?,output_path=? WHERE task_id=?',
                    ('blocked' if review else SUPERSEDED,REVIEW_STATE if review else OUTCOME,
                     str(output.resolve().relative_to(root.resolve())),task_id))
        con.execute("UPDATE desktop_attempts SET state='observed_capacity_exceeded',finished_at_utc=? WHERE attempt_id=?",
                    (observed,task['active_attempt_id']))
        if review:
            con.execute("UPDATE tables SET state='needs_review',last_error_code=? WHERE table_id=?",(REVIEW_STATE,task['table_id']))
        elif not con.execute("SELECT 1 FROM tasks WHERE table_id=? AND state='blocked' LIMIT 1",(task['table_id'],)).fetchone():
            con.execute("UPDATE tables SET state='backfilling',last_error_code=NULL WHERE table_id=?",(task['table_id'],))
        if event_id:
            con.execute("UPDATE traffic SET state='capacity_replanned',completed_at_utc=? WHERE event_id=?",(observed,event_id))
    audit['state'] = 'committed'
    try:
        atomic_write_json(audit_path,audit,durable=True)
    except OSError:
        # Intent and original backup are already durable; the DB commit is
        # authoritative. A later reconciliation finishes this marker. Do not
        # relabel the superseded attempt unknown and corrupt committed state.
        result = {**result,'audit_commit_marker_pending':True}
    return result


def replan_restarted_unknown(root: Path, task_id: str, prepared: Path, recovery_run: Path,
                             *, record_density_prior: int) -> dict:
    """Explicit operator geometry change, NOT a proved native-row overflow.

    The old process was stopped and a fresh interface verified by the caller.
    Reuse the canonical lazy planner with a labelled prior, retaining the old
    unknown attempt and excluding every already downloaded field/grid scope.
    This sends no query and never supplies an invented native record count.
    """
    from downloader.tej_history import SOURCE_SCOPE_CONTRACT, connect, expand_request, stable_id, task_request
    from downloader.tej_key_layout import KEY3_CONTRACT, key_count, verified_empty_scope
    from downloader.tej_planning import OPERATOR_GEOMETRY_CONTRACT
    from downloader.tej_query_lifecycle import RESTART_CONTRACT, _unknown_download

    if type(record_density_prior) is not int or not 2 <= record_density_prior <= 10000:
        raise ValueError('Explicit bounded native-record density prior required')
    task=_unknown_download(root,task_id,prepared);request=task_request(root,task)
    run=recovery_run.resolve()
    if (run.parent!=(root/'query_lifecycle').resolve() or not re.fullmatch('[0-9a-f]{32}',run.name)
            or any((run/(phase+'.json')).stat().st_size>16384 for phase in ('inspect-addin','stop-addin','open'))):
        raise ValueError('Exact private completed restart required')
    proofs={phase:json.loads((run/(phase+'.json')).read_text(encoding='utf-8-sig'))
            for phase in ('inspect-addin','stop-addin','open')}
    if (any(p.get('contract')!=RESTART_CONTRACT or p.get('action')!=phase
            or p.get('addin_process_restart_authorized') is not True
            or p.get('discard_scratch_query_settings_authorized') is not True
            or any(p.get(k) is not False for k in ('market_data_query_submitted','workbook_closed','workbook_saved','credentials_read'))
            for phase,p in proofs.items())
            or proofs['stop-addin'].get('addin_stopped_verified') is not True
            or proofs['open'].get('query_open_verified') is not True
            or request.get('source_key_mode')!=3 or request.get('key_layout_contract')!=KEY3_CONTRACT):
        raise ValueError('Verified operator restart of native-record query required')
    original_session=json.loads((run/'original_session.json').read_text(encoding='utf-8-sig'))
    if (any(p.get('old_query_window')!=original_session.get('ExpectedWindow') for p in proofs.values())
            or any(proofs[phase].get('process_id')!=original_session.get('TejProcessId')
                   for phase in ('inspect-addin','stop-addin'))
            or proofs['open'].get('new_session')!=json.loads((root/'desktop_session.json').read_text())
            or proofs['open']['new_session'].get('TejProcessId')==original_session.get('TejProcessId')
            or proofs['inspect-addin'].get('image_sha256')!=proofs['stop-addin'].get('image_sha256')
            or proofs['inspect-addin'].get('image_sha256')!=proofs['open'].get('image_sha256')):
        raise ValueError('Restart scope/process/session differs from operator geometry')
    clocks=[datetime.fromisoformat(value.replace('Z','+00:00')) for value in
            [task['attempted_at_utc'],*[proofs[phase]['observed_at_utc'] for phase in ('inspect-addin','stop-addin','open')]]]
    if any(clock.tzinfo is None for clock in clocks) or clocks!=sorted(clocks):
        raise ValueError('Fresh ordered restart evidence required for unknown query')
    audit_path=root/'operator_replanning'/(task['active_attempt_id']+'.json')
    with closing(connect(root)) as con,con:
        if dict(con.execute('SELECT * FROM tasks WHERE task_id=?',(task_id,)).fetchone())!=task:
            raise ValueError('Unknown task changed before operator replan')
        if con.execute("SELECT 1 FROM meta WHERE key IN ('desktop_interface_recovery_required','source_period_replan_required')").fetchone():
            raise ValueError('Restarted shared interface not verified')
        definition=dict(con.execute('SELECT * FROM tables WHERE table_id=?',(task['table_id'],)).fetchone())
        row=con.execute('SELECT * FROM download_plans WHERE table_id=?',(task['table_id'],)).fetchone()
        if row is None or row['contract']!=CONTRACT:
            raise ValueError('Canonical native-record lazy plan required')
        former_row=dict(row);former=json.loads(row['plan_json']);next_request(former,0)
        if (stable_id([task['table_id'],request])!=task_id
                or request.get('field_partition',{}).get('plan_fingerprint')!=former['fingerprint']
                or record_density_prior<=former['request'].get('native_record_density_hint',1)):
            raise ValueError('Exact original plan and strictly smaller native-record query required')
        pending=list(con.execute("SELECT * FROM tasks WHERE table_id=? AND kind='download' AND state='pending' AND scope_contract=?",
                                 (task['table_id'],SOURCE_SCOPE_CONTRACT)))
        for pending_row in pending:
            query=expand_request(json.loads(pending_row['request_json']),definition,task['table_id'])
            if (query.get('field_partition',{}).get('plan_fingerprint')!=former['fingerprint']
                    or stable_id([task['table_id'],query])!=pending_row['task_id']):
                raise ValueError('Pending scope outside original operator plan')
        if con.execute("SELECT 1 FROM tasks WHERE table_id=? AND task_id!=? AND state IN ('running','blocked') LIMIT 1",
                       (task['table_id'],task_id)).fetchone():
            raise ValueError('Other unresolved scopes block operator table replan')
        completed=list(con.execute("SELECT * FROM tasks WHERE table_id=? AND kind='download' AND state='complete' AND scope_contract=?",
                                   (task['table_id'],SOURCE_SCOPE_CONTRACT)))
        done=[];inherited=[]
        for completed_row in completed:
            query=expand_request(json.loads(completed_row['request_json']),definition,task['table_id'])
            if key_count(query)!=3:
                query,empty_proof=verified_empty_scope(root,dict(completed_row));inherited.append(empty_proof)
                query={**query,'source_key_mode':3,'key_layout_contract':KEY3_CONTRACT}
            done.append(query)
        base={**former['request'],'native_record_density_hint':record_density_prior,
              'record_capacity_contract':OPERATOR_GEOMETRY_CONTRACT}
        config=json.loads(con.execute("SELECT value FROM meta WHERE key='config'").fetchone()[0])
        plan=build_plan(base,former['companies'],former['dates'],config,done)
        weights=sum(r['work_expected_rows'] if r['work_expected_rows'] is not None else r['expected_rows'] for r in completed)
        if (plan['total_work_rows']!=former['total_work_rows'] or plan['completed_work_rows']!=weights
                or plan['remaining_work_rows']!=plan['total_work_rows']-weights):
            raise ValueError('Operator geometry changed or overlaps downloaded coverage')
        backup=audit_path.with_suffix('.original_plan.json')
        if backup.exists() or audit_path.exists():
            raise ValueError('Operator geometry evidence already exists; no repeat')
        atomic_write_json(backup,former_row,durable=True)
        result={'state':'operator_unknown_geometry_replanned','provider_queries_sent':0,
            'original_unknown_evidence_retained':True,'completed_scopes_preserved':True,
            'source_rows_adopted':False,'data_query_repeated':False,'record_density_prior':record_density_prior,
            'remaining_queries':plan['total_queries'],'audit':str(audit_path.relative_to(root))}
        audit={'contract':OPERATOR_GEOMETRY_CONTRACT,'state':'prepared','task_id':task_id,
            'observed_at_utc':datetime.now(UTC).isoformat(),'original_attempt_id':task['active_attempt_id'],
            'original_outcome':'unknown_retained_not_claimed_unsent',
            'density_basis':'operator_geometry_prior_not_observed_native_row_count',
            'original_plan_backup':str(backup.relative_to(root)),'original_plan_backup_sha256':_digest(backup),
            'old_plan_fingerprint':former['fingerprint'],'new_plan_fingerprint':plan['fingerprint'],
            'prepared_request_sha256':_digest(prepared),
            'query_stage_sha256':_digest(root/'raw'/(task['active_attempt_id']+'.json.stage.json')),
            'restart_evidence_sha256':{phase:_digest(run/(phase+'.json')) for phase in proofs},
            'completed_work_rows_preserved':weights,'inherited_empty_key_layout_coverage':inherited,
            'automatic_retry':False,'replacement_queries_use_normal_provider_quota':True,
            'source_files_deleted':False,'result':result}
        atomic_write_json(audit_path,audit,durable=True)
        con.execute("UPDATE tasks SET state='superseded_operator_unknown_geometry_v1' WHERE task_id=?",(task_id,))
        con.executemany("UPDATE tasks SET state='superseded_operator_unknown_geometry_v1' WHERE task_id=? AND state='pending'",
                        [(r['task_id'],) for r in pending])
        con.execute('UPDATE download_plans SET plan_json=?,next_query=0,total_queries=? WHERE table_id=?',
                    (json.dumps(plan,ensure_ascii=False,separators=(',',':')),plan['total_queries'],task['table_id']))
        con.execute("UPDATE tables SET state='backfilling',last_error_code=NULL WHERE table_id=?",(task['table_id'],))
    audit['state']='committed'
    try:
        atomic_write_json(audit_path,audit,durable=True)
    except OSError:
        result={**result,'audit_commit_marker_pending':True}
    return result
