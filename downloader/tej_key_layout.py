"""Source-reported Key=1 snapshots / Key=3 records, not assumed dated series."""
from contextlib import closing
from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path
import uuid

from downloader.artifact_io import atomic_write_json

KEY3_CONTRACT = 'native_company_period_record_key3_v1'
KEY1_CONTRACT = 'native_company_observed_snapshot_key1_v1'
SNAPSHOT_DATE_AXIS = 'observed_source_snapshot_no_historical_date_claim'


def key_count(request: dict) -> int:
    mode = request.get('source_key_mode', 2)
    if type(mode) is not int:
        raise ValueError('Unverified source key layout contract')
    if mode == 2 and request.get('key_layout_contract') is None:
        return 2
    if mode == 3 and request.get('key_layout_contract') == KEY3_CONTRACT:
        return 3
    if mode == 1 and request.get('key_layout_contract') == KEY1_CONTRACT:
        return 1
    raise ValueError('Unverified source key layout contract')


def parquet_relative(task: dict, payload: dict) -> Path:
    base = Path('datasets') / f"v{payload['contract_version']}"
    if payload.get('key_layout_contract') in (KEY1_CONTRACT, KEY3_CONTRACT):
        base /= payload['key_layout_contract']
    from downloader.tej_period_keys import MONTH_CONTRACTS
    if payload.get('source_period_key_contract') in MONTH_CONTRACTS:
        base /= payload['source_period_key_contract']
    return base / task['table_id'] / (task['task_id'] + '.parquet')


def repair_source_key_plan(root: Path, task_id: str, bridge, *, only_mode: int | None = None) -> dict:
    """Replan an exact rejected/unsent scope from independently read source keys.

    No source query here. Completed nonempty old-grain rows are never silently
    migrated or double-counted. Old tasks/plan remain inspectable evidence.
    """
    from downloader.tej_history import connect, task_request, validate_download_evidence, SOURCE_SCOPE_CONTRACT
    from downloader.tej_planning import build_plan, refill_ready_plans, CONTRACT

    with closing(connect(root)) as con:
        if con.execute("SELECT 1 FROM tasks WHERE state='running' OR (state='blocked' AND last_error_code='unknown_outcome_no_auto_retry') LIMIT 1").fetchone():
            raise ValueError('Unknown/running desktop outcome blocks source-key migration')
        row = con.execute('SELECT * FROM tasks WHERE task_id=?', (task_id,)).fetchone()
        if (row is None or row['kind']!='download' or row['state']!='blocked'
                or row['last_error_code'] not in {'preview_column_limit_repartition_required','source_key_layout_replan_required'}):
            raise ValueError('Exact rejected or proved unsent key-layout task required')
        task = dict(row)
        definition = dict(con.execute('SELECT * FROM tables WHERE table_id=?', (task['table_id'],)).fetchone())
        stored = con.execute('SELECT * FROM download_plans WHERE table_id=?', (task['table_id'],)).fetchone()
        if stored is None or stored['contract']!=CONTRACT:
            raise ValueError('One compatible canonical lazy plan required')
        old = json.loads(stored['plan_json'])
        completed = [dict(r) for r in con.execute("SELECT * FROM tasks WHERE table_id=? AND kind='download' AND state='complete' AND scope_contract=?", (task['table_id'], SOURCE_SCOPE_CONTRACT))]
        if any(done['actual_rows'] != 0 for done in completed):
            # Nonempty observations cannot acquire a fabricated third key or
            # be relabelled as current snapshots by a layout migration.
            raise ValueError('Completed old-grain observations require a coverage-preserving migration first')
        config = json.loads(con.execute("SELECT value FROM meta WHERE key='config'").fetchone()[0])
    req = task_request(root, task)
    payload, output, _ = bridge.execute(root, {**task,'request_json':json.dumps({**req,'action':'inspect_query_runtime'})})
    if (payload.get('contract_version')!=4 or payload.get('provider')!='tej_smart_wizard'
            or payload.get('task_id')!=task_id or payload.get('action')!='inspect_query_runtime'
            or payload.get('source_key_mode') not in ((only_mode,) if only_mode else (1,3))
            or any(payload.get(k)!=req.get(k) for k in ('type','smart_id','table'))
            or any(payload.get(k) is not True for k in ('binding_matches_failed_plan','source_binding_stable',
                      'vendor_notices_absent','source_selectors_enabled','company_group_enabled'))
            or payload.get('source_key_mode')==3 and payload.get('date_group_enabled') is not True
            or any(payload.get(k) is not False for k in ('market_data_query_submitted','credentials_read','query_button_invoked'))):
        raise ValueError('Exact stable source-reported key-format readback required')
    mode=payload['source_key_mode']; contract=KEY1_CONTRACT if mode==1 else KEY3_CONTRACT
    request = {**old['request'],'source_key_mode':mode,'key_layout_contract':contract}
    if mode==1:
        request['frequency']='snapshot'
    empty_scopes=[]; empty_proofs=[]; weights=[]
    for done in completed:
        req_done=task_request(root,done)
        receipt_path=root/done['receipt_path']; raw=root/done['output_path']
        if (receipt_path.resolve().parent!=(root/'receipts').resolve()
                or raw.resolve().parent!=(root/'raw').resolve() or not raw.is_file()
                or receipt_path.stat().st_size>1024**2 or raw.stat().st_size>64*1024**2):
            raise ValueError('Exact bounded old empty-scope evidence required')
        receipt=json.loads(receipt_path.read_text(encoding='utf-8-sig'))
        payload=json.loads(raw.read_text(encoding='utf-8-sig'))
        _,rows,_=validate_download_evidence(req_done,payload)
        if (rows or receipt.get('source_outcome')!='explicit_empty_scope' or receipt.get('exported_rows')!=0
                or receipt.get('task_id')!=done['task_id'] or receipt.get('table_id')!=done['table_id']
                or receipt.get('requested_query_rows')!=done['expected_rows']
                or payload.get('task_id')!=done['task_id'] or receipt.get('raw_sha256')!=hashlib.sha256(raw.read_bytes()).hexdigest()):
            raise ValueError('Old empty query was not independently verified; no inferred emptiness')
        empty_proofs.append({'task_id':done['task_id'],'receipt_sha256':hashlib.sha256(receipt_path.read_bytes()).hexdigest(),
                             'raw_sha256':receipt['raw_sha256']})
        if mode==3:
            empty_scopes.append({**req_done,'source_key_mode':3,'key_layout_contract':KEY3_CONTRACT})
            # Count only whole new field partitions proved empty by this
            # old scope. Do not double-count a straddling partial partition.
            batches=sum(set(request['fields'][begin:begin+27])<=set(req_done['fields'])
                        for begin in range(0,len(request['fields']),27))
            weights.append((batches*done['expected_rows'],done['task_id']))
        else:
            # A dated historical empty query says nothing about today's
            # snapshot; preserve its receipt but not current snapshot coverage.
            weights.append((0,done['task_id']))
    plan = build_plan(request, old['companies'], [] if mode==1 else old['dates'], config, empty_scopes)
    if sum(weight for weight,_ in weights)!=plan['completed_work_rows']:
        raise ValueError('Overlapping empty scopes require explicit union-weight migration')
    audit_path = root/'key_layout_repairs'/(task_id+'-'+uuid.uuid4().hex+'.json')
    atomic_write_json(audit_path, {'contract':contract,'task_id':task_id,'table_id':task['table_id'],
        'observed_at_utc':datetime.now(UTC).isoformat(),'original_plan':old,
        'new_plan_fingerprint':plan['fingerprint'],'readback_path':str(output.relative_to(root)),
        'readback_sha256':hashlib.sha256(output.read_bytes()).hexdigest(),'source_rows_adopted':False,
        'empty_scope_proofs_preserved':empty_proofs,'completed_work_rows_preserved':plan['completed_work_rows'],
        'data_query_repeated':False,'source_files_deleted':False})
    with closing(connect(root)) as con, con:
        if dict(con.execute('SELECT * FROM tasks WHERE task_id=?',(task_id,)).fetchone()) != task:
            raise ValueError('Task changed during source-key readback')
        con.execute("UPDATE tasks SET state=? WHERE table_id=? AND kind='download' AND scope_contract=? AND state IN ('pending','blocked')", (f'superseded_key{mode}_layout_v1',task['table_id'], SOURCE_SCOPE_CONTRACT))
        con.execute('UPDATE download_plans SET plan_json=?,next_query=0,total_queries=? WHERE table_id=?',
                    (json.dumps(plan,ensure_ascii=False,separators=(',',':')),plan['total_queries'],task['table_id']))
        con.executemany('UPDATE tasks SET work_expected_rows=? WHERE task_id=?',weights)
        con.execute("UPDATE tables SET source_key_mode=?,work_grid_rows=?,field_batches=?,last_error_code=NULL,state='backfilling' WHERE table_id=?", (mode,plan['total_work_rows'],plan['field_batches'],task['table_id']))
        if mode==1:
            con.execute("UPDATE tables SET grid_rows=universe_count,grid_dates=0,frequency='snapshot',first_available_query_period=NULL,last_available_query_period=NULL,axis_profile_basis='native_key1_current_snapshot_no_history_axis' WHERE table_id=?",(task['table_id'],))
        refill_ready_plans(con)
    return {'state':'source_key_layout_replanned','source_key_mode':mode,'key_layout_contract':contract,
            'completed_work_rows_preserved':plan['completed_work_rows'],'verified_empty_scopes_preserved':len(empty_proofs),
            'max_fields_per_query':30-mode,'data_query_repeated':False,'source_rows_adopted':False}


def repair_key3_plan(root: Path, task_id: str, bridge) -> dict:
    """Compatibility entrypoint that still refuses any non-Key=3 migration."""
    return repair_source_key_plan(root,task_id,bridge,only_mode=3)
