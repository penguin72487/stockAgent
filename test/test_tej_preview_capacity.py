"""Known overflow recovery must never become unknown-query replay or fake rows."""
from collections import Counter
from contextlib import closing
import hashlib
import json
import uuid

import pytest

from downloader.artifact_io import atomic_write_json
from downloader.tej_desktop_attempts import begin_attempt
from downloader.tej_history import (PREVIEW_SUBMISSION_CONTRACT, SOURCE_SCOPE_CONTRACT, compact_request, stable_id, configure_preview_planning,
    configure_runtime_policy, connect, recover_desktop_response, run_one, task_request)
from downloader.tej_key_layout import KEY3_CONTRACT
from downloader.tej_planning import CAPACITY_CONTRACT, build_plan, next_request, refill_ready_plans
from downloader.tej_preview_capacity import OUTCOME, REVIEW_STATE, SUPERSEDED, replan_capacity
from test_tej_history import scope_proof
from test_tej_key_layout import Key3Bridge
from test_tej_planning import axes, base_request, configuration, registry as registry


def overflow(root,task,*,upper=100):
    req=task_request(root,task)
    attempt=task['task_id']+'-'+uuid.uuid4().hex
    prepared=root/'requests'/(attempt+'.json');output=root/'raw'/(attempt+'.json')
    atomic_write_json(prepared,{**req,'task_id':task['task_id'],'query_attempt_id':attempt})
    begin_attempt(root,task,attempt,prepared,output)
    stage={**{k:req[k] for k in ('type','smart_id','table','fields','company_labels','date_labels')},
           'contract_version':4,'task_id':task['task_id'],'stage':'prepreview_verified',
           'source_scope_proof':scope_proof(),'query_attempt_id':attempt,
           'preview_submission_contract':PREVIEW_SUBMISSION_CONTRACT,'before_preview_signatures':['a'*64]}
    atomic_write_json(root/'raw'/(attempt+'.json.stage.json'),stage)
    payload={**{k:req[k] for k in ('contract_version','type','smart_id','table','fields',
                 'company_labels','date_labels','source_key_mode','key_layout_contract')},
             'provider':'tej_smart_wizard','action':'download','task_id':task['task_id'],
             'source_outcome':OUTCOME,'capacity_contract':CAPACITY_CONTRACT,
             'source_scope_proof':scope_proof(),'query_attempt_id':attempt,
             'preview_submission_contract':PREVIEW_SUBMISSION_CONTRACT,
             'fresh_preview_transition_verified':True,'preview_signature':'b'*64,
             'native_rows_lower_bound':req['max_rows']+3,'native_rows_upper_bound':upper,
             'native_columns':len(req['fields'])+3,
             'requested_capacity':{k:req[k] for k in ('max_rows','max_cells')},
             'universe_scope':'exact_type_smart_id_all_sectors','calendar_date_mode':False,
             'checkbox_verification_method':'msaa_role44_state_flags',
             'date_axis':'requested_smart_wizard_grid_not_native_observation_dates',
             **{k:False for k in ('market_data_query_repeated','source_rows_adopted','credentials_read','query_comments_read')}}
    atomic_write_json(output,payload)
    return payload,output


@pytest.fixture
def ready(registry):
    root,config=registry
    config={**config,'auto_preview_capacity_replanning':True}
    configure_runtime_policy(root,config)
    configure_preview_planning(root,config)
    bridge=Key3Bridge()
    assert run_one(root,bridge)=='completed_task'  # Verified Key=3 discovery.
    return root,config,bridge


def unresolved(root):
    with closing(connect(root)) as con,con:
        refill_ready_plans(con)
        task=dict(con.execute("SELECT * FROM tasks WHERE kind='download' AND state='pending' LIMIT 1").fetchone())
        con.execute("UPDATE tasks SET state='blocked',last_error_code='unknown_outcome_no_auto_retry' WHERE task_id=?",(task['task_id'],))
    return task


def test_operator_unknown_repartition_preserves_all_fields_grid_and_completed_values(ready):
    from datetime import UTC, datetime
    from downloader.tej_preview_capacity import replan_restarted_unknown
    from downloader.tej_planning import OPERATOR_GEOMETRY_CONTRACT
    from test_tej_query_lifecycle import restart_phase_payload
    root,config,bridge=ready
    assert run_one(root,bridge)=='completed_task'
    task=unresolved(root)
    payload,output=overflow(root,task)
    # A hung query has no usable response/observed native-row count. The old
    # prepreview stage is retained and an explicit geometry prior is used.
    output.unlink()
    prepared=root/'requests'/output.name
    with closing(connect(root)) as con:
        task=dict(con.execute('SELECT * FROM tasks WHERE task_id=?',(task['task_id'],)).fetchone())
        completed=[dict(r) for r in con.execute("SELECT * FROM tasks WHERE state='complete'")]
        features=[tuple(r) for r in con.execute('SELECT * FROM features')]
    with closing(connect(root)) as con,con:
        con.execute("UPDATE tasks SET attempted_at_utc='2026-10-02T01:00:00+00:00' WHERE task_id=?",(task['task_id'],))
    session={'TejProcessId':42,'ExpectedWindow':100,'ExpectedWorkbook':'Book2','ExpectedExcelWindow':90,
             'ExpectedTitle':'TEJ Smart Wizard (Version 4.1.1.7) -- Book2'}
    run=root/'query_lifecycle'/('a'*32)
    atomic_write_json(run/'original_session.json',session)
    for phase in ('inspect-addin','stop-addin','open'):
        proof={**restart_phase_payload(session,phase),'observed_at_utc':datetime.now(UTC).isoformat()}
        atomic_write_json(run/(phase+'.json'),proof)
    atomic_write_json(root/'desktop_session.json',proof['new_session'])
    original=prepared.read_bytes();stage=output.with_suffix('.json.stage.json').read_bytes()
    result=replan_restarted_unknown(root,task['task_id'],prepared,run,record_density_prior=2)
    assert result['state']=='operator_unknown_geometry_replanned'
    assert result['provider_queries_sent']==0 and result['original_unknown_evidence_retained'] is True
    assert prepared.read_bytes()==original and output.with_suffix('.json.stage.json').read_bytes()==stage
    audit=json.loads((root/result['audit']).read_text())
    assert audit['density_basis']=='operator_geometry_prior_not_observed_native_row_count'
    assert audit['original_outcome']=='unknown_retained_not_claimed_unsent'
    with closing(connect(root)) as con:
        assert [dict(r) for r in con.execute("SELECT * FROM tasks WHERE state='complete'")]==completed
        assert [tuple(r) for r in con.execute('SELECT * FROM features')]==features
        row=con.execute('SELECT state,last_error_code,actual_rows FROM tasks WHERE task_id=?',(task['task_id'],)).fetchone()
        assert tuple(row)==('superseded_operator_unknown_geometry_v1','unknown_outcome_no_auto_retry',None)
        plan=json.loads(con.execute('SELECT plan_json FROM download_plans').fetchone()[0])
    assert plan['request']['record_capacity_contract']==OPERATOR_GEOMETRY_CONTRACT
    seen=Counter()
    requests=[task_request(root,r) for r in completed if r['kind']=='download']
    requests += [next_request(plan,i) for i in range(plan['total_queries'])]
    for request in requests:
        seen.update((f,c,d) for f in request['fields'] for c in request['company_labels'] for d in request['date_labels'])
    assert len(seen)==59*2*5 and set(seen.values())=={1}
    for _ in range(plan['total_queries']):
        assert run_one(root,bridge)=='completed_task'
    assert run_one(root,bridge)=='idle'


def test_known_overflow_preserves_completed_scope_and_replans_only_missing_cells(ready):
    root,config,bridge=ready
    assert run_one(root,bridge)=='completed_task'
    task=unresolved(root)
    payload,output=overflow(root,task)
    with closing(connect(root)) as con:
        former=dict(con.execute('SELECT * FROM download_plans').fetchone())
        completed=[dict(r) for r in con.execute("SELECT * FROM tasks WHERE state='complete'")]
        features=[tuple(r) for r in con.execute('SELECT * FROM features')]
        old_task=dict(con.execute('SELECT * FROM tasks WHERE task_id=?',(task['task_id'],)).fetchone())
    immutable={p:p.read_bytes() for p in [output,*list((root/'requests').glob(old_task['active_attempt_id']+'*')),
                                         root/'raw'/(old_task['active_attempt_id']+'.json.stage.json')]}
    result=replan_capacity(root,task['task_id'],payload,output)
    assert result['state']=='source_capacity_replanned' and result['provider_queries_sent']==0
    assert result['source_rows_adopted'] is False and result['data_query_repeated'] is False
    audit=json.loads((root/result['audit']).read_text())
    backup=root/audit['original_plan_backup']
    assert json.loads(backup.read_text())==former
    assert hashlib.sha256(backup.read_bytes()).hexdigest()==audit['original_plan_backup_sha256']
    assert audit['state']=='committed' and audit['original_outcome'].startswith('proved_overflow')
    assert all(p.read_bytes()==b for p,b in immutable.items())
    with closing(connect(root)) as con:
        assert [dict(r) for r in con.execute("SELECT * FROM tasks WHERE state='complete'")]==completed
        assert [tuple(r) for r in con.execute('SELECT * FROM features')]==features
        row=con.execute('SELECT state,actual_rows,completed_at_utc FROM tasks WHERE task_id=?',(task['task_id'],)).fetchone()
        assert tuple(row)==(SUPERSEDED,None,None)
        plan=json.loads(con.execute('SELECT plan_json FROM download_plans').fetchone()[0])
    assert plan['total_work_rows']==json.loads(former['plan_json'])['total_work_rows']
    assert plan['completed_work_rows']==sum(r['work_expected_rows'] for r in completed if r['kind']=='download')
    seen=Counter()
    for req in [task_request(root,r) for r in completed if r['kind']=='download']+[next_request(plan,i) for i in range(plan['total_queries'])]:
        seen.update((f,c,d) for f in req['fields'] for c in req['company_labels'] for d in req['date_labels'])
    assert len(seen)==59*2*5 and set(seen.values())=={1}
    assert replan_capacity(root,task['task_id'],payload,output)['already_reconciled']
    for _ in range(plan['total_queries']):
        assert run_one(root,bridge)=='completed_task'
    assert run_one(root,bridge)=='idle'


def test_deferred_former_plan_keeps_immutable_capacity_after_global_upgrade(ready):
    root,config,_=ready
    task=unresolved(root);payload,output=overflow(root,task)
    with closing(connect(root)) as con,con:
        upgraded={**config,'max_cells_per_export':config['max_cells_per_export']*2,
                  'max_rows_per_export':config['max_rows_per_export']*2}
        con.execute("UPDATE meta SET value=? WHERE key='config'",(json.dumps(upgraded),))
    result=replan_capacity(root,task['task_id'],payload,output)
    assert result['state']=='source_capacity_replanned'
    with closing(connect(root)) as con:
        plan=json.loads(con.execute('SELECT plan_json FROM download_plans').fetchone()[0])
    assert plan['request']['max_rows']==config['max_rows_per_export']
    assert plan['request']['max_cells']==config['max_cells_per_export']


def test_capacity_retiling_keeps_the_verified_empty_scope_inherited_from_key2(ready):
    from test_tej_history import empty_export
    root,config,_=ready
    with closing(connect(root)) as con,con:
        refill_ready_plans(con)
        definition=dict(con.execute('SELECT * FROM tables').fetchone())
        original_task=dict(con.execute("SELECT * FROM tasks WHERE kind='download' AND state='pending'").fetchone())
        former=json.loads(con.execute('SELECT plan_json FROM download_plans').fetchone()[0])
    request=task_request(root,original_task)
    request={k:v for k,v in request.items() if k not in ('source_key_mode','key_layout_contract')}
    empty_id=stable_id([original_task['table_id'],request])
    with closing(connect(root)) as con,con:
        con.execute('INSERT INTO tasks(task_id,table_id,kind,phase,priority,request_json,expected_rows,work_expected_rows,scope_contract) VALUES(?,?,?,?,?,?,?,?,?)',
            (empty_id,original_task['table_id'],'download','P1',0,json.dumps(compact_request(request,definition)),
             original_task['expected_rows'],original_task['work_expected_rows'],SOURCE_SCOPE_CONTRACT))
    class Empty:
        def execute(self,root,task):
            req=json.loads(task['request_json'])
            doc={**empty_export(),'task_id':task['task_id'],**{k:req[k] for k in ('type','smart_id','table','fields')}}
            out=root/'raw'/(task['task_id']+'-empty.json');atomic_write_json(out,doc)
            return doc,out,1
    assert run_one(root,Empty())=='completed_task'
    receipt=root/'receipts'/(empty_id+'.json');original_receipt=receipt.read_bytes()
    mapped={**request,'source_key_mode':3,'key_layout_contract':KEY3_CONTRACT}
    inherited=build_plan(former['request'],former['companies'],former['dates'],config,[mapped])
    with closing(connect(root)) as con,con:
        con.execute("UPDATE tasks SET state='superseded_key3_layout_v1' WHERE task_id=?",(original_task['task_id'],))
        con.execute('UPDATE download_plans SET plan_json=?,next_query=0,total_queries=?',
                    (json.dumps(inherited),inherited['total_queries']))
    task=unresolved(root);payload,output=overflow(root,task)
    result=replan_capacity(root,task['task_id'],payload,output)
    audit=json.loads((root/result['audit']).read_text())
    assert audit['inherited_empty_key_layout_coverage'][0]['task_id']==empty_id
    assert audit['completed_work_rows_preserved']==original_task['work_expected_rows']
    assert receipt.read_bytes()==original_receipt


@pytest.mark.parametrize('field,value',[
    ('native_rows_lower_bound',True),('native_rows_lower_bound',0),('native_rows_lower_bound',10),
    ('native_rows_upper_bound',5),('native_rows_upper_bound',1000001),('native_columns',29),
    ('requested_capacity',{'max_rows':11,'max_cells':300}),('source_key_mode',2),
    ('key_layout_contract','unknown'),('source_scope_proof',{}),('preview_signature','a'*64),
    ('preview_signature','missing'),('fresh_preview_transition_verified',False),
    ('query_attempt_id','wrong'),('preview_submission_contract','unknown'),
    ('company_labels',[]),('date_labels',[]),('fields',[]),('credentials_read',True),
    ('query_comments_read',True),('market_data_query_repeated',True),('source_rows_adopted',True),
    ('cells',[]),('contract_version',3),('capacity_contract','unreviewed'),('source_outcome','unknown'),
])
def test_false_or_foreign_overflow_keeps_unknown_barrier_without_mutation(ready,field,value):
    root,config,_=ready;task=unresolved(root)
    payload,output=overflow(root,task);payload[field]=value;atomic_write_json(output,payload)
    with closing(connect(root)) as con:
        before=[tuple(r) for r in con.execute('SELECT * FROM tasks')]
        plan=con.execute('SELECT plan_json FROM download_plans').fetchone()[0]
    with pytest.raises(ValueError):replan_capacity(root,task['task_id'],payload,output)
    with closing(connect(root)) as con:
        assert [tuple(r) for r in con.execute('SELECT * FROM tasks')]==before
        assert con.execute('SELECT plan_json FROM download_plans').fetchone()[0]==plan
    assert not (root/'capacity_replanning').exists()


def test_unknown_attempt_without_stage_or_changed_immutable_plan_cannot_replan(ready):
    root,config,_=ready;task=unresolved(root);payload,output=overflow(root,task)
    with closing(connect(root)) as con,con:
        plan=json.loads(con.execute('SELECT plan_json FROM download_plans').fetchone()[0])
        plan['total_work_rows']+=1
        con.execute('UPDATE download_plans SET plan_json=?',(json.dumps(plan),))
    with pytest.raises(ValueError,match='fingerprint'):replan_capacity(root,task['task_id'],payload,output)


def test_local_descriptor_recovery_does_not_invoke_provider(ready):
    root,config,_=ready;task=unresolved(root);payload,output=overflow(root,task)
    from downloader.tej_scheduler import recover_complete_local_response
    assert recover_complete_local_response(root)
    assert json.loads((root/'worker_status.json').read_text())['state']=='source_capacity_replanned'
    with closing(connect(root)) as con:
        assert con.execute('SELECT state FROM tasks WHERE task_id=?',(task['task_id'],)).fetchone()[0]==SUPERSEDED


def test_operator_existing_preview_readback_replans_without_new_preview(ready):
    root,config,_=ready;task=unresolved(root);payload,output=overflow(root,task)
    class Existing:
        def execute(self,root,task):
            assert json.loads(task['request_json'])['action']=='recover_preview'
            return payload,output,1
    assert recover_desktop_response(root,task['task_id'],Existing(),response='preview')['state']=='source_capacity_replanned'


def test_fresh_worker_known_overflow_is_progress_not_completed_data(ready):
    root,config,_=ready
    class Oversized:
        def execute(self,root,task):
            payload,output=overflow(root,task)
            return payload,output,1
    assert run_one(root,Oversized())=='source_capacity_replanned'
    with closing(connect(root)) as con:
        assert con.execute("SELECT COUNT(*) FROM tasks WHERE kind='download' AND state='complete'").fetchone()[0]==0
        assert con.execute("SELECT state FROM traffic WHERE action='download'").fetchone()[0]=='capacity_replanned'


def test_replanning_disabled_is_durable_review_not_unknown_replay(ready):
    root,config,_=ready
    configure_runtime_policy(root,{**config,'auto_preview_capacity_replanning':False})
    task=unresolved(root);payload,output=overflow(root,task)
    assert replan_capacity(root,task['task_id'],payload,output)['state']==REVIEW_STATE
    assert run_one(root,None)==REVIEW_STATE
    assert replan_capacity(root,task['task_id'],payload,output)['already_reconciled']


def test_atomic_company_period_overflow_stops_instead_of_looping(ready):
    root,config,_=ready;task=unresolved(root);payload,output=overflow(root,task)
    replan_capacity(root,task['task_id'],payload,output)  # Hint now restricts each query to one pair.
    task=unresolved(root);payload,output=overflow(root,task)
    assert len(task_request(root,task)['company_labels'])*len(task_request(root,task)['date_labels'])==1
    assert replan_capacity(root,task['task_id'],payload,output)['state']==REVIEW_STATE
    assert run_one(root,None)==REVIEW_STATE


def test_postcommit_audit_is_reconciled_idempotently_after_interruption(ready):
    root,config,_=ready;task=unresolved(root);payload,output=overflow(root,task)
    result=replan_capacity(root,task['task_id'],payload,output)
    path=root/result['audit'];audit=json.loads(path.read_text());audit['state']='prepared'
    atomic_write_json(path,audit)
    assert replan_capacity(root,task['task_id'],payload,output)['already_reconciled']
    assert json.loads(path.read_text())['state']=='committed'


def test_failed_durable_intent_rolls_back_and_retains_the_original_unknown(ready,monkeypatch):
    root,config,_=ready;task=unresolved(root);payload,output=overflow(root,task)
    import downloader.tej_preview_capacity as capacity
    original=capacity.atomic_write_json
    def fail_intent(path,value,**kwargs):
        if value.get('state')=='prepared':raise OSError('injected disk failure')
        return original(path,value,**kwargs)
    monkeypatch.setattr(capacity,'atomic_write_json',fail_intent)
    with pytest.raises(OSError):replan_capacity(root,task['task_id'],payload,output)
    with closing(connect(root)) as con:
        row=con.execute('SELECT state,last_error_code FROM tasks WHERE task_id=?',(task['task_id'],)).fetchone()
        assert tuple(row)==('blocked','unknown_outcome_no_auto_retry')
    monkeypatch.setattr(capacity,'atomic_write_json',original)
    assert replan_capacity(root,task['task_id'],payload,output)['state']=='source_capacity_replanned'


def test_postcommit_marker_failure_never_relabels_committed_plan_unknown(ready,monkeypatch):
    root,config,_=ready;task=unresolved(root);payload,output=overflow(root,task)
    import downloader.tej_preview_capacity as capacity
    original=capacity.atomic_write_json
    def fail_marker(path,value,**kwargs):
        if value.get('state')=='committed':raise OSError('injected postcommit failure')
        return original(path,value,**kwargs)
    monkeypatch.setattr(capacity,'atomic_write_json',fail_marker)
    result=replan_capacity(root,task['task_id'],payload,output)
    assert result['audit_commit_marker_pending'] and result['state']=='source_capacity_replanned'
    with closing(connect(root)) as con:
        assert con.execute('SELECT state FROM tasks WHERE task_id=?',(task['task_id'],)).fetchone()[0]==SUPERSEDED
    monkeypatch.setattr(capacity,'atomic_write_json',original)
    assert replan_capacity(root,task['task_id'],payload,output)['already_reconciled']


def test_changed_prepared_scope_is_not_capacity_proof(ready):
    root,config,_=ready;task=unresolved(root);payload,output=overflow(root,task)
    with closing(connect(root)) as con:
        attempt=con.execute('SELECT active_attempt_id FROM tasks WHERE task_id=?',(task['task_id'],)).fetchone()[0]
    prepared=root/'requests'/(attempt+'.json');raw=json.loads(prepared.read_text());raw['end']='2025-01-01'
    atomic_write_json(prepared,raw)
    with pytest.raises(ValueError,match='prepared download scope'):replan_capacity(root,task['task_id'],payload,output)


def test_adopted_observations_cannot_be_superseded_by_capacity_metadata(ready):
    root,config,_=ready;task=unresolved(root);payload,output=overflow(root,task)
    with closing(connect(root)) as con,con:
        con.execute('UPDATE tasks SET actual_rows=1 WHERE task_id=?',(task['task_id'],))
    with pytest.raises(ValueError,match='adopted source observations'):replan_capacity(root,task['task_id'],payload,output)


def test_unrelated_unresolved_preview_prevents_capacity_replanning(ready):
    root,config,_=ready;task=unresolved(root);payload,output=overflow(root,task)
    with closing(connect(root)) as con,con:
        con.execute("INSERT INTO tasks(task_id,table_id,kind,phase,priority,request_json,state) "
                    "VALUES('foreign','other','download','P1',100,'{}','running')")
    with pytest.raises(ValueError,match='foreign source action'):replan_capacity(root,task['task_id'],payload,output)


@pytest.mark.parametrize('hint,contract,keys',[(True,CAPACITY_CONTRACT,3),(0,CAPACITY_CONTRACT,3),
    (3,None,3),(3,CAPACITY_CONTRACT,2),(1,'unknown',3)])
def test_planner_refuses_unverified_density(hint,contract,keys):
    req={**base_request(),'native_record_density_hint':hint,'record_capacity_contract':contract}
    if keys==3:req.update(source_key_mode=3,key_layout_contract=KEY3_CONTRACT)
    with pytest.raises(ValueError,match='capacity hint'):build_plan(req,*axes(),configuration(),[])


def test_density_changes_only_query_geometry_not_workload_or_grain():
    req={**base_request(),'source_key_mode':3,'key_layout_contract':KEY3_CONTRACT}
    old=build_plan(req,*axes(),configuration(),[])
    new=build_plan({**req,'native_record_density_hint':3,'record_capacity_contract':CAPACITY_CONTRACT},*axes(),configuration(),[])
    assert new['total_work_rows']==old['total_work_rows'] and new['fingerprint']!=old['fingerprint']
    for i in range(new['total_queries']):
        q=next_request(new,i)
        assert len(q['company_labels'])*len(q['date_labels'])*3<=q['max_rows']
        assert (len(q['company_labels'])*len(q['date_labels'])*3+1)*(len(q['fields'])+3)<=q['max_cells']
