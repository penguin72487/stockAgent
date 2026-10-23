"""Native selector capacity is independent from Preview output capacity."""
from collections import Counter
from contextlib import closing
from datetime import date, timedelta
from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path

import pytest

from downloader.tej_history import compact_request, connect, stable_id, task_request, SOURCE_SCOPE_CONTRACT, run_one
from downloader.tej_planning import (
    SELECTOR_CONTRACT, MAX_SELECTOR_LABELS, TILING_CONTRACT, build_plan, next_request,
    company_batch_size, rectangle_queries, repartition_selector_pending,
)
from test_tej_planning import base_request, registry as registry


def pending_scope(root, *, companies=1, days=10000):
    request=base_request()
    labels=[(date(1988,1,1)+timedelta(days=i)).strftime('%Y/%m/%d') for i in range(days)]
    request={**request,'action':'download','max_rows':10000,'max_cells':400000,
        'fields':request['fields'][28:56], 'company_labels':[f'{i+1000}=>Company{i}' for i in range(companies)],
        'date_labels':labels,'end':'2026-10-01',
        'field_partition':{'contract':'preview_30_columns_lazy_fields_v1','field_range':[28,56],
            'catalog_fields':59,'plan_fingerprint':'original-preserved-field-partition'}}
    with closing(connect(root)) as con,con:
        definition=dict(con.execute('SELECT * FROM tables LIMIT 1').fetchone())
        identifier=stable_id([definition['table_id'],request]);expected=companies*days
        con.execute('INSERT INTO tasks(task_id,table_id,kind,phase,priority,request_json,expected_rows,'
            'scope_contract,work_expected_rows,safe_prequery_retries) VALUES(?,?,?,?,?,?,?,?,?,?)',
            (identifier,definition['table_id'],'download','P1',100,json.dumps(compact_request(request,definition)),
             expected,SOURCE_SCOPE_CONTRACT,expected,2))
        return dict(con.execute('SELECT * FROM tasks WHERE task_id=?',(identifier,)).fetchone()),request


@pytest.mark.parametrize('companies,days,expected_queries',[(1,10000,5),(2,5000,3),(5,2000,1)])
def test_pending_scope_keeps_every_original_field_company_date_once(registry,companies,days,expected_queries):
    root,_=registry;parent,request=pending_scope(root,companies=companies,days=days)
    original=dict(parent)
    with closing(connect(root)) as con,con:
        changed=repartition_selector_pending(con,root,parent)
        assert changed is (expected_queries>1)
    with closing(connect(root)) as con:
        children=[dict(r) for r in con.execute("SELECT * FROM tasks WHERE kind='download' AND state='pending'")]
        assert len(children)==expected_queries
        scopes=[task_request(root,t) for t in children]
        seen=Counter((c,d) for q in scopes for c in q['company_labels'] for d in q['date_labels'])
        assert len(seen)==companies*days and set(seen.values())=={1}
        assert all(q['fields']==request['fields'] and q['field_partition']==request['field_partition'] for q in scopes)
        assert all(max(len(q['company_labels']),len(q['date_labels']))<=MAX_SELECTOR_LABELS for q in scopes)
        assert sum(r['work_expected_rows'] for r in children)==parent['work_expected_rows']
        assert all(r['safe_prequery_retries']==2 and r['actual_rows'] is None for r in children)
        retained=dict(con.execute('SELECT * FROM tasks WHERE task_id=?',(parent['task_id'],)).fetchone())
        if changed:
            assert retained=={**original,'state':'superseded_selector_geometry_v1'}
        else:
            assert retained==original


def test_input_bound_matches_all_native_lookup_and_selection_paths():
    code=Path('scripts/tej_smart_wizard_bridge.ps1').read_text()
    assert code.count(f'labels.Length>{MAX_SELECTOR_LABELS}')==3


def membership_failure(root, *, missing=(1,3), companies=4, immediate=False):
    from downloader.artifact_io import atomic_write_json
    from downloader.tej_history import DesktopBridge
    from test_tej_prequery_recovery import exhausted
    parent,request=pending_scope(root,companies=companies,days=3)
    parent,outcome,prepared=exhausted(root,parent)
    bridge=DesktopBridge(Path(__file__).resolve().parents[1],{})
    bridge.script_release(root,parent['active_attempt_id'],prepared)
    sidecar=root/'raw'/(parent['active_attempt_id']+'.json.company-membership.json')
    payload={'contract':'exact_current_company_membership_v1','task_id':parent['task_id'],
        'query_attempt_id':parent['active_attempt_id'],'requested_count':companies,
        'available_count':500,'absent_positions':list(missing),
        'prepared_request_sha256':hashlib.sha256(prepared.read_bytes()).hexdigest(),
        'observed_at_utc':datetime.now(UTC).isoformat(),'market_data_query_submitted':False,
        'complete_lookup_verified':True,'source_binding_verified':True,'source_list_count_stable':True}
    atomic_write_json(sidecar,payload)
    proof=json.loads(outcome.read_text());proof['company_membership_sha256']=hashlib.sha256(sidecar.read_bytes()).hexdigest()
    atomic_write_json(outcome,proof)
    if immediate:
        with closing(connect(root)) as con,con:
            con.execute("UPDATE tasks SET state='pending',safe_prequery_retries=1,last_error_code='local_list_selection_failed_before_preview' WHERE task_id=?",(parent['task_id'],))
    return parent,request,sidecar,outcome,prepared


@pytest.mark.parametrize('immediate',[False,True])
def test_missing_company_partition_preserves_every_axis_and_unblocks_present_companies(registry,immediate):
    from downloader.tej_desktop_attempts import defer_unsent_prequery
    from downloader.tej_history import _ready_task
    from test_tej_prequery_recovery import HealthyReadback
    root,_=registry
    parent,request,sidecar,outcome,prepared=membership_failure(root,immediate=immediate)
    retained={p:p.read_bytes() for p in (sidecar,outcome,prepared)}
    result=defer_unsent_prequery(root,parent['task_id'],HealthyReadback(),allow_company_partition=True)
    assert result['state']=='selector_scope_repartitioned'
    assert result['present_companies']==result['deferred_companies']==2
    assert result['work_rows_before']==result['work_rows_after']==12
    assert result['provider_queries_sent']==0 and result['source_rows_adopted'] is False
    with closing(connect(root)) as con:
        children=[dict(r) for r in con.execute("SELECT * FROM tasks WHERE kind='download' AND state='pending'")]
        assert len(children)==2
        scopes=[task_request(root,child) for child in children]
        seen=Counter((c,d,f) for q in scopes for c in q['company_labels'] for d in q['date_labels'] for f in q['fields'])
        expected={(c,d,f) for c in request['company_labels'] for d in request['date_labels'] for f in request['fields']}
        assert set(seen)==expected and set(seen.values())=={1}
        assert all(q['field_partition']==request['field_partition'] for q in scopes)
        assert sum(c['work_expected_rows'] for c in children)==12
        assert all(c['safe_prequery_retries']==(1 if immediate else 2) for c in children)
        assert _ready_task(con,datetime.now(UTC).isoformat(),'download',{})['last_error_code'] is None
        saved=dict(con.execute('SELECT * FROM tasks WHERE task_id=?',(parent['task_id'],)).fetchone())
        assert saved['state']=='superseded_company_membership_v1' and saved['active_attempt_id']==parent['active_attempt_id']
        assert con.execute('SELECT state FROM desktop_attempts WHERE attempt_id=?',(parent['active_attempt_id'],)).fetchone()[0]=='proven_not_submitted'
    assert all(p.read_bytes()==body for p,body in retained.items())


def test_all_missing_companies_remain_pending_and_lazy_plan_can_advance(registry):
    from downloader.tej_desktop_attempts import defer_unsent_prequery
    from test_tej_prequery_recovery import HealthyReadback
    root,_=registry
    parent,request,*_=membership_failure(root,missing=(0,1,2,3))
    result=defer_unsent_prequery(root,parent['task_id'],HealthyReadback())
    assert result['state']=='prequery_failure_deferred' and result['deferred_companies']==4
    with closing(connect(root)) as con:
        saved=dict(con.execute('SELECT * FROM tasks WHERE task_id=?',(parent['task_id'],)).fetchone())
        assert saved['state']=='pending' and saved['last_error_code']=='source_company_labels_absent_fragment'
        assert saved['next_attempt_at_utc']==result['next_attempt_at_utc']
        assert task_request(root,saved)==request


@pytest.mark.parametrize('state,error',[
    ('pending',None), ('pending','prequery_failure_deferred'),
    ('running','source_company_labels_absent_fragment'),
    ('blocked','source_company_labels_absent_fragment'),
])
def test_lazy_refill_does_not_bypass_a_healthy_or_unresolved_fragment(registry,state,error):
    from downloader.tej_history import configure_preview_planning
    from downloader.tej_planning import refill_ready_plans
    from test_tej_planning import DynamicBridge
    root,config=registry
    configure_preview_planning(root,config)
    assert run_one(root,DynamicBridge())=='completed_task'
    with closing(connect(root)) as con,con:
        assert refill_ready_plans(con)==1
        task=con.execute("SELECT * FROM tasks WHERE kind='download' AND state='pending'").fetchone()
        con.execute('UPDATE tasks SET state=?,last_error_code=? WHERE task_id=?',(state,error,task['task_id']))
        assert refill_ready_plans(con)==0
        assert con.execute('SELECT next_query FROM download_plans').fetchone()[0]==1
        assert con.execute("SELECT count(*) FROM tasks WHERE kind='download'").fetchone()[0]==1


def test_lazy_refill_advances_once_without_erasing_the_missing_fragment_or_denominator(registry):
    from downloader.tej_history import configure_preview_planning
    from downloader.tej_planning import refill_ready_plans
    from test_tej_planning import DynamicBridge
    root,config=registry
    configure_preview_planning(root,config)
    assert run_one(root,DynamicBridge())=='completed_task'
    with closing(connect(root)) as con,con:
        assert refill_ready_plans(con)==1
        parent=dict(con.execute("SELECT * FROM tasks WHERE kind='download' AND state='pending'").fetchone())
        original_table=dict(con.execute('SELECT * FROM tables').fetchone())
        original_plan=dict(con.execute('SELECT * FROM download_plans').fetchone())
        con.execute("UPDATE tasks SET last_error_code='source_company_labels_absent_fragment',next_attempt_at_utc='2099-01-01' WHERE task_id=?",(parent['task_id'],))
        deferred=dict(con.execute('SELECT * FROM tasks WHERE task_id=?',(parent['task_id'],)).fetchone())
        assert refill_ready_plans(con)==1
        assert refill_ready_plans(con)==0  # New healthy work, including a NULL error, blocks duplication.
        assert dict(con.execute('SELECT * FROM tasks WHERE task_id=?',(parent['task_id'],)).fetchone())==deferred
        assert dict(con.execute('SELECT * FROM tables').fetchone())==original_table
        saved_plan=dict(con.execute('SELECT * FROM download_plans').fetchone())
        assert saved_plan=={**original_plan,'next_query':2}
        child=dict(con.execute("SELECT * FROM tasks WHERE kind='download' AND task_id<>?",(parent['task_id'],)).fetchone())
        expected=next_request(json.loads(original_plan['plan_json']),1)
        actual=task_request(root,child)
        assert all(actual[key]==expected[key] for key in ('fields','company_labels','date_labels'))
        assert child['actual_rows'] is None and deferred['actual_rows'] is None


def test_dashboard_distinguishes_partial_company_retry_and_counts_every_pending_fragment(registry):
    from downloader.tej_desktop_attempts import defer_unsent_prequery
    from stockagent.live.tej_dashboard import build_tej_public_status
    from test_tej_prequery_recovery import HealthyReadback
    root,_=registry
    parent,*_=membership_failure(root)
    defer_unsent_prequery(root,parent['task_id'],HealthyReadback())
    public=build_tej_public_status(root.parent)
    row=next(t for t in public['tables'] if t['table_id']==parent['table_id'])
    assert row['state']=='backfilling_with_deferred_companies'
    assert row['local_retry_window']['retry_scope']=='company_fragment'
    assert row['local_retry_window']['error_code']=='source_company_labels_absent_fragment'
    assert row['deferred_tasks']==public['activity']['deferred_tasks_total']==1
    assert row['remaining_seconds'] is None  # Unknown missing companies prevent a full-table ETA.
    with closing(connect(root)) as con,con:
        child=dict(con.execute("SELECT * FROM tasks WHERE last_error_code='source_company_labels_absent_fragment'").fetchone())
        earlier='2000-01-01T00:00:00+00:00'
        # Another independently preserved pending fragment shares the table.
        con.execute('INSERT INTO tasks(task_id,table_id,kind,phase,priority,request_json,state,expected_rows,'
            'scope_contract,work_expected_rows,last_error_code,next_attempt_at_utc) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)',
            ('f'*24,parent['table_id'],'download','P1',100,child['request_json'],'pending',6,
             SOURCE_SCOPE_CONTRACT,6,'source_company_labels_absent_fragment',earlier))
    public=build_tej_public_status(root.parent)
    row=next(t for t in public['tables'] if t['table_id']==parent['table_id'])
    assert row['state']=='backfilling_with_deferred_companies'
    assert row['deferred_tasks']==public['activity']['deferred_tasks_total']==2
    assert row['local_retry_window']['next_attempt_at_utc']==earlier
    assert 'audit_path' not in json.dumps(public) and 'Company1' not in json.dumps(public)
    with closing(connect(root)) as con,con:
        con.execute('DELETE FROM desktop_retry_windows')  # A different fragment may have completed.
    public=build_tej_public_status(root.parent)
    row=next(t for t in public['tables'] if t['table_id']==parent['table_id'])
    assert row['deferred_tasks']==public['activity']['deferred_tasks_total']==2
    assert row['state']=='backfilling_with_deferred_companies'
    assert row['local_retry_window']['consecutive_failures'] is None


def new_hint_tile(root,request,*,labels=None,day_offset=0):
    from downloader.tej_history import compact_request
    new={**request,'date_labels':[(date(1988,2,1)+timedelta(days=day_offset+i)).strftime('%Y/%m/%d') for i in range(3)]}
    if labels is not None:new['company_labels']=labels
    with closing(connect(root)) as con,con:
        definition=dict(con.execute('SELECT * FROM tables LIMIT 1').fetchone())
        identifier=stable_id([definition['table_id'],new]);count=len(new['company_labels'])*3
        con.execute('INSERT INTO tasks(task_id,table_id,kind,phase,priority,request_json,expected_rows,'
            'scope_contract,work_expected_rows) VALUES(?,?,?,?,?,?,?,?,?)',
            (identifier,definition['table_id'],'download','P1',100,
             json.dumps(compact_request(new,definition)),count,SOURCE_SCOPE_CONTRACT,count))
        return dict(con.execute('SELECT * FROM tasks WHERE task_id=?',(identifier,)).fetchone()),new


def test_company_hint_reuses_owned_write_transaction_instead_of_locking_itself(registry,monkeypatch):
    from downloader.tej_desktop_attempts import defer_unsent_prequery
    from downloader.tej_planning import defer_planned_company_hint
    from test_tej_prequery_recovery import HealthyReadback
    root,_=registry
    prior,request,*_=membership_failure(root)
    defer_unsent_prequery(root,prior['task_id'],HealthyReadback())
    task,_=new_hint_tile(root,request)
    with closing(connect(root)) as con,con:
        # A real write transaction models lazy refill before validating the
        # existing negative proof. A second schema-opening connection would
        # wait on our own RESERVED lock and roll back all new queue work.
        con.execute("INSERT OR REPLACE INTO meta VALUES('owned_refill_test','retained')")
        monkeypatch.setattr('downloader.tej_history.connect',
                            lambda *_:pytest.fail('Must reuse the original transaction'))
        assert defer_planned_company_hint(con,root,task)
        assert con.in_transaction
        assert con.execute("SELECT value FROM meta WHERE key='owned_refill_test'").fetchone()[0]=='retained'


@pytest.mark.parametrize('all_absent,different_fields',[(False,False),(True,False),(False,True)])
def test_recent_company_hint_avoids_repeating_failed_ui_preparation_but_keeps_all_work(registry,all_absent,different_fields):
    from downloader.tej_desktop_attempts import defer_unsent_prequery
    from downloader.tej_planning import COMPANY_HINT_CONTRACT,defer_planned_company_hint
    from test_tej_prequery_recovery import HealthyReadback
    root,_=registry;prior,request,sidecar,outcome,prepared=membership_failure(root)
    defer_unsent_prequery(root,prior['task_id'],HealthyReadback())
    tile_request=({**request,'fields':request['catalog_fields'][:28],
        'field_partition':{**request['field_partition'],'field_range':[0,28]}} if different_fields else request)
    task,scope=new_hint_tile(root,tile_request,labels=[request['company_labels'][1]] if all_absent else None)
    original={p:p.read_bytes() for p in (sidecar,outcome,prepared)}
    with closing(connect(root)) as con,con:
        window=dict(con.execute('SELECT * FROM desktop_retry_windows').fetchone())
        attempts=con.execute('SELECT count(*) FROM desktop_attempts').fetchone()[0]
        traffic=con.execute('SELECT count(*) FROM traffic').fetchone()[0]
        assert defer_planned_company_hint(con,root,task)
        assert dict(con.execute('SELECT * FROM desktop_retry_windows').fetchone())==window
        assert con.execute('SELECT count(*) FROM desktop_attempts').fetchone()[0]==attempts
        assert con.execute('SELECT count(*) FROM traffic').fetchone()[0]==traffic
        parent=dict(con.execute('SELECT * FROM tasks WHERE task_id=?',(task['task_id'],)).fetchone())
        if all_absent:
            assert parent['state']=='pending' and parent['last_error_code']=='source_company_labels_absent_fragment'
            assert parent['next_attempt_at_utc']==window['next_attempt_at_utc']
            assert task_request(root,parent)==scope
            assert not defer_planned_company_hint(con,root,parent)
        else:
            assert parent['state']=='superseded_company_hint_v1' and parent['active_attempt_id'] is None
            children=[dict(r) for r in con.execute("SELECT * FROM tasks WHERE kind='download' AND task_id<>?",(task['task_id'],))
                if task_request(root,dict(r)).get('company_membership_partition',{}).get('parent_task_id')==task['task_id']]
            assert len(children)==2 and sum(c['work_expected_rows'] for c in children)==task['work_expected_rows']
            queries=[task_request(root,c) for c in children]
            seen=Counter((c,d,f) for q in queries for c in q['company_labels'] for d in q['date_labels'] for f in q['fields'])
            assert set(seen)=={(c,d,f) for c in scope['company_labels'] for d in scope['date_labels'] for f in scope['fields']}
            assert set(seen.values())=={1}
            assert all(q['company_membership_partition']['contract']==COMPANY_HINT_CONTRACT for q in queries)
            assert all(c['safe_prequery_retries']==0 and c['actual_rows'] is None for c in children)
    audit=next(p for p in (root/'selector_replanning').glob(task['task_id']+'-*-company-hint.json'))
    proof=json.loads(audit.read_text())
    assert proof['fresh_source_membership_claimed'] is False and proof['source_failure_count_increased'] is False
    assert proof['prepared_request_sha256'] is None and proof['prequery_outcome_sha256'] is None
    assert proof['scheduling_hint']['prior_negative_proof_applies_only_to_prior_request'] is True
    assert all(p.read_bytes()==body for p,body in original.items())


@pytest.mark.parametrize('fault',['expired','stale','proof','outside_root','fields','no_overlap',
    'attempted','active','adopted','wrong_attempt'])
def test_unverified_or_inapplicable_hint_never_delays_a_fresh_tile(registry,fault):
    from downloader.tej_desktop_attempts import defer_unsent_prequery
    from downloader.tej_planning import defer_planned_company_hint
    from test_tej_prequery_recovery import HealthyReadback
    root,_=registry;prior,request,sidecar,*_=membership_failure(root)
    defer_unsent_prequery(root,prior['task_id'],HealthyReadback())
    task,scope=new_hint_tile(root,request,labels=[request['company_labels'][0]] if fault=='no_overlap' else None)
    with closing(connect(root)) as con,con:
        window=dict(con.execute('SELECT * FROM desktop_retry_windows').fetchone())
        if fault=='expired':con.execute("UPDATE desktop_retry_windows SET next_attempt_at_utc='2000-01-01T00:00:00+00:00'")
        elif fault=='stale':
            p=root/window['audit_path'];d=json.loads(p.read_text());d['observed_at_utc']='2000-01-01T00:00:00+00:00';p.write_text(json.dumps(d))
        elif fault=='proof':sidecar.write_text('{}')
        elif fault=='outside_root':con.execute("UPDATE desktop_retry_windows SET audit_path='../foreign.json'")
        elif fault=='fields':
            d=dict(con.execute('SELECT * FROM tables LIMIT 1').fetchone());changed={**scope,'fields':scope['fields'][1:]}
            con.execute('UPDATE tasks SET request_json=? WHERE task_id=?',(json.dumps(changed),task['task_id']))
            task=dict(con.execute('SELECT * FROM tasks WHERE task_id=?',(task['task_id'],)).fetchone())
        elif fault=='attempted':task['attempted_at_utc']=datetime.now(UTC).isoformat()
        elif fault=='active':task['active_attempt_id']='foreign'
        elif fault=='adopted':task['actual_rows']=0
        elif fault=='wrong_attempt':con.execute("UPDATE desktop_retry_windows SET attempt_id='foreign'")
        before=[dict(r) for r in con.execute('SELECT * FROM tasks')]
        assert not defer_planned_company_hint(con,root,task)
        assert [dict(r) for r in con.execute('SELECT * FROM tasks')]==before


@pytest.mark.parametrize('fault',[None,'future_due','naive_due','missing_due','partial_labels','attempted','active'])
def test_due_unattempted_missing_fragment_shares_one_fresh_lookup_without_rechecking_each_date(registry,fault):
    from datetime import timedelta
    from downloader.tej_desktop_attempts import defer_unsent_prequery
    from downloader.tej_planning import COMPANY_MEMBERSHIP_DEFERRED,defer_planned_company_hint
    from test_tej_prequery_recovery import HealthyReadback
    root,_=registry;prior,request,*_=membership_failure(root)
    defer_unsent_prequery(root,prior['task_id'],HealthyReadback())
    task,scope=new_hint_tile(root,request,labels=None if fault=='partial_labels' else [request['company_labels'][1]])
    now=datetime.now(UTC)
    due=(now-timedelta(seconds=10)).isoformat()
    if fault=='future_due':due=(now+timedelta(seconds=10)).isoformat()
    elif fault=='naive_due':due=now.replace(tzinfo=None).isoformat()
    elif fault=='missing_due':due=None
    with closing(connect(root)) as con,con:
        con.execute('UPDATE tasks SET last_error_code=?,next_attempt_at_utc=? WHERE task_id=?',
                    (COMPANY_MEMBERSHIP_DEFERRED,due,task['task_id']))
        task=dict(con.execute('SELECT * FROM tasks WHERE task_id=?',(task['task_id'],)).fetchone())
        if fault=='attempted':task['attempted_at_utc']=now.isoformat()
        elif fault=='active':task['active_attempt_id']='foreign'
        window=dict(con.execute('SELECT * FROM desktop_retry_windows').fetchone())
        before=dict(con.execute('SELECT * FROM tasks WHERE task_id=?',(task['task_id'],)).fetchone())
        attempts=con.execute('SELECT count(*) FROM desktop_attempts').fetchone()[0]
        traffic=con.execute('SELECT count(*) FROM traffic').fetchone()[0]
        assert defer_planned_company_hint(con,root,task,now=now) is (fault is None)
        after=dict(con.execute('SELECT * FROM tasks WHERE task_id=?',(task['task_id'],)).fetchone())
        if fault is None:
            assert after=={**before,'next_attempt_at_utc':window['next_attempt_at_utc']}
            assert task_request(root,after)==scope
            assert not defer_planned_company_hint(con,root,after,now=now)
            audits=list((root/'selector_replanning').glob(task['task_id']+'-*-company-hint.json'))
            assert len(audits)==1
            proof=json.loads(audits[0].read_text())
            assert proof['scheduling_hint']['previously_deferred_fragment'] is True
            assert proof['scheduling_hint']['previous_fragment_due_at_utc']==due
            assert proof['source_failure_count_increased'] is False
        else:assert after==before
        assert dict(con.execute('SELECT * FROM desktop_retry_windows').fetchone())==window
        assert con.execute('SELECT count(*) FROM desktop_attempts').fetchone()[0]==attempts
        assert con.execute('SELECT count(*) FROM traffic').fetchone()[0]==traffic


@pytest.mark.parametrize('fault',[None,'unfinished','possible_preview','sidecar_changed','original_not_all_absent'])
def test_due_previously_attempted_fragment_requires_its_own_exact_finished_all_absent_negative(registry,fault):
    from datetime import timedelta
    from downloader.tej_desktop_attempts import defer_unsent_prequery
    from downloader.tej_planning import COMPANY_MEMBERSHIP_DEFERRED,defer_planned_company_hint
    from test_tej_prequery_recovery import HealthyReadback
    root,_=registry
    old,scope,sidecar,outcome,prepared=membership_failure(root,companies=2,
        missing=(0,) if fault=='original_not_all_absent' else (0,1))
    defer_unsent_prequery(root,old['task_id'],HealthyReadback())
    latest,*_=membership_failure(root,companies=4,missing=(0,1))
    defer_unsent_prequery(root,latest['task_id'],HealthyReadback())
    if fault=='possible_preview':
        data=json.loads(outcome.read_text());data['market_data_query_submission_possible']=True
        outcome.write_text(json.dumps(data))
    elif fault=='sidecar_changed':sidecar.write_text('{}')
    original={p:p.read_bytes() for p in (prepared,sidecar,outcome)}
    with closing(connect(root)) as con,con:
        if fault=='unfinished':
            con.execute("UPDATE desktop_attempts SET state='running',finished_at_utc=NULL WHERE attempt_id=?",(old['active_attempt_id'],))
        started=con.execute('SELECT started_at_utc FROM desktop_attempts WHERE attempt_id=?',
                            (old['active_attempt_id'],)).fetchone()[0]
        con.execute("UPDATE tasks SET state='pending',last_error_code=?,next_attempt_at_utc=?,attempted_at_utc=? WHERE task_id=?",
            (COMPANY_MEMBERSHIP_DEFERRED,(datetime.now(UTC)-timedelta(seconds=1)).isoformat(),started,old['task_id']))
        task=dict(con.execute('SELECT * FROM tasks WHERE task_id=?',(old['task_id'],)).fetchone())
        window=dict(con.execute('SELECT * FROM desktop_retry_windows').fetchone())
        attempts=[dict(r) for r in con.execute('SELECT * FROM desktop_attempts')]
        traffic=[dict(r) for r in con.execute('SELECT * FROM traffic')]
        assert defer_planned_company_hint(con,root,task) is (fault is None)
        after=dict(con.execute('SELECT * FROM tasks WHERE task_id=?',(old['task_id'],)).fetchone())
        assert after==({**task,'next_attempt_at_utc':window['next_attempt_at_utc']} if fault is None else task)
        assert task_request(root,after)==scope
        assert dict(con.execute('SELECT * FROM desktop_retry_windows').fetchone())==window
        assert [dict(r) for r in con.execute('SELECT * FROM desktop_attempts')]==attempts
        assert [dict(r) for r in con.execute('SELECT * FROM traffic')]==traffic
    assert all(p.read_bytes()==body for p,body in original.items())
    if fault is None:
        proof=json.loads(next((root/'selector_replanning').glob(old['task_id']+'-*-company-hint.json')).read_text())
        hint=proof['scheduling_hint']
        assert hint['current_request_has_no_desktop_attempt'] is False
        assert hint['original_finished_all_absent_attempt']['attempt_id']==old['active_attempt_id']
        assert proof['fresh_source_membership_claimed'] is False and proof['source_failure_count_increased'] is False


@pytest.mark.parametrize('api_owns',[False,True])
def test_canonical_runner_consumes_hint_without_launching_another_desktop_operation(registry,monkeypatch,api_owns):
    from downloader.tej_desktop_attempts import defer_unsent_prequery,PREQUERY_RETRY_CONTRACT
    from downloader.tej_history import configure_runtime_policy
    from test_tej_prequery_recovery import HealthyReadback
    root,config=registry;prior,request,*_=membership_failure(root)
    defer_unsent_prequery(root,prior['task_id'],HealthyReadback())
    task,_=new_hint_tile(root,request)
    configure_runtime_policy(root,{**config,'prequery_failure_isolation':True,
        'prequery_recovery_contract':PREQUERY_RETRY_CONTRACT})
    with closing(connect(root)) as con,con:
        con.execute('UPDATE tasks SET priority=0 WHERE task_id=?',(task['task_id'],))
        before=con.execute('SELECT count(*) FROM traffic').fetchone()[0]
    class NoDesktop:
        def execute(self,*args):pytest.fail('Scheduling-only hints must not call the desktop')
    if api_owns:
        monkeypatch.setattr('downloader.tej_api_ownership.repartition_pending',lambda *args:True)
        monkeypatch.setattr('downloader.tej_planning.defer_planned_company_hint',
            lambda *args:pytest.fail('Verified API ownership must be resolved before a scheduling hint'))
    expected='api_scope_repartitioned' if api_owns else 'selector_scope_repartitioned'
    assert run_one(root,NoDesktop(),kind='download',table_id=task['table_id'])==expected
    with closing(connect(root)) as con:
        assert con.execute('SELECT count(*) FROM traffic').fetchone()[0]==before
        assert con.execute('SELECT state FROM tasks WHERE task_id=?',(task['task_id'],)).fetchone()[0]==('pending' if api_owns else 'superseded_company_hint_v1')


@pytest.mark.parametrize('batch,deadline_expired',[(1,False),(4,False),(8,False),(32,False),(8,True)])
def test_local_batch_keeps_every_scope_and_retry_clock_without_claiming_source_work(registry,monkeypatch,batch,deadline_expired):
    from types import SimpleNamespace
    from downloader.tej_desktop_attempts import defer_unsent_prequery,PREQUERY_RETRY_CONTRACT
    from downloader.tej_history import configure_runtime_policy
    from test_tej_prequery_recovery import HealthyReadback
    root,config=registry;prior,request,sidecar,outcome,prepared=membership_failure(root)
    defer_unsent_prequery(root,prior['task_id'],HealthyReadback())
    with closing(connect(root)) as con,con:
        con.execute('UPDATE tasks SET priority=1000')
    tasks=[new_hint_tile(root,request,labels=[request['company_labels'][1]],day_offset=i*3)[0] for i in range(8)]
    configure_runtime_policy(root,{**config,'prequery_failure_isolation':True,
        'prequery_recovery_contract':PREQUERY_RETRY_CONTRACT,'local_planning_batch_size':batch})
    with closing(connect(root)) as con,con:
        for task in tasks:con.execute('UPDATE tasks SET priority=0 WHERE task_id=?',(task['task_id'],))
        window=dict(con.execute('SELECT * FROM desktop_retry_windows').fetchone())
        attempts=[dict(r) for r in con.execute('SELECT * FROM desktop_attempts')]
        traffic=[dict(r) for r in con.execute('SELECT * FROM traffic')]
        scopes={t['task_id']:task_request(root,t) for t in tasks}
    saved={p:p.read_bytes() for p in (sidecar,outcome,prepared)}
    clock=iter([100,101.1]) if deadline_expired else None
    monkeypatch.setattr('downloader.tej_history.time',SimpleNamespace(monotonic=lambda:next(clock) if clock else 100))
    class NoDesktop:
        def execute(self,*args):pytest.fail('A local batch cannot launch or claim a source query')
    assert run_one(root,NoDesktop(),kind='download',table_id=prior['table_id'])=='selector_scope_repartitioned'
    expected=1 if deadline_expired else min(8,batch)
    with closing(connect(root)) as con:
        after=[dict(con.execute('SELECT * FROM tasks WHERE task_id=?',(t['task_id'],)).fetchone()) for t in tasks]
        assert sum(t['last_error_code']=='source_company_labels_absent_fragment' for t in after)==expected
        assert all(t['state']=='pending' and t['active_attempt_id'] is None and t['actual_rows'] is None for t in after)
        assert all(task_request(root,t)==scopes[t['task_id']] for t in after)
        assert all(t['next_attempt_at_utc']==window['next_attempt_at_utc'] for t in after if t['last_error_code'])
        assert dict(con.execute('SELECT * FROM desktop_retry_windows').fetchone())==window
        assert [dict(r) for r in con.execute('SELECT * FROM desktop_attempts')]==attempts
        assert [dict(r) for r in con.execute('SELECT * FROM traffic')]==traffic
    assert all(p.read_bytes()==body for p,body in saved.items())
    assert len(list((root/'selector_replanning').glob('*-company-hint.json')))==expected


@pytest.mark.parametrize('batch',[0,33,True,'8',2.5])
def test_invalid_local_batch_is_rejected_before_any_source_action(registry,batch):
    from downloader.tej_history import configure_runtime_policy
    root,config=registry
    with pytest.raises(ValueError):configure_runtime_policy(root,{**config,'local_planning_batch_size':batch})


@pytest.mark.parametrize('barrier',['unknown','running','interface'])
def test_local_batch_does_not_bypass_shared_source_reconciliation_barriers(registry,monkeypatch,barrier):
    from downloader.tej_history import configure_runtime_policy
    root,config=registry;task,_=pending_scope(root,days=3000)
    configure_runtime_policy(root,{**config,'query_selector_contract':SELECTOR_CONTRACT,'local_planning_batch_size':8})
    with closing(connect(root)) as con,con:
        if barrier=='interface':con.execute("INSERT INTO meta VALUES ('desktop_interface_recovery_required','original')")
        else:con.execute('UPDATE tasks SET state=?,last_error_code=? WHERE task_id=?',
            ('running' if barrier=='running' else 'blocked',None if barrier=='running' else 'unknown_outcome_no_auto_retry',task['task_id']))
        before=[dict(r) for r in con.execute('SELECT * FROM tasks')]
    monkeypatch.setattr('downloader.tej_history._prepare_ready_task',lambda *args:pytest.fail('Barrier must precede all local batch work'))
    class NoDesktop:
        def execute(self,*args):pytest.fail('No source action with an unresolved barrier')
    assert run_one(root,NoDesktop())==('desktop_interface_recovery_required' if barrier=='interface' else 'inflight_requires_recovery')
    with closing(connect(root)) as con:
        assert [dict(r) for r in con.execute('SELECT * FROM tasks')]==before
        assert not con.execute('SELECT 1 FROM traffic').fetchone()


def test_local_batch_balanced_complete_queue_transaction_comparison(registry,tmp_path):
    """Time full finite local queue preparation, not a provider speed claim."""
    import shutil
    import time
    from downloader.tej_desktop_attempts import defer_unsent_prequery,PREQUERY_RETRY_CONTRACT
    from downloader.tej_history import configure_runtime_policy
    from test_tej_prequery_recovery import HealthyReadback
    root,config=registry;prior,request,*_=membership_failure(root)
    defer_unsent_prequery(root,prior['task_id'],HealthyReadback())
    with closing(connect(root)) as con,con:
        con.execute('UPDATE tasks SET priority=1000')
    tasks=[new_hint_tile(root,request,labels=[request['company_labels'][1]],day_offset=i*3)[0] for i in range(8)]
    with closing(connect(root)) as con,con:
        for task in tasks:con.execute('UPDATE tasks SET priority=0 WHERE task_id=?',(task['task_id'],))
        original_traffic=con.execute('SELECT count(*) FROM traffic').fetchone()[0]
    class NoDesktop:
        def execute(self,*args):pytest.fail('Local benchmark must never send a provider query')
    samples=[];signatures=[]
    for pass_id,batch in enumerate((1,8,8,1)):
        trial=tmp_path/('local-transaction-'+str(pass_id));shutil.copytree(root,trial)
        configure_runtime_policy(trial,{**config,'prequery_failure_isolation':True,
            'prequery_recovery_contract':PREQUERY_RETRY_CONTRACT,'local_planning_batch_size':batch})
        calls=0;started=time.perf_counter()
        while True:
            assert calls<8
            assert run_one(trial,NoDesktop(),kind='download',table_id=prior['table_id'])=='selector_scope_repartitioned'
            calls+=1
            with closing(connect(trial)) as con:
                after=[dict(con.execute('SELECT * FROM tasks WHERE task_id=?',(t['task_id'],)).fetchone()) for t in tasks]
            if all(t['last_error_code']=='source_company_labels_absent_fragment' for t in after):break
        seconds=time.perf_counter()-started
        with closing(connect(trial)) as con:
            signatures.append([{
                key:t[key] for key in ('task_id','state','last_error_code','next_attempt_at_utc',
                                      'work_expected_rows','active_attempt_id','actual_rows','safe_prequery_retries')
            } for t in after])
            assert con.execute('SELECT count(*) FROM traffic').fetchone()[0]==original_traffic
        assert len(list((trial/'selector_replanning').glob('*-company-hint.json')))==8
        samples.append({'batch_size':batch,'seconds':seconds,'canonical_calls':calls,'local_fragments':8})
    assert all(signature==signatures[0] for signature in signatures)
    print(json.dumps({'contract':'tej_finite_local_queue_abba_v1','accepted':True,
        'provider_queries_sent':0,'same_scope_and_retry_clocks':True,'samples':samples,
        'interpretation':'Eight original fixture scopes, complete local durable intents and queue commits; not provider throughput or full-history completion'}))


@pytest.mark.parametrize('error',['hash','task','attempt','count','duplicates','out_of_range','bool_position',
    'partial_lookup','changed_source','changed_count','query_possible','clock','manifest','saved_raw'])
def test_bad_membership_evidence_never_partitions_or_requeues(registry,error):
    from downloader.artifact_io import atomic_write_json
    from downloader.tej_desktop_attempts import defer_unsent_prequery
    from test_tej_prequery_recovery import HealthyReadback
    root,_=registry
    parent,_,sidecar,outcome,prepared=membership_failure(root)
    doc=json.loads(sidecar.read_text())
    mutations={'task':('task_id','foreign'),'attempt':('query_attempt_id','foreign'),
        'count':('requested_count',3),'duplicates':('absent_positions',[1,1]),
        'out_of_range':('absent_positions',[4]),'bool_position':('absent_positions',[True]),
        'partial_lookup':('complete_lookup_verified',False),'changed_source':('source_binding_verified',False),
        'changed_count':('source_list_count_stable',False),'query_possible':('market_data_query_submitted',True),
        'clock':('observed_at_utc','2000-01-01T00:00:00+00:00')}
    if error in mutations:
        key,value=mutations[error];doc[key]=value;atomic_write_json(sidecar,doc)
        proof=json.loads(outcome.read_text());proof['company_membership_sha256']=hashlib.sha256(sidecar.read_bytes()).hexdigest()
        atomic_write_json(outcome,proof)
    elif error=='hash':atomic_write_json(sidecar,{'changed':True})
    elif error=='manifest':atomic_write_json(root/'launches'/(parent['active_attempt_id']+'.bridge.json'),{})
    else:atomic_write_json(root/'raw'/(parent['active_attempt_id']+'.json'),{'unverified':True})
    bridge=HealthyReadback()
    with pytest.raises(ValueError):defer_unsent_prequery(root,parent['task_id'],bridge)
    assert bridge.calls==0
    with closing(connect(root)) as con:
        assert con.execute('SELECT state FROM tasks WHERE task_id=?',(parent['task_id'],)).fetchone()[0]=='blocked'
        assert con.execute("SELECT COUNT(*) FROM tasks WHERE state='superseded_company_membership_v1'").fetchone()[0]==0


def test_membership_change_during_fresh_interface_check_retains_the_original_barrier(registry):
    from downloader.artifact_io import atomic_write_json
    from downloader.tej_desktop_attempts import defer_unsent_prequery
    from test_tej_prequery_recovery import HealthyReadback
    root,_=registry
    parent,_,sidecar,*_=membership_failure(root)
    class Changed(HealthyReadback):
        def execute(self,*args):
            result=super().execute(*args)
            atomic_write_json(sidecar,{'changed_after_admission':True})
            return result
    with pytest.raises(ValueError):defer_unsent_prequery(root,parent['task_id'],Changed())
    with closing(connect(root)) as con:
        assert con.execute('SELECT state FROM tasks WHERE task_id=?',(parent['task_id'],)).fetchone()[0]=='blocked'


@pytest.mark.parametrize('companies,dates',[(1,10000),(9,7000),(65,10000),(30,1999)])
def test_uniform_geometry_call_minimum_includes_date_selector_capacity(companies,dates):
    size=company_batch_size(companies,dates,10000,32,2,optimize=True,date_cap=MAX_SELECTOR_LABELS)
    costs=[rectangle_queries(companies,dates,10000,n,2,MAX_SELECTOR_LABELS) for n in range(1,min(32,companies)+1)]
    assert rectangle_queries(companies,dates,10000,size,2,MAX_SELECTOR_LABELS)==min(costs)


def test_unsent_table_retry_window_is_transferred_without_reset(registry):
    root,_=registry;parent,_=pending_scope(root)
    due='2099-01-01T00:00:00+00:00'
    with closing(connect(root)) as con,con:
        con.execute('UPDATE tasks SET next_attempt_at_utc=? WHERE task_id=?',(due,parent['task_id']))
        con.execute('INSERT INTO desktop_retry_windows VALUES(?,?,?,?,?,?,?)',
            (parent['table_id'],parent['task_id'],'retained-original-attempt',444,due,'prequery_failure_deferred','original-audit.json'))
        parent=dict(con.execute('SELECT * FROM tasks WHERE task_id=?',(parent['task_id'],)).fetchone())
        assert repartition_selector_pending(con,root,parent)
    with closing(connect(root)) as con:
        retry=dict(con.execute('SELECT * FROM desktop_retry_windows').fetchone())
        assert retry['consecutive_failures']==444 and retry['next_attempt_at_utc']==due
        assert retry['attempt_id']=='retained-original-attempt' and retry['audit_path']=='original-audit.json'
        assert retry['task_id']!=parent['task_id']
        assert con.execute('SELECT state FROM tasks WHERE task_id=?',(retry['task_id'],)).fetchone()[0]=='pending'


@pytest.mark.parametrize('fragment_only',[True,False])
def test_selector_partition_preserves_another_company_fragments_retry_window(registry,fragment_only):
    from downloader.tej_planning import COMPANY_MEMBERSHIP_DEFERRED
    root,_=registry
    parent,_=pending_scope(root)
    other,_=pending_scope(root,companies=2,days=3)
    due='2099-01-01T00:00:00+00:00'
    code=COMPANY_MEMBERSHIP_DEFERRED if fragment_only else 'prequery_failure_deferred'
    with closing(connect(root)) as con,con:
        con.execute('INSERT INTO desktop_retry_windows VALUES(?,?,?,?,?,?,?)',
            (other['table_id'],other['task_id'],'retained-other-attempt',7,due,code,'other-audit.json'))
        original=dict(con.execute('SELECT * FROM desktop_retry_windows').fetchone())
        if not fragment_only:
            with pytest.raises(ValueError,match='Another original fragment'):
                repartition_selector_pending(con,root,parent)
            return
        assert repartition_selector_pending(con,root,parent)
        assert dict(con.execute('SELECT * FROM desktop_retry_windows').fetchone())==original
        assert dict(con.execute('SELECT * FROM tasks WHERE task_id=?',(other['task_id'],)).fetchone())==other
        assert con.execute('SELECT COUNT(*) FROM traffic').fetchone()[0]==0
        children=[dict(r) for r in con.execute("SELECT * FROM tasks WHERE kind='download' AND state='pending' AND task_id<>?",(other['task_id'],))]
        assert len(children)==5 and sum(r['work_expected_rows'] for r in children)==parent['work_expected_rows']


def test_native_ui_is_not_invoked_for_locally_impossible_scope(registry):
    from downloader.tej_history import configure_runtime_policy
    root,config=registry;parent,_=pending_scope(root)
    configure_runtime_policy(root,{**config,'query_selector_contract':SELECTOR_CONTRACT})
    class NeverInvoke:
        def execute(self,*_):
            pytest.fail('Oversized input must be repartitioned before source UI')
    assert run_one(root,NeverInvoke(),kind='download',table_id=parent['table_id'])=='selector_scope_repartitioned'
    with closing(connect(root)) as con:
        assert not con.execute('SELECT 1 FROM traffic').fetchone()


def test_changed_denominator_or_previously_attempted_scope_is_not_repartitioned(registry):
    root,_=registry;parent,_=pending_scope(root)
    with closing(connect(root)) as con,con:
        for changes in ({'work_expected_rows':1},{'attempted_at_utc':'2026-10-01T00:00:00+00:00'},
                        {'actual_rows':0}):
            with pytest.raises(ValueError):
                repartition_selector_pending(con,root,{**parent,**changes})
        assert con.execute("SELECT COUNT(*) FROM tasks WHERE kind='download'").fetchone()[0]==1


@pytest.mark.parametrize('keys',[1,3])
def test_selector_geometry_keeps_snapshot_and_native_record_semantics(keys):
    from downloader.tej_key_layout import KEY1_CONTRACT, KEY3_CONTRACT
    request=base_request(3)
    request={**request,'source_key_mode':keys,'key_layout_contract':KEY1_CONTRACT if keys==1 else KEY3_CONTRACT,
        'max_rows':10000,'max_cells':400000,'frequency':'snapshot' if keys==1 else 'daily'}
    if keys==3:
        request.update(native_record_density_hint=50,record_capacity_contract='native_row_capacity_replanning_v1')
    companies=['A','B']; dates=[] if keys==1 else [
        (date(2000,1,1)+timedelta(days=i)).strftime('%Y/%m/%d') for i in range(4000)]
    plan=build_plan(request,companies,dates,{'max_companies_per_export':2,'query_tiling_contract':TILING_CONTRACT,
        'query_selector_contract':SELECTOR_CONTRACT},[])
    assert plan['query_selector_contract']==SELECTOR_CONTRACT
    assert sum(len(next_request(plan,i)['company_labels'])*(1 if keys==1 else len(next_request(plan,i)['date_labels']))
        for i in range(plan['total_queries']))==len(companies)*(1 if keys==1 else len(dates))
    assert all(next_request(plan,i)['source_key_mode']==keys for i in range(plan['total_queries']))
