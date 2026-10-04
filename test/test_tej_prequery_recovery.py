"""SQLite interruption cannot erase a structured proof that no query was sent."""
from contextlib import closing
from datetime import UTC,datetime
import json
import sqlite3
import uuid

import pytest

from downloader.artifact_io import atomic_write_json
from downloader.tej_desktop_attempts import begin_attempt, finish_attempt, recover_prequery_outcome
from downloader.tej_history import connect, task_request
from downloader.tej_scheduler import recover_complete_local_response
from test_tej_preview_capacity import ready as ready, registry as registry, unresolved


def prepare(root,task,*,code='local_date_input_failed_before_preview'):
    request=task_request(root,task);attempt=task['task_id']+'-'+uuid.uuid4().hex
    prepared=root/'requests'/(attempt+'.json');response=root/'raw'/(attempt+'.json')
    atomic_write_json(prepared,{**request,'task_id':task['task_id'],'query_attempt_id':attempt})
    begin_attempt(root,task,attempt,prepared,response)
    proof={'contract_version':4,'provider':'tej_smart_wizard','action':'download','task_id':task['task_id'],
           **{k:request[k] for k in ('type','smart_id','table')},
           'market_data_query_submission_possible':False,'error_code':code,'observed_at_utc':datetime.now(UTC).isoformat()}
    path=root/'raw'/(attempt+'.json.outcome.json');atomic_write_json(path,proof)
    return attempt,path,prepared,proof


def test_prequery_crash_recovery_is_evidence_preserving_idempotent_and_uses_shared_budget(ready):
    root,config,_=ready;task=unresolved(root);attempt,path,prepared,proof=prepare(root,task)
    before=[path.read_bytes(),prepared.read_bytes()]
    assert recover_complete_local_response(root)
    assert [path.read_bytes(),prepared.read_bytes()]==before
    assert not recover_prequery_outcome(root,task['task_id'])
    with closing(connect(root)) as con:
        row=con.execute('SELECT state,safe_prequery_retries,actual_rows FROM tasks WHERE task_id=?',(task['task_id'],)).fetchone()
        assert tuple(row)==('pending',1,None)
        assert con.execute('SELECT state FROM desktop_attempts WHERE attempt_id=?',(attempt,)).fetchone()[0]=='proven_not_submitted'
    audit=json.loads((root/'local_prequery_recovery'/(attempt+'.json')).read_text())
    assert audit['state']=='committed' and audit['provider_queries_sent']==0 and audit['automatic_unknown_replay'] is False
    for number in (2,3):
        with closing(connect(root)) as con,con:
            con.execute("UPDATE tasks SET state='running' WHERE task_id=?",(task['task_id'],))
            task=dict(con.execute('SELECT * FROM tasks WHERE task_id=?',(task['task_id'],)).fetchone())
        prepare(root,task)
        assert recover_prequery_outcome(root,task['task_id'])
        with closing(connect(root)) as con:
            row=con.execute('SELECT state,safe_prequery_retries,last_error_code FROM tasks WHERE task_id=?',(task['task_id'],)).fetchone()
            assert tuple(row)==(('pending',2,'local_date_input_failed_before_preview') if number==2 else
                                ('blocked',2,'date_input_prequery_needs_review'))


@pytest.mark.parametrize('code',['desktop_foreground_unavailable_before_preview','desktop_context_unavailable_before_preview'])
def test_foreground_wait_recovery_does_not_consume_local_bug_budget(ready,code):
    root,config,_=ready;task=unresolved(root);prepare(root,task,code=code)
    assert recover_prequery_outcome(root,task['task_id'])
    with closing(connect(root)) as con:
        assert tuple(con.execute('SELECT state,safe_prequery_retries FROM tasks WHERE task_id=?',(task['task_id'],)).fetchone())==('pending',0)
    assert json.loads((root/'worker_status.json').read_text())['state']=='desktop_unavailable'


@pytest.mark.parametrize('field,value',[
    ('market_data_query_submission_possible',True),('market_data_query_submission_possible',None),
    ('market_data_query_submission_possible',0),('error_code','unknown'),('task_id','foreign'),
    ('contract_version',3),('provider','other'),('action','plan'),('table','foreign'),('type','foreign'),
    ('observed_at_utc','2000-01-01T00:00:00+00:00'),('observed_at_utc','2099-01-01T00:00:00+00:00'),
    ('observed_at_utc','2026-10-02T14:00:00'),('observed_at_utc',None),('observed_at_utc','invalid')])
def test_unproved_outcome_keeps_source_barrier_and_never_changes_retry_budget(ready,field,value):
    root,config,_=ready;task=unresolved(root);attempt,path,prepared,proof=prepare(root,task)
    proof[field]=value;atomic_write_json(path,proof)
    with pytest.raises(ValueError):recover_prequery_outcome(root,task['task_id'])
    assert not recover_complete_local_response(root)
    with closing(connect(root)) as con:
        row=con.execute('SELECT state,safe_prequery_retries FROM tasks WHERE task_id=?',(task['task_id'],)).fetchone()
        assert tuple(row)==('blocked',0)


@pytest.mark.parametrize('mutation',['prepared','response','adopted','registration'])
def test_changed_or_ambiguous_prequery_evidence_is_not_replayed(ready,mutation):
    root,config,_=ready;task=unresolved(root);attempt,path,prepared,proof=prepare(root,task)
    if mutation=='prepared':
        request=json.loads(prepared.read_text());request['fields']=[];atomic_write_json(prepared,request)
    elif mutation=='response':atomic_write_json(root/'raw'/(attempt+'.json'),{'unexpected':'response'})
    else:
        with closing(connect(root)) as con,con:
            if mutation=='adopted':con.execute('UPDATE tasks SET actual_rows=1 WHERE task_id=?',(task['task_id'],))
            else:con.execute("UPDATE desktop_attempts SET request_path='foreign.json' WHERE attempt_id=?",(attempt,))
    with pytest.raises(ValueError):recover_prequery_outcome(root,task['task_id'])
    assert not (root/'local_prequery_recovery').exists()


def test_busy_attempt_annotation_does_not_mask_stronger_durable_source_outcome(ready,monkeypatch):
    root,config,_=ready;task=unresolved(root);attempt,path,prepared,proof=prepare(root,task)
    import downloader.tej_desktop_attempts as attempts
    def busy(*_):raise sqlite3.OperationalError('database is locked')
    monkeypatch.setattr(attempts,'_connect',busy)
    finish_attempt(root,attempt,'proven_not_submitted')
    assert path.is_file() and prepared.is_file()
    note=json.loads((root/'diagnostics'/(attempt+'-attempt_state_deferred.json')).read_text())
    assert note['automatic_query_replay'] is False and note['state']=='metadata_busy_annotation_deferred'


def test_inventory_count_plan_uses_covering_index_not_historical_request_blobs(ready):
    root,config,_=ready
    with closing(connect(root)) as con:
        details=' '.join(r[-1] for r in con.execute("EXPLAIN QUERY PLAN SELECT kind,state,last_error_code,COUNT(*) "
            "FROM tasks WHERE scope_contract='editable_source_scope_v1' GROUP BY kind,state,last_error_code"))
    assert 'COVERING INDEX tasks_scope_state_counts' in details


def test_shared_worker_metadata_wait_keeps_unresolved_evidence_not_an_unsafe_retry(ready,monkeypatch):
    from downloader.tej_history import run_one
    root,config,_=ready
    class Interrupted:
        def execute(self,root,task):
            prepare(root,task)
            raise sqlite3.OperationalError('database is locked')
    assert run_one(root,Interrupted())=='local_metadata_busy'
    assert json.loads((root/'worker_status.json').read_text())['state']=='waiting_metadata'
    # No second bridge call is needed: the current exact negative proof is
    # reconciled locally before the ordinary shared retry policy can proceed.
    assert recover_complete_local_response(root)


def test_source_may_have_been_submitted_metadata_wait_never_replays(ready):
    from downloader.tej_history import run_one
    root,config,_=ready
    class Unknown:
        def execute(self,root,task):
            request=task_request(root,task);attempt=task['task_id']+'-'+uuid.uuid4().hex
            prepared=root/'requests'/(attempt+'.json');response=root/'raw'/(attempt+'.json')
            atomic_write_json(prepared,{**request,'task_id':task['task_id'],'query_attempt_id':attempt})
            begin_attempt(root,task,attempt,prepared,response)
            raise sqlite3.OperationalError('database is locked')
    assert run_one(root,Unknown())=='local_metadata_busy'
    assert not recover_complete_local_response(root)
    assert run_one(root,None)=='inflight_requires_recovery'


class HealthyReadback:
    """Only the canonical read-only UI check; never a data/source query."""
    def __init__(self, **changes):
        self.changes=changes
        self.calls=0

    def execute(self, root, task):
        request=json.loads(task['request_json'])
        assert request['action']=='confirm_metadata_error_cleared'
        self.calls+=1
        payload={**{k:request[k] for k in ('contract_version','action','type','smart_id','table')},
            'task_id':task['task_id'],
            'provider':'tej_smart_wizard','observed_at_utc':datetime.now(UTC).isoformat(),
            'source_binding_stable':True,'vendor_notices_absent':True,'source_selectors_enabled':True,
            'company_group_enabled':True,'market_data_query_submitted':False,
            'source_axes_adopted':False,'credentials_read':False,**self.changes}
        output=root/'raw'/(task['task_id']+'-'+uuid.uuid4().hex+'.json')
        atomic_write_json(output,payload)
        return payload,output,0.01


def exhausted(root, task=None):
    task=task or unresolved(root)
    attempt,path,prepared,proof=prepare(root,task,code='local_list_selection_failed_before_preview')
    finish_attempt(root,attempt,'proven_not_submitted')
    with closing(connect(root)) as con,con:
        con.execute("UPDATE tasks SET state='blocked',safe_prequery_retries=2,"
            "last_error_code='list_selection_prequery_needs_review' WHERE task_id=?",(task['task_id'],))
        return dict(con.execute('SELECT * FROM tasks WHERE task_id=?',(task['task_id'],)).fetchone()),path,prepared


def test_durable_table_backoff_preserves_unsent_evidence_and_exhausted_immediate_budget(ready):
    from downloader.tej_desktop_attempts import defer_unsent_prequery
    root,_,_=ready;task,proof,prepared=exhausted(root)
    original={p:p.read_bytes() for p in (proof,prepared)}
    bridge=HealthyReadback()
    for number,seconds in enumerate((60,120,240,480,900,900),1):
        result=defer_unsent_prequery(root,task['task_id'],bridge)
        assert result['state']=='prequery_failure_deferred'
        assert result['consecutive_failures']==number and result['backoff_seconds']==seconds
        assert result['provider_queries_sent']==0 and result['source_rows_adopted'] is False
        with closing(connect(root)) as con:
            row=con.execute('SELECT state,safe_prequery_retries,actual_rows,receipt_path FROM tasks WHERE task_id=?',
                (task['task_id'],)).fetchone()
            assert tuple(row)==('pending',2,None,None)
            assert con.execute('SELECT COUNT(*) FROM desktop_retry_windows').fetchone()[0]==1
        audit=json.loads((root/result['audit']).read_text())
        assert audit['state']=='committed' and audit['unknown_outcome_auto_retry'] is False
        assert all(p.read_bytes()==body for p,body in original.items())
        # A repeated reconciliation does not reuse the same negative outcome.
        with pytest.raises(ValueError):defer_unsent_prequery(root,task['task_id'],bridge)
        assert bridge.calls==number
        if number<6:
            task,_,_=exhausted(root,task)


@pytest.mark.parametrize('mutation',['raw','stage','unfinished','classification','foreign_running','unknown',
    'interface','period','query_possible','old_clock','wrong_task','changed_request'])
def test_table_retry_needs_exact_finished_negative_proof_before_any_ui_check(ready,mutation):
    from downloader.tej_desktop_attempts import defer_unsent_prequery
    root,_,_=ready;task,proof,prepared=exhausted(root);attempt=task['active_attempt_id']
    if mutation in ('raw','stage'):
        atomic_write_json(root/'raw'/(attempt+('.json' if mutation=='raw' else '.json.stage.json')),{'stage':'unknown'})
    elif mutation in ('query_possible','old_clock','wrong_task'):
        doc=json.loads(proof.read_text())
        field,value={'query_possible':('market_data_query_submission_possible',True),
            'old_clock':('observed_at_utc','2000-01-01T00:00:00+00:00'),
            'wrong_task':('task_id','foreign')}[mutation]
        doc[field]=value;atomic_write_json(proof,doc)
    elif mutation=='changed_request':
        doc=json.loads(prepared.read_text());doc['fields']=[];atomic_write_json(prepared,doc)
    else:
        with closing(connect(root)) as con,con:
            if mutation=='unfinished':con.execute("UPDATE desktop_attempts SET state='prepared',finished_at_utc=NULL")
            elif mutation=='classification':con.execute("UPDATE tasks SET last_error_code='date_input_prequery_needs_review'")
            elif mutation in ('interface','period'):
                con.execute('INSERT INTO meta VALUES(?,?)',
                    ('desktop_interface_recovery_required' if mutation=='interface' else 'source_period_replan_required','1'))
            else:
                con.execute("INSERT INTO tasks(task_id,table_id,kind,phase,priority,request_json,state,last_error_code) "
                    "VALUES('foreign','foreign','download','P1',100,'{}',?,?)",
                    ('running' if mutation=='foreign_running' else 'blocked','unknown_outcome_no_auto_retry'))
    bridge=HealthyReadback()
    with pytest.raises(ValueError):defer_unsent_prequery(root,task['task_id'],bridge)
    assert bridge.calls==0
    with closing(connect(root)) as con:
        assert con.execute('SELECT COUNT(*) FROM desktop_retry_windows').fetchone()[0]==0


@pytest.mark.parametrize('field,value',[
    ('source_binding_stable',False),('vendor_notices_absent',False),('source_selectors_enabled',False),
    ('company_group_enabled',False),('market_data_query_submitted',True),('source_axes_adopted',True),
    ('credentials_read',True),('provider','other'),('table','other'),
    ('observed_at_utc','2000-01-01T00:00:00+00:00'),('observed_at_utc',None)])
def test_stale_or_unusable_shared_ui_never_unblocks_the_table(ready,field,value):
    from downloader.tej_desktop_attempts import defer_unsent_prequery
    root,_,_=ready;task,_,_=exhausted(root);bridge=HealthyReadback(**{field:value})
    with pytest.raises(ValueError):defer_unsent_prequery(root,task['task_id'],bridge)
    assert bridge.calls==1
    with closing(connect(root)) as con:
        assert con.execute('SELECT state FROM tasks WHERE task_id=?',(task['task_id'],)).fetchone()[0]=='blocked'


@pytest.mark.parametrize('kind',[None,'download','discover'])
def test_table_window_excludes_all_other_fragments_but_not_other_tables(ready,kind):
    from downloader.tej_desktop_attempts import defer_unsent_prequery
    from downloader.tej_history import _ready_task
    root,_,_=ready;task,_,_=exhausted(root);result=defer_unsent_prequery(root,task['task_id'],HealthyReadback())
    with closing(connect(root)) as con,con:
        for identity,table in [('a-same',task['table_id']),('b-other','other')]:
            con.execute("INSERT INTO tasks(task_id,table_id,kind,phase,priority,request_json) VALUES(?,?,?,?,?,?)",
                (identity,table,kind or 'download','P1',0,'{}'))
        con.execute("INSERT OR REPLACE INTO meta VALUES('scheduler_last_download_table',?)",(task['table_id'],))
        assert _ready_task(con,datetime.now(UTC).isoformat(),kind,{})['task_id']=='b-other'
        assert _ready_task(con,datetime.now(UTC).isoformat(),'download',{},task['table_id']) is None
        # Once due, only the exact original fragment probes this table.
        assert _ready_task(con,result['next_attempt_at_utc'],'download',{},task['table_id'])['task_id']==task['task_id']


def test_accepted_source_response_clears_only_its_own_retry_window(ready):
    from downloader.tej_desktop_attempts import defer_unsent_prequery
    from downloader.tej_history import run_one
    root,_,bridge=ready;task,_,_=exhausted(root)
    defer_unsent_prequery(root,task['task_id'],HealthyReadback())
    with closing(connect(root)) as con,con:
        con.execute("UPDATE desktop_retry_windows SET next_attempt_at_utc='2000-01-01T00:00:00+00:00'")
        con.execute("UPDATE tasks SET next_attempt_at_utc=NULL WHERE task_id=?",(task['task_id'],))
        con.execute("INSERT INTO desktop_retry_windows VALUES('other','other','other',1,'2099-01-01T00:00:00+00:00','other','other')")
    class FreshAttempt:
        def execute(self, root, task):
            from downloader.tej_history import PREVIEW_SUBMISSION_CONTRACT, task_request
            from test_tej_history import scope_proof
            request=task_request(root,task);attempt=task['task_id']+'-'+uuid.uuid4().hex
            prepared=root/'requests'/(attempt+'.json');output=root/'raw'/(attempt+'.json')
            atomic_write_json(prepared,{**request,'task_id':task['task_id'],'query_attempt_id':attempt})
            begin_attempt(root,task,attempt,prepared,output)
            stage={**{k:request[k] for k in ('type','smart_id','table','fields','company_labels','date_labels')},
                'contract_version':4,'task_id':task['task_id'],'query_attempt_id':attempt,
                'stage':'prepreview_verified','source_scope_proof':scope_proof(),
                'preview_submission_contract':PREVIEW_SUBMISSION_CONTRACT}
            atomic_write_json(root/'raw'/(attempt+'.json.stage.json'),stage)
            payload,_,seconds=bridge.execute(root,task)
            payload={**payload,'query_attempt_id':attempt,'preview_submission_contract':PREVIEW_SUBMISSION_CONTRACT,
                'fresh_preview_transition_verified':True}
            atomic_write_json(output,payload)
            finish_attempt(root,attempt,'response_received_not_yet_adopted')
            return payload,output,seconds
    assert run_one(root,FreshAttempt(),kind='download',table_id=task['table_id'])=='completed_task'
    with closing(connect(root)) as con:
        assert [r[0] for r in con.execute('SELECT table_id FROM desktop_retry_windows')]==['other']


def test_watch_restart_recovers_only_proved_unsent_window_and_keeps_other_work_running(ready):
    from downloader.tej_desktop_attempts import PREQUERY_RETRY_CONTRACT
    from downloader.tej_scheduler import watch_queue
    from test_tej_scheduler import InstantWait
    root,config,_=ready;task,_,_=exhausted(root);bridge=HealthyReadback();calls=[]
    config={**config,'automation':{'enabled':True},'prequery_failure_isolation':True,
        'prequery_recovery_contract':PREQUERY_RETRY_CONTRACT}
    watch_queue(root,bridge,config,stop=InstantWait(),max_cycles=1,emit=lambda *_,**__:None,
        runner=lambda *_:calls.append('other-work') or 'completed_task')
    assert bridge.calls==1 and calls==['other-work']
    status=json.loads((root/'scheduler_status.json').read_text())
    assert status['paused_reason'] is None and status['completed_tasks']==1
    # Persisted windows remain after a different supervisor instance starts.
    watch_queue(root,bridge,config,stop=InstantWait(),max_cycles=1,emit=lambda *_,**__:None,
        runner=lambda *_:'idle')
    assert bridge.calls==1


@pytest.mark.parametrize('field,value',[
    ('prequery_failure_isolation',1),('prequery_recovery_contract','unreviewed'),
    ('prequery_retry_base_seconds',True),('prequery_retry_base_seconds',0),
    ('prequery_retry_max_seconds',4000),('prequery_retry_max_seconds',30)])
def test_invalid_retry_policy_is_rejected_before_desktop_actions(ready,field,value):
    from downloader.tej_desktop_attempts import PREQUERY_RETRY_CONTRACT
    from downloader.tej_history import configure_runtime_policy
    root,config,_=ready
    config={**config,'prequery_failure_isolation':True,'prequery_recovery_contract':PREQUERY_RETRY_CONTRACT}
    with pytest.raises(ValueError):configure_runtime_policy(root,{**config,field:value})


def test_dashboard_shows_only_metadata_of_deferred_table_and_unknown_time_is_not_zero(ready):
    from downloader.tej_desktop_attempts import defer_unsent_prequery
    from stockagent.live.tej_dashboard import build_tej_public_status
    root,_,_=ready;task,_,_=exhausted(root)
    result=defer_unsent_prequery(root,task['task_id'],HealthyReadback())
    public=build_tej_public_status(root.parent)
    assert public['activity']['deferred_tasks_total']==1
    row=public['activity']['deferred_tasks'][0]
    assert row['next_attempt_at_utc']==result['next_attempt_at_utc']
    assert 0<row['retry_after_seconds']<=60 and row['consecutive_failures']==1
    assert 'audit_path' not in json.dumps(public) and 'query_attempt_id' not in json.dumps(public)
    table=next(t for t in public['tables'] if t['table_id']==task['table_id'])
    assert table['state']=='waiting_local_retry' and table['remaining_seconds'] is None
    assert public['eta']['forecast']['phases'][0]['deferred_dependency_tasks']==1
    with closing(connect(root)) as con,con:
        con.execute("UPDATE desktop_retry_windows SET next_attempt_at_utc='invalid',error_code='SECRET'")
    public=build_tej_public_status(root.parent)
    row=public['activity']['deferred_tasks'][0]
    assert row['retry_after_seconds'] is None and 'SECRET' not in json.dumps(public)
