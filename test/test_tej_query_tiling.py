from contextlib import closing
from datetime import date, timedelta
import hashlib
import json
import math

import pytest

from downloader.tej_history import compact_request, connect, configure_preview_planning, run_one, stable_id, SOURCE_SCOPE_CONTRACT
from downloader.tej_planning import TILING_CONTRACT, build_plan, company_batch_size, next_request, refill_ready_plans, upgrade_query_tiling
from test_tej_planning import DynamicBridge, base_request, configuration
from test_tej_planning import registry as registry  # Explicitly shared pytest fixture.


@pytest.mark.parametrize('companies',[1,9,31,32,65,735,7366])
@pytest.mark.parametrize('dates',[3,56,223,670,14948])
@pytest.mark.parametrize('rows',[6665,9999])
def test_company_choice_minimizes_uniform_rectangular_calls_independently(companies,dates,rows):
    size=company_batch_size(companies,dates,rows,32,2,optimize=True)
    def calls(n):
        groups=[n]*(companies//n)+([companies%n] if companies%n else [])
        return sum(math.ceil(dates/(rows//g)) for g in groups)
    assert calls(size)==min(calls(n) for n in range(1,min(32,companies)+1))
    assert calls(size)<=calls(32)


def test_typical_quarterly_company_tail_is_removed_without_bigger_payload():
    companies=7366;periods=223;rows=6665
    size=company_batch_size(companies,periods,rows,32,2,optimize=True)
    assert size==29
    old=math.ceil(companies/32)*2
    new=math.ceil(companies/29)
    assert new < old and size*periods<=rows


def setup_migration(registry,monkeypatch):
    root,config=registry;config={**config,'max_companies_per_export':4}
    companies=[str(1000+i)+'=>Company'+str(i) for i in range(8)]
    periods=[(date(2014,1,2)+timedelta(days=i)).strftime('%Y/%m/%d') for i in range(3)]
    monkeypatch.setattr('test_tej_planning.axes',lambda:(companies,periods))
    with closing(connect(root)) as con,con:
        con.execute("UPDATE meta SET value=? WHERE key='config'",(json.dumps(config),))
    configure_preview_planning(root,config)
    bridge=DynamicBridge();assert run_one(root,bridge)=='completed_task'
    return root, {**config,'query_tiling_contract':TILING_CONTRACT}, bridge


def test_upgrade_is_recoverable_reduces_calls_and_preserves_all_completed_cells(registry,monkeypatch):
    root,config,bridge=setup_migration(registry,monkeypatch)
    assert run_one(root,bridge)=='completed_task'
    with closing(connect(root)) as con,con:
        refill_ready_plans(con)
        old=dict(con.execute('SELECT * FROM download_plans').fetchone())
        completed=dict(con.execute("SELECT * FROM tasks WHERE kind='download' AND state='complete'").fetchone())
        original_bytes=(root/completed['output_path']).read_bytes()
    result=upgrade_query_tiling(root,config)
    assert result['tables_optimized']==1 and result['queries_saved']>0
    assert result['completed_scopes_preserved'] and result['provider_queries_sent']==0
    audit=json.loads((root/result['audit']).read_text())
    item=audit['tables'][0];backup=root/item['original_plan_backup']
    assert hashlib.sha256(backup.read_bytes()).hexdigest()==item['original_plan_backup_sha256']
    assert json.loads(backup.read_text())==old and audit['state']=='committed'
    assert (root/completed['output_path']).read_bytes()==original_bytes
    with closing(connect(root)) as con:
        assert dict(con.execute('SELECT * FROM tasks WHERE task_id=?',(completed['task_id'],)).fetchone())==completed
        assert con.execute("SELECT COUNT(*) FROM tasks WHERE state='superseded_query_tiling_v1'").fetchone()[0]==1
        plan=json.loads(con.execute('SELECT plan_json FROM download_plans').fetchone()[0])
        assert plan['query_tiling_contract']==TILING_CONTRACT
        assert plan['completed_work_rows']==completed['work_expected_rows']
    assert upgrade_query_tiling(root,config)['already_installed']
    for _ in range(plan['total_queries']):
        assert run_one(root,bridge)=='completed_task'
    assert run_one(root,bridge)=='idle'
    seen=set()
    for request in bridge.requests:
        if request['action']!='download':continue
        for f in request['fields']:
            for c in request['company_labels']:
                for d in request['date_labels']:
                    assert (f,c,d) not in seen
                    seen.add((f,c,d))
    assert len(seen)==59*8*3


@pytest.mark.parametrize('state',['running','blocked'])
def test_source_action_barrier_prevents_migration(registry,monkeypatch,state):
    root,config,bridge=setup_migration(registry,monkeypatch)
    with closing(connect(root)) as con,con:
        refill_ready_plans(con)
        con.execute("UPDATE tasks SET state=? WHERE kind='download'",(state,))
    with pytest.raises(ValueError,match='Unresolved'):upgrade_query_tiling(root,config)
    assert not (root/'planning_migrations').exists()


def test_migration_rejects_changed_capacity_or_corrupt_original(registry,monkeypatch):
    root,config,_=setup_migration(registry,monkeypatch)
    with pytest.raises(ValueError,match='capacity'):upgrade_query_tiling(root,{**config,'max_rows_per_export':11})
    with closing(connect(root)) as con,con:
        plan=json.loads(con.execute('SELECT plan_json FROM download_plans').fetchone()[0])
        plan['total_queries']+=1
        con.execute('UPDATE download_plans SET plan_json=?',(json.dumps(plan),))
    with pytest.raises(ValueError,match='fingerprint'):upgrade_query_tiling(root,config)


def test_unknown_geometry_contract_is_rejected_before_planning():
    request=base_request()
    with pytest.raises(ValueError,match='tiling contract'):
        build_plan(request,['a'],['2014/01/02'],{**configuration(),'query_tiling_contract':'guessed'},[])


def test_fresh_planner_records_policy_and_repeated_configuration_is_safe(registry):
    root,config=registry;config={**config,'query_tiling_contract':TILING_CONTRACT}
    configure_preview_planning(root,config)
    assert configure_preview_planning(root,config)['already_installed']
    with closing(connect(root)) as con:
        assert con.execute("SELECT value FROM meta WHERE key='query_tiling_contract'").fetchone()[0]==TILING_CONTRACT


def test_pending_native_probe_is_preserved_and_table_upgrade_can_resume(registry,monkeypatch):
    root,config,bridge=setup_migration(registry,monkeypatch)
    with closing(connect(root)) as con,con:
        definition=dict(con.execute('SELECT * FROM tables').fetchone())
        former=json.loads(con.execute('SELECT plan_json FROM download_plans').fetchone()[0])
        request={**former['request'],'action':'download',
                 'company_labels':former['companies'][-1:], 'date_labels':former['dates'][-1:]}
        key=stable_id([definition['table_id'],request])
        encoded=json.dumps(compact_request(request,definition))
        con.execute('INSERT INTO tasks(task_id,table_id,kind,phase,priority,request_json,expected_rows,work_expected_rows,scope_contract) VALUES(?,?,?,?,?,?,?,?,?)',
                    (key,definition['table_id'],'download','P1',99,encoded,1,former['field_batches'],SOURCE_SCOPE_CONTRACT))
    result=upgrade_query_tiling(root,config)
    assert result['tables_optimized']==0 and result['queries_saved']==0
    assert result['deferred_tables'][0]['pending_native_tasks']==[key]
    with closing(connect(root)) as con,con:
        assert con.execute('SELECT plan_json FROM download_plans').fetchone()[0]==json.dumps(former,ensure_ascii=False,separators=(',',':'))
        assert con.execute('SELECT state FROM tasks WHERE task_id=?',(key,)).fetchone()[0]=='pending'
        # Simulate a completed native receipt. We never relabel it in the
        # migration itself; the original complete lifecycle does that.
        con.execute("UPDATE tasks SET state='complete' WHERE task_id=?",(key,))
    resumed=upgrade_query_tiling(root,config)
    assert resumed['completed_scopes_preserved'] and not resumed['deferred_tables']
    assert upgrade_query_tiling(root,config)['already_installed']


def test_wrong_pending_partition_fingerprint_remains_a_hard_barrier(registry,monkeypatch):
    root,config,_=setup_migration(registry,monkeypatch)
    with closing(connect(root)) as con,con:
        refill_ready_plans(con)
        row=con.execute("SELECT task_id,request_json FROM tasks WHERE kind='download' AND state='pending'").fetchone()
        request=json.loads(row['request_json']);request['field_partition']['plan_fingerprint']='unverified'
        con.execute('UPDATE tasks SET request_json=? WHERE task_id=?',(json.dumps(request),row['task_id']))
    with pytest.raises(ValueError,match='outside the exact'):upgrade_query_tiling(root,config)


def capacity_config(config):
    from downloader.tej_planning import LOCAL_CAPACITY_CONTRACT
    return {**config,'query_capacity_contract':LOCAL_CAPACITY_CONTRACT,
            'max_rows_per_export':20,'max_cells_per_export':1000,'max_companies_per_export':8,
            'query_interval_contract':'minimum_query_start_interval_v1'}


def test_explicit_capacity_upgrade_reduces_calls_with_exact_complete_scope_parity(registry,monkeypatch):
    root,config,bridge=setup_migration(registry,monkeypatch)
    assert run_one(root,bridge)=='completed_task'
    with closing(connect(root)) as con,con:
        refill_ready_plans(con)
        completed=[dict(r) for r in con.execute("SELECT * FROM tasks WHERE state='complete'")]
        files={root/r['output_path']:(root/r['output_path']).read_bytes() for r in completed}
    candidate=capacity_config(config)
    result=upgrade_query_tiling(root,candidate,capacity_upgrade=True)
    assert result['queries_saved']>0 and result['provider_queries_sent']==0
    with closing(connect(root)) as con:
        assert [dict(r) for r in con.execute("SELECT * FROM tasks WHERE state='complete'")]==completed
        plan=json.loads(con.execute('SELECT plan_json FROM download_plans').fetchone()[0])
        assert plan['request']['max_cells']==1000 and plan['request']['max_rows']==20
    for _ in range(plan['total_queries']):assert run_one(root,bridge)=='completed_task'
    assert run_one(root,bridge)=='idle'
    from collections import Counter
    seen=Counter((f,c,d) for req in bridge.requests if req['action']=='download'
                 for f in req['fields'] for c in req['company_labels'] for d in req['date_labels'])
    assert len(seen)==59*8*3 and set(seen.values())=={1}
    assert all(p.read_bytes()==body for p,body in files.items())
    assert upgrade_query_tiling(root,candidate,capacity_upgrade=True)['already_installed']


def test_capacity_migration_preserves_inconsistent_table_instead_of_blocking_all_work(registry,monkeypatch):
    root,config,bridge=setup_migration(registry,monkeypatch)
    assert run_one(root,bridge)=='completed_task'
    with closing(connect(root)) as con,con:
        # Simulate an old inconsistent accounting record, not a proved scope.
        con.execute("UPDATE tasks SET work_expected_rows=work_expected_rows+1 WHERE kind='download' AND state='complete'")
        original_plan=dict(con.execute('SELECT * FROM download_plans').fetchone())
        original_tasks=[dict(r) for r in con.execute('SELECT * FROM tasks ORDER BY task_id')]
    result=upgrade_query_tiling(root,capacity_config(config),capacity_upgrade=True)
    assert result['tables_optimized']==0 and result['queries_saved']==0
    assert result['provider_queries_sent']==0
    assert result['deferred_tables'][0]['reason']=='completed_coverage_reconciliation_required'
    assert result['deferred_tables'][0]['receipt_work_rows']>result['deferred_tables'][0]['candidate_completed_work_rows']
    with closing(connect(root)) as con:
        assert dict(con.execute('SELECT * FROM download_plans').fetchone())==original_plan
        assert [dict(r) for r in con.execute('SELECT * FROM tasks ORDER BY task_id')]==original_tasks
    # The old strict tiling operation still rejects this discrepancy.
    with pytest.raises(ValueError,match='Completed scope coverage'):
        upgrade_query_tiling(root,capacity_config(config))


@pytest.mark.parametrize('state,error',[('running',None),('blocked','unknown_outcome_no_auto_retry')])
def test_capacity_upgrade_never_bypasses_unknown_or_active_query(registry,monkeypatch,state,error):
    root,config,_=setup_migration(registry,monkeypatch)
    with closing(connect(root)) as con,con:
        refill_ready_plans(con)
        con.execute("UPDATE tasks SET state=?,last_error_code=? WHERE kind='download'",(state,error))
    with pytest.raises(ValueError,match='Unresolved'):
        upgrade_query_tiling(root,capacity_config(config),capacity_upgrade=True)
    assert not (root/'planning_migrations').exists()


def test_blocked_table_retains_former_capacity_scope_and_all_evidence(registry,monkeypatch):
    root,config,_=setup_migration(registry,monkeypatch)
    with closing(connect(root)) as con,con:
        refill_ready_plans(con)
        con.execute("UPDATE tasks SET state='blocked',last_error_code='source_validation_failed_deferred' WHERE kind='download'")
        tasks=[tuple(r) for r in con.execute('SELECT * FROM tasks')]
        original=con.execute('SELECT plan_json FROM download_plans').fetchone()[0]
    result=upgrade_query_tiling(root,capacity_config(config),capacity_upgrade=True)
    assert result['deferred_tables'][0]['reason']=='blocked_table_scope_unchanged'
    assert result['tables_optimized']==0
    with closing(connect(root)) as con:
        assert [tuple(r) for r in con.execute('SELECT * FROM tasks')]==tasks
        assert con.execute('SELECT plan_json FROM download_plans').fetchone()[0]==original


def test_future_metadata_discovery_uses_explicit_upgraded_bounds(registry):
    root,config=registry
    candidate=capacity_config({**config,'query_tiling_contract':TILING_CONTRACT})
    configure_preview_planning(root,candidate)
    with closing(connect(root)) as con,con:
        con.execute("UPDATE meta SET value=? WHERE key='config'",(json.dumps(candidate),))
    assert run_one(root,DynamicBridge())=='completed_task'
    with closing(connect(root)) as con:
        plan=json.loads(con.execute('SELECT plan_json FROM download_plans').fetchone()[0])
    assert plan['request']['max_cells']==1000 and plan['request']['max_rows']==20


@pytest.mark.parametrize('key',['desktop_interface_recovery_required','source_period_replan_required'])
def test_capacity_upgrade_retains_shared_interface_barriers(registry,monkeypatch,key):
    root,config,_=setup_migration(registry,monkeypatch)
    with closing(connect(root)) as con,con:con.execute('INSERT INTO meta VALUES(?,?)',(key,'required'))
    with pytest.raises(ValueError,match='shared source interface'):
        upgrade_query_tiling(root,capacity_config(config),capacity_upgrade=True)


def test_efficiency_audit_is_same_scope_read_only_and_does_not_overwrite(registry,monkeypatch):
    from scripts.audit_tej_acquisition_efficiency import audit
    root,_,_=setup_migration(registry,monkeypatch)
    original=(root/'queue.sqlite3').read_bytes()
    output=root.parent/'efficiency.json'
    result=audit(root.parent,output,max_cells=1000,max_companies=8)
    assert result['queue_modified'] is False and result['provider_queries_sent']==0
    assert result['source_values_read'] is False and result['comparable_tables']==1
    assert result['after_remaining_queries']<=result['before_remaining_queries']
    assert (root/'queue.sqlite3').read_bytes()==original
    with pytest.raises(FileExistsError):audit(root.parent,output,max_cells=1000,max_companies=8)


def test_efficiency_audit_releases_read_transaction_before_slow_planning(registry,monkeypatch):
    import sqlite3
    import scripts.audit_tej_acquisition_efficiency as module
    root,_,_=setup_migration(registry,monkeypatch)
    original=module.build_plan
    calls=[]
    def check_unlocked(*args,**kwargs):
        # An independent canonical writer can acquire its commit lock while
        # the diagnostic computes; no source call or queue mutation needed.
        with closing(sqlite3.connect(root/'queue.sqlite3',timeout=.05)) as writer:
            writer.execute('BEGIN EXCLUSIVE')
            writer.rollback()
        calls.append(True)
        return original(*args,**kwargs)
    monkeypatch.setattr(module,'build_plan',check_unlocked)
    report=module.audit(root.parent,root.parent/'unlocked-audit.json',max_cells=1000,max_companies=8)
    assert calls==[True]
    assert report['snapshot_contract']=='per_table_bounded_read_snapshot_before_cpu_planning_v1'
    assert report['queue_modified'] is False and report['provider_queries_sent']==0


@pytest.mark.parametrize('fields',[1,9,22,28])
def test_sparse_scope_budget_fills_cells_without_increasing_native_readback(fields):
    from datetime import date,timedelta
    from downloader.tej_planning import SELECTOR_CONTRACT,SCOPE_BUDGET_CONTRACT
    companies=[f'{i:04d}=>Native {i}' for i in range(128)]
    dates=[(date(2010,1,1)+timedelta(days=i)).strftime('%Y/%m/%d') for i in range(600)]
    request={**base_request(fields),'max_rows':10000,'max_cells':400000}
    config={**configuration(),'max_companies_per_export':128,
            'query_tiling_contract':TILING_CONTRACT,'query_selector_contract':SELECTOR_CONTRACT}
    before=build_plan(request,companies,dates,config,[])
    after=build_plan({**request,'max_scope_rows':50000,'query_scope_contract':SCOPE_BUDGET_CONTRACT},
                     companies,dates,config,[])
    assert after['total_queries']<before['total_queries']
    assert before['total_work_rows']==after['total_work_rows']==len(companies)*len(dates)
    seen=set()
    for i in range(after['total_queries']):
        selected=next_request(after,i)
        assert selected['max_rows']==10000
        rows=len(selected['company_labels'])*len(selected['date_labels'])
        assert rows<=50000 and (rows+1)*(fields+2)<=400000
        assert len(selected['date_labels'])<=2000 and len(selected['fields'])+2<=30
        cells={(c,d) for c in selected['company_labels'] for d in selected['date_labels']}
        assert seen.isdisjoint(cells)
        seen.update(cells)
    assert len(seen)==len(companies)*len(dates)


@pytest.mark.parametrize('rows',[10001,50001,True,0,-1])
def test_capacity_upgrade_rejects_unreviewed_row_budgets_before_mutation(registry,rows):
    root,config=registry
    candidate={**capacity_config(config),'query_tiling_contract':TILING_CONTRACT,
               'max_rows_per_export':rows}
    with pytest.raises(ValueError,match='Preview capacity'):
        upgrade_query_tiling(root,candidate,capacity_upgrade=True)
    assert not (root/'planning_migrations').exists()


def test_row_budget_audit_and_upgrade_preserve_complete_values_and_accounting(registry,monkeypatch):
    from scripts.audit_tej_acquisition_efficiency import audit
    from downloader.tej_planning import SCOPE_BUDGET_CONTRACT
    root,config,bridge=setup_migration(registry,monkeypatch)
    assert run_one(root,bridge)=='completed_task'
    output=root.parent/'row_capacity.json'
    result=audit(root.parent,output,max_cells=400000,max_companies=8,max_rows=30,max_scope_rows=50000)
    assert result['proposed_config']['max_rows_per_export']==30
    assert result['proposed_scope_budget']==50000
    assert result['proposed_config']['max_cells_per_export']==400000
    assert result['queue_modified'] is False and result['source_values_read'] is False
    assert result['after_remaining_queries']<result['before_remaining_queries']
    candidate={**capacity_config(config),'max_rows_per_export':30,'max_cells_per_export':400000,
               'max_scope_rows_per_export':50000,'query_scope_contract':SCOPE_BUDGET_CONTRACT}
    result=upgrade_query_tiling(root,candidate,capacity_upgrade=True)
    assert result['queries_saved']>0 and result['completed_scopes_preserved']
    with closing(connect(root)) as con:
        plan=json.loads(con.execute('SELECT plan_json FROM download_plans').fetchone()[0])
    assert plan['request']['max_scope_rows']==50000 and plan['request']['max_rows']==30
    for _ in range(plan['total_queries']):
        assert run_one(root,bridge)=='completed_task'
    assert run_one(root,bridge)=='idle'
    from collections import Counter
    seen=Counter((f,c,d) for req in bridge.requests if req['action']=='download'
                 for f in req['fields'] for c in req['company_labels'] for d in req['date_labels'])
    assert len(seen)==59*8*3 and set(seen.values())=={1}


def test_expanded_scope_accepts_real_sparse_rows_without_inventing_missing_cells(registry,monkeypatch):
    from downloader.artifact_io import atomic_write_json
    from downloader.tej_planning import SCOPE_BUDGET_CONTRACT
    from scripts.audit_tej_history import audit

    root,config,_=setup_migration(registry,monkeypatch)
    config={**capacity_config(config),'max_cells_per_export':400000,'max_rows_per_export':10,
            'max_scope_rows_per_export':50000,'query_scope_contract':SCOPE_BUDGET_CONTRACT}
    assert upgrade_query_tiling(root,config,capacity_upgrade=True)['queries_saved']>0

    class SparseBridge(DynamicBridge):
        def execute(self,root,task):
            payload,output,seconds=super().execute(root,task)
            request=self.requests[-1]
            assert len(request['company_labels'])*len(request['date_labels'])>request['max_rows']
            # Source explicitly returns only three observations from the
            # larger query grid. Capture capacity remains unchanged.
            cells=payload['cells'][:4]
            payload.update(cells=cells,source_grid_rows=3,
                           preview=[len(cells),[[i,row] for i,row in enumerate(cells,1)]])
            atomic_write_json(output,payload)
            return payload,output,seconds

    bridge=SparseBridge()
    with closing(connect(root)) as con:
        queries=con.execute('SELECT total_queries FROM download_plans').fetchone()[0]
    for _ in range(queries):
        assert run_one(root,bridge)=='completed_task'
    assert run_one(root,bridge)=='idle'
    report=audit(root,root.parent/'sparse_values_audit',verify_original_cells=True)
    assert report['accepted'] and report['original_source_cells_verified']
    assert report['exported_rows']==3*queries
    assert report['exported_non_null_cells']==59*3
    with closing(connect(root)) as con:
        plan=json.loads(con.execute('SELECT plan_json FROM download_plans').fetchone()[0])
        assert plan['total_work_rows']==8*3*3 and plan['request']['max_rows']==10
        assert {r[0] for r in con.execute('SELECT exported_non_null_cells FROM features')}=={3}


@pytest.mark.parametrize('value,contract',[(50001,None),(50000,None),(True,'wrong'),(0,'wrong')])
def test_unreviewed_scope_budget_never_enters_plan(value,contract):
    from downloader.tej_planning import SCOPE_BUDGET_CONTRACT
    request={**base_request(),'max_scope_rows':value,
             'query_scope_contract':contract or SCOPE_BUDGET_CONTRACT}
    if value==50000:request.pop('query_scope_contract')
    with pytest.raises(ValueError,match='source scope budget'):
        build_plan(request,['2330'],['2014/01/02'],configuration(),[])


@pytest.mark.parametrize('keys',[1,3])
def test_scope_expansion_never_changes_snapshot_or_multi_record_geometry(keys):
    from downloader.tej_key_layout import KEY1_CONTRACT,KEY3_CONTRACT
    from downloader.tej_planning import apply_scope_budget,SCOPE_BUDGET_CONTRACT
    req={**base_request(),'source_key_mode':keys,
         'key_layout_contract':KEY1_CONTRACT if keys==1 else KEY3_CONTRACT}
    companies=['2330','2317'];dates=[] if keys==1 else ['2014/01/02','2014/01/03']
    config={**configuration(),'query_scope_contract':SCOPE_BUDGET_CONTRACT,'max_scope_rows_per_export':50000}
    assert apply_scope_budget(req,config)==req
    assert build_plan(req,companies,dates,config,[])==build_plan(req,companies,dates,configuration(),[])


def test_indexed_completed_coverage_has_identical_plan_and_exact_missing_cells():
    companies=[f'{i:04d}=>Fixture' for i in range(24)]
    dates=[(date(2014,1,1)+timedelta(days=i)).strftime('%Y/%m/%d') for i in range(40)]
    request={**base_request(59),'max_rows':100,'max_cells':3000}
    config={**configuration(),'max_companies_per_export':8,'query_tiling_contract':TILING_CONTRACT}
    original=build_plan(request,companies,dates,config,[])
    completed=[next_request(original,i) for i in range(0,original['total_queries'],3)]
    plan=build_plan(request,companies,dates,config,completed)
    # Scope witnesses may be compact groups or repeated one-company proofs.
    # Their representation/order must not alter segments, ordering or SHA.
    expanded=[{**done,'company_labels':[company]} for done in completed
              for company in done['company_labels']]
    assert plan==build_plan(request,companies,dates,config,list(reversed(expanded))+expanded)
    covered={(f,c,d) for done in completed for f in done['fields']
             for c in done['company_labels'] for d in done['date_labels']}
    from collections import Counter
    remaining=Counter((f,c,d) for i in range(plan['total_queries'])
                      for req in [next_request(plan,i)] for f in req['fields']
                      for c in req['company_labels'] for d in req['date_labels'])
    all_cells={(f,c,d) for f in request['fields'] for c in companies for d in dates}
    assert set(remaining)==all_cells-covered
    assert set(remaining.values())=={1}
