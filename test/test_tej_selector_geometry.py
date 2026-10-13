"""Native selector capacity is independent from Preview output capacity."""
from collections import Counter
from contextlib import closing
from datetime import date, timedelta
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
