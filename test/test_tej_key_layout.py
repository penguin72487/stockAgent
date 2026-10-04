"""Preview capacity is total columns; native event keys must remain lossless."""
from collections import Counter
from contextlib import closing
import json

import pytest

from downloader.artifact_io import atomic_write_json
from downloader.tej_history import (SOURCE_SCOPE_CONTRACT, company_code, configure_preview_planning,
                                    connect, run_one, task_request, validate_export, validate_download_evidence)
from downloader.tej_key_layout import KEY3_CONTRACT, key_count, repair_key3_plan
from downloader.tej_planning import build_plan, next_request
from scripts.audit_tej_history import audit
from test_tej_history import request, full_grid_export, scope_proof
from test_tej_planning import registry, axes, base_request, configuration, DynamicBridge


def key3_request():
    return {**request(), 'source_key_mode':3, 'key_layout_contract':KEY3_CONTRACT}


def key3_export():
    doc=full_grid_export()
    doc.update(source_key_mode=3, key_layout_contract=KEY3_CONTRACT,
               source_key_headers=['CO_ID','Date','Event sequence'], source_grid_rows=2, source_grid_columns=5)
    # Two distinct events at the exact same company/period, including a negative ratio.
    doc['cells']=[['CO_ID','Date','Event sequence',*request()['fields']],
                  ['2330 TSMC','2014/01/02','001','10','-1.5'],
                  ['2330 TSMC','2014/01/02','002','20','0']]
    doc['preview']=[3,[[i,row] for i,row in enumerate(doc['cells'],1)]]
    return doc


def test_key3_keeps_two_native_records_per_company_period():
    headers,rows,profile=validate_export(key3_request(),key3_export())
    assert headers[:3]==['CO_ID','Date','Event sequence']
    assert [row[2] for row in rows]==['001','002'] and rows[0][-1]=='-1.5'
    assert profile['keys']==[('2330','2014-01-02')]*2
    assert profile['non_null_counts']==[2,2] and profile['omitted_query_grid_rows']==3


@pytest.mark.parametrize('mutation',['duplicate','empty_key','bool_key','nonfinite_key','missing_tag','wrong_columns','wrong_date'])
def test_key3_never_hides_wrong_native_grain(mutation):
    doc=key3_export()
    if mutation=='duplicate':doc['cells'][-1][2]='001'
    elif mutation=='empty_key':doc['cells'][-1][2]=''
    elif mutation=='bool_key':doc['cells'][-1][2]=True
    elif mutation=='nonfinite_key':doc['cells'][-1][2]=float('nan')
    elif mutation=='missing_tag':doc.pop('key_layout_contract')
    elif mutation=='wrong_columns':doc['source_grid_columns']=4
    else:doc['cells'][-1][1]='2030/01/01'
    with pytest.raises(ValueError):validate_export(key3_request(),doc)


@pytest.mark.parametrize('mode',[None,True,False,1,3.0,'3'])
def test_unverified_key_mode_never_changes_capacity(mode):
    with pytest.raises(ValueError):key_count({'source_key_mode':mode,'key_layout_contract':KEY3_CONTRACT})


def test_key3_plans_27_features_and_covers_every_requested_cell_once():
    request3={**base_request(),'source_key_mode':3,'key_layout_contract':KEY3_CONTRACT}
    companies,dates=axes();plan=build_plan(request3,companies,dates,configuration(),[])
    old=build_plan(base_request(),companies,dates,configuration(),[])
    assert old['fingerprint']!=plan['fingerprint']
    seen=Counter()
    for ordinal in range(plan['total_queries']):
        query=next_request(plan,ordinal)
        assert len(query['fields'])<=27
        assert (len(query['company_labels'])*len(query['date_labels'])+1)*(len(query['fields'])+3)<=query['max_cells']
        seen.update((field,company,day) for field in query['fields'] for company in query['company_labels'] for day in query['date_labels'])
    assert len(seen)==59*2*5 and set(seen.values())=={1}
    with pytest.raises(ValueError):build_plan(request3,companies,dates,configuration(),[{**base_request(), 'company_labels':companies,'date_labels':dates}])


class Key3Bridge(DynamicBridge):
    def execute(self,root,task):
        req=json.loads(task['request_json']);self.requests.append(req)
        payload={'provider':'tej_smart_wizard','contract_version':4,'task_id':task['task_id'],
                 'action':req['action'],'date_axis':'requested_smart_wizard_grid_not_native_observation_dates',
                 **{k:req[k] for k in ('type','smart_id','table','fields')},'source_scope_proof':scope_proof(),
                 'credentials_read':False,'query_comments_read':False,'calendar_date_mode':False,
                 'universe_scope':'exact_type_smart_id_all_sectors','checkbox_verification_method':'msaa_role44_state_flags',
                 'source_key_mode':3,'observed_at_utc':'2026-10-02T06:00:00+00:00'}
        if req['action']=='plan':payload.update(company_labels=axes()[0],date_labels=axes()[1])
        else:
            assert len(req['fields'])+3<=30 and req['key_layout_contract']==KEY3_CONTRACT
            cells=[['CO_ID','Date','Event sequence',*req['fields']]]
            cells.extend([company_code(req['company_labels'][0]),req['date_labels'][0],key,*(['-1.5']*len(req['fields']))] for key in ['001','002'])
            payload.update(key_layout_contract=KEY3_CONTRACT,source_key_headers=cells[0][:3],cells=cells,
                           preview=[3,[[i,row] for i,row in enumerate(cells,1)]],
                           capture_method='native_msaa_preview_full',source_value_representation='vendor_display_strings_not_underlying_excel_values',
                           source_grid_rows=2,source_grid_columns=len(req['fields'])+3)
        output=root/'raw'/(task['task_id']+'-test.json');atomic_write_json(output,payload);return payload,output,1


def test_key3_artifacts_are_separate_and_independently_audited(registry,tmp_path):
    import pyarrow.parquet as pq
    root,config=registry;configure_preview_planning(root,config);bridge=Key3Bridge()
    assert run_one(root,bridge)=='completed_task'
    assert run_one(root,bridge)=='completed_task'
    with closing(connect(root)) as con:
        task=dict(con.execute("SELECT * FROM tasks WHERE kind='download' AND state='complete'").fetchone())
    receipt=json.loads((root/'receipts'/(task['task_id']+'.json')).read_text())
    assert receipt['source_key_headers']==['CO_ID','Date','Event sequence']
    assert KEY3_CONTRACT in receipt['parquet_path'] and receipt['exported_rows']==2
    frame=pq.read_table(root/receipt['parquet_path'])
    assert frame['_native_record_key'].to_pylist()==['001','002']
    assert audit(root,tmp_path/'key3-audit')['accepted']


@pytest.mark.parametrize('unknown,completed,bad_readback',[(False,False,False),(True,False,False),(False,True,False),(False,False,True)])
def test_key3_replan_never_resends_or_discards_existing_evidence(registry,unknown,completed,bad_readback):
    root,config=registry;configure_preview_planning(root,config);run_one(root,DynamicBridge())
    with closing(connect(root)) as con,con:
        from downloader.tej_planning import refill_ready_plans
        refill_ready_plans(con)
        task=dict(con.execute("SELECT * FROM tasks WHERE kind='download'").fetchone())
        error='unknown_outcome_no_auto_retry' if unknown else 'preview_column_limit_repartition_required'
        con.execute("UPDATE tasks SET state=?,last_error_code=? WHERE task_id=?",('complete' if completed else 'blocked',error,task['task_id']))
    calls=[]
    class Readback:
        def execute(self,root,task):
            req=json.loads(task['request_json']);calls.append(req['action'])
            assert req['action']=='inspect_query_runtime'
            payload={'contract_version':4,'provider':'tej_smart_wizard','task_id':task['task_id'],'action':req['action'],
                **{k:req[k] for k in ('type','smart_id','table')},'source_key_mode':2 if bad_readback else 3,
                **{k:True for k in ('binding_matches_failed_plan','source_binding_stable','vendor_notices_absent','source_selectors_enabled','company_group_enabled','date_group_enabled')},
                **{k:False for k in ('market_data_query_submitted','credentials_read','query_button_invoked')}}
            output=root/'raw'/(task['task_id']+'-readback.json');atomic_write_json(output,payload);return payload,output,1
    if unknown or completed or bad_readback:
        with pytest.raises(ValueError):repair_key3_plan(root,task['task_id'],Readback())
        return
    result=repair_key3_plan(root,task['task_id'],Readback())
    assert calls==['inspect_query_runtime'] and result['data_query_repeated'] is False
    with closing(connect(root)) as con:
        old=con.execute('SELECT state FROM tasks WHERE task_id=?',(task['task_id'],)).fetchone()[0]
        assert old=='superseded_key3_layout_v1'
        replacement=dict(con.execute("SELECT * FROM tasks WHERE kind='download' AND state='pending' AND scope_contract=?",(SOURCE_SCOPE_CONTRACT,)).fetchone())
    assert len(task_request(root,replacement)['fields'])==27


def test_empty_key3_reply_cannot_adopt_unverified_extra_columns():
    from test_tej_history import empty_export
    with pytest.raises(ValueError):validate_download_evidence(key3_request(),empty_export())


def test_verified_old_full_schema_empty_reply_is_reused_without_fabricating_native_records(registry):
    from downloader.tej_history import compact_request, stable_id
    from downloader.tej_planning import refill_ready_plans
    from test_tej_history import empty_export
    root,config=registry;configure_preview_planning(root,config);run_one(root,DynamicBridge())
    with closing(connect(root)) as con,con:
        definition=dict(con.execute('SELECT * FROM tables').fetchone())
        plan=json.loads(con.execute('SELECT plan_json FROM download_plans').fetchone()[0])
        req={**plan['request'],'action':'download','company_labels':axes()[0][:1],'date_labels':axes()[1][:1]}
        legacy=stable_id([definition['table_id'],'verified-empty-old-full-schema'])
        con.execute("INSERT INTO tasks(task_id,table_id,kind,phase,priority,request_json,expected_rows,scope_contract) VALUES(?,?,'download','P1',100,?,1,?)",
                    (legacy,definition['table_id'],json.dumps(compact_request(req,definition)),SOURCE_SCOPE_CONTRACT))
    class Empty:
        def execute(self,root,task):
            req=json.loads(task['request_json']);doc=empty_export()
            doc.update(task_id=task['task_id'],**{k:req[k] for k in ('type','smart_id','table','fields')})
            out=root/'raw'/(task['task_id']+'-empty.json');atomic_write_json(out,doc);return doc,out,1
    assert run_one(root,Empty(),kind='download')=='completed_task'
    receipt=root/'receipts'/(legacy+'.json'); original=receipt.read_bytes()
    with closing(connect(root)) as con,con:
        refill_ready_plans(con)
        blocked=con.execute("SELECT task_id FROM tasks WHERE state='pending' AND kind='download'").fetchone()[0]
        con.execute("UPDATE tasks SET state='blocked',last_error_code='source_key_layout_replan_required' WHERE task_id=?",(blocked,))
    calls=[]
    class Readback:
        def execute(self,root,task):
            q=json.loads(task['request_json']);calls.append(q['action'])
            doc={'contract_version':4,'provider':'tej_smart_wizard','task_id':task['task_id'],'action':q['action'],
                 **{k:q[k] for k in ('type','smart_id','table')},'source_key_mode':3,
                 **{k:True for k in ('binding_matches_failed_plan','source_binding_stable','vendor_notices_absent','source_selectors_enabled','company_group_enabled','date_group_enabled')},
                 **{k:False for k in ('market_data_query_submitted','credentials_read','query_button_invoked')}}
            out=root/'raw'/(task['task_id']+'-readback.json');atomic_write_json(out,doc);return doc,out,1
    result=repair_key3_plan(root,blocked,Readback())
    assert calls==['inspect_query_runtime'] and result['data_query_repeated'] is False
    assert result['verified_empty_scopes_preserved']==1 and result['completed_work_rows_preserved']==3
    assert receipt.read_bytes()==original
    with closing(connect(root)) as con:
        migrated=json.loads(con.execute('SELECT plan_json FROM download_plans').fetchone()[0])
        assert con.execute('SELECT actual_rows,work_expected_rows FROM tasks WHERE task_id=?',(legacy,)).fetchone()[:]==(0,3)
    for index in range(migrated['total_queries']):
        q=next_request(migrated,index)
        assert not (axes()[0][0] in q['company_labels'] and axes()[1][0] in q['date_labels'])
