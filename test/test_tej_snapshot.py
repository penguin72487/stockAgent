"""Key=1 downloads current attributes once, never on a fabricated date grid."""
from collections import Counter
from contextlib import closing
import json

import pytest

from downloader.artifact_io import atomic_write_json
from downloader.tej_history import company_code, configure_preview_planning, connect, run_one, validate_export, validate_download_evidence
from downloader.tej_key_layout import KEY1_CONTRACT, SNAPSHOT_DATE_AXIS, repair_source_key_plan
from downloader.tej_planning import build_plan, next_request
from scripts.audit_tej_history import audit
from test_tej_history import request, full_grid_export, scope_proof
from test_tej_planning import registry, axes, base_request, configuration, DynamicBridge


def snapshot_request():
    return {**request(),'source_key_mode':1,'key_layout_contract':KEY1_CONTRACT,'date_labels':[],'frequency':'snapshot'}


def snapshot_proof():
    return {**scope_proof(),'source_key_mode':1,'date_axis_not_used':True,'date_group_enabled':False}


def snapshot_export():
    doc=full_grid_export()
    doc.update(source_key_mode=1,key_layout_contract=KEY1_CONTRACT,date_axis=SNAPSHOT_DATE_AXIS,
               calendar_date_mode=None,
               source_key_headers=['Company Code'],source_grid_rows=2,source_grid_columns=3,
               source_scope_proof=snapshot_proof(),
               cells=[['Company Code',*request()['fields']],['2330 TSMC','10','-1.5'],['2317 HonHai','0','']])
    doc['preview']=[3,[[i,row] for i,row in enumerate(doc['cells'],1)]]
    return doc


def test_snapshot_has_raw_signed_values_and_no_historical_dates():
    headers,rows,profile=validate_export(snapshot_request(),snapshot_export())
    assert headers==['Company Code','Volume(1000S)','Ratio%'] and rows[0][-1]=='-1.5'
    assert profile['keys']==[('2330',None),('2317',None)]
    assert profile['first']==profile['last']==[None,None] and profile['non_null_counts']==[2,1]
    assert profile['requested_query_rows']==2


@pytest.mark.parametrize('mutation',['wrong_grain','fake_history','duplicate','out_of_scope','schema','nonfinite','date_axis','naive_time','missing_proof','first_feature_sample'])
def test_snapshot_rejects_unproved_or_historical_substitutions(mutation):
    req=snapshot_request();doc=snapshot_export()
    if mutation=='wrong_grain':doc.pop('key_layout_contract')
    elif mutation=='fake_history':req['date_labels']=['2014/01/02']
    elif mutation=='duplicate':doc['cells'][-1][0]='2330 TSMC'
    elif mutation=='out_of_scope':doc['cells'][-1][0]='9999 Unknown'
    elif mutation=='schema':doc['source_grid_columns']=4
    elif mutation=='nonfinite':doc['cells'][1][1]=float('nan')
    elif mutation=='date_axis':doc['date_axis']='requested_smart_wizard_grid_not_native_observation_dates'
    elif mutation=='naive_time':doc['observed_at_utc']='2026-10-02T06:00:00'
    elif mutation=='missing_proof':doc['source_scope_proof'].pop('date_axis_not_used')
    else:
        doc['preview']=json.loads(json.dumps(doc['preview']));doc['preview'][1][1][1][1]='999'
    with pytest.raises(ValueError):validate_export(req,doc)


def test_snapshot_planner_queries_each_company_field_once_not_every_old_date():
    companies,_=axes();req={**base_request(),'source_key_mode':1,'key_layout_contract':KEY1_CONTRACT,'frequency':'snapshot'}
    plan=build_plan(req,companies,[],configuration(),[]);seen=Counter()
    assert plan['total_work_rows']==6 and plan['field_batches']==3
    for ordinal in range(plan['total_queries']):
        query=next_request(plan,ordinal)
        assert query['date_labels']==[] and len(query['fields'])<=29
        seen.update((field,company) for field in query['fields'] for company in query['company_labels'])
    assert len(seen)==59*2 and set(seen.values())=={1}
    with pytest.raises(ValueError):build_plan(req,companies,['2014/01/02'],configuration(),[])


class SnapshotBridge(DynamicBridge):
    def execute(self,root,task):
        req=json.loads(task['request_json']);self.requests.append(req)
        payload={**snapshot_export(),**{k:req[k] for k in ('type','smart_id','table','fields')},'task_id':task['task_id'],'action':req['action']}
        if req['action']=='plan':
            payload.update(company_labels=axes()[0],date_labels=[])
        else:
            assert req['date_labels']==[] and req['source_key_mode']==1
            cells=[['Company Code',*req['fields']]]
            cells.extend([company_code(label),*(['-1.5']*len(req['fields']))] for label in req['company_labels'])
            payload.update(cells=cells,preview=[len(cells),[[i,row] for i,row in enumerate(cells,1)]],
                           source_grid_rows=len(cells)-1,source_grid_columns=len(req['fields'])+1)
        output=root/'raw'/(task['task_id']+'-snapshot.json');atomic_write_json(output,payload);return payload,output,1


def test_snapshot_storage_and_completed_scope_are_independently_audited(registry,tmp_path):
    import pyarrow.parquet as pq
    root,config=registry;configure_preview_planning(root,config);bridge=SnapshotBridge()
    assert run_one(root,bridge)=='completed_task'
    while run_one(root,bridge)=='completed_task':pass
    with closing(connect(root)) as con:
        tasks=[dict(row) for row in con.execute("SELECT * FROM tasks WHERE kind='download'")]
        table=dict(con.execute('SELECT * FROM tables').fetchone())
    assert len(tasks)==3 and all(task['state']=='complete' and task['expected_rows']==2 for task in tasks)
    assert table['grid_rows']==2 and table['work_grid_rows']==6 and table['grid_dates']==0
    assert table['first_available_query_period'] is None and table['frequency']=='snapshot'
    receipt=json.loads((root/'receipts'/(tasks[0]['task_id']+'.json')).read_text())
    assert KEY1_CONTRACT in receipt['parquet_path'] and receipt['historical_values_reconstructed'] is False
    assert receipt['first_query_period'] is None and receipt['last_query_period'] is None
    frame=pq.read_table(root/receipt['parquet_path'])
    assert frame['_query_period'].to_pylist()==[None,None]
    assert len(set(frame['_snapshot_observed_at_utc'].to_pylist()))==1
    report=audit(root,tmp_path/'snapshot-audit')
    assert report['accepted'] and report['key1_snapshot_tasks_audited']==3


def test_single_key_empty_reply_is_a_company_scope_not_zero_work():
    from test_tej_history import empty_export
    doc={**empty_export(),**{k:snapshot_export()[k] for k in ('source_key_mode','key_layout_contract','date_axis','source_scope_proof','calendar_date_mode')}}
    headers,rows,profile=validate_download_evidence(snapshot_request(),doc)
    assert rows==[] and headers[0]=='CO_ID' and profile['requested_query_rows']==2


def test_rejected_historical_plan_is_replaced_with_no_dates_and_retained_original(registry):
    root,config=registry;configure_preview_planning(root,config);run_one(root,DynamicBridge())
    with closing(connect(root)) as con,con:
        from downloader.tej_planning import refill_ready_plans
        refill_ready_plans(con)
        task=dict(con.execute("SELECT * FROM tasks WHERE kind='download'").fetchone())
        con.execute("UPDATE tasks SET state='blocked',last_error_code='source_key_layout_replan_required' WHERE task_id=?",(task['task_id'],))
    calls=[]
    class Readback:
        def execute(self,root,task):
            req=json.loads(task['request_json']);calls.append(req['action'])
            payload={'contract_version':4,'provider':'tej_smart_wizard','task_id':task['task_id'],'action':req['action'],
                **{k:req[k] for k in ('type','smart_id','table')},'source_key_mode':1,'date_group_enabled':False,
                **{k:True for k in ('binding_matches_failed_plan','source_binding_stable','vendor_notices_absent','source_selectors_enabled','company_group_enabled')},
                **{k:False for k in ('market_data_query_submitted','credentials_read','query_button_invoked')}}
            output=root/'raw'/(task['task_id']+'-snapshot-readback.json');atomic_write_json(output,payload);return payload,output,1
    result=repair_source_key_plan(root,task['task_id'],Readback())
    assert result['source_key_mode']==1 and calls==['inspect_query_runtime']
    with closing(connect(root)) as con:
        definition=dict(con.execute('SELECT * FROM tables').fetchone())
        retained=con.execute('SELECT state FROM tasks WHERE task_id=?',(task['task_id'],)).fetchone()[0]
        plan=json.loads(con.execute('SELECT plan_json FROM download_plans').fetchone()[0])
    assert retained=='superseded_key1_layout_v1'
    assert plan['dates']==[] and plan['total_work_rows']==6
    assert definition['frequency']=='snapshot' and definition['grid_rows']==2 and definition['grid_dates']==0
