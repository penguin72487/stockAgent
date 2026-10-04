from collections import Counter
from contextlib import closing
from datetime import date,timedelta
import json

import pytest

from downloader.artifact_io import atomic_write_json
from downloader.tej_history import (connect,company_code,compact_request,configure_preview_planning,
    expand_request,register_inventory,run_one,task_request,settle_preview_column_limit)
from downloader.tej_planning import CONTRACT,build_plan,next_request
from scripts.build_tej_smart_wizard_inventory import main as build_inventory
from scripts.audit_tej_history import audit
from stockagent.live.tej_dashboard import build_tej_public_status


def base_request(fields=59):
    names=[f'Value_{i}' for i in range(fields)]
    return {'contract_version':4,'type':'LISTED & DELISTED','smart_id':'TEJEquity','table':'Test Daily',
            'action':'plan','fields':names,'catalog_fields':names,'start':'1900-01-01','end':'2014-01-07',
            'frequency':'daily','max_rows':10,'max_cells':300}


def axes():
    return ['2330=>TSMC','2317=>HonHai'],[(date(2014,1,2)+timedelta(days=i)).strftime('%Y/%m/%d') for i in range(5)]


def configuration():
    return {'history_search_start':'1900-01-01','max_rows_per_export':10,'max_cells_per_export':300,
            'max_companies_per_export':2,'minimum_free_disk_bytes':0,'quota':{},
            'preview_max_columns':30,'download_planning_contract':CONTRACT}


@pytest.mark.parametrize('count',[1,2,28,29,59,2000])
def test_partitions_exhaust_fields_keys_once_and_fill_bounded_calls(count):
    request=base_request(count); companies,dates=axes();config=configuration()
    plan=build_plan(request,companies,dates,config,[]);observed=Counter()
    for ordinal in range(plan['total_queries']):
        q=next_request(plan,ordinal);rows=len(q['company_labels'])*len(q['date_labels'])
        assert len(q['fields'])+2<=30 and rows<=q['max_rows'] and (rows+1)*(len(q['fields'])+2)<=q['max_cells']
        for field in q['fields']:
            for company in q['company_labels']:
                for period in q['date_labels']:observed[field,company,period]+=1
    assert len(observed)==count*len(companies)*len(dates) and set(observed.values())=={1}
    assert plan['total_work_rows']==plan['remaining_work_rows'] and plan['completed_work_rows']==0


def test_completed_full_schema_sparse_or_empty_query_scopes_are_not_requested_again():
    request=base_request();companies,dates=axes()
    done={**request,'action':'download','company_labels':companies[:1],'date_labels':dates[:2]}
    plan=build_plan(request,companies,dates,configuration(),[done]);seen=set()
    for ordinal in range(plan['total_queries']):
        q=next_request(plan,ordinal)
        seen.update((f,c,d) for f in q['fields'] for c in q['company_labels'] for d in q['date_labels'])
    assert not any((f,companies[0],d) in seen for f in request['fields'] for d in dates[:2])
    assert len(seen)==len(request['fields'])*(len(companies)*len(dates)-2)
    assert plan['completed_work_rows']==6


def test_changed_plan_and_invalid_cap_are_not_resumed():
    request=base_request();companies,dates=axes();config=configuration()
    plan=build_plan(request,companies,dates,config,[]);plan['companies'][0]='wrong'
    with pytest.raises(ValueError,match='fingerprint'):next_request(plan,0)
    with pytest.raises(ValueError,match='cap'):build_plan(request,companies,dates,{**config,'preview_max_columns':31},[])


@pytest.fixture
def registry(tmp_path):
    source=tmp_path/'catalog.jsonl';req=base_request()
    source.write_text('\n'.join(json.dumps(r) for r in [
        {'contract_version':1,'record_kind':'header','provider':'tej_smart_wizard','types':['LISTED & DELISTED']},
        {'record_kind':'binding','type':req['type'],'smart_id':req['smart_id'],'table':req['table'],
         'fields':req['fields'],'field_count':len(req['fields'])},
        {'record_kind':'completion','complete':True}]))
    inventory=tmp_path/'inventory';build_inventory(['--catalog',str(source),'--root',str(tmp_path),'--output-dir',str(inventory)])
    root=tmp_path/'data_tej';config=configuration();register_inventory(root,inventory,config,cutoff='2014-01-07')
    return root,config


class DynamicBridge:
    def __init__(self):self.requests=[]
    def execute(self,root,task):
        req=json.loads(task['request_json']);self.requests.append(req)
        proof={'contract':'editable_source_scope_v1','company_group_enabled':True,'date_group_enabled':True,
               'vendor_notices_absent':True,'binding_readback_verified':True}
        payload={'provider':'tej_smart_wizard','contract_version':4,'task_id':task['task_id'],
                 'action':req['action'],'date_axis':'requested_smart_wizard_grid_not_native_observation_dates',
                 **{k:req[k] for k in ('type','smart_id','table','fields')},'source_scope_proof':proof,
                 'credentials_read':False,'query_comments_read':False,'calendar_date_mode':False,
                 'universe_scope':'exact_type_smart_id_all_sectors','checkbox_verification_method':'msaa_role44_state_flags'}
        if req['action']=='plan':payload.update(company_labels=axes()[0],date_labels=axes()[1])
        else:
            assert len(req['fields'])+2<=30
            cells=[['CO_ID','Date',*req['fields']]]
            cells.extend([company_code(c),d,*(['1']*len(req['fields']))] for c in req['company_labels'] for d in req['date_labels'])
            samples=[[i,row] for i,row in enumerate(cells,1) if i<=4 or i>=len(cells)-2]
            payload.update(cells=cells,preview=[len(cells),samples],
                           capture_method='native_msaa_preview_full',source_value_representation='vendor_display_strings_not_underlying_excel_values',
                           source_grid_rows=len(cells)-1,source_grid_columns=len(req['fields'])+2,
                           observed_at_utc='2026-10-02T03:00:00+00:00')
        output=root/'raw'/(task['task_id']+'-test.json');atomic_write_json(output,payload);return payload,output,1


def test_lazy_queue_is_bounded_and_complete_field_counts_reconcile(registry,tmp_path):
    root,config=registry;configure_preview_planning(root,config);bridge=DynamicBridge()
    assert run_one(root,bridge)=='completed_task'
    with closing(connect(root)) as con:
        assert con.execute("SELECT COUNT(*) FROM tasks WHERE kind='download'").fetchone()[0]==0
        assert con.execute('SELECT total_queries FROM download_plans').fetchone()[0]==5
    for _ in range(5):
        assert run_one(root,bridge)=='completed_task'
        with closing(connect(root)) as con:
            assert con.execute("SELECT COUNT(*) FROM tasks WHERE kind='download' AND state IN ('pending','running')").fetchone()[0]<=1
    assert run_one(root,bridge)=='idle'
    with closing(connect(root)) as con:
        assert {r[0] for r in con.execute('SELECT exported_non_null_cells FROM features')}=={10}
        task=dict(con.execute("SELECT * FROM tasks WHERE kind='download' LIMIT 1").fetchone())
        definition=dict(con.execute('SELECT * FROM tables').fetchone())
        compact=json.loads(task['request_json']);assert compact['_fields_ref']['version']==2
        assert 'fields' not in compact and 'catalog_fields' not in compact
        request=expand_request(compact,definition,task['table_id'])
        assert compact_request(request,definition)==compact
        compact['_fields_ref']['field_range']=[0,29]
        with pytest.raises(ValueError):expand_request(compact,definition,task['table_id'])
    report=audit(root,tmp_path/'audit')
    assert report['accepted'] and report['exported_rows']==30 and report['exported_non_null_cells']==590
    assert report['completed_download_tasks_audited']==5
    status=build_tej_public_status(tmp_path)
    table=status['tables'][0]
    assert table['company_period_grid_rows']==10 and table['work_grid_rows']==30
    assert table['resolved_grid_rows']==30 and table['query_scope_ratio']==1
    assert table['field_batches']==3 and table['exported_rows']==30
    assert status['planning']['buffered_download_tasks']==0
    assert status['planning']['unmaterialized_queries']==0
    assert status['state']=='query_grid_exported'
    assert status['native_observation_completeness_verified'] is False
    with pytest.raises(FileExistsError):audit(root,tmp_path/'audit')


def test_pending_migration_is_idempotent_and_keeps_old_work_as_evidence(registry):
    root,config=registry
    assert run_one(root,DynamicBridge())=='completed_task'
    with closing(connect(root)) as con:
        old=[r[0] for r in con.execute("SELECT task_id FROM tasks WHERE kind='download'")]
    assert len(old)==5  # These wide legacy requests are retained, not sent.
    result=configure_preview_planning(root,config)
    assert result['logical_queries_remaining']==5 and result['tables']==1
    assert configure_preview_planning(root,config)['already_installed']
    with closing(connect(root)) as con:
        retained=[r[0] for r in con.execute("SELECT task_id FROM tasks WHERE state='superseded_preview_partition_v1'")]
        assert retained==old and con.execute('SELECT COUNT(*) FROM download_plans').fetchone()[0]==1


def test_unknown_source_outcome_is_a_hard_migration_barrier(registry):
    root,config=registry
    with closing(connect(root)) as con,con:
        con.execute("UPDATE tasks SET state='blocked',last_error_code='unknown_outcome_no_auto_retry'")
    with pytest.raises(ValueError,match='Unresolved'):configure_preview_planning(root,config)
    with closing(connect(root)) as con:
        assert con.execute('SELECT COUNT(*) FROM download_plans').fetchone()[0]==0


def test_field_weave_does_not_starve_later_features_and_covers_tail():
    request=base_request();companies,dates=axes();plan=build_plan(request,companies,dates,configuration(),[])
    first=[next_request(plan,i)['field_partition']['field_range'][0] for i in range(3)]
    assert first==[0,28,56]
    assert [next_request(plan,i)['field_partition']['field_range'][0] for i in range(3,5)]==[0,28]


@pytest.mark.parametrize('mutation',[None,'request','stage','proof','ack_code','query_repeated','readback','unknown_download'])
def test_native_column_limit_recovery_is_exact_rejection_not_fake_data(registry,mutation):
    root,_=registry;run_one(root,DynamicBridge())
    with closing(connect(root)) as con,con:
        task=dict(con.execute("SELECT * FROM tasks WHERE kind='download' LIMIT 1").fetchone())
        con.execute("UPDATE tasks SET state='blocked',last_error_code='unknown_outcome_no_auto_retry' WHERE task_id=?",(task['task_id'],))
    req=task_request(root,task);prepared=root/'requests'/(task['task_id']+'-rejected.json')
    original={**req,'contract_version':4,'task_id':task['task_id']}
    if mutation=='request':original['table']='wrong'
    atomic_write_json(prepared,original)
    proof={'contract':'editable_source_scope_v1','company_group_enabled':True,'date_group_enabled':True,
           'vendor_notices_absent':True,'binding_readback_verified':True}
    stage={**{k:req[k] for k in ('type','smart_id','table','fields','company_labels','date_labels')},
           'task_id':task['task_id'],'contract_version':4,'stage':'prepreview_verified','source_scope_proof':proof}
    if mutation=='stage':stage['date_labels']=[]
    if mutation=='proof':stage['source_scope_proof']['vendor_notices_absent']=False
    atomic_write_json(root/'raw'/(prepared.name+'.stage.json'),stage)
    class Recovery:
        def __init__(self):self.calls=[]
        def execute(self,root,task):
            request=json.loads(task['request_json']);self.calls.append(request['action'])
            assert request['action'] in {'ack_preview_column_limit','confirm_metadata_error_cleared'}
            payload={'provider':'tej_smart_wizard','contract_version':4,'task_id':task['task_id'],'action':request['action'],
                     **{k:req[k] for k in ('type','smart_id','table')},'credentials_read':False}
            if request['action']=='ack_preview_column_limit':
                payload.update(error_code='wrong' if mutation=='ack_code' else 'vendor_preview_30_column_limit',
                               data_query_repeated=mutation=='query_repeated',source_query_rejected=mutation!='unknown_download')
            else:
                payload.update(source_binding_stable=mutation!='readback',vendor_notices_absent=True,
                               source_selectors_enabled=True,market_data_query_submitted=False)
            output=root/'raw'/(request['action']+'.json');atomic_write_json(output,payload);return payload,output,1
    bridge=Recovery()
    if mutation:
        with pytest.raises(ValueError):settle_preview_column_limit(root,task['task_id'],bridge,prepared,1234)
        with closing(connect(root)) as con:
            assert con.execute('SELECT last_error_code FROM tasks WHERE task_id=?',(task['task_id'],)).fetchone()[0]=='unknown_outcome_no_auto_retry'
    else:
        result=settle_preview_column_limit(root,task['task_id'],bridge,prepared,1234)
        assert result['state']=='preview_column_limit_repartition_required' and not result['data_query_repeated']
        with closing(connect(root)) as con:
            row=con.execute('SELECT state,actual_rows FROM tasks WHERE task_id=?',(task['task_id'],)).fetchone()
            assert tuple(row)==('blocked',None)
