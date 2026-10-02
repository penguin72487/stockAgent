from contextlib import closing
import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from downloader.artifact_io import atomic_write_json
from downloader.tej_history import (connect, configure_preview_planning, configure_runtime_policy,
                                    run_one, task_request, validate_export)
from downloader.tej_period_keys import BANK_MONTH_CONTRACT, CONTRACT, month_key, replan_month_periods
from downloader.tej_planning import CONTRACT as PLAN_CONTRACT, next_request
from test_tej_history import export, request, registry
from test_tej_desktop_attempts import unknown, _activate


def monthly_export(header='YYYYMM'):
    doc = export()
    doc.update(contract_version=4, capture_method='native_msaa_preview_full',
               source_period_key_contract=CONTRACT if header=='YYYYMM' else BANK_MONTH_CONTRACT,
               source_value_representation='vendor_display_strings_not_underlying_excel_values',
               source_key_headers=['ID', header], source_grid_rows=2, source_grid_columns=4,
               source_scope_proof={'contract':'editable_source_scope_v1', 'company_group_enabled':True,
                   'date_group_enabled':True,'vendor_notices_absent':True,'binding_readback_verified':True})
    doc['cells'] = [['ID',header,*request()['fields']],
                    ['2330 TSMC','2014/01/31','10','-1.5'],
                    ['2317 HonHai','2014/01/31','20',None]]
    doc['preview'] = [3, [[i,row] for i,row in enumerate(doc['cells'],1)]]
    return doc


def test_month_header_contracts_match_windows_capture_and_selected_radio_guard():
    from downloader.tej_period_keys import HEADER_CONTRACTS,MONTH_CONTRACTS
    source=(Path(__file__).resolve().parents[1]/'scripts/tej_smart_wizard_bridge.ps1').read_text()
    guard=source.split('$frequencySelectionReadbackContract=$null',1)[1].split("Set-Checkbox $dates 'Last Period'",1)[0]
    capture=source.split("if($payload.action -ceq 'download'",1)[1].split('$json=$payload',1)[0]
    for contract in MONTH_CONTRACTS:
        assert contract in guard and contract in capture
    for header in HEADER_CONTRACTS:assert "'"+header+"'" in capture
    assert 'AccessibleState($frequencyHandle)' in guard and 'owned_checked_frequency_button_v1' in guard


def test_literal_month_header_preserves_raw_day_but_never_claims_future_publication_or_daily_rows():
    req = {**request(), 'start':'2014-01-02','end':'2014-01-03','frequency':'daily'}
    _, rows, profile = validate_export(req, monthly_export())
    assert rows[0][1] == '2014/01/31' and len(rows) == 2
    assert profile['keys'] == [('2330','2014-01'),('2317','2014-01')]
    assert profile['source_period_key_contract'] == CONTRACT
    assert profile['requested_query_rows'] == 4 and profile['source_period_scope_rows'] == 2
    assert profile['omitted_query_grid_rows'] is None
    assert profile['omitted_source_period_scope_rows'] == 0
    assert profile['publication_verified'] is False


@pytest.mark.parametrize('label',['YYYY-MM','Date','YYYYMMDD'])
def test_other_headers_never_relax_exact_day_bounds(label):
    doc = monthly_export(); doc['source_key_headers'][1] = label; doc['cells'][0][1] = label
    with pytest.raises(ValueError, match='header contract'):
        validate_export(request(), doc)


def test_month_contract_is_not_accepted_on_old_or_unverified_capture():
    doc = monthly_export(); doc['contract_version'] = 2
    with pytest.raises(ValueError):
        validate_export({**request(),'source_period_key_contract':CONTRACT}, doc)


def test_old_v4_receipt_keeps_its_original_day_key_abi():
    doc = monthly_export(); doc.pop('source_period_key_contract')
    doc['cells'][1][1] = '2014/01/02'; doc['cells'][2][1] = '2014/01/03'
    _, _, profile = validate_export(request(),doc)
    assert profile['keys']==[('2330','2014-01-02'),('2317','2014-01-03')]
    assert 'source_period_key_contract' not in profile


def rejected_saved_month_capture(unknown):
    from downloader.tej_history import PREVIEW_SUBMISSION_CONTRACT
    root, task, req, _, _ = unknown
    task, attempt, prepared = _activate(root,task,req)
    payload = monthly_export(); payload.pop('source_period_key_contract')
    payload.update(task_id=task['task_id'],query_attempt_id=attempt,fresh_preview_transition_verified=True,
                   preview_submission_contract=PREVIEW_SUBMISSION_CONTRACT,
                   **{k:req[k] for k in ('type','smart_id','table','fields')})
    raw = root/'raw'/(attempt+'.json'); atomic_write_json(raw,payload)
    with closing(connect(root)) as con, con:
        con.execute("UPDATE tasks SET state='blocked',last_error_code='source_validation_failed' WHERE task_id=?",(task['task_id'],))
    return root, task, raw, prepared


def test_exact_saved_capture_interpretation_never_rewrites_source_or_prepared_request(unknown):
    from downloader.tej_period_keys import approve_saved_month_interpretation
    from downloader.tej_history import recover_evidence
    root, task, raw, prepared = rejected_saved_month_capture(unknown)
    source_before, request_before = raw.read_bytes(), prepared.read_bytes()
    proof = approve_saved_month_interpretation(root,task['task_id'],raw)
    assert proof['data_query_repeated'] is False
    recover_evidence(root,task['task_id'],raw)
    assert raw.read_bytes()==source_before and prepared.read_bytes()==request_before
    receipt = json.loads((root/'receipts'/(task['task_id']+'.json')).read_text())
    assert receipt['source_period_key_contract']==CONTRACT
    assert receipt['source_period_interpretation_sha256']
    assert '/'+CONTRACT+'/' in receipt['parquet_path']
    from scripts.audit_tej_history import audit
    assert audit(root,root.parent/'recovered-month-audit')['accepted']


def test_changed_saved_capture_and_unknown_outcome_cannot_be_reinterpreted(unknown):
    from downloader.tej_period_keys import approve_saved_month_interpretation, interpreted_payload
    root, task, raw, _ = rejected_saved_month_capture(unknown)
    approve_saved_month_interpretation(root,task['task_id'],raw)
    payload = json.loads(raw.read_text()); payload['cells'][1][-1]='changed'; atomic_write_json(raw,payload)
    with pytest.raises(ValueError,match='exact original'):
        interpreted_payload(root,task,payload)
    with closing(connect(root)) as con, con:
        con.execute("UPDATE tasks SET last_error_code='unknown_outcome_no_auto_retry' WHERE task_id=?",(task['task_id'],))
    with pytest.raises(ValueError,match='rejected'):
        approve_saved_month_interpretation(root,task['task_id'],raw)


def test_changed_planned_header_and_outside_month_are_rejected():
    doc = monthly_export(); doc['cells'][1][1] = '2014/02/28'
    with pytest.raises(ValueError, match='outside requested'):
        validate_export(request(), doc)
    doc = monthly_export(); doc['source_key_headers'][1] = doc['cells'][0][1] = 'Date'
    with pytest.raises(ValueError, match='header changed'):
        validate_export({**request(),'source_period_key_contract':CONTRACT}, doc)


def test_duplicate_month_is_not_silently_collapsed():
    doc = monthly_export(); doc['cells'][2] = ['2330 TSMC','2014/01/30','20',None]
    with pytest.raises(ValueError, match='Duplicate'):
        validate_export(request(), doc)


def test_bank_month_header_has_separate_abi_and_keeps_original_period_label():
    _,rows,profile=validate_export(request(),monthly_export('Data YYMM'))
    assert profile['source_period_key_contract']==BANK_MONTH_CONTRACT
    assert profile['keys']==[('2330','2014-01'),('2317','2014-01')]
    assert rows[0][1]=='2014/01/31' and profile['publication_verified'] is False
    assert profile['source_observation_cadence_verified'] is False


@pytest.mark.parametrize('label',['Data YYMMDD','Data YYMM ','Data YYMM(Release)','data yymm'])
def test_bank_header_is_exact_not_an_open_ended_month_substring(label):
    doc=monthly_export('Data YYMM');doc['cells'][0][1]=doc['source_key_headers'][1]=label
    with pytest.raises(ValueError,match='header contract'):
        validate_export(request(),doc)


@pytest.mark.parametrize('value',['1401','24/01','2014/02/28'])
def test_bank_month_keys_do_not_guess_two_digit_year_or_adopt_another_month(value):
    doc=monthly_export('Data YYMM');doc['cells'][1][1]=value
    with pytest.raises(ValueError):validate_export(request(),doc)


def test_bank_legacy_capture_keeps_old_day_abi_until_exact_interpretation():
    doc=monthly_export('Data YYMM');doc.pop('source_period_key_contract')
    doc['cells'][1][1]='2014/01/02';doc['cells'][2][1]='2014/01/03'
    _,_,profile=validate_export(request(),doc)
    assert profile['keys']==[('2330','2014-01-02'),('2317','2014-01-03')]
    assert 'source_period_key_contract' not in profile


def test_bank_header_cannot_reuse_yyyymm_contract_even_with_matching_calendar_day():
    doc=monthly_export('Data YYMM');doc['source_period_key_contract']=CONTRACT
    doc['cells'][1][1]='2014/01/02';doc['cells'][2][1]='2014/01/03'
    with pytest.raises(ValueError,match='header contract'):validate_export(request(),doc)


def test_exact_saved_bank_capture_recovers_without_replay_and_audits_new_parquet_abi(unknown):
    from downloader.tej_period_keys import approve_saved_month_interpretation
    from downloader.tej_history import recover_evidence
    root,task,raw,prepared=rejected_saved_month_capture(unknown)
    payload=json.loads(raw.read_text())
    payload['cells'][0][1]=payload['source_key_headers'][1]='Data YYMM'
    payload['preview'][1][0][1][1]='Data YYMM'
    atomic_write_json(raw,payload)
    original=raw.read_bytes();original_request=prepared.read_bytes()
    approve_saved_month_interpretation(root,task['task_id'],raw)
    recover_evidence(root,task['task_id'],raw)
    receipt=json.loads((root/'receipts'/(task['task_id']+'.json')).read_text())
    assert receipt['source_period_key_contract']==BANK_MONTH_CONTRACT
    assert '/'+BANK_MONTH_CONTRACT+'/' in receipt['parquet_path']
    assert raw.read_bytes()==original and prepared.read_bytes()==original_request
    from scripts.audit_tej_history import audit
    assert audit(root,root.parent/'bank-month-audit')['accepted']


def test_month_scope_union_weights_count_overlap_once_and_only_whole_partitions():
    from downloader.tej_period_keys import union_scope_weights
    from downloader.tej_planning import build_plan
    fields=['F'+str(n) for n in range(31)]
    req={**request(),'fields':fields};companies=['a','b'];months={'2014-01':'201401','2014-02':'201402','2014-03':'201403'}
    scopes=[({'task_id':'first'},{**req,'company_labels':companies},set(months)-{'2014-03'}),
            ({'task_id':'overlap'},{**req,'company_labels':['b'],'fields':fields[:28]},set(months)-{'2014-01'}),
            ({'task_id':'last-partition'},{**req,'company_labels':['a'],'fields':fields[28:]},{'2014-03'}),
            ({'task_id':'partial'},{**req,'company_labels':['a'],'fields':fields[:2]},{'2014-03'})]
    weights=union_scope_weights(req,scopes,months)
    assert weights==[(8,'first'),(1,'overlap'),(1,'last-partition'),(0,'partial')]
    coverage=[{**scope,'date_labels':[months[m] for m in selected]} for _,scope,selected in scopes]
    config={'max_companies_per_export':32}
    plan=build_plan(req,companies,list(months.values()),config,coverage)
    assert sum(w for w,_ in weights)==plan['completed_work_rows']==10


def test_month_scope_union_merges_only_real_adjacent_months():
    from downloader.tej_period_keys import union_scope_weights
    req={**request(),'company_labels':['a']};months={'2014-01':'201401','2014-02':'201402','2014-03':'201403'}
    scopes=[({'task_id':'ends'},req,{'2014-01','2014-03'}),
            ({'task_id':'fill'},req,{'2014-02'}),({'task_id':'same'},req,set(months))]
    assert union_scope_weights(req,scopes,months)==[(2,'ends'),(1,'fill'),(0,'same')]


@pytest.mark.parametrize('corruption',[None,'parquet','counts'])
def test_legacy_bank_planning_preserves_published_day_abi_and_exact_artifacts(registry,corruption):
    _,root,_,config=registry
    config={**config,'preview_max_columns':30,'download_planning_contract':PLAN_CONTRACT}
    configure_preview_planning(root,config);configure_runtime_policy(root,config)
    class LegacyBankBridge(MonthlyBridge):
        def execute(self,root,task):
            payload,output,seconds=super().execute(root,task)
            if payload['action']=='download':
                payload.pop('source_period_key_contract')
                for i,row in enumerate(payload['cells'][1:],2):row[1]='2014/01/'+str(i).zfill(2)
                atomic_write_json(output,payload)
            return payload,output,seconds
    bridge=LegacyBankBridge(header='Data YYMM')
    assert run_one(root,bridge)=='completed_task'
    assert run_one(root,bridge)=='completed_task'
    with closing(connect(root)) as con:
        task=dict(con.execute("SELECT * FROM tasks WHERE kind='download' AND state='complete'").fetchone())
    receipt_path=root/task['receipt_path'];receipt=json.loads(receipt_path.read_text())
    parquet=root/receipt['parquet_path'];original=parquet.read_bytes();raw=(root/task['output_path']).read_bytes()
    if corruption=='parquet':parquet.write_bytes(b'corrupted')
    if corruption=='counts':receipt['exported_rows']=999;atomic_write_json(receipt_path,receipt)
    before_receipt=receipt_path.read_bytes()
    if corruption:
        with pytest.raises(ValueError,match='artifact/count proof'):replan_month_periods(root,task['task_id'],bridge)
        assert len(bridge.requests)==2
    else:
        result=replan_month_periods(root,task['task_id'],bridge)
        assert result['source_period_key_contract']==BANK_MONTH_CONTRACT
        assert parquet.read_bytes()==original and receipt_path.read_bytes()==before_receipt
        assert (root/task['output_path']).read_bytes()==raw
        assert pq.read_table(parquet)['_query_period'].to_pylist()==['2014-01-02','2014-01-03']
        audit=json.loads(next((root/'period_key_repairs').glob('*.json')).read_text())
        assert audit['planning_only_legacy_month_coverage'][0]['source_and_published_abi_changed'] is False
        assert sum(audit['completed_scope_union_weights'].values())==2


@pytest.mark.parametrize('value',['2014','2014/02/30','unknown',True, float('nan')])
def test_ambiguous_months_are_not_invented(value):
    with pytest.raises(ValueError): month_key(value)


class MonthlyBridge:
    def __init__(self, mutation=None,header='YYYYMM'): self.requests = []; self.mutation = mutation;self.header=header
    def execute(self, root, task):
        req = json.loads(task['request_json']); self.requests.append(req)
        doc = monthly_export(self.header)
        doc.update(task_id=task['task_id'], **{k:req[k] for k in ('type','smart_id','table','fields')})
        if req['action'] == 'plan':
            doc.update(action='plan', company_labels=request()['company_labels'],
                       date_labels=['201401','201312'] if req['frequency']=='monthly' else request()['date_labels'],
                       frequency=req['frequency'], frequency_selection_readback_contract='owned_checked_frequency_button_v1')
            if req['frequency']=='monthly' and self.mutation:
                doc[self.mutation] = None
        else:
            doc.update(action='download',source_key_mode=2)
        output = root/'raw'/(task['task_id']+'-'+str(len(self.requests))+'.json')
        atomic_write_json(output,doc)
        return doc, output, 1


def initialize_month_download(registry, *, automatic=False, mutation=None,header='YYYYMM'):
    _, root, _, config = registry
    config = {**config, 'preview_max_columns':30,'download_planning_contract':PLAN_CONTRACT,
              'auto_month_period_replanning':automatic}
    configure_preview_planning(root,config); configure_runtime_policy(root,config)
    bridge = MonthlyBridge(mutation,header)
    assert run_one(root,bridge)=='completed_task'
    result = run_one(root,bridge)
    with closing(connect(root)) as con:
        task = dict(con.execute("SELECT * FROM tasks WHERE kind='download' AND state='complete'").fetchone())
    return root, bridge, task, result


def test_replanning_uses_verified_month_menu_preserves_receipt_and_never_repeats_preview(registry):
    root, bridge, task, result = initialize_month_download(registry)
    assert result == 'completed_task'
    receipt_file = root/task['receipt_path']; original = receipt_file.read_bytes()
    original_raw = (root/task['output_path']).read_bytes()
    receipt = json.loads(original)
    frame = pq.read_table(root/receipt['parquet_path'])
    assert frame.num_rows == 2 and frame['_query_period'].to_pylist()==['2014-01']*2
    repair = replan_month_periods(root,task['task_id'],bridge)
    assert repair['data_query_repeated'] is False and repair['completed_month_scope_rows_preserved']==2
    assert [r['action'] for r in bridge.requests] == ['plan','download','plan']
    assert (root/task['output_path']).read_bytes()==original_raw and receipt_file.read_bytes()==original
    with closing(connect(root)) as con:
        plan = json.loads(con.execute('SELECT plan_json FROM download_plans').fetchone()[0])
        definition = dict(con.execute('SELECT * FROM tables').fetchone())
        done = dict(con.execute('SELECT * FROM tasks WHERE task_id=?',(task['task_id'],)).fetchone())
        metadata = con.execute("SELECT * FROM tasks WHERE kind='discover' AND task_id!=?",(bridge.requests[0].get('task_id',''),)).fetchall()
        assert definition['frequency']=='monthly' and definition['grid_dates']==2
        assert done['expected_rows']==4 and done['work_expected_rows']==2
        assert not con.execute("SELECT 1 FROM meta WHERE key='source_period_replan_required'").fetchone()
        assert len(metadata)==2  # original discovery plus the exact Monthly replan readback
    assert plan['completed_work_rows']==2
    for ordinal in range(plan['total_queries']):
        req = next_request(plan,ordinal)
        assert req['date_labels']==['201312']
        assert req['source_period_key_contract']==CONTRACT
    assert replan_month_periods(root,task['task_id'],bridge)['state']=='source_period_plan_current'
    assert len(bridge.requests)==3
    from scripts.audit_tej_history import audit
    report = audit(root,root.parent/'monthly-audit')
    assert report['accepted'] and report['versioned_month_period_key_tasks_audited']==1


@pytest.mark.parametrize('header',['YYYYMM','Data YYMM'])
def test_automatic_month_replan_reuses_canonical_queue(registry,header):
    root, bridge, task, result = initialize_month_download(registry,automatic=True,header=header)
    assert result=='completed_task' and len(bridge.requests)==3
    with closing(connect(root)) as con:
        assert con.execute('SELECT frequency FROM tables').fetchone()[0]=='monthly'
        plan=json.loads(con.execute('SELECT plan_json FROM download_plans').fetchone()[0])
        assert plan['request']['source_period_key_contract']==(CONTRACT if header=='YYYYMM' else BANK_MONTH_CONTRACT)


@pytest.mark.parametrize('mutation',['frequency','frequency_selection_readback_contract','company_labels','date_labels'])
def test_failed_month_readback_retains_saved_source_and_a_durable_barrier(registry,mutation):
    root, bridge, task, result = initialize_month_download(registry,automatic=True,mutation=mutation)
    assert result=='source_period_replan_required'
    assert (root/task['receipt_path']).is_file()
    assert run_one(root,bridge)=='source_period_replan_required' and len(bridge.requests)==3
    with closing(connect(root)) as con:
        assert con.execute('SELECT frequency FROM tables').fetchone()[0]=='daily'
        assert json.loads(con.execute('SELECT plan_json FROM download_plans').fetchone()[0])['request']['frequency']=='daily'
