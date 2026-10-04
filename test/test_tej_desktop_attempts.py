"""Attempt identity, stale result rejection and explicit one-shot replay gates."""
from contextlib import closing
import json
from pathlib import Path
import uuid

import pytest

from downloader.artifact_io import atomic_write_json
from downloader.tej_desktop_attempts import begin_attempt, query_stage, retry_unknown_download, validate_active_attempt, prepared_request_matches
from downloader.tej_history import DESKTOP_INPUT_CONTRACT, PREVIEW_SUBMISSION_CONTRACT, connect, recover_desktop_response, run_one, task_request
from test_tej_history import FakeBridge, full_grid_export, registry, scope_proof


@pytest.fixture
def unknown(registry):
    _, root, _, _ = registry
    run_one(root,FakeBridge())
    with closing(connect(root)) as con, con:
        task = dict(con.execute("SELECT * FROM tasks WHERE kind='download'").fetchone())
        con.execute("UPDATE tasks SET state='blocked',last_error_code='unknown_outcome_no_auto_retry',attempted_at_utc='2026-10-02T01:00:00+00:00' WHERE task_id=?",(task['task_id'],))
    with closing(connect(root)) as con:
        task = dict(con.execute('SELECT * FROM tasks WHERE task_id=?',(task['task_id'],)).fetchone())
    req = task_request(root,task)
    prepared = root/'requests'/(task['task_id']+'-'+uuid.uuid4().hex+'.json')
    atomic_write_json(prepared,{**req,'task_id':task['task_id']})
    stage = {**{k:req[k] for k in ('type','smart_id','table','fields','company_labels','date_labels')},
             'contract_version':4,'task_id':task['task_id'],'stage':'prepreview_verified','source_scope_proof':scope_proof()}
    atomic_write_json(root/'raw'/(prepared.name+'.stage.json'),stage)
    return root,task,req,prepared,stage


class ReplayBridge:
    def __init__(self, mutation=None, fail=False):
        self.calls=[];self.mutation=mutation;self.fail=fail

    def execute(self, root, task):
        req=json.loads(task['request_json']);self.calls.append(req['action'])
        if req['action']=='inspect_query_runtime':
            payload={'contract_version':4,'provider':'tej_smart_wizard','action':req['action'],'task_id':task['task_id'],
                     **{k:req[k] for k in ('type','smart_id','table')},
                     **{k:True for k in ('vendor_notices_absent','source_binding_stable','source_selectors_enabled',
                                        'binding_matches_failed_plan','company_group_enabled','date_group_enabled','source_binding_unchanged')},
                     **{k:False for k in ('market_data_query_submitted','date_text_input_sent','query_button_invoked','source_rows_adopted','credentials_read')},
                     'current_field_lists':[{'items':req['fields']},{'items':req['fields']}],
                     'current_query_axes':[{'items':[]} for _ in range(6)],
                     'date_input_controls':[{'native_text':req[k].replace('-','/')} for k in ('start','end')],
                     'preview_button':{'enabled':True,'visible':True,'accessible_role_state':[43,1048576],'default_action':'Press'}}
            payload['current_query_axes'][3]['items']=req['company_labels'];payload['current_query_axes'][-1]['items']=req['date_labels']
            if self.mutation=='fields':payload['current_field_lists'][-1]['items']=[]
            elif self.mutation=='axes':payload['current_query_axes'][3]['items']=[]
            elif self.mutation=='dates':payload['date_input_controls']=[]
            elif self.mutation=='button':payload['preview_button']['default_action']=''
            elif self.mutation:payload[self.mutation]=None
        else:
            assert req['action']=='download'
            if self.fail:raise RuntimeError('no response after once-only submission')
            payload={**full_grid_export(),'task_id':task['task_id']}
        output=root/'raw'/(task['task_id']+'-'+uuid.uuid4().hex+'.json')
        atomic_write_json(output,payload)
        return payload,output,1.0


def test_explicit_replay_preserves_original_unknown_and_adopts_only_new_real_result(unknown):
    root,task,req,prepared,stage=unknown
    before=prepared.read_bytes();before_stage=(root/'raw'/(prepared.name+'.stage.json')).read_bytes()
    bridge=ReplayBridge();result=retry_unknown_download(root,task['task_id'],bridge,prepared)
    assert result['state']=='completed_task' and result['original_unknown_evidence_retained']
    assert result['automatic_retry'] is False
    assert bridge.calls==['inspect_query_runtime','download']
    assert prepared.read_bytes()==before and (root/'raw'/(prepared.name+'.stage.json')).read_bytes()==before_stage
    audit=json.loads(next((root/'operator_replays').glob('*.json')).read_text())
    assert audit['original_outcome']=='unknown_retained_not_claimed_unsent'
    assert audit['possible_additional_provider_usage']
    with pytest.raises(ValueError,match='already consumed'):
        run_one(root,bridge,retry_authorization_id=result['authorization_id'])
    assert bridge.calls==['inspect_query_runtime','download']


def test_second_unknown_stops_and_normal_scheduler_never_repeats(unknown):
    root,task,req,prepared,stage=unknown;bridge=ReplayBridge(fail=True)
    result=retry_unknown_download(root,task['task_id'],bridge,prepared)
    assert result['state']=='unknown_outcome_no_auto_retry'
    assert run_one(root,bridge)=='inflight_requires_recovery'
    assert bridge.calls==['inspect_query_runtime','download']
    with closing(connect(root)) as con:
        row=con.execute('SELECT state,actual_rows FROM tasks WHERE task_id=?',(task['task_id'],)).fetchone()
        assert tuple(row)==('blocked',None)


@pytest.mark.parametrize('mutation',['fields','axes','dates','button','vendor_notices_absent','source_binding_stable',
    'source_selectors_enabled','binding_matches_failed_plan','company_group_enabled','date_group_enabled',
    'source_binding_unchanged','market_data_query_submitted','date_text_input_sent','query_button_invoked',
    'source_rows_adopted','credentials_read','provider','action','task_id'])
def test_unproved_replay_context_never_resets_unknown_or_submits(unknown,mutation):
    root,task,req,prepared,stage=unknown;bridge=ReplayBridge(mutation)
    with pytest.raises(ValueError):retry_unknown_download(root,task['task_id'],bridge,prepared)
    assert bridge.calls==['inspect_query_runtime']
    assert run_one(root,bridge)=='inflight_requires_recovery'
    assert not (root/'operator_replays').exists()


def test_wrong_original_scope_refused_before_any_source_action(unknown):
    root,task,req,prepared,stage=unknown;bridge=ReplayBridge()
    atomic_write_json(prepared,{**req,'task_id':task['task_id'],'start':'1900-01-01'})
    with pytest.raises(ValueError):retry_unknown_download(root,task['task_id'],bridge,prepared)
    assert bridge.calls==[]


def _activate(root,task,req):
    attempt=task['task_id']+'-'+uuid.uuid4().hex
    prepared=root/'requests'/(attempt+'.json');output=root/'raw'/(attempt+'.json')
    atomic_write_json(prepared,{**req,'task_id':task['task_id'],'query_attempt_id':attempt})
    begin_attempt(root,task,attempt,prepared,output)
    stage={**{k:req[k] for k in ('type','smart_id','table','fields','company_labels','date_labels')},
           'contract_version':4,'task_id':task['task_id'],'stage':'prepreview_verified','source_scope_proof':scope_proof(),
           'query_attempt_id':attempt,'preview_submission_contract':PREVIEW_SUBMISSION_CONTRACT,'before_preview_signatures':['old']}
    atomic_write_json(root/'raw'/(attempt+'.json.stage.json'),stage)
    with closing(connect(root)) as con:task=dict(con.execute('SELECT * FROM tasks WHERE task_id=?',(task['task_id'],)).fetchone())
    return task,attempt,prepared


def test_multiple_original_stages_recover_only_exact_active_attempt(unknown):
    root,task,req,_,_=unknown;task,attempt,_=_activate(root,task,req)
    stage,path=query_stage(root,task,req)
    assert stage['query_attempt_id']==attempt
    assert path.name==attempt+'.json.stage.json'
    bridge=ReplayBridge()
    with pytest.raises(ValueError,match='different desktop attempt'):
        validate_active_attempt(root,task,{**full_grid_export(),'query_attempt_id':'old'})


@pytest.mark.parametrize('mutation',['query_attempt_id','fresh_preview_transition_verified','preview_submission_contract'])
def test_missing_attempt_or_freshness_never_adopted(unknown,mutation):
    root,task,req,_,_=unknown;task,attempt,_=_activate(root,task,req)
    payload={**full_grid_export(),'query_attempt_id':attempt,'fresh_preview_transition_verified':True,
             'preview_submission_contract':PREVIEW_SUBMISSION_CONTRACT}
    validate_active_attempt(root,task,payload)
    payload[mutation]=None
    with pytest.raises(ValueError):validate_active_attempt(root,task,payload)


def test_stale_operator_stage_refused_before_readback(unknown):
    root,task,req,prepared,_=unknown;_activate(root,task,req);bridge=ReplayBridge()
    with pytest.raises(ValueError,match='current unresolved attempt'):
        retry_unknown_download(root,task['task_id'],bridge,prepared)
    assert bridge.calls==[]


def test_bridge_submission_has_one_guarded_msaa_action_no_message_fallback():
    source=(Path(__file__).resolve().parents[1]/'scripts/tej_smart_wizard_bridge.ps1').read_text()
    body=source.split('$querySubmissionPossible=$true',1)[1].split('$json=$payload',1)[0]
    assert body.count('BeginPreviewDefaultAction')==1
    assert 'PostMessageW' not in body and '0xF5' not in body
    assert 'No proved fresh result transition' in body and '$signature -cnotin $beforePreviewSignatures' in body
    worker=source.split('public static void BeginPreviewDefaultAction',1)[1].split('public static int PreviewActionState',1)[0]
    assert worker.count('a.accDoDefaultAction(0)')==1
    assert 'CompareExchange(ref previewStarted,1,0)' in worker


def test_prepared_execution_metadata_does_not_break_exact_source_scope_or_expand_authority():
    request={'action':'plan','table':'Exact','fields':['Raw'],'contract_version':4}
    task={'task_id':'task','active_attempt_id':None}
    original={**request,'task_id':'task','desktop_input_contract':DESKTOP_INPUT_CONTRACT}
    assert prepared_request_matches(original,request,task)
    assert not prepared_request_matches({**original,'table':'Different'},request,task)
    assert not prepared_request_matches({**original,'unreviewed_execution_flag':True},request,task)
    assert not prepared_request_matches({**original,'desktop_input_contract':'unknown'},request,task)
    assert not prepared_request_matches({**original,'query_attempt_id':'other'},request,task)
    request['action']='download';task['active_attempt_id']='task-current'
    assert prepared_request_matches({**request,'task_id':'task','query_attempt_id':'task-current',
                                     'desktop_input_contract':DESKTOP_INPUT_CONTRACT},request,task)
