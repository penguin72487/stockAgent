"""Recover source progress, not merely process liveness or an empty stage."""
from contextlib import closing
from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path
import uuid

import pytest

from downloader.artifact_io import atomic_write_json
from downloader.tej_desktop_attempts import finish_attempt, defer_unsent_prequery
from downloader.tej_history import (connect, recover_legacy_local_input,
    PREQUERY_FAILURES, DesktopBridge)
from downloader.tej_scheduler import (RESPONSE_RECOVERY_CONTRACT,
    recover_retained_desktop_response, WatchPolicy)
from test_tej_desktop_attempts import unknown as unknown, registry as registry, _activate
from test_tej_desktop_attempts import ReplayBridge
from test_tej_history import FakeBridge, full_grid_export, empty_export
from test_tej_prequery_recovery import prepare, unresolved


CONFIG={'automation':{'response_recovery_contract':RESPONSE_RECOVERY_CONTRACT}}


@pytest.fixture
def ready(registry):
    from downloader.tej_history import run_one
    _,root,_,config=registry
    assert run_one(root,FakeBridge())=='completed_task'
    return root,config,FakeBridge()


class RetainedBridge:
    def __init__(self, *, notices=None, mutation=None):
        self.calls=[]; self.notices=notices or []; self.mutation=mutation

    def execute(self, root, task):
        req=json.loads(task['request_json']); action=req['action']; self.calls.append(action)
        if action=='inspect_notices':
            payload={'contract_version':4,'provider':'tej_smart_wizard','task_id':task['task_id'],
                'observed_at_utc':datetime.now(UTC).isoformat(),
                'action':action,'notices':self.notices,'data_query_repeated':False,'credentials_read':False,
                'root_enabled':True,'root_visible':True,'normal_message_completed':True,'hung_window':False}
        elif action in {'recover_preview','resolve_empty'}:
            payload={**(empty_export() if action=='resolve_empty' else full_grid_export()),
                'task_id':task['task_id'],'query_attempt_id':req['query_attempt_id'],
                'preview_submission_contract':req['original_query_stage']['preview_submission_contract'],
                'fresh_preview_transition_verified':True}
            if self.mutation: payload[self.mutation]=None
        else:
            pytest.fail('Self healing must not submit any new query: '+action)
        path=root/'raw'/(task['task_id']+'-'+uuid.uuid4().hex+'.json')
        atomic_write_json(path,payload)
        return payload,path,0.01


def active_unknown(unknown):
    root,task,req,_,_=unknown
    task,attempt,prepared=_activate(root,task,req)
    finish_attempt(root,attempt,'unknown_outcome')
    return root,task,attempt,prepared


@pytest.mark.parametrize('empty',[False,True])
def test_late_original_response_is_adopted_without_a_new_preview_or_fresh_throughput_sample(unknown,empty):
    root,task,attempt,prepared=active_unknown(unknown)
    notices=[{'controls':[{'class':'Static','name':'ERROR1:No data !!(wbk22)'},
                          {'class':'Button','name':'OK'}]}] if empty else []
    bridge=RetainedBridge(notices=notices)
    original=prepared.read_bytes()
    assert recover_retained_desktop_response(root,bridge,CONFIG)
    assert bridge.calls==['inspect_notices','resolve_empty' if empty else 'recover_preview']
    assert prepared.read_bytes()==original
    with closing(connect(root)) as con:
        result=dict(con.execute('SELECT * FROM tasks WHERE task_id=?',(task['task_id'],)).fetchone())
    assert result['state']=='complete' and result['actual_rows']==(0 if empty else 4)
    assert result['seconds'] is None
    assert not recover_retained_desktop_response(root,bridge,CONFIG)
    assert len(bridge.calls)==2


@pytest.mark.parametrize('mutation',['fresh_preview_transition_verified','query_attempt_id',
    'preview_submission_contract','source_scope_proof'])
def test_stale_or_foreign_retained_response_stays_unknown_and_backoff_survives_restart(unknown,mutation):
    root,task,attempt,_=active_unknown(unknown); bridge=RetainedBridge(mutation=mutation)
    assert not recover_retained_desktop_response(root,bridge,CONFIG)
    status=json.loads((root/'desktop_response_recovery.json').read_text())
    assert status['state']=='waiting_original_response' and status['failures']==1
    assert not recover_retained_desktop_response(root,RetainedBridge(),CONFIG)
    assert bridge.calls==['inspect_notices','recover_preview']
    with closing(connect(root)) as con:
        result=con.execute('SELECT state,actual_rows FROM tasks WHERE task_id=?',(task['task_id'],)).fetchone()
    assert tuple(result)==('blocked',None)


@pytest.mark.parametrize('text,button',[('Please login','OK'),('Quota exceeded','OK'),
    ('ERROR1:No data !!(wbk22)','Yes'),('ERROR1:No data !!(unknown space)','OK')])
def test_auth_quota_and_unreviewed_notices_are_never_acknowledged(unknown,text,button):
    root,task,attempt,_=active_unknown(unknown)
    bridge=RetainedBridge(notices=[{'controls':[{'class':'Static','name':text},{'class':'Button','name':button}]}])
    assert not recover_retained_desktop_response(root,bridge,CONFIG)
    assert bridge.calls==['inspect_notices']


@pytest.mark.parametrize('gate',['disabled','missing_stage','wrong_stage','unfinished','running','other_unknown'])
def test_ambiguous_submission_evidence_cannot_trigger_even_a_desktop_probe(unknown,gate):
    root,task,attempt,_=active_unknown(unknown); config=CONFIG; bridge=RetainedBridge()
    if gate=='disabled':config={}
    elif gate=='missing_stage':(root/'raw'/(attempt+'.json.stage.json')).unlink()
    elif gate=='wrong_stage':
        path=root/'raw'/(attempt+'.json.stage.json');doc=json.loads(path.read_text())
        doc['query_attempt_id']='other';atomic_write_json(path,doc)
    else:
        with closing(connect(root)) as con,con:
            if gate=='unfinished':con.execute('UPDATE desktop_attempts SET finished_at_utc=NULL')
            elif gate=='running':con.execute("UPDATE tasks SET state='running' WHERE task_id=?",(task['task_id'],))
            else:con.execute("INSERT INTO tasks(task_id,table_id,kind,phase,priority,request_json,state,last_error_code) "
                "VALUES ('other','other','download','P1',100,'{}','blocked','unknown_outcome_no_auto_retry')")
    assert not recover_retained_desktop_response(root,bridge,config)
    assert bridge.calls==[]


@pytest.fixture
def legacy_catalog(unknown,monkeypatch):
    root,task,attempt,prepared=active_unknown(unknown)
    (root/'raw'/(attempt+'.json.stage.json')).unlink()
    script=root/'reviewed_bridge.ps1'
    script.write_text("if($i -lt 0){throw 'Catalog binding did not become available; selection not sent'}\n"
                      "} else {Select-Combo $h $pair[1]}\n"
                      "[TejBridgeNative]::BeginPreviewDefaultAction($ExpectedWindow,$previewButton)\n")
    monkeypatch.setattr('downloader.tej_history.LEGACY_CATALOG_BINDING_BRIDGE_SHA256',
                        hashlib.sha256(script.read_bytes()).hexdigest())
    monkeypatch.setattr('downloader.tej_history.subprocess.check_output',lambda *a,**k:str(script))
    path='C:\\Users\\fixture\\AppData\\Local\\StockAgent\\TEJSmartWizard\\worker-'+('a'*32)+'\\bridge.ps1'
    diagnostic=root/'diagnostics'/(attempt+'.txt');diagnostic.parent.mkdir(exist_ok=True)
    diagnostic.write_text('Catalog binding did not become available; selection not sent\n'
        'at Select-Combo, '+path+': line 1\nat <ScriptBlock>, '+path+': line 2\nat <ScriptBlock>, <No file>: line 1')
    return root,task,attempt,prepared,script,diagnostic


def test_reviewed_legacy_binding_exception_is_resolved_without_inventing_a_stage(legacy_catalog):
    root,task,attempt,prepared,_,diagnostic=legacy_catalog;bridge=RetainedBridge()
    original=(prepared.read_bytes(),diagnostic.read_bytes())
    result=recover_legacy_local_input(root,task['task_id'],bridge,prepared)
    assert result['original_attempt_proven_not_submitted'] and result['state']=='prequery_retry_scheduled'
    assert bridge.calls==['inspect_notices']
    assert (prepared.read_bytes(),diagnostic.read_bytes())==original
    assert not (root/'raw'/(attempt+'.json.stage.json')).exists()
    assert not (root/'raw'/(attempt+'.json.outcome.json')).exists()
    with closing(connect(root)) as con:
        assert con.execute('SELECT state FROM desktop_attempts WHERE attempt_id=?',(attempt,)).fetchone()[0]=='proven_not_submitted'


@pytest.mark.parametrize('mutation',['script','callsite','stack','stage','response','notices','finished'])
def test_legacy_negative_proof_cannot_be_inferred_from_absent_stage_alone(legacy_catalog,mutation):
    root,task,attempt,prepared,script,diagnostic=legacy_catalog;bridge=RetainedBridge()
    if mutation=='script':script.write_text(script.read_text()+'changed')
    elif mutation=='callsite':diagnostic.write_text(diagnostic.read_text().replace(': line 1\n',': line 3\n',1))
    elif mutation=='stack':diagnostic.write_text(diagnostic.read_text()+'\nforeign frame')
    elif mutation in {'stage','response'}:
        atomic_write_json(root/'raw'/(attempt+('.json.stage.json' if mutation=='stage' else '.json')),{'may_have_submitted':True})
    elif mutation=='notices':bridge=RetainedBridge(notices=[{'controls':[]}])
    else:
        with closing(connect(root)) as con,con:con.execute('UPDATE desktop_attempts SET finished_at_utc=NULL')
    with pytest.raises(ValueError):recover_legacy_local_input(root,task['task_id'],bridge,prepared)
    with closing(connect(root)) as con:
        assert con.execute('SELECT state FROM tasks WHERE task_id=?',(task['task_id'],)).fetchone()[0]=='blocked'


@pytest.mark.parametrize('code',['local_source_binding_failed_before_preview','local_query_preparation_failed_before_preview'])
def test_new_preparation_failures_share_existing_bounded_proof_and_retry_policy(ready,code):
    from downloader.tej_desktop_attempts import recover_prequery_outcome
    root,_,_=ready;task=unresolved(root)
    for index in range(3):
        if index:
            with closing(connect(root)) as con,con:
                con.execute("UPDATE tasks SET state='running' WHERE task_id=?",(task['task_id'],))
                task=dict(con.execute('SELECT * FROM tasks WHERE task_id=?',(task['task_id'],)).fetchone())
        prepare(root,task,code=code)
        assert recover_prequery_outcome(root,task['task_id'])
    with closing(connect(root)) as con:
        row=con.execute('SELECT state,last_error_code,safe_prequery_retries FROM tasks WHERE task_id=?',(task['task_id'],)).fetchone()
    assert tuple(row)==('blocked',PREQUERY_FAILURES[code],2)


def test_binding_repair_is_metadata_only_and_checked_before_table_defer(ready):
    root,_,_=ready;task=unresolved(root)
    attempt,_,_,_=prepare(root,task,code='local_source_binding_failed_before_preview')
    finish_attempt(root,attempt,'proven_not_submitted')
    with closing(connect(root)) as con,con:
        con.execute("UPDATE tasks SET safe_prequery_retries=2,last_error_code='source_binding_prequery_needs_review' WHERE task_id=?",(task['task_id'],))
    class Repair:
        calls=[]
        def execute(self,root,task):
            req=json.loads(task['request_json']);self.calls.append(req['action'])
            assert req['action']=='repair_source_binding'
            payload={**{k:req[k] for k in ('type','smart_id','table')},'contract_version':4,
                'provider':'tej_smart_wizard','action':req['action'],'task_id':task['task_id'],
                'observed_at_utc':datetime.now(UTC).isoformat(),
                **{k:True for k in ('source_binding_stable','vendor_notices_absent','source_selectors_enabled','company_group_enabled','binding_matches_failed_plan')},
                **{k:False for k in ('market_data_query_submitted','source_axes_adopted','credentials_read')}}
            path=root/'raw'/(task['task_id']+'-'+uuid.uuid4().hex+'.json');atomic_write_json(path,payload)
            return payload,path,0.1
    bridge=Repair();result=defer_unsent_prequery(root,task['task_id'],bridge)
    assert result['provider_queries_sent']==0 and result['backoff_seconds']==60
    assert bridge.calls==['repair_source_binding']


def test_unreviewed_automatic_recovery_contract_rejected_before_windows_access():
    with pytest.raises(ValueError,match='Unreviewed automatic response recovery'):
        WatchPolicy.from_config({'automation':{'enabled':True,'response_recovery_contract':'retry_everything'}})


def test_canonical_watch_recovers_the_retained_response_before_continuing_queue(unknown):
    from downloader.tej_scheduler import watch_queue
    from test_tej_scheduler import InstantWait
    root,task,attempt,_=active_unknown(unknown); bridge=RetainedBridge(); calls=[]
    config={'automation':{'enabled':True,'response_recovery_contract':RESPONSE_RECOVERY_CONTRACT}}
    watch_queue(root,bridge,config,stop=InstantWait(),runner=lambda *_:calls.append(1) or 'idle',
                max_cycles=1,emit=lambda *a,**k:None)
    assert bridge.calls==['inspect_notices','recover_preview'] and calls==[1]
    with closing(connect(root)) as con:
        assert con.execute('SELECT state FROM tasks WHERE task_id=?',(task['task_id'],)).fetchone()[0]=='complete'


@pytest.mark.parametrize('action,deadline',[
    ('inspect_notices',30),('inspect_source_binding',30),('inspect_query_runtime',30),
    ('confirm_metadata_error_cleared',30),('recover_preview',900),('repair_source_binding',900)])
def test_metadata_probes_cannot_block_for_the_full_grid_readback_deadline(registry,monkeypatch,action,deadline):
    repo,root,_,_=registry;captured=[]
    monkeypatch.setattr(DesktopBridge,'windows_path',staticmethod(str))
    def windows(*args,**kwargs):
        captured.append(kwargs['timeout'])
        raise RuntimeError('fixture transport ended, no provider call')
    monkeypatch.setattr('downloader.tej_windows_transport.run_guarded_windows',windows)
    bridge=DesktopBridge(Path(__file__).resolve().parents[1],{'TejProcessId':1,'ExpectedWindow':2,'ExpectedTitle':'fixture',
                             'ExpectedWorkbook':'fixture','ExpectedExcelWindow':3})
    with pytest.raises(RuntimeError,match='fixture transport ended'):
        bridge.execute(root,{'task_id':'a'*24,'request_json':json.dumps({'action':action})})
    assert captured==[deadline]


def test_prequery_catch_guard_and_binding_repair_have_no_preview_submission_path():
    source=(Path(__file__).resolve().parents[1]/'scripts/tej_smart_wizard_bridge.ps1').read_text()
    body=source.split('function Prepare-SourceBinding {',1)[1].split('function Click-Button',1)[0]
    assert 'BeginPreview' not in body and 'Select-Combo $parent $parentPair[1] $true' in body
    catch=source.split('# The durable negative proof covers EVERY',1)[1].split('if($requestDoc.action',2)
    assert '-not $querySubmissionPossible' in catch[1]
    assert "-not (Test-Path -LiteralPath ($Output+'.stage.json'))" in catch[1]
    assert 'SignedControlResult(Message(control,0x147u,0,0))' in source
    assert "contract_version=4;provider='tej_smart_wizard';action='inspect_notices'" in source


def test_active_script_release_does_not_change_when_the_checkout_is_updated(tmp_path):
    repo=tmp_path/'repo';repo.joinpath('scripts').mkdir(parents=True)
    source=repo/'scripts/tej_smart_wizard_bridge.ps1';source.write_text('first version')
    root=tmp_path/'data';request=root/'requests'/('a'*24+'-'+ 'b'*32+'.json')
    atomic_write_json(request,{'task_id':'a'*24})
    bridge=DesktopBridge(repo,{})
    release,digest=bridge.script_release(root,request.stem,request)
    source.write_text('second version')
    assert bridge.script_release(root,request.stem,request)==(release,digest)
    request=root/'requests'/('a'*24+'-'+ 'c'*32+'.json')
    atomic_write_json(request,{'task_id':'a'*24})
    next_release,next_digest=bridge.script_release(root,request.stem,request)
    assert release.read_text()=='first version' and next_release.read_text()=='second version'
    assert release!=next_release and digest!=next_digest
    manifest=json.loads((root/'launches'/(request.stem+'.bridge.json')).read_text())
    assert manifest['request_sha256']==hashlib.sha256(request.read_bytes()).hexdigest()
    assert manifest['bridge_script_sha256']==next_digest
    next_release.write_text('corrupt old published object')
    with pytest.raises(ValueError,match='never overwrite'):
        bridge.script_release(root,request.stem,request)


@pytest.fixture
def unstaged_script_copy(unknown):
    root,task,attempt,prepared=active_unknown(unknown)
    (root/'raw'/(attempt+'.json.stage.json')).unlink()
    diagnostic=root/'diagnostics'/(attempt+'.txt');diagnostic.parent.mkdir(exist_ok=True)
    diagnostic.write_text("Script copy mismatch\nAt line:1 char:1414\n"
        "+ ...){throw 'Script copy mismatch'};try{& ...\n+ ~~~~~~~~~~\n"
        "    + CategoryInfo : OperationStopped: (Script copy mismatch:String) [], RuntimeException\n"
        "    + FullyQualifiedErrorId : Script copy mismatch\n")
    return root,task,prepared,diagnostic


def test_old_unstaged_script_failure_requires_explicit_one_shot_operator_authority(unstaged_script_copy):
    from downloader.tej_desktop_attempts import retry_unknown_download
    from downloader.tej_history import run_one
    root,task,prepared,diagnostic=unstaged_script_copy;bridge=ReplayBridge()
    before=(prepared.read_bytes(),diagnostic.read_bytes())
    with pytest.raises(ValueError):retry_unknown_download(root,task['task_id'],bridge,prepared)
    assert bridge.calls==[] and run_one(root,bridge)=='inflight_requires_recovery'
    result=retry_unknown_download(root,task['task_id'],bridge,prepared,allow_unstaged_script_preparation=True)
    assert result['state']=='completed_task' and result['automatic_retry'] is False
    assert bridge.calls==['inspect_query_runtime','download']
    assert before==(prepared.read_bytes(),diagnostic.read_bytes())
    audit=json.loads(next((root/'operator_replays').glob('*.json')).read_text())
    assert audit['unstaged_script_preparation_replay_explicitly_authorized'] is True
    assert audit['original_outcome']=='unknown_retained_not_claimed_unsent'


@pytest.mark.parametrize('mutation',['wrong_error','stage','response','progress','both_modes'])
def test_script_prepare_operator_grant_is_not_a_generic_unknown_replay_escape(unstaged_script_copy,mutation):
    from downloader.tej_desktop_attempts import retry_unknown_download
    root,task,prepared,diagnostic=unstaged_script_copy;bridge=ReplayBridge()
    if mutation=='wrong_error':diagnostic.write_text('Any failure at all')
    elif mutation in {'stage','response','progress'}:
        folder='progress' if mutation=='progress' else 'raw'
        suffix='.json.stage.json' if mutation=='stage' else '.json.progress.json' if mutation=='progress' else '.json'
        atomic_write_json(root/folder/(prepared.stem+suffix),{'may_have_submitted':True})
    with pytest.raises(ValueError):
        retry_unknown_download(root,task['task_id'],bridge,prepared,allow_unstaged_script_preparation=True,
                               allow_unstaged_interop=mutation=='both_modes')
    assert bridge.calls==[]


def test_rendered_acceptance_keeps_body_reads_out_of_page_close_event_callbacks():
    source=(Path(__file__).resolve().parents[1]/'scripts/verify_tej_dashboard.py').read_text()
    callback=source.split('def record_status_response(reply):',1)[1].split("page.on('response'",1)[0]
    assert 'status_responses.append(reply)' in callback and 'reply.json()' not in callback
    assert "page.remove_listener('response',record_status_response)" in source
