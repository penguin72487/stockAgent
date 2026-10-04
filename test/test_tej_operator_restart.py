"""Explicit reset of an unknown query retains evidence and replays once."""
from contextlib import closing
import json
from pathlib import Path
import uuid

import pytest

from downloader.artifact_io import atomic_write_json
from downloader.tej_desktop_attempts import begin_attempt, finish_attempt
from downloader.tej_history import connect, run_one, task_request, PREVIEW_SUBMISSION_CONTRACT
from downloader.tej_query_lifecycle import BARRIER, restart_addin, restart_unknown_download
from test_tej_history import FakeBridge, full_grid_export, registry, scope_proof
from test_tej_query_lifecycle import InterfaceBridge, restart_phase_payload


def prepare_attempt(root, task):
    request=task_request(root,task)
    attempt=task['task_id']+'-'+uuid.uuid4().hex
    prepared=root/'requests'/(attempt+'.json');output=root/'raw'/(attempt+'.json')
    atomic_write_json(prepared,{**request,'task_id':task['task_id'],'query_attempt_id':attempt})
    begin_attempt(root,task,attempt,prepared,output)
    stage={**{k:request[k] for k in ('type','smart_id','table','fields','company_labels','date_labels')},
        'contract_version':4,'task_id':task['task_id'],'stage':'prepreview_verified',
        'source_scope_proof':scope_proof(),'query_attempt_id':attempt,
        'preview_submission_contract':PREVIEW_SUBMISSION_CONTRACT}
    atomic_write_json(output.with_suffix('.json.stage.json'),stage)
    return prepared,output,attempt


@pytest.fixture
def stalled(registry):
    _,root,_,_=registry
    run_one(root,FakeBridge())
    with closing(connect(root)) as con,con:
        row=con.execute("SELECT * FROM tasks WHERE kind='download' LIMIT 1").fetchone()
        task=dict(row)
        con.execute("UPDATE tasks SET state='blocked',last_error_code='unknown_outcome_no_auto_retry',"
                    "attempted_at_utc='2026-10-02T01:00:00+00:00' WHERE task_id=?",(task['task_id'],))
    prepared,output,attempt=prepare_attempt(root,task)
    finish_attempt(root,attempt,'unknown_outcome')
    session={'TejProcessId':42,'ExpectedWindow':100,'ExpectedTitle':'TEJ Smart Wizard (Version 4.1.1.7) -- Book2',
             'ExpectedWorkbook':'Book2','ExpectedExcelWindow':90}
    atomic_write_json(root/'desktop_session.json',session)
    with closing(connect(root)) as con:
        task=dict(con.execute('SELECT * FROM tasks WHERE task_id=?',(task['task_id'],)).fetchone())
    return root,task,session,prepared,output


class RestartBridge(InterfaceBridge):
    def __init__(self,session,fail=False):
        super().__init__(session);self.fail=fail

    def execute(self,root,task):
        request=json.loads(task['request_json'])
        if request['action']=='confirm_metadata_error_cleared':
            return super().execute(root,task)
        assert request['action']=='download'
        self.calls.append('download')
        _,output,attempt=prepare_attempt(root,task)
        if self.fail:
            finish_attempt(root,attempt,'unknown_outcome')
            raise RuntimeError('new source outcome unknown')
        payload={**full_grid_export(),'task_id':task['task_id'],'query_attempt_id':attempt,
                 'preview_submission_contract':PREVIEW_SUBMISSION_CONTRACT,'fresh_preview_transition_verified':True}
        atomic_write_json(output,payload)
        finish_attempt(root,attempt,'response_received_not_yet_adopted')
        return payload,output,1


def fake_restart(monkeypatch,session):
    calls=[]
    def phase(bridge,root,action,path,*,restart_pins):
        calls.append(action);payload=restart_phase_payload(session,action)
        atomic_write_json(path,payload);return payload
    monkeypatch.setattr('downloader.tej_query_lifecycle._execute_phase',phase)
    return calls


@pytest.mark.parametrize('ack',[False,None,1])
def test_unknown_restart_requires_explicit_ack_before_any_reset(stalled,ack):
    root,task,session,prepared,_=stalled;bridge=RestartBridge(session)
    with pytest.raises(ValueError,match='acknowledgement'):
        restart_unknown_download(root,task['task_id'],bridge,root/'desktop_session.json',prepared,
            allow_restart=True,allow_discard=True,acknowledge_unknown_usage=ack)
    assert not bridge.calls and not (root/'query_lifecycle').exists()


@pytest.mark.parametrize('mutation',['saved_response','receipt','wrong_request','wrong_stage','another_unknown','running'])
def test_saved_or_ambiguous_unknown_is_not_discarded(stalled,mutation):
    root,task,session,prepared,output=stalled;bridge=RestartBridge(session)
    if mutation=='saved_response':atomic_write_json(output,{'task_id':task['task_id']})
    elif mutation=='receipt':atomic_write_json(root/'receipts'/(task['task_id']+'.json'),{})
    elif mutation=='wrong_request':
        doc=json.loads(prepared.read_text());doc['table']='foreign';atomic_write_json(prepared,doc)
    elif mutation=='wrong_stage':
        path=output.with_suffix('.json.stage.json');doc=json.loads(path.read_text());doc['query_attempt_id']='foreign';atomic_write_json(path,doc)
    else:
        with closing(connect(root)) as con,con:
            con.execute("UPDATE tasks SET state=?,last_error_code='unknown_outcome_no_auto_retry' WHERE task_id!=?",
                        ('running' if mutation=='running' else 'blocked',task['task_id']))
    with pytest.raises(ValueError):
        restart_unknown_download(root,task['task_id'],bridge,root/'desktop_session.json',prepared,
            allow_restart=True,allow_discard=True,acknowledge_unknown_usage=True)
    assert not bridge.calls and not (root/'query_lifecycle').exists()


@pytest.mark.parametrize('fail',[False,True])
def test_exact_reset_keeps_unknown_then_consumes_only_one_replay(stalled,monkeypatch,fail):
    root,task,session,prepared,output=stalled
    original=prepared.read_bytes();stage=output.with_suffix('.json.stage.json').read_bytes()
    calls=fake_restart(monkeypatch,session);bridge=RestartBridge(session,fail=fail)
    result=restart_unknown_download(root,task['task_id'],bridge,root/'desktop_session.json',prepared,
        allow_restart=True,allow_discard=True,acknowledge_unknown_usage=True)
    assert calls==['inspect-addin','stop-addin','open']
    assert bridge.calls==['confirm_metadata_error_cleared','download']
    assert result['automatic_retry'] is False and result['original_unknown_evidence_retained'] is True
    assert result['possible_additional_provider_usage'] is True
    assert prepared.read_bytes()==original and output.with_suffix('.json.stage.json').read_bytes()==stage
    assert not output.exists()
    assert result['state']==('unknown_outcome_no_auto_retry' if fail else 'completed_task')
    with closing(connect(root)) as con:
        old=con.execute('SELECT state FROM desktop_attempts WHERE attempt_id=?',(task['active_attempt_id'],)).fetchone()
        assert old[0]=='unknown_outcome'
        replay=dict(con.execute('SELECT * FROM desktop_replays WHERE authorization_id=?',(result['authorization_id'],)).fetchone())
        assert replay['consumed_at_utc'] and replay['outcome']==result['state']
        assert con.execute('SELECT 1 FROM meta WHERE key=?',(BARRIER,)).fetchone() is None
        audit=json.loads((root/replay['audit_path']).read_text())
        assert audit['original_outcome']=='unknown_retained_not_claimed_unsent'
        assert set(audit['restart_evidence_sha256'])=={'inspect-addin','stop-addin','open'}
    with pytest.raises(ValueError,match='consumed'):
        run_one(root,bridge,retry_authorization_id=result['authorization_id'])
    assert bridge.calls==['confirm_metadata_error_cleared','download']


def test_failed_source_reset_never_replays_unknown(stalled,monkeypatch):
    root,task,session,prepared,_=stalled;bridge=RestartBridge(session);calls=[]
    def fail(bridge,root,action,path,*,restart_pins):
        calls.append(action);raise RuntimeError('reset unresolved')
    monkeypatch.setattr('downloader.tej_query_lifecycle._execute_phase',fail)
    with pytest.raises(RuntimeError):
        restart_unknown_download(root,task['task_id'],bridge,root/'desktop_session.json',prepared,
            allow_restart=True,allow_discard=True,acknowledge_unknown_usage=True)
    assert calls==['inspect-addin'] and not bridge.calls
    with closing(connect(root)) as con:
        assert con.execute('SELECT COUNT(*) FROM desktop_replays').fetchone()[0]==0
        assert con.execute('SELECT 1 FROM meta WHERE key=?',(BARRIER,)).fetchone()
        assert con.execute('SELECT active_attempt_id FROM tasks WHERE task_id=?',(task['task_id'],)).fetchone()[0]==task['active_attempt_id']


def test_hung_restart_uses_native_original_identity_not_ghost_uia():
    repo=Path(__file__).resolve().parents[1]
    script=(repo/'scripts/reopen_tej_smart_wizard_query.ps1').read_text()
    restart=script.split("if($Action -in @('inspect-addin','stop-addin')) {",1)[1].split("} elseif($Action -eq 'close') {",1)[0]
    assert 'AutomationElement' not in restart
    for proof in ('GetWindowThreadProcessId','WindowTitle($ExpectedWindow)','WindowClass($ExpectedWindow)',
                  '$nativeOwner -ne $TejProcessId','$ExpectedProcessStartUtc -cne $processStart',
                  '$ExpectedImageSha256 -cne $imageHash'):
        assert proof in restart
    assert 'GhostWindowFromHungWindow' not in script and 'EnableWindow' not in script
    assert '[TejBridgeNative]::ProcessWindows($ExpectedWindow,$false)' in restart
    assert "$windows[0] -eq $ExpectedWindow -and [TejBridgeNative]::IsHungAppWindow" in restart
    assert "'sole_native_hung_query_no_other_application_windows_v1'" in restart
