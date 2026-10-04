"""Scratch-form reset is operator-only and cannot discard unresolved queries."""
from contextlib import closing
import json
from pathlib import Path

import pytest

from downloader.artifact_io import atomic_write_bytes, atomic_write_json
from downloader.tej_history import connect, run_one
from downloader.tej_query_lifecycle import (
    BARRIER, CONTRACT, RESTART_CONTRACT, mark_interface_barrier, reopen_query,
    restart_addin, verify_interface,
)
from test_tej_history import registry


class InterfaceBridge:
    def __init__(self,session,mutation=None):
        self.session=session;self.calls=[];self.mutation=mutation

    def execute(self,root,task):
        request=json.loads(task['request_json']);self.calls.append(request['action'])
        assert request['action']=='confirm_metadata_error_cleared'
        payload={'contract_version':4,'provider':'tej_smart_wizard','action':request['action'],'task_id':task['task_id'],
            **{k:request[k] for k in ('type','smart_id','table')},
            **{k:True for k in ('vendor_notices_absent','source_selectors_enabled','source_binding_stable')},
            **{k:False for k in ('market_data_query_submitted','source_axes_adopted','credentials_read')}}
        if self.mutation:payload[self.mutation]=None
        evidence=root/'raw'/'interface-readback.json';atomic_write_json(evidence,payload)
        return payload,evidence,1


@pytest.fixture
def reset_scope(registry):
    _,root,_,_=registry
    session={'TejProcessId':42,'ExpectedWindow':100,'ExpectedTitle':'TEJ Smart Wizard (Version 4.1.1.7) -- Book2',
             'ExpectedWorkbook':'Book2','ExpectedExcelWindow':90}
    atomic_write_json(root/'desktop_session.json',session)
    with closing(connect(root)) as con:
        task=con.execute('SELECT task_id FROM tasks LIMIT 1').fetchone()[0]
    return root,task,session


def test_interface_barrier_blocks_before_claiming_task_or_any_desktop_call(reset_scope):
    root,task,session=reset_scope
    mark_interface_barrier(root,reason='fixture_disabled_root')
    bridge=InterfaceBridge(session)
    assert run_one(root,bridge)==BARRIER
    assert not bridge.calls
    with closing(connect(root)) as con:
        assert con.execute('SELECT state FROM tasks WHERE task_id=?',(task,)).fetchone()[0]=='pending'
        assert con.execute('SELECT COUNT(*) FROM traffic').fetchone()[0]==0


@pytest.mark.parametrize('mutation',['source_binding_stable','vendor_notices_absent','market_data_query_submitted','source_axes_adopted'])
def test_bad_interface_proof_preserves_barrier(reset_scope,mutation):
    root,task,session=reset_scope;mark_interface_barrier(root,reason='fixture')
    with pytest.raises(ValueError):verify_interface(root,task,InterfaceBridge(session,mutation))
    with closing(connect(root)) as con:
        assert con.execute('SELECT 1 FROM meta WHERE key=?',(BARRIER,)).fetchone()


def test_verified_interface_never_resets_failed_table_or_sends_preview(reset_scope):
    root,task,session=reset_scope;mark_interface_barrier(root,reason='fixture')
    with closing(connect(root)) as con,con:
        con.execute("UPDATE tasks SET state='blocked',last_error_code='metadata_preparation_failed_deferred'")
    bridge=InterfaceBridge(session);result=verify_interface(root,task,bridge)
    assert result['failed_tasks_reset'] is False and result['data_query_repeated'] is False
    assert bridge.calls==['confirm_metadata_error_cleared']
    with closing(connect(root)) as con:
        assert con.execute('SELECT 1 FROM meta WHERE key=?',(BARRIER,)).fetchone() is None
        assert con.execute('SELECT state FROM tasks WHERE task_id=?',(task,)).fetchone()[0]=='blocked'


@pytest.mark.parametrize('allow_discard',[False,None,1])
def test_reset_requires_explicit_authorization_before_windows_action(reset_scope,allow_discard):
    root,task,session=reset_scope;bridge=InterfaceBridge(session)
    with pytest.raises(ValueError):reopen_query(root,task,bridge,root/'desktop_session.json',allow_discard=allow_discard)
    assert not bridge.calls and not (root/'query_lifecycle').exists()


def test_reset_refuses_unresolved_preview(reset_scope):
    root,task,session=reset_scope
    with closing(connect(root)) as con,con:
        con.execute("UPDATE tasks SET kind='download',state='blocked',last_error_code='unknown_outcome_no_auto_retry'")
    with pytest.raises(ValueError,match='unresolved Preview'):
        reopen_query(root,task,InterfaceBridge(session),root/'desktop_session.json',allow_discard=True)
    assert not (root/'query_lifecycle').exists()


def test_reset_rebinds_only_query_handle_and_retains_original_session(reset_scope,monkeypatch):
    root,task,session=reset_scope;calls=[];original=(root/'desktop_session.json').read_bytes()
    def phase(bridge,root,action,path):
        calls.append(action)
        payload={'contract':CONTRACT,'action':action,'old_query_window':session['ExpectedWindow'],
            'discard_scratch_query_settings_authorized':True,
            **{k:False for k in ('workbook_closed','workbook_saved','credentials_read','market_data_query_submitted')},
            'query_closed_verified' if action=='close' else 'query_open_verified':True}
        if action=='open':payload['new_session']={**session,'ExpectedWindow':200}
        atomic_write_json(path,payload);return payload
    monkeypatch.setattr('downloader.tej_query_lifecycle._execute_phase',phase)
    result=reopen_query(root,task,InterfaceBridge(session),root/'desktop_session.json',allow_discard=True)
    assert result['scratch_query_reopened'] and not result['raw_source_changed']
    assert calls==['close','open']
    assert json.loads((root/'desktop_session.json').read_text())=={**session,'ExpectedWindow':200}
    assert next((root/'query_lifecycle').glob('*/original_session.json')).read_bytes()==original


def test_reset_stops_after_failed_close_and_never_launches_or_changes_session(reset_scope,monkeypatch):
    root,task,session=reset_scope;calls=[]
    def fail(bridge,root,action,path):
        calls.append(action);raise RuntimeError('close unresolved')
    monkeypatch.setattr('downloader.tej_query_lifecycle._execute_phase',fail)
    with pytest.raises(RuntimeError):reopen_query(root,task,InterfaceBridge(session),root/'desktop_session.json',allow_discard=True)
    assert calls==['close'] and json.loads((root/'desktop_session.json').read_text())==session
    with closing(connect(root)) as con:
        assert con.execute('SELECT 1 FROM meta WHERE key=?',(BARRIER,)).fetchone()


@pytest.mark.parametrize('pid,attested,accepted',[(99,True,True),(99,False,False),(True,True,False),(0,True,False),('99',True,False)])
def test_query_only_recreated_process_requires_attestation_and_positive_integer_identity(reset_scope,monkeypatch,pid,attested,accepted):
    root,task,session=reset_scope
    def phase(bridge,root,action,path):
        proof={'contract':CONTRACT,'action':action,'old_query_window':session['ExpectedWindow'],
               'discard_scratch_query_settings_authorized':True,
               **{k:False for k in ('workbook_closed','workbook_saved','credentials_read','market_data_query_submitted')},
               'query_closed_verified' if action=='close' else 'query_open_verified':True}
        if action=='open':proof.update(new_session={**session,'ExpectedWindow':200,'TejProcessId':pid},
            process_recreated_after_query_close=attested,process_name='TEJAddin')
        atomic_write_json(path,proof);return proof
    monkeypatch.setattr('downloader.tej_query_lifecycle._execute_phase',phase)
    bridge=InterfaceBridge(session)
    if accepted:
        assert reopen_query(root,task,bridge,root/'desktop_session.json',allow_discard=True)['scratch_query_reopened']
    else:
        with pytest.raises(ValueError,match='session identity'):
            reopen_query(root,task,bridge,root/'desktop_session.json',allow_discard=True)
        assert json.loads((root/'desktop_session.json').read_text())==session


def test_query_only_reset_never_kills_and_addin_stop_has_separate_exact_identity_guard():
    script=(Path(__file__).resolve().parents[1]/'scripts/reopen_tej_smart_wizard_query.ps1').read_text()
    for forbidden in ('Kill(', 'mouse_event','SetCursorPos','SendInput','0xF5,0xF5','EnableWindow','DestroyWindow'):
        assert forbidden not in script
    query_only=script.split("} elseif($Action -eq 'close') {",1)[1].split('    } else {',1)[0]
    assert 'Stop-Process' not in query_only
    assert script.count('Stop-Process -InputObject $process -Force')==1
    assert "if($Action -in @('inspect-addin','stop-addin') -and -not $AllowRestartAddin)" in script
    assert "if($process.ProcessName -cne 'TEJAddin')" in script
    assert '$ExpectedProcessStartUtc -cne $processStart -or $ExpectedImageSha256 -cne $imageHash' in script
    assert 'Stop-Process -Name' not in script and 'Stop-Process -Id' not in script
    assert '$emptyConnectorDialogs+$scratchCloseConfirmations -gt 1' in script
    assert '$window.Current.Name -cne' in script and '$window.Current.ClassName -cne' in script
    assert '$nativeChildren.Count -ne 3' in script and '$nativeTexts.Count -ne 1' in script
    assert '$question -cne' in script and '$nativeButtons.Count -ne 2' in script
    assert script.count('PostMessageW([IntPtr]$ExpectedWindow,0x10')==1
    assert 'Had been setting conditions. Do you close form?' in script
    assert 'Original query still exists; do not create another query' in script
    assert "$texts[0].Current.Name).Replace(\"`r`n\",' ').Replace(\"`n\",' ')" in script
    assert "adopt_exact_owned_close_confirmation_without_close_replay_v1" in script
    assert "$newProcess.SessionId -ne $excelProcess.SessionId" in script
    assert "$newProcess.StartTime.ToUniversalTime() -lt $launchAt.AddSeconds(-5)" in script


def restart_phase_payload(session,phase):
    payload={'contract':RESTART_CONTRACT,'action':phase,'old_query_window':session['ExpectedWindow'],
        'discard_scratch_query_settings_authorized':True,'addin_process_restart_authorized':True,
        'process_name':'TEJAddin','process_id':session['TejProcessId'],
        'process_start_utc':'2026-10-02T00:00:00+00:00','image_sha256':'A'*64,
        **{k:False for k in ('workbook_closed','workbook_saved','credentials_read','market_data_query_submitted')}}
    if phase=='stop-addin':payload['addin_stopped_verified']=True
    if phase=='open':
        payload['query_open_verified']=True
        payload['new_session']={**session,'TejProcessId':43,'ExpectedWindow':200}
    return payload


@pytest.mark.parametrize(('allow_restart','allow_discard'),[(False,True),(None,True),(1,True),(True,False),(True,1)])
def test_process_restart_needs_both_explicit_authorizations(reset_scope,allow_restart,allow_discard):
    root,task,session=reset_scope;bridge=InterfaceBridge(session)
    with pytest.raises(ValueError,match='separate process-restart'):
        restart_addin(root,task,bridge,root/'desktop_session.json',
                      allow_restart=allow_restart,allow_discard=allow_discard)
    assert not bridge.calls and not (root/'query_lifecycle').exists()


@pytest.mark.parametrize('state',['running','blocked'])
def test_process_restart_never_discards_unresolved_query(reset_scope,state):
    root,task,session=reset_scope
    with closing(connect(root)) as con,con:
        con.execute("UPDATE tasks SET kind='download',state=?,last_error_code='unknown_outcome_no_auto_retry'",(state,))
    with pytest.raises(ValueError,match='unresolved Preview'):
        restart_addin(root,task,InterfaceBridge(session),root/'desktop_session.json',allow_restart=True,allow_discard=True)
    assert not (root/'query_lifecycle').exists()


def test_process_restart_pins_old_image_and_rebinds_new_pid_without_closing_workbook(reset_scope,monkeypatch):
    root,task,session=reset_scope;calls=[];original=(root/'desktop_session.json').read_bytes()
    def phase(bridge,root,action,path,*,restart_pins):
        calls.append(action)
        expected={'ExpectedProcessStartUtc':'','ExpectedImageSha256':''} if action=='inspect-addin' else {
            'ExpectedProcessStartUtc':'2026-10-02T00:00:00+00:00','ExpectedImageSha256':'A'*64}
        assert restart_pins==expected
        payload=restart_phase_payload(session,action);atomic_write_json(path,payload);return payload
    monkeypatch.setattr('downloader.tej_query_lifecycle._execute_phase',phase)
    bridge=InterfaceBridge(session)
    result=restart_addin(root,task,bridge,root/'desktop_session.json',allow_restart=True,allow_discard=True)
    assert calls==['inspect-addin','stop-addin','open']
    assert result['addin_restarted'] and not result['workbook_closed'] and not result['raw_source_changed']
    assert json.loads((root/'desktop_session.json').read_text())=={**session,'TejProcessId':43,'ExpectedWindow':200}
    assert next((root/'query_lifecycle').glob('*/original_session.json')).read_bytes()==original
    assert bridge.calls==['confirm_metadata_error_cleared']


@pytest.mark.parametrize(('phase','key','value'),[
    ('inspect-addin','process_id',44),('inspect-addin','process_name','EXCEL'),
    ('inspect-addin','process_start_utc','2026-10-02T00:00:00'),('inspect-addin','image_sha256','not-a-hash'),
    ('inspect-addin','addin_process_restart_authorized',False),
    ('stop-addin','process_id',44),('stop-addin','addin_stopped_verified',False),
    ('stop-addin','image_sha256','B'*64),('stop-addin','process_start_utc','2026-10-02T00:00:01+00:00'),
    ('open','query_open_verified',False),('open','image_sha256','B'*64),
    ('open','workbook_closed',True),('open','market_data_query_submitted',True),
])
def test_bad_restart_phase_never_repeated_and_keeps_barrier_and_session(reset_scope,monkeypatch,phase,key,value):
    root,task,session=reset_scope;calls=[]
    def fake(bridge,root,action,path,*,restart_pins):
        calls.append(action);payload=restart_phase_payload(session,action)
        if action==phase:payload[key]=value
        atomic_write_json(path,payload);return payload
    monkeypatch.setattr('downloader.tej_query_lifecycle._execute_phase',fake)
    with pytest.raises(ValueError):
        restart_addin(root,task,InterfaceBridge(session),root/'desktop_session.json',allow_restart=True,allow_discard=True)
    assert calls==['inspect-addin','stop-addin','open'][:['inspect-addin','stop-addin','open'].index(phase)+1]
    assert json.loads((root/'desktop_session.json').read_text())==session
    with closing(connect(root)) as con:
        assert con.execute('SELECT 1 FROM meta WHERE key=?',(BARRIER,)).fetchone()


@pytest.mark.parametrize(('key','value'),[
    ('TejProcessId',42),('TejProcessId',True),('ExpectedWindow',100),
    ('ExpectedWorkbook','OtherBook'),('ExpectedTitle','OtherTitle'),('ExpectedExcelWindow',91),
])
def test_restart_rejects_foreign_workbook_or_unverified_new_identity(reset_scope,monkeypatch,key,value):
    root,task,session=reset_scope
    def phase(bridge,root,action,path,*,restart_pins):
        payload=restart_phase_payload(session,action)
        if action=='open':payload['new_session'][key]=value
        atomic_write_json(path,payload);return payload
    monkeypatch.setattr('downloader.tej_query_lifecycle._execute_phase',phase)
    bridge=InterfaceBridge(session)
    with pytest.raises(ValueError,match='session identity'):
        restart_addin(root,task,bridge,root/'desktop_session.json',allow_restart=True,allow_discard=True)
    assert not bridge.calls and json.loads((root/'desktop_session.json').read_text())==session


def stopped_run(root,session):
    previous=root/'query_lifecycle'/('a'*32)
    previous.mkdir(parents=True)
    atomic_write_bytes(previous/'original_session.json',(root/'desktop_session.json').read_bytes())
    for phase in ('inspect-addin','stop-addin'):
        atomic_write_json(previous/(phase+'.json'),restart_phase_payload(session,phase))
    atomic_write_bytes(root/'diagnostics'/('query_lifecycle-'+previous.name+'-open.txt'),
        b'Exception calling "GetCurrentPattern" with "1" argument(s): "Unsupported Pattern."\r\n')
    return previous


def test_verified_stop_resume_never_stops_again_or_changes_source_bindings(reset_scope,monkeypatch):
    root,task,session=reset_scope;previous=stopped_run(root,session);calls=[]
    def phase(bridge,root,action,path,*,restart_pins):
        calls.append(action);assert action=='open' and restart_pins['ExpectedImageSha256']=='A'*64
        payload=restart_phase_payload(session,action);atomic_write_json(path,payload);return payload
    monkeypatch.setattr('downloader.tej_query_lifecycle._execute_phase',phase)
    result=restart_addin(root,task,InterfaceBridge(session),root/'desktop_session.json',
                        allow_restart=True,allow_discard=True,recovery_run=previous)
    assert result['addin_restarted'] and calls==['open']
    proofs=list((root/'query_lifecycle').glob('*/resumed_stopped_run.json'))
    assert len(proofs)==1
    proof=json.loads(proofs[0].read_text())
    assert proof['stop_repeated'] is False and proof['open_previously_submitted'] is False
    assert set(proof['phase_evidence_sha256'])=={'inspect-addin','stop-addin'}


@pytest.mark.parametrize('mutation',['stop_false','foreign_original','open_verified','unknown_action_failure','submission_in_later_resume'])
def test_resume_refuses_unproven_stop_or_any_possible_prior_launch(reset_scope,monkeypatch,mutation):
    root,task,session=reset_scope;previous=stopped_run(root,session);calls=[]
    if mutation=='stop_false':
        p=restart_phase_payload(session,'stop-addin');p['addin_stopped_verified']=False
        atomic_write_json(previous/'stop-addin.json',p)
    elif mutation=='foreign_original':atomic_write_json(previous/'original_session.json',{'another':'session'})
    elif mutation=='open_verified':atomic_write_json(previous/'open.json',restart_phase_payload(session,'open'))
    elif mutation=='unknown_action_failure':
        atomic_write_bytes(root/'diagnostics'/('query_lifecycle-'+previous.name+'-open.txt'),b'Launcher result unknown')
    else:
        later=root/'query_lifecycle'/('b'*32);later.mkdir()
        atomic_write_json(later/'open.json.submission.json',{'old_query_window':session['ExpectedWindow']})
    def phase(*args,**kwargs):calls.append('called');raise AssertionError('No phase may be sent')
    monkeypatch.setattr('downloader.tej_query_lifecycle._execute_phase',phase)
    with pytest.raises(ValueError):
        restart_addin(root,task,InterfaceBridge(session),root/'desktop_session.json',
                      allow_restart=True,allow_discard=True,recovery_run=previous)
    assert not calls and json.loads((root/'desktop_session.json').read_text())==session


def test_launcher_selects_supported_pattern_before_one_action_and_records_intent():
    script=(Path(__file__).resolve().parents[1]/'scripts/reopen_tej_smart_wizard_query.ps1').read_text()
    assert 'GetSupportedPatterns()' in script
    assert script.count('$toggle.Toggle()')==1
    assert '[Windows.Automation.ToggleState]::Off' in script
    assert script.index("($Output+'.submission.json')") < script.index('$toggle.Toggle()')
    assert '$book.Activate()' in script and '$book.Windows.Item(1).Hwnd -ne $ExpectedExcelWindow' in script
    assert '$book.Close(' not in script and '$book.Save(' not in script
