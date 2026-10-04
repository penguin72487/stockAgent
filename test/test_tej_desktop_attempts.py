"""Attempt identity, stale result rejection and explicit one-shot replay gates."""
from contextlib import closing
import json
from pathlib import Path
import uuid

import pytest

from downloader.artifact_io import atomic_write_json
from downloader.tej_desktop_attempts import begin_attempt, query_stage, retry_unknown_download, validate_active_attempt, prepared_request_matches
from downloader.tej_history import DESKTOP_INPUT_CONTRACT, PREVIEW_SUBMISSION_CONTRACT, DesktopBridge, connect, recover_desktop_response, run_one, task_request
from test_tej_history import FakeBridge, full_grid_export, registry, scope_proof


def _unlaunched_claim(registry):
    from datetime import UTC, datetime, timedelta
    repo, root, _, _ = registry
    assert run_one(root, FakeBridge()) == 'completed_task'
    started = datetime.now(UTC) - timedelta(seconds=1)
    with closing(connect(root)) as con, con:
        task = dict(con.execute("SELECT * FROM tasks WHERE kind='download' AND state='pending' LIMIT 1").fetchone())
        con.execute("UPDATE tasks SET state='running',attempted_at_utc=? WHERE task_id=?", (started.isoformat(), task['task_id']))
        con.execute("INSERT INTO traffic VALUES('unlaunched','download',?,NULL,'running')", (started.isoformat(),))
    task['attempted_at_utc'] = started.isoformat()
    attempt = task['task_id'] + '-' + uuid.uuid4().hex
    request = root / 'requests' / (attempt + '.json')
    atomic_write_json(request, {**task_request(root, task), 'task_id': task['task_id'],
                               'query_attempt_id': attempt, 'desktop_input_contract': DESKTOP_INPUT_CONTRACT})
    atomic_write_json(root / 'worker_status.json', {'state': 'waiting_metadata',
                      'observed_at_utc': datetime.now(UTC).isoformat()})
    return root, task, attempt, request


def test_exact_failed_attempt_registration_is_recovered_without_source_retry(registry):
    from downloader.tej_desktop_attempts import recover_unlaunched_metadata_claim
    root, task, attempt, request = _unlaunched_claim(registry)
    assert recover_unlaunched_metadata_claim(root, task['task_id'], request)
    with closing(connect(root)) as con:
        row = con.execute('SELECT state,active_attempt_id FROM tasks WHERE task_id=?', (task['task_id'],)).fetchone()
        assert tuple(row) == ('pending', None)
        assert con.execute("SELECT state FROM traffic WHERE event_id='unlaunched'").fetchone()[0] == 'failed_before_desktop_launch'
        assert con.execute('SELECT count(*) FROM desktop_attempts').fetchone()[0] == 0
    audit = json.loads((root / 'operator_replays' / (attempt + '-unlaunched-metadata.json')).read_text())
    assert audit['provider_queries_sent'] == 0 and audit['unknown_source_actions_retried'] == 0
    assert not recover_unlaunched_metadata_claim(root, task['task_id'], request)


@pytest.mark.parametrize('artifact', ['stage', 'response', 'progress', 'registered_attempt',
                                     'foreign_request', 'wrong_source', 'bare_null', 'stale_clock', 'running_bridge'])
def test_missing_attempt_alone_never_licenses_unresolved_source_replay(registry, artifact):
    from downloader.tej_desktop_attempts import recover_unlaunched_metadata_claim
    root, task, attempt, request = _unlaunched_claim(registry)
    if artifact in {'stage', 'response', 'progress'}:
        path = root / ('progress' if artifact == 'progress' else 'raw') / (attempt + ('.json.stage.json' if artifact == 'stage' else '.json'))
        atomic_write_json(path, {'market_data_query_submission_possible': True})
    elif artifact == 'registered_attempt':
        begin_attempt(root, task, attempt, request, root / 'raw' / (attempt + '.json'))
    elif artifact == 'foreign_request':
        body = json.loads(request.read_text()); body['query_attempt_id'] = 'other'
        atomic_write_json(request, body)
    elif artifact == 'wrong_source':
        body = json.loads(request.read_text()); body['table'] = 'Other'
        atomic_write_json(request, body)
    elif artifact == 'bare_null':
        request.unlink()
    elif artifact == 'stale_clock':
        atomic_write_json(root / 'worker_status.json', {'state': 'waiting_metadata', 'observed_at_utc': '2000-01-01T00:00:00+00:00'})
    else:
        atomic_write_json(root / 'worker_status.json', {'state': 'running'})
    try:
        assert not recover_unlaunched_metadata_claim(root, task['task_id'], request)
    except ValueError:
        pass
    with closing(connect(root)) as con:
        assert con.execute('SELECT state FROM tasks WHERE task_id=?', (task['task_id'],)).fetchone()[0] == 'running'


def test_supervisor_adopts_exact_local_failure_but_not_bare_unresolved_claim(registry):
    from downloader.tej_scheduler import recover_complete_local_response
    root, task, _, request = _unlaunched_claim(registry)
    assert recover_complete_local_response(root)
    assert not recover_complete_local_response(root)


def test_metadata_failure_before_attempt_commit_never_launches_powershell(registry, monkeypatch):
    import sqlite3
    import downloader.tej_desktop_attempts as attempts
    root, task, _, _ = _unlaunched_claim(registry)
    monkeypatch.setattr(DesktopBridge, 'windows_path', staticmethod(str))
    def busy(*args):
        raise sqlite3.OperationalError('database is locked')
    def forbidden(*args, **kwargs):
        pytest.fail('PowerShell must not launch before the desktop attempt is committed')
    monkeypatch.setattr(attempts, 'begin_attempt', busy)
    monkeypatch.setattr('downloader.tej_windows_transport.run_guarded_windows', forbidden)
    bridge = DesktopBridge(root.parent, {'TejProcessId': 1, 'ExpectedWindow': 2, 'ExpectedTitle': 'x',
                                      'ExpectedWorkbook': 'y', 'ExpectedExcelWindow': 3})
    with pytest.raises(sqlite3.OperationalError):
        bridge.execute(root, task)


@pytest.mark.parametrize('progress_failure', [False, True])
def test_canonical_desktop_attempt_publishes_live_readback_then_only_committed_totals(registry, monkeypatch, progress_failure):
    from datetime import UTC, datetime
    from types import SimpleNamespace
    from stockagent.live.tej_dashboard import build_tej_public_status
    repo, root, _, _ = registry
    run_one(root, FakeBridge())
    monkeypatch.setattr(DesktopBridge, 'windows_path', staticmethod(lambda path: str(path)))
    calls = []

    def desktop_call(*args, **kwargs):
        calls.append(args)
        with closing(connect(root)) as con:
            task = dict(con.execute("SELECT * FROM tasks WHERE kind='download' AND state='running'").fetchone())
        attempt = task['active_attempt_id']
        req = json.loads((root/'requests'/(attempt+'.json')).read_text())
        output = root/'raw'/(attempt+'.json')
        progress_path = root/'progress'/(attempt+'.json.progress.json')
        worker = json.loads((root/'worker_status.json').read_text())
        assert worker['bridge_attempt_id'] == attempt
        progress = {'contract':'tej_native_readback_progress_v1', 'task_id':task['task_id'],
                    'attempt_id':attempt, 'stage':'reading_preview',
                    'observed_at_utc':datetime.now(UTC).isoformat(), 'scanned_row_slots':2, 'total_row_slots':4}
        atomic_write_json(progress_path, progress)
        current = build_tej_public_status(repo)
        assert current['activity']['current_task']['readback_ratio'] == .5
        assert current['activity']['completed_download_tasks'] == current['workload']['exported_rows'] == 0
        assert json.loads((root/'worker_status.json').read_text())['deadline_at_utc'] == worker['deadline_at_utc']
        stage = {**{k:req[k] for k in ('type','smart_id','table','fields','company_labels','date_labels')},
                 'contract_version':4, 'task_id':task['task_id'], 'stage':'prepreview_verified',
                 'source_scope_proof':scope_proof(), 'query_attempt_id':attempt,
                 'preview_submission_contract':PREVIEW_SUBMISSION_CONTRACT}
        atomic_write_json(output.with_suffix('.json.stage.json'), stage)
        payload = {**full_grid_export(), 'task_id':task['task_id'], 'query_attempt_id':attempt,
                   'preview_submission_contract':PREVIEW_SUBMISSION_CONTRACT,
                   'fresh_preview_transition_verified':True}
        atomic_write_json(output, payload)
        if progress_failure: atomic_write_json(progress_path, ['invalid display telemetry'])
        return SimpleNamespace(returncode=0, stderr=b'', stdout=b'')

    monkeypatch.setattr('downloader.tej_windows_transport.run_guarded_windows', desktop_call)
    bridge = DesktopBridge(repo, dict(TejProcessId=1, ExpectedWindow=2, ExpectedTitle='fixture',
                                     ExpectedWorkbook='fixture', ExpectedExcelWindow=3))
    assert run_one(root, bridge) == 'completed_task'
    assert len(calls) == 1
    result = build_tej_public_status(repo)
    assert result['activity']['current_task'] is None
    assert result['activity']['completed_download_tasks'] == 1
    assert result['activity']['last_successful_download']['actual_rows'] == result['workload']['exported_rows'] == 4
    assert result['workload']['exported_non_null_cells'] == 6


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
                     'source_key_mode':2,
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
            if task.get('active_attempt_id'):
                task, attempt, _ = _activate(root, task, req)
                payload.update(query_attempt_id=attempt, fresh_preview_transition_verified=True,
                               preview_submission_contract=PREVIEW_SUBMISSION_CONTRACT)
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


@pytest.fixture
def unstaged_interop(unknown):
    from downloader.tej_desktop_attempts import finish_attempt
    root, task, req, old_prepared, _ = unknown
    old_stage = root/'raw'/(old_prepared.name+'.stage.json')
    old_stage.unlink()  # Isolated fixture, not real acquisition evidence.
    task, attempt, prepared = _activate(root, task, req)
    (root/'raw'/(attempt+'.json.stage.json')).unlink()
    finish_attempt(root, attempt, 'unknown_outcome')
    diagnostic = root/'diagnostics'/(attempt+'.txt')
    diagnostic.parent.mkdir(exist_ok=True)
    diagnostic.write_text('<3>WSL (1234 - ) ERROR: UtilAcceptVsock:273: accept4 failed 110\n')
    return root, task, req, prepared, diagnostic


def test_unstaged_interop_requires_explicit_operator_mode(unstaged_interop):
    root, task, _, prepared, _ = unstaged_interop
    bridge = ReplayBridge()
    with pytest.raises((ValueError, OSError)):
        retry_unknown_download(root, task['task_id'], bridge, prepared)
    assert bridge.calls == []
    assert run_one(root, bridge) == 'inflight_requires_recovery'


def test_authorized_interop_replay_keeps_unknown_request_and_diagnostic(unstaged_interop):
    root, task, _, prepared, diagnostic = unstaged_interop
    original = prepared.read_bytes(), diagnostic.read_bytes()
    bridge = ReplayBridge('fields')  # The idle UI may be on the previous task.
    result = retry_unknown_download(root, task['task_id'], bridge, prepared, allow_unstaged_interop=True)
    assert result['state'] == 'completed_task'
    assert bridge.calls == ['inspect_query_runtime', 'download']
    assert original == (prepared.read_bytes(), diagnostic.read_bytes())
    audit = json.loads(next((root/'operator_replays').glob('*.json')).read_text())
    assert audit['unstaged_interop_replay_explicitly_authorized'] is True
    assert audit['original_stage_path'] is None
    assert audit['original_launch_diagnostic_sha256']
    assert audit['original_outcome'] == 'unknown_retained_not_claimed_unsent'
    with closing(connect(root)) as con:
        assert con.execute('SELECT state FROM desktop_attempts WHERE attempt_id=?',
                           (task['active_attempt_id'],)).fetchone()[0] == 'unknown_outcome'


@pytest.mark.parametrize('mutation', ['diagnostic', 'stage', 'raw', 'progress', 'attempt_state', 'scope'])
def test_unstaged_replay_refuses_unreviewed_or_conflicting_evidence(unstaged_interop, mutation):
    from downloader.tej_desktop_attempts import finish_attempt
    root, task, _, prepared, diagnostic = unstaged_interop
    attempt = task['active_attempt_id']
    if mutation == 'diagnostic':
        diagnostic.write_text('a different failure, not permission to replay')
    elif mutation == 'attempt_state':
        finish_attempt(root, attempt, 'prepared')
    elif mutation == 'scope':
        original = json.loads(prepared.read_text()); original['end'] = '1900-01-01'
        atomic_write_json(prepared, original)
    else:
        path = {'stage':root/'raw'/(attempt+'.json.stage.json'),
                'raw':root/'raw'/(attempt+'.json'),
                'progress':root/'progress'/(attempt+'.json.progress.json')}[mutation]
        atomic_write_json(path, {'query_may_have_been_sent':True})
    bridge = ReplayBridge()
    with pytest.raises(ValueError):
        retry_unknown_download(root, task['task_id'], bridge, prepared, allow_unstaged_interop=True)
    assert bridge.calls == []


def test_authorized_unstaged_retry_cannot_become_automatic_repeat(unstaged_interop):
    root, task, _, prepared, _ = unstaged_interop
    bridge = ReplayBridge(fail=True)
    result = retry_unknown_download(root, task['task_id'], bridge, prepared, allow_unstaged_interop=True)
    assert result['state'] == 'unknown_outcome_no_auto_retry'
    assert run_one(root, bridge) == 'inflight_requires_recovery'
    assert bridge.calls == ['inspect_query_runtime', 'download']


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
    assert prepared_request_matches({**original,'desktop_input_contract':
        'native_acknowledged_date_model_commit_blank_mask_no_mouse_v5'},request,task)
    assert not prepared_request_matches({**original,'table':'Different'},request,task)
    assert not prepared_request_matches({**original,'unreviewed_execution_flag':True},request,task)
    assert not prepared_request_matches({**original,'desktop_input_contract':'unknown'},request,task)
    assert not prepared_request_matches({**original,'query_attempt_id':'other'},request,task)
    request['action']='download';task['active_attempt_id']='task-current'
    assert prepared_request_matches({**request,'task_id':'task','query_attempt_id':'task-current',
                                     'desktop_input_contract':DESKTOP_INPUT_CONTRACT},request,task)
