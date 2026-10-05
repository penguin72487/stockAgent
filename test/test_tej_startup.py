"""Boot recovery preserves unknown queries, source scope and desktop ownership."""
from contextlib import closing
from datetime import UTC, datetime, timedelta
import hashlib
import json
import os
from pathlib import Path
import uuid

import pytest

from downloader.artifact_io import atomic_write_json
from downloader.tej_desktop_attempts import begin_attempt
from downloader.tej_history import connect, run_one, task_request
from downloader.tej_startup import (CONTRACT, desktop_ready, owner_alive,
    orphan_desktop_loss_proof, retire_expired_worker, valid_session)
from test_tej_history import FakeBridge, registry


def session(n=1):
    return dict(TejProcessId=n, ExpectedWindow=n+1, ExpectedExcelWindow=n+2,
        ExpectedWorkbook='Book2', ExpectedTitle='TEJ Smart Wizard (Version 4.1.1.7) -- Book2')


@pytest.fixture
def abandoned(registry):
    _, root, _, _ = registry
    assert run_one(root, FakeBridge()) == 'completed_task'
    now = datetime.now(UTC); start = now-timedelta(seconds=902)
    with closing(connect(root)) as con, con:
        task = dict(con.execute("SELECT * FROM tasks WHERE kind='download' AND state='pending' LIMIT 1").fetchone())
        con.execute("UPDATE tasks SET state='running',attempted_at_utc=? WHERE task_id=?", (start.isoformat(), task['task_id']))
        con.execute("INSERT INTO traffic VALUES('orphan-test','download',?,NULL,'running')", (start.isoformat(),))
    task['attempted_at_utc'] = start.isoformat()
    attempt = task['task_id']+'-'+uuid.uuid4().hex
    prepared = root/'requests'/(attempt+'.json')
    req = task_request(root, task)
    atomic_write_json(prepared, {**req, 'query_attempt_id': attempt, 'task_id': task['task_id']})
    begin_attempt(root, task, attempt, prepared, root/'raw'/(attempt+'.json'))
    worker = dict(state='running', task_id=task['task_id'], bridge_attempt_id=attempt,
        owner_pid=99999999, owner_start_ticks='0', observed_at_utc=start.isoformat(),
        deadline_at_utc=(start+timedelta(seconds=900)).isoformat())
    atomic_write_json(root/'worker_status.json', worker)
    atomic_write_json(root/'desktop_session.json', session())
    return root, task, attempt, prepared, now


def replacement(root):
    new = session(11); path = root/'desktop_startup'/'test.json'
    atomic_write_json(path, dict(contract=CONTRACT, retired_session=session(), new_session=new,
        retired_window_absent=True, retired_process_absent=True, provider_queries_sent=0))
    atomic_write_json(root/'desktop_session.json', new)
    status = dict(contract=CONTRACT, state='desktop_ready', observed_at_utc=datetime.now(UTC).isoformat(),
        session_sha256=hashlib.sha256((root/'desktop_session.json').read_bytes()).hexdigest(),
        private_evidence_path=str(path.relative_to(root)), private_evidence_sha256=hashlib.sha256(path.read_bytes()).hexdigest())
    atomic_write_json(root/'desktop_startup_status.json', status)
    return path


def current_task(root, task):
    with closing(connect(root)) as con:
        return dict(con.execute('SELECT * FROM tasks WHERE task_id=?', (task['task_id'],)).fetchone())


def test_expired_dead_worker_is_unknown_not_pending_and_original_request_is_retained(abandoned):
    root, task, attempt, prepared, now = abandoned; before=prepared.read_bytes()
    assert retire_expired_worker(root, now=now)
    current=current_task(root, task)
    assert current['state']=='blocked' and current['last_error_code']=='unknown_outcome_no_auto_retry'
    assert current['active_attempt_id']==attempt and current['actual_rows'] is None
    with closing(connect(root)) as con:
        assert con.execute('SELECT state FROM desktop_attempts WHERE attempt_id=?', (attempt,)).fetchone()[0]=='unknown_outcome'
        assert con.execute('SELECT count(*) FROM desktop_replays').fetchone()[0]==0
    assert prepared.read_bytes()==before
    assert not retire_expired_worker(root, now=now)


@pytest.mark.parametrize('gate', ['live_owner','future_deadline','foreign_attempt','changed_scope','saved_result','prequery_proof','missing_pid','pid_reuse'])
def test_missing_data_or_reboot_alone_never_requeues_a_query(abandoned, gate):
    root, task, attempt, prepared, now=abandoned
    worker=json.loads((root/'worker_status.json').read_text())
    if gate in ('live_owner','pid_reuse'):
        worker['owner_pid']=os.getpid()
        worker['owner_start_ticks']=Path(f'/proc/{os.getpid()}/stat').read_text().rsplit(') ',1)[1].split()[19]
        if gate=='pid_reuse':worker['owner_start_ticks']='0'
    elif gate=='future_deadline':now=now-timedelta(seconds=100)
    elif gate=='foreign_attempt':worker['bridge_attempt_id']='foreign'
    elif gate=='changed_scope':
        req=json.loads(prepared.read_text());req['table']='other';atomic_write_json(prepared,req)
    elif gate=='saved_result':atomic_write_json(root/'raw'/(attempt+'.json'), {'unverified': True})
    elif gate=='prequery_proof':atomic_write_json(root/'raw'/(attempt+'.json.outcome.json'), {'unverified': True})
    elif gate=='missing_pid':worker.pop('owner_pid')
    atomic_write_json(root/'worker_status.json',worker)
    if gate in ('changed_scope','missing_pid'):
        with pytest.raises(ValueError):retire_expired_worker(root,now=now)
    else:
        assert retire_expired_worker(root,now=now) is (gate=='pid_reuse')
    assert current_task(root,task)['state']==('blocked' if gate=='pid_reuse' else 'running')


def test_only_exact_lost_desktop_authorizes_orphan_mode_not_unknown_unsent(abandoned):
    root,task,attempt,prepared,now=abandoned
    assert retire_expired_worker(root,now=now)
    replacement(root);current=current_task(root,task)
    proof=orphan_desktop_loss_proof(root,current,task_request(root,current),prepared)
    assert proof.name==attempt+'.json'
    assert json.loads(proof.read_text())['original_outcome']=='unknown_not_proven_unsent'


@pytest.mark.parametrize('gate', ['old_window_alive','old_process_alive','other_session','changed_request','late_response','proof_tamper'])
def test_foreign_or_live_desktop_and_late_source_result_prevent_orphan_replay(abandoned, gate):
    root,task,attempt,prepared,now=abandoned;retire_expired_worker(root,now=now)
    path=replacement(root)
    if gate=='changed_request':
        value=json.loads(prepared.read_text());value['table']='other';atomic_write_json(prepared,value)
    elif gate=='late_response':atomic_write_json(root/'raw'/(attempt+'.json'),{'late':True})
    else:
        proof=json.loads(path.read_text())
        if gate=='old_window_alive':proof['retired_window_absent']=False
        elif gate=='old_process_alive':proof['retired_process_absent']=False
        elif gate=='other_session':proof['retired_session']=session(21)
        else:proof['provider_queries_sent']=9
        atomic_write_json(path,proof)
        if gate!='proof_tamper':
            status=json.loads((root/'desktop_startup_status.json').read_text());status['private_evidence_sha256']=hashlib.sha256(path.read_bytes()).hexdigest()
            atomic_write_json(root/'desktop_startup_status.json',status)
    current=current_task(root,task)
    with pytest.raises(ValueError):orphan_desktop_loss_proof(root,current,task_request(root,current),prepared)


def test_ready_status_is_fresh_and_tied_to_exact_session(tmp_path):
    replacement(tmp_path)
    assert desktop_ready(tmp_path)
    assert not desktop_ready(tmp_path,now=datetime.now(UTC)+timedelta(seconds=181))
    assert desktop_ready(tmp_path,now=datetime.now(UTC)+timedelta(seconds=181),require_recent=False)
    atomic_write_json(tmp_path/'desktop_session.json',session(25))
    assert not desktop_ready(tmp_path)


def test_session_validation_rejects_pid_boolean_extra_scope_and_title_drift():
    assert valid_session(session())
    assert not valid_session({**session(), 'TejProcessId': True})
    assert not valid_session({**session(), 'token': 'not allowed'})
    assert not valid_session({**session(), 'ExpectedTitle': 'other'})
    assert valid_session({**session(), 'ExpectedTitle':
        'TEJ Smart Wizard (Version 4.1.1.7) -- C:\\research\\Book2'})
    assert not valid_session({**session(), 'ExpectedTitle':
        'TEJ Smart Wizard (Version 4.1.1.7) -- C:\\research\\Other'})


def test_startup_public_status_never_exposes_desktop_identity_or_vendor_diagnostics(registry):
    from stockagent.live.tej_dashboard import build_tej_public_status
    repo,root,_,_=registry
    replacement(root)
    private=json.loads((root/'desktop_startup_status.json').read_text())
    private.update(interop_socket='private-socket',private_error='secret-vendor-error',windows_username='secret-user')
    atomic_write_json(root/'desktop_startup_status.json',private)
    atomic_write_json(repo/'configs/tej_history.json',{'automation': {'desktop_startup': {'enabled':True,'contract':CONTRACT}}})
    status=build_tej_public_status(repo)
    assert status['desktop_startup']['enabled'] and status['desktop_startup']['ready']
    text=json.dumps(status)
    for forbidden in ('private-socket','secret-vendor-error','secret-user','Book2','private_evidence_path','ExpectedWindow'):
        assert forbidden not in text


def test_lost_boot_worker_uses_existing_bounded_replay_and_new_source_receipt(abandoned):
    from downloader.tej_history import configure_runtime_policy
    from downloader.tej_scheduler import replay_authorized_unknown
    from test_tej_authorized_replay import grant_config, FreshReplayBridge, age_original
    root,task,attempt,prepared,now=abandoned
    before=prepared.read_bytes()
    retire_expired_worker(root,now=now);replacement(root);age_original(root,attempt)
    config=grant_config();configure_runtime_policy(root,config)
    bridge=FreshReplayBridge()
    result=replay_authorized_unknown(root,bridge,config)
    assert result['performed'] and result['state']=='completed_task'
    assert bridge.calls==['inspect_query_runtime','download']
    with closing(connect(root)) as con:
        assert con.execute('SELECT state FROM desktop_attempts WHERE attempt_id=?',(attempt,)).fetchone()[0]=='unknown_outcome'
        replay=dict(con.execute('SELECT * FROM desktop_replays').fetchone())
        current=dict(con.execute('SELECT * FROM tasks WHERE task_id=?',(task['task_id'],)).fetchone())
    audit=json.loads((root/replay['audit_path']).read_text())
    assert audit['orphaned_desktop_loss_replay_authorized'] is True
    assert audit['possible_additional_provider_usage'] is True
    assert current['state']=='complete' and current['actual_rows']==4 and current['receipt_path']
    assert prepared.read_bytes()==before


def test_boot_policy_is_exact_and_does_not_accept_arbitrary_user_workbooks():
    from downloader.tej_startup import startup_policy
    policy=dict(enabled=True,contract=CONTRACT,
        authorization_basis='explicit_user_request_automatic_tej_restart_after_boot',
        owned_workbook_windows='%LOCALAPPDATA%\\StockAgent\\TEJSmartWizard\\StockAgent-TEJ-Acquisition.xlsx')
    assert startup_policy({'automation':{'desktop_startup':policy}})==policy
    for changed in ({'enabled':1},{'owned_workbook_windows':'C:\\user\\important.xlsx'},
                    {'authorization_basis':'inferred'},{'contract':'wrong'}):
        with pytest.raises(ValueError):startup_policy({'automation':{'desktop_startup':{**policy,**changed}}})


def test_interactive_relay_is_owned_by_alive_process_and_this_boot(tmp_path, monkeypatch):
    from downloader.tej_startup import interactive_transport
    ticks=Path(f'/proc/{os.getpid()}/stat').read_text().rsplit(') ',1)[1].split()[19]
    body=dict(contract='persistent_owned_interactive_tej_relay_v1',owner_pid=os.getpid(),owner_start_ticks=ticks,
        linux_boot_id=Path('/proc/sys/kernel/random/boot_id').read_text().strip(),windows_session_id=1,
        interop_socket='/run/WSL/1_interop')
    monkeypatch.setattr(Path,'is_socket',lambda self:str(self)=='/run/WSL/1_interop')
    atomic_write_json(tmp_path/'desktop_transport_session.json',body)
    assert interactive_transport(tmp_path)==body
    for changed in ({'owner_start_ticks':'0'},{'linux_boot_id':'old'},{'windows_session_id':0},
                    {'interop_socket':'/tmp/unowned_socket'},{'windows_session_id':True}, {'state':'draining'}):
        atomic_write_json(tmp_path/'desktop_transport_session.json',{**body,**changed})
        with pytest.raises(ValueError):interactive_transport(tmp_path)


def test_graceful_relay_exit_blocks_new_queries_before_draining_current_writer(tmp_path, monkeypatch):
    from contextlib import contextmanager
    from downloader.tej_startup import drain_interactive_transport, interactive_transport
    from downloader import dataset_lock
    ticks=Path(f'/proc/{os.getpid()}/stat').read_text().rsplit(') ',1)[1].split()[19]
    body=dict(contract='persistent_owned_interactive_tej_relay_v1',owner_pid=os.getpid(),owner_start_ticks=ticks,
        linux_boot_id=Path('/proc/sys/kernel/random/boot_id').read_text().strip(),windows_session_id=1,
        interop_socket='/run/WSL/1_interop',state='active')
    monkeypatch.setattr(Path,'is_socket',lambda self:str(self)=='/run/WSL/1_interop')
    atomic_write_json(tmp_path/'desktop_transport_session.json',body)
    calls=[]
    @contextmanager
    def drain(path, **kwargs):
        assert path==tmp_path/'.download.lock' and kwargs['timeout_seconds']==1900
        with pytest.raises(ValueError):interactive_transport(tmp_path)
        calls.append('drained_canonical_writer');yield
    monkeypatch.setattr(dataset_lock,'exclusive_dataset_lock',drain)
    assert drain_interactive_transport(tmp_path)['state']=='interactive_relay_drained'
    assert calls==['drained_canonical_writer']
    assert json.loads((tmp_path/'desktop_transport_session.json').read_text())['state']=='draining'


def test_relay_shutdown_never_marks_a_foreign_or_reused_owner_draining(tmp_path):
    from downloader.tej_startup import drain_interactive_transport
    path=tmp_path/'desktop_transport_session.json'
    for pid,ticks in ((99999999,'0'),(os.getpid(),'0')):
        atomic_write_json(path,dict(contract='persistent_owned_interactive_tej_relay_v1',owner_pid=pid,owner_start_ticks=ticks))
        original=path.read_bytes()
        assert drain_interactive_transport(tmp_path)['state']=='other_interactive_owner_preserved'
        assert path.read_bytes()==original


def test_owned_windows_bootstrap_is_not_a_query_or_unknown_launch_retry():
    script=(Path(__file__).resolve().parents[1]/'scripts/start_tej_desktop.ps1').read_text()
    for forbidden in ('SetForegroundWindow','SendWait(','Stop-Process','taskkill','Preview','ActiveWorkbook','Workbooks.Close'):
        assert forbidden not in script
    for proof in ("state='prepared'", "-cne 'confirmed'", 'Same-Session $previous.new_session $old',
                  'retired_window_absent', 'retired_process_absent', 'AddHours(-1)', '-ge 3', '.retained.json'):
        assert proof in script


def test_windows_startup_installation_requires_readback_and_interactive_logon():
    script=(Path(__file__).resolve().parents[1]/'scripts/install_windows_tej_startup.ps1').read_text()
    assert script.index('Get-ScheduledTask -TaskName \'StockAgent TEJ Desktop Recovery\'') < script.index('interactive_task_registered=$true')
    assert script.index('Get-ScheduledTask -TaskName \'StockAgent TEJ WSL Startup\'') < script.index('$bootRegistered=$true')
    for proof in ('-LogonType Interactive', '-AtLogOn', '-MultipleInstances IgnoreNew', '-ErrorAction Stop',
                  'administrator_permission_required', 'windows_autologin_changed=$false'):
        assert proof in script
