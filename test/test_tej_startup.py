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


def test_pid_and_birth_tick_reuse_after_reboot_is_not_the_old_owner():
    ticks=Path(f'/proc/{os.getpid()}/stat').read_text().rsplit(') ',1)[1].split()[19]
    worker={'owner_pid':os.getpid(),'owner_start_ticks':ticks,
            'linux_boot_id':Path('/proc/sys/kernel/random/boot_id').read_text().strip()}
    assert owner_alive(worker)
    assert not owner_alive({**worker,'linux_boot_id':'00000000-0000-0000-0000-000000000000'})
    with pytest.raises(ValueError):owner_alive({**worker,'linux_boot_id':'unverified-boot'})
    assert owner_alive({k:v for k,v in worker.items() if k!='linux_boot_id'})  # Retained legacy claims.


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


def test_previous_boot_owner_retires_as_unknown_without_waiting_an_obsolete_deadline(abandoned):
    root,task,attempt,prepared,now=abandoned
    worker=json.loads((root/'worker_status.json').read_text())
    worker['linux_boot_id']='00000000-0000-0000-0000-000000000000'
    atomic_write_json(root/'worker_status.json',worker)
    before=prepared.read_bytes()
    assert retire_expired_worker(root,now=now-timedelta(seconds=100))
    saved=current_task(root,task)
    assert saved['state']=='blocked' and saved['last_error_code']=='unknown_outcome_no_auto_retry'
    assert saved['actual_rows'] is None and prepared.read_bytes()==before
    audit=json.loads((root/'orphaned_attempts'/(attempt+'.json')).read_text())
    assert audit['retirement_basis']=='previous_linux_boot_dead_owner'
    assert audit['original_outcome']=='unknown_not_proven_unsent' and audit['task_requeued'] is False
    replacement(root)
    assert orphan_desktop_loss_proof(root,saved,task_request(root,saved),prepared).is_file()
    with closing(connect(root)) as con:
        assert not con.execute('SELECT 1 FROM desktop_replays').fetchone()


def test_current_boot_or_legacy_owner_keeps_the_original_unexpired_deadline(abandoned):
    root,task,attempt,prepared,now=abandoned
    worker=json.loads((root/'worker_status.json').read_text())
    worker['linux_boot_id']=Path('/proc/sys/kernel/random/boot_id').read_text().strip()
    atomic_write_json(root/'worker_status.json',worker)
    assert not retire_expired_worker(root,now=now-timedelta(seconds=100))
    assert current_task(root,task)['state']=='running'


def test_existing_v1_orphan_audit_remains_compatible_without_resetting_unknown(abandoned):
    root,task,attempt,prepared,now=abandoned
    assert retire_expired_worker(root,now=now)
    path=root/'orphaned_attempts'/(attempt+'.json');audit=json.loads(path.read_text())
    audit['contract']='exact_expired_dead_tej_worker_unknown_v1';audit.pop('retirement_basis')
    atomic_write_json(path,audit)
    replacement(root);saved=current_task(root,task)
    assert orphan_desktop_loss_proof(root,saved,task_request(root,saved),prepared)==path
    assert saved['state']=='blocked'


@pytest.mark.parametrize('fault',['basis','same_boot','missing_boot','foreign_owner','unexpired_legacy_basis'])
def test_early_retirement_proof_never_turns_an_unverified_boot_into_replay_permission(abandoned,fault):
    root,task,attempt,prepared,now=abandoned
    worker=json.loads((root/'worker_status.json').read_text())
    worker['linux_boot_id']='00000000-0000-0000-0000-000000000000'
    atomic_write_json(root/'worker_status.json',worker)
    assert retire_expired_worker(root,now=now-timedelta(seconds=100))
    path=root/'orphaned_attempts'/(attempt+'.json');audit=json.loads(path.read_text())
    if fault=='basis':audit['retirement_basis']='unverified_restart'
    elif fault=='same_boot':audit['linux_boot_id']=worker['linux_boot_id']
    elif fault=='missing_boot':audit['worker'].pop('linux_boot_id')
    elif fault=='foreign_owner':audit['worker']['task_id']='foreign'
    else:audit['retirement_basis']='expired_dead_owner'
    atomic_write_json(path,audit)
    replacement(root);saved=current_task(root,task)
    with pytest.raises(ValueError):orphan_desktop_loss_proof(root,saved,task_request(root,saved),prepared)
    assert saved['state']=='blocked'


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


def test_public_startup_distinguishes_live_relay_from_old_interface_check(registry,monkeypatch):
    from stockagent.live.tej_dashboard import build_tej_public_status
    repo,root,_,_=registry
    replacement(root)
    status=json.loads((root/'desktop_startup_status.json').read_text())
    status['observed_at_utc']=(datetime.now(UTC)-timedelta(minutes=10)).isoformat()
    atomic_write_json(root/'desktop_startup_status.json',status)
    atomic_write_json(root/'desktop_transport_session.json',{})
    monkeypatch.setattr('downloader.tej_startup.interactive_transport',lambda _: {'fixture':'live exact relay'})
    public=build_tej_public_status(repo)['desktop_startup']
    assert public['ready'] and not public['recent_interface_check']
    def dead(_):raise ValueError('Dead relay')
    monkeypatch.setattr('downloader.tej_startup.interactive_transport',dead)
    assert not build_tej_public_status(repo)['desktop_startup']['ready']


def test_hardened_gateway_uses_only_fresh_live_source_owner_readiness(registry,monkeypatch):
    import os
    from stockagent.live.tej_dashboard import build_tej_public_status
    repo,root,_,_=registry
    replacement(root)
    monkeypatch.setattr('downloader.tej_startup.desktop_ready',lambda *a,**k:False)
    now=datetime.now(UTC)
    ticks=Path(f'/proc/{os.getpid()}/stat').read_text().rsplit(') ',1)[1].split()[19]
    receipt={'contract':'persistent_serial_evidence_preserving_supervision_v1','continuous':True,
        'state':'executing','owner_pid':os.getpid(),'owner_start_ticks':ticks,
        'observed_at_utc':now.isoformat(),'deadline_at_utc':(now+timedelta(seconds=1800)).isoformat(),
        'desktop_readiness':{'contract':'owned_source_desktop_transport_readiness_v1','ready':True,'observed_at_utc':now.isoformat(),
                             'recent_interface_check':False,'private_socket':'secret-relay'}}
    atomic_write_json(root/'scheduler_status.json',receipt)
    public=build_tej_public_status(repo)
    assert public['desktop_startup']['ready'] and not public['desktop_startup']['recent_interface_check']
    assert 'secret-relay' not in json.dumps(public)
    receipt['desktop_readiness']['observed_at_utc']=(now-timedelta(seconds=61)).isoformat()
    atomic_write_json(root/'scheduler_status.json',receipt)
    assert not build_tej_public_status(repo)['desktop_startup']['ready']
    receipt['desktop_readiness']['observed_at_utc']=now.isoformat();receipt['owner_start_ticks']='foreign-birth'
    atomic_write_json(root/'scheduler_status.json',receipt)
    assert not build_tej_public_status(repo)['desktop_startup']['ready']


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


def surviving_desktop(root):
    """Only the WSL worker died: the original Windows form is still owned."""
    original = json.loads((root/'desktop_session.json').read_text())
    atomic_write_json(root/'desktop_startup_status.json', {
        'contract':CONTRACT, 'state':'desktop_ready',
        'observed_at_utc':datetime.now(UTC).isoformat(),
        'session_sha256':hashlib.sha256((root/'desktop_session.json').read_bytes()).hexdigest()})
    return original


def test_wsl_loss_with_original_idle_desktop_replays_without_destroying_windows(abandoned):
    from downloader.tej_history import configure_runtime_policy
    from downloader.tej_scheduler import replay_authorized_unknown
    from test_tej_authorized_replay import grant_config, FreshReplayBridge, age_original
    root,task,attempt,prepared,now = abandoned
    retire_expired_worker(root,now=now); age_original(root,attempt)
    original = surviving_desktop(root); before = prepared.read_bytes()
    config = grant_config(); configure_runtime_policy(root,config)

    class SurvivingBridge(FreshReplayBridge):
        def execute(self,root,task):
            payload,output,seconds = super().execute(root,task)
            if payload.get('action') == 'inspect_query_runtime':
                payload['current_preview_grids'] = [{'native_shape':[1,6]}]
                payload['owned_desktop_process_start_utc'] = (now-timedelta(days=1)).isoformat()
                atomic_write_json(output,payload)
            return payload,output,seconds

    bridge = SurvivingBridge('fields')  # Loss could precede selecting the requested axes.
    result = replay_authorized_unknown(root,bridge,config)
    assert result['performed'] and result['state'] == 'completed_task'
    assert bridge.calls == ['inspect_query_runtime','download']
    assert json.loads((root/'desktop_session.json').read_text()) == original
    assert prepared.read_bytes() == before
    with closing(connect(root)) as con:
        old = con.execute('SELECT state FROM desktop_attempts WHERE attempt_id=?',(attempt,)).fetchone()[0]
        replay = dict(con.execute('SELECT * FROM desktop_replays').fetchone())
        current = dict(con.execute('SELECT * FROM tasks WHERE task_id=?',(task['task_id'],)).fetchone())
    audit = json.loads((root/replay['audit_path']).read_text())
    assert old == 'unknown_outcome' and current['actual_rows'] == 4 and current['receipt_path']
    assert audit['surviving_desktop_dead_worker_replay_authorized'] is True
    assert audit['orphaned_desktop_loss_replay_authorized'] is False
    assert audit['original_outcome'] == 'unknown_retained_not_claimed_unsent'


@pytest.mark.parametrize('fault',['raw','stage','outcome','stale_desktop','foreign_desktop','live_worker'])
def test_surviving_desktop_loss_requires_exact_dead_owner_and_no_saved_response(abandoned,fault):
    from downloader.tej_startup import surviving_desktop_worker_loss_proof
    root,task,attempt,prepared,now = abandoned
    retire_expired_worker(root,now=now); surviving_desktop(root)
    if fault in ('raw','stage','outcome'):
        suffix = {'raw':'.json','stage':'.json.stage.json','outcome':'.json.outcome.json'}[fault]
        atomic_write_json(root/'raw'/(attempt+suffix),{'must_be_preserved':True})
    elif fault == 'stale_desktop':
        p = root/'desktop_startup_status.json'; d = json.loads(p.read_text())
        d['observed_at_utc'] = (now-timedelta(minutes=4)).isoformat(); atomic_write_json(p,d)
    elif fault == 'foreign_desktop':
        atomic_write_json(root/'desktop_session.json',session(42)); surviving_desktop(root)
    else:
        p = root/'orphaned_attempts'/(attempt+'.json'); d = json.loads(p.read_text())
        d['worker'].update(owner_pid=os.getpid(),owner_start_ticks=
            Path(f'/proc/{os.getpid()}/stat').read_text().rsplit(') ',1)[1].split()[19])
        atomic_write_json(p,d)
    current = current_task(root,task)
    with pytest.raises(ValueError):
        surviving_desktop_worker_loss_proof(root,current,task_request(root,current),prepared)
    assert current_task(root,task)['state'] == 'blocked'


@pytest.mark.parametrize('grids',[None,{},[{'native_shape':[2,6]}],[{'native_shape':[-1,6]}],
    [{'native_shape':[True,6]}],[{'native_shape':[1]}],[{}]])
def test_surviving_desktop_never_overwrites_unbound_or_unverified_preview(abandoned,grids):
    from downloader.tej_history import configure_runtime_policy
    from downloader.tej_scheduler import replay_authorized_unknown
    from test_tej_authorized_replay import grant_config, FreshReplayBridge, age_original
    root,task,attempt,prepared,now = abandoned
    retire_expired_worker(root,now=now); surviving_desktop(root); age_original(root,attempt)
    config = grant_config(); configure_runtime_policy(root,config)

    class UnboundPreview(FreshReplayBridge):
        def execute(self,root,task):
            payload,output,seconds = super().execute(root,task)
            payload['current_preview_grids'] = grids
            payload['owned_desktop_process_start_utc'] = (now-timedelta(days=1)).isoformat()
            atomic_write_json(output,payload)
            return payload,output,seconds

    bridge = UnboundPreview(); result = replay_authorized_unknown(root,bridge,config)
    assert not result['performed'] and bridge.calls == ['inspect_query_runtime']
    assert result['failure_phase'] == 'fresh_runtime_readback_and_replay'
    diagnostic = root/result['failure_diagnostic_path']
    assert diagnostic.stat().st_mode & 0o777 == 0o600
    assert 'Preview requires reconciliation' in json.loads(diagnostic.read_text())['private_diagnostic']
    assert current_task(root,task)['state'] == 'blocked'
    with closing(connect(root)) as con:
        assert not con.execute('SELECT 1 FROM desktop_replays').fetchone()


@pytest.mark.parametrize('clock',[None,'invalid','2026-01-01T00:00:00','2999-01-01T00:00:00Z'])
def test_surviving_desktop_rejects_reused_pid_or_unverified_process_birth(abandoned,clock):
    from downloader.tej_history import configure_runtime_policy
    from downloader.tej_scheduler import replay_authorized_unknown
    from test_tej_authorized_replay import grant_config, FreshReplayBridge, age_original
    root,task,attempt,prepared,now = abandoned
    retire_expired_worker(root,now=now); surviving_desktop(root); age_original(root,attempt)
    config = grant_config(); configure_runtime_policy(root,config)

    class ReusedDesktop(FreshReplayBridge):
        def execute(self,root,task):
            payload,output,seconds = super().execute(root,task)
            payload['current_preview_grids'] = []
            payload['owned_desktop_process_start_utc'] = clock
            atomic_write_json(output,payload)
            return payload,output,seconds

    bridge = ReusedDesktop(); result = replay_authorized_unknown(root,bridge,config)
    assert not result['performed'] and bridge.calls == ['inspect_query_runtime']
    assert current_task(root,task)['state'] == 'blocked'


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


def test_owned_windows_bootstrap_is_not_a_query_or_unbounded_launcher_retry():
    script=(Path(__file__).resolve().parents[1]/'scripts/start_tej_desktop.ps1').read_text()
    for forbidden in ('SetForegroundWindow','SendWait(','Stop-Process','taskkill','Preview','ActiveWorkbook','Workbooks.Close'):
        assert forbidden not in script
    for proof in ("state='prepared'", "$previous.state -ceq 'confirmed'", 'Same-Session $previous.new_session $old',
                  'retired_window_absent', 'retired_process_absent', 'AddHours(-1)', '-ge 3', '.retained.json'):
        assert proof in script


def test_prepared_desktop_launcher_recovery_requires_fresh_absence_and_keeps_unknown():
    script=(Path(__file__).resolve().parents[1]/'scripts/start_tej_desktop.ps1').read_text()
    policy=script.split('function Launcher-RetryBasis',1)[1].split('try {',1)[0]
    for proof in ('previous.contract', 'previous.windows_boot_at_utc', 'previous.workbook',
                  'retired_window_absent', 'retired_process_absent', "-cne 'prepared'",
                  '-not $addinAbsent', '-not $excelReady', 'TotalSeconds -lt 60'):
        assert proof in policy
    assert 'original_unknown_intent_preserved=($null -ne $retryBasis)' in script
    assert 'Get-Process -Name TEJAddin' in script
    assert '$history.Count -gt 64' in script and '-ge 3' in script


def test_windows_startup_installation_requires_readback_and_interactive_logon():
    script=(Path(__file__).resolve().parents[1]/'scripts/install_windows_tej_startup.ps1').read_text()
    assert script.index('Get-ScheduledTask -TaskName \'StockAgent TEJ Desktop Recovery\'') < script.index('interactive_task_registered=$true')
    assert script.index('Get-ScheduledTask -TaskName \'StockAgent TEJ WSL Startup\'') < script.index('$bootRegistered=$true')
    for proof in ('-LogonType Interactive', '-AtLogOn', '-MultipleInstances IgnoreNew', '-ErrorAction Stop',
                  'administrator_permission_required', 'windows_autologin_changed=$false'):
        assert proof in script
