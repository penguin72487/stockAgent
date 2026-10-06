"""Admission failures can recover; admitted/unknown source actions cannot repeat."""
import json
from pathlib import Path
import subprocess

import pytest

from downloader.artifact_io import atomic_write_json
from downloader import tej_windows_transport as transport
from test_tej_history import registry


@pytest.fixture
def launch_request(tmp_path):
    path = tmp_path/'data_tej'/'requests'/('a'*24+'-'+'b'*32+'.json')
    atomic_write_json(path, {'task_id':'a'*24, 'action':'download'})
    return path


class Process:
    def __init__(self, command, *, acknowledgement=True, failure=None, timeout=False):
        self.returncode = None
        self.terminated = False
        self.command = command
        self.timeout = timeout
        self.failure = failure
        # Use the actual wrapper paths/token, never an invented controller ACK.
        assignments = {}
        for name in ('ready','permit','token'):
            assignments[name] = command.split('$'+name+"='",1)[1].split("'",1)[0]
        self.permit = Path(assignments['permit'])
        if acknowledgement:
            ack = command.split('[IO.File]::WriteAllText($tmp,\'',1)[1].split("'",1)[0]
            body = json.loads(ack)
            if failure == 'wrong_digest': body['request_sha256'] = '0'*64
            if failure == 'wrong_token': body['token'] = '0'*32
            atomic_write_json(Path(assignments['ready']),body)
        if not acknowledgement or failure: self.returncode = 75

    def poll(self): return self.returncode
    def terminate(self): self.terminated = True; self.returncode = -15
    def kill(self): self.terminated = True; self.returncode = -9

    def communicate(self, timeout):
        if not self.terminated and self.timeout:
            raise subprocess.TimeoutExpired('private bridge command', timeout, stderr=b'private')
        if not self.terminated and not self.failure and self.returncode is None:
            assert self.permit.is_file(), 'A bridge must never run without the exact durable permit'
        self.returncode = self.returncode if self.returncode is not None else 0
        return b'', b'private failure' if self.failure else b''


def _relay_candidates(monkeypatch):
    monkeypatch.setattr(Path,'is_socket',lambda _:True)
    monkeypatch.setenv('WSL_INTEROP','/run/WSL/test_unhealthy_interop')


@pytest.mark.parametrize('failure', [None, 'wrong_digest', 'wrong_token'])
def test_unpermitted_launches_never_invoke_bridge_and_keep_exact_negative_evidence(launch_request, monkeypatch, failure):
    request = launch_request
    _relay_candidates(monkeypatch)
    calls = []
    def launch(argv, **kwargs):
        process = Process(argv[-1],acknowledgement=failure is not None,failure=failure)
        calls.append(process); return process
    monkeypatch.setattr(transport.subprocess,'Popen',launch)
    with pytest.raises(transport.UnpermittedWindowsLaunch) as caught:
        transport.run_guarded_windows('SOURCE_ACTION', request=request, windows_path=str, readiness_seconds=.1)
    assert len(calls) == 2 and all(not p.permit.exists() for p in calls)
    transport.validate_unpermitted(request.parent.parent,request,caught.value.evidence)
    body = json.loads(caught.value.evidence.read_text())
    assert body['market_data_query_submission_possible'] is False
    assert all(row['permit_published'] is False for row in body['launches'])


def test_failed_relay_then_ready_relay_invokes_one_bridge_only(launch_request, monkeypatch):
    request = launch_request
    _relay_candidates(monkeypatch)
    calls = []
    def launch(argv, **kwargs):
        process = Process(argv[-1],acknowledgement=bool(calls))
        calls.append(process); return process
    monkeypatch.setattr(transport.subprocess,'Popen',launch)
    result = transport.run_guarded_windows('SOURCE_ACTION',request=request,windows_path=str,readiness_seconds=.1)
    assert result.returncode == 0 and len(calls) == 2
    assert not calls[0].permit.exists() and calls[1].permit.exists()
    assert not list((request.parent.parent/'launches').glob('*-unpermitted.json'))


@pytest.mark.parametrize('timeout', [False, True])
def test_admitted_failures_never_launch_second_worker(launch_request, monkeypatch, timeout):
    request = launch_request
    _relay_candidates(monkeypatch)
    calls = []
    def launch(argv, **kwargs):
        process = Process(argv[-1],timeout=timeout)
        if not timeout:
            original = process.communicate
            def unknown(**kwargs):
                original(**kwargs);process.returncode=1;return b'',b'unknown after admission'
            process.communicate = unknown
        calls.append(process); return process
    monkeypatch.setattr(transport.subprocess,'Popen',launch)
    if timeout:
        with pytest.raises(subprocess.TimeoutExpired):
            transport.run_guarded_windows('SOURCE_ACTION',request=request,windows_path=str,readiness_seconds=.1)
    else:
        result = transport.run_guarded_windows('SOURCE_ACTION',request=request,windows_path=str,readiness_seconds=.1)
        assert result.returncode == 1
    assert len(calls) == 1 and calls[0].permit.is_file()


def test_a_late_durable_permit_invalidates_unsent_recovery(launch_request, monkeypatch):
    request = launch_request
    _relay_candidates(monkeypatch)
    monkeypatch.setattr(transport.subprocess,'Popen',lambda argv,**_:Process(argv[-1],acknowledgement=False))
    with pytest.raises(transport.UnpermittedWindowsLaunch) as caught:
        transport.run_guarded_windows('SOURCE_ACTION',request=request,windows_path=str,readiness_seconds=.1)
    body = json.loads(caught.value.evidence.read_text())
    permit = request.parent.parent/body['launches'][0]['permit_path']
    permit.write_bytes(b'a late authorization')
    with pytest.raises(ValueError,match='durable launch permit'):
        transport.validate_unpermitted(request.parent.parent,request,caught.value.evidence)


def test_wrapper_requires_exact_permit_before_any_source_command():
    command = transport._command('SOURCE_ACTION','ready','permit','t'*32,'d'*64,20)
    assert command.index('Move-Item') < command.index('while(-not') < command.index('-cne $token') < command.index('SOURCE_ACTION')
    assert 'exit 75' in command and 'exit 76' in command


def test_interactive_gui_session_is_checked_before_any_readiness_or_permit():
    command=transport._command('SOURCE_ACTION','ready','permit','t'*32,'d'*64,20,1)
    assert command.index('SessionId -ne 1') < command.index('WriteAllText') < command.index('SOURCE_ACTION')
    assert 'exit 77' in command


@pytest.mark.parametrize('session', [0,-1,True,'1'])
def test_invalid_gui_session_never_launches_windows(launch_request, monkeypatch, session):
    monkeypatch.setattr(transport.subprocess,'Popen',lambda *a,**k:pytest.fail('No Windows launch permitted'))
    with pytest.raises(ValueError):
        transport.run_guarded_windows('SOURCE_ACTION',request=launch_request,windows_path=str,windows_session_id=session)


def test_exhausted_transport_returns_pending_without_input_bug_retry_budget(registry,monkeypatch):
    from contextlib import closing
    from downloader.tej_history import DesktopBridge, connect, run_one
    from test_tej_history import FakeBridge
    _,root,_,_ = registry
    run_one(root,FakeBridge())
    monkeypatch.setattr(DesktopBridge,'windows_path',staticmethod(str))
    monkeypatch.setattr(transport.subprocess,'Popen',lambda argv,**_:Process(argv[-1],acknowledgement=False))
    bridge = DesktopBridge(Path(__file__).resolve().parents[1],dict(TejProcessId=1,ExpectedWindow=2,ExpectedTitle='test',ExpectedWorkbook='test',ExpectedExcelWindow=3))
    assert run_one(root,bridge) == 'desktop_unavailable'
    with closing(connect(root)) as con:
        task = dict(con.execute("SELECT * FROM tasks WHERE kind='download'").fetchone())
        assert task['state']=='pending' and task['safe_prequery_retries']==0 and task['actual_rows'] is None
        assert con.execute('SELECT state FROM desktop_attempts').fetchone()[0]=='proven_not_submitted'
        from downloader.tej_desktop_attempts import proved_unsent_prequery
        proved_unsent_prequery(root,con,task,finished=True)
