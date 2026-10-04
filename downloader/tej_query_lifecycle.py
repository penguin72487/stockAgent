"""Explicit, scoped desktop recovery; never a periodic or automatic restart."""
from contextlib import closing
from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path
import subprocess
import uuid

from downloader.artifact_io import atomic_write_bytes, atomic_write_json
from downloader.tej_history import connect, task_request

CONTRACT = 'explicit_scratch_query_lifecycle_v1'
RESTART_CONTRACT = 'explicit_exact_addin_restart_v1'
BARRIER = 'desktop_interface_recovery_required'


def mark_interface_barrier(root: Path, *, reason: str, evidence: Path | None = None) -> None:
    status = {'contract_version':4, 'state':BARRIER, 'reason':reason,
              'observed_at_utc':datetime.now(UTC).isoformat(), 'market_data_query_submitted':False}
    if evidence is not None:
        status['private_evidence_sha256'] = hashlib.sha256(evidence.read_bytes()).hexdigest()
    with closing(connect(root)) as con, con:
        con.execute('INSERT OR REPLACE INTO meta VALUES (?,?)',(BARRIER,json.dumps(status)))
    atomic_write_json(root/'worker_status.json',status)


def verify_interface(root: Path, task_id: str, bridge) -> dict:
    """Read-only stability proof, not recovery of any failed data query."""
    with closing(connect(root)) as con:
        row=con.execute('SELECT * FROM tasks WHERE task_id=?',(task_id,)).fetchone()
        if row is None or con.execute("SELECT 1 FROM tasks WHERE state='running' OR "
                "(kind='download' AND state='blocked' AND last_error_code='unknown_outcome_no_auto_retry') LIMIT 1").fetchone():
            raise ValueError('Quiescent interface and no unresolved Preview required')
    return _verify_stable_interface(root, dict(row), bridge)


def _verify_stable_interface(root: Path, task: dict, bridge) -> dict:
    """Only caller-verified quiescent/restarted interfaces reach this readback.

    A restarted old unknown query stays unknown; this proves only that the new
    interface is usable, never that the original download succeeded or was free.
    """
    task_id=task['task_id']; request=task_request(root,task)
    request['action']='confirm_metadata_error_cleared'
    payload,evidence,_=bridge.execute(root,{**task,'request_json':json.dumps(request)})
    if (payload.get('contract_version')!=4 or payload.get('provider')!='tej_smart_wizard'
            or payload.get('action')!=request['action'] or payload.get('task_id')!=task_id
            or any(payload.get(k)!=request.get(k) for k in ('type','smart_id','table'))
            or any(payload.get(k) is not True for k in ('vendor_notices_absent','source_selectors_enabled','source_binding_stable'))
            or any(payload.get(k) is not False for k in ('market_data_query_submitted','source_axes_adopted','credentials_read'))):
        raise ValueError('Source interface stability not verified; barrier retained')
    audit={'contract':CONTRACT,'state':'desktop_interface_verified','observed_at_utc':datetime.now(UTC).isoformat(),
           'task_id':task_id,'market_data_query_submitted':False,'failed_tasks_reset':False,
           'private_evidence_sha256':hashlib.sha256(evidence.read_bytes()).hexdigest()}
    atomic_write_json(root/'diagnostics'/('interface_verified-'+uuid.uuid4().hex+'.json'),audit)
    with closing(connect(root)) as con, con:
        con.execute('DELETE FROM meta WHERE key=?',(BARRIER,))
    atomic_write_json(root/'worker_status.json',{'contract_version':4,**audit})
    return {'state':audit['state'],'data_query_repeated':False,'failed_tasks_reset':False}


def _unknown_download(root: Path, task_id: str, prepared: Path) -> dict:
    """Exact unadopted active attempt for explicit operator reset/replay only."""
    from downloader.tej_desktop_attempts import query_stage
    with closing(connect(root)) as con:
        row=con.execute('SELECT * FROM tasks WHERE task_id=?',(task_id,)).fetchone()
        if (row is None or row['kind']!='download' or row['state']!='blocked'
                or row['last_error_code']!='unknown_outcome_no_auto_retry'
                or con.execute("SELECT 1 FROM tasks WHERE state='running' OR "
                    "(state='blocked' AND last_error_code='unknown_outcome_no_auto_retry' AND task_id!=?) LIMIT 1",
                    (task_id,)).fetchone()):
            raise ValueError('One exact quiescent unknown download required for operator restart')
    task=dict(row);attempt=task.get('active_attempt_id')
    if (not isinstance(attempt,str) or not attempt.startswith(task_id+'-')
            or any(task.get(key) is not None for key in ('actual_rows','receipt_path','completed_at_utc'))
            or (root/'raw'/(attempt+'.json')).exists()
            or (root/'receipts'/(task_id+'.json')).exists()):
        raise ValueError('Saved/adopted response must be recovered before operator restart')
    query_stage(root,task,task_request(root,task),prepared)
    return task


def _execute_phase(bridge, root: Path, phase: str, output: Path, *, restart_pins: dict | None = None,
                   launch_after_utc: str | None = None) -> dict:
    source=bridge.quote(bridge.windows_path(bridge.repo/'scripts/reopen_tej_smart_wizard_query.ps1'))
    args=' '.join(f'-{key} {bridge.quote(str(value))}' for key,value in bridge.session.items())
    if launch_after_utc is not None:
        args += ' -ExpectedLaunchAtUtc ' + bridge.quote(launch_after_utc)
    if restart_pins is not None:
        if set(restart_pins)!={'ExpectedProcessStartUtc','ExpectedImageSha256'}:
            raise ValueError('Unreviewed process restart identity fields')
        args+=' -AllowRestartAddin '+ ' '.join(f'-{key} {bridge.quote(value)}' for key,value in restart_pins.items())
    command=("$ErrorActionPreference='Stop';$s="+source+";"
        "$d=Join-Path ([Environment]::GetFolderPath('LocalApplicationData')) ('StockAgent\\TEJSmartWizard\\reopen-'+[Guid]::NewGuid().ToString('N'));"
        "[void](New-Item -ItemType Directory -Path $d);$p=Join-Path $d 'reopen.ps1';Copy-Item -LiteralPath $s -Destination $p;"
        "if((Get-FileHash -LiteralPath $s).Hash -cne (Get-FileHash -LiteralPath $p).Hash){throw 'Script copy mismatch'};"
        f"try{{& $p {args} -Action {phase} -AllowDiscardQuerySettings "
        f"-BridgeScript {bridge.quote(bridge.windows_path(bridge.repo/'scripts/tej_smart_wizard_bridge.ps1'))} "
        f"-Output {bridge.quote(bridge.windows_path(output))}}}"
        "catch{[Console]::Error.WriteLine($_.Exception.Message);[Console]::Error.WriteLine($_.ScriptStackTrace);exit 1}")
    try:
        result=subprocess.run(['/mnt/c/WINDOWS/System32/WindowsPowerShell/v1.0/powershell.exe',
            '-NoProfile','-NonInteractive','-Command',command],capture_output=True,timeout=60)
    except subprocess.TimeoutExpired:
        raise RuntimeError('query_lifecycle_phase_unresolved_no_repeat') from None
    if result.returncode or not output.is_file():
        diagnostic=root/'diagnostics'/('query_lifecycle-'+output.parent.name+'-'+output.stem+'.txt')
        atomic_write_bytes(diagnostic,result.stderr[:32768]);diagnostic.chmod(0o600)
        raise RuntimeError('query_lifecycle_phase_failed_requires_scoped_diagnostic')
    if output.stat().st_size>16384:
        raise ValueError('Unreviewed lifecycle response bound')
    return json.loads(output.read_text(encoding='utf-8-sig'))


def reopen_query(root: Path, task_id: str, bridge, session_path: Path, *, allow_discard: bool) -> dict:
    """CLI holds canonical lock. The user must allow this scratch form reset."""
    keys={'TejProcessId','ExpectedWindow','ExpectedTitle','ExpectedWorkbook','ExpectedExcelWindow'}
    if (allow_discard is not True or set(bridge.session)!=keys
            or session_path.resolve()!=(root/'desktop_session.json').resolve()
            or json.loads(session_path.read_text())!=bridge.session):
        raise ValueError('Exact private session and explicit scratch-query reset authorization required')
    with closing(connect(root)) as con:
        if con.execute('SELECT 1 FROM tasks WHERE task_id=?',(task_id,)).fetchone() is None:
            raise ValueError('Exact registered interface-verification task required')
        if con.execute("SELECT 1 FROM tasks WHERE state='running' OR "
                "(kind='download' AND state='blocked' AND last_error_code='unknown_outcome_no_auto_retry') LIMIT 1").fetchone():
            raise ValueError('Recover unresolved Preview before discarding the scratch query')
    run=root/'query_lifecycle'/uuid.uuid4().hex
    run.mkdir(parents=True,mode=0o700)
    original=session_path.read_bytes()
    atomic_write_bytes(run/'original_session.json',original);(run/'original_session.json').chmod(0o600)
    mark_interface_barrier(root,reason='explicit_operator_scratch_query_reset')
    for phase in ('close','open'):
        payload=_execute_phase(bridge,root,phase,run/(phase+'.json'))
        if (payload.get('contract')!=CONTRACT or payload.get('action')!=phase
                or payload.get('old_query_window')!=bridge.session['ExpectedWindow']
                or payload.get('discard_scratch_query_settings_authorized') is not True
                or any(payload.get(k) is not False for k in ('workbook_closed','workbook_saved','credentials_read','market_data_query_submitted'))
                or payload.get('query_closed_verified' if phase=='close' else 'query_open_verified') is not True):
            raise ValueError('Unverified query lifecycle phase; barrier retained')
    new=payload.get('new_session',{})
    recreated = payload.get('process_recreated_after_query_close') is True and payload.get('process_name') == 'TEJAddin'
    if (set(new)!=keys or any(new[k]!=bridge.session[k] for k in keys-{'ExpectedWindow','TejProcessId'})
            or new.get('TejProcessId') != bridge.session['TejProcessId'] and not recreated
            or any(not isinstance(new[k],int) or isinstance(new[k],bool) or new[k]<=0 for k in ('ExpectedWindow','TejProcessId'))
            or new['ExpectedWindow']==bridge.session['ExpectedWindow']
            or session_path.read_bytes()!=original):
        raise ValueError('Reopened query session identity not verified; no session replacement')
    atomic_write_json(session_path,new);session_path.chmod(0o600)
    bridge.session=new
    result=verify_interface(root,task_id,bridge)
    return {**result,'scratch_query_reopened':True,'workbook_closed':False,'raw_source_changed':False}


def adopt_open_query(root: Path, task_id: str, bridge, session_path: Path, previous: Path) -> dict:
    """Read an already opened query; never repeat the close or launcher action."""
    previous = previous.resolve()
    if (previous.parent != (root/'query_lifecycle').resolve() or len(previous.name) != 32
            or any(c not in '0123456789abcdef' for c in previous.name)
            or session_path.resolve() != (root/'desktop_session.json').resolve()
            or (previous/'original_session.json').read_bytes() != session_path.read_bytes()
            or json.loads(session_path.read_text()) != bridge.session or (previous/'open.json').exists()):
        raise ValueError('Exact unresolved original query launch required')
    proofs = {}
    for filename in ('close.json', 'open.json.submission.json'):
        path = previous/filename
        if path.stat().st_size > 16384:
            raise ValueError('Unreviewed lifecycle proof bound')
        proof = json.loads(path.read_text(encoding='utf-8-sig'))
        if (proof.get('contract') != CONTRACT or proof.get('old_query_window') != bridge.session['ExpectedWindow']
                or proof.get('discard_scratch_query_settings_authorized') is not True
                or any(proof.get(k) is not False for k in ('workbook_closed','workbook_saved','credentials_read','market_data_query_submitted'))):
            raise ValueError('Original reset scope differs; no adoption')
        proofs[filename] = proof
    if (proofs['close.json'].get('query_closed_verified') is not True
            or proofs['close.json'].get('action') != 'close'
            or proofs['open.json.submission.json'].get('action') != 'open'
            or proofs['open.json.submission.json'].get('open_method') not in {'owned_uia_toggle_once_v1','owned_uia_invoke_once_v1'}):
        raise ValueError('Exact closed form and single normal launch intent required')
    with closing(connect(root)) as con:
        if con.execute("SELECT 1 FROM tasks WHERE state='running' OR (state='blocked' AND "
                       "last_error_code='unknown_outcome_no_auto_retry') LIMIT 1").fetchone():
            raise ValueError('Unresolved Preview cannot be discarded')
    original = session_path.read_bytes()
    payload = _execute_phase(bridge, root, 'inspect-open', previous/'adopt_open.json',
                             launch_after_utc=proofs['open.json.submission.json']['observed_at_utc'])
    new = payload.get('new_session', {})
    keys = set(bridge.session)
    if (payload.get('contract') != CONTRACT or payload.get('action') != 'inspect-open'
            or payload.get('old_query_window') != bridge.session['ExpectedWindow']
            or payload.get('query_open_verified') is not True
            or any(payload.get(k) is not False for k in ('workbook_closed','workbook_saved','credentials_read','market_data_query_submitted'))
            or set(new) != keys or any(new[k] != bridge.session[k] for k in keys - {'ExpectedWindow','TejProcessId'})
            or any(not isinstance(new[k],int) or isinstance(new[k],bool) or new[k] <= 0 for k in ('ExpectedWindow','TejProcessId'))
            or new['ExpectedWindow'] == bridge.session['ExpectedWindow']
            or new['TejProcessId'] != bridge.session['TejProcessId'] and
                not (payload.get('process_recreated_after_query_close') is True and payload.get('process_name') == 'TEJAddin')
            or session_path.read_bytes() != original):
        raise ValueError('Already opened query identity not verified; no rebind')
    atomic_write_json(session_path, new); session_path.chmod(0o600); bridge.session = new
    result = verify_interface(root, task_id, bridge)
    return {**result,'existing_open_adopted':True,'query_open_repeated':False,'workbook_closed':False}


def restart_addin(root: Path, task_id: str, bridge, session_path: Path, *, allow_restart: bool,
                  allow_discard: bool, recovery_run: Path | None = None,
                  unknown_prepared: Path | None = None, acknowledge_unknown_usage: bool = False) -> dict:
    """Extra operator permission; exact process only, preserving Excel/source.

    No scheduler calls this path. Pin a live process before stopping it, then
    launch once through the existing Excel command and independently verify
    the new source interface. A phase failure leaves the durable barrier set.
    """
    keys={'TejProcessId','ExpectedWindow','ExpectedTitle','ExpectedWorkbook','ExpectedExcelWindow'}
    if (allow_restart is not True or allow_discard is not True or set(bridge.session)!=keys
            or session_path.resolve()!=(root/'desktop_session.json').resolve()
            or json.loads(session_path.read_text())!=bridge.session):
        raise ValueError('Exact private session and separate process-restart/discard authorization required')
    if acknowledge_unknown_usage is not False and (acknowledge_unknown_usage is not True or unknown_prepared is None):
        raise ValueError('Exact unknown prepared request and explicit usage acknowledgement required')
    if unknown_prepared is not None and acknowledge_unknown_usage is not True:
        raise ValueError('Unknown query reset requires explicit usage acknowledgement')
    unknown=_unknown_download(root,task_id,unknown_prepared) if unknown_prepared is not None else None
    with closing(connect(root)) as con:
        if con.execute('SELECT 1 FROM tasks WHERE task_id=?',(task_id,)).fetchone() is None:
            raise ValueError('Exact registered interface-verification task required')
        if con.execute("SELECT 1 FROM tasks WHERE state='running' OR "
                "(kind='download' AND state='blocked' AND last_error_code='unknown_outcome_no_auto_retry' AND task_id!=?) LIMIT 1",
                (task_id if unknown is not None else '',)).fetchone():
            raise ValueError('Recover unresolved Preview before restarting the add-in')
    resumed={}
    if recovery_run is not None:
        previous=recovery_run.resolve()
        diagnostic=root/'diagnostics'/('query_lifecycle-'+previous.name+'-open.txt')
        if (previous.parent!=(root/'query_lifecycle').resolve() or len(previous.name)!=32
                or any(c not in '0123456789abcdef' for c in previous.name)
                or (previous/'original_session.json').read_bytes()!=session_path.read_bytes()
                or any(previous.glob('open*.json')) or not diagnostic.is_file()
                or not diagnostic.read_bytes()[:2048].decode('utf-8-sig').startswith(
                    'Exception calling "GetCurrentPattern" with "1" argument(s): "Unsupported Pattern."')):
            raise ValueError('Exact stopped run and proven pre-launch unsupported-pattern failure required')
        # A later resume can fail after the normal launcher action was sent.
        # Preserve that intent and never use an earlier unsent failure to
        # blindly launch again. The operator must inspect/adopt its result.
        for intent in (root/'query_lifecycle').glob('*/open.json.submission.json'):
            if intent.stat().st_size>16384:
                raise ValueError('Unreviewed lifecycle submission evidence')
            sent=json.loads(intent.read_text(encoding='utf-8-sig'))
            if sent.get('old_query_window')==bridge.session['ExpectedWindow']:
                raise ValueError('Open action may already have been submitted; no launch retry')
        for phase in ('inspect-addin','stop-addin'):
            path=previous/(phase+'.json')
            if path.stat().st_size>16384 or path.resolve().parent!=previous:
                raise ValueError('Unreviewed stopped-run evidence')
            resumed[phase]=json.loads(path.read_text(encoding='utf-8-sig'))
    run=root/'query_lifecycle'/uuid.uuid4().hex
    run.mkdir(parents=True,mode=0o700)
    original=session_path.read_bytes()
    atomic_write_bytes(run/'original_session.json',original);(run/'original_session.json').chmod(0o600)
    if resumed:
        atomic_write_json(run/'resumed_stopped_run.json',{'contract':RESTART_CONTRACT,
            'previous_run':str(previous.relative_to(root)),
            'phase_evidence_sha256':{phase:hashlib.sha256((previous/(phase+'.json')).read_bytes()).hexdigest() for phase in resumed},
            'stop_repeated':False,'open_previously_submitted':False})
    mark_interface_barrier(root,reason='explicit_operator_exact_addin_restart')
    pins={'ExpectedProcessStartUtc':'','ExpectedImageSha256':''}
    for phase in ('inspect-addin','stop-addin','open'):
        if phase in resumed:
            payload=resumed[phase]
            atomic_write_json(run/(phase+'.json'),payload)
        else:
            payload=_execute_phase(bridge,root,phase,run/(phase+'.json'),restart_pins=pins)
        if (payload.get('contract')!=RESTART_CONTRACT or payload.get('action')!=phase
                or payload.get('old_query_window')!=bridge.session['ExpectedWindow']
                or payload.get('addin_process_restart_authorized') is not True
                or payload.get('discard_scratch_query_settings_authorized') is not True
                or any(payload.get(k) is not False for k in ('workbook_closed','workbook_saved','credentials_read','market_data_query_submitted'))):
            raise ValueError('Unverified exact-addin restart phase; barrier retained')
        if phase=='inspect-addin':
            stamp=payload.get('process_start_utc');digest=payload.get('image_sha256')
            if (payload.get('process_name')!='TEJAddin' or payload.get('process_id')!=bridge.session['TejProcessId']
                    or not isinstance(stamp,str) or not datetime.fromisoformat(stamp.replace('Z','+00:00')).tzinfo
                    or not isinstance(digest,str) or len(digest)!=64 or any(c not in '0123456789ABCDEF' for c in digest)):
                raise ValueError('Live add-in process identity not verified; stop refused')
            pins={'ExpectedProcessStartUtc':stamp,'ExpectedImageSha256':digest}
        elif phase=='stop-addin':
            if (payload.get('process_name')!='TEJAddin' or payload.get('process_id')!=bridge.session['TejProcessId']
                    or payload.get('addin_stopped_verified') is not True or payload.get('process_start_utc')!=pins['ExpectedProcessStartUtc']
                    or payload.get('image_sha256')!=pins['ExpectedImageSha256']):
                raise ValueError('Pinned old add-in stop not verified; launch refused')
        elif payload.get('query_open_verified') is not True or payload.get('image_sha256')!=pins['ExpectedImageSha256']:
            raise ValueError('New add-in query/image identity not verified; no session replacement')
    new=payload.get('new_session',{})
    if (set(new)!=keys or any(new[k]!=bridge.session[k] for k in keys-{'TejProcessId','ExpectedWindow'})
            or any(not isinstance(new[k],int) or isinstance(new[k],bool) or new[k]<=0 for k in ('TejProcessId','ExpectedWindow'))
            or new['TejProcessId']==bridge.session['TejProcessId'] or new['ExpectedWindow']==bridge.session['ExpectedWindow']
            or session_path.read_bytes()!=original):
        raise ValueError('Restarted query session identity not verified; no session replacement')
    atomic_write_json(session_path,new);session_path.chmod(0o600)
    bridge.session=new
    if unknown is not None:
        if _unknown_download(root,task_id,unknown_prepared)!=unknown:
            raise ValueError('Unknown download changed during exact restart; no replay')
        result=_verify_stable_interface(root,unknown,bridge)
    else:
        result=verify_interface(root,task_id,bridge)
    return {**result,'addin_restarted':True,'scratch_query_reopened':True,'workbook_closed':False,
            'raw_source_changed':False,'recovery_run':str(run.relative_to(root))}


def restart_unknown_download(root: Path, task_id: str, bridge, session_path: Path, prepared: Path, *,
                             allow_restart: bool, allow_discard: bool, acknowledge_unknown_usage: bool,
                             record_density_prior: int | None = None) -> dict:
    """Operator-only exact add-in reset plus one canonical authorized replay.

    Never called by supervision. All old unknown evidence is retained, and an
    immutable, consumed-once authorization uses the existing replay mechanism.
    The old result is NOT relabelled unsent, empty or completed.
    """
    from downloader.tej_desktop_attempts import OPERATOR_REPLAY_CONTRACT, query_stage
    from downloader.tej_history import run_one
    if acknowledge_unknown_usage is not True:
        raise ValueError('Explicit unknown usage acknowledgement required')
    if record_density_prior is not None and (type(record_density_prior) is not int or not 2<=record_density_prior<=10000):
        raise ValueError('Explicit bounded native-record density prior required')
    task=_unknown_download(root,task_id,prepared)
    request=task_request(root,task);_,stage_path=query_stage(root,task,request,prepared)
    if record_density_prior is not None:
        from downloader.tej_planning import CONTRACT
        with closing(connect(root)) as con:
            row=con.execute('SELECT contract,plan_json FROM download_plans WHERE table_id=?',(task['table_id'],)).fetchone()
        if (request.get('source_key_mode')!=3 or row is None or row['contract']!=CONTRACT
                or record_density_prior<=json.loads(row['plan_json'])['request'].get('native_record_density_hint',1)):
            raise ValueError('Existing native-record plan and strictly larger density prior required before restart')
    original_hashes={name:hashlib.sha256(path.read_bytes()).hexdigest()
                     for name,path in (('request',prepared),('stage',stage_path))}
    restored=restart_addin(root,task_id,bridge,session_path,allow_restart=allow_restart,
        allow_discard=allow_discard,unknown_prepared=prepared,acknowledge_unknown_usage=True)
    if (_unknown_download(root,task_id,prepared)!=task or
            any(hashlib.sha256(path.read_bytes()).hexdigest()!=original_hashes[name]
                for name,path in (('request',prepared),('stage',stage_path)))):
        raise ValueError('Original unknown evidence changed during restart; no replay')
    if record_density_prior is not None:
        from downloader.tej_preview_capacity import replan_restarted_unknown
        return {**restored,**replan_restarted_unknown(root,task_id,prepared,root/restored['recovery_run'],
                    record_density_prior=record_density_prior),'automatic_retry':False,
                'possible_additional_provider_usage':True}
    run=root/restored['recovery_run'];authorization=uuid.uuid4().hex
    audit_path=root/'operator_replays'/(task_id+'-'+authorization+'.json')
    audit={'contract':OPERATOR_REPLAY_CONTRACT,'recovery_contract':'explicit_restart_unknown_download_v1',
        'authorization_id':authorization,'task_id':task_id,'observed_at_utc':datetime.now(UTC).isoformat(),
        'original_attempted_at_utc':task['attempted_at_utc'],'original_attempt_id':task['active_attempt_id'],
        'original_request_path':str(prepared.resolve().relative_to(root.resolve())),
        'original_request_sha256':original_hashes['request'],
        'original_stage_path':str(stage_path.relative_to(root)),'original_stage_sha256':original_hashes['stage'],
        'restart_evidence_sha256':{phase:hashlib.sha256((run/(phase+'.json')).read_bytes()).hexdigest()
                                  for phase in ('inspect-addin','stop-addin','open')},
        'recovery_run':restored['recovery_run'],'original_outcome':'unknown_retained_not_claimed_unsent',
        'automatic_retry':False,'possible_additional_provider_usage':True,'source_rows_adopted':False}
    atomic_write_json(audit_path,audit)
    with closing(connect(root)) as con,con:
        if dict(con.execute('SELECT * FROM tasks WHERE task_id=?',(task_id,)).fetchone())!=task:
            raise ValueError('Unknown task changed before operator authorization; no replay')
        con.execute('INSERT INTO desktop_replays VALUES(?,?,?,?,?,?,?)',
            (authorization,task_id,task['attempted_at_utc'],str(audit_path.relative_to(root)),
             hashlib.sha256(audit_path.read_bytes()).hexdigest(),None,None))
    state=run_one(root,bridge,retry_authorization_id=authorization)
    with closing(connect(root)) as con,con:
        con.execute('UPDATE desktop_replays SET outcome=? WHERE authorization_id=?',(state,authorization))
    return {**restored,'state':state,'operator_replay':True,'automatic_retry':False,
            'original_unknown_evidence_retained':True,'authorization_id':authorization,
            'possible_additional_provider_usage':True}
