"""Owned desktop startup and abandoned-worker recovery, not a new collector.

The Windows Interactive scheduled task enters here under the dataset lock.
Dead workers become UNKNOWN, never implicitly unsent. Only the installed
bounded replay grant and an independently verified retired desktop permit
resubmission of the exact original source request.
"""
from contextlib import closing
from datetime import UTC, datetime, timedelta
import hashlib
import json
import os
from pathlib import Path, PureWindowsPath
import re
import subprocess
import uuid

from downloader.artifact_io import atomic_write_bytes, atomic_write_json

CONTRACT = 'owned_interactive_tej_desktop_startup_v1'
ORPHAN_CONTRACT = 'exact_expired_dead_tej_worker_unknown_v1'
SESSION_KEYS = {'TejProcessId', 'ExpectedWindow', 'ExpectedTitle', 'ExpectedWorkbook', 'ExpectedExcelWindow'}


def startup_policy(config: dict) -> dict | None:
    policy = config.get('automation', {}).get('desktop_startup')
    if policy is None:
        return None
    keys = {'enabled', 'contract', 'authorization_basis', 'owned_workbook_windows'}
    if (not isinstance(policy, dict) or set(policy) != keys or type(policy['enabled']) is not bool
            or policy['contract'] != CONTRACT
            or policy['authorization_basis'] != 'explicit_user_request_automatic_tej_restart_after_boot'
            or policy['owned_workbook_windows'] != '%LOCALAPPDATA%\\StockAgent\\TEJSmartWizard\\StockAgent-TEJ-Acquisition.xlsx'):
        raise ValueError('Exact authorized acquisition desktop startup policy required')
    return dict(policy) if policy['enabled'] else None


def valid_session(session: dict) -> bool:
    if not isinstance(session, dict):
        return False
    prefix = 'TEJ Smart Wizard (Version 4.1.1.7) -- '
    title = session.get('ExpectedTitle', '')
    suffix = title[len(prefix):] if isinstance(title, str) and title.startswith(prefix) else ''
    named = session.get('ExpectedWorkbook', '')
    return (isinstance(session, dict) and set(session) == SESSION_KEYS
            and all(type(session[k]) is int and session[k] > 0
                    for k in ('TejProcessId', 'ExpectedWindow', 'ExpectedExcelWindow'))
            and isinstance(session['ExpectedWorkbook'], str) and bool(session['ExpectedWorkbook'])
            and bool(suffix) and (suffix == named or
                 PureWindowsPath(suffix).is_absolute() and PureWindowsPath(suffix).name == named))


def _read(path: Path, limit: int = 16384) -> dict:
    if path.is_symlink() or not path.is_file() or path.stat().st_size > limit:
        raise ValueError('Bounded private startup evidence required')
    value = json.loads(path.read_text(encoding='utf-8-sig'))
    if not isinstance(value, dict):
        raise ValueError('Private startup evidence object required')
    return value


def owner_alive(worker: dict) -> bool:
    """PID existence alone is insufficient: compare Linux process birth ticks."""
    pid = worker.get('owner_pid'); ticks = worker.get('owner_start_ticks')
    if type(pid) is not int or pid <= 0 or not isinstance(ticks, str) or not ticks.isdecimal():
        raise ValueError('Exact original worker process identity required')
    try:
        stat = Path(f'/proc/{pid}/stat').read_text().rsplit(') ', 1)[1].split()
    except FileNotFoundError:
        return False
    return stat[0] != 'Z' and stat[19] == ticks


def retire_expired_worker(root: Path, *, now: datetime | None = None) -> bool:
    """Called under .download.lock after exact saved-result reconciliation.

    A dead Linux relay does not prove a Windows query was never submitted.
    Keep original artifacts/traffic and classify UNKNOWN; never reset pending.
    """
    from downloader.tej_history import connect, task_request
    from downloader.tej_desktop_attempts import prepared_request_matches
    now = now or datetime.now(UTC)
    worker = _read(root / 'worker_status.json')
    if worker.get('state') != 'running' or owner_alive(worker):
        return False
    with closing(connect(root)) as con, con:
        rows = con.execute("SELECT * FROM tasks WHERE state='running'").fetchall()
        if len(rows) != 1:
            return False
        task = dict(rows[0]); attempt = task.get('active_attempt_id')
        if (task['kind'] != 'download' or task['task_id'] != worker.get('task_id')
                or worker.get('bridge_attempt_id') != attempt
                or not isinstance(attempt, str)
                or not re.fullmatch(re.escape(task['task_id']) + r'-[0-9a-f]{32}', attempt)):
            return False
        started = datetime.fromisoformat(task['attempted_at_utc'])
        deadline = datetime.fromisoformat(worker['deadline_at_utc'])
        if (now.tzinfo is None or started.tzinfo is None or deadline.tzinfo is None
                or worker.get('observed_at_utc') != task['attempted_at_utc']
                or not 0 < (deadline-started).total_seconds() <= 900 or now < deadline):
            return False
        raw = root / 'raw' / (attempt + '.json')
        if raw.exists() or raw.with_suffix('.json.outcome.json').exists() or task['receipt_path']:
            return False  # Canonical source/prequery reconciliation must go first.
        prepared = root / 'requests' / (attempt + '.json')
        request = _read(prepared, 2*1024**2)
        if (request.get('query_attempt_id') != attempt
                or not prepared_request_matches(request, task_request(root, task), task)):
            raise ValueError('Original abandoned request scope differs')
        registered = con.execute('SELECT * FROM desktop_attempts WHERE attempt_id=?', (attempt,)).fetchone()
        if (registered is None or registered['task_id'] != task['task_id']
                or registered['request_path'] != str(prepared.relative_to(root))
                or registered['raw_path'] != str(raw.relative_to(root))):
            raise ValueError('Registered abandoned attempt differs')
        audit = {'contract': ORPHAN_CONTRACT, 'attempt_id': attempt, 'task_id': task['task_id'],
                 'observed_at_utc': now.isoformat(), 'worker': worker,
                 'desktop_session': _read(root/'desktop_session.json'),
                 'prepared_request_sha256': hashlib.sha256(prepared.read_bytes()).hexdigest(),
                 'linux_boot_id': Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
                 'original_outcome': 'unknown_not_proven_unsent', 'provider_queries_sent': 0,
                 'task_requeued': False, 'raw_sources_preserved': True}
        atomic_write_json(root / 'orphaned_attempts' / (attempt + '.json'), audit, durable=True)
        con.execute("UPDATE desktop_attempts SET state='unknown_outcome',finished_at_utc=? WHERE attempt_id=?",
                    (now.isoformat(), attempt))
        con.execute("UPDATE tasks SET state='blocked',last_error_code='unknown_outcome_no_auto_retry' WHERE task_id=?",
                    (task['task_id'],))
        con.execute("UPDATE traffic SET state='unknown_after_worker_loss',completed_at_utc=? "
                    "WHERE action='download' AND started_at_utc=? AND state='running'",
                    (now.isoformat(), task['attempted_at_utc']))
    atomic_write_json(root / 'worker_status.json', {'contract_version': 4,
        'state': 'unknown_outcome_no_auto_retry', 'task_id': task['task_id'], 'observed_at_utc': now.isoformat()})
    return True


def orphan_desktop_loss_proof(root: Path, task: dict, request: dict, prepared: Path) -> Path:
    """Verify exact dead-worker UNKNOWN + retired desktop, not bare missing data."""
    from downloader.tej_history import connect
    from downloader.tej_desktop_attempts import prepared_request_matches
    attempt = task.get('active_attempt_id')
    if not isinstance(attempt, str) or prepared.resolve() != (root/'requests'/(attempt+'.json')).resolve():
        raise ValueError('Exact abandoned prepared request required')
    audit_path = root/'orphaned_attempts'/(attempt+'.json')
    audit = _read(audit_path); original = _read(prepared, 2*1024**2)
    if (audit.get('contract') != ORPHAN_CONTRACT or audit.get('attempt_id') != attempt
            or audit.get('task_id') != task['task_id'] or audit.get('task_requeued') is not False
            or audit.get('original_outcome') != 'unknown_not_proven_unsent'
            or audit.get('prepared_request_sha256') != hashlib.sha256(prepared.read_bytes()).hexdigest()
            or not prepared_request_matches(original, request, task) or owner_alive(audit['worker'])
            or (root/'raw'/(attempt+'.json')).exists() or task.get('receipt_path')):
        raise ValueError('Abandoned source action evidence differs')
    with closing(connect(root)) as con:
        row = con.execute('SELECT * FROM desktop_attempts WHERE attempt_id=?', (attempt,)).fetchone()
    if row is None or row['state'] != 'unknown_outcome' or row['task_id'] != task['task_id']:
        raise ValueError('Registered abandoned UNKNOWN required')
    status = _read(root/'desktop_startup_status.json')
    evidence = root/status.get('private_evidence_path', '')
    if (status.get('contract') != CONTRACT or status.get('state') != 'desktop_ready'
            or evidence.resolve().parent != (root/'desktop_startup').resolve()
            or evidence.is_symlink() or evidence.stat().st_size > 16384
            or status.get('private_evidence_sha256') != hashlib.sha256(evidence.read_bytes()).hexdigest()):
        raise ValueError('Verified replacement desktop startup required')
    proof = _read(evidence)
    if (proof.get('contract') != CONTRACT or proof.get('retired_window_absent') is not True
            or proof.get('retired_process_absent') is not True
            or not valid_session(proof.get('new_session')) or not valid_session(proof.get('retired_session'))
            or proof['new_session'] == proof['retired_session']
            or _read(root/'desktop_session.json') != proof['new_session']
            or status.get('session_sha256') != hashlib.sha256((root/'desktop_session.json').read_bytes()).hexdigest()):
        raise ValueError('Original desktop was not verified lost; recover retained response first')
    # The orphan audit captures the desktop identity at retirement. Never
    # transfer loss evidence from another workbook/query to this request.
    if audit.get('desktop_session') != proof['retired_session']:
        raise ValueError('Retired desktop differs from the abandoned attempt')
    return audit_path


def desktop_ready(root: Path, *, now: datetime | None = None, require_recent: bool = True) -> bool:
    try:
        status = _read(root/'desktop_startup_status.json')
        observed = datetime.fromisoformat(status['observed_at_utc'])
        age = ((now or datetime.now(UTC))-observed).total_seconds()
        ready = (status.get('contract') == CONTRACT and status.get('state') == 'desktop_ready'
                and observed.tzinfo is not None and age >= -5 and (not require_recent or age <= 180)
                and status.get('session_sha256') == hashlib.sha256((root/'desktop_session.json').read_bytes()).hexdigest())
        if ready and (root/'desktop_transport_session.json').exists():
            interactive_transport(root)
        return ready
    except (OSError, ValueError, KeyError, TypeError):
        return False


def interactive_transport(root: Path) -> dict:
    """No inherited Session-0 relay or terminal PID is treated as GUI readiness."""
    value = _read(root/'desktop_transport_session.json')
    socket = value.get('interop_socket', '')
    if (value.get('contract') != 'persistent_owned_interactive_tej_relay_v1'
            or value.get('state', 'active') != 'active'
            or type(value.get('windows_session_id')) is not int or value['windows_session_id'] <= 0
            or value.get('linux_boot_id') != Path('/proc/sys/kernel/random/boot_id').read_text().strip()
            or not owner_alive(value) or not re.fullmatch(r'/run/WSL/[0-9]+_interop', socket)
            or not Path(socket).is_socket()):
        raise ValueError('Live exact interactive desktop relay required')
    return value


def drain_interactive_transport(root: Path, *, timeout_seconds: float = 1900) -> dict:
    """Retain the relay until the current canonical source action has finished.

    SIGTERM is not permission to destroy a transport underneath a live query.
    First block NEW attempts, then drain the existing writer (same 1,900-second
    outer stop budget as the canonical service, including reconciliation).
    Never modify a
    replacement supervisor's manifest, even if its numeric PID was reused.
    """
    from downloader.dataset_lock import exclusive_dataset_lock
    path = root/'desktop_transport_session.json'
    try:
        value = _read(path)
        if (value.get('contract') != 'persistent_owned_interactive_tej_relay_v1'
                or value.get('owner_pid') != os.getpid() or not owner_alive(value)):
            return {'state': 'other_interactive_owner_preserved', 'provider_queries_sent': 0}
    except (OSError, ValueError, KeyError, TypeError):
        return {'state': 'no_owned_interactive_relay', 'provider_queries_sent': 0}
    value.update(state='draining', draining_at_utc=datetime.now(UTC).isoformat())
    atomic_write_json(path, value, durable=True)
    with exclusive_dataset_lock(root/'.download.lock', provider='tej_interactive_relay_drain',
                                timeout_seconds=timeout_seconds):
        return {'state': 'interactive_relay_drained', 'provider_queries_sent': 0}


def bootstrap(root: Path, repo: Path, windows_session_id: int, *, config: dict) -> dict:
    """Caller holds the canonical dataset lock; no provider request is issued."""
    from downloader.tej_history import DesktopBridge, connect
    from downloader.tej_scheduler import recover_complete_local_response
    from downloader.tej_windows_transport import run_guarded_windows, UnpermittedWindowsLaunch
    policy = startup_policy(config)
    if policy is None:
        raise ValueError('Explicit desktop startup policy required')
    recover_complete_local_response(root)
    try:
        retire_expired_worker(root)
    except (OSError, ValueError, KeyError, TypeError):
        pass  # Ambiguous claims cannot be cleared by bootstrapping a UI.
    with closing(connect(root)) as con:
        if con.execute("SELECT 1 FROM tasks WHERE state='running' LIMIT 1").fetchone():
            return {'state': 'waiting_existing_worker'}
    session_path = root/'desktop_session.json'; old = _read(session_path)
    if not valid_session(old):
        raise ValueError('Exact retained private desktop identity required')
    token = '0'*24 + '-' + uuid.uuid4().hex
    request = root/'requests'/(token+'.json'); output = root/'desktop_startup'/(token+'.json')
    atomic_write_json(request, {'contract': CONTRACT, 'action': 'start_owned_desktop',
        'windows_session_id': windows_session_id, 'session_sha256': hashlib.sha256(session_path.read_bytes()).hexdigest()})
    pinned = []
    for filename in ('start_tej_desktop.ps1', 'tej_smart_wizard_bridge.ps1'):
        code = (repo/'scripts'/filename).read_bytes()
        if not code or len(code) > 256*1024:
            raise ValueError('Bounded reviewed desktop startup script required')
        digest = hashlib.sha256(code).hexdigest()
        release = root/'desktop_startup'/'releases'/(digest+'.ps1')
        if release.exists():
            if release.is_symlink() or release.read_bytes() != code:
                raise ValueError('Immutable startup script differs')
        else:
            atomic_write_bytes(release, code)
        pinned.append((release, digest))
    # Stage immutable code on Windows, as the canonical bridge already does.
    # Do not hash a mutable checkout twice or execute a UNC script directly.
    command = "$ErrorActionPreference='Stop';$d=Join-Path ([Environment]::GetFolderPath('LocalApplicationData')) ('StockAgent\\TEJStartup\\worker-'+[Guid]::NewGuid().ToString('N'));[void](New-Item -ItemType Directory -Path $d);"
    for i, (script, digest) in enumerate(pinned):
        command += ('$p'+str(i)+"=Join-Path $d '"+str(i)+".ps1';Copy-Item -LiteralPath "+
            DesktopBridge.quote(DesktopBridge.windows_path(script))+' -Destination $p'+str(i)+';'+
            'if((Get-FileHash -LiteralPath $p'+str(i)+').Hash.ToLowerInvariant() -cne '+
            DesktopBridge.quote(digest)+"){throw 'Pinned startup code differs'};")
    command += ('& $p0 -BridgeScript $p1'+
        ' -Session '+DesktopBridge.quote(DesktopBridge.windows_path(session_path))+
        ' -OwnedWorkbook '+DesktopBridge.quote(policy['owned_workbook_windows'])+
        ' -Output '+DesktopBridge.quote(DesktopBridge.windows_path(output))+
        ' -WindowsSessionId '+str(windows_session_id))
    status = {'contract': CONTRACT, 'state': 'desktop_startup_unverified', 'provider_queries_sent': 0,
              'observed_at_utc': datetime.now(UTC).isoformat()}
    output.parent.mkdir(parents=True, exist_ok=True)
    try:
        result = run_guarded_windows(command, request=request, windows_path=DesktopBridge.windows_path,
                                     timeout=100, windows_session_id=windows_session_id)
        if result.returncode or not output.is_file():
            atomic_write_bytes(root/'diagnostics'/(token+'-startup.txt'), result.stderr[:32768])
            raise ValueError('Owned desktop bootstrap failed; inspect retained private diagnostics')
        proof = _read(output)
        if (proof.get('contract') != CONTRACT
                or proof.get('windows_session_id') != windows_session_id or proof.get('provider_queries_sent') != 0
                or any(proof.get(k) is not False for k in ('credentials_read', 'mouse_used', 'user_workbooks_closed'))):
            raise ValueError('Unverified interactive desktop startup result')
        new = proof.get('new_session')
        if proof.get('state') == 'desktop_ready':
            if (not valid_session(new) or proof.get('retired_session') != old
                    or new != old and (proof.get('retired_window_absent') is not True
                                       or proof.get('retired_process_absent') is not True)
                    or _read(session_path) != old):
                raise ValueError('Desktop replacement ownership differs')
            if new != old:
                atomic_write_json(root/'desktop_startup'/(token+'.old_session.json'), old)
                atomic_write_json(session_path, new, durable=True); session_path.chmod(0o600)
            # Preserve the original loss proof across later idempotent probes.
            previous = _read(root/'desktop_startup_status.json') if (root/'desktop_startup_status.json').exists() else {}
            evidence = (root/previous['private_evidence_path'] if new == old and
                        previous.get('state') == 'desktop_ready' and previous.get('session_sha256') ==
                        hashlib.sha256(session_path.read_bytes()).hexdigest() else output)
            status.update(state='desktop_ready', session_sha256=hashlib.sha256(session_path.read_bytes()).hexdigest(),
                          private_evidence_path=str(evidence.relative_to(root)),
                          private_evidence_sha256=hashlib.sha256(evidence.read_bytes()).hexdigest())
            socket = result.windows_interop_socket
            if isinstance(socket, str) and re.fullmatch(r'/run/WSL/[0-9]+_interop', socket) and Path(socket).is_socket():
                atomic_write_json(root/'desktop_transport_session.json', {
                    'contract': 'persistent_owned_interactive_tej_relay_v1', 'interop_socket': socket,
                    'state': 'active',
                    'owner_pid': os.getpid(),
                    'owner_start_ticks': Path(f'/proc/{os.getpid()}/stat').read_text().rsplit(') ',1)[1].split()[19],
                    'linux_boot_id': Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
                    'windows_session_id': windows_session_id, 'observed_at_utc': datetime.now(UTC).isoformat()})
        else:
            status.update(state=proof.get('state', 'desktop_startup_unverified'))
    except UnpermittedWindowsLaunch:
        status['state'] = 'interactive_windows_transport_unavailable'
    except (OSError, ValueError, subprocess.SubprocessError):
        status['state'] = 'desktop_startup_unverified'
    atomic_write_json(root/'desktop_startup_status.json', status, durable=True)
    return {k: v for k, v in status.items() if k not in ('private_evidence_path', 'private_evidence_sha256', 'session_sha256')}
