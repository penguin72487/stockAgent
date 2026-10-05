"""Two-phase Windows launch admission, before any TEJ/Excel interaction.

No launch receives a permit until its exact readiness acknowledgement arrives.
An unpermitted worker exits after its deadline and can safely be replaced. Once
a permit is durable, every error is an unknown outcome: never launch it again.
This is transport recovery, not a second data collector or a source retry.
"""
from __future__ import annotations

from datetime import UTC, datetime
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import time
import uuid

from downloader.artifact_io import atomic_write_bytes, atomic_write_json

CONTRACT = 'windows_ready_then_durable_bridge_permit_v1'
POWERSHELL = '/mnt/c/WINDOWS/System32/WindowsPowerShell/v1.0/powershell.exe'


class UnpermittedWindowsLaunch(RuntimeError):
    def __init__(self, evidence: Path):
        super().__init__('windows_transport_not_admitted_before_bridge')
        self.evidence = evidence


def _quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _command(command: str, ready: str, permit: str, token: str, digest: str, wait_seconds: float,
             windows_session_id: int | None = None) -> str:
    """The reviewed bridge command is unreachable until the exact permit exists."""
    acknowledgement = json.dumps({'contract':CONTRACT, 'token':token, 'request_sha256':digest}, separators=(',', ':'))
    return (
        "$ErrorActionPreference='Stop';" + ("if([Diagnostics.Process]::GetCurrentProcess().SessionId -ne " +
        str(windows_session_id) + "){exit 77};" if windows_session_id is not None else "") +
        "$ready="+_quote(ready)+";$permit="+_quote(permit)+";"
        "$token="+_quote(token)+";$tmp=$ready+'.tmp';"
        "[IO.File]::WriteAllText($tmp,"+_quote(acknowledgement)+",[Text.UTF8Encoding]::new($false));"
        "Move-Item -LiteralPath $tmp -Destination $ready;"
        "$deadline=[DateTime]::UtcNow.AddSeconds("+str(wait_seconds)+");"
        "while(-not (Test-Path -LiteralPath $permit)){"
        "if([DateTime]::UtcNow -ge $deadline){exit 75};Start-Sleep -Milliseconds 50};"
        "if([IO.File]::ReadAllText($permit) -cne $token){exit 76};" + command)


def _ready(path: Path, token: str, digest: str) -> bool:
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 2048:
        return False
    try:
        value = json.loads(path.read_text(encoding='utf-8-sig'))
    except (OSError, ValueError):
        return False
    return value == {'contract':CONTRACT, 'token':token, 'request_sha256':digest}


def _stop_unpermitted(process) -> bytes:
    # Even if terminating the Linux relay doesn't kill Windows, its unique
    # permit was NEVER published; its bounded gate cannot invoke the bridge.
    if process.poll() is None:
        process.terminate()
    try:
        _, stderr = process.communicate(timeout=2)
    except subprocess.TimeoutExpired:
        process.kill()
        _, stderr = process.communicate(timeout=2)
    return (stderr or b'')[:32768]


def run_guarded_windows(command: str, *, request: Path, windows_path, timeout: float = 900,
                        readiness_seconds: float = 20,
                        windows_session_id: int | None = None,
                        interop_socket: str | None = None) -> subprocess.CompletedProcess:
    request = request.resolve()
    if not re.fullmatch(r'[0-9a-f]{24}-[0-9a-f]{32}\.json', request.name):
        raise ValueError('Exact canonical bridge request required')
    if not 0 < readiness_seconds <= 30 or timeout < readiness_seconds:
        raise ValueError('Bounded Windows admission deadline required')
    if windows_session_id is not None and (type(windows_session_id) is not int or windows_session_id <= 0):
        raise ValueError('Exact interactive Windows session required')
    if interop_socket is not None and (windows_session_id is None or
            not re.fullmatch(r'/run/WSL/[0-9]+_interop', interop_socket) or not Path(interop_socket).is_socket()):
        raise ValueError('Exact live interactive WSL relay required')
    root = request.parent.parent
    digest = hashlib.sha256(request.read_bytes()).hexdigest()
    gates = root/'launches'
    gates.mkdir(mode=0o700, exist_ok=True)
    attempts = []
    # Prefer the WSL instance-owned relay instead of a short-lived interactive
    # shell relay. It is a candidate, never considered healthy without an ACK.
    stable = Path('/run/WSL/1_interop')
    environments = [None]
    if interop_socket is not None:
        environments = [{**os.environ, 'WSL_INTEROP': interop_socket}]
    elif stable.is_socket() and os.environ.get('WSL_INTEROP') != str(stable):
        # GUI startup must prefer its logged-in scheduled task's own relay.
        # The stable boot relay may belong to a noninteractive Session 0.
        environments.insert(len(environments) if windows_session_id is not None else 0,
                            {**os.environ, 'WSL_INTEROP':str(stable)})
    for slot, environment in enumerate(environments):
        token = uuid.uuid4().hex
        prefix = request.stem+'-'+str(slot)+'-'+token
        ready, permit = gates/(prefix+'.ready.json'), gates/(prefix+'.permit')
        argv = [POWERSHELL, '-NoProfile', '-NonInteractive', '-Command',
                _command(command, windows_path(ready), windows_path(permit), token, digest,
                         readiness_seconds+2, windows_session_id)]
        started = time.monotonic()
        process = None
        try:
            process = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=environment)
        except OSError:
            # No child was created, and no permit was written.
            pass
        if process is not None:
            deadline = started+readiness_seconds
            while time.monotonic() < deadline and process.poll() is None:
                if _ready(ready, token, digest):
                    # This is the irrevocable submission boundary. Everything
                    # after it, including a communicate timeout, is unknown.
                    atomic_write_bytes(permit, token.encode('ascii'))
                    try:
                        stdout, stderr = process.communicate(timeout=max(1, timeout-(time.monotonic()-started)))
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.communicate(timeout=2)
                        raise  # Durable permit means unknown, never another launch.
                    completed = subprocess.CompletedProcess(argv, process.returncode, stdout, stderr)
                    completed.windows_interop_socket = (environment or os.environ).get('WSL_INTEROP')
                    return completed
                time.sleep(.05)
        stderr = _stop_unpermitted(process) if process is not None else b''
        diagnostic = root/'diagnostics'/(prefix+'-unpermitted.txt')
        atomic_write_bytes(diagnostic, stderr)
        attempts.append({'ready_path':str(ready.relative_to(root)), 'permit_path':str(permit.relative_to(root)),
                         'permit_published':False, 'diagnostic_path':str(diagnostic.relative_to(root)),
                         'wrapper_sha256':hashlib.sha256(argv[-1].encode()).hexdigest()})
    evidence = gates/(request.stem+'-unpermitted.json')
    atomic_write_json(evidence, {'contract':CONTRACT, 'request_path':str(request.relative_to(root)),
        'request_sha256':digest, 'attempt_id':request.stem, 'observed_at_utc':datetime.now(UTC).isoformat(),
        'bridge_invocation_permitted':False, 'market_data_query_submission_possible':False, 'launches':attempts})
    raise UnpermittedWindowsLaunch(evidence)


def validate_unpermitted(root: Path, request: Path, evidence: Path) -> None:
    root, request, evidence = root.resolve(), request.resolve(), evidence.resolve()
    if (evidence.parent != root/'launches' or evidence.name != request.stem+'-unpermitted.json'
            or not evidence.is_file() or evidence.stat().st_size > 4096):
        raise ValueError('Exact bounded launch admission evidence required')
    body = json.loads(evidence.read_text())
    if (body.get('contract') != CONTRACT or body.get('request_path') != str(request.relative_to(root))
            or body.get('attempt_id') != request.stem
            or body.get('request_sha256') != hashlib.sha256(request.read_bytes()).hexdigest()
            or body.get('bridge_invocation_permitted') is not False
            or body.get('market_data_query_submission_possible') is not False
            or not isinstance(body.get('launches'), list) or not 1 <= len(body['launches']) <= 2):
        raise ValueError('Launch admission identity/proof differs')
    for slot, entry in enumerate(body['launches']):
        permit = root/entry.get('permit_path','')
        if (permit.parent != root/'launches' or not re.fullmatch(re.escape(request.stem)+
                '-'+str(slot)+r'-[0-9a-f]{32}\.permit', permit.name)
                or entry.get('permit_published') is not False or permit.exists() or permit.is_symlink()):
            raise ValueError('A durable launch permit prevents automatic replay')
