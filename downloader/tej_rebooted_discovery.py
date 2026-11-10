"""Quarantine one exact interrupted metadata plan after both hosts rebooted.

This never requeues the plan, adopts observations or repeats a source query.
Caller must hold the original TEJ dataset lock. An independently verified new
desktop is still required before the canonical scheduler may do source work.
"""
from __future__ import annotations

from contextlib import closing
from datetime import UTC, datetime, timedelta
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
import uuid

from downloader.artifact_io import atomic_write_json


CONTRACT = 'tej_rebooted_metadata_plan_quarantine_v1'
# This retained immutable bridge was inspected: the plan branch captures only
# metadata, with Preview/export confined to the download branch. Other bridge
# generations need independent review before adding their exact digest.
REVIEWED_PLAN_BRIDGES = frozenset({'938fb5400b2a2c106ec019ca131a56e91f5e85ee6a7194bccd301330f0f28fbd'})
QUARANTINE = 'metadata_interrupted_host_reboot_requires_review'


def windows_boot_at() -> datetime:
    exe = shutil.which('powershell.exe')
    if not exe:
        raise ValueError('Windows boot evidence unavailable')
    result = subprocess.run([exe, '-NoProfile', '-NonInteractive', '-Command',
        '((Get-CimInstance Win32_OperatingSystem).LastBootUpTime).ToUniversalTime().ToString("o")'],
        capture_output=True, text=True, timeout=15, check=True)
    value = datetime.fromisoformat(result.stdout.strip().replace('Z', '+00:00'))
    if value.tzinfo is None:
        raise ValueError('Windows boot clock lacks timezone')
    return value.astimezone(UTC)


def quarantine(root: Path, *, now: datetime | None = None, boot_at: datetime | None = None,
               dry_run: bool = False) -> bool:
    """Preserve the original incomplete task, freeing no unknown data Preview."""
    from downloader.tej_desktop_attempts import prepared_request_matches
    from downloader.tej_history import SOURCE_SCOPE_CONTRACT, connect, task_request
    from downloader.tej_startup import owner_alive

    now = now or datetime.now(UTC)
    if type(dry_run) is not bool:
        raise ValueError('dry_run must be boolean')
    root = root.resolve()
    worker_path = root / 'worker_status.json'
    worker_raw = worker_path.read_bytes()
    worker = json.loads(worker_raw)
    if (now.tzinfo is None or worker.get('state') != 'running'
            or worker.get('kind') != 'discover' or owner_alive(worker)):
        return False
    old_boot = worker.get('linux_boot_id')
    current_boot = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
    if (not isinstance(old_boot, str) or not re.fullmatch(r'[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}', old_boot)
            or old_boot == current_boot):
        return False
    start = datetime.fromisoformat(worker['observed_at_utc'])
    deadline = datetime.fromisoformat(worker['deadline_at_utc'])
    if (start.tzinfo is None or deadline.tzinfo is None or not 0 < (deadline-start).total_seconds() <= 900
            or now < deadline):
        return False
    boot_at = boot_at or windows_boot_at()
    if boot_at.tzinfo is None or not deadline + timedelta(seconds=5) < boot_at <= now + timedelta(seconds=5):
        return False

    with closing(connect(root)) as con:
        rows = con.execute("SELECT * FROM tasks WHERE state='running' LIMIT 2").fetchall()
        if len(rows) != 1:
            return False
        task = dict(rows[0])
        task_id, attempt = task['task_id'], worker.get('bridge_attempt_id')
        if (task['kind'] != 'discover' or task_id != worker.get('task_id')
                or task['scope_contract'] != SOURCE_SCOPE_CONTRACT or task['active_attempt_id'] is not None
                or task['attempted_at_utc'] != worker['observed_at_utc']
                or any(task[k] is not None for k in ('actual_rows', 'receipt_path', 'completed_at_utc'))
                or not isinstance(attempt, str)
                or not re.fullmatch(re.escape(task_id) + r'-[0-9a-f]{32}', attempt)):
            return False
        events = con.execute("SELECT * FROM traffic WHERE action='discover' AND started_at_utc=?",
                             (task['attempted_at_utc'],)).fetchall()
        if (len(events) != 1 or events[0]['state'] != 'running' or events[0]['completed_at_utc'] is not None
                or con.execute('SELECT 1 FROM desktop_attempts WHERE task_id=?', (task_id,)).fetchone()):
            return False
        event = dict(events[0])
        request = task_request(root, task, connection=con)
    prepared = root / 'requests' / (attempt + '.json')
    manifest = root / 'launches' / (attempt + '.bridge.json')
    raw = root / 'raw' / (attempt + '.json')
    if (prepared.is_symlink() or manifest.is_symlink() or not prepared.is_file() or not manifest.is_file()
            or prepared.stat().st_size > 2*1024**2 or manifest.stat().st_size > 4096
            or not start.timestamp() <= prepared.stat().st_mtime <= deadline.timestamp() + 5
            or any(path.exists() or path.is_symlink() for path in
                   (raw, raw.with_suffix('.json.stage.json'), raw.with_suffix('.json.outcome.json')))):
        return False
    prepared_raw, manifest_raw = prepared.read_bytes(), manifest.read_bytes()
    original, launch = json.loads(prepared_raw), json.loads(manifest_raw)
    digest = launch.get('bridge_script_sha256')
    release = root / 'bridge_releases' / (str(digest) + '.ps1')
    if (request.get('action') != 'plan' or original.get('action') != 'plan'
            or original.get('query_attempt_id') is not None or not prepared_request_matches(original, request, task)
            or launch.get('contract') != 'immutable_bridge_release_bound_to_prepared_request_v1'
            or launch.get('attempt_id') != attempt or launch.get('request_path') != str(prepared.relative_to(root))
            or launch.get('request_sha256') != hashlib.sha256(prepared_raw).hexdigest()
            or digest not in REVIEWED_PLAN_BRIDGES or launch.get('bridge_release_path') != str(release.relative_to(root))
            or release.is_symlink() or not release.is_file() or release.stat().st_size > 256*1024
            or hashlib.sha256(release.read_bytes()).hexdigest() != digest):
        return False
    if dry_run:
        return True
    audit_path = root / 'diagnostics' / (attempt + '-host-reboot-quarantine-' + uuid.uuid4().hex + '.json')
    audit = {'contract': CONTRACT, 'task_id': task_id, 'attempt_id': attempt,
             'observed_at_utc': now.isoformat(), 'windows_boot_at_utc': boot_at.isoformat(),
             'previous_linux_boot_id': old_boot, 'current_linux_boot_id': current_boot,
             'original_worker': worker, 'original_traffic': event,
             'prepared_request_sha256': hashlib.sha256(prepared_raw).hexdigest(),
             'launch_manifest_sha256': hashlib.sha256(manifest_raw).hexdigest(),
             'bridge_script_sha256': digest, 'original_outcome': 'unknown_not_adopted',
             'task_requeued': False, 'provider_queries_sent': 0, 'source_rows_adopted': False,
             'original_evidence_preserved': True}
    atomic_write_json(audit_path, audit, durable=True)
    with closing(connect(root)) as con, con:
        current = con.execute('SELECT * FROM tasks WHERE task_id=?', (task_id,)).fetchone()
        current_event = con.execute('SELECT * FROM traffic WHERE event_id=?', (event['event_id'],)).fetchone()
        if (current is None or dict(current) != task or current_event is None or dict(current_event) != event
                or worker_path.read_bytes() != worker_raw or prepared.read_bytes() != prepared_raw
                or manifest.read_bytes() != manifest_raw
                or any(path.exists() or path.is_symlink() for path in
                       (raw, raw.with_suffix('.json.stage.json'), raw.with_suffix('.json.outcome.json')))):
            raise ValueError('Interrupted metadata evidence changed before quarantine')
        con.execute("UPDATE tasks SET state='blocked',last_error_code=? WHERE task_id=?", (QUARANTINE, task_id))
        con.execute("UPDATE traffic SET state='unknown_after_worker_loss' WHERE event_id=?", (event['event_id'],))
        con.execute('INSERT INTO meta VALUES (?,?)', ('rebooted_metadata_quarantine.' + attempt,
                    json.dumps({'audit_path': str(audit_path.relative_to(root)), **audit})))
    atomic_write_json(worker_path, {'contract_version': 4, 'state': QUARANTINE, 'task_id': task_id,
                      'observed_at_utc': now.isoformat(), 'audit_path': str(audit_path.relative_to(root))}, durable=True)
    return True
