#!/usr/bin/env python3
"""Arm one bounded, read-only menu capture on the existing TEJ owner.

Registration sends no provider query and does not authorize Preview, admit a
historical universe, or bypass the normal queue. The owner consumes this exact
intent at most once while executing the already-prioritized discovery task.
"""
from __future__ import annotations

import argparse
from contextlib import closing
from datetime import UTC, datetime, timedelta
import json
import os
from pathlib import Path
import re
import sqlite3
import sys
import uuid

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.tej_capture_claims import require_reconciled_failures
from downloader.provider_priority import inspect
from downloader.tej_history import (CONTRACT_VERSION, DESKTOP_INPUT_CONTRACT,
                                    DesktopBridge, task_request)
from scripts.check_acquisition_capacity import priority_dispatch_status

SAMPLE = re.compile(r'^([0-9a-f]{24})-([0-9a-f]{32})\.json\.company-axis-sample\.json$')
INTENT = 'tej_large_company_axis_capture_once_v1'
MAX_CAPTURE_BYTES = 256 * 1024 * 1024
# Use the same finite-priority fluctuation buffer as other bounded source work;
# MAX_CAPTURE_BYTES is checked separately against both measured volumes.
PRIORITY_BUFFER = 24 * 1024 ** 3


def register(root: Path, sample_path: Path, priority_id: str) -> dict:
    root = root.resolve(strict=True)
    sample_path = sample_path.absolute()
    match = SAMPLE.fullmatch(sample_path.name)
    if (not match or sample_path.is_symlink() or not sample_path.is_file()
            or sample_path.parent.resolve(strict=True) != (root / 'raw').resolve(strict=True)
            or sample_path.stat().st_size > 65536):
        raise ValueError('Exact retained native sample required')
    task_id, attempt_id = match.group(1), match.group(1) + '-' + match.group(2)
    sample = json.loads(sample_path.read_text(encoding='utf-8-sig'))
    request_path = root / 'requests' / (attempt_id + '.json')
    request_sha = sha256_file(request_path)
    request = json.loads(request_path.read_text(encoding='utf-8-sig'))
    DesktopBridge.bound_script_release(root, attempt_id, request_path)
    if (sample.get('contract') != 'large_company_axis_diagnostic_only_v1'
            or sample.get('task_id') != task_id or sample.get('prepared_request_sha256') != request_sha
            or sample.get('market_data_query_submitted') is not False
            or sample.get('full_axis_admitted') is not False
            or sample.get('source_scope_proof', {}).get('binding_readback_verified') is not True
            or sample.get('source_scope_proof', {}).get('vendor_notices_absent') is not True
            or type(sample.get('available_count')) is not int
            or not 150000 < sample['available_count'] <= 2000000
            or any(sample.get(key) != request.get(key) for key in ('type', 'smart_id', 'table'))
            or request.get('action') != 'plan' or request.get('type') != 'Option (ALL)'
            or request.get('smart_id') != 'TEJ Derivatives'):
        raise ValueError('Sample source, count or no-query binding differs')
    priority = inspect(root, priority_id)
    if priority.get('provider') != 'tej' or priority.get('priority_active') is not True:
        raise ValueError('Active canonical TEJ priority required')
    now = datetime.now(UTC)
    with closing(sqlite3.connect(f'file:{root / "queue.sqlite3"}?mode=ro', uri=True, timeout=5)) as con:
        con.row_factory = sqlite3.Row
        task = con.execute('SELECT * FROM tasks WHERE task_id=?', (task_id,)).fetchone()
        intent = con.execute('SELECT 1 FROM provider_query_intents WHERE request_id=? AND task_key=? '
                             'AND created_at_utc<=? AND expires_at_utc>?',
                             (priority_id, task_id, now.isoformat(), now.isoformat())).fetchone()
        if task is None or task['kind'] != 'discover' or task['state'] not in ('pending', 'running') or not intent:
            raise ValueError('Exact existing prioritized discovery task is not ready')
        current = task_request(root, dict(task), connection=con)
    current = {**current, 'contract_version': CONTRACT_VERSION, 'task_id': task_id,
               'desktop_input_contract': DESKTOP_INPUT_CONTRACT}
    if request != current:
        raise ValueError('Registered source scope changed since the sample')
    capacity = priority_dispatch_status(root, additional_headroom_bytes=PRIORITY_BUFFER)
    backing = capacity.get('backing_volume', {})
    if (capacity.get('state') != 'admitted'
            or type(capacity.get('available_bytes')) is not int
            or type(capacity.get('required_bytes')) is not int
            or capacity['available_bytes'] < capacity['required_bytes'] + MAX_CAPTURE_BYTES
            or type(backing.get('physical_free_bytes')) is not int
            or type(backing.get('required_bytes')) is not int
            or backing['physical_free_bytes'] < backing['required_bytes'] + MAX_CAPTURE_BYTES):
        raise ValueError('Physical backing and capture headroom are not admitted')
    expires = min(now + timedelta(minutes=12), datetime.fromisoformat(priority['expires_at_utc']))
    script = REPO / 'scripts/tej_smart_wizard_bridge.ps1'
    if script.is_symlink() or not script.is_file() or script.stat().st_size > 256 * 1024:
        raise ValueError('Bounded canonical bridge source required')
    intent_dir = root / 'large_axis_capture_intents'
    claimed_dir = intent_dir / 'claimed'
    intent_dir.mkdir(mode=0o700, exist_ok=True)
    if intent_dir.is_symlink():
        raise ValueError('Capture intent directory redirected')
    claimed_dir.mkdir(mode=0o700, exist_ok=True)
    if claimed_dir.is_symlink():
        raise ValueError('Capture intent directory redirected')
    require_reconciled_failures(root, claimed_dir, task_id, 'large')
    destination = intent_dir / (task_id + '.json')
    body = {'contract': INTENT, 'task_id': task_id, 'priority_request_id': priority_id,
            'type': request['type'], 'smart_id': request['smart_id'], 'table': request['table'],
            'prepared_request_sha256': request_sha, 'bridge_script_sha256': sha256_file(script),
            'prior_sample_basename': sample_path.name, 'prior_sample_sha256': sha256_file(sample_path),
            'expected_count': sample['available_count'], 'created_at_utc': now.isoformat(),
            'expires_at_utc': expires.isoformat(), 'capacity_checked_at_utc': now.isoformat(),
            'capacity': capacity, 'maximum_capture_bytes': MAX_CAPTURE_BYTES,
            'provider_queries_by_registration': 0, 'market_data_query_authorized': False}
    temporary = intent_dir / ('.' + task_id + '-' + uuid.uuid4().hex + '.tmp')
    try:
        atomic_write_json(temporary, body, durable=True)
        intent_sha = sha256_file(temporary)
        os.link(temporary, destination, follow_symlinks=False)  # exclusive, fully written publication
    finally:
        temporary.unlink(missing_ok=True)
    return {'intent_path': str(destination), 'intent_sha256': intent_sha,
            'task_id': task_id, 'expires_at_utc': expires.isoformat(),
            'source_calls': 0, 'source_data_admitted': False}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=REPO / 'data_tej')
    parser.add_argument('--sample', type=Path, required=True)
    parser.add_argument('--priority-request-id', required=True)
    args = parser.parse_args()
    print(json.dumps(register(args.root, args.sample, args.priority_request_id), ensure_ascii=False))


if __name__ == '__main__':
    main()
