#!/usr/bin/env python3
"""Wait for one existing TEJ owner task; arm and audit one read-only capture.

This process never opens a vendor session, changes the task scheduler, or
submits Preview. It expires finite local intent rather than bypassing the
owner, capacity checks or original source scope.
"""
from __future__ import annotations

import argparse
from datetime import UTC, datetime, timedelta
import json
from pathlib import Path
import re
import sys
import time

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.tej_capture_claims import find_capture_claim, verified_unsent_failure
from downloader.tej_history import DesktopBridge
from downloader.tej_startup import owner_alive
from scripts.audit_tej_large_axis_capture import audit
from scripts.register_tej_large_axis_capture import SAMPLE, register

MAX_SAFE_FAILURE_RETRIES = 3


def wait(root: Path, sample: Path, priority_id: str, output: Path, hours: float) -> int:
    if not 0 < hours <= 24 or output.exists():
        raise ValueError('Finite wait and new independent receipt required')
    root, sample = root.resolve(strict=True), sample.absolute()
    match = SAMPLE.fullmatch(sample.name)
    if not match:
        raise ValueError('Exact prior native sample required')
    task_id = match.group(1)
    tracked = [REPO / 'scripts/tej_smart_wizard_bridge.ps1',
               REPO / 'scripts/register_tej_large_axis_capture.py',
               REPO / 'scripts/audit_tej_large_axis_capture.py',
               REPO / 'downloader/tej_company_axis_snapshot.py',
               REPO / 'downloader/tej_capture_claims.py', sample]
    pins = {path: sha256_file(path) for path in tracked}
    deadline = datetime.now(UTC) + timedelta(hours=hours)
    intent_path = root / 'large_axis_capture_intents' / (task_id + '.json')
    if intent_path.exists():
        raise ValueError('An existing capture intent needs reconciliation')
    armed = None
    failed_attempts = set()
    last_status = 0.0
    print(json.dumps({'state': 'waiting_for_exact_existing_owner', 'task_id': task_id,
                      'deadline_utc': deadline.isoformat(), 'source_queries': 0}), flush=True)
    while datetime.now(UTC) < deadline:
        if any(sha256_file(path) != digest for path, digest in pins.items()):
            raise ValueError('Pinned capture implementation or native sample changed')
        try:
            worker = json.loads((root / 'worker_status.json').read_text())
        except (OSError, ValueError):
            worker = {}
        attempt = worker.get('bridge_attempt_id')
        live = (worker.get('state') == 'running' and worker.get('kind') == 'discover'
                and worker.get('task_id') == task_id and isinstance(attempt, str)
                and re.fullmatch(re.escape(task_id) + r'-[0-9a-f]{32}', attempt)
                and owner_alive(worker))
        if armed is None and live and attempt not in failed_attempts:
            request = root / 'requests' / (attempt + '.json')
            try:
                _, release_sha = DesktopBridge.bound_script_release(root, attempt, request)
            except (OSError, ValueError):
                release_sha = None
            if release_sha == pins[tracked[0]]:
                try:
                    result = register(root, sample, priority_id)
                except ValueError as exc:
                    print(json.dumps({'state': 'registration_deferred', 'reason': str(exc),
                                      'source_queries': 0}), flush=True)
                    time.sleep(30)
                    continue
                armed = {'sha256': result['intent_sha256'], 'expires': datetime.fromisoformat(result['expires_at_utc']),
                         'attempt': attempt}
                print(json.dumps({'state': 'armed_for_existing_owner', 'attempt_id': attempt,
                                  'expires_utc': armed['expires'].isoformat(), 'source_queries': 0}), flush=True)
        if armed is not None:
            claim = find_capture_claim(intent_path.parent / 'claimed', task_id, armed['sha256'], armed['attempt'])
            if claim is not None:
                claim_attempt = claim.stem
                manifest = root / 'raw' / (claim_attempt + '.json.company-axis.snapshot.json')
                if manifest.is_file():
                    receipt = audit(root, manifest)
                    atomic_write_json(output, receipt, durable=True)
                    print(json.dumps({'state': 'independently_read_back', 'receipt': str(output),
                                      'labels': receipt['source_proof']['count'], 'source_queries': 0,
                                      'training_admitted': False}), flush=True)
                    return 0
                try:
                    outcome = verified_unsent_failure(root, claim, 'large')
                except ValueError:
                    if manifest.is_file():
                        continue
                    raise
                if outcome is not None:
                    if manifest.is_file():
                        continue
                    failed_attempts.add(claim_attempt)
                    print(json.dumps({'state': 'capture_failed_before_preview_retained',
                                      'outcome': str(outcome), 'source_queries': 0,
                                      'source_data_admitted': False, 'safe_failures': len(failed_attempts)}), flush=True)
                    if len(failed_attempts) >= MAX_SAFE_FAILURE_RETRIES:
                        return 75
                    armed = None
                    continue
            elif datetime.now(UTC) > armed['expires'] and intent_path.is_file():
                try:
                    if sha256_file(intent_path) != armed['sha256']:
                        raise ValueError('Expired unclaimed capture intent changed')
                    expired = intent_path.parent / 'expired'
                    expired.mkdir(mode=0o700, exist_ok=True)
                    target = expired / (task_id + '-' + armed['sha256'] + '.json')
                    if target.exists():
                        raise FileExistsError('Expired intent evidence already exists')
                    intent_path.rename(target)
                except FileNotFoundError:
                    # The owner may have atomically claimed it after the
                    # previous scan. Re-read that claim; never replace it.
                    continue
                print(json.dumps({'state': 'expired_unclaimed_retained', 'intent': str(target),
                                  'source_queries': 0}), flush=True)
                armed = None
        if time.monotonic() - last_status >= 60:
            print(json.dumps({'state': 'waiting_for_capture' if armed else 'waiting_for_owner',
                              'task_id': task_id, 'source_queries': 0}), flush=True)
            last_status = time.monotonic()
        time.sleep(5)
    print(json.dumps({'state': 'finite_wait_expired', 'source_queries': 0,
                      'source_data_admitted': False}), flush=True)
    return 75


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=REPO / 'data_tej')
    parser.add_argument('--sample', type=Path, required=True)
    parser.add_argument('--priority-request-id', required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--hours', type=float, default=6)
    args = parser.parse_args()
    raise SystemExit(wait(args.root, args.sample, args.priority_request_id, args.output, args.hours))


if __name__ == '__main__':
    main()
