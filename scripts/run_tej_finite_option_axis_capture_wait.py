#!/usr/bin/env python3
"""Attach one finite option-menu observation to the existing TEJ owner.

This watcher never creates an owner, submits a market-data query, or adopts a
partial source menu as full discovery. It retains expired and failed claims.
"""
from __future__ import annotations

import argparse
from datetime import UTC, date, datetime, timedelta
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
from scripts.audit_tej_finite_option_axis_capture import audit
from scripts.audit_tej_large_axis_capture import NAME as SNAPSHOT_NAME
from scripts.register_tej_finite_option_axis_capture import register

MAX_SAFE_FAILURE_RETRIES = 3


def verified_plan_failure(root: Path, attempt: str, bridge_sha: str) -> Path | None:
    """Recognize only the exact owner's attested metadata-only failure.

    A date-selection error can occur after the finite intent is claimed but
    before its dedicated readback is written. The owner's original plan
    outcome is evidence of that failure, not a successful source capture.
    """
    outcome_path = root / 'raw' / (attempt + '.json.outcome.json')
    if not outcome_path.is_file():
        return None
    if outcome_path.is_symlink() or outcome_path.stat().st_size > 4096:
        raise ValueError('Invalid metadata outcome evidence')
    request_path = root / 'requests' / (attempt + '.json')
    _, actual_sha = DesktopBridge.bound_script_release(root, attempt, request_path)
    request = json.loads(request_path.read_text(encoding='utf-8-sig'))
    outcome = json.loads(outcome_path.read_text(encoding='utf-8-sig'))
    task_id = attempt[:24]
    if (actual_sha != bridge_sha or request.get('task_id') != task_id
            or request.get('action') != 'plan'
            or outcome.get('contract_version') != 4
            or outcome.get('provider') != 'tej_smart_wizard'
            or outcome.get('task_id') != task_id or outcome.get('action') != 'plan'
            or outcome.get('market_data_query_submission_possible') is not False
            or outcome.get('error_code') != 'metadata_preparation_failed_before_preview'
            or any(outcome.get(key) != request.get(key) for key in ('type', 'smart_id', 'table'))):
        raise ValueError('Metadata failure does not match the exact source attempt')
    return outcome_path


def retire_unclaimed_failure(root: Path, intent_path: Path, attempt: str,
                             intent_sha: str, bridge_sha: str) -> Path | None:
    """Retain an exact unclaimed intent only after two pre-Preview proofs.

    The native failure alone is not a query outcome; the matching plan outcome,
    request, immutable bridge and intent are also required. An atomic rename
    races safely with a native claim: if the owner wins, let normal claim
    reconciliation handle it. No source request is submitted here.
    """
    if verified_plan_failure(root, attempt, bridge_sha) is None:
        return None
    task_id = attempt[:24]
    failure_path = root / 'raw' / (attempt + '.json.finite-axis-failure.json')
    if not failure_path.exists():
        return None
    if failure_path.is_symlink() or not failure_path.is_file() or failure_path.stat().st_size > 4096:
        raise ValueError('Invalid unclaimed failure evidence')
    failure = json.loads(failure_path.read_text(encoding='utf-8-sig'))
    if (failure.get('contract') != 'tej_finite_company_date_axis_capture_failure_v1'
            or failure.get('task_id') != task_id or failure.get('query_attempt_id') != attempt
            or failure.get('intent_claimed') is not False
            or failure.get('market_data_query_submitted') is not False
            or failure.get('full_axis_admitted') is not False):
        raise ValueError('Invalid unclaimed failure evidence')
    if not intent_path.exists():
        return None  # A native claim may have won the race.
    if (intent_path.is_symlink() or not intent_path.is_file()
            or intent_path.stat().st_size > 256 * 1024 or sha256_file(intent_path) != intent_sha):
        raise ValueError('Unclaimed intent identity changed')
    request_path = root / 'requests' / (attempt + '.json')
    request = json.loads(request_path.read_text(encoding='utf-8-sig'))
    intent = json.loads(intent_path.read_text(encoding='utf-8-sig'))
    if (intent.get('contract') != 'tej_finite_company_date_axis_capture_once_v1'
            or intent.get('task_id') != task_id
            or intent.get('prepared_request_sha256') != sha256_file(request_path)
            or intent.get('bridge_script_sha256') != bridge_sha
            or intent.get('market_data_query_authorized') is not False
            or intent.get('provider_queries_by_registration') != 0
            or any(intent.get(key) != request.get(key) for key in ('type', 'smart_id', 'table'))):
        raise ValueError('Unclaimed failure does not match the exact source intent')
    claim_dir = intent_path.parent / 'claimed'
    if find_capture_claim(claim_dir, task_id, intent_sha, attempt) is not None:
        raise ValueError('An unclaimed intent also has a native claim')
    retained = intent_path.parent / 'unclaimed_failed'
    retained.mkdir(mode=0o700, exist_ok=True)
    if retained.is_symlink():
        raise ValueError('Unclaimed failure retention path is redirected')
    target = retained / (task_id + '-' + intent_sha + '.json')
    if target.exists() or target.is_symlink():
        raise FileExistsError('Unclaimed failure evidence already exists')
    try:
        intent_path.rename(target)
    except FileNotFoundError:
        return None  # The native owner may have atomically claimed it.
    if sha256_file(target) != intent_sha or find_capture_claim(claim_dir, task_id, intent_sha, attempt) is not None:
        raise ValueError('Unclaimed failure retention is inconsistent')
    return target


def wait(root: Path, snapshot: Path, candidates: Path, priority_id: str,
         day: date, output: Path, hours: float) -> int:
    if not 0 < hours <= 24 or output.exists():
        raise ValueError('Finite wait and new independent receipt required')
    root, snapshot, candidates = root.resolve(strict=True), snapshot.absolute(), candidates.absolute()
    match = SNAPSHOT_NAME.fullmatch(snapshot.name)
    if not match:
        raise ValueError('Exact prior native menu snapshot required')
    task_id = match.group(1)[:24]
    tracked = [REPO / 'scripts/tej_smart_wizard_bridge.ps1',
               REPO / 'scripts/register_tej_finite_option_axis_capture.py',
               REPO / 'scripts/audit_tej_finite_option_axis_capture.py',
               REPO / 'scripts/audit_tej_large_axis_capture.py',
               REPO / 'downloader/tej_company_axis_snapshot.py',
               REPO / 'downloader/tej_capture_claims.py', snapshot, candidates]
    pins = {path: sha256_file(path) for path in tracked}
    deadline = datetime.now(UTC) + timedelta(hours=hours)
    intent_path = root / 'finite_axis_capture_intents' / (task_id + '.json')
    if intent_path.exists():
        raise ValueError('An existing finite capture intent needs reconciliation')
    armed = None
    failed_attempts = set()
    last_status = 0.0
    print(json.dumps({'state': 'waiting_for_exact_existing_owner', 'task_id': task_id,
                      'date': day.isoformat(), 'deadline_utc': deadline.isoformat(),
                      'market_data_queries': 0}), flush=True)
    while datetime.now(UTC) < deadline:
        if any(sha256_file(path) != digest for path, digest in pins.items()):
            raise ValueError('Pinned capture implementation or source evidence changed')
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
                    result = register(root, snapshot, candidates, priority_id, day)
                except ValueError as exc:
                    print(json.dumps({'state': 'registration_deferred', 'reason': str(exc),
                                      'market_data_queries': 0}), flush=True)
                    time.sleep(30)
                    continue
                armed = {'sha256': result['intent_sha256'],
                         'expires': datetime.fromisoformat(result['expires_at_utc']), 'attempt': attempt}
                print(json.dumps({'state': 'armed_for_existing_owner', 'attempt_id': attempt,
                                  'labels': result['labels'], 'expires_utc': armed['expires'].isoformat(),
                                  'market_data_queries': 0}), flush=True)
        if armed is not None:
            claim = find_capture_claim(intent_path.parent / 'claimed', task_id, armed['sha256'], armed['attempt'])
            if claim is not None:
                attempt = claim.stem
                observation = root / 'raw' / (attempt + '.json.finite-axis.json')
                if observation.is_file():
                    receipt = audit(root, observation)
                    atomic_write_json(output, receipt, durable=True)
                    print(json.dumps({'state': 'independently_read_back', 'receipt': str(output),
                                      'labels': receipt['company_labels'], 'market_data_queries': 0,
                                      'training_admitted': False}), flush=True)
                    return 0
                outcome = verified_plan_failure(root, attempt, pins[tracked[0]])
                if outcome is not None:
                    # Successful capture writes its readback before the owner
                    # unwinds the metadata-only plan; handle that race first.
                    if observation.is_file():
                        continue
                    try:
                        verified = verified_unsent_failure(root, claim, 'finite')
                    except ValueError:
                        if observation.is_file():
                            continue
                        raise
                    if verified != outcome:
                        raise ValueError('Claim and pre-Preview outcome differ')
                    failed_attempts.add(attempt)
                    print(json.dumps({'state': 'capture_failed_before_preview_retained',
                                      'outcome': str(outcome), 'market_data_queries': 0,
                                      'source_data_admitted': False, 'safe_failures': len(failed_attempts)}), flush=True)
                    if len(failed_attempts) >= MAX_SAFE_FAILURE_RETRIES:
                        return 75
                    armed = None
                    continue
            elif not (live and attempt != armed['attempt']):
                retained = retire_unclaimed_failure(root, intent_path, armed['attempt'],
                                                   armed['sha256'], pins[tracked[0]])
                if retained is not None:
                    failed_attempts.add(armed['attempt'])
                    print(json.dumps({'state': 'unclaimed_failure_before_preview_retained',
                                      'intent': str(retained), 'market_data_queries': 0,
                                      'source_data_admitted': False, 'safe_failures': len(failed_attempts)}), flush=True)
                    if len(failed_attempts) >= MAX_SAFE_FAILURE_RETRIES:
                        return 75
                    armed = None
                    continue
            if armed is not None and claim is None and datetime.now(UTC) > armed['expires'] and intent_path.is_file():
                try:
                    if sha256_file(intent_path) != armed['sha256']:
                        raise ValueError('Expired finite intent changed')
                    expired = intent_path.parent / 'expired'
                    expired.mkdir(mode=0o700, exist_ok=True)
                    target = expired / (task_id + '-' + armed['sha256'] + '.json')
                    if target.exists():
                        raise FileExistsError('Expired finite evidence already exists')
                    intent_path.rename(target)
                except FileNotFoundError:
                    continue  # Owner may just have atomically claimed it.
                print(json.dumps({'state': 'expired_unclaimed_retained', 'intent': str(target),
                                  'market_data_queries': 0}), flush=True)
                armed = None
        if time.monotonic() - last_status >= 60:
            print(json.dumps({'state': 'waiting_for_capture' if armed else 'waiting_for_owner',
                              'task_id': task_id, 'market_data_queries': 0}), flush=True)
            last_status = time.monotonic()
        time.sleep(5)
    print(json.dumps({'state': 'finite_wait_expired', 'market_data_queries': 0,
                      'source_data_admitted': False}), flush=True)
    return 75


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=REPO / 'data_tej')
    parser.add_argument('--snapshot-manifest', type=Path, required=True)
    parser.add_argument('--candidate-manifest', type=Path, required=True)
    parser.add_argument('--priority-request-id', required=True)
    parser.add_argument('--date', type=date.fromisoformat, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--hours', type=float, default=6)
    args = parser.parse_args()
    raise SystemExit(wait(args.root, args.snapshot_manifest, args.candidate_manifest,
                          args.priority_request_id, args.date, args.output, args.hours))


if __name__ == '__main__':
    main()
