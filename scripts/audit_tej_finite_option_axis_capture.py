#!/usr/bin/env python3
"""Read back a finite, source-bound TEJ option-menu observation.

This independently verifies metadata only; it never admits a market price,
historical eligibility, a complete source universe, or a training input.
"""
from __future__ import annotations

import argparse
from datetime import UTC, date, datetime
import json
from pathlib import Path
import re
import sys

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.tej_company_axis_snapshot import iter_snapshot
from downloader.tej_history import DesktopBridge, normalize_period
from scripts.audit_tej_large_axis_capture import audit as audit_snapshot
from scripts.register_tej_finite_option_axis_capture import INTENT, candidate_labels

NAME = re.compile(r'^([0-9a-f]{24}-[0-9a-f]{32})\.json\.finite-axis\.json$')


def audit(root: Path, observation_path: Path) -> dict:
    root = root.resolve(strict=True)
    observation_path = observation_path.absolute()
    match = NAME.fullmatch(observation_path.name)
    if (not match or observation_path.is_symlink() or not observation_path.is_file()
            or observation_path.parent.resolve(strict=True) != (root / 'raw').resolve(strict=True)
            or observation_path.stat().st_size > 256 * 1024):
        raise ValueError('Exact retained finite observation required')
    attempt = match.group(1)
    task_id = attempt[:24]
    observation = json.loads(observation_path.read_text(encoding='utf-8-sig'))
    request_path = root / 'requests' / (attempt + '.json')
    _, release_sha = DesktopBridge.bound_script_release(root, attempt, request_path)
    request = json.loads(request_path.read_text(encoding='utf-8-sig'))
    claim = root / 'finite_axis_capture_intents' / 'claimed' / (attempt + '.json')
    if claim.is_symlink() or not claim.is_file() or claim.stat().st_size > 256 * 1024:
        raise ValueError('Exact immutable finite intent claim required')
    intent = json.loads(claim.read_text(encoding='utf-8-sig'))
    request_sha = sha256_file(request_path)
    if (observation.get('contract') != 'tej_finite_company_date_axis_observation_v1'
            or observation.get('task_id') != task_id or observation.get('query_attempt_id') != attempt
            or observation.get('market_data_query_submitted') is not False
            or observation.get('full_axis_admitted') is not False
            or observation.get('complete_live_company_reads') != 2
            or observation.get('complete_date_reads') != 2
            or type(observation.get('source_key_mode')) is not int
            or observation.get('source_key_mode') not in (2, 3)
            or not all(observation.get(side, {}).get('binding_readback_verified') is True
                       and observation.get(side, {}).get('vendor_notices_absent') is True
                       for side in ('before_scope', 'after_scope'))
            or observation.get('prepared_request_sha256') != request_sha
            or observation.get('bridge_script_sha256') != release_sha
            or observation.get('intent_sha256') != sha256_file(claim)
            or intent.get('contract') != INTENT or intent.get('task_id') != task_id
            or intent.get('market_data_query_authorized') is not False
            or intent.get('provider_queries_by_registration') != 0
            or intent.get('prepared_request_sha256') != request_sha
            or intent.get('bridge_script_sha256') != release_sha
            or intent.get('priority_request_id') != observation.get('priority_request_id')
            or request.get('action') != 'plan' or request.get('frequency') != 'daily'
            or request.get('task_id') != task_id
            or any(observation.get(k) != request.get(k) or intent.get(k) != request.get(k)
                   for k in ('type', 'smart_id', 'table'))
            or observation.get('fields') != request.get('fields')):
        raise ValueError('Finite observation, source binding or one-shot claim differs')
    observed = datetime.fromisoformat(observation['observed_at_utc'].replace('Z', '+00:00'))
    created = datetime.fromisoformat(intent['created_at_utc'].replace('Z', '+00:00'))
    expires = datetime.fromisoformat(intent['expires_at_utc'].replace('Z', '+00:00'))
    if (not created.tzinfo or not expires.tzinfo or not observed.tzinfo or observed < created
            or not 0 < (expires - created).total_seconds() <= 720):
        raise ValueError('Finite observation timestamps differ')
    snapshot_path = Path(intent['snapshot_manifest_path'])
    if sha256_file(snapshot_path) != intent.get('snapshot_manifest_sha256'):
        raise ValueError('Prior menu manifest changed')
    snapshot = audit_snapshot(root, snapshot_path)
    day = date.fromisoformat(intent['date'])
    labels, candidate_sha = candidate_labels(Path(intent['candidate_manifest_path']), snapshot, day)
    indices = observation.get('company_indices')
    dates = observation.get('date_labels')
    if (snapshot['task_id'] != task_id or snapshot['table'] != request['table']
            or snapshot['source_proof']['sha256'] != intent.get('prior_snapshot_sha256')
            or observation.get('prior_snapshot_sha256') != intent.get('prior_snapshot_sha256')
            or candidate_sha != intent.get('candidate_manifest_sha256')
            or observation.get('candidate_manifest_sha256') != candidate_sha
            or observation.get('available_company_count') != snapshot['source_proof']['count']
            or intent.get('expected_count') != snapshot['source_proof']['count']
            or labels != intent.get('company_labels') or labels != observation.get('company_labels')
            or not isinstance(indices, list) or len(indices) != len(labels)
            or any(type(i) is not int or i < 0 or i >= intent['expected_count'] for i in indices)
            or len(set(indices)) != len(indices)
            or not request['start'] <= day.isoformat() <= request['end']
            or observation.get('date_search_start') != day.isoformat()
            or observation.get('date_search_end') != day.isoformat()
            or not isinstance(dates, list) or len(dates) > 1
            or any(normalize_period(value) != day.isoformat() for value in dates)):
        raise ValueError('Finite menu labels, indices, date or source evidence differs')
    positions = dict(zip(indices, labels, strict=True))
    observed_positions = {}
    for index, label in enumerate(iter_snapshot(Path(snapshot['snapshot_path']))):
        if index in positions:
            observed_positions[index] = label
            if len(observed_positions) == len(positions):
                break
    if observed_positions != positions:
        raise ValueError('Retained and live native menu positions differ')
    return {'contract': 'tej_finite_company_date_axis_independent_readback_v1',
            'observed_at_utc': datetime.now(UTC).isoformat(), 'task_id': task_id,
            'query_attempt_id': attempt, 'type': request['type'], 'smart_id': request['smart_id'],
            'table': request['table'], 'date': day.isoformat(), 'company_labels': len(labels),
            'source_key_mode': observation['source_key_mode'],
            'date_menu_contains_day': bool(dates), 'observation_path': str(observation_path),
            'observation_sha256': sha256_file(observation_path), 'claimed_intent_sha256': sha256_file(claim),
            'candidate_manifest_sha256': candidate_sha, 'snapshot_sha256': snapshot['source_proof']['sha256'],
            'provider_queries_by_audit': 0, 'market_observations_obtained': False,
            'full_axis_admitted_to_plan': False, 'training_input_admitted': False,
            'limitations': ['Native menu metadata only: no historical price, availability or execution proof.',
                            'Absent date menu is not proof that the underlying market had no trades.']}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=REPO / 'data_tej')
    parser.add_argument('--observation', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise FileExistsError('Retain the existing independent readback')
    receipt = audit(args.root, args.observation)
    atomic_write_json(args.output, receipt, durable=True)
    print(json.dumps(receipt, ensure_ascii=False))


if __name__ == '__main__':
    main()
