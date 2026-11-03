#!/usr/bin/env python3
"""Arm one read-only company/date metadata observation on the TEJ owner.

The intent is finite, tied to an existing prioritized discovery and a retained
official-gap candidate manifest. It submits no Preview and admits no data into
the normal full-discovery plan or training.
"""
from __future__ import annotations

import argparse
from contextlib import closing
from datetime import UTC, date, datetime, timedelta
import json
import os
from pathlib import Path
import re
import sqlite3
import sys
import uuid

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

import polars as pl

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.tej_capture_claims import require_reconciled_failures
from downloader.provider_priority import inspect
from downloader.tej_company_axis_snapshot import iter_snapshot
from downloader.tej_history import CONTRACT_VERSION, DESKTOP_INPUT_CONTRACT, task_request
from scripts.audit_tej_large_axis_capture import audit as audit_snapshot
from scripts.check_acquisition_capacity import priority_dispatch_status

INTENT = 'tej_finite_company_date_axis_capture_once_v1'
HEADROOM = 2 * 1024 * 1024
# Preserve the documented finite-priority control/ledger reserve and the
# observed Windows backing-volume fluctuation buffer. This exception only
# arms bounded, source-pinned menu metadata on an already selected task.
PRIORITY_BUFFER = 24 * 1024 ** 3


def _regular(path: Path, root: Path, maximum: int | None = None) -> Path:
    if path.is_symlink() or not path.is_file():
        raise ValueError('Retained regular source file required')
    resolved = path.resolve(strict=True)
    if not resolved.is_relative_to(root.resolve(strict=True)) or maximum is not None and path.stat().st_size > maximum:
        raise ValueError('Source path or size differs from the retained scope')
    return resolved


def candidate_labels(manifest_path: Path, snapshot: dict, day: date) -> tuple[list[str], str]:
    """Verify candidate provenance and exact source labels without a query."""
    manifest_path = _regular(manifest_path, REPO / 'data_tw_index_options_daily', 1024 * 1024)
    manifest = json.loads(manifest_path.read_text(encoding='utf-8-sig'))
    if (manifest.get('contract') != 'tej_option_official_gap_menu_candidates_v1'
            or manifest.get('source_table') != snapshot['table']
            or manifest.get('provider_queries') != 0 or manifest.get('market_observations_obtained') is not False
            or manifest.get('full_axis_admitted_to_plan') is not False or manifest.get('training_ready') is not False):
        raise ValueError('Official gap candidate evidence is incompatible')
    match = re.fullmatch(r'Delisted Option\((\d{4})~(\d{4})\)', snapshot['table'])
    if not match or not int(match.group(1)) <= day.year <= int(match.group(2)):
        raise ValueError('Candidate day is outside this table label; source coverage remains unproved')
    data = _regular(Path(manifest['data_path']), REPO / 'data_tw_index_options_daily')
    if sha256_file(data) != manifest.get('data_sha256'):
        raise ValueError('Candidate data fingerprint changed')
    for name, expected in manifest.get('source_pins', {}).items():
        path = _regular(Path(name), REPO)
        if sha256_file(path) != expected:
            raise ValueError('Candidate source fingerprint changed')
    if not manifest.get('implementation_sha256'):
        raise ValueError('Candidate implementation evidence is missing')
    for name, expected in manifest['implementation_sha256'].items():
        if sha256_file(_regular(Path(name), REPO)) != expected:
            raise ValueError('Candidate implementation fingerprint changed')
    binary = Path(snapshot['snapshot_path']).resolve(strict=True)
    if manifest.get('source_pins', {}).get(str(binary)) != snapshot['source_proof']['sha256']:
        raise ValueError('Candidate labels refer to a different TEJ snapshot')
    rows = pl.read_parquet(data).filter(pl.col('date') == day)
    if rows.height < 1:
        raise ValueError('No official candidates exist on this exact day')
    labels = []
    for row in rows.select('candidate_labels', 'candidate_count', 'status').iter_rows(named=True):
        if row['status'] != 'unique_label_candidate' or row['candidate_count'] != 1 or len(row['candidate_labels']) != 1:
            raise ValueError('Ambiguous official/source candidate needs separate reconciliation')
        labels.append(row['candidate_labels'][0])
    if not 1 <= len(labels) <= 2000 or len(set(labels)) != len(labels):
        raise ValueError('Finite native selector requires unique bounded labels')
    wanted = set(labels)
    observed = {label for label in iter_snapshot(binary) if label in wanted}
    if observed != wanted:
        raise ValueError('A candidate is absent from the retained native menu')
    return labels, sha256_file(manifest_path)


def register(root: Path, snapshot_path: Path, candidate_manifest: Path,
             priority_id: str, day: date) -> dict:
    root = root.resolve(strict=True)
    snapshot = audit_snapshot(root, snapshot_path)
    task_id = snapshot['task_id']
    labels, candidate_sha = candidate_labels(candidate_manifest, snapshot, day)
    if (snapshot['type'] != 'Option (ALL)' or snapshot['smart_id'] != 'TEJ Derivatives'
            or snapshot['source_proof']['duplicate_exact_labels'] != 0):
        raise ValueError('Exact unambiguous option menu proof required')
    priority = inspect(root, priority_id)
    if priority.get('provider') != 'tej' or priority.get('priority_active') is not True:
        raise ValueError('Active canonical TEJ priority required')
    now = datetime.now(UTC)
    attempt = snapshot['query_attempt_id']
    request_path = root / 'requests' / (attempt + '.json')
    request = json.loads(request_path.read_text(encoding='utf-8-sig'))
    with closing(sqlite3.connect((root / 'queue.sqlite3').as_uri() + '?mode=ro', uri=True, timeout=5)) as con:
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
    if (request != current or request.get('action') != 'plan' or request.get('frequency') != 'daily'
            or not request['start'] <= day.isoformat() <= request['end']):
        raise ValueError('Original native source, scope or date changed')
    capacity = priority_dispatch_status(root, additional_headroom_bytes=PRIORITY_BUFFER)
    backing = capacity.get('backing_volume', {})
    if (capacity.get('state') != 'admitted' or type(capacity.get('available_bytes')) is not int
            or type(capacity.get('required_bytes')) is not int
            or capacity['available_bytes'] < capacity['required_bytes'] + HEADROOM
            or type(backing.get('physical_free_bytes')) is not int or type(backing.get('required_bytes')) is not int
            or backing['physical_free_bytes'] < backing['required_bytes'] + HEADROOM):
        raise ValueError('Physical backing and finite metadata headroom are not admitted')
    script = _regular(REPO / 'scripts/tej_smart_wizard_bridge.ps1', REPO, 256 * 1024)
    expires = min(now + timedelta(minutes=12), datetime.fromisoformat(priority['expires_at_utc']))
    if expires <= datetime.now(UTC):
        raise ValueError('Priority expired before the finite capture was registered')
    intent_dir = root / 'finite_axis_capture_intents'
    intent_dir.mkdir(mode=0o700, exist_ok=True)
    claimed_dir = intent_dir / 'claimed'
    claimed_dir.mkdir(mode=0o700, exist_ok=True)
    if intent_dir.is_symlink() or claimed_dir.is_symlink():
        raise ValueError('A redirected finite-axis directory requires reconciliation')
    require_reconciled_failures(root, claimed_dir, task_id, 'finite')
    destination = intent_dir / (task_id + '.json')
    body = {'contract': INTENT, 'task_id': task_id, 'priority_request_id': priority_id,
            'type': request['type'], 'smart_id': request['smart_id'], 'table': request['table'],
            'prepared_request_sha256': sha256_file(request_path), 'bridge_script_sha256': sha256_file(script),
            'prior_snapshot_sha256': snapshot['source_proof']['sha256'],
            'snapshot_manifest_path': str(snapshot_path.resolve(strict=True)),
            'snapshot_manifest_sha256': snapshot['snapshot_manifest_sha256'],
            'candidate_manifest_path': str(candidate_manifest.resolve()), 'candidate_manifest_sha256': candidate_sha,
            'expected_count': snapshot['source_proof']['count'], 'date': day.isoformat(),
            'company_labels': labels, 'created_at_utc': now.isoformat(), 'expires_at_utc': expires.isoformat(),
            'capacity': capacity, 'capacity_checked_at_utc': datetime.now(UTC).isoformat(),
            'provider_queries_by_registration': 0, 'market_data_query_authorized': False}
    temporary = intent_dir / ('.' + task_id + '-' + uuid.uuid4().hex + '.tmp')
    try:
        atomic_write_json(temporary, body, durable=True)
        digest = sha256_file(temporary)
        os.link(temporary, destination, follow_symlinks=False)
    finally:
        temporary.unlink(missing_ok=True)
    return {'intent_path': str(destination), 'intent_sha256': digest, 'task_id': task_id,
            'date': day.isoformat(), 'labels': len(labels), 'expires_at_utc': expires.isoformat(),
            'source_calls': 0, 'source_data_admitted': False}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=REPO / 'data_tej')
    parser.add_argument('--snapshot-manifest', type=Path, required=True)
    parser.add_argument('--candidate-manifest', type=Path, required=True)
    parser.add_argument('--priority-request-id', required=True)
    parser.add_argument('--date', type=date.fromisoformat, required=True)
    args = parser.parse_args()
    print(json.dumps(register(args.root, args.snapshot_manifest, args.candidate_manifest,
                              args.priority_request_id, args.date), ensure_ascii=False))


if __name__ == '__main__':
    main()
