#!/usr/bin/env python3
"""Prioritize audited finite rechecks in the existing FinLab account worker."""
from __future__ import annotations

import argparse
import csv
from datetime import UTC, datetime, timedelta
import fcntl
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from downloader.artifact_io import atomic_write_json
from scripts.prepare_tw_day_trade_feature_catalog import sha256, write_csv
from stockagent.data.finlab_acquisition_contract import UPSTREAM_CHECK_MODES, safe_stem, utc_time
from stockagent.data.finlab_gap_priority import CONTRACT, MAX_REQUESTS, pending_gap_requests


def enqueue(audit: Path, root: Path, *, now: datetime):
    manifest = json.loads((audit / 'audit_manifest.json').read_text())
    worklist = audit / 'provider_priority_worklist.csv'
    digest = sha256(worklist)
    if digest != manifest['outputs']['provider_priority_worklist.csv']['sha256']:
        raise ValueError('priority worklist differs from fixed gap audit')
    with worklist.open(newline='', encoding='utf-8') as stream:
        rows = list(csv.DictReader(stream))
    if len(rows) != manifest['priority_dataset_requests']:
        raise ValueError('incomplete audited priority list')
    discovery = json.loads((root / 'catalog/discovery.json').read_text())
    allowed = set(discovery['keys'])
    requests = []
    for row in rows:
        if row['source'] != 'FinLab' or row['dataset'] not in allowed:
            raise ValueError('unknown/unowned provider key in gap priority worklist')
        requests.append({'dataset': row['dataset'], 'reason': row['reason'], 'priority': int(row['priority']),
            'request_id': hashlib.sha256((digest + '\0' + row['dataset']).encode()).hexdigest(),
            'evidence_sha256': digest, 'required_after_utc': now.isoformat(),
            'expires_at_utc': (now + timedelta(days=7)).isoformat(),
            'candidate_period_keys': int(row['unresolved_recheck_keys']),
            'evidence_status': row.get('evidence_status') or 'native_period_hint_not_download_corruption_proof'})
    with (root / '.gap_priority.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        path = root / 'gap_priority_requests.json'
        previous = json.loads(path.read_text()) if path.exists() else None
        old_rows = previous.get('requests', []) if previous else []
        if previous:
            pending_gap_requests(root, now=now, available=allowed)
        ids = {r['request_id'] for r in old_rows}
        added = [r for r in requests if r['request_id'] not in ids]
        merged = [*old_rows, *added]
        if len(merged) > MAX_REQUESTS:
            raise ValueError('priority intent bound exceeded; inspect old evidence first')
        if previous:
            archive = root / 'gap_priority_history' / (sha256(path) + '.json')
            if not archive.exists():
                atomic_write_json(archive, previous)
        payload = {'contract': CONTRACT, 'created_at_utc': now.isoformat(),
                   'audit_manifest_sha256': sha256(audit / 'audit_manifest.json'), 'requests': merged}
        atomic_write_json(path, payload); path.chmod(0o600)
    return {'queued_dataset_requests': len(added), 'total_intents': len(merged),
            'pending_now': len(pending_gap_requests(root, now=now, available=allowed)),
            'network_requests': 0, 'filled_observations': 0,
            'worker': 'existing_stockagent_finlab_local_refresh', 'intent_path': str(path.resolve())}


def report_intents(root: Path, *, now: datetime):
    """Report every finite intent without treating expiry or cache reads as checks."""
    path = root / 'gap_priority_requests.json'
    # Share the collector's identity, shape, size and evidence validation.
    pending_gap_requests(root, now=now)
    if not path.exists():
        return {'total_intents': 0, 'states': {}, 'all_provider_gaps_filled': False}, []
    captured = path.read_bytes()
    plan = json.loads(captured)
    rows, states = [], {}
    for request in plan['requests']:
        key = request['dataset']
        receipt_path = root / 'receipts' / (safe_stem(key) + '.json')
        try:
            raw = receipt_path.read_bytes()
            receipt = json.loads(raw)
        except (OSError, ValueError):
            raw, receipt = b'', {}
        if not isinstance(receipt, dict):
            receipt = {}
        checked = utc_time(receipt.get('source_checked_at_utc'))
        required = utc_time(request['required_after_utc'])
        expiry = utc_time(request['expires_at_utc'])
        if (receipt.get('dataset') == key and receipt.get('source_check_mode') in UPSTREAM_CHECK_MODES
                and checked and required <= checked <= now):
            state = 'true_upstream_checked_not_gap_fill_proof'
        elif expiry <= now:
            state = 'expired_without_verified_check'
        elif required > now:
            state = 'not_yet_due'
        else:
            state = 'pending_existing_owner'
        states[state] = states.get(state, 0) + 1
        rows.append({**request, 'state': state,
                     'source_checked_at_utc': receipt.get('source_checked_at_utc', ''),
                     'source_check_mode': receipt.get('source_check_mode', ''),
                     'source_sha256': receipt.get('sha256', ''),
                     'last_check_result': receipt.get('last_check_result', ''),
                     'captured_receipt_sha256': hashlib.sha256(raw).hexdigest() if raw else ''})
    return {'reported_at_utc': now.isoformat(), 'total_intents': len(rows), 'states': states,
            'intent_sha256': hashlib.sha256(captured).hexdigest(), 'network_requests': 0,
            'filled_observations_claim': None, 'all_provider_gaps_filled': False,
            'worker': 'existing_stockagent_finlab_local_refresh'}, rows


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--audit-dir', type=Path)
    p.add_argument('--finlab-root', type=Path, default=ROOT / 'data_finlab')
    p.add_argument('--receipt', type=Path, required=True)
    p.add_argument('--status-only', action='store_true', help='inspect existing intents; no enqueue or provider calls')
    p.add_argument('--status-csv', type=Path)
    a = p.parse_args()
    if a.status_only:
        result, rows = report_intents(a.finlab_root, now=datetime.now(UTC))
        if a.status_csv:
            write_csv(a.status_csv, rows)
            result['status_csv'] = str(a.status_csv.resolve())
            result['status_csv_sha256'] = sha256(a.status_csv)
    else:
        if not a.audit_dir:
            p.error('--audit-dir is required when enqueueing')
        if a.status_csv:
            p.error('--status-csv requires --status-only')
        result = enqueue(a.audit_dir, a.finlab_root, now=datetime.now(UTC))
    atomic_write_json(a.receipt, result)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
