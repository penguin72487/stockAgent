#!/usr/bin/env python3
"""Register finite TEJ priority only after the exact native witness is audited.

This bounded watcher does not open a vendor session, submit Preview, or mark
the partial menu as complete. Existing owner/locks, quota and capacity govern
any later source query.
"""
from __future__ import annotations

import argparse
from contextlib import closing
from datetime import UTC, date, datetime, timedelta
import json
from pathlib import Path
import sqlite3
import sys
import time

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.provider_priority import CONTRACT as PROVIDER_CONTRACT, inspect, register as provider_register
from downloader.tej_priority import PriorityQueueChanged, register_priority_requests
from scripts.audit_tej_finite_option_axis_capture import audit
from scripts.build_tej_finite_option_priority import build


def _existing_source_receipt(root: Path, plan_path: Path, receipt_path: Path) -> dict:
    receipt = json.loads(receipt_path.read_text())
    digest = sha256_file(plan_path)
    if receipt.get('plan_sha256') != digest or receipt.get('global_plan_changed') is not False:
        raise ValueError('Finite source receipt does not match its retained plan')
    db = (root / 'queue.sqlite3').resolve(strict=True)
    with closing(sqlite3.connect(db.as_uri() + '?mode=ro', uri=True, timeout=5)) as con:
        row = con.execute('SELECT value FROM meta WHERE key=?', ('finite_priority_plan:' + digest,)).fetchone()
        if row is None or json.loads(row[0]) != receipt:
            raise ValueError('Finite source receipt differs from the committed owner audit')
    return receipt


def wait(root: Path, witness_receipt: Path, parent_priority: str, output_dir: Path,
         hours: float, target_day: date = date(2011, 2, 16)) -> int:
    if not 0 < hours <= 24:
        raise ValueError('A bounded wait is required')
    root = root.resolve(strict=True)
    output_dir = output_dir.resolve(strict=True)
    witness_receipt = witness_receipt.absolute()
    tracked = [REPO / p for p in ('scripts/tej_smart_wizard_bridge.ps1',
               'scripts/audit_tej_finite_option_axis_capture.py',
               'scripts/audit_tej_large_axis_capture.py',
               'scripts/register_tej_finite_option_axis_capture.py',
               'scripts/build_tej_finite_option_priority.py',
               'downloader/tej_company_axis_snapshot.py', 'downloader/tej_history.py',
               'downloader/tej_planning.py', 'downloader/tej_priority.py',
               'downloader/tej_priority_coverage.py', 'downloader/provider_priority.py')]
    pins = {path: sha256_file(path) for path in tracked}
    deadline = datetime.now(UTC) + timedelta(hours=hours)
    tag = target_day.strftime('%Y%m%d')
    plan_path = output_dir / f'tej_option_finite_{tag}_priority_plan_v1.json'
    source_receipt = output_dir / f'tej_option_finite_{tag}_priority_source_receipt_v1.json'
    provider_plan = output_dir / f'tej_option_finite_{tag}_provider_plan_v1.json'
    provider_receipt = output_dir / f'tej_option_finite_{tag}_provider_receipt_v1.json'
    print(json.dumps({'state': 'waiting_for_independently_audited_finite_menu',
                      'deadline_utc': deadline.isoformat(), 'provider_calls_by_waiter': 0}), flush=True)
    last_status = 0.0
    while datetime.now(UTC) < deadline:
        if any(sha256_file(path) != digest for path, digest in pins.items()):
            raise ValueError('Pinned implementation changed; preserve pending scope for review')
        parent = inspect(root, parent_priority)
        if parent.get('provider') != 'tej' or parent.get('priority_active') is not True:
            print(json.dumps({'state': 'parent_priority_expired', 'provider_calls_by_waiter': 0}), flush=True)
            return 75
        if witness_receipt.is_file():
            witness = json.loads(witness_receipt.read_text())
            observation = Path(witness['observation_path'])
            proof = audit(root, observation)
            if (witness.get('contract') != 'tej_finite_company_date_axis_independent_readback_v1'
                    or witness.get('observation_sha256') != proof['observation_sha256']
                    or witness.get('claimed_intent_sha256') != proof['claimed_intent_sha256']
                    or proof.get('date') != target_day.isoformat()):
                raise ValueError('Retained finite witness readback changed')
            if proof.get('source_key_mode') != 2 or proof.get('date_menu_contains_day') is not True:
                print(json.dumps({'state': 'finite_source_date_or_key_not_admitted',
                                  'source_key_mode': proof.get('source_key_mode'),
                                  'date_menu_contains_day': proof.get('date_menu_contains_day'),
                                  'provider_calls_by_waiter': 0}), flush=True)
                return 2
            plan = build(root, observation)
            if plan_path.exists():
                if json.loads(plan_path.read_text()) != plan:
                    raise ValueError('Retained finite plan changed')
            else:
                atomic_write_json(plan_path, plan, durable=True)
            if source_receipt.exists():
                receipt = _existing_source_receipt(root, plan_path, source_receipt)
            else:
                try:
                    receipt = register_priority_requests(root, plan_path, source_receipt)
                except PriorityQueueChanged:
                    print(json.dumps({'state': 'native_queue_changed_retrying_local_plan',
                                      'provider_calls_by_waiter': 0}), flush=True)
                    time.sleep(5)
                    continue
            if provider_plan.exists():
                provider_body = json.loads(provider_plan.read_text())
                if (provider_body.get('contract') != PROVIDER_CONTRACT or provider_body.get('provider') != 'tej'
                        or datetime.fromisoformat(provider_body['expires_at_utc']) >
                           datetime.fromisoformat(parent['expires_at_utc'])
                        or sorted(provider_body.get('items', [])) != sorted(item['task_id'] for item in receipt['tasks'])):
                    raise ValueError('Retained provider priority differs from finite tasks')
            else:
                now = datetime.now(UTC)
                expiry = min(datetime.fromisoformat(parent['expires_at_utc']), now + timedelta(days=7))
                if expiry <= now:
                    return 75
                provider_body = {'contract': PROVIDER_CONTRACT, 'provider': 'tej',
                    'reason': 'Finite verified option contract/date source cross-check; preserve original history and owner',
                    'created_at_utc': now.isoformat(), 'expires_at_utc': expiry.isoformat(),
                    'items': [item['task_id'] for item in receipt['tasks']]}
                atomic_write_json(provider_plan, provider_body, durable=True)
            status = provider_register(root, provider_body)
            if provider_receipt.exists():
                saved = json.loads(provider_receipt.read_text())
                if saved.get('request_id') != status['request_id']:
                    raise ValueError('Provider priority receipt identity changed')
            else:
                atomic_write_json(provider_receipt, status, durable=True)
            print(json.dumps({'state': 'finite_source_work_registered', 'source_tasks': len(receipt['tasks']),
                              'priority_request_id': status['request_id'], 'source_receipt': str(source_receipt),
                              'provider_receipt': str(provider_receipt), 'provider_calls_by_waiter': 0,
                              'market_data_obtained': False}), flush=True)
            return 0
        if time.monotonic() - last_status >= 60:
            print(json.dumps({'state': 'waiting_for_native_witness', 'provider_calls_by_waiter': 0}), flush=True)
            last_status = time.monotonic()
        time.sleep(10)
    print(json.dumps({'state': 'finite_wait_expired', 'provider_calls_by_waiter': 0}), flush=True)
    return 75


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=REPO / 'data_tej')
    parser.add_argument('--witness-receipt', type=Path, required=True)
    parser.add_argument('--parent-priority-request-id', required=True)
    parser.add_argument('--output-dir', type=Path, required=True)
    parser.add_argument('--hours', type=float, default=6)
    parser.add_argument('--date', type=date.fromisoformat, default=date(2011, 2, 16))
    args = parser.parse_args()
    raise SystemExit(wait(args.root, args.witness_receipt, args.parent_priority_request_id,
                          args.output_dir, args.hours, args.date))


if __name__ == '__main__':
    main()
