#!/usr/bin/env python3
"""Register source-bound daily-option conflicts with the existing Sponsor owner.

This writes only metadata and native priority/queue entries. The original
worker alone makes source calls under its normal account, quota and capacity
guards; prior source payloads and receipt snapshots remain preserved.
"""
from __future__ import annotations

import argparse
from datetime import UTC, datetime, timedelta
import csv
import fcntl
import hashlib
import io
import json
from pathlib import Path
import sqlite3

from downloader.artifact_io import atomic_write_bytes, atomic_write_json
from downloader.finmind_corrections import _apply_worker_plan
from downloader.finmind_option_daily_conflicts import CONTRACT, SIDECAR, load_daily_option_conflicts
from downloader.provider_priority import register as register_priority


def register(root: Path, product_summary: Path, now: datetime) -> dict:
    root, product_summary = root.resolve(), product_summary.resolve()
    if now.tzinfo is None:
        raise ValueError('timezone-aware timestamp required')
    summary_bytes = product_summary.read_bytes()
    summary = json.loads(summary_bytes)
    if (summary.get('contract') != 'taifex_option_vs_finmind_all_market_daily_presence_v2'
            or Path(summary['finmind_daily_root']).resolve() != root):
        raise ValueError('product audit must match the Sponsor owner')
    inventory_bytes = Path(summary['day_results']).read_bytes()
    if hashlib.sha256(inventory_bytes).hexdigest() != summary['day_results_sha256']:
        raise ValueError('daily product audit inventory changed')
    rows = list(csv.DictReader(io.StringIO(inventory_bytes.decode('utf-8'), newline='')))
    selected = [r for r in rows if r['source_status'] == 'daily_readback_verified'
                and r['missing_day_positive_volume_products']]
    if not 0 < len(selected) <= 64 or len({row['date'] for row in selected}) != len(selected):
        raise ValueError('expected a bounded set of unique source conflicts')
    sidecar = root / SIDECAR
    with (root / 'worker.lock').open('a+') as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        if sidecar.exists():
            raise FileExistsError('daily option conflict sidecar already exists')
        entries = []
        for row in selected:
            original = root / 'receipts' / 'TaiwanOptionDaily' / 'all' / f"{row['date']}.json"
            if original.is_symlink():
                raise ValueError('redirected original receipt')
            raw = original.read_bytes()
            if hashlib.sha256(raw).hexdigest() != row['receipt_sha256']:
                raise ValueError('native receipt changed after product audit')
            relative = Path('option_daily_conflict_evidence') / f"{row['receipt_sha256']}.json"
            snapshot = root / relative
            if snapshot.exists():
                if snapshot.is_symlink() or snapshot.read_bytes() != raw:
                    raise ValueError('conflicting original receipt snapshot')
            else:
                atomic_write_bytes(snapshot, raw)
            entries.append({'date': row['date'], 'original_receipt_snapshot': str(relative)})
        sidecar_plan = {'contract': CONTRACT, 'created_at_utc': now.isoformat(),
                        'expires_at_utc': (now + timedelta(days=7)).isoformat(),
                        'product_summary_path': str(product_summary),
                        'product_summary_sha256': hashlib.sha256(summary_bytes).hexdigest(),
                        'dates': entries}
        atomic_write_json(sidecar, sidecar_plan)
        verified = load_daily_option_conflicts(root, now)
        if verified is None:
            raise ValueError('conflict plan expired before registration')
        plan, digest = verified
        with sqlite3.connect(root / 'queue.sqlite3', timeout=10) as con:
            con.execute('BEGIN IMMEDIATE')
            result = _apply_worker_plan(con, root, 'sponsor', now, plan, digest, max_receipts=64)
            con.commit()
        priority = {'contract': 'provider_priority_query_v1', 'provider': 'finmind',
                    'reason': 'Evidence-backed TAIFEX versus FinMind option daily product-volume conflicts',
                    'created_at_utc': now.isoformat(), 'expires_at_utc': (now + timedelta(days=7)).isoformat(),
                    'items': [{'dataset': 'TaiwanOptionDaily', 'data_id': '', 'partition': item['date']}
                              for item in entries]}
        status = register_priority(root, priority, now=now)
    return {'contract': CONTRACT, 'observed_at_utc': now.isoformat(), 'sidecar': str(sidecar),
            'correction_plan_id': plan['plan_id'], 'date_count': len(entries),
            'correction_adoption': result, 'priority': status, 'provider_calls': 0,
            'limitations': ['Registration does not prove that a provider request occurred or resolved the conflict.',
                            'Product-volume presence alone is not executable price, full contract coverage or PIT.']}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path('data_finmind/sponsor'))
    parser.add_argument('--product-summary', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    result = register(args.root, args.product_summary, datetime.now(UTC))
    atomic_write_json(args.output, result)
    print(json.dumps({'output': str(args.output), 'date_count': result['date_count'],
                      'correction_adoption': result['correction_adoption'],
                      'request_id': result['priority']['request_id'], 'provider_calls': 0}, ensure_ascii=False))


if __name__ == '__main__':
    main()
