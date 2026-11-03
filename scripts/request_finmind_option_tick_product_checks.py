#!/usr/bin/env python3
"""Seed evidenced per-product/day option tick checks in the existing owner.

These are independent source observations, not replacements for the retained
whole-market objects and not training data.  The finite priority registration
uses the normal quota, capacity, retry and incremental-release policies.
"""
from __future__ import annotations

import argparse
from datetime import UTC, date, datetime, timedelta
import fcntl
import json
from pathlib import Path
import re
import sqlite3

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.finmind_source_conflict_rechecks import load_option_contract_conflicts
from downloader.finmind_supplemental import SOURCES
from downloader.provider_priority import register


DATASET = 'TaiwanOptionTick'
CONTRACT = 'finmind_option_tick_product_source_check_v1'


def evidence(root: Path, selections: list[tuple[str, str]], now: datetime) -> list[dict]:
    """Recheck the original conflict chain and both native source artifacts."""
    root = root.resolve()
    if now.tzinfo is None or not selections or len(set(selections)) != len(selections):
        raise ValueError('unique dated product selections and an aware clock are required')
    wanted = set(selections)
    for day, product in wanted:
        if (date.fromisoformat(day).isoformat() != day or date.fromisoformat(day) > now.date()
                or not re.fullmatch(r'[A-Z0-9]{2,12}', product)):
            raise ValueError('invalid product or physical source date')
    by_day: dict[str, dict] = {}
    for path in sorted((root / 'option_contract_conflict_batches').glob('*.json')):
        raw = path.read_bytes()
        metadata = json.loads(raw)
        if not ({day for day, _ in wanted} & set(metadata.get('dates', []))):
            continue
        loaded = load_option_contract_conflicts(root, now, sidecar=path)
        if not loaded:
            raise ValueError('source conflict evidence expired')
        expiry = datetime.fromisoformat(metadata['expires_at_utc'].replace('Z', '+00:00'))
        for item in loaded[0]['requests']:
            if item['start_date'] in by_day:
                raise ValueError('source date occurs in multiple conflict plans')
            by_day[item['start_date']] = (item, path, sha256_file(path), expiry)
        if path.read_bytes() != raw:
            raise ValueError('source conflict plan changed during verification')
    checked = []
    for day, product in sorted(wanted):
        if day not in by_day:
            raise ValueError('no active official-source conflict for requested date')
        item, sidecar, sidecar_sha, expiry = by_day[day]
        contracts = [row for row in item['expected_source_contracts'] if row[0] == product]
        if not contracts:
            raise ValueError('requested product has no independently audited missing contract')
        receipt = root / 'receipts' / DATASET / 'all' / f'{day}.json'
        prior = (root / item['original_parquet_path']).resolve()
        if (not prior.is_relative_to(root) or sha256_file(prior) != item['original_parquet_sha256']
                or sha256_file(receipt) != item['original_receipt_sha256']):
            raise ValueError('original whole-market source or receipt changed; re-audit before querying')
        checked.append({'dataset': DATASET, 'data_id': product, 'partition': day,
                        'audited_missing_contracts': contracts,
                        'original_receipt': str(receipt), 'original_receipt_sha256': item['original_receipt_sha256'],
                        'original_parquet': str(prior), 'original_parquet_sha256': item['original_parquet_sha256'],
                        'conflict_plan': str(sidecar), 'conflict_plan_sha256': sidecar_sha,
                        'conflict_plan_expires_at_utc': expiry.isoformat()})
    return checked


def register_checks(root: Path, selections: list[tuple[str, str]], now: datetime, output: Path) -> dict:
    checked = evidence(root, selections, now)
    return register_evidenced(root, checked, now, output, dataset=DATASET, contract=CONTRACT,
                              reason='Compare independently audited missing option contract-days with the documented per-product tick source; retain original whole-market objects.')


def register_evidenced(root: Path, checked: list[dict], now: datetime, output: Path, *,
                       dataset: str, contract: str, reason: str,
                       additional_evidence_pins: dict[str, str] | None = None) -> dict:
    """Register an evidenced finite per-product request with the existing owner."""
    root, output = root.resolve(), output.resolve()
    if not (root / 'queue.sqlite3').is_file():
        raise ValueError('an existing canonical FinMind queue is required')
    if (dataset not in {'TaiwanOptionTick', 'TaiwanFuturesTick'} or not checked
            or any(row.get('dataset') != dataset for row in checked)):
        raise ValueError('all evidenced selections must use the requested tick dataset')
    expires = min([now + timedelta(days=6), *(datetime.fromisoformat(row['conflict_plan_expires_at_utc'])
                                               for row in checked)])
    plan = {'contract': 'provider_priority_query_v1', 'provider': 'finmind',
            'reason': reason,
            'created_at_utc': now.isoformat(), 'expires_at_utc': expires.isoformat(),
            'items': [{key: row[key] for key in ('dataset', 'data_id', 'partition')} for row in checked]}
    source_plan = {'contract': contract, 'created_at_utc': now.isoformat(), 'expires_at_utc': expires.isoformat(),
                   'checks': checked, 'provider_calls': 0,
                   'additional_evidence_pins': additional_evidence_pins or {},
                   'implementation_sha256': sha256_file(Path(__file__)),
                   'documentation': 'https://finmind.github.io/tutor/TaiwanMarket/Derivative/',
                   'scope': 'per_product_physical_day_source_comparison_only_no_training_injection'}
    priority_path = output.with_name(output.stem + '_priority.json')
    with (root / 'worker.lock').open('a+') as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            # No original source was allowed to change between audit and seed.
            workspace = root.parents[1]
            for path, digest in source_plan['additional_evidence_pins'].items():
                evidence_path = Path(path).resolve()
                if not evidence_path.is_relative_to(workspace) or sha256_file(evidence_path) != digest:
                    raise ValueError('additional source evidence changed before registration')
            for row in checked:
                pins = [('original_receipt', 'original_receipt_sha256'),
                        ('original_parquet', 'original_parquet_sha256'),
                        ('conflict_plan', 'conflict_plan_sha256')]
                if 'baseline_parquet' in row:
                    pins.append(('baseline_parquet', 'baseline_parquet_sha256'))
                for field, digest in pins:
                    if sha256_file(Path(row[field])) != row[digest]:
                        raise ValueError('source evidence changed before registration')
            # The source evidence is durable before any native task can be adopted.
            if output.exists() or output.is_symlink() or priority_path.exists() or priority_path.is_symlink():
                raise FileExistsError('preserve existing source and priority evidence')
            atomic_write_json(output, source_plan)
            atomic_write_json(priority_path, plan)
            with sqlite3.connect(root / 'queue.sqlite3', timeout=10) as con:
                con.execute('BEGIN IMMEDIATE')
                for row in checked:
                    key = tuple(row[name] for name in ('dataset', 'data_id', 'partition'))
                    old = con.execute('SELECT kind FROM tasks WHERE dataset=? AND data_id=? AND partition=?', key).fetchone()
                    if old and old[0] != 'id_day':
                        raise ValueError('existing owner task has an incompatible query shape')
                    con.execute("INSERT OR IGNORE INTO tasks(dataset,data_id,partition,kind,priority,state) "
                                "VALUES (?,?,?,'id_day',?,'pending')", (*key, SOURCES[dataset].priority))
                con.commit()
            priority = register(root, plan, now=now)
        finally:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
    return {'contract': contract, 'source_plan': str(output), 'priority_plan': str(priority_path),
            'request_id': priority['request_id'], 'priority_active': priority['priority_active'],
            'items': priority['items'], 'provider_calls_by_registration': 0,
            'source_complete': False, 'training_ready': False}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path('data_finmind/complement'))
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--product-day', nargs=2, metavar=('DATE', 'PRODUCT'), action='append', required=True)
    args = parser.parse_args()
    print(json.dumps(register_checks(args.root, [tuple(x) for x in args.product_day], datetime.now(UTC), args.output),
                     ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
