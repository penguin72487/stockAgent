#!/usr/bin/env python3
"""Drain corrected option product/day checks in bounded native-owner batches.

Each batch is separately evidenced and expires no later than the original
source-conflict plan. No sidecar or original receipt is edited or removed.
"""
from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
from pathlib import Path
import subprocess
import sys
import time

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.finmind_source_conflict_rechecks import CONTRACT_BATCHES, load_option_contract_conflicts
from downloader.provider_priority import inspect
from scripts.request_finmind_option_tick_product_checks import CONTRACT, DATASET, evidence, register_evidenced
from scripts.run_finmind_option_conflict_wait import corrected_contract_readbacks


REPO = Path(__file__).resolve().parents[1]
ROOT = REPO / 'data_finmind/complement'


def product_days(requests: list[dict], readbacks: dict, already: set[tuple[str, str]]) -> list[tuple[str, str]]:
    """Use only same-original, corrected missing keys; retain unknowns outside this scope."""
    result = set()
    for old in requests:
        day = old['start_date']
        new = readbacks.get(day)
        if (not new or not new['verified'] or new['receipt'] != old['original_receipt_sha256']
                or new['parquet'] != old['original_parquet_sha256']):
            continue
        if not new['keys'].issubset({tuple(key) for key in old['expected_source_contracts']}):
            raise ValueError('corrected keys differ from original option source audit')
        result.update((day, key[0]) for key in new['keys'])
    return sorted(result - already)


def prior_checked(path: Path) -> set[tuple[str, str]]:
    plan = json.loads(path.read_text())
    if plan.get('contract') != CONTRACT or not 0 < len(plan.get('checks', [])) <= 64:
        raise ValueError('invalid prior option product source plan')
    return {(row['partition'], row['data_id']) for row in plan['checks']}


def register_if_owner_idle(root: Path, checked: list[dict], now: datetime, path: Path, pins: dict):
    """Retry an unstarted registration when another native owner holds its lock."""
    try:
        return register_evidenced(root, checked, now, path, dataset=DATASET, contract=CONTRACT,
            reason='Drain corrected option contract gaps through finite native per-product tick checks.',
            additional_evidence_pins=pins)
    except BlockingIOError:
        priority = path.with_name(path.stem + '_priority.json')
        if path.exists() or path.is_symlink() or priority.exists() or priority.is_symlink():
            raise
        return None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--corrected-contract-readback', type=Path, action='append', required=True)
    parser.add_argument('--previous-source-plan', type=Path, action='append', required=True)
    parser.add_argument('--output-prefix', type=Path, required=True)
    args = parser.parse_args()
    root, prefix = ROOT.resolve(), args.output_prefix.resolve()
    if not prefix.is_relative_to(REPO / 'artifacts/markets'):
        raise ValueError('formal source plans require the markets artifact root')
    readbacks, pins = corrected_contract_readbacks(root, args.corrected_contract_readback)
    for path in (Path(__file__).resolve(), *(p.resolve() for p in args.previous_source_plan)):
        pins[str(path)] = sha256_file(path)
    for previous in args.previous_source_plan:
        report = previous.with_name(previous.stem + '_readback.json')
        registration = previous.with_name(previous.stem + '_registration.json')
        if not registration.is_file():
            raise ValueError('previous product batch lacks its native registration')
        request_id = json.loads(registration.read_text())['request_id']
        while not report.exists():
            if any(sha256_file(Path(path)) != digest for path, digest in pins.items()):
                print(json.dumps({'state': 'evidence_changed', 'provider_calls_by_waiter': 0}), flush=True)
                return 75
            if not inspect(root, request_id, now=datetime.now(UTC))['priority_active']:
                print(json.dumps({'state': 'previous_priority_expired_without_readback',
                                  'provider_calls_by_waiter': 0}), flush=True)
                return 75
            time.sleep(20)
        read = json.loads(report.read_text())
        if (read.get('source_plan_sha256') != sha256_file(previous)
                or read.get('request_id') != request_id
                or read.get('contract') != 'finmind_option_tick_product_source_readback_v2'):
            raise ValueError('previous batch has no matching terminal source readback')
    already = set().union(*(prior_checked(path) for path in args.previous_source_plan))
    existing = sorted(prefix.parent.glob(prefix.name + '_batch_[0-9][0-9][0-9][0-9].json'))
    for path in existing:
        registration = path.with_name(path.stem + '_registration.json')
        if not registration.is_file():
            raise ValueError('existing batch lacks its native registration receipt')
        info = json.loads(registration.read_text())
        if info.get('source_plan') != str(path):
            raise ValueError('existing batch registration differs from plan')
        report = path.with_name(path.stem + '_readback.json')
        if not report.exists():
            child = subprocess.run([sys.executable, 'scripts/run_finmind_option_tick_product_priority_wait.py',
                                    '--source-plan', str(path), '--request-id', info['request_id']], cwd=REPO)
            if child.returncode:
                return child.returncode
        read = json.loads(report.read_text())
        if (read.get('source_plan_sha256') != sha256_file(path) or read.get('request_id') != info['request_id']):
            raise ValueError('existing batch source readback differs')
        already.update(prior_checked(path))
    batches = sorted((root / CONTRACT_BATCHES).glob('*.json'))
    requests = []
    for path in batches:
        loaded = load_option_contract_conflicts(root, datetime.now(UTC), sidecar=path)
        if loaded:
            requests.extend(loaded[0]['requests'])
    remaining = product_days(requests, readbacks, already)
    originals = {request['start_date']: request for request in requests}
    stale_days = set()
    print(json.dumps({'state': 'backlog_ready', 'product_days': len(remaining),
                      'previously_attempted_product_days': len(already),
                      'full_contract_or_training_completeness': False}, ensure_ascii=False), flush=True)
    number = len(existing)
    while remaining:
        if any(sha256_file(Path(path)) != digest for path, digest in pins.items()):
            print(json.dumps({'state': 'evidence_changed', 'provider_calls_by_waiter': 0}), flush=True)
            return 75
        now = datetime.now(UTC)
        selection = remaining[:64]
        changed = set()
        for day in {day for day, _ in selection}:
            old = originals[day]
            receipt = root / 'receipts' / DATASET / 'all' / f'{day}.json'
            prior = (root / old['original_parquet_path']).resolve()
            if (not receipt.is_file() or not prior.is_relative_to(root) or not prior.is_file()
                    or sha256_file(receipt) != old['original_receipt_sha256']
                    or sha256_file(prior) != old['original_parquet_sha256']):
                changed.add(day)
        if changed:
            stale_days.update(changed)
            remaining = [item for item in remaining if item[0] not in changed]
            print(json.dumps({'state': 'changed_sources_require_reaudit', 'days': sorted(changed),
                              'remaining_comparable_product_days': len(remaining)}, ensure_ascii=False), flush=True)
            continue
        checked = evidence(root, selection, now)
        for item in checked:
            checked_day = readbacks[item['partition']]
            old = {tuple(key) for key in item['audited_missing_contracts']}
            keys = sorted(key for key in checked_day['keys'] if key[0] == item['data_id'])
            if (not checked_day['verified'] or checked_day['receipt'] != item['original_receipt_sha256']
                    or checked_day['parquet'] != item['original_parquet_sha256']
                    or not keys or not set(keys).issubset(old)):
                raise ValueError('corrected product audit changed before registration')
            item['audited_missing_contracts'] = [list(key) for key in keys]
        path = prefix.with_name(f'{prefix.name}_batch_{number + 1:04d}.json')
        result = register_if_owner_idle(root, checked, now, path, pins)
        if result is None:
            print(json.dumps({'state': 'registration_owner_lock_busy', 'batch': number + 1,
                              'provider_calls_by_waiter': 0}), flush=True)
            time.sleep(5)
            continue
        number += 1
        registration = path.with_name(path.stem + '_registration.json')
        atomic_write_json(registration, result)
        print(json.dumps({'state': 'batch_registered', 'batch': number, 'items': len(checked),
                          'remaining_after_batch': len(remaining) - len(checked),
                          'source_plan': str(path), 'provider_calls_by_waiter': 0}), flush=True)
        child = subprocess.run([sys.executable, 'scripts/run_finmind_option_tick_product_priority_wait.py',
                                '--source-plan', str(path), '--request-id', result['request_id']], cwd=REPO)
        if child.returncode:
            return child.returncode
        report = json.loads(path.with_name(path.stem + '_readback.json').read_text())
        if report.get('source_plan_sha256') != sha256_file(path) or report.get('request_id') != result['request_id']:
            raise ValueError('batch source readback differs')
        remaining = remaining[len(checked):]
    print(json.dumps({'state': 'evidenced_product_day_checks_finished', 'batches': number,
                      'source_days_requiring_reaudit': len(stale_days),
                      'full_contract_or_training_completeness': False,
                      'provider_calls_by_waiter': 0}), flush=True)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
