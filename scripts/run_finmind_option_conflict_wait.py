#!/usr/bin/env python3
"""Wait for capacity, then run bounded source checks through the owner.

The waiter holds no worker/account lock and does not invoke a provider. The
existing Complement CLI owns the lock, capacity and quota checks, and retains
the original receipts. The sidecar's expiry bounds the entire operation.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import csv
from datetime import UTC, datetime
import json
import math
from pathlib import Path
import re
import sqlite3
import subprocess
import sys
import time

from downloader.artifact_io import sha256_file
from downloader.finmind_futures_tick_conflicts import BATCHES as FUTURES_BATCHES, load_futures_tick_conflicts
from downloader.finmind_source_conflict_rechecks import (
    CONTRACT_BATCHES, PRODUCT_BATCHES, RECENT_BATCHES, SIDECAR, load_option_conflicts,
    load_option_contract_conflicts, load_option_product_conflicts, load_recent_option_conflicts,
)
from stockagent.data_sync.acquisition_capacity_sampler import AcquisitionCapacitySampler, capacity_retry_delay_seconds


REPO = Path(__file__).resolve().parents[1]
ROOT = REPO / 'data_finmind/complement'
OWNERS = (REPO / 'downloader/download_finmind_complement.py',
          REPO / 'downloader/finmind_errors.py',
          REPO / 'downloader/finmind_corrections.py',
          REPO / 'downloader/finmind_source_conflict_rechecks.py',
          REPO / 'downloader/finmind_option_monthly_evidence.py',
          REPO / 'scripts/run_finmind_option_conflict_wait.py')


def read_states(root: Path, requests: list[dict]) -> list[dict]:
    db = (root / 'queue.sqlite3').resolve().as_uri() + '?mode=ro'
    with sqlite3.connect(db, uri=True, timeout=5) as connection:
        table = connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='finmind_correction_tasks'").fetchone()
        result = []
        for request in requests:
            day, identity = request['start_date'], request['correction_id']
            dataset = request.get('dataset', 'TaiwanOptionTick')
            task = connection.execute("SELECT state,error_code FROM tasks WHERE dataset=? AND data_id='' AND partition=?",
                                      (dataset, day)).fetchone()
            correction = (connection.execute("SELECT state FROM finmind_correction_tasks WHERE owner='complement' AND correction_id=? "
                                             "AND dataset=? AND data_id='' AND partition=?",
                                             (identity, dataset, day)).fetchone() if table else None)
            result.append({'date': day, 'task_state': task[0] if task else 'missing_task',
                           'error_code': task[1] if task else None,
                           'correction_state': correction[0] if correction else 'not_adopted'})
    return result


def done(states: list[dict]) -> bool:
    # The owner only records already_refreshed after verifying the existing
    # post-notice receipt. Treat it as terminal without re-fetching that key.
    terminal = {'repaired', 'already_refreshed', 'observed_empty_unverified', 'calendar_excluded_unverified'}
    return bool(states) and all(item['correction_state'] in terminal
                                or (item['correction_state'] == 'queued' and item['task_state'] == 'complete')
                                for item in states)


def active_plans(root: Path, now: datetime, product_paths: list[Path], contract_paths: list[Path],
                 recent_paths: list[Path] = ()) -> list[dict]:
    plans = []
    loaded = load_option_conflicts(root, now)
    if loaded:
        plans.append(loaded[0])
    for path in product_paths:
        loaded = load_option_product_conflicts(root, now, sidecar=path)
        if loaded:
            plans.append(loaded[0])
    for path in contract_paths:
        loaded = load_option_contract_conflicts(root, now, sidecar=path)
        if loaded:
            plans.append(loaded[0])
    for path in recent_paths:
        loaded = load_recent_option_conflicts(root, now, sidecar=path)
        if loaded:
            plans.append(loaded[0])
    return plans


def select_keys(requests: list[dict], states: list[dict]) -> list[tuple[str, str, str]]:
    """Select at most 64 distinct pending source days from the evidenced plans."""
    keys = []
    seen = set()
    for request, state in zip(requests, states, strict=True):
        key = (request.get('dataset', 'TaiwanOptionTick'), '', request['start_date'])
        if not done([state]) and key not in seen:
            keys.append(key)
            seen.add(key)
            if len(keys) == 64:
                break
    return keys


def owner_retry_delay(returncode: int, owner_state: str | None, stderr: str) -> int | None:
    """A proved pre-query lock collision may retry without a five-minute delay."""
    if returncode == 2:
        lines = stderr.strip().splitlines()
        return 5 if lines and lines[-1] == 'FinMind complement worker already running' else None
    if returncode not in {0, 75}:
        return None
    return 60 if owner_state == 'disk_guard' else 5 if owner_state == 'batch_complete' else 300


def product_predecessor_decision(record: dict, run_id: str, log_path: Path,
                                 terminal: dict | None, unit_state: str | None) -> str:
    """Gate all-market replacement on the exact product backlog's completion.

    A worker lock serializes individual calls but does not protect a product
    batch's original all-market receipt between calls. A successful workflow
    receipt *and* the backlog's own terminal event are required before source
    replacement. A failed, cancelled, or unproven predecessor never releases
    the gate.
    """
    argv = record.get('argv')
    if (record.get('run_id') != run_id or record.get('repo_root') != str(REPO)
            or record.get('backend') != 'systemd'
            or record.get('unit') != f'stockagent-agent-{run_id}.service'
            or Path(record.get('log', '')).resolve() != log_path.resolve()
            or not isinstance(argv, list) or len(argv) < 2
            or argv[0] != 'run_fintech_python'
            or argv[1] != 'scripts/run_finmind_option_corrected_product_backlog.py'):
        raise ValueError('product predecessor workflow identity mismatch')
    state = record.get('state')
    if state == 'succeeded' and record.get('exit_code') == 0:
        if (not isinstance(terminal, dict)
                or terminal.get('state') != 'evidenced_product_day_checks_finished'
                or type(terminal.get('batches')) is not int or terminal['batches'] < 0
                or type(terminal.get('source_days_requiring_reaudit')) is not int
                or terminal['source_days_requiring_reaudit'] < 0
                or terminal.get('full_contract_or_training_completeness') is not False):
            return 'blocked'
        return 'ready'
    if state in {'queued', 'running'} and unit_state in {'active', 'activating', 'deactivating'}:
        return 'wait'
    return 'blocked'


def wait_for_product_predecessor(root: Path, run_id: str, request_id: str) -> bool:
    if not re.fullmatch(r'[a-zA-Z0-9][a-zA-Z0-9_-]{0,127}', run_id):
        raise ValueError('invalid product predecessor run id')
    run_dir = REPO / 'artifacts/operations/agent-workflow/runs' / run_id
    run_path, log_path = run_dir / 'run.json', run_dir / 'run.log'
    priority_path = root / 'priority_queries' / f'{request_id}.json'
    previous = None
    while True:
        priority = json.loads(priority_path.read_text())
        if (priority.get('request_id') != request_id
                or priority.get('contract') != 'provider_priority_query_v1'):
            raise ValueError('all-market priority identity mismatch')
        expires = datetime.fromisoformat(priority['plan']['expires_at_utc'].replace('Z', '+00:00'))
        if datetime.now(UTC) >= expires:
            print(json.dumps({'state': 'priority_expired_waiting_for_product_backlog',
                              'provider_calls_by_waiter': 0}), flush=True)
            return False
        record = json.loads(run_path.read_text())
        unit_state = None
        if record.get('state') in {'queued', 'running'}:
            unit = f'stockagent-agent-{run_id}.service'
            result = subprocess.run(['systemctl', 'show', '--property=ActiveState', '--value', unit],
                                    text=True, capture_output=True, timeout=10)
            if result.returncode:
                raise RuntimeError('cannot inspect product predecessor service')
            unit_state = result.stdout.strip()
            # Re-read after systemd: the process can finish between the two reads.
            record = json.loads(run_path.read_text())
        terminal = None
        if record.get('state') == 'succeeded':
            for line in reversed(log_path.read_text().splitlines()):
                try:
                    item = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(item, dict) and item.get('state'):
                    terminal = item
                    break
        decision = product_predecessor_decision(record, run_id, log_path, terminal, unit_state)
        marker = (decision, record.get('state'), unit_state)
        if marker != previous:
            print(json.dumps({'state': 'product_backlog_gate', 'decision': decision,
                              'predecessor_state': record.get('state'), 'unit_state': unit_state,
                              'source_days_requiring_reaudit': (terminal or {}).get('source_days_requiring_reaudit'),
                              'provider_calls_by_waiter': 0}), flush=True)
            previous = marker
        if decision != 'wait':
            return decision == 'ready'
        time.sleep(30)


def corrected_contract_readbacks(root: Path, paths: list[Path]) -> tuple[dict, dict]:
    """Read pinned corrected audits; never equate source presence with a fill.

    A readback can suppress an obsolete whole-day check only for the *same*
    original receipt and object that the old request examined. Unknown or
    changed evidence leaves the old request in place.
    """
    workspace = root.resolve().parents[1]
    pins, rows = {}, {}

    def pinned(path: str | Path, expected: str | None = None) -> Path:
        path = Path(path).resolve()
        if not path.is_relative_to(workspace):
            raise ValueError('corrected option readback outside workspace')
        actual = sha256_file(path)
        if expected is not None and actual != expected:
            raise ValueError('corrected option readback evidence changed')
        pins[str(path)] = actual
        return path

    def csv_rows(path: str, digest: str) -> list[dict]:
        with pinned(path, digest).open(newline='', encoding='utf-8') as source:
            return list(csv.DictReader(source))

    for summary_path in paths:
        summary = json.loads(pinned(summary_path).read_text(encoding='utf-8'))
        contract = summary.get('contract')
        if contract == 'taifex_option_ordinary_open_contract_vs_finmind_v2':
            source = json.loads(pinned(summary['coverage_summary'], summary['coverage_summary_sha256']).read_text())
            if (source.get('contract') != 'taifex_option_annual_positive_days_finmind_receipts_v1'
                    or Path(source['finmind_root']).resolve() != root.resolve()):
                raise ValueError('corrected annual option source identity mismatch')
            coverage = csv_rows(source['day_inventory'], source['day_inventory_sha256'])
            pinned(REPO / 'scripts/audit_taifex_option_contract_presence.py', summary['audit_script_sha256'])
        elif contract == 'taifex_option_monthly_open_contract_vs_finmind_v2':
            pinned(summary['source_readback'], summary['source_readback_sha256'])
            if Path(summary['finmind_root']).resolve() != root.resolve():
                raise ValueError('corrected monthly option source identity mismatch')
            coverage = None
            pinned(REPO / 'scripts/audit_taifex_option_monthly_contract_presence.py', summary['audit_sha256'])
            pinned(REPO / 'scripts/audit_taifex_option_contract_presence.py', summary['official_parser_sha256'])
        else:
            raise ValueError('unsupported corrected option contract readback')
        pinned(REPO / 'downloader/finmind_option_contract_keys.py', summary['series_normalizer_sha256'])
        day_rows = csv_rows(summary['day_inventory'], summary['day_inventory_sha256'])
        missing_rows = csv_rows(summary['missing_inventory'], summary['missing_inventory_sha256'])
        day_map = {item['date']: item for item in day_rows}
        if len(day_map) != len(day_rows) or set(day_map) & set(rows):
            raise ValueError('duplicate corrected option audit date')
        coverage_map = {item['date']: item for item in coverage} if coverage is not None else day_map
        if coverage is not None and len(coverage_map) != len(coverage):
            raise ValueError('duplicate corrected option coverage date')
        keys = defaultdict(set)
        for item in missing_rows:
            strike = float(item['strike'])
            if (item['date'] not in day_map or not re.fullmatch(r'[A-Z0-9]+', item['product'])
                    or not re.fullmatch(r'\d{6}([WF][1-5])?', item['series'])
                    or not math.isfinite(strike) or strike <= 0 or item['right'] not in {'C', 'P'}
                    or item['session'] not in {'day', 'unreported'}):
                raise ValueError('invalid corrected option contract key')
            keys[item['date']].add((item['product'], item['series'], strike, item['right']))
        for day, item in day_map.items():
            if int(item['missing_contracts']) != len(keys[day]):
                raise ValueError('corrected option contract count mismatch')
            cov = coverage_map.get(day, {})
            rows[day] = {'verified': item['source_status'] == 'contract_key_readback_verified'
                          and cov.get('finmind_status') == 'complete_payload_sha_verified',
                         'receipt': cov.get('finmind_receipt_sha256'),
                         'parquet': cov.get('finmind_parquet_sha256'),
                         'keys': keys[day]}
    return rows, pins


def filter_superseded_contract_requests(requests: list[dict], readbacks: dict) -> tuple[list[dict], Counter]:
    selected, counts = [], Counter()
    for request in requests:
        if not request['correction_id'].startswith('contract-conflict-'):
            selected.append(request)
            continue
        audited = readbacks.get(request['start_date'])
        if (not audited or not audited['verified']
                or audited['receipt'] != request['original_receipt_sha256']
                or audited['parquet'] != request['original_parquet_sha256']):
            # No comparable corrected snapshot: leave the evidenced work in
            # the queue instead of interpreting an unknown as resolved.
            counts['not_comparable'] += 1
            selected.append(request)
            continue
        old_keys = {tuple(key) for key in request['expected_source_contracts']}
        if not audited['keys'].issubset(old_keys):
            raise ValueError('corrected option keys are not a subset of the original audit')
        if not audited['keys']:
            counts['superseded'] += 1
        else:
            counts['retained'] += 1
            selected.append(request)
    return selected, counts


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--request-id', required=True)
    parser.add_argument('--futures-only', action='store_true', help='Use evidenced futures tick batches only')
    parser.add_argument('--corrected-contract-readback', action='append', type=Path, default=[],
                        help='Pinned v2 source comparison; repeat for each historical range')
    parser.add_argument('--after-product-run',
                        help='Wait for this evidenced product backlog before replacing all-market sources')
    args = parser.parse_args()
    root = ROOT.resolve()
    if args.futures_only and args.after_product_run:
        parser.error('product predecessor gate applies to option source checks only')
    if args.after_product_run and not wait_for_product_predecessor(root, args.after_product_run, args.request_id):
        return 75
    sidecar = root / SIDECAR
    product_paths = sorted((root / PRODUCT_BATCHES).glob('*.json'))
    contract_paths = sorted((root / CONTRACT_BATCHES).glob('*.json'))
    recent_paths = sorted((root / RECENT_BATCHES).glob('*.json'))
    futures_paths = sorted((root / FUTURES_BATCHES).glob('*.json')) if args.futures_only else []
    if not args.futures_only and contract_paths and not args.corrected_contract_readback:
        parser.error('corrected contract readback required for existing option contract batches')
    selected_batches = (futures_paths,) if args.futures_only else (product_paths, contract_paths, recent_paths)
    if any(len(paths) > 16 for paths in selected_batches):
        raise ValueError('too many source-conflict batches')
    plan_paths = (futures_paths if args.futures_only else
                  ([sidecar] if sidecar.exists() else []) + product_paths + contract_paths + recent_paths)
    priority_receipt = root / 'priority_queries' / f'{args.request_id}.json'
    owner_paths = (*OWNERS, REPO / 'downloader/finmind_futures_tick_conflicts.py') if args.futures_only else OWNERS
    pins = {str(path): sha256_file(path) for path in (*owner_paths, *plan_paths, priority_receipt)}
    readbacks, readback_pins = corrected_contract_readbacks(root, args.corrected_contract_readback)
    pins.update(readback_pins)
    def current_plans(now):
        if args.futures_only:
            return [loaded[0] for path in futures_paths
                    if (loaded := load_futures_tick_conflicts(root, now, sidecar=path))]
        return active_plans(root, now, product_paths, contract_paths, recent_paths)
    plans = current_plans(datetime.now(UTC))
    def requests_for(plans):
        requests = [request for plan in plans for request in plan['requests']]
        return filter_superseded_contract_requests(requests, readbacks) if not args.futures_only else (requests, Counter())
    _, initial_counts = requests_for(plans)
    if initial_counts:
        print(json.dumps({'state': 'corrected_contract_scope', **initial_counts,
                          'full_contract_or_training_completeness': False}, ensure_ascii=False), flush=True)
    expiry = {str(path): datetime.fromisoformat(json.loads(path.read_text())['expires_at_utc'].replace('Z', '+00:00'))
              for path in plan_paths}
    sampler = AcquisitionCapacitySampler(root)
    previous = None
    while True:
        now = datetime.now(UTC)
        changed_batches = (sorted((root / FUTURES_BATCHES).glob('*.json')) != futures_paths
                           if args.futures_only else
                           sorted((root / PRODUCT_BATCHES).glob('*.json')) != product_paths
                           or sorted((root / CONTRACT_BATCHES).glob('*.json')) != contract_paths
                           or sorted((root / RECENT_BATCHES).glob('*.json')) != recent_paths)
        if changed_batches or any(sha256_file(Path(path)) != digest for path, digest in pins.items()):
            print(json.dumps({'state': 'owner_or_plan_changed', 'provider_calls_by_waiter': 0}), flush=True)
            return 75
        if not expiry or any(now >= value for value in expiry.values()):
            # Revalidate and drop expired batches, keeping still-valid intents.
            plans = current_plans(now)
            expiry = {path: value for path, value in expiry.items() if now < value}
        if not plans:
            print(json.dumps({'state': 'plan_expired', 'provider_calls_by_waiter': 0}), flush=True)
            return 75
        requests, _ = requests_for(plans)
        if not requests:
            print(json.dumps({'state': 'source_scope_empty_after_superseded_filter',
                              'full_contract_or_training_completeness': False,
                              'provider_calls_by_waiter': 0}, ensure_ascii=False), flush=True)
            return 0
        states = read_states(root, requests)
        if done(states):
            unverified = sum(item['correction_state'] not in {'repaired', 'already_refreshed'} for item in states)
            print(json.dumps({'state': 'source_rechecks_finished' if not unverified else 'source_unresolved_after_attempts',
                              'items': len(states),
                              'verified_repaired': sum(item['correction_state'] == 'repaired' for item in states),
                              'verified_already_refreshed': sum(item['correction_state'] == 'already_refreshed' for item in states),
                              'unverified': unverified,
                              'full_contract_or_training_completeness': False,
                              'provider_calls_by_waiter': 0}, ensure_ascii=False), flush=True)
            return 75 if unverified else 0
        capacity = sampler.initial_sample()
        marker = (capacity.get('state'), capacity.get('reason'))
        if marker != previous:
            print(json.dumps({'state': 'capacity_wait' if marker[0] != 'admitted' else 'capacity_admitted',
                              'capacity_state': marker[0], 'reason': marker[1], 'items': len(states),
                              'unadopted': sum(item['correction_state'] == 'not_adopted' for item in states),
                              'provider_calls_by_waiter': 0}, ensure_ascii=False), flush=True)
            previous = marker
        if marker[0] == 'admitted':
            # Recheck all source hashes at dispatch, not merely at wait start.
            plans = current_plans(now)
            requests, _ = requests_for(plans)
            states = read_states(root, requests)
            selected = select_keys(requests, states)
            if not selected:
                continue
            command = [sys.executable, '-m', 'downloader.download_finmind_complement', '--root', str(root),
                       '--max-requests', str(len(selected)), '--finite-priority-request-id', args.request_id]
            for key in selected:
                command.extend(['--task-key', *key])
            try:
                child = subprocess.run(command, cwd=REPO, text=True, capture_output=True, timeout=1800)
                try:
                    owner_state = json.loads(child.stdout.strip().splitlines()[-1]).get('state')
                except (IndexError, json.JSONDecodeError, AttributeError):
                    owner_state = None
                print(json.dumps({'state': 'owner_pass', 'exit_code': child.returncode,
                                  'stdout_tail': child.stdout[-2000:], 'stderr_tail': child.stderr[-1000:],
                                  'provider_calls_by_waiter': 0}, ensure_ascii=False), flush=True)
            except subprocess.TimeoutExpired:
                print(json.dumps({'state': 'owner_pass_timeout', 'provider_calls_by_waiter': 0}), flush=True)
                return 75
            delay = owner_retry_delay(child.returncode, owner_state, child.stderr)
            if delay is None:
                return child.returncode
            # A fresh sample can finish after the short owner preflight. Retry
            # that deferred case promptly; completed finite batches can advance
            # immediately, while other unresolved states retain a longer wait.
            # The owner still repeats its own capacity and quota checks.
            time.sleep(delay)
        else:
            time.sleep(capacity_retry_delay_seconds(capacity))


if __name__ == '__main__':
    raise SystemExit(main())
