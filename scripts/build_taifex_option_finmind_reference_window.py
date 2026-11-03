#!/usr/bin/env python3
"""Build source-only option reference candidates from retained FinMind ticks.

This deliberately requires consecutive natural dates: it cannot prove that a
missing source date was a market holiday. It neither completes historical PIT
admission nor supplies executable prices, eligibility or an option account.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import re
import tempfile

import polars as pl
import pyarrow.parquet as pq

from downloader.artifact_io import atomic_write_json, durable_replace, sha256_file
from scripts.build_taifex_option_reference_source_window import derive_day
from scripts.check_acquisition_capacity import priority_dispatch_status
from stockagent.data.tw_option_finmind_reference import candidate_rows


CONTRACT = 'finmind_option_observed_reference_source_window_v1'


class CapacityNotAdmitted(RuntimeError):
    def __init__(self, status: dict):
        super().__init__(f"physical capacity not admitted: {status.get('reason')}")
        self.status = status


def pin_source(root: Path, day: date) -> dict:
    """Verify an immutable source object and its exact retained receipt."""
    root = root.resolve()
    receipt = root / 'receipts' / 'TaiwanOptionTick' / 'all' / f'{day.isoformat()}.json'
    if not receipt.is_file() or receipt.is_symlink():
        raise ValueError(f'missing canonical receipt: {day}')
    receipt_bytes = receipt.read_bytes()
    doc = json.loads(receipt_bytes)
    expected = {'dataset': 'TaiwanOptionTick', 'data_id': '', 'partition': day.isoformat(),
                'kind': 'id_day', 'status': 'complete', 'storage_contract_version': 3,
                'source_first_date': day.isoformat(), 'source_last_date': day.isoformat()}
    if any(doc.get(key) != value for key, value in expected.items()):
        raise ValueError(f'unsupported source receipt identity: {day}')
    if (doc.get('source_bytes_preserved') is not True or not isinstance(doc.get('rows'), int)
            or doc['rows'] <= 0 or not re.fullmatch(r'[0-9a-f]{64}', doc.get('sha256', ''))):
        raise ValueError(f'invalid source identity or row count: {day}')
    relative = Path(doc.get('parquet_path', ''))
    path = (root / relative).resolve()
    if (relative.is_absolute() or not path.is_relative_to(root) or not path.is_file()
            or path.stat().st_size != doc.get('parquet_size_bytes')
            or sha256_file(path) != doc['sha256'] or pq.ParquetFile(path).metadata.num_rows != doc['rows']):
        raise ValueError(f'original source data differs from receipt: {day}')
    return {'day': day.isoformat(), 'receipt_path': str(receipt),
            'receipt_sha256': hashlib.sha256(receipt_bytes).hexdigest(),
            'path': str(path), 'sha256': doc['sha256'], 'rows': doc['rows'],
            'bytes': doc['parquet_size_bytes']}


def candidate_policy(path: Path) -> tuple[dict[str, tuple[int, int]], dict[str, str]]:
    doc = json.loads(path.read_bytes())
    if doc.get('contract') != 'taifex_option_session_policy_candidate_v1' or doc.get('research_candidate_only') is not True:
        raise ValueError('an explicit research-only candidate policy is required')
    windows = {key: tuple(value['day_minutes']) for key, value in doc['products'].items()}
    if not windows or any(not isinstance(key, str) or not key or key != key.upper()
                          or len(window) != 2 or not all(isinstance(n, int) for n in window)
                          or not 0 <= window[0] < window[1] <= 1440 for key, window in windows.items()):
        raise ValueError('candidate product windows are invalid')
    pins = {}
    for name, fields in [('inventory_comparison_pins', (('inventory_path', 'inventory_sha256'),
                                                        ('receipt_path', 'receipt_sha256'))),
                         ('current_catalog_comparison_pins', (('raw_path', 'raw_sha256'),
                                                               ('receipt_path', 'receipt_sha256')))]:
        for path_field, hash_field in fields:
            evidence = doc[name]
            linked = Path(evidence[path_field]).resolve()
            if sha256_file(linked) != evidence[hash_field]:
                raise ValueError('candidate-policy linked evidence changed')
            pins[str(linked)] = evidence[hash_field]
    return windows, pins


def capacity_check(path: Path) -> dict:
    ancestor = path.resolve()
    while not ancestor.exists():
        ancestor = ancestor.parent
    status = priority_dispatch_status(ancestor, additional_headroom_bytes=40 * 1024**3 + 128 * 1024**2)
    if status.get('state') not in {'admitted', 'not_enrolled'}:
        raise CapacityNotAdmitted(status)
    return status


def build(root: Path, policy_path: Path, dates: list[date], output: Path) -> dict:
    if (not dates or dates != sorted(set(dates))
            or any(b - a != timedelta(days=1) for a, b in zip(dates, dates[1:]))
            or output.exists() or output.is_symlink()):
        raise ValueError('ordered, consecutive dates and a new output directory are required')
    root, policy_path, output = root.resolve(), policy_path.resolve(), output.resolve()
    policy_sha = sha256_file(policy_path)
    windows, linked_pins = candidate_policy(policy_path)
    sources = [pin_source(root, day) for day in dates]
    implementation_paths = [Path(__file__), Path('scripts/build_taifex_option_reference_source_window.py'),
                            Path('stockagent/data/tw_option_finmind_reference.py'),
                            Path('stockagent/data/tw_option_reference_candidates.py'),
                            Path('downloader/finmind_option_contract_keys.py')]
    implementation_pins = {str(p.resolve()): sha256_file(p) for p in implementation_paths}
    initial_capacity = capacity_check(output.parent)
    state, results, counts = None, [], []
    for day, source in zip(dates, sources, strict=True):
        raw = pl.read_parquet(source['path'])
        current, stats = candidate_rows(raw, day=day, source_sha256=source['sha256'],
                                        candidate_products=set(windows))
        if current.is_empty():
            raise ValueError(f'no candidate source rows for {day}; prior continuity is not inferred')
        result, state = derive_day(state, current, day, windows)
        results.append(result)
        counts.append({'day': day.isoformat(), **stats,
                       'rows_by_clock': dict(Counter(result['decision_time'].to_list())),
                       'retained_state_rows': state.height})
        print(json.dumps(counts[-1]), flush=True)
    result = pl.concat(results).sort('decision_date', 'decision_time', 'product_raw', 'series_raw',
                                     'strike_decimal', 'right_raw')
    if result.estimated_size() > 64 * 1024**2:
        raise ValueError('derived window exceeds bounded output estimate; split it into smaller windows')
    if (sha256_file(policy_path) != policy_sha
            or any(sha256_file(Path(path)) != sha for path, sha in linked_pins.items())
            or any(sha256_file(Path(path)) != sha for path, sha in implementation_pins.items())
            or any(sha256_file(Path(s['receipt_path'])) != s['receipt_sha256']
                   or sha256_file(Path(s['path'])) != s['sha256'] for s in sources)):
        raise ValueError('source or implementation changed during the window')
    publication_capacity = capacity_check(output.parent)
    output.mkdir(parents=True, exist_ok=False)
    payload = output / 'observed_references.parquet'
    with tempfile.NamedTemporaryFile(dir=output, prefix='.reference-', suffix='.tmp', delete=False) as stream:
        temporary = Path(stream.name)
    try:
        result.write_parquet(temporary, compression='zstd', statistics=True)
        if temporary.stat().st_size > 128 * 1024**2:
            raise ValueError('derived window exceeds bounded output size')
        if not result.equals(pl.read_parquet(temporary)):
            raise ValueError('independent source-window parquet readback differs')
        durable_replace(temporary, payload)
    finally:
        temporary.unlink(missing_ok=True)
    identity = {'contract': CONTRACT, 'policy_sha256': policy_sha, 'sources': sources,
                'implementation_sha256': implementation_pins}
    fingerprint = hashlib.sha256(json.dumps(identity, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    receipt = {'contract': CONTRACT, 'generated_at_utc': datetime.now(timezone.utc).isoformat(),
               'source_dates': [day.isoformat() for day in dates], 'source_pins': sources,
               'data_fingerprint': fingerprint, 'policy_path': str(policy_path), 'policy_sha256': policy_sha,
               'policy_linked_pins': linked_pins, 'implementation_sha256': implementation_pins,
               'data_path': str(payload), 'data_sha256': sha256_file(payload),
               'rows': result.height, 'daily_counts': counts, 'training_ready': False,
               'capacity_at_start': initial_capacity, 'capacity_before_publication': publication_capacity,
               'limitations': [
                   'Source timestamps are assumed Asia/Taipei; historic PIT delivery is unproven.',
                   'Retained Float32 price and strike strings do not restore source-native decimal precision.',
                   'Current candidate windows do not prove historical sessions, listing or expiry eligibility.',
                   'No observations before the first selected source date; whole-market completeness is unproven.',
                   'Unknown products and invalid source rows are excluded and counted by source date.',
                   'Whole-second price ties stay ambiguous; no quotes, fills, fees, margin or account are inferred.',
                   'This candidate output is not a model, checkpoint or training admission.']}
    atomic_write_json(output / 'manifest.json', receipt)
    return receipt


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path('data_finmind/complement'))
    parser.add_argument('--policy', type=Path, required=True)
    parser.add_argument('--dates', type=date.fromisoformat, nargs='+', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    try:
        result = build(args.root, args.policy, args.dates, args.output)
    except CapacityNotAdmitted as error:
        print(json.dumps({'state': 'capacity_deferred', 'capacity': error.status}), flush=True)
        raise SystemExit(75) from error
    print(json.dumps({'manifest': str(args.output / 'manifest.json'), 'rows': result['rows'],
                      'training_ready': False}), flush=True)


if __name__ == '__main__':
    main()
