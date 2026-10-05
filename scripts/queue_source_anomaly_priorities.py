#!/usr/bin/env python3
"""Dispatch finite quality intents to canonical market collectors, never TEJ.

Reads fixed audit files or a committed live audit checkpoint. No provider calls
or alternate quota buckets. The owning collector still validates actual dates,
rechecks upstream, preserves healthy history and records bounded attempts.
"""
from __future__ import annotations

import argparse
import csv
from datetime import UTC, datetime, timedelta
import fcntl
import hashlib
import json
from pathlib import Path
import sqlite3
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.quality_priority import CONTRACT, load_plan, pending_requests
from downloader.source_anomalies import market_profile, file_binding
from argparse import Namespace


def yahoo_candidates(root: Path, report: Path):
    """Confirm stored value/structure evidence; heuristic stale/gaps are not facts."""
    digest = sha256_file(report)
    args = Namespace(end_date='today', stale_max_lag_days=14, daily_gap_days=10, intraday_gap_multiple=4)
    with report.open(newline='') as stream:
        for row in csv.DictReader(stream):
            path = Path(row['path'])
            path = path if path.is_absolute() else root / path
            if not path.resolve().is_relative_to(root.resolve()):
                continue
            relative = path.relative_to(root)
            legacy_crypto = len(relative.parts) == 3 and relative.parts[:2] == ('data_yahoo', 'crypto')
            current_crypto = len(relative.parts) == 4 and relative.parts[:3] == ('data_yahoo', 'crypto', '1m')
            daily_market = len(relative.parts) == 3 and relative.parts[0] == 'data_yahoo' and relative.parts[1] in {'us_stocks', 'forex'}
            if not (legacy_crypto or current_crypto or daily_market) or not relative.name.endswith('_features.parquet'):
                continue
            flagged = row.get('status') == 'failed' or any(
                int(row.get(key) or 0) for key in ('bad_ohlc_rows', 'nonpositive_price_rows', 'negative_volume_rows', 'nan_ohlc_rows'))
            if not flagged:
                continue
            try:
                before = file_binding(path)
            except OSError:
                continue  # Disappearance is not stable corrupt-byte evidence.
            try:
                evidence = market_profile(path, args, crypto_1m=current_crypto)
                if evidence.get('status') == 'changed_during_check' or not evidence.get('confirmed_value_error'):
                    continue
                reason = 'invalid_values'
                start = evidence.get('source_recheck_start_date')
                identity = {'invalid_observed_rows': evidence['invalid_observed_rows'],
                            'first_bad_date': start}
            except Exception as exc:
                if type(exc).__name__ not in {'ArrowInvalid', 'ComputeError', 'FileNotFoundError'}:
                    continue  # Unknown scanner failure is not proven source corruption.
                reason, start, identity = 'structural_integrity_failure', None, before
            try:
                stable = file_binding(path) == before
            except OSError:
                stable = False
            if not stable:
                continue
            yield path.parent, {'code': relative.name.removesuffix('_features.parquet'), 'reason': reason,
                'evidence_sha256': digest, 'audit_path': str(report.resolve()),
                'repair_start_date': start, 'asset_class': relative.parts[1],
                'initial_state': 'legacy_grain_review' if legacy_crypto else 'pending',
                'dispatch_scope': ('legacy_daily_not_current_1m_collector_no_overwrite' if legacy_crypto
                                   else 'current_native_market_collector'),
                'repair_identity_sha256': hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest(),
                'theoretical_basis': 'stored_value_or_structure; not_heuristic_staleness_or_calendar_gap'}


def candidates(root: Path, audit: Path, *, live: bool):
    if live:
        with sqlite3.connect(f'file:{audit / "scan.sqlite3"}?mode=ro', uri=True, timeout=10) as connection:
            rows = [json.loads(row[0]) for row in connection.execute(
                "SELECT result FROM files WHERE json_extract(result,'$.status') "
                "IN ('gap_candidate','invalid_values','invalid_timestamp_keys','duplicate_timestamp_keys')")]
        digest = hashlib.sha256(json.dumps(rows, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
    else:
        manifest = json.loads((audit / 'audit_manifest.json').read_text())
        digest = sha256_file(audit / 'issues.csv')
        if manifest['outputs']['issues.csv']['sha256'] != digest or not manifest.get('TEJ_excluded'):
            raise ValueError('audit evidence mismatch or TEJ scope not excluded')
        with (audit / 'issues.csv').open(newline='') as stream:
            rows = list(csv.DictReader(stream))
    for row in rows:
        path = Path(row['path'])
        if not path.resolve().is_relative_to(root.resolve()):
            continue
        relative = path.relative_to(root)
        if len(relative.parts) != 3 or not relative.name.endswith('_features.parquet'):
            continue
        if relative.parts[0] not in {'data_binance', 'data_okx', 'data_bybit'} or relative.parts[1] != '1m':
            continue
        # Footer infinities can belong to derived/log features rather than
        # candles. Never send a candle download to repair another grain.
        if row.get('validation') not in {'every_logical_OHLCV_value_and_key; linear_bounded_batch_scan',
                                          'logical_OHLCV_values_and_timestamp_keys'}:
            continue
        status = row['status']
        if status not in {'gap_candidate', 'invalid_values', 'invalid_timestamp_keys', 'duplicate_timestamp_keys'}:
            # A file changing during audit is not stable anomaly evidence.
            continue
        if status == 'invalid_values' and row.get('confirmed_value_error') not in {True, 'True'}:
            continue
        reason = 'timestamp_keys' if status.endswith('timestamp_keys') else status
        windows = row.get('repair_windows_ms') or []
        if isinstance(windows, str):
            windows = json.loads(windows)
        witness = windows or {k: row.get(k) for k in ('invalid_timestamps', 'off_grid_rows', 'duplicate_excess_rows')}
        yield path.parent, {'code': relative.name.removesuffix('_features.parquet'), 'reason': reason,
            'evidence_sha256': digest, 'audit_path': str(audit.resolve()),
            'repair_identity_sha256': hashlib.sha256(json.dumps(witness, sort_keys=True).encode()).hexdigest(),
            'missing_minutes_candidate': row.get('missing_minutes_in_observed_span'),
            'invalid_observed_rows': row.get('invalid_observed_rows'),
            'theoretical_basis': 'actual_completed_observed_span_not_source_availability_proof'}


def enqueue(root: Path, audit: Path, *, live: bool, now: datetime, yahoo_report: Path | None = None):
    grouped = {}
    for owner, item in candidates(root, audit, live=live):
        grouped.setdefault(owner, []).append(item)
    if yahoo_report is not None:
        for owner, item in yahoo_candidates(root, yahoo_report):
            grouped.setdefault(owner, []).append(item)
    receipts = []
    for owner, incoming in grouped.items():
        with (owner / '.quality_priority.lock').open('a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            plan = load_plan(owner)
            existing = {r['request_id'] for r in plan['requests']}
            added = 0
            for item in incoming:
                # A repeated audit must not reset an unresolved symbol's cap.
                identity = '\0'.join([str(owner.relative_to(root)), item['code'], item['reason'], item['repair_identity_sha256']])
                request_id = hashlib.sha256(identity.encode()).hexdigest()
                known = next((row for row in plan['requests'] if row['request_id'] == request_id or (
                    row['code'] == item['code'] and row['reason'] == item['reason']
                    and row.get('repair_identity_sha256') == item['repair_identity_sha256'])), None)
                if known is not None:
                    if (item.get('initial_state') == 'legacy_grain_review' and known['attempts'] == 0
                            and known['state'] == 'pending' and known.get('audit_path') == item.get('audit_path')):
                        known.update(state='legacy_grain_review', dispatch_scope=item['dispatch_scope'])
                    continue
                # Adopt a previous v1 identity once, without duplicating work
                # or resetting its retry cap. Subsequent distinct windows get
                # their own identity and never revive this old observation.
                legacy = next((row for row in plan['requests']
                    if row['code'] == item['code'] and row['reason'] == item['reason']
                    and not row.get('repair_identity_sha256')), None)
                if legacy is not None:
                    legacy['repair_identity_sha256'] = item['repair_identity_sha256']
                    continue
                plan['requests'].append({**item, 'request_id': request_id, 'attempts': 0,
                    'state': item.get('initial_state', 'pending'),
                    'created_at_utc': now.isoformat(), 'expires_at_utc': (now + timedelta(days=7)).isoformat()})
                existing.add(request_id); added += 1
            if len(plan['requests']) > 4096:
                raise ValueError('quality intent count bound exceeded')
            path = owner / 'quality_priority_requests.json'
            atomic_write_json(path, plan); path.chmod(0o600)
            receipts.append({'owner_root': str(owner), 'added': added,
                             'pending': len(pending_requests(owner)), 'intent_sha256': sha256_file(path)})
    return {'contract': CONTRACT, 'observed_at_utc': now.isoformat(), 'TEJ_excluded': True,
            'network_calls': 0, 'gap_fills_claimed': 0, 'owners': receipts,
            'dispatch': 'existing_canonical_historical_collectors; due_tail_refresh_unaffected'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=ROOT)
    parser.add_argument('--audit-dir', type=Path, required=True)
    parser.add_argument('--live-checkpoint', action='store_true')
    parser.add_argument('--yahoo-ohlcv-report', type=Path)
    parser.add_argument('--receipt', type=Path, required=True)
    args = parser.parse_args()
    result = enqueue(args.root.resolve(), args.audit_dir, live=args.live_checkpoint,
                     now=datetime.now(UTC), yahoo_report=args.yahoo_ohlcv_report)
    atomic_write_json(args.receipt, result)
    print(json.dumps(result, ensure_ascii=False))


if __name__ == '__main__':
    main()
