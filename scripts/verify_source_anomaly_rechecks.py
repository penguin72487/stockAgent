#!/usr/bin/env python3
"""Verify source rechecks against logical values, not HTTP success or job state."""
from __future__ import annotations

import argparse
from argparse import Namespace
import csv
from datetime import UTC, datetime
import fcntl
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.source_anomalies import market_profile, footer_profile
from downloader.quality_priority import load_plan
from scripts.audit_source_anomalies import csv_file


def acknowledge(root: Path, code: str, result: dict, output: Path) -> None:
    """A successfully returned response is not evidence that the missing data exists."""
    if result['status'] == 'changed_during_check':
        return
    with (root / '.quality_priority.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        plan = load_plan(root)
        for row in plan['requests']:
            if row['code'] != code or row['state'] != 'source_checked_not_fill_proof':
                continue
            clear = result['status'] == 'values_checked'
            row['state'] = 'verified_logical_price_clear' if clear else 'source_rechecked_anomaly_remains'
            row['verification'] = {
                'observed_at_utc': datetime.now(UTC).isoformat(),
                'audit_path': str(output.resolve()),
                'logical_binding_sha256': hashlib.sha256(
                    json.dumps(result.get('file_binding'), sort_keys=True).encode()).hexdigest(),
                'status': result['status'],
                'missing_minutes_in_observed_span': result.get('missing_minutes_in_observed_span'),
                'invalid_observed_rows': result.get('invalid_observed_rows'),
                'all_financial_features_checked': False,
                'gap_filled': clear,
            }
        path = root / 'quality_priority_requests.json'
        atomic_write_json(path, plan)
        path.chmod(0o600)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=ROOT)
    parser.add_argument('--audit-dir', type=Path, required=True)
    parser.add_argument('--output-dir', type=Path, required=True)
    parser.add_argument('--acknowledge', action='store_true')
    args = parser.parse_args()
    out = args.output_dir
    manifest = json.loads((args.audit_dir / 'audit_manifest.json').read_text())
    if (not manifest.get('TEJ_excluded') or
            sha256_file(args.audit_dir / 'issues.csv') != manifest['outputs']['issues.csv']['sha256']):
        raise ValueError('original audit evidence not verified')
    with (args.audit_dir / 'issues.csv').open(newline='') as stream:
        issues = list(csv.DictReader(stream))
    completed = []
    options = Namespace(end_date='today', stale_max_lag_days=14, daily_gap_days=10, intraday_gap_multiple=4)
    for row in issues:
        path = Path(row['path'])
        relative = path.relative_to(args.root)
        if len(relative.parts) != 3 or relative.parts[0] not in {'data_binance', 'data_bybit', 'data_okx'}:
            continue
        if relative.parts[1] != '1m' or not path.name.endswith('_features.parquet'):
            continue
        result = market_profile(path, options, crypto_1m=True)
        result['before_status'] = row['status']
        result['before_missing_minutes_candidate'] = row.get('missing_minutes_in_observed_span')
        result['before_invalid_observed_rows'] = row.get('invalid_observed_rows')
        completed.append(result)
        print(f"[source-acceptance] {relative.parts[0]}:{path.name} status={result['status']} "
              f"missing={result.get('missing_minutes_in_observed_span')} bad={result.get('invalid_observed_rows')}", flush=True)
        atomic_write_json(out / 'progress.json', {'state': 'verifying_logical_source_values',
            'completed': len(completed), 'observed_at_utc': datetime.now(UTC).isoformat()})
    # Source ratios with a zero denominator are not candle acquisition errors.
    feature_results = [footer_profile(args.root / 'data_binance/1m' / f'{code}_features.parquet')
                       for code in ('ICXUSDT', 'SCRTUSDT')]
    finlab_results = []
    for receipt_path in (args.root / 'data_finlab/receipts').glob('*.json'):
        receipt = json.loads(receipt_path.read_text())
        if receipt.get('dataset') not in {'etl:us_market_value', 'fundamental_features:營業費用率'}:
            continue
        relative = Path(receipt['parquet_path'])
        if relative.is_absolute() or '..' in relative.parts:
            raise ValueError('unsafe FinLab receipt reference')
        path = args.root / 'data_finlab' / relative
        result = footer_profile(path)
        result.update(dataset=receipt['dataset'], receipt_sha256=sha256_file(receipt_path))
        finlab_results.append(result)
    atomic_write_json(out / 'logical_price_verification.json', completed)
    csv_file(out / 'logical_price_verification.csv', completed)
    atomic_write_json(out / 'nonfinite_feature_verification.json', {
        'binance_non_price_features': feature_results, 'finlab_current_receipt_heads': finlab_results})
    if args.acknowledge:
        for result in completed:
            path = Path(result['path'])
            acknowledge(path.parent, path.name.removesuffix('_features.parquet'), result, out)
    summary = {
        'contract': 'source_quality_recheck_acceptance_v1', 'TEJ_excluded': True,
        'observed_at_utc': datetime.now(UTC).isoformat(), 'network_calls': 0,
        'verified_candle_files': len(completed),
        'before_missing_minutes_candidate': sum(int(r.get('before_missing_minutes_candidate') or 0)
            for r in completed if r['before_status'] != 'changed_during_check'),
        'remaining_missing_minutes_candidate': sum(r.get('missing_minutes_in_observed_span') or 0
            for r in completed if r['status'] != 'changed_during_check'),
        'remaining_invalid_rows': sum(r.get('invalid_observed_rows') or 0
            for r in completed if r['status'] != 'changed_during_check'),
        'changed_during_verification': sum(r['status'] == 'changed_during_check' for r in completed),
        'finlab_remaining_infinite_cells': sum(r.get('infinite_values') or 0 for r in finlab_results),
        'raw_source_files_not_rewritten_by_verifier': True,
        'all_source_history_or_field_semantics_complete': False,
        'outputs': {p.name: {'sha256': sha256_file(p)} for p in out.glob('*.json')
                    if p.name not in {'summary.json', 'progress.json'}},
    }
    atomic_write_json(out / 'summary.json', summary)
    print(json.dumps(summary, ensure_ascii=False))


if __name__ == '__main__':
    main()
