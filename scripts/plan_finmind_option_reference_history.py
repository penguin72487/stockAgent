#!/usr/bin/env python3
"""Plan bounded source-only option-reference windows from prior readbacks.

Only receipt-verified days from the supplied official-positive-day inventories
are planned. Consecutive natural dates are grouped, with state reset at every
gap or window boundary. This metadata is not a source download or training job.
"""
from __future__ import annotations

import argparse
from collections import Counter
import csv
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import re

from downloader.artifact_io import atomic_write_json, sha256_file


CONTRACT = 'finmind_option_reference_history_plan_v1'


def partition_days(rows: list[dict], maximum_days: int = 5) -> list[list[dict]]:
    if not 1 <= maximum_days <= 31:
        raise ValueError('window size must be between 1 and 31 days')
    result: list[list[dict]] = []
    for row in rows:
        day = date.fromisoformat(row['date'])
        if result and day <= date.fromisoformat(result[-1][-1]['date']):
            raise ValueError('source days must be ordered and unique')
        if (not result or len(result[-1]) >= maximum_days
                or day - date.fromisoformat(result[-1][-1]['date']) != timedelta(days=1)):
            result.append([])
        result[-1].append(row)
    return result


def build(summaries: list[Path], policy_path: Path, output: Path, *, maximum_days: int = 5) -> dict:
    if not summaries or output.exists() or output.is_symlink():
        raise ValueError('source summaries and a new plan path are required')
    policy_sha = sha256_file(policy_path)
    policy = json.loads(policy_path.read_bytes())
    if policy.get('contract') != 'taifex_option_session_policy_candidate_v1' or policy.get('research_candidate_only') is not True:
        raise ValueError('an explicit research-only policy is required')
    source_pins, all_rows = [], []
    for summary_path in summaries:
        summary_path = summary_path.resolve()
        summary_sha = sha256_file(summary_path)
        summary = json.loads(summary_path.read_bytes())
        inventory_path = Path(summary['day_inventory']).resolve()
        if sha256_file(inventory_path) != summary['day_inventory_sha256']:
            raise ValueError('day inventory differs from its pinned source summary')
        with inventory_path.open(newline='', encoding='utf-8') as stream:
            rows = list(csv.DictReader(stream))
        for row in rows:
            if 'day_or_unreported_positive_candidate' in row and row['day_or_unreported_positive_candidate'] != 'True':
                continue
            day = date.fromisoformat(row['date'])
            status = row.get('finmind_status', '')
            receipt_sha, data_sha = row.get('finmind_receipt_sha256', ''), row.get('finmind_parquet_sha256', '')
            ready = (status == 'complete_payload_sha_verified'
                     and re.fullmatch(r'[0-9a-f]{64}', receipt_sha) is not None
                     and re.fullmatch(r'[0-9a-f]{64}', data_sha) is not None)
            all_rows.append({'date': day.isoformat(), 'prior_readback_status': status,
                             'receipt_sha256': receipt_sha if ready else None,
                             'parquet_sha256': data_sha if ready else None, 'planned': ready})
        source_pins.append({'summary_path': str(summary_path), 'summary_sha256': summary_sha,
                            'inventory_path': str(inventory_path), 'inventory_sha256': summary['day_inventory_sha256']})
    all_rows.sort(key=lambda row: row['date'])
    if len({row['date'] for row in all_rows}) != len(all_rows):
        raise ValueError('source inventories contain duplicate days')
    groups = partition_days([row for row in all_rows if row['planned']], maximum_days)
    windows = []
    for group in groups:
        fingerprint = hashlib.sha256(json.dumps(group, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
        windows.append({'start': group[0]['date'], 'end': group[-1]['date'],
                        'source_days': len(group), 'source_receipt_pins': group,
                        'source_group_fingerprint': fingerprint,
                        'prior_before_first_source_unknown': True})
    if (sha256_file(policy_path) != policy_sha
            or any(sha256_file(Path(pin['summary_path'])) != pin['summary_sha256']
                   or sha256_file(Path(pin['inventory_path'])) != pin['inventory_sha256'] for pin in source_pins)):
        raise ValueError('a planning input changed during enumeration')
    plan = {'contract': CONTRACT, 'generated_at_utc': datetime.now(timezone.utc).isoformat(),
            'policy_path': str(policy_path.resolve()), 'policy_sha256': policy_sha,
            'source_inventory_pins': source_pins, 'maximum_days_per_window': maximum_days,
            'official_positive_source_days': len(all_rows),
            'receipt_verified_candidate_days': sum(row['planned'] for row in all_rows),
            'not_planned_days': [row for row in all_rows if not row['planned']],
            'readback_status_counts': dict(Counter(row['prior_readback_status'] for row in all_rows)),
            'windows': windows, 'training_ready': False,
            'limitations': [
                'Only the supplied official-positive-day inventories are in scope; dates outside them are unknown.',
                'Prior receipts are evidence at their readback time; every source must be re-pinned before processing.',
                'Every gap and window boundary resets prior state; it does not assert a closed market.',
                'Vendor timestamps, decimal precision, source completeness, sessions and listing remain unverified.',
                'This is a processing plan, not data completion, executable prices or training admission.']}
    atomic_write_json(output, plan)
    return plan


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--summary', type=Path, action='append', required=True)
    parser.add_argument('--policy', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--maximum-days', type=int, default=5)
    args = parser.parse_args()
    plan = build(args.summary, args.policy, args.output, maximum_days=args.maximum_days)
    print(json.dumps({'output': str(args.output), 'official_positive_source_days': plan['official_positive_source_days'],
                      'receipt_verified_candidate_days': plan['receipt_verified_candidate_days'],
                      'windows': len(plan['windows']), 'not_planned_days': len(plan['not_planned_days']),
                      'training_ready': False}), flush=True)


if __name__ == '__main__':
    main()
