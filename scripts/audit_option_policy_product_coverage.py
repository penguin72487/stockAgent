#!/usr/bin/env python3
"""Compare observed official option products with a pinned candidate policy.

This is a source-coverage audit, not a historical listing, execution or
accounting admission. Unknown and invalid volume must remain distinguishable
from zero, and a policy cannot establish dated tradability.
"""
from __future__ import annotations

import argparse
from datetime import UTC, date, datetime
import json
from pathlib import Path

import polars as pl

from downloader.artifact_io import atomic_write_json, sha256_file
from scripts.build_taifex_option_finmind_reference_window import candidate_policy
from stockagent.data.tw_option_product_daily import (
    iter_option_product_daily_window, load_option_daily_source_plan,
)


def audit(plan_path: Path, policy_path: Path, start: date, end: date) -> dict:
    plan_path, policy_path = plan_path.resolve(), policy_path.resolve()
    plan_sha, policy_sha = sha256_file(plan_path), sha256_file(policy_path)
    implementations = [Path(__file__).resolve(),
                       Path('stockagent/data/tw_option_product_daily.py').resolve(),
                       Path('scripts/build_taifex_option_finmind_reference_window.py').resolve(),
                       Path('scripts/audit_taifex_option_source_universe.py').resolve(),
                       Path('stockagent/data/tw_index_futures.py').resolve()]
    pins = {str(path): sha256_file(path) for path in implementations}
    windows, linked = candidate_policy(policy_path)
    sources = load_option_daily_source_plan(plan_path, expected_sha256=plan_sha)
    observed: dict[str, dict] = {}
    for frame in iter_option_product_daily_window(sources, start=start, end=end):
        volume = pl.col('volume_raw').str.strip_chars().str.replace_all(',', '').cast(pl.Int64, strict=False)
        grouped = frame.group_by('product', 'date').agg(
            pl.len().alias('source_rows'),
            (volume > 0).fill_null(False).sum().alias('positive_rows'),
            (volume.is_null() | (volume < 0)).sum().alias('invalid_volume_rows'),
        )
        for product, day, rows, positive, invalid in grouped.iter_rows():
            item = observed.setdefault(product, {'days': set(), 'positive_days': set(),
                                                  'rows': 0, 'positive_rows': 0, 'invalid_rows': 0})
            item['days'].add(day)
            if positive:
                item['positive_days'].add(day)
            item['rows'] += rows
            item['positive_rows'] += positive
            item['invalid_rows'] += invalid
    if not observed:
        raise ValueError('no official option observations in the selected source window')
    if (sha256_file(plan_path) != plan_sha or sha256_file(policy_path) != policy_sha
            or any(sha256_file(Path(path)) != digest for path, digest in (pins | linked).items())):
        raise ValueError('source policy, plan or implementation changed during audit')
    products = []
    for product, item in sorted(observed.items()):
        days, positive = item['days'], item['positive_days']
        products.append({'product': product, 'in_policy': product in windows,
                         'first_observed_date': min(days).isoformat(),
                         'last_observed_date': max(days).isoformat(),
                         'observed_days': len(days), 'positive_volume_days': len(positive),
                         'first_positive_volume_date': min(positive).isoformat() if positive else None,
                         'last_positive_volume_date': max(positive).isoformat() if positive else None,
                         'source_rows': item['rows'], 'positive_volume_rows': item['positive_rows'],
                         'invalid_volume_rows': item['invalid_rows']})
    missing = [item['product'] for item in products if not item['in_policy']]
    missing_positive = [item['product'] for item in products
                        if not item['in_policy'] and item['positive_volume_days']]
    return {'contract': 'taifex_option_observed_product_policy_coverage_v1',
            'observed_at_utc': datetime.now(UTC).isoformat(), 'start': str(start), 'end': str(end),
            'source_plan': str(plan_path), 'source_plan_sha256': plan_sha,
            'policy': str(policy_path), 'policy_sha256': policy_sha, 'policy_linked_pins': linked,
            'implementation_sha256': pins,
            'source_windows': [{'path': str(x.path), 'sha256': x.sha256,
                                'start': str(x.start), 'end': str(x.end)}
                               for x in sources if x.end >= start and x.start <= end],
            'observed_products': len(products),
            'positive_volume_products': sum(item['positive_volume_days'] > 0 for item in products),
            'unknown_observed_products': missing, 'unknown_positive_volume_products': missing_positive,
            'invalid_volume_rows': sum(item['invalid_volume_rows'] for item in products),
            'products': products, 'training_ready': False,
            'limitations': [
                'Only rows in the pinned source window are compared; a source day is not proof of a complete listing universe.',
                'Positive daily volume proves neither an 08:45/09:00 trade nor an executable fill.',
                'Policy presence does not prove historic sessions, contract eligibility, currency or accounting.',
                'Unclassified rows and invalid volume do not prove absence of trading or data.',
            ]}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--plan', required=True, type=Path)
    parser.add_argument('--policy', required=True, type=Path)
    parser.add_argument('--start', required=True, type=date.fromisoformat)
    parser.add_argument('--end', required=True, type=date.fromisoformat)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--strict', action='store_true', help='Exit nonzero for observed products outside the policy')
    args = parser.parse_args()
    if args.output.exists() or args.output.is_symlink():
        raise FileExistsError('preserve previous research audit; choose a new output')
    result = audit(args.plan, args.policy, args.start, args.end)
    atomic_write_json(args.output, result)
    print(json.dumps({'observed_products': result['observed_products'],
                      'positive_volume_products': result['positive_volume_products'],
                      'unknown_observed_products': result['unknown_observed_products'],
                      'invalid_volume_rows': result['invalid_volume_rows'],
                      'output': str(args.output), 'training_ready': False}, ensure_ascii=False), flush=True)
    return 2 if args.strict and result['unknown_observed_products'] else 0


if __name__ == '__main__':
    raise SystemExit(main())
