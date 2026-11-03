#!/usr/bin/env python3
"""Audit a bounded recent option archive using each pinned daily source once."""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timezone
import json
from pathlib import Path

from downloader.artifact_io import atomic_write_json, sha256_file
from scripts.audit_taifex_option_original_daily_presence import compare_pinned_day, read_daily_keys
from stockagent.data.tw_futures_official_ticks import pin_official_option_tick_inventory
from stockagent.data.tw_option_product_daily import load_option_daily_source_plan


def audit(start: date, end: date, plan_path: Path, plan_sha256: str,
          tick_manifest: Path, workers: int) -> dict:
    if start > end or not 1 <= workers <= 16:
        raise ValueError('an ordered date range and valid worker count are required')
    implementation = [Path(__file__).resolve(),
                      Path('scripts/audit_taifex_option_original_daily_presence.py').resolve(),
                      Path('stockagent/data/tw_option_product_ticks.py').resolve(),
                      Path('stockagent/data/tw_option_product_daily.py').resolve()]
    pins = {str(path): sha256_file(path) for path in implementation}
    manifest_before = sha256_file(tick_manifest)
    manifest = json.loads(tick_manifest.read_bytes())
    dates = sorted(date.fromisoformat(x['trading_date']) for x in manifest['raw_downloads']
                   if x['kind'] == 'options' and start <= date.fromisoformat(x['trading_date']) <= end)
    if not dates or len(dates) != len(set(dates)):
        raise ValueError('missing or duplicated option dates in canonical manifest')
    sources = pin_official_option_tick_inventory(tick_manifest, dates)
    daily_sources = load_option_daily_source_plan(plan_path, expected_sha256=plan_sha256)
    grouped: dict[int, set[date]] = {}
    for day in dates:
        candidates = [i for i, source in enumerate(daily_sources) if source.start <= day <= source.end]
        if len(candidates) != 1:
            raise ValueError(f'no unambiguous official daily source for {day}')
        grouped.setdefault(candidates[0], set()).add(day)
    daily: dict[date, dict] = {}
    for index, days in grouped.items():
        source = daily_sources[index]
        daily.update(read_daily_keys(days, source.path, source.sha256))
    with ThreadPoolExecutor(max_workers=workers) as executor:
        rows = list(executor.map(lambda source: compare_pinned_day(
            date.fromisoformat(source['date']), daily[date.fromisoformat(source['date'])], source), sources))
    if (sha256_file(tick_manifest) != manifest_before or sha256_file(plan_path) != plan_sha256
            or any(sha256_file(Path(path)) != digest for path, digest in pins.items())):
        raise ValueError('source manifest, plan or audit implementation changed during readback')
    return {
        'contract': 'official_option_recent_daily_vs_original_tick_contract_existence_v1',
        'observed_at_utc': datetime.now(timezone.utc).isoformat(), 'start': str(start), 'end': str(end),
        'daily_source_plan': str(plan_path.resolve()), 'daily_source_plan_sha256': plan_sha256,
        'tick_manifest': str(tick_manifest.resolve()), 'tick_manifest_sha256': manifest_before,
        'implementation_sha256': pins, 'workers': workers, 'audited_dates': len(rows),
        'archive_side_rows': sum(row['archive_side_rows'] for row in rows),
        'natural_day_side_rows': sum(row['natural_day_side_rows'] for row in rows),
        'expected_contract_days': sum(row['official_positive_supported_contracts'] for row in rows),
        'matched_contract_days': sum(row['expected_contracts_with_tick'] for row in rows),
        'missing_contract_days': sum(row['expected_contracts_without_tick'] for row in rows),
        'unsupported_daily_key_rows': sum(row['unsupported_daily_key_rows'] for row in rows),
        'days': rows,
        'limitations': [
            'Only dates in the pinned recent manifest and requested range are audited.',
            'Daily and tick sources are both TAIFEX, not independent providers.',
            'A natural-day key can include a night trade; existence is not day-session or event completeness.',
            'Side rows do not prove executable quotes, causal clock, listing, account rules or training readiness.',
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--start', type=date.fromisoformat, required=True)
    parser.add_argument('--end', type=date.fromisoformat, required=True)
    parser.add_argument('--daily-plan', type=Path, required=True)
    parser.add_argument('--daily-plan-sha256', required=True)
    parser.add_argument('--tick-manifest', type=Path, default=Path('data_tw_index_derivatives_ticks/manifest.json'))
    parser.add_argument('--workers', type=int, default=4)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise FileExistsError('preserve the previous audit; choose a new output path')
    result = audit(args.start, args.end, args.daily_plan, args.daily_plan_sha256,
                   args.tick_manifest, args.workers)
    atomic_write_json(args.output, result)
    print(f"{result['audited_dates']} dates: {result['matched_contract_days']}/"
          f"{result['expected_contract_days']} contract-days present; output={args.output}")


if __name__ == '__main__':
    main()
