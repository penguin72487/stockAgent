#!/usr/bin/env python3
"""Compare observed option reference keys with post-session official daily rows.

This is a retrospective source diagnostic only: daily reports do not prove
what was listed, known, priced, or executable at either decision clock.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
import json
from pathlib import Path

import polars as pl

from downloader.artifact_io import atomic_write_json, sha256_file
from scripts.audit_taifex_option_original_daily_presence import _key
from scripts.build_taifex_option_reference_source_window import CONTRACT
from stockagent.data.tw_option_product_daily import iter_option_product_daily_batches, load_option_daily_source_plan


def daily_index(frames: list[pl.DataFrame]) -> tuple[dict, int]:
    """Collect native day/key evidence, retaining conflicting expiry texts."""
    index = defaultdict(lambda: {'sessions': set(), 'expiry_texts': set()})
    unsupported = 0
    for frame in frames:
        columns = ('date', 'product', 'series', 'strike_raw', 'right', 'session', 'contract_expiry_raw')
        for day, product, series, strike, right, session, expiry in frame.select(*columns).unique().iter_rows():
            key = _key(product, series, strike, right)
            if key is None:
                unsupported += 1
                continue
            evidence = index[(day, *key)]
            evidence['sessions'].add(session)
            evidence['expiry_texts'].add(expiry)
    return dict(index), unsupported


def expiry_state(day: date, evidence: dict | None) -> str:
    if evidence is None:
        return 'daily_key_absent'
    texts = evidence['expiry_texts']
    if len(texts) != 1:
        return 'multiple_expiry_texts'
    raw = next(iter(texts))
    if raw is None or raw == '':
        return 'expiry_missing'
    try:
        if len(raw) != 8 or not raw.isascii() or not raw.isdigit():
            raise ValueError
        scheduled = datetime.strptime(raw, '%Y%m%d').date()
    except ValueError:
        return 'expiry_invalid_text'
    if scheduled < day:
        return 'scheduled_date_before'
    return 'scheduled_date_on' if scheduled == day else 'scheduled_date_after'


def compare(references: pl.DataFrame, index: dict) -> dict:
    counts: dict[str, Counter] = defaultdict(Counter)
    for row in references.iter_rows(named=True):
        clock = row['decision_time']
        if clock not in {'08:45', '09:00'}:
            raise ValueError('unexpected reference decision clock')
        key = _key(row['product_raw'], row['series_raw'], row['strike_decimal'], row['right_raw'])
        counts[clock]['reference_rows'] += 1
        if key is None:
            counts[clock]['unsupported_reference_key'] += 1
            continue
        evidence = index.get((row['decision_date'], *key))
        counts[clock][expiry_state(row['decision_date'], evidence)] += 1
        if evidence is not None:
            counts[clock]['daily_key_present'] += 1
            if 'day' in evidence['sessions']:
                counts[clock]['daily_day_session_key_present'] += 1
    return {clock: dict(values) for clock, values in sorted(counts.items())}


def audit(window: Path, plan_path: Path, plan_sha: str) -> dict:
    manifest_path = window / 'manifest.json'
    manifest_sha = sha256_file(manifest_path)
    manifest = json.loads(manifest_path.read_bytes())
    if manifest.get('contract') != CONTRACT or manifest.get('training_ready') is not False:
        raise ValueError('source-only reference window is required')
    data_path = Path(manifest['data_path'])
    if sha256_file(data_path) != manifest['data_sha256']:
        raise ValueError('reference payload changed')
    refs = pl.read_parquet(data_path)
    days = set(refs['decision_date'].to_list())
    if not days or sorted(day.isoformat() for day in days) != sorted(manifest['archive_days']):
        raise ValueError('reference dates differ from pinned source window')
    sources = load_option_daily_source_plan(plan_path, expected_sha256=plan_sha)
    grouped: dict[int, set[date]] = {}
    for day in days:
        matching = [i for i, source in enumerate(sources) if source.start <= day <= source.end]
        if len(matching) != 1:
            raise ValueError(f'no unique daily source for {day}')
        grouped.setdefault(matching[0], set()).add(day)
    implementation = [Path(__file__).resolve(),
                      Path('stockagent/data/tw_option_product_daily.py').resolve(),
                      Path('scripts/audit_taifex_option_original_daily_presence.py').resolve()]
    pins = {str(path): sha256_file(path) for path in implementation}
    daily, unsupported = {}, 0
    used = []
    for source_index, selected_days in sorted(grouped.items()):
        source = sources[source_index]
        # The source reader is exhausted once per file and verifies the native SHA.
        frames = list(iter_option_product_daily_batches(source.path, expected_sha256=source.sha256,
                                                        trading_dates=selected_days))
        subset, invalid = daily_index(frames)
        if set(daily) & set(subset):
            raise ValueError('daily source plan contains overlapping keys')
        daily.update(subset)
        unsupported += invalid
        used.append({'path': str(source.path), 'sha256': source.sha256,
                     'selected_days': sorted(day.isoformat() for day in selected_days)})
    result = compare(refs, daily)
    if (sha256_file(manifest_path) != manifest_sha or sha256_file(data_path) != manifest['data_sha256']
            or sha256_file(plan_path) != plan_sha
            or any(sha256_file(Path(item['path'])) != item['sha256'] for item in used)
            or any(sha256_file(Path(path)) != digest for path, digest in pins.items())):
        raise ValueError('source or implementation changed during retrospective readback')
    return {'contract': 'option_reference_daily_source_diagnostic_v1',
            'observed_at_utc': datetime.now(timezone.utc).isoformat(),
            'window_manifest': str(manifest_path), 'window_manifest_sha256': manifest_sha,
            'window_data_sha256': manifest['data_sha256'], 'daily_plan': str(plan_path),
            'daily_plan_sha256': plan_sha, 'daily_source_files': used,
            'implementation_sha256': pins, 'archive_days': len(days),
            'supported_distinct_daily_contract_keys': len(daily),
            'unsupported_distinct_daily_source_rows': unsupported,
            'by_clock': result, 'training_ready': False,
            'limitations': [
                'Daily data are acquired after the session and cannot be backdated as decision-time knowledge.',
                'A matching key is not proof of same contract generation, listing, quote, trade or executable fill.',
                'Scheduled contract date is the original planned last trading day and may have changed.',
                'Absent daily rows do not prove no listing; both sources belong to the exchange.',
                'The source window omits earlier state and may miss market days; no account is certified.',
            ]}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--window', type=Path, required=True)
    parser.add_argument('--daily-plan', type=Path, required=True)
    parser.add_argument('--daily-plan-sha256', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise FileExistsError('preserve the previous diagnostic')
    result = audit(args.window.resolve(), args.daily_plan.resolve(), args.daily_plan_sha256)
    atomic_write_json(args.output, result)
    print(json.dumps({'output': str(args.output), 'days': result['archive_days'],
                      'by_clock': result['by_clock']}, ensure_ascii=False), flush=True)


if __name__ == '__main__':
    main()
