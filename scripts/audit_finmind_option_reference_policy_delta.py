#!/usr/bin/env python3
"""Audit newly observable option reference keys under two candidate policies."""
from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import json
from pathlib import Path

import polars as pl

from downloader.artifact_io import atomic_write_json, sha256_file
from scripts.build_taifex_historical_option_candidate_policy import assemble
from scripts.build_taifex_option_finmind_reference_window import candidate_policy, pin_source
from stockagent.data.tw_option_finmind_reference import candidate_rows
from stockagent.data.tw_option_reference_candidates import KEYS, reference_candidates


def compare(frame: pl.DataFrame, day: date, source_sha: str, old_windows: dict, new_windows: dict) -> dict:
    if not set(old_windows).issubset(new_windows) or any(old_windows[p] != new_windows[p] for p in old_windows):
        raise ValueError('the historical policy must preserve all prior windows')
    old, old_stats = candidate_rows(frame, day=day, source_sha256=source_sha,
                                    candidate_products=set(old_windows))
    new, new_stats = candidate_rows(frame, day=day, source_sha256=source_sha,
                                    candidate_products=set(new_windows))
    clocks = {}
    for clock in ('08:45', '09:00'):
        before = reference_candidates(old, trading_date=day, decision_time=clock, session_windows=old_windows)
        after = reference_candidates(new, trading_date=day, decision_time=clock, session_windows=new_windows)
        old_after = after.filter(pl.col('product_raw').is_in(sorted(old_windows)))
        if not before.equals(old_after):
            raise ValueError('extension changed an existing candidate reference')
        extra = after.join(before.select(*KEYS), on=KEYS, how='anti')
        clocks[clock] = {'previous_candidate_keys': before.height, 'extended_candidate_keys': after.height,
                         'new_candidate_keys': extra.height,
                         'new_candidate_keys_by_product': (extra.group_by('product_raw').len()
                             .sort('product_raw').to_dicts())}
    return {'previous_source_rows': old_stats, 'extended_source_rows': new_stats, 'clocks': clocks}


def build(root: Path, day: date, base: Path, extended: Path, summary: Path, output: Path) -> dict:
    if output.exists() or output.is_symlink():
        raise ValueError('preserve the previous independent readback')
    if json.loads(extended.read_bytes()) != assemble(base, summary):
        raise ValueError('historical policy differs from its pinned source inventory')
    base_sha, extended_sha, summary_sha = (sha256_file(p) for p in (base, extended, summary))
    old_windows, _ = candidate_policy(base)
    new_windows, _ = candidate_policy(extended)
    source = pin_source(root, day)
    comparison = compare(pl.read_parquet(source['path']), day, source['sha256'], old_windows, new_windows)
    if (sha256_file(base) != base_sha or sha256_file(extended) != extended_sha
            or sha256_file(summary) != summary_sha or sha256_file(Path(source['path'])) != source['sha256']
            or sha256_file(Path(source['receipt_path'])) != source['receipt_sha256']):
        raise ValueError('source or policy changed during the comparison')
    report = {'contract': 'finmind_option_reference_policy_delta_v1',
              'generated_at_utc': datetime.now(timezone.utc).isoformat(), 'day': day.isoformat(),
              'source_pin': source, 'base_policy_sha256': base_sha, 'extended_policy_sha256': extended_sha,
              'inventory_summary_sha256': summary_sha, 'comparison': comparison,
              'training_ready': False,
              'limitations': ['One retained day only; no observations before this day are included.',
                              'Both outputs use the same observed source and reducer.',
                              'Retained Float32, historical sessions, eligibility and PIT delivery remain unverified.',
                              'Reference candidates do not prove executable prices, margins or account admission.']}
    atomic_write_json(output, report)
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path('data_finmind/complement'))
    parser.add_argument('--date', type=date.fromisoformat, required=True)
    parser.add_argument('--base', type=Path, required=True)
    parser.add_argument('--extended', type=Path, required=True)
    parser.add_argument('--summary', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    report = build(args.root, args.date, args.base, args.extended, args.summary, args.output)
    print(json.dumps({'output': str(args.output), 'comparison': report['comparison']}), flush=True)


if __name__ == '__main__':
    main()
