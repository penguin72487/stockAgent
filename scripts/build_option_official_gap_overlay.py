#!/usr/bin/env python3
"""Build one receipt-bound official fallback across a FinMind source gap.

Only the explicitly selected dates and observed prints are covered. Original
vendor and official files are retained separately; no provider receipt is
rewritten and no contract, price or option account is admitted to training.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import hashlib
import json
from pathlib import Path
import tempfile

import polars as pl

from downloader.artifact_io import atomic_write_json, durable_replace, sha256_file
from scripts.build_taifex_option_calendar_event_index import CONTRACT as OFFICIAL_CONTRACT
from scripts.build_taifex_option_finmind_reference_window import (
    CapacityNotAdmitted, candidate_policy, capacity_check, pin_source,
)
from scripts.run_finmind_option_reference_history import window_dates
from stockagent.data.tw_option_finmind_reference import candidate_rows
from stockagent.data.tw_option_product_ticks import iter_official_option_trade_batches
from stockagent.data.tw_option_reference_candidates import _normalize, compact_last_observations, reference_candidates
from stockagent.data.tw_option_reference_event_index import SOURCE_COLUMNS


CONTRACT = 'option_official_gap_overlay_research_v1'
KEYS = ['product_raw', 'series_raw', 'strike_decimal', 'right_raw']


def check_strike_identity(official: pl.DataFrame, vendor: pl.DataFrame) -> dict:
    """Fail rather than join different exact strikes sharing one Float32 key."""
    group = ['product_raw', 'series_raw', 'right_raw']

    def keys(frame: pl.DataFrame) -> pl.DataFrame:
        raw = frame.select(*group, 'strike_raw').unique()
        result = (_normalize(raw, 'strike_raw').select(*group, 'strike_decimal').unique()
                  .with_columns(pl.col('strike_decimal').cast(pl.Float32).alias('_f32')))
        if result.filter(~pl.col('_f32').is_finite()).height:
            raise ValueError('cross-source strike cannot be represented at vendor precision')
        return result.with_columns(pl.col('_f32').cast(pl.String))

    left, right = keys(official), keys(vendor)
    collisions = (left.group_by(*group, '_f32').agg(pl.col('strike_decimal').n_unique().alias('n'))
                  .filter(pl.col('n') > 1))
    paired = right.join(left, on=[*group, '_f32'], how='inner')
    mismatch = paired.filter(pl.col('strike_decimal') != pl.col('strike_decimal_right'))
    if collisions.height or mismatch.height:
        raise ValueError('cross-source strike identity is ambiguous at vendor Float32 precision')
    return {'official_keys': left.height, 'vendor_keys': right.height,
            'matching_float32_key_pairs_with_exact_decimal': paired.height,
            'official_float32_collision_groups': 0, 'decimal_mismatch_pairs': 0}


def change_counts(baseline: pl.DataFrame, overlay: pl.DataFrame) -> dict:
    joined = baseline.join(overlay, on=KEYS, how='full', coalesce=True, suffix='_new')
    new = joined.filter(pl.col('trade_ts').is_null()).height
    lost = joined.filter(pl.col('trade_ts_new').is_null()).height
    price_changed = ((pl.col('reference_price_decimal') != pl.col('reference_price_decimal_new'))
                     .fill_null(pl.col('reference_price_decimal').is_null()
                                != pl.col('reference_price_decimal_new').is_null()))
    changed = joined.filter(pl.col('trade_ts').is_not_null() & pl.col('trade_ts_new').is_not_null()
                            & ((pl.col('trade_ts') != pl.col('trade_ts_new'))
                               | (pl.col('reference_kind') != pl.col('reference_kind_new')) | price_changed)).height
    return {'baseline_candidate_keys': baseline.height, 'overlay_candidate_keys': overlay.height,
            'new_candidate_keys': new, 'lost_candidate_keys': lost, 'changed_existing_reference_keys': changed}


def build(root: Path, plan_path: Path, official_manifest: Path, prior: date, missing_day: date,
          day: date, output: Path) -> dict:
    if output.is_symlink():
        raise ValueError('a new regular research output is required')
    root, plan_path, official_manifest, output = (p.resolve() for p in (root, plan_path, official_manifest, output))
    workspace = root.parents[1]
    if (not prior < missing_day < day or output.exists()
            or not output.is_relative_to(workspace / 'data_tw_index_derivatives_ticks' / 'research')):
        raise ValueError('ordered dates and a new derivative research directory are required')
    plan_sha, official_sha = sha256_file(plan_path), sha256_file(official_manifest)
    plan = json.loads(plan_path.read_bytes())
    official_doc = json.loads(official_manifest.read_bytes())
    code_paths = [Path(__file__), Path('stockagent/data/tw_option_product_ticks.py'),
                  Path('stockagent/data/tw_option_finmind_reference.py'),
                  Path('stockagent/data/tw_option_reference_candidates.py'),
                  Path('stockagent/data/tw_option_reference_event_index.py'),
                  Path('stockagent/data/tw_futures_official_ticks.py'),
                  Path('downloader/finmind_option_contract_keys.py'), Path('downloader/artifact_io.py'),
                  Path('scripts/build_taifex_option_finmind_reference_window.py'),
                  Path('scripts/run_finmind_option_reference_history.py'),
                  Path('scripts/build_taifex_option_calendar_event_index.py')]
    code_pins = {str(p.resolve()): sha256_file(p) for p in code_paths}
    if (plan.get('contract') != 'finmind_option_reference_history_plan_v1'
            or plan.get('training_ready') is not False
            or official_doc.get('contract') != OFFICIAL_CONTRACT
            or official_doc.get('training_ready') is not False
            or official_doc.get('event_calendar_date') != missing_day.isoformat()
            or plan['policy_sha256'] != official_doc['policy_sha256']):
        raise ValueError('source-only plan and same-policy official gap index required')
    gaps = [row['date'] for row in plan['not_planned_days'] if prior.isoformat() < row['date'] < day.isoformat()]
    if gaps != [missing_day.isoformat()]:
        raise ValueError('the finite interval must have exactly the selected known source gap')
    planned = {}
    for window in plan['windows']:
        for record, source_day in zip(window['source_receipt_pins'], window_dates(window), strict=True):
            if source_day in (prior, day):
                if source_day in planned:
                    raise ValueError('source plan repeats a selected day')
                planned[source_day] = record
    if set(planned) != {prior, day}:
        raise ValueError('both FinMind source dates must be present in the pinned plan')
    policy = Path(plan['policy_path'])
    if sha256_file(policy) != plan['policy_sha256']:
        raise ValueError('candidate policy changed from plan')
    sessions, linked_pins = candidate_policy(policy)
    official_data = Path(official_doc['data_path'])
    if (official_data.is_symlink() or official_data.resolve().parent != official_manifest.parent
            or sha256_file(official_data) != official_doc['data_sha256']):
        raise ValueError('official event index differs from receipt')
    for path, digest in official_doc['implementation_sha256'].items():
        if sha256_file(Path(path)) != digest:
            raise ValueError('official event-index implementation changed')
    for path, digest in official_doc['policy_linked_pins'].items():
        if sha256_file(Path(path)) != digest:
            raise ValueError('official event-index policy evidence changed')
    sources, vendor, counts = [], [], []
    capacity_start = capacity_check(output.parent)
    for source_day in (prior, day):
        source = pin_source(root, source_day)
        if (source['sha256'] != planned[source_day]['parquet_sha256']
                or source['receipt_sha256'] != planned[source_day]['receipt_sha256']):
            raise ValueError('FinMind source differs from the frozen plan')
        rows, stats = candidate_rows(pl.read_parquet(source['path']), day=source_day,
                                     source_sha256=source['sha256'], candidate_products=set(sessions))
        if stats['excluded_rows'] or rows.is_empty():
            raise ValueError('a selected FinMind day has invalid or missing source rows')
        sources.append(source)
        vendor.append(rows)
        counts.append({'day': source_day.isoformat(), **stats})
    gap_receipt = root / 'receipts' / 'TaiwanOptionTick' / 'all' / f'{missing_day.isoformat()}.json'
    if gap_receipt.is_symlink():
        raise ValueError('source gap receipt must be a regular file')
    gap_receipt_sha = sha256_file(gap_receipt)
    gap = json.loads(gap_receipt.read_bytes())
    if (gap.get('dataset') != 'TaiwanOptionTick' or gap.get('partition') != missing_day.isoformat()
            or gap.get('status') != 'observed_empty' or gap.get('rows') != 0):
        raise ValueError('FinMind gap receipt changed; a fresh source plan is required')
    official_index = (pl.read_parquet(official_data).filter(pl.col('source_date') == missing_day)
                      .select(*SOURCE_COLUMNS))
    if official_index.is_empty():
        raise ValueError('official index contains no selected calendar-day observations')
    identity = check_strike_identity(official_index, pl.concat(vendor))
    official_native_parts = []
    for source in official_doc['source_pins']:
        for frame in iter_official_option_trade_batches(source):
            chosen = frame.filter(pl.col('event_date') == missing_day).select(*SOURCE_COLUMNS)
            if chosen.height:
                official_native_parts.append(chosen)
    if not official_native_parts:
        raise ValueError('official native files contain no selected calendar-day observations')
    official_native = pl.concat(official_native_parts)
    prior_state = compact_last_observations(vendor[0])
    baseline = pl.concat([prior_state, vendor[1]])
    reduced = pl.concat([prior_state, official_index, vendor[1]])
    full = pl.concat([prior_state, official_native, vendor[1]])
    results, clocks = [], {}
    for clock in ('08:45', '09:00'):
        before = reference_candidates(baseline, trading_date=day, decision_time=clock, session_windows=sessions)
        after = reference_candidates(reduced, trading_date=day, decision_time=clock, session_windows=sessions)
        expected = reference_candidates(full, trading_date=day, decision_time=clock, session_windows=sessions)
        if not after.equals(expected):
            raise ValueError(f'official event overlay differs from full native rows at {clock}')
        clocks[clock] = {**change_counts(before, after),
                         'selected_gap_day_observations': after.filter(pl.col('trade_ts').dt.date() == missing_day).height,
                         'full_native_comparison': 'identical'}
        results.append(after.with_columns(pl.lit(day).cast(pl.Date).alias('decision_date'),
                                          pl.lit(clock).alias('decision_time')))
    def check_unchanged() -> None:
        if (sha256_file(plan_path) != plan_sha or sha256_file(official_manifest) != official_sha
                or sha256_file(official_data) != official_doc['data_sha256']
                or sha256_file(policy) != plan['policy_sha256'] or sha256_file(gap_receipt) != gap_receipt_sha
                or any(sha256_file(Path(p)) != h for p, h in linked_pins.items())
                or any(sha256_file(Path(p)) != h for p, h in code_pins.items())
                or any(sha256_file(Path(s['path'])) != s['sha256']
                       or sha256_file(Path(s['receipt_path'])) != s['receipt_sha256'] for s in sources)
                or any(sha256_file(Path(s['path'])) != s['sha256']
                       or sha256_file(Path(s['receipt_path'])) != s['receipt_sha256']
                       for s in official_doc['source_pins'])):
            raise ValueError('source, policy or receipt changed during overlay')

    check_unchanged()
    capacity_publish = capacity_check(output.parent)
    output.mkdir(parents=True, exist_ok=False)
    payload = output / 'reference_candidates.parquet'
    result = pl.concat(results).sort('decision_time', *KEYS)
    with tempfile.NamedTemporaryFile(dir=output, prefix='.reference-', suffix='.tmp', delete=False) as stream:
        tmp = Path(stream.name)
    try:
        result.write_parquet(tmp, compression='zstd', statistics=True)
        if not result.equals(pl.read_parquet(tmp)):
            raise ValueError('mixed reference readback differs')
        durable_replace(tmp, payload)
    finally:
        tmp.unlink(missing_ok=True)
    check_unchanged()
    fingerprint_input = {'contract': CONTRACT, 'prior_source_day': prior.isoformat(),
                         'official_fallback_day': missing_day.isoformat(), 'decision_date': day.isoformat(),
                         'plan_sha256': plan_sha, 'policy_sha256': plan['policy_sha256'],
                         'official_manifest_sha256': official_sha, 'gap_receipt_sha256': gap_receipt_sha,
                         'vendor_sources': [{'day': s['day'], 'sha256': s['sha256'],
                                             'receipt_sha256': s['receipt_sha256']} for s in sources],
                         'code_pins': code_pins}
    fingerprint = hashlib.sha256(json.dumps(fingerprint_input, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    receipt = {'contract': CONTRACT, 'generated_at_utc': datetime.now(timezone.utc).isoformat(),
               'prior_source_day': prior.isoformat(), 'official_fallback_day': missing_day.isoformat(),
               'decision_date': day.isoformat(), 'data_fingerprint': fingerprint,
               'plan_path': str(plan_path), 'plan_sha256': plan_sha, 'official_manifest_path': str(official_manifest),
               'official_manifest_sha256': official_sha, 'vendor_source_pins': sources,
               'gap_receipt_path': str(gap_receipt), 'gap_receipt_sha256': gap_receipt_sha,
               'policy_path': str(policy), 'policy_sha256': plan['policy_sha256'],
               'policy_linked_pins': linked_pins, 'implementation_sha256': code_pins,
               'source_counts': counts, 'official_native_calendar_day_rows': official_native.height,
               'cross_source_strike_identity': identity, 'clock_readback': clocks,
               'data_path': str(payload), 'data_sha256': sha256_file(payload), 'rows': result.height,
               'capacity_at_start': capacity_start, 'capacity_before_publication': capacity_publish,
               'training_ready': False,
               'limitations': ['Only named source days are included; earlier history and other provider gaps remain unknown.',
                               'Official fallback is retrospective source evidence, not proven delivery at the decision.',
                               'Observed keys need expiry, generation, listing, quote, fill and option account checks.',
                               'This does not mutate the original FinMind plan, receipt or training dataset.']}
    atomic_write_json(output / 'manifest.json', receipt)
    return receipt


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path('data_finmind/complement'))
    parser.add_argument('--plan', type=Path, required=True)
    parser.add_argument('--official-manifest', type=Path, required=True)
    parser.add_argument('--prior', type=date.fromisoformat, required=True)
    parser.add_argument('--missing-day', type=date.fromisoformat, required=True)
    parser.add_argument('--date', type=date.fromisoformat, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    try:
        result = build(args.root, args.plan, args.official_manifest, args.prior, args.missing_day,
                       args.date, args.output)
    except CapacityNotAdmitted as error:
        print(json.dumps({'state': 'capacity_deferred', 'capacity': error.status}), flush=True)
        raise SystemExit(75) from error
    print(json.dumps({'manifest': str(args.output / 'manifest.json'), 'clock_readback': result['clock_readback'],
                      'training_ready': False}, ensure_ascii=False), flush=True)


if __name__ == '__main__':
    main()
