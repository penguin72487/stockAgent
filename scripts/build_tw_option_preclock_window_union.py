#!/usr/bin/env python3
"""Combine verified pre-decision option references over a longer source window.

This retains source-only candidate prices and does not admit any contract to
training. A separate post-session daily comparison is diagnostic only and is
never attached to the published candidates as decision-time information.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import json
from pathlib import Path
import tempfile

import polars as pl
import pyarrow.parquet as pq

from downloader.artifact_io import atomic_write_json, durable_replace, sha256_file
from scripts.audit_taifex_option_reference_daily_source import compare, daily_index
from scripts.audit_tw_option_cross_source_references import audit_rows
from scripts.build_taifex_option_finmind_reference_window import capacity_check
from scripts.build_tw_option_cross_source_references import (
    LAST_KINDS, OPEN_KINDS, merge_reference_oracles, reference_map,
)
from stockagent.data.tw_option_product_daily import iter_option_product_daily_batches
from stockagent.data.tw_option_reference_candidates import KEYS


GROUP = ['decision_date', 'decision_time', *KEYS]


def merge_candidates(left: pl.DataFrame, right: pl.DataFrame) -> pl.DataFrame:
    """Merge two candidate reductions, keeping first open or latest trade.

    Equal-second alternatives combine source hashes and remain ambiguous if
    either input is ambiguous or the reported prices differ.
    """
    for frame in (left, right):
        if frame.select(GROUP).is_duplicated().any():
            raise ValueError('a source has duplicate decision contract keys')
        if frame.filter(~pl.col('reference_kind').is_in(sorted(OPEN_KINDS | LAST_KINDS))).height:
            raise ValueError('unsupported reference kind')
    candidates = pl.concat([left, right]).with_columns(
        pl.col('reference_kind').is_in(sorted(OPEN_KINDS)).alias('_is_open'))
    candidates = candidates.with_columns(pl.col('_is_open').any().over(GROUP).alias('_has_open'))
    candidates = candidates.filter(pl.col('_is_open') == pl.col('_has_open'))
    candidates = candidates.with_columns(
        pl.when(pl.col('_has_open')).then(pl.col('trade_ts').min().over(GROUP))
        .otherwise(pl.col('trade_ts').max().over(GROUP)).alias('_selected_ts'))
    candidates = candidates.filter(pl.col('trade_ts') == pl.col('_selected_ts'))
    grouped = candidates.group_by(GROUP).agg(
        pl.col('_has_open').first(), pl.col('trade_ts').first(),
        pl.col('reference_price_decimal').null_count().alias('_null_prices'),
        pl.col('reference_price_decimal').drop_nulls().n_unique().alias('_distinct_prices'),
        pl.col('reference_price_decimal').drop_nulls().first().alias('_price'),
        pl.col('source_hashes').list.explode(keep_nulls=False, empty_as_null=False)
          .unique().sort().alias('source_hashes'))
    ambiguous = (pl.col('_null_prices') > 0) | (pl.col('_distinct_prices') != 1)
    return grouped.select(*KEYS,
        pl.when(pl.col('_has_open') & ambiguous).then(pl.lit('ambiguous_first_second'))
          .when(pl.col('_has_open')).then(pl.lit('observed_session_open_candidate'))
          .when(ambiguous).then(pl.lit('ambiguous_last_second'))
          .otherwise(pl.lit('last_observed_trade_candidate')).alias('reference_kind'),
        pl.when(ambiguous).then(None).otherwise(pl.col('_price')).alias('reference_price_decimal'),
        pl.col('trade_ts'), (pl.col('trade_ts') + pl.duration(seconds=1)).alias('available_by'),
        pl.col('source_hashes'), 'decision_date', 'decision_time').sort(*GROUP)


def _regular(path: Path, digest: str) -> None:
    if path.is_symlink() or not path.is_file() or sha256_file(path) != digest:
        raise ValueError(f'input missing or digest changed: {path}')


def build(window_manifest: Path, oracle: Path, preclock_manifest: Path,
          daily_receipt: Path, day: date, output: Path, report: Path) -> dict:
    workspace = Path.cwd().resolve()
    window_manifest, oracle, preclock_manifest, daily_receipt, output, report = (
        p.resolve() for p in (window_manifest, oracle, preclock_manifest, daily_receipt, output, report))
    if (not output.is_relative_to(workspace / 'data_finmind/research')
            or not report.is_relative_to(workspace / 'artifacts/markets')
            or output.exists() or report.exists()):
        raise ValueError('new research data and market report locations are required')
    input_paths = [window_manifest, oracle, preclock_manifest, daily_receipt]
    pins = {str(path): sha256_file(path) for path in input_paths}
    official, proof, recent, daily = (json.loads(path.read_bytes()) for path in input_paths)
    if (official.get('contract') != 'taifex_option_observed_reference_source_window_v1'
            or official.get('training_ready') is not False
            or day.isoformat() not in official['archive_days']
            or proof.get('contract') != 'taifex_option_reference_source_window_native_csv_oracle_v1'
            or proof.get('training_ready') is not False or proof.get('day') != day.isoformat()
            or proof.get('window_manifest_sha256') != pins[str(window_manifest)]
            or proof.get('data_sha256') != official['data_sha256']
            or proof.get('source_archives_read') != len(official['source_pins'])):
        raise ValueError('official source-window oracle identity differs')
    for clock in ('08:45', '09:00'):
        result = proof['decisions'][clock]
        if any(result[k] != 0 for k in ('missing', 'extra', 'kind_price_second_or_provenance_mismatch')):
            raise ValueError('official native-source oracle did not pass')
    if (recent.get('contract') != 'tw_option_preclock_cross_source_reference_v1'
            or recent.get('training_ready') is not False or recent.get('decision_date') != day.isoformat()
            or any(recent['clock_comparison'][clock]['native_oracle_mismatches'] != 0
                   for clock in ('08:45', '09:00'))):
        raise ValueError('recent cross-source independent oracle did not pass')
    if (daily.get('contract') != 'taifex_option_all_products_daily_raw_v1'
            or daily.get('point_in_time_snapshot') is not False
            or not daily['range_start'] <= day.isoformat() <= daily['range_end']):
        raise ValueError('dated diagnostic requires the retained official daily receipt')
    for doc in (official, recent):
        pins[doc['data_path']] = doc['data_sha256']
    for source in official['source_pins'] + recent['official_source_pins'] + recent['finmind_source_pins']:
        pins[source['path']] = source['sha256']
        pins[source['receipt_path']] = source['receipt_sha256']
    pins[daily['path']] = daily['sha256']
    for path, digest in recent['input_pins'].items():
        if path in pins and pins[path] != digest:
            raise ValueError('source identities conflict')
        pins[path] = digest
    for path in (Path(__file__), Path('scripts/audit_taifex_option_reference_daily_source.py'),
                 Path('scripts/audit_tw_option_cross_source_references.py'),
                 Path('scripts/build_tw_option_cross_source_references.py'),
                 Path('stockagent/data/tw_option_product_daily.py')):
        pins[str(path.resolve())] = sha256_file(path)
    for path, digest in pins.items():
        _regular(Path(path), digest)
    capacity = capacity_check(output.parent)
    official_rows = pl.read_parquet(official['data_path']).filter(pl.col('decision_date') == day)
    recent_rows = pl.read_parquet(recent['data_path'])
    official_hashes = {s['sha256'] for s in official['source_pins']}
    finmind_hashes = {s['sha256'] for s in recent['finmind_source_pins']}
    if not {s['sha256'] for s in recent['official_source_pins']} <= official_hashes:
        raise ValueError('recent official sources are outside the long window')
    audit_rows(official_rows.to_dicts(), day, official_hashes, finmind_hashes)
    audit_rows(recent_rows.to_dicts(), day, official_hashes, finmind_hashes)
    combined = merge_candidates(official_rows, recent_rows)
    selected = {}
    for clock in ('08:45', '09:00'):
        a = official_rows.filter(pl.col('decision_time') == clock)
        b = recent_rows.filter(pl.col('decision_time') == clock)
        out = combined.filter(pl.col('decision_time') == clock)
        if a.height != proof['decisions'][clock]['keys_compared']:
            raise ValueError('official clock count differs from native oracle')
        if reference_map(out) != merge_reference_oracles(reference_map(a), reference_map(b)):
            raise ValueError('vectorized union disagrees with independent scalar reduction')
        selected[clock] = {'official_window_keys': a.height, 'recent_two_day_keys': b.height,
                           'union_keys': out.height,
                           'added_over_two_day_keys': out.join(b.select(GROUP), on=GROUP, how='anti').height,
                           'ambiguous_keys': out['reference_price_decimal'].null_count(),
                           'scalar_oracle_mismatches': 0}
    independent = audit_rows(combined.to_dicts(), day, official_hashes, finmind_hashes)
    extra = combined.join(recent_rows.select(GROUP), on=GROUP, how='anti')
    batches = list(iter_option_product_daily_batches(Path(daily['path']), expected_sha256=daily['sha256'],
                                                      trading_dates={day}))
    index, unsupported = daily_index(batches)
    post_session = {'all_union_keys': compare(combined, index),
                    'two_day_keys': compare(recent_rows, index),
                    'added_window_keys': compare(extra, index),
                    'unsupported_daily_keys': unsupported}
    if any(sha256_file(Path(path)) != digest for path, digest in pins.items()):
        raise ValueError('source or implementation changed during processing')
    capacity_check(output.parent)
    output.mkdir(parents=True, exist_ok=False)
    payload = output / 'observed_references.parquet'
    with tempfile.NamedTemporaryFile(dir=output, prefix='.references-', suffix='.tmp', delete=False) as handle:
        temp = Path(handle.name)
    try:
        combined.write_parquet(temp, compression='zstd', statistics=True)
        if not combined.equals(pl.read_parquet(temp)):
            raise ValueError('candidate payload readback differs')
        durable_replace(temp, payload)
    finally:
        temp.unlink(missing_ok=True)
    data_sha = sha256_file(payload)
    arrow = pq.read_table(payload)
    arrow_counts = audit_rows(arrow.to_pylist(), day, official_hashes, finmind_hashes)
    if arrow_counts != independent or any(sha256_file(Path(path)) != digest for path, digest in pins.items()):
        raise ValueError('published readback or pinned identity differs')
    result = {'contract': 'tw_option_preclock_window_union_v1',
              'observed_at_utc': datetime.now(timezone.utc).isoformat(),
              'decision_date': day.isoformat(), 'official_window': official['requested_window'],
              'official_archive_days': len(official['source_pins']),
              'data_path': str(payload), 'data_sha256': data_sha, 'rows': combined.height,
              'clock_comparison': selected, 'arrow_independent_readback': arrow_counts,
              'post_session_daily_diagnostic': post_session, 'input_pins': pins,
              'capacity_at_start': capacity, 'training_ready': False,
              'limitations': ['A finite retained source window cannot establish all past trades.',
                  'Current candidate session rules are retrospective, not historical delivery or PIT proof.',
                  'Post-session daily comparison is a diagnostic and is not a decision-time feature or admission.',
                  'Prior observed reference prices can belong to expired or otherwise ineligible contracts.',
                  'No listing, executable quote, fill, option account or training admission is implied.']}
    atomic_write_json(output / 'manifest.json', result)
    atomic_write_json(report, result)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('window-manifest', 'oracle', 'preclock-manifest', 'daily-receipt', 'output', 'report'):
        parser.add_argument('--' + name, type=Path, required=True)
    parser.add_argument('--date', type=date.fromisoformat, required=True)
    args = parser.parse_args()
    result = build(args.window_manifest, args.oracle, args.preclock_manifest, args.daily_receipt,
                   args.date, args.output, args.report)
    print(json.dumps({'output': str(args.output / 'manifest.json'), 'report': str(args.report),
                      'clock_comparison': result['clock_comparison'],
                      'post_session_daily_diagnostic': result['post_session_daily_diagnostic'],
                      'training_ready': False}, ensure_ascii=False), flush=True)


if __name__ == '__main__':
    main()
