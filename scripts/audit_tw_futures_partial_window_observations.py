#!/usr/bin/env python3
"""Recover source-pinned partial observations for diagnostics, never admission.

The input is an existing all-TWD bulk checkpoint and its exact quarantine. This
does not change a clock, infer an empty window, establish PIT, or create fills.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timezone
import json
from pathlib import Path
import sys

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file
from stockagent.data.tw_futures_decision_clock import (
    bind_entry_specifications, decision_clock_contract, observed_window_entries,
    validate_execution_quarantine,
)
from stockagent.data.tw_futures_finmind_ticks import normalize_bulk_stock_ticks
from stockagent.data.tw_futures_margin_release import read_bound_output


def _checked_file(path: Path, digest: str) -> None:
    if not path.is_file() or sha256_file(path) != digest:
        raise ValueError(f'input SHA mismatch: {path}')


def build_diagnostic(*, clocks: Path, checkpoint: Path, daily: Path, rules: Path,
                     output: Path, report: Path, workers: int = 4,
                     selected_days: tuple[str, ...] = ()) -> dict:
    """Write positive, source-bound candidate windows and a separate audit."""
    if workers < 1 or len(set(selected_days)) != len(selected_days):
        raise ValueError('workers and selected dates must be valid')
    keys = ['date', 'physical_contract']
    manifest_path = checkpoint / 'manifest.json'
    manifest_hash = sha256_file(manifest_path)
    manifest = json.loads(manifest_path.read_text())
    identity = manifest['identity']
    if (manifest.get('contract') != 'verified_bulk_window_checkpoint_v1'
            or identity.get('asset_scope') != 'all_twd'
            or identity.get('streaming') is not True
            or identity.get('entry_windows') != [1, 300]):
        raise ValueError('expected an all-TWD streamed 1/300-minute checkpoint')
    clock_path = clocks / '0845' / 'manifest.json'
    clock_hash = sha256_file(clock_path)
    clock = json.loads(clock_path.read_text())
    _checked_file(daily, clock['source_daily_sha256'])
    _checked_file(rules, clock['source_rules_sha256'])
    quarantine_path = clock_path.parent / clock['execution_quarantine']['file']
    _checked_file(quarantine_path, clock['execution_quarantine']['sha256'])
    quarantine = json.loads(quarantine_path.read_text())
    gaps_path = clocks / 'coverage_gaps.parquet'
    gaps_hash = sha256_file(gaps_path)
    gaps = pl.read_parquet(gaps_path)
    validate_execution_quarantine(quarantine, gaps,
        daily_sha256=clock['source_daily_sha256'], rules_sha256=clock['source_rules_sha256'])
    if gaps.height != clock['execution_quarantine']['contract_days']:
        raise ValueError('quarantine count differs')
    coverage_path = checkpoint / 'coverage.parquet'
    _checked_file(coverage_path, manifest['outputs']['coverage']['sha256'])
    coverage = pl.read_parquet(coverage_path).join(gaps.select(keys), on=keys, how='semi')
    if coverage.height != gaps.height or coverage.filter(pl.col('verified')).height:
        raise ValueError('checkpoint and current quarantine disagree')
    spec_dir = clock_path.parent / clock['entry_specifications']['directory']
    spec_path = spec_dir / 'specifications.parquet'
    spec_manifest = spec_dir / 'manifest.json'
    _checked_file(spec_path, clock['entry_specifications']['data_sha256'])
    _checked_file(spec_manifest, clock['entry_specifications']['manifest_sha256'])
    specs, spec_proof = read_bound_output(spec_path)
    if spec_proof.get('dataset') != 'taifex_dated_product_specifications':
        raise ValueError('dated specification identity differs')
    fields = ['date', 'physical_contract', 'product', 'contract', 'asset_class',
              'official_expiry', 'high', 'low', 'close', 'observed_open']
    expected = (pl.scan_parquet(daily).select(fields).collect()
        .join(coverage.select(*keys, 'outright_volume', 'block_volume'), on=keys,
              how='inner', validate='1:1'))
    if expected.height != gaps.height:
        raise ValueError('financial source does not contain every quarantined identity')
    expected = bind_entry_specifications(expected, specs)
    if selected_days:
        requested = {date.fromisoformat(value) for value in selected_days}
        expected = expected.filter(pl.col('date').is_in(list(requested)))
        if set(expected['date'].to_list()) != requested:
            raise ValueError('requested diagnostic dates are not all quarantined')
    by_day = {str(f['date'][0]): f for f in expected.partition_by('date')}
    prior = {str(f['date'][0]): f for f in coverage.partition_by('date')}
    objects = [item for item in identity['objects'] if item['date'] in by_day]
    if len(objects) != len(by_day) or len({item['date'] for item in objects}) != len(by_day):
        raise ValueError('checkpoint does not pin one original per selected day')
    if any(item.get('source_kind') for item in objects):
        raise ValueError('diagnostic currently accepts only native whole-market objects')
    inventories = {(item['inventory_path'], item['inventory_sha256']) for item in objects}
    for path, digest in inventories:
        _checked_file(Path(path), digest)

    def inspect(item):
        raw_path, receipt_path = Path(item['path']), Path(item['receipt_path'])
        before = raw_path.stat()
        _checked_file(raw_path, item['sha256'])
        _checked_file(receipt_path, item['receipt_sha256'])
        raw = pl.read_parquet(raw_path)
        if raw.height != item['rows'] or before.st_size != item['bytes']:
            raise ValueError('original tick row count or byte length differs')
        day = date.fromisoformat(item['date'])
        _, observed_coverage, events, partial = normalize_bulk_stock_ticks(raw,
            day=day, expected=by_day[item['date']], source_sha256=item['sha256'],
            asset_scope='all_twd', return_unverified_observations=True)
        previous = prior[item['date']].sort(keys)
        current = observed_coverage.sort(keys)
        compare = ['date', 'physical_contract', 'product', 'contract', 'high', 'low', 'close',
                   'outright_volume', 'block_volume', 'tick_high', 'tick_low', 'tick_close',
                   'tick_volume', 'tick_rows', 'verified', 'source_file_sha256']
        if current.select(compare).to_dicts() != previous.select(compare).to_dicts():
            raise ValueError(f'partial source recomputation differs from pinned checkpoint: {day}')
        if events.height:
            raise ValueError('quarantined contracts must not emit reference events')
        pieces = []
        for window in (1, 300):
            for clock_time in ('08:45', '09:00'):
                policy = decision_clock_contract(clock_time, asset_scope='all_twd',
                    window_minutes=window, quarantine_unknown=True)
                reduced = observed_window_entries(partial, policy)
                if reduced.height:
                    piece = (reduced.join(current.select(*keys, 'product', 'contract',
                                'outright_volume', 'tick_volume'), on=keys, how='inner', validate='1:1')
                        .with_columns(pl.lit(window).alias('window_minutes'),
                                      pl.lit(clock_time).alias('decision_time')))
                    if piece.filter((pl.col('entry_volume') <= 0)
                            | (pl.col('entry_volume') > pl.col('tick_volume'))
                            | ~pl.col('entry_quote').is_finite()).height:
                        raise ValueError('observed candidate is not bounded by its real prints')
                    pieces.append(piece)
        after = raw_path.stat()
        if ((before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns)
                != (after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns)):
            raise ValueError('original tick data changed during diagnostic')
        _checked_file(receipt_path, item['receipt_sha256'])
        return pieces, partial.select(keys).unique().height

    with ThreadPoolExecutor(max_workers=workers) as pool:
        results = list(pool.map(inspect, objects))
    pieces = [p for day_pieces, _ in results for p in day_pieces]
    if pieces:
        candidates = pl.concat(pieces, how='vertical_relaxed').sort(
            'date', 'physical_contract', 'window_minutes', 'decision_time')
    else:
        candidates = pl.DataFrame(schema={'date': pl.Date, 'physical_contract': pl.String,
            'entry_quote': pl.Float64, 'entry_volume': pl.Float64, 'source_file_sha256': pl.String,
            'product': pl.String, 'contract': pl.String, 'outright_volume': pl.Int64,
            'tick_volume': pl.Int64, 'window_minutes': pl.Int32, 'decision_time': pl.String})
    if candidates.select(*keys, 'window_minutes', 'decision_time').is_duplicated().any():
        raise ValueError('duplicate diagnostic window identity')
    for path, digest in ((daily, clock['source_daily_sha256']), (rules, clock['source_rules_sha256']),
                         (quarantine_path, clock['execution_quarantine']['sha256']),
                         (coverage_path, manifest['outputs']['coverage']['sha256']),
                         (spec_path, clock['entry_specifications']['data_sha256']),
                         (spec_manifest, clock['entry_specifications']['manifest_sha256']),
                         (gaps_path, gaps_hash), (manifest_path, manifest_hash), (clock_path, clock_hash)):
        _checked_file(path, digest)
    output.mkdir(parents=True, exist_ok=False)
    data_path = output / 'observed_window_candidates.parquet'
    atomic_write_parquet(data_path, candidates)
    counts = (candidates.group_by('window_minutes', 'decision_time', 'product').len()
              .sort('window_minutes', 'decision_time', 'product').to_dicts())
    doc = dict(contract='tw_futures_partial_window_observations_diagnostic_v1',
        completed_at_utc=datetime.now(timezone.utc).isoformat(),
        research_only=True, training_ready=False, source_coverage_complete=False,
        point_in_time_verified=False, fill_verified=False, opening_reference_verified=False,
        selected_contract_days=expected.height, selected_dates=len(objects),
        partial_positive_contract_days=sum(count for _, count in results),
        candidate_rows=candidates.height, counts_by_window_clock_product=counts,
        candidate_file=dict(path=str(data_path), sha256=sha256_file(data_path)),
        sources=dict(clock_manifest_sha256=clock_hash, checkpoint_manifest_sha256=manifest_hash,
                     gap_file_sha256=gaps_hash, daily_sha256=clock['source_daily_sha256'],
                     rules_sha256=clock['source_rules_sha256'],
                     quarantine_sha256=clock['execution_quarantine']['sha256'],
                     specifications_sha256=clock['entry_specifications']['data_sha256'],
                     source_objects=len(objects), source_bytes=sum(item['bytes'] for item in objects)),
        policy=('observed positive prints only with official price and volume bounds; '
                'no missing minute interpreted as zero and no full-window completeness claimed'))
    atomic_write_json(output / 'manifest.json', doc)
    atomic_write_json(report, doc)
    return doc


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('clocks', 'checkpoint', 'daily', 'rules', 'output', 'report'):
        parser.add_argument('--' + name, required=True, type=Path)
    parser.add_argument('--workers', type=int, default=4)
    parser.add_argument('--day', action='append', default=[])
    args = parser.parse_args()
    result = build_diagnostic(clocks=args.clocks, checkpoint=args.checkpoint, daily=args.daily,
        rules=args.rules, output=args.output, report=args.report, workers=args.workers,
        selected_days=tuple(args.day))
    print(json.dumps(result, ensure_ascii=False), flush=True)


if __name__ == '__main__':
    main()
