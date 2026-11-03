#!/usr/bin/env python3
"""Build two pre-decision reference views from pinned, completed source files.

This deliberately does not claim the target's entire calendar day is present.
It combines only observations before the two decisions and is not an options
execution or accounting contract.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import json
from pathlib import Path
import tempfile
from zoneinfo import ZoneInfo

import polars as pl
import pyarrow.parquet as pq

from downloader.artifact_io import atomic_write_json, durable_replace, sha256_file
from scripts.audit_taifex_option_reference_oracle import native_reference
from scripts.audit_tw_option_cross_source_references import audit_rows
from scripts.build_taifex_option_calendar_event_index import append_chunk
from scripts.build_taifex_option_finmind_reference_window import candidate_policy, capacity_check, pin_source
from scripts.build_tw_option_cross_source_references import (
    FINMIND_INDEX_CONTRACT, finmind_native_reference, merge_reference_oracles,
    read_index, reference_map, snapshot_receipt,
)
from stockagent.data.tw_futures_official_ticks import pin_official_option_tick_inventory
from stockagent.data.tw_option_product_ticks import iter_official_option_trade_batches
from stockagent.data.tw_option_reference_candidates import reference_candidates
from stockagent.data.tw_option_reference_event_index import SOURCE_COLUMNS


def build(official_head: Path, finmind_manifest: Path, day: date, previous: date,
          output: Path, report: Path) -> dict:
    workspace = Path.cwd().resolve()
    official_head, finmind_manifest, output, report = (
        path.resolve() for path in (official_head, finmind_manifest, output, report))
    if (not output.is_relative_to(workspace / 'data_finmind/research')
            or not report.is_relative_to(workspace / 'artifacts/markets')
            or output.exists() or report.exists()):
        raise ValueError('new research data and market report locations are required')
    head_sha = sha256_file(official_head)
    head = json.loads(official_head.read_bytes())
    days = head['listing_page_dates']['options']
    if days != sorted(set(days)) or day.isoformat() not in days:
        raise ValueError('target must be an official listed source date')
    position = days.index(day.isoformat())
    if position == 0 or days[position - 1] != previous.isoformat():
        raise ValueError('an immediately preceding official source date is required')
    frozen = Path(snapshot_receipt({'receipt_path': str(official_head), 'receipt_sha256': head_sha},
                                   workspace / 'data_tw_index_derivatives_ticks/research/option_reference_source_receipts')['receipt_path'])
    official_sources = pin_official_option_tick_inventory(frozen, [previous, day])
    finmind, finmind_rows, finmind_sha = read_index(finmind_manifest, FINMIND_INDEX_CONTRACT, day)
    if [s['day'] for s in finmind['source_pins']] != [previous.isoformat(), day.isoformat()]:
        raise ValueError('FinMind index must pin the same two physical dates')
    policy = Path(finmind['policy_path'])
    if sha256_file(policy) != finmind['policy_sha256']:
        raise ValueError('source policy changed')
    windows, linked_pins = candidate_policy(policy)
    finmind_sources = []
    for source in finmind['source_pins']:
        observed = pin_source(workspace / 'data_finmind/complement', date.fromisoformat(source['day']))
        if observed != source:
            raise ValueError('FinMind source no longer matches the index')
        finmind_sources.append(snapshot_receipt(
            observed, workspace / 'data_finmind/research/option_reference_source_receipts'))
    implementations = [Path(__file__), Path('scripts/build_taifex_option_calendar_event_index.py'),
                       Path('scripts/audit_taifex_option_reference_oracle.py'),
                       Path('scripts/build_tw_option_cross_source_references.py'),
                       Path('scripts/audit_tw_option_cross_source_references.py'),
                       Path('stockagent/data/tw_option_product_ticks.py'),
                       Path('stockagent/data/tw_option_reference_candidates.py'),
                       Path('stockagent/data/tw_option_reference_event_index.py')]
    code_pins = {str(p.resolve()): sha256_file(p) for p in implementations}
    capacity_start = capacity_check(output.parent)
    cutoff = datetime(day.year, day.month, day.day, 9, tzinfo=ZoneInfo('Asia/Taipei'))
    indices, states, source_counts = {}, {}, []
    for source in official_sources:
        raw, preclock = 0, 0
        for batch in iter_official_option_trade_batches(source):
            raw += batch.height
            chosen = batch.filter(pl.col('event_ts') < cutoff)
            preclock += chosen.height
            if set(chosen['product_raw'].unique().to_list()) - set(windows):
                raise ValueError('official source product absent from candidate policy')
            for observed_day in chosen['event_date'].unique().sort().to_list():
                part = chosen.filter(pl.col('event_date') == observed_day).select(*SOURCE_COLUMNS)
                indices[observed_day], states[observed_day] = append_chunk(
                    indices.get(observed_day), states.get(observed_day), part,
                    day=observed_day, sessions=windows)
        source_counts.append({'trading_file_date': source['date'], 'source_rows': raw,
                              'rows_before_0900_decision': preclock})
    if not indices or day not in indices:
        raise ValueError('target day has no observed official pre-decision events')
    official_rows = pl.concat([indices[d] for d in sorted(indices)])
    official_oracle, official_counts = native_reference(official_sources, day, windows)
    finmind_oracle, finmind_counts = finmind_native_reference(finmind_sources, day, windows)
    union = pl.concat([official_rows, finmind_rows])
    parts, comparisons, official_oracle_check = [], {}, {}
    for clock in ('08:45', '09:00'):
        merged = reference_candidates(union, trading_date=day, decision_time=clock, session_windows=windows)
        actual = reference_map(merged)
        fo = reference_map(reference_candidates(finmind_rows, trading_date=day,
                                               decision_time=clock, session_windows=windows))
        oo = reference_map(reference_candidates(official_rows, trading_date=day,
                                               decision_time=clock, session_windows=windows))
        expected = merge_reference_oracles(official_oracle[clock], finmind_oracle[clock])
        if actual != expected or fo != finmind_oracle[clock] or oo != official_oracle[clock]:
            raise ValueError(f'{clock} reference differs from independent native reductions')
        official_oracle_check[clock] = {'contract_keys_compared': len(oo), 'missing': 0,
                                        'extra': 0, 'reference_kind_price_second_or_provenance_mismatch': 0}
        comparisons[clock] = {'finmind_keys': len(fo), 'official_keys': len(oo),
                              'union_keys': len(actual), 'official_only_keys': len(oo.keys() - fo.keys()),
                              'finmind_only_keys': len(fo.keys() - oo.keys()),
                              'union_ambiguous_keys': sum(v[1] is None for v in actual.values()),
                              'native_oracle_mismatches': 0}
        parts.append(merged.with_columns(pl.lit(day).cast(pl.Date).alias('decision_date'),
                                         pl.lit(clock).alias('decision_time')))
    frame = pl.concat(parts).sort('decision_time', 'product_raw', 'series_raw', 'strike_decimal', 'right_raw')
    pins = {str(policy): finmind['policy_sha256'], str(finmind_manifest): finmind_sha,
            str(Path(finmind['data_path'])): finmind['data_sha256'], str(frozen): head_sha,
            **linked_pins, **code_pins}
    for source in official_sources + finmind_sources:
        pins[source['path']] = source['sha256']
        pins[source['receipt_path']] = source['receipt_sha256']
    if any(sha256_file(Path(path)) != digest for path, digest in pins.items()):
        raise ValueError('source, receipt, policy or implementation changed during processing')
    capacity_check(output.parent)
    output.mkdir(parents=True, exist_ok=False)
    payload = output / 'observed_references.parquet'
    with tempfile.NamedTemporaryFile(dir=output, prefix='.references-', suffix='.tmp', delete=False) as handle:
        temp = Path(handle.name)
    try:
        frame.write_parquet(temp, compression='zstd', statistics=True)
        if not frame.equals(pl.read_parquet(temp)):
            raise ValueError('source reference payload readback differs')
        durable_replace(temp, payload)
    finally:
        temp.unlink(missing_ok=True)
    # A separate Arrow reader checks the published date, clock, ambiguity,
    # decimal prices and source identities, in addition to the native oracle.
    data_sha = sha256_file(payload)
    arrow = pq.read_table(payload)
    independent = audit_rows(arrow.to_pylist(), day, {s['sha256'] for s in official_sources},
                             {s['sha256'] for s in finmind_sources})
    if any(independent[c]['keys'] != comparisons[c]['union_keys']
           or independent[c]['ambiguous'] != comparisons[c]['union_ambiguous_keys']
           for c in comparisons):
        raise ValueError('independent published clock counts differ')
    if sha256_file(payload) != data_sha or any(sha256_file(Path(p)) != h for p, h in pins.items()):
        raise ValueError('source or published payload changed during readback')
    result = {'contract': 'tw_option_preclock_cross_source_reference_v1',
              'observed_at_utc': datetime.now(timezone.utc).isoformat(),
              'decision_date': day.isoformat(), 'previous_official_trading_date': previous.isoformat(),
              'official_source_pins': official_sources, 'finmind_source_pins': finmind_sources,
              'policy_path': str(policy), 'policy_sha256': finmind['policy_sha256'],
              'finmind_index_path': str(finmind_manifest), 'finmind_index_sha256': finmind_sha,
              'data_path': str(payload), 'data_sha256': data_sha, 'rows': frame.height,
              'clock_comparison': comparisons, 'arrow_independent_readback': independent,
              'official_clock_oracle': official_oracle_check, 'official_raw_counts': official_counts,
              'finmind_raw_counts': finmind_counts, 'official_source_counts': source_counts,
              'input_pins': pins, 'capacity_at_start': capacity_start, 'training_ready': False,
              'limitations': ['Only the two pinned trading-day files and two FinMind physical dates are included.',
                              'Earlier missing sources can change the true prior trade; no completeness is implied.',
                              'Post-09:00 events and the whole target calendar day are outside this contract.',
                              'Observed trades are not proven delivered, executable, listed or accounted.']}
    atomic_write_json(output / 'manifest.json', result)
    atomic_write_json(report, result)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('official-head', 'finmind-manifest', 'output', 'report'):
        parser.add_argument('--' + name, type=Path, required=True)
    parser.add_argument('--date', type=date.fromisoformat, required=True)
    parser.add_argument('--previous-date', type=date.fromisoformat, required=True)
    args = parser.parse_args()
    result = build(args.official_head, args.finmind_manifest, args.date, args.previous_date,
                   args.output, args.report)
    print(json.dumps({'manifest': str(args.output / 'manifest.json'), 'report': str(args.report),
                      'clock_comparison': result['clock_comparison'], 'training_ready': False},
                     ensure_ascii=False), flush=True)


if __name__ == '__main__':
    main()
