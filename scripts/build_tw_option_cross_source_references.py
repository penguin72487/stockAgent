#!/usr/bin/env python3
"""Combine pinned option events as research-only observed references.

The official and FinMind indexes remain separate. This derived view preserves
source hashes and conflicts and validates its two clocks against a separately
reduced native-source oracle. It does not admit prices, listings, fills or an
option account for training.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import hashlib
import json
from pathlib import Path
import tempfile
from zoneinfo import ZoneInfo

import polars as pl

from downloader.artifact_io import atomic_write_bytes, atomic_write_json, durable_replace, sha256_file
from scripts.audit_taifex_option_reference_oracle import decimal_text, native_reference, update
from scripts.build_taifex_option_finmind_reference_window import (
    CapacityNotAdmitted, candidate_policy, capacity_check, pin_source,
)
from stockagent.data.tw_option_finmind_reference import candidate_rows
from stockagent.data.tw_option_reference_candidates import KEYS, reference_candidates
from stockagent.data.tw_option_reference_event_index import CONTRACT as FINMIND_INDEX_CONTRACT, SOURCE_COLUMNS


CONTRACT = 'tw_option_cross_source_observed_reference_v1'
OFFICIAL_INDEX_CONTRACT = 'taifex_option_calendar_event_index_v1'
OPEN_KINDS = {'observed_session_open_candidate', 'ambiguous_first_second'}
LAST_KINDS = {'last_observed_trade_candidate', 'ambiguous_last_second'}


def snapshot_receipt(source: dict, directory: Path) -> dict:
    """Preserve exact receipt bytes when the canonical owner later refreshes."""
    original = Path(source['receipt_path'])
    expected = source['receipt_sha256']
    if original.is_symlink() or not original.is_file():
        raise ValueError('source receipt is not a regular file')
    data = original.read_bytes()
    if not 0 < len(data) <= 8 * 1024**2 or hashlib.sha256(data).hexdigest() != expected:
        raise ValueError('source receipt bytes differ from its index')
    target = directory / f'{expected}.json'
    if target.is_symlink() or (target.exists() and (not target.is_file() or sha256_file(target) != expected)):
        raise ValueError('receipt snapshot collision')
    if not target.exists():
        atomic_write_bytes(target, data, durable=True, mode=0o600)
    if sha256_file(target) != expected or sha256_file(original) != expected:
        raise ValueError('source receipt changed during snapshot')
    return {**source, 'receipt_path': str(target.resolve()), 'native_receipt_path': str(original)}


def reference_map(frame: pl.DataFrame) -> dict:
    return {
        tuple(row[name] for name in KEYS): (
            row['reference_kind'], row['reference_price_decimal'],
            row['trade_ts'].strftime('%Y%m%d%H%M%S'), tuple(row['source_hashes']))
        for row in frame.iter_rows(named=True)
    }


def merge_reference_oracles(left: dict, right: dict) -> dict:
    """Combine per-source native reductions without guessing same-second order."""
    result = {}
    for key in left.keys() | right.keys():
        values = [side[key] for side in (left, right) if key in side]
        if any(v[0] not in OPEN_KINDS | LAST_KINDS for v in values):
            raise ValueError('unsupported reference kind')
        opens = [value for value in values if value[0] in OPEN_KINDS]
        relevant = opens or values
        stamp = (min if opens else max)(value[2] for value in relevant)
        chosen = [value for value in relevant if value[2] == stamp]
        prices = {value[1] for value in chosen if value[1] is not None}
        ambiguous = any(value[1] is None for value in chosen) or len(prices) != 1
        hashes = tuple(sorted({item for value in chosen for item in value[3]}))
        if opens:
            kind = 'ambiguous_first_second' if ambiguous else 'observed_session_open_candidate'
        else:
            kind = 'ambiguous_last_second' if ambiguous else 'last_observed_trade_candidate'
        result[key] = (kind, None if ambiguous else next(iter(prices)), stamp, hashes)
    return result


def finmind_native_reference(sources: list[dict], day: date, windows: dict) -> tuple[dict, dict]:
    """Reduce native FinMind rows with the independent sequential oracle."""
    clocks = {'08:45': day.strftime('%Y%m%d') + '084500',
              '09:00': day.strftime('%Y%m%d') + '090000'}
    latest = {clock: {} for clock in clocks}
    opening = {clock: {} for clock in clocks}
    counts = {'source_rows': 0, 'candidate_rows': 0, 'observed_rows_by_clock': {c: 0 for c in clocks}}
    for source in sources:
        observed_day = date.fromisoformat(source['day'])
        original = pl.read_parquet(source['path'])
        rows, stats = candidate_rows(original, day=observed_day, source_sha256=source['sha256'],
                                     candidate_products=set(windows))
        counts['source_rows'] += stats['source_rows']
        counts['candidate_rows'] += stats['candidate_rows']
        # Filter first so the sequential oracle visits only relevant rows.
        cutoff = datetime(day.year, day.month, day.day, 9, 0, tzinfo=ZoneInfo('Asia/Taipei'))
        for ts, product, series, strike, right, price, source_hash in rows.filter(
                pl.col('event_ts') < cutoff).select(*SOURCE_COLUMNS).iter_rows():
            stamp = ts.strftime('%Y%m%d%H%M%S')
            active = [clock for clock, boundary in clocks.items() if stamp < boundary]
            if not active:
                continue
            key = (product, series, decimal_text(strike), right)
            value = decimal_text(price)
            minute = ts.hour * 60 + ts.minute
            start, end = windows[product]
            is_open = ts.date() == day and start <= minute < end
            for clock in active:
                counts['observed_rows_by_clock'][clock] += 1
                update(latest[clock], key, stamp, value, source_hash, first=False)
                if is_open:
                    update(opening[clock], key, stamp, value, source_hash, first=True)
    result = {}
    for clock in clocks:
        records = {}
        for key, last in latest[clock].items():
            has_open = key in opening[clock]
            stamp, prices, hashes = opening[clock].get(key, last)
            kind = ('observed_session_open_candidate' if len(prices) == 1 else 'ambiguous_first_second') if has_open else (
                'last_observed_trade_candidate' if len(prices) == 1 else 'ambiguous_last_second')
            records[key] = (kind, next(iter(prices)) if len(prices) == 1 else None, stamp, tuple(sorted(hashes)))
        result[clock] = records
    return result, counts


def read_index(path: Path, expected_contract: str, day: date) -> tuple[dict, pl.DataFrame, str]:
    if not path.is_file() or path.is_symlink():
        raise ValueError('a regular pinned index manifest is required')
    digest = sha256_file(path)
    doc = json.loads(path.read_bytes())
    payload = Path(doc['data_path'])
    if (doc.get('contract') != expected_contract or doc.get('training_ready') is not False
            or payload.is_symlink() or not payload.is_file() or sha256_file(payload) != doc['data_sha256']):
        raise ValueError('research index identity or data digest differs')
    frame = pl.read_parquet(payload)
    source_hashes = {source['sha256'] for source in doc['source_pins']}
    if (frame.height != doc['rows'] or set((*SOURCE_COLUMNS, 'source_date')) - set(frame.columns)
            or set(frame['source_sha256'].unique().to_list()) - source_hashes
            or frame.filter(pl.col('source_date').is_null() | pl.col('event_ts').is_null()
                            | pl.col('source_sha256').is_null()
                            | (pl.col('source_date') != pl.col('event_ts').dt.date())
                            | (pl.col('source_date') > day)).height
            or day not in frame['source_date'].unique().to_list()):
        raise ValueError('index dates, rows, columns or source identities differ')
    return doc, frame.select(*SOURCE_COLUMNS), digest


def build(official_manifest: Path, finmind_manifest: Path, day: date, output: Path) -> dict:
    workspace = Path.cwd().resolve()
    if output.is_symlink() or output.exists():
        raise ValueError('a new regular research output is required')
    output = output.resolve()
    if not output.is_relative_to(workspace / 'data_finmind' / 'research'):
        raise ValueError('cross-source output must remain under data_finmind/research')
    if official_manifest.is_symlink() or finmind_manifest.is_symlink():
        raise ValueError('index manifests cannot be symlinks')
    official_manifest, finmind_manifest = official_manifest.resolve(), finmind_manifest.resolve()
    official, official_rows, official_sha = read_index(official_manifest, OFFICIAL_INDEX_CONTRACT, day)
    finmind, finmind_rows, finmind_sha = read_index(finmind_manifest, FINMIND_INDEX_CONTRACT, day)
    if official.get('event_calendar_date') != day.isoformat():
        raise ValueError('official index calendar date differs')
    if (official['policy_sha256'] != finmind['policy_sha256']
            or Path(official['policy_path']).resolve() != Path(finmind['policy_path']).resolve()):
        raise ValueError('cross-source candidate policy differs')
    policy = Path(official['policy_path'])
    if sha256_file(policy) != official['policy_sha256']:
        raise ValueError('candidate policy digest differs')
    windows, policy_pins = candidate_policy(policy)
    capacity_start = capacity_check(output.parent)
    finmind_sources = []
    for source in finmind['source_pins']:
        observed = pin_source(workspace / 'data_finmind' / 'complement', date.fromisoformat(source['day']))
        if observed != source:
            raise ValueError('FinMind native source differs from index')
        finmind_sources.append(snapshot_receipt(
            observed, workspace / 'data_finmind' / 'research' / 'option_reference_source_receipts'))
    if (len({source['day'] for source in finmind_sources}) != len(finmind_sources)
            or len({source['sha256'] for source in finmind_sources}) != len(finmind_sources)):
        raise ValueError('FinMind source days or identities repeat')
    source_days = {source['sha256']: source['day'] for source in finmind_sources}
    if finmind_rows.filter(pl.col('event_ts').dt.date().cast(pl.String)
                           != pl.col('source_sha256').replace_strict(source_days)).height:
        raise ValueError('FinMind event date differs from its native source partition')
    official_sources = [snapshot_receipt(
        source, workspace / 'data_tw_index_derivatives_ticks' / 'research' / 'option_reference_source_receipts')
        for source in official['source_pins']]
    code_paths = [Path(__file__), Path('scripts/audit_taifex_option_reference_oracle.py'),
                  Path('scripts/build_taifex_option_finmind_reference_window.py'),
                  Path('stockagent/data/tw_option_finmind_reference.py'),
                  Path('stockagent/data/tw_option_reference_candidates.py'),
                  Path('stockagent/data/tw_option_reference_event_index.py'),
                  Path('downloader/finmind_option_contract_keys.py')]
    code_pins = {str(p.resolve()): sha256_file(p) for p in code_paths}
    official_oracle, official_counts = native_reference(official_sources, day, windows)
    finmind_oracle, finmind_counts = finmind_native_reference(finmind_sources, day, windows)
    union_rows = pl.concat([official_rows, finmind_rows])
    parts, comparisons = [], {}
    for clock in ('08:45', '09:00'):
        merged = reference_candidates(union_rows, trading_date=day, decision_time=clock, session_windows=windows)
        finmind_only = reference_candidates(finmind_rows, trading_date=day, decision_time=clock, session_windows=windows)
        official_only = reference_candidates(official_rows, trading_date=day, decision_time=clock, session_windows=windows)
        actual = reference_map(merged)
        wanted = merge_reference_oracles(official_oracle[clock], finmind_oracle[clock])
        missing, extra = set(wanted) - set(actual), set(actual) - set(wanted)
        mismatch = [key for key in wanted.keys() & actual.keys() if wanted[key] != actual[key]]
        if missing or extra or mismatch:
            raise ValueError(f'{clock}: missing={len(missing)} extra={len(extra)} mismatched={len(mismatch)}')
        f, o = reference_map(finmind_only), reference_map(official_only)
        if f != finmind_oracle[clock] or o != official_oracle[clock]:
            raise ValueError(f'{clock}: source index differs from sequential native oracle')
        common = actual.keys() & f.keys()
        comparisons[clock] = {
            'finmind_keys': len(f), 'official_keys': len(o), 'union_keys': len(actual),
            'official_only_keys': len(o.keys() - f.keys()), 'finmind_only_keys': len(f.keys() - o.keys()),
            'union_ambiguous_keys': sum(v[1] is None for v in actual.values()),
            'added_ambiguous_keys_vs_finmind': sum(actual[k][1] is None for k in actual.keys() - f.keys()),
            'shared_kind_price_or_second_changed_vs_finmind': sum(actual[k][:3] != f[k][:3] for k in common),
            'shared_only_provenance_changed_vs_finmind': sum(actual[k][:3] == f[k][:3] and actual[k][3] != f[k][3] for k in common),
            'native_oracle_mismatches': 0,
        }
        parts.append(merged.with_columns(pl.lit(day).cast(pl.Date).alias('decision_date'),
                                         pl.lit(clock).alias('decision_time')))
    data = pl.concat(parts).sort('decision_date', 'decision_time', *KEYS)
    if (sha256_file(official_manifest) != official_sha or sha256_file(finmind_manifest) != finmind_sha
            or sha256_file(Path(official['data_path'])) != official['data_sha256']
            or sha256_file(Path(finmind['data_path'])) != finmind['data_sha256']
            or sha256_file(policy) != official['policy_sha256']
            or any(sha256_file(Path(p)) != h for p, h in {**policy_pins, **code_pins}.items())
            or any(sha256_file(Path(s['path'])) != s['sha256']
                   or sha256_file(Path(s['receipt_path'])) != s['receipt_sha256']
                   or sha256_file(Path(s['native_receipt_path'])) != s['receipt_sha256']
                   for s in official_sources + finmind_sources)):
        raise ValueError('a source, receipt, index, policy or implementation changed during processing')
    capacity_publish = capacity_check(output.parent)
    output.mkdir(parents=True, exist_ok=False)
    payload = output / 'observed_references.parquet'
    with tempfile.NamedTemporaryFile(dir=output, prefix='.references-', suffix='.tmp', delete=False) as stream:
        tmp = Path(stream.name)
    try:
        data.write_parquet(tmp, compression='zstd', statistics=True)
        if not data.equals(pl.read_parquet(tmp)):
            raise ValueError('reference payload readback differs')
        durable_replace(tmp, payload)
    finally:
        tmp.unlink(missing_ok=True)
    identity = {'contract': CONTRACT, 'official_index_sha256': official_sha, 'finmind_index_sha256': finmind_sha,
                'policy_sha256': official['policy_sha256'], 'implementation_sha256': code_pins}
    fingerprint = hashlib.sha256(json.dumps(identity, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    receipt = {'contract': CONTRACT, 'generated_at_utc': datetime.now(timezone.utc).isoformat(),
               'decision_date': day.isoformat(), 'data_fingerprint': fingerprint,
               'official_index_path': str(official_manifest), 'official_index_sha256': official_sha,
               'finmind_index_path': str(finmind_manifest), 'finmind_index_sha256': finmind_sha,
               'policy_path': str(policy), 'policy_sha256': official['policy_sha256'], 'policy_linked_pins': policy_pins,
               'implementation_sha256': code_pins, 'official_source_pins': official_sources,
               'finmind_source_pins': finmind_sources, 'native_official_counts': official_counts,
               'native_finmind_counts': finmind_counts, 'clock_comparison': comparisons,
               'data_path': str(payload), 'data_sha256': sha256_file(payload), 'rows': data.height,
               'capacity_at_start': capacity_start, 'capacity_before_publication': capacity_publish,
               'training_ready': False,
               'limitations': ['Only selected source files and prior calendar dates are represented.',
                   'A missing print or an ambiguous second cannot establish the true session open or last trade.',
                   'FinMind Float32 and official native-decimal values remain distinct; disagreement is not resolved.',
                   'Content-addressed receipt snapshots preserve bytes; the canonical owner may later refresh its receipt.',
                   'Source timestamp is not proof of historic arrival, listing, executable fill or account rules.',
                   'Research candidates cannot be used as training admission without separate causal and account gates.']}
    atomic_write_json(output / 'manifest.json', receipt)
    return receipt


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--official-index', type=Path, required=True)
    parser.add_argument('--finmind-index', type=Path, required=True)
    parser.add_argument('--date', type=date.fromisoformat, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    try:
        result = build(args.official_index, args.finmind_index, args.date, args.output)
    except CapacityNotAdmitted as error:
        print(json.dumps({'state': 'capacity_deferred', 'capacity': error.status}), flush=True)
        raise SystemExit(75) from error
    print(json.dumps({'manifest': str(args.output / 'manifest.json'), 'clock_comparison': result['clock_comparison'],
                      'training_ready': False}, ensure_ascii=False), flush=True)


if __name__ == '__main__':
    main()
