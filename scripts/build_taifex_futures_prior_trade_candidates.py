#!/usr/bin/env python3
"""Build a source-only, versioned prior-trade candidate for two clocks.

This bounded sample does not rewrite training inputs, execute orders, or prove
historical data delivery.  Only exact dated physical generations are matched.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, time, timedelta, timezone
import hashlib
import json
from pathlib import Path
import tempfile

import polars as pl

from downloader.artifact_io import atomic_write_json, durable_replace, sha256_file
from scripts.build_taifex_option_finmind_reference_window import capacity_check
from scripts.download_taifex_recent_index_derivatives_ticks import _parse_zip
from stockagent.data.tw_futures_decision_clock import (bind_entry_specifications, dated_entry_sessions,
    opening_reference_contract, resolve_opening_references)
from stockagent.data.tw_futures_margin_release import read_bound_output
from stockagent.data.tw_futures_official_ticks import pin_official_tick_inventory
from stockagent.data.tw_futures_prior_trade_reference import prior_trade_events


def pinned_reference(path: Path, day: date, products: tuple[str, ...], daily_sha: str, clock: str):
    path = path.resolve()
    manifest = path.with_name('opening_reference_manifest.json')
    data_sha, manifest_sha = sha256_file(path), sha256_file(manifest)
    doc = json.loads(manifest.read_bytes())
    if (doc.get('status') != 'complete' or doc.get('contract') != opening_reference_contract(clock)
            or doc.get('data_sha256') != data_sha or doc.get('source_daily_sha256') != daily_sha):
        raise ValueError('previous reference identity or daily source differs')
    pattern = '^(' + '|'.join(products) + '):'
    rows = (pl.scan_parquet(path).filter((pl.col('date') == day)
             & pl.col('physical_contract').str.contains(pattern)).collect())
    if not rows.height or rows.select('date', 'physical_contract').is_duplicated().any():
        raise ValueError('empty or duplicate prior reference scope')
    return rows, {'path': str(path), 'sha256': data_sha, 'manifest': str(manifest),
                  'manifest_sha256': manifest_sha, 'pair_identity': doc['pair_identity']}


def candidate_references(early: pl.DataFrame, late: pl.DataFrame, official_events: pl.DataFrame,
                         day: date) -> tuple[dict[str, pl.DataFrame], dict]:
    keys = ['date', 'physical_contract', 'symbol']
    if not early.select(keys).sort(keys).equals(late.select(keys).sort(keys)):
        raise ValueError('old paired clocks do not have identical physical identities')
    if early.filter(pl.col('reference_kind') == 'observed_session_open').height:
        raise ValueError('the sample cannot reconstruct an earlier already-observed open')
    available = early.filter(pl.col('reference_kind') == 'last_observed_trade')
    # The saved ABI lacks the previous quote's timestamp.  It can be safely
    # reconstructed only when the old last trade is also that denominator.
    if available.filter(pl.col('reference_quote').is_null() | pl.col('prior_trade_quote').is_null()
            | (pl.col('reference_quote') != pl.col('prior_trade_quote'))
            | (pl.col('reference_available_by') >= datetime.combine(day, time()))).height:
        raise ValueError('old prior denominator cannot be reconstructed from this sample')
    base = available.select('physical_contract', pl.col('reference_quote').alias('price'),
        pl.col('reference_trade_date').alias('trade_date'), pl.col('reference_trade_ts').alias('trade_ts'),
        pl.col('reference_available_by').alias('available_by'),
        pl.col('reference_source_sha256').alias('source_sha256'), pl.lit(False).alias('is_session_open'),
        # A saved scalar does not prove whether another price occurred in its
        # last second. Preserve it as unknown unless a later certified event
        # replaces it; do not backfill certainty into the old receipt.
        pl.lit(False).alias('opening_ambiguous'), pl.lit(True).alias('fallback_ambiguous'))
    opens = late.filter(pl.col('reference_kind') == 'observed_session_open').select(
        'physical_contract', pl.col('reference_quote').alias('price'),
        pl.col('reference_trade_date').alias('trade_date'), pl.col('reference_trade_ts').alias('trade_ts'),
        pl.col('reference_available_by').alias('available_by'),
        pl.col('reference_source_sha256').alias('source_sha256'), pl.lit(True).alias('is_session_open'),
        pl.lit(False).alias('opening_ambiguous'), pl.lit(False).alias('fallback_ambiguous'))
    trades = pl.concat([base, official_events, opens], how='vertical_relaxed').unique()
    decisions = early.select(keys)
    result = {clock: resolve_opening_references(decisions, trades, decision_time=clock, version=2)
              for clock in ('08:45', '09:00')}
    # An old unavailable 09:00 can conceal an unverified or ambiguous opening.
    # Its lost source details are not reconstructed from a displayed null.
    unavailable = late.filter(pl.col('reference_kind') == 'unavailable').select(keys)
    unknown_last = (late.filter(pl.col('reference_kind') == 'last_observed_trade')
                    .select(*keys, 'reference_quote', 'reference_trade_ts', 'reference_available_by')
                    .join(early.select(*keys, 'reference_quote', 'reference_trade_ts', 'reference_available_by'),
                          on=keys, how='inner', validate='1:1', suffix='_early')
                    .filter(~pl.col('reference_quote').eq_missing(pl.col('reference_quote_early'))
                            | ~pl.col('reference_trade_ts').eq_missing(pl.col('reference_trade_ts_early'))
                            | ~pl.col('reference_available_by').eq_missing(pl.col('reference_available_by_early')))
                    .select(keys))
    withheld = pl.concat([unavailable, unknown_last], how='vertical').unique()
    result['08:45'] = result['08:45'].with_columns(pl.lit('v2_candidate').alias('candidate_selection'))
    result['09:00'] = (pl.concat([
        result['09:00'].join(withheld, on=keys, how='anti')
            .with_columns(pl.lit('v2_candidate').alias('candidate_selection')),
        late.join(withheld, on=keys, how='semi')
            .with_columns(pl.lit('unchanged_legacy_v1_unknown').alias('candidate_selection'))],
        how='vertical_relaxed').select(*early.columns, 'candidate_selection').sort(keys))
    return result, {'late_rows_withheld_and_unchanged': withheld.height}


def publish_frame(path: Path, frame: pl.DataFrame) -> None:
    with tempfile.NamedTemporaryFile(dir=path.parent, prefix='.prior-', suffix='.tmp', delete=False) as handle:
        temporary = Path(handle.name)
    try:
        frame.write_parquet(temporary, compression='zstd')
        if not frame.equals(pl.read_parquet(temporary)):
            raise ValueError('candidate parquet readback differs')
        durable_replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def build(args) -> dict:
    workspace = Path.cwd().resolve()
    output, daily, specs_path = args.output.resolve(), args.daily.resolve(), args.specifications.resolve()
    relative = output.relative_to(workspace) if output.is_relative_to(workspace) else None
    if (relative is None or not relative.parts or not relative.parts[0].startswith('data_')
            or output.exists() or output.is_symlink()):
        raise ValueError('candidate output must be a new data_* directory')
    products = tuple(sorted(set(args.products)))
    if not products or any(not p.isascii() or not p.isalnum() or p != p.upper() for p in products):
        raise ValueError('explicit uppercase product codes are required')
    daily_sha, specs_sha = sha256_file(daily), sha256_file(specs_path)
    specs_manifest_sha = sha256_file(specs_path.with_name('manifest.json'))
    specs, _ = read_bound_output(specs_path)
    early, old_early = pinned_reference(args.early, args.day, products, daily_sha, '08:45')
    late, old_late = pinned_reference(args.late, args.day, products, daily_sha, '09:00')
    if old_early['pair_identity'] != old_late['pair_identity']:
        raise ValueError('previous 08:45 and 09:00 were not a paired source')
    source = pin_official_tick_inventory(args.official_manifest, [args.day])[0]
    archive = Path(source['path'])
    if archive.stat().st_size != source['bytes'] or sha256_file(archive) != source['sha256']:
        raise ValueError('official archive differs from receipt')
    frames = (pl.scan_parquet(daily).filter(pl.col('date').is_between(args.day - timedelta(days=10), args.day)
             & pl.col('product').is_in(products)).select('date', 'product', 'contract', 'physical_contract',
                                                          'official_expiry').collect())
    # The union is an exclusion envelope: no possibly regular-session print is
    # introduced as a previous-session event when final-day hours are unknown.
    sessions = dated_entry_sessions(bind_entry_specifications(frames, specs),
                                    unknown_final_mode='union_for_full_book_verification')
    parsed = _parse_zip(archive, kind='futures', trading_date=args.day, source_sha256=source['sha256'],
        futures_products=products, futures_outright_contracts_only=True,
        futures_day_sessions={product: (0, 1440) for product in products})
    prints = parsed.select('product', pl.col('delivery_month_week').alias('contract'),
                           pl.col('event_ts').dt.replace_time_zone(None).alias('event_ts'), 'price')
    events, stats = prior_trade_events(prints, sessions, day=args.day, source_sha256=source['sha256'])
    candidates, selection = candidate_references(early, late, events, args.day)
    if (sha256_file(daily) != daily_sha or sha256_file(specs_path) != specs_sha
            or sha256_file(specs_path.with_name('manifest.json')) != specs_manifest_sha
            or sha256_file(archive) != source['sha256']
            or sha256_file(Path(source['receipt_path'])) != source['receipt_sha256']
            or any(sha256_file(Path(pin['path'])) != pin['sha256']
                   or sha256_file(Path(pin['manifest'])) != pin['manifest_sha256']
                   for pin in (old_early, old_late))):
        raise ValueError('candidate inputs changed during readback')
    capacity = capacity_check(output.parent)
    output.mkdir(parents=True, exist_ok=False)
    files = {}
    for name, frame in [('official_pre_session_events.parquet', events),
                        ('0845_candidates.parquet', candidates['08:45']),
                        ('0900_candidates.parquet', candidates['09:00'])]:
        path = output / name
        publish_frame(path, frame)
        files[name] = {'path': str(path), 'sha256': sha256_file(path), 'rows': frame.height}
    pin = {'daily_sha256': daily_sha, 'specifications_sha256': specs_sha,
           'specifications_manifest_sha256': specs_manifest_sha, 'official': source,
           'old_early': old_early, 'old_late': old_late,
           'implementation_sha256': {str(p.resolve()): sha256_file(p) for p in (
               Path(__file__), Path('stockagent/data/tw_futures_prior_trade_reference.py'),
               Path('stockagent/data/tw_futures_decision_clock.py'))}}
    result = {'contract': 'taifex_futures_prior_trade_candidate_sample_v2',
              'generated_at_utc': datetime.now(timezone.utc).isoformat(), 'date': args.day.isoformat(),
              'products': list(products), 'sources': pin,
              'fingerprint': hashlib.sha256(json.dumps(pin, sort_keys=True).encode()).hexdigest(),
              'reconstructed_reference_contracts': {clock: opening_reference_contract(clock, version=2)
                                                     for clock in candidates},
              'event_counts': stats, 'selection': selection, 'files': files, 'capacity': capacity,
              'training_ready': False,
              'limitations': ['This is a bounded source-only sample, not a full-history clock or training admission.',
                              'Archive timestamps do not establish historic provider receive times.',
                              'Missing natural-date or changed physical-generation identities remain unknown.',
                              'Unreconstructable old 09:00 rows stay unchanged and carry an explicit legacy-v1 selection label.',
                              'No fills, fees, volume capacity, margin, settlement or returns are inferred.']}
    atomic_write_json(output / 'manifest.json', result)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    for flag in ('official-manifest', 'daily', 'specifications', 'early', 'late', 'output'):
        parser.add_argument('--' + flag, type=Path, required=True)
    parser.add_argument('--day', type=date.fromisoformat, required=True)
    parser.add_argument('--products', nargs='+', required=True)
    args = parser.parse_args()
    result = build(args)
    print(json.dumps({'manifest': str(args.output / 'manifest.json'), 'event_counts': result['event_counts'],
                      'selection': result['selection'], 'training_ready': False}), flush=True)


if __name__ == '__main__':
    main()
