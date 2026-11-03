#!/usr/bin/env python3
"""Materialize authenticated pre-session reference events for dated futures.

The product axis comes from each dated financial source, not a fixed present-day
universe.  This bounded source artifact is not an execution or training release.
"""
from __future__ import annotations

import argparse
import csv
from datetime import date, datetime, timedelta, timezone
import hashlib
import io
import json
from pathlib import Path
import re
import tempfile
import zipfile

import polars as pl

from downloader.artifact_io import atomic_write_json, durable_replace, sha256_file
from scripts.build_taifex_option_finmind_reference_window import capacity_check
from scripts.download_taifex_recent_index_derivatives_ticks import _parse_zip
from stockagent.data.tw_futures_decision_clock import (
    bind_entry_specifications,
    dated_entry_sessions,
    opening_reference_contract_from_manifest,
)
from stockagent.data.tw_futures_margin_release import read_bound_output
from stockagent.data.tw_futures_official_ticks import pin_official_tick_inventory
from stockagent.data.tw_futures_prior_trade_reference import prior_trade_events

SOURCE_ROOT = Path(__file__).resolve().parents[1]


def archive_outright_products(path: Path, day: date) -> set[str]:
    """Inventory exchange-observed products even when absent from the axis."""
    result = set()
    with zipfile.ZipFile(path) as archive, archive.open(f"Daily_{day.strftime('%Y_%m_%d')}.csv") as binary:
        with io.TextIOWrapper(binary, encoding='cp950', newline='') as text:
            for row in csv.reader(text):
                cells = [v.strip() for v in row]
                if (len(cells) >= 6 and cells[0].isdigit()
                        and re.fullmatch(r'\d{6}(?:W[1-5])?', cells[2])):
                    result.add(cells[1])
    return result


def stale_reference_counts(refs: pl.DataFrame, events: pl.DataFrame) -> dict:
    latest = (events.sort('trade_ts').group_by('physical_contract', maintain_order=True).last()
              .select('physical_contract', pl.col('trade_ts').alias('official_trade_ts'),
                      pl.col('price').alias('official_price'), 'fallback_ambiguous'))
    compared = refs.join(latest, on='physical_contract', how='inner', validate='1:1')
    later = compared.filter((pl.col('reference_kind') == 'last_observed_trade')
        & ((pl.col('reference_trade_ts').is_not_null() & (pl.col('official_trade_ts') > pl.col('reference_trade_ts')))
           | (pl.col('reference_trade_ts').is_null() & pl.col('reference_trade_date').is_not_null()
              & (pl.col('official_trade_ts').dt.date() > pl.col('reference_trade_date')))))
    return {'official_candidates_joined': compared.height, 'later_official_trade': later.height,
            'official_candidate_for_old_unavailable': compared.filter(pl.col('reference_kind') == 'unavailable').height,
            'later_unambiguous_price_different': later.filter(~pl.col('fallback_ambiguous')
                & (pl.col('reference_quote').is_null() | (pl.col('reference_quote') != pl.col('official_price')))).height,
            'later_ambiguous_last_second': later.filter(pl.col('fallback_ambiguous')).height}


def validate_reference_document(document: dict, *, data_sha: str, daily_sha: str) -> dict:
    """Accept any explicitly supported 08:45 reference ABI with exact input pins.

    This bounded investigation reads the existing reference for comparison. It
    does not adopt that version's fallback policy or publish a training clock.
    """
    contract = opening_reference_contract_from_manifest(document, '08:45')
    if document.get('data_sha256') != data_sha or document.get('source_daily_sha256') != daily_sha:
        raise ValueError('old paired reference source does not match daily history')
    return contract


def earlier_frame_identities(released: pl.DataFrame, supplemental: pl.DataFrame,
                             day: date) -> tuple[pl.DataFrame, int]:
    """Use same-identity earlier frame rows only as research event context.

    The target-day axis always stays the released axis. A blocked frame row
    cannot add a target contract, change an existing identity, or authorize a
    fill; it only helps identify an earlier official trade for that target.
    """
    target = released.filter(pl.col('date') == day).select(
        'product', 'contract', 'physical_contract').unique()
    context = (supplemental.filter(pl.col('date') < day)
        .join(target, on=['product', 'contract', 'physical_contract'], how='inner', validate='m:1')
        .join(released.select('date', 'product', 'contract').unique(),
              on=['date', 'product', 'contract'], how='anti'))
    if context.select('date', 'product', 'contract').is_duplicated().any():
        raise ValueError('supplemental frame has ambiguous physical identities')
    return pl.concat([released, context], how='vertical_relaxed'), context.height


def pin_identity_frame(frame: Path, daily: Path, daily_sha: str) -> dict:
    """Bind an unpublished frame to the same checkpoint as the daily release."""
    owner = daily.parents[2]
    checkpoint, release_manifest = owner / 'accounting_checkpoint.json', daily.parent / 'manifest.json'
    if frame != owner / 'frame.parquet' or frame.is_symlink():
        raise ValueError('identity frame must be the same financial owner')
    frame_sha, checkpoint_sha, release_sha = (sha256_file(p) for p in (frame, checkpoint, release_manifest))
    cp, release = json.loads(checkpoint.read_bytes()), json.loads(release_manifest.read_bytes())
    if (cp.get('outputs', {}).get('frame.parquet') != frame_sha
            or release.get('status') != 'complete'
            or release.get('outputs', {}).get('continuous_daily', {}).get('sha256') != daily_sha
            or cp.get('inputs', {}).get('source_manifest_sha256') != release.get('source_evidence_sha256')):
        raise ValueError('identity frame and released daily do not share a pinned source')
    return {'path': str(frame), 'sha256': frame_sha, 'checkpoint_path': str(checkpoint),
            'checkpoint_sha256': checkpoint_sha, 'daily_manifest_path': str(release_manifest),
            'daily_manifest_sha256': release_sha, 'use': 'prior_identity_context_only_not_tradability'}


def build(args) -> dict:
    workspace = Path.cwd().resolve()
    output, daily, specifications, refs = (p.resolve() for p in (args.output, args.daily, args.specifications, args.references))
    relative = output.relative_to(workspace) if output.is_relative_to(workspace) else None
    if relative is None or not relative.parts or not relative.parts[0].startswith('data_') or output.exists() or output.is_symlink():
        raise ValueError('new data_* output required')
    dates = args.dates
    if not dates or dates != sorted(set(dates)):
        raise ValueError('ordered, unique source trading dates required')
    daily_sha, spec_sha, spec_manifest_sha = (sha256_file(p) for p in
        (daily, specifications, specifications.with_name('manifest.json')))
    ref_manifest = refs.with_name('opening_reference_manifest.json')
    ref_sha, ref_manifest_sha = sha256_file(refs), sha256_file(ref_manifest)
    reference_doc = json.loads(ref_manifest.read_bytes())
    validate_reference_document(reference_doc, data_sha=ref_sha, daily_sha=daily_sha)
    entry_manifest = refs.with_name('manifest.json')
    entry_sha, entry_doc = sha256_file(entry_manifest), json.loads(entry_manifest.read_bytes())
    if (entry_doc.get('pair_identity') != reference_doc.get('pair_identity')
            or entry_doc.get('source_daily_sha256') != daily_sha
            or entry_doc.get('entry_specifications', {}).get('data_sha256') != spec_sha
            or entry_doc.get('entry_specifications', {}).get('manifest_sha256') != spec_manifest_sha):
        raise ValueError('dated specifications do not belong to the paired reference')
    specs, _ = read_bound_output(specifications)
    official = pin_official_tick_inventory(args.official_manifest, dates)
    for pin in official:
        if Path(pin['path']).stat().st_size != pin['bytes'] or sha256_file(Path(pin['path'])) != pin['sha256']:
            raise ValueError('official archive differs from its retained receipt')
    capacity_at_start = capacity_check(output.parent)
    daily_rows = (pl.scan_parquet(daily).filter(pl.col('date').is_between(dates[0] - timedelta(days=10), dates[-1]))
                  .select('date', 'product', 'contract', 'physical_contract', 'official_expiry').collect())
    sessions = dated_entry_sessions(bind_entry_specifications(daily_rows, specs),
                                    unknown_final_mode='union_for_full_book_verification')
    identity_pin, supplemental_sessions = None, None
    if getattr(args, 'identity_frame', None) is not None:
        frame = args.identity_frame.resolve()
        identity_pin = pin_identity_frame(frame, daily, daily_sha)
        axis = daily_rows.select('product', 'contract', 'physical_contract').unique()
        frame_rows = (pl.scan_parquet(frame)
            .filter(pl.col('date').is_between(dates[0] - timedelta(days=10), dates[-1])
                    & pl.col('official_row_observed').fill_null(False)
                    & ~pl.col('accounting_only_date').fill_null(True))
            .select('date', 'product', 'contract', 'physical_contract', 'official_expiry')
            .collect().join(axis, on=['product', 'contract', 'physical_contract'], how='inner'))
        supplemental_sessions = dated_entry_sessions(bind_entry_specifications(frame_rows, specs),
            unknown_final_mode='union_for_full_book_verification')
    results, counts = [], []
    for day, pin in zip(dates, official, strict=True):
        # Restrict each parse to that day's dated financial identities.  The
        # parser excludes spreads, but does not select products by today's list.
        products = tuple(sorted(sessions.filter(pl.col('date') == day)['product'].unique().to_list()))
        if not products:
            raise ValueError(f'no dated product identities for {day}')
        observed_products = archive_outright_products(Path(pin['path']), day)
        parsed = _parse_zip(Path(pin['path']), kind='futures', trading_date=day, source_sha256=pin['sha256'],
            futures_products=products, futures_outright_contracts_only=True,
            futures_day_sessions={p: (0, 1440) for p in products})
        prints = parsed.select('product', pl.col('delivery_month_week').alias('contract'),
                               pl.col('event_ts').dt.replace_time_zone(None).alias('event_ts'), 'price')
        day_sessions, added = (earlier_frame_identities(sessions, supplemental_sessions, day)
            if supplemental_sessions is not None else (sessions, 0))
        events, stats = prior_trade_events(prints, day_sessions, day=day, source_sha256=pin['sha256'])
        old = (pl.scan_parquet(refs).filter(pl.col('date') == day)
               .select('physical_contract', 'reference_quote', 'reference_kind', 'reference_trade_ts',
                       'reference_trade_date').collect())
        if old.select('physical_contract').is_duplicated().any():
            raise ValueError('previous reference has duplicate physical contracts')
        comparison = stale_reference_counts(old, events)
        results.append(events.with_columns(pl.lit(day).alias('source_trading_date')))
        counts.append({'date': day.isoformat(), 'products_in_dated_financial_axis': len(products),
                       'products_observed_in_official_archive': len(observed_products),
                       'official_observed_products_outside_financial_axis': sorted(observed_products - set(products)),
                       'official_source_rows': parsed.height, 'frame_prior_identity_rows_added': added,
                       **stats, **comparison})
        print(json.dumps(counts[-1]), flush=True)
    combined = pl.concat(results, how='vertical_relaxed').sort('source_trading_date', 'physical_contract', 'available_by')
    if combined.estimated_size() > 128 * 1024**2:
        raise ValueError('bounded candidate window exceeds output memory allowance; split dates')
    implementations = {str(p.resolve()): sha256_file(p) for p in (
        Path(__file__), SOURCE_ROOT / 'stockagent/data/tw_futures_prior_trade_reference.py',
        SOURCE_ROOT / 'stockagent/data/tw_futures_decision_clock.py',
        SOURCE_ROOT / 'scripts/download_taifex_recent_index_derivatives_ticks.py')}
    if (sha256_file(daily) != daily_sha or sha256_file(specifications) != spec_sha
            or sha256_file(specifications.with_name('manifest.json')) != spec_manifest_sha
            or sha256_file(refs) != ref_sha or sha256_file(ref_manifest) != ref_manifest_sha
            or sha256_file(entry_manifest) != entry_sha
            or (identity_pin is not None and any(sha256_file(Path(identity_pin[path])) != identity_pin[digest]
                for path, digest in [('path', 'sha256'), ('checkpoint_path', 'checkpoint_sha256'),
                                     ('daily_manifest_path', 'daily_manifest_sha256')]))
            or any(sha256_file(Path(s['path'])) != s['sha256']
                   or sha256_file(Path(s['receipt_path'])) != s['receipt_sha256'] for s in official)):
        raise ValueError('source changed while generating candidate window')
    capacity_before_publication = capacity_check(output.parent)
    output.mkdir(parents=True, exist_ok=False)
    payload = output / 'prior_trade_events.parquet'
    with tempfile.NamedTemporaryFile(dir=output, prefix='.prior-', suffix='.tmp', delete=False) as handle:
        temporary = Path(handle.name)
    try:
        combined.write_parquet(temporary, compression='zstd')
        if temporary.stat().st_size > 128 * 1024**2 or not combined.equals(pl.read_parquet(temporary)):
            raise ValueError('bounded event window failed independent readback')
        durable_replace(temporary, payload)
    finally:
        temporary.unlink(missing_ok=True)
    pins = {'daily': {'path': str(daily), 'sha256': daily_sha},
            'specifications': {'path': str(specifications), 'sha256': spec_sha,
                               'manifest_sha256': spec_manifest_sha},
            'references': {'path': str(refs), 'sha256': ref_sha, 'manifest_sha256': ref_manifest_sha,
                           'entry_manifest_sha256': entry_sha},
            'official_sources': official, 'implementation_sha256': implementations}
    if identity_pin is not None:
        pins['supplemental_prior_identity_frame'] = identity_pin
    result = {'contract': ('taifex_futures_pre_session_reference_events_window_v3' if identity_pin is not None
                           else 'taifex_futures_pre_session_reference_events_window_v2'),
              'generated_at_utc': datetime.now(timezone.utc).isoformat(),
              'dates': [d.isoformat() for d in dates], 'sources': pins,
              'fingerprint': hashlib.sha256(json.dumps(pins, sort_keys=True).encode()).hexdigest(),
              'data_path': str(payload), 'data_sha256': sha256_file(payload), 'rows': combined.height,
              'daily_counts': counts, 'capacity_at_start': capacity_at_start,
              'capacity_before_publication': capacity_before_publication, 'training_ready': False,
              'limitations': ['Only selected dates and financial-axis products; no absent date is inferred closed.',
                              'Exchange-observed codes outside the dated axis are inventoried but not assigned an inferred currency or identity.',
                              'Missing natural-date identities and changed physical generations are excluded, not repaired.',
                              'Supplemental blocked-frame identities are research candidates only, never target eligibility.',
                              'Observed event time is not proof of historical source delivery or complete night tape.',
                              'Last-second different prices are marked ambiguous; use version 2 reducer only.',
                              'This source is not a fill, eligibility, margin, accounting, full-history clock or trainer release.']}
    atomic_write_json(output / 'manifest.json', result)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    for flag in ('official-manifest', 'daily', 'specifications', 'references', 'output'):
        parser.add_argument('--' + flag, type=Path, required=True)
    parser.add_argument('--identity-frame', type=Path,
                        help='same-owner blocked frame, used for prior identity context only')
    parser.add_argument('--dates', nargs='+', type=date.fromisoformat, required=True)
    args = parser.parse_args()
    result = build(args)
    print(json.dumps({'manifest': str(args.output / 'manifest.json'), 'dates': len(result['dates']),
                      'rows': result['rows'], 'training_ready': False}), flush=True)


if __name__ == '__main__':
    main()
