#!/usr/bin/env python3
"""Reconcile retained recent option prints without making training claims.

This only reads the existing FinMind owner and official ZIP receipts.  Each
published day is checked against the native sequential oracles in the shared
builders and the separate PyArrow readback.  Missing sources remain missing.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import fcntl
import hashlib
import json
from pathlib import Path
import tempfile

import polars as pl

from downloader.artifact_io import atomic_write_json, durable_replace, sha256_file
from scripts.audit_tw_option_cross_source_references import audit
from scripts.build_finmind_option_reference_event_index import build as build_finmind
from scripts.build_taifex_option_calendar_event_index import build as build_official
from scripts.build_taifex_option_finmind_reference_window import CapacityNotAdmitted, capacity_check
from scripts.build_tw_option_cross_source_references import (
    FINMIND_INDEX_CONTRACT, OFFICIAL_INDEX_CONTRACT, build as build_cross,
    read_index, snapshot_receipt,
)
from scripts.run_finmind_option_reference_event_history import verify as verify_finmind
from scripts.run_finmind_option_reference_history import window_dates


def select_days(official_dates: list[str], plan: dict) -> tuple[list[dict], dict]:
    """Select only explicitly pinned dates; never turn an absent day into empty."""
    if not official_dates or official_dates != sorted(set(official_dates)):
        raise ValueError('official dates must be nonempty, distinct and ordered')
    for value in official_dates:
        date.fromisoformat(value)
    planned = {}
    for window in plan['windows']:
        for day, pin in zip(window_dates(window), window['source_receipt_pins'], strict=True):
            key = day.isoformat()
            if key in planned:
                raise ValueError('repeated FinMind source date')
            planned[key] = pin
    dates = sorted(planned)
    if not dates:
        raise ValueError('empty FinMind source plan')
    selected = []
    for i, day in enumerate(official_dates):
        prior = next((candidate for candidate in reversed(dates) if candidate < day), None)
        if i == len(official_dates) - 1:
            state = 'next_official_source_unavailable'
        elif day not in planned:
            state = 'finmind_source_unavailable_or_unverified'
        elif prior is None:
            state = 'no_pinned_prior_finmind_source'
        else:
            state = 'selected'
        selected.append({'day': day, 'prior_finmind_day': prior,
                         'next_official_day': official_dates[i + 1] if i + 1 < len(official_dates) else None,
                         'state': state})
    return selected, planned


def single_window(pin: dict) -> dict:
    pins = [pin]
    fingerprint = hashlib.sha256(json.dumps(pins, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    return {'start': pin['date'], 'end': pin['date'], 'source_days': 1,
            'source_receipt_pins': pins, 'source_group_fingerprint': fingerprint}


def ensure_daily_finmind(root: Path, plan_path: Path, plan: dict, pin: dict, output: Path) -> Path:
    if not output.exists() and not output.is_symlink():
        build_finmind(root, plan_path, [date.fromisoformat(pin['date'])], output)
    verify_finmind(output, plan_path, plan, single_window(pin), root)
    return output / 'manifest.json'


def ensure_official(source_manifest: Path, policy: Path, day: date, next_day: str, output: Path) -> Path:
    if not output.exists() and not output.is_symlink():
        build_official(source_manifest, policy, day, output)
    manifest = output / 'manifest.json'
    doc, _, _ = read_index(manifest, OFFICIAL_INDEX_CONTRACT, day)
    if (doc.get('event_calendar_date') != day.isoformat()
            or doc.get('source_manifest_sha256') != sha256_file(source_manifest)
            or Path(doc.get('source_manifest_path', '')).resolve() != source_manifest.resolve()
            or doc.get('policy_sha256') != sha256_file(policy)
            or [s['date'] for s in doc['source_pins']] != [day.isoformat(), next_day]):
        raise ValueError('official index does not match frozen input scope')
    for source in doc['source_pins']:
        if (sha256_file(Path(source['path'])) != source['sha256']
                or sha256_file(Path(source['receipt_path'])) != source['receipt_sha256']
                or source['receipt_sha256'] != sha256_file(source_manifest)):
            raise ValueError('official source or receipt changed')
    pins = {**doc.get('implementation_sha256', {}), **doc.get('policy_linked_pins', {})}
    if not doc.get('implementation_sha256') or any(sha256_file(Path(p)) != h for p, h in pins.items()):
        raise ValueError('official index code or policy evidence changed')
    for clock in ('08:45', '09:00'):
        check = doc['native_sequential_oracle']['clock_readback'][clock]
        if any(check[name] != 0 for name in ('missing', 'extra', 'reference_kind_price_second_or_provenance_mismatch')):
            raise ValueError('official source oracle did not pass')
    return manifest


def ensure_composite(parents: list[Path], day: date, plan_path: Path, plan: dict, output: Path) -> Path:
    """Concatenate verified one-day compact indexes; no source values are invented."""
    docs, frames, parent_pins = [], [], []
    for parent in parents:
        # The outer runner has already checked the complete per-day source receipt.
        header = json.loads(parent.read_bytes())
        source_pins = header.get('source_pins', [])
        if len(source_pins) != 1:
            raise ValueError('one source day is required in each parent index')
        source_day = date.fromisoformat(source_pins[0]['day'])
        if source_day > day:
            raise ValueError('a parent source belongs to a future day')
        doc, _, digest = read_index(parent, FINMIND_INDEX_CONTRACT, source_day)
        if (doc.get('plan_sha256') != sha256_file(plan_path)
                or doc.get('policy_sha256') != plan['policy_sha256']
                or len(doc['source_pins']) != 1):
            raise ValueError('daily event index belongs to a different plan')
        docs.append(doc)
        frames.append(pl.read_parquet(doc['data_path']))
        parent_pins.append({'manifest_path': str(parent.resolve()), 'manifest_sha256': digest,
                            'data_sha256': doc['data_sha256']})
    days = [doc['source_pins'][0]['day'] for doc in docs]
    if len(days) != 2 or days != sorted(set(days)) or days[-1] != day.isoformat():
        raise ValueError('composite requires one earlier source and the current day')
    manifest = output / 'manifest.json'
    runner_sha = sha256_file(Path(__file__))
    if not output.exists() and not output.is_symlink():
        capacity_check(output.parent)
        data = pl.concat(frames).sort('source_date', 'product_raw', 'series_raw', 'strike_raw',
                                      'right_raw', 'event_ts', 'price_raw', 'source_sha256')
        output.mkdir(parents=True, exist_ok=False)
        payload = output / 'observed_events.parquet'
        with tempfile.NamedTemporaryFile(dir=output, prefix='.events-', suffix='.tmp', delete=False) as stream:
            tmp = Path(stream.name)
        try:
            data.write_parquet(tmp, compression='zstd', statistics=True)
            if not data.equals(pl.read_parquet(tmp)):
                raise ValueError('composite payload readback differs')
            if any(sha256_file(Path(p['manifest_path'])) != p['manifest_sha256'] for p in parent_pins):
                raise ValueError('parent index changed while composing')
            capacity_check(output.parent)
            durable_replace(tmp, payload)
        finally:
            tmp.unlink(missing_ok=True)
        identity = {'plan_sha256': sha256_file(plan_path), 'parents': parent_pins,
                    'runner_sha256': runner_sha, 'policy_sha256': plan['policy_sha256']}
        doc = {'contract': FINMIND_INDEX_CONTRACT, 'composition_contract': 'two_daily_indexes_v1',
               'generated_at_utc': datetime.now(timezone.utc).isoformat(),
               'data_fingerprint': hashlib.sha256(json.dumps(identity, sort_keys=True, separators=(',', ':')).encode()).hexdigest(),
               'plan_path': str(plan_path), 'plan_sha256': identity['plan_sha256'],
               'policy_path': plan['policy_path'], 'policy_sha256': plan['policy_sha256'],
               'parent_indexes': parent_pins, 'composition_implementation_sha256': runner_sha,
               'source_pins': [item['source_pins'][0] for item in docs],
               'known_missing_source_days': [item for item in plan.get('not_planned_days', [])
                                             if days[0] < item['date'] < days[-1]],
               'data_path': str(payload), 'data_sha256': sha256_file(payload), 'rows': data.height,
               'training_ready': False,
               'limitations': ['Only two observed days are included; intervening missing sources remain unknown.',
                               'Observed references are not fills, listing eligibility or training admission.']}
        atomic_write_json(manifest, doc)
    doc, _, _ = read_index(manifest, FINMIND_INDEX_CONTRACT, day)
    if (doc.get('composition_contract') != 'two_daily_indexes_v1'
            or doc.get('parent_indexes') != parent_pins or doc.get('composition_implementation_sha256') != runner_sha
            or doc.get('source_pins') != [item['source_pins'][0] for item in docs]
            or doc.get('plan_sha256') != sha256_file(plan_path)
            or doc.get('policy_sha256') != plan['policy_sha256']):
        raise ValueError('composite index differs from its verified parents')
    # Equality catches a stale or corrupt composition independently of its receipt.
    combined = pl.concat(frames).sort('source_date', 'product_raw', 'series_raw', 'strike_raw',
                                      'right_raw', 'event_ts', 'price_raw', 'source_sha256')
    if not combined.equals(pl.read_parquet(doc['data_path'])):
        raise ValueError('composite index data differs from parent payloads')
    return manifest


def verify_cross(manifest: Path, output: Path, official: Path, finmind: Path, day: date) -> dict:
    doc = json.loads(manifest.read_bytes())
    if (doc.get('decision_date') != day.isoformat()
            or doc.get('official_index_sha256') != sha256_file(official)
            or doc.get('finmind_index_sha256') != sha256_file(finmind)):
        raise ValueError('cross-source output scope or index identity changed')
    if output.exists() or output.is_symlink():
        if output.is_symlink() or not output.is_file():
            raise ValueError('existing audit is not a regular file')
        # Independent repeat uses a private temporary receipt and does not alter evidence.
        with tempfile.TemporaryDirectory(prefix='option-cross-audit-') as directory:
            check = audit(manifest, Path(directory) / 'readback.json')
        stored = json.loads(output.read_bytes())
        if any(stored.get(k) != check[k] for k in ('contract', 'manifest_sha256', 'data_sha256', 'rows', 'clock_readback')):
            raise ValueError('existing independent audit differs from repeated readback')
        return check
    return audit(manifest, output)


def run(root: Path, plan_path: Path, live_official: Path, data_root: Path, report_root: Path,
        summary_path: Path, wanted: list[str] | None) -> dict:
    workspace = Path.cwd().resolve()
    root, plan_path, live_official, data_root, report_root, summary_path = (
        p.resolve() for p in (root, plan_path, live_official, data_root, report_root, summary_path))
    if (not data_root.is_relative_to(workspace / 'data_finmind' / 'research')
            or not report_root.is_relative_to(workspace / 'artifacts' / 'markets')
            or not summary_path.is_relative_to(report_root) or summary_path.exists()):
        raise ValueError('research data and a new market report are required')
    plan_sha = sha256_file(plan_path)
    plan = json.loads(plan_path.read_bytes())
    if plan.get('contract') != 'finmind_option_reference_history_plan_v1' or plan.get('training_ready') is not False:
        raise ValueError('a research-only FinMind source plan is required')
    policy = Path(plan['policy_path'])
    if sha256_file(policy) != plan['policy_sha256']:
        raise ValueError('source plan policy changed')
    capacity_check(data_root)
    official_sha = sha256_file(live_official)
    frozen = Path(snapshot_receipt({'receipt_path': str(live_official), 'receipt_sha256': official_sha},
                                   workspace / 'data_tw_index_derivatives_ticks')['receipt_path'])
    official_doc = json.loads(frozen.read_bytes())
    selected, pins = select_days(official_doc['listing_page_dates']['options'], plan)
    if wanted is not None:
        if not wanted or len(wanted) != len(set(wanted)) or any(d not in {row['day'] for row in selected} for d in wanted):
            raise ValueError('requested dates must be distinct listed official days')
        selected = [row for row in selected if row['day'] in set(wanted)]
    data_root.mkdir(parents=True, exist_ok=True)
    report_root.mkdir(parents=True, exist_ok=True)
    official_root = workspace / 'data_tw_index_derivatives_ticks' / 'research' / data_root.name
    results, deferred = [], False
    with (data_root / '.writer.lock').open('a+') as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        for row in selected:
            item = dict(row)
            if row['state'] == 'selected':
                day = date.fromisoformat(row['day'])
                try:
                    if sha256_file(plan_path) != plan_sha or sha256_file(frozen) != official_sha:
                        raise ValueError('frozen input identity changed')
                    daily = [ensure_daily_finmind(root, plan_path, plan, pins[d], data_root / 'finmind_daily' / d)
                             for d in (row['prior_finmind_day'], row['day'])]
                    composite = ensure_composite(daily, day, plan_path, plan, data_root / 'finmind_pairs' / row['day'])
                    official = ensure_official(frozen, policy, day, row['next_official_day'], official_root / row['day'])
                    out = data_root / 'cross_source' / row['day']
                    if not out.exists() and not out.is_symlink():
                        build_cross(official, composite, day, out)
                        item['state'] = 'built_verified'
                    else:
                        item['state'] = 'reused_verified'
                    audit_path = report_root / f"{row['day']}_audit.json"
                    result = verify_cross(out / 'manifest.json', audit_path, official, composite, day)
                    item.update({'manifest': str(out / 'manifest.json'), 'audit': str(audit_path),
                                 'rows': result['rows'], 'clock_readback': result['clock_readback']})
                except CapacityNotAdmitted as error:
                    item.update({'state': 'capacity_deferred', 'capacity': error.status})
                    deferred = True
                except Exception as error:
                    item.update({'state': 'failed', 'error_type': type(error).__name__, 'error': str(error)})
            results.append(item)
            print(json.dumps(item, ensure_ascii=False), flush=True)
            if deferred:
                break
        fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
    if sha256_file(plan_path) != plan_sha or sha256_file(frozen) != official_sha:
        raise ValueError('frozen inputs changed during publication')
    summary = {'contract': 'tw_option_recent_cross_source_batch_v1',
               'generated_at_utc': datetime.now(timezone.utc).isoformat(),
               'finmind_plan_path': str(plan_path), 'finmind_plan_sha256': plan_sha,
               'official_receipt_snapshot': str(frozen), 'official_receipt_sha256': official_sha,
               'implementation_sha256': sha256_file(Path(__file__)),
               'selected_dates': len(selected), 'attempted_dates': len(results), 'results': results,
               'capacity_deferred': deferred, 'training_ready': False,
               'limitations': ['Only retained sources and the immediately preceding pinned FinMind date are compared.',
                               'Source holes or price ambiguity remain unresolved; no historic arrival is proven.',
                               'These are observed reference candidates, not execution, margin or training admission.']}
    atomic_write_json(summary_path, summary)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path('data_finmind/complement'))
    parser.add_argument('--plan', type=Path, required=True)
    parser.add_argument('--official', type=Path, default=Path('data_tw_index_derivatives_ticks/manifest.json'))
    parser.add_argument('--data-root', type=Path, required=True)
    parser.add_argument('--report-root', type=Path, required=True)
    parser.add_argument('--summary', type=Path, required=True)
    parser.add_argument('--dates', nargs='+')
    args = parser.parse_args()
    try:
        result = run(args.root, args.plan, args.official, args.data_root, args.report_root, args.summary, args.dates)
    except CapacityNotAdmitted as error:
        print(json.dumps({'state': 'capacity_deferred', 'capacity': error.status}), flush=True)
        raise SystemExit(75) from error
    if result['capacity_deferred']:
        raise SystemExit(75)
    if any(row['state'] == 'failed' for row in result['results']):
        raise SystemExit(1)


if __name__ == '__main__':
    main()
