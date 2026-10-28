"""Source-bound Sponsor daily-option rechecks; this module never calls a provider."""
from __future__ import annotations

import csv
from datetime import date, datetime, timedelta
import hashlib
import io
import json
from pathlib import Path

from downloader.artifact_io import sha256_file


CONTRACT = 'finmind_option_daily_product_conflicts_v1'
SIDECAR = 'option_daily_source_conflicts.json'
DAILY_URL = 'https://www.taifex.com.tw/cht/3/optDailyMarketView'


def _stamp(value: str) -> datetime:
    result = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if result.tzinfo is None:
        raise ValueError('timezone-aware timestamp required')
    return result


def _within(base: Path, value: str) -> Path:
    path = Path(value)
    path = (base / path).resolve() if not path.is_absolute() else path.resolve()
    if not path.is_relative_to(base.resolve()):
        raise ValueError('source conflict evidence outside workspace')
    return path


def _products(value: str) -> set[str]:
    return set(filter(None, value.split('|')))


def load_daily_option_conflicts(root: Path, now: datetime, *, sidecar: Path | None = None):
    root = root.resolve()
    sidecar = sidecar or root / SIDECAR
    if not sidecar.exists():
        return None
    raw = sidecar.read_bytes()
    if len(raw) > 1024 * 1024:
        raise ValueError('oversized daily option conflict plan')
    plan = json.loads(raw)
    if plan.get('contract') != CONTRACT:
        raise ValueError('invalid daily option conflict plan')
    created, expires = _stamp(plan['created_at_utc']), _stamp(plan['expires_at_utc'])
    if expires <= created or expires - created > timedelta(days=7):
        raise ValueError('invalid daily option conflict duration')
    if not created <= now < expires:
        return None
    workspace = root.parents[1]
    summary_path = _within(workspace, plan['product_summary_path'])
    summary_bytes = summary_path.read_bytes()
    if hashlib.sha256(summary_bytes).hexdigest() != plan['product_summary_sha256']:
        raise ValueError('product summary changed')
    summary = json.loads(summary_bytes)
    if (summary.get('contract') != 'taifex_option_vs_finmind_all_market_daily_presence_v2'
            or Path(summary['finmind_daily_root']).resolve() != root):
        raise ValueError('wrong daily product audit owner')
    inventory = _within(workspace, summary['day_results'])
    inventory_bytes = inventory.read_bytes()
    if hashlib.sha256(inventory_bytes).hexdigest() != summary['day_results_sha256']:
        raise ValueError('daily product comparison changed')
    coverage = _within(workspace, summary['official_coverage_summary'])
    coverage_bytes = coverage.read_bytes()
    if hashlib.sha256(coverage_bytes).hexdigest() != summary['official_coverage_summary_sha256']:
        raise ValueError('official daily inventory changed')
    official = json.loads(coverage_bytes)
    if official.get('contract') != 'tw_option_all_product_monthly_range_readback_v1':
        raise ValueError('wrong official monthly inventory')
    official_days = {row['date']: row for row in official['daily_positive_products']}
    comparison = {row['date']: row for row in csv.DictReader(io.StringIO(inventory_bytes.decode('utf-8'), newline=''))}
    entries = plan['dates']
    if not isinstance(entries, list) or not 0 < len(entries) <= 64 or len({x['date'] for x in entries}) != len(entries):
        raise ValueError('daily option conflicts require up to 64 unique dates')
    for source in official['sources']:
        if not any(source['start'] <= item['date'] <= source['end'] for item in entries):
            continue
        path, receipt_path = _within(workspace, source['path']), _within(workspace, source['receipt'])
        receipt = json.loads(receipt_path.read_bytes())
        if (receipt.get('contract') != 'taifex_option_all_products_daily_raw_v1'
                or receipt.get('range_start') != source['start'] or receipt.get('range_end') != source['end']
                or receipt.get('point_in_time_snapshot') is not False or receipt.get('path') != str(path)
                or receipt.get('sha256') != source['sha256'] or receipt.get('bytes') != source['bytes']
                or path.stat().st_size != source['bytes'] or sha256_file(path) != source['sha256']):
            raise ValueError('official source evidence changed')
    requests = []
    for item in entries:
        day = item['date']
        if date.fromisoformat(day).isoformat() != day:
            raise ValueError('invalid date')
        matches = [source for source in official['sources'] if source['start'] <= day <= source['end']]
        row, official_row = comparison.get(day), official_days.get(day)
        if len(matches) != 1 or row is None or official_row is None or row['source_status'] != 'daily_readback_verified':
            raise ValueError('conflict date lacks exact verified source coverage')
        expected = _products(official_row['day_positive_products'])
        old_products = _products(row['vendor_products'])
        old_positive = _products(row['vendor_positive_volume_products'])
        missing = expected - old_positive
        if (not missing or row['official_day_positive_products'] != official_row['day_positive_products']
                or missing != _products(row['missing_day_positive_volume_products'])):
            raise ValueError('daily positive-volume conflict no longer matches audit')
        snapshot = _within(root, item['original_receipt_snapshot'])
        snapshot_bytes = snapshot.read_bytes()
        if hashlib.sha256(snapshot_bytes).hexdigest() != row['receipt_sha256']:
            raise ValueError('original receipt snapshot changed')
        prior = json.loads(snapshot_bytes)
        if (prior.get('dataset') != 'TaiwanOptionDaily' or prior.get('data_id') != ''
                or prior.get('partition') != day or prior.get('kind') != row['source_kind']
                or prior.get('status') != 'complete' or prior.get('sha256') != row['parquet_sha256']):
            raise ValueError('original receipt identity changed')
        original_parquet = _within(root, prior['parquet_path'])
        if sha256_file(original_parquet) != prior['sha256']:
            raise ValueError('original daily data changed')
        identity = hashlib.sha256(f"{day}:{row['receipt_sha256']}:{summary['official_coverage_summary_sha256']}".encode()).hexdigest()
        requests.append({
            'correction_id': f'daily-option-product-conflict-{identity}', 'owner': 'sponsor',
            'dataset': 'TaiwanOptionDaily', 'start_date': day, 'end_date': day,
            'data_ids': None, 'required_after_utc': created.isoformat(), 'allow_empty': False,
            'reason': 'official option positive volume absent from retained FinMind daily product rows',
            'source_url': DAILY_URL, 'watermark_basis': 'source_conflict_detection',
            'exact_source_conflict_key': {'dataset': 'TaiwanOptionDaily', 'data_id': '',
                                          'partition': day, 'kind': 'day'},
            'expected_original_task_state': 'complete', 'original_receipt_sha256': row['receipt_sha256'],
            'original_parquet_path': prior['parquet_path'], 'original_parquet_sha256': prior['sha256'],
            'expected_source_products': sorted(expected | old_products),
            'expected_source_positive_volume_products': sorted(expected | old_positive),
            'preserve_original_daily_contract_keys': True,
        })
    if (sidecar.read_bytes() != raw or summary_path.read_bytes() != summary_bytes
            or inventory.read_bytes() != inventory_bytes or coverage.read_bytes() != coverage_bytes):
        raise ValueError('daily conflict evidence changed during verification')
    digest = hashlib.sha256(raw).hexdigest()
    return {'schema_version': 1, 'plan_id': f'daily-option-conflict-{digest}', 'requests': requests}, digest


def validate_priority_keys(root: Path, request_id: str, keys: tuple, now: datetime) -> None:
    """Bind every finite owner pass to active exact conflict and priority plans."""
    import sqlite3
    from downloader.provider_priority import inspect

    loaded = load_daily_option_conflicts(root, now)
    if not loaded:
        raise ValueError('daily option source-conflict intent is inactive')
    plan, _ = loaded
    planned = {(x['dataset'], '', x['start_date']) for x in plan['requests']}
    status = inspect(root, request_id, now=now)
    priority = {tuple(json.loads(row['key'])) for row in status['items']}
    if (status.get('provider') != 'finmind' or not status.get('priority_active')
            or not keys or len(keys) != len(set(keys)) or not set(keys) <= planned.intersection(priority)):
        raise ValueError('finite daily option keys require active exact priority')
    uri = (root / 'queue.sqlite3').resolve().as_uri() + '?mode=ro'
    with sqlite3.connect(uri, uri=True, timeout=5) as con:
        for request in plan['requests']:
            key = (request['dataset'], '', request['start_date'])
            if key not in keys:
                continue
            row = con.execute("SELECT state FROM finmind_correction_tasks WHERE owner='sponsor' "
                              "AND correction_id=? AND dataset=? AND data_id=? AND partition=?",
                              (request['correction_id'], *key)).fetchone()
            if row is None or row[0] not in {'queued', 'repaired', 'already_refreshed',
                                             'observed_empty_unverified', 'calendar_excluded_unverified'}:
                raise ValueError('finite daily option correction is not adopted')


def capacity_probe(root: Path, request_id: str, keys: tuple) -> dict:
    """Conservative physical preflight for one explicitly bounded response."""
    from datetime import UTC
    from downloader.finmind_option_finite_priority import ORDINARY_CAPACITY_BLOCKED
    from scripts.check_acquisition_capacity import priority_dispatch_status

    validate_priority_keys(root, request_id, keys, datetime.now(UTC))
    result = priority_dispatch_status(root, additional_headroom_bytes=24 * 1024**3 + 64 * 1024**2)
    if result.get('state') == 'not_enrolled':
        return {'state': 'deferred', 'reason': 'finite_priority_requires_enrolled_physical_capacity'}
    if (result.get('state') == 'admitted'
            and result.get('backing_volume', {}).get('observation_basis') == 'same_dispatch_physical_probe'):
        result = {**result, 'guard_kind': ORDINARY_CAPACITY_BLOCKED}
    return result


def priority_reservation(root: Path, reservation: dict, now: datetime) -> dict:
    """Use the existing foreign-history yield without mutating either queue."""
    import sqlite3
    from downloader.finmind_product_query_verification import prioritize_source_checks

    path = root.parent / 'complement/queue.sqlite3'
    if not path.is_file() or path.is_symlink():
        return reservation
    try:
        with sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True, timeout=2) as con:
            return prioritize_source_checks(con, reservation, now, frozenset(), finite_query_ready=True)
    except (sqlite3.Error, OSError):
        return reservation
