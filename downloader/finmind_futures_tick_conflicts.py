"""Evidence-bound futures tick rechecks for the existing Complement owner.

Registration is metadata only.  No provider is called and no task is reset
here; the owner's correction handler retains lock, quota and capacity control.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timedelta
import hashlib
import json
from pathlib import Path
import re

import polars as pl

from downloader.artifact_io import sha256_file


CONTRACT = 'finmind_futures_tick_contract_conflict_v1'
BATCHES = 'futures_tick_contract_conflict_batches'
AUDIT_CONTRACT = 'taifex_finmind_futures_contract_day_presence_v1'


def _stamp(value: str) -> datetime:
    result = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if result.tzinfo is None:
        raise ValueError('futures conflict timestamps must be timezone-aware')
    return result


def _within(workspace: Path, value: str) -> Path:
    path = Path(value)
    path = (workspace / path).resolve() if not path.is_absolute() else path.resolve()
    if not path.is_relative_to(workspace.resolve()):
        raise ValueError('futures conflict evidence outside workspace')
    return path


def _pinned(workspace: Path, value: str, expected: str) -> bytes:
    path = _within(workspace, value)
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != expected:
        raise ValueError('futures conflict evidence changed')
    return raw


def load_futures_tick_conflicts(root: Path, now: datetime, *, sidecar: Path):
    """Return exact requests from a pinned audited universe, or None if expired."""
    root = root.resolve()
    workspace = root.parents[1]
    raw = sidecar.read_bytes()
    if len(raw) > 64 * 1024:
        raise ValueError('oversized futures conflict plan')
    plan = json.loads(raw)
    if not isinstance(plan, dict) or plan.get('contract') != CONTRACT:
        raise ValueError('invalid futures conflict plan')
    created, expires = _stamp(plan['created_at_utc']), _stamp(plan['expires_at_utc'])
    if expires <= created or expires - created > timedelta(days=7):
        raise ValueError('invalid futures conflict duration')
    if not created <= now < expires:
        return None
    dates = plan.get('dates')
    if (not isinstance(dates, list) or not 0 < len(dates) <= 64
            or len(set(dates)) != len(dates)):
        raise ValueError('futures conflict requires 1 to 64 distinct dates')
    for day in dates:
        if not isinstance(day, str) or date.fromisoformat(day).isoformat() != day:
            raise ValueError('invalid futures conflict date')
    summary_bytes = _pinned(workspace, plan['summary_path'], plan['summary_sha256'])
    summary = json.loads(summary_bytes)
    if (summary.get('contract') != AUDIT_CONTRACT
            or _within(workspace, summary['provider_root']) != root):
        raise ValueError('futures conflict audit identity mismatch')
    manifest = json.loads(_pinned(workspace, summary['official_manifest_path'],
                                  summary['official_manifest_sha256']))
    official = _within(workspace, summary['official_path'])
    if (sha256_file(official) != summary['official_sha256']
            or manifest['all_futures_daily']['sha256'] != summary['official_sha256']
            or _within(workspace, manifest['all_futures_daily']['path']) != official):
        raise ValueError('futures conflict official source changed')
    days = json.loads(_pinned(workspace, summary['days_path'], summary['days_sha256']))
    missing = json.loads(_pinned(workspace, summary['unmatched_path'], summary['unmatched_sha256']))
    by_day = {row['date']: row for row in days}
    if len(by_day) != len(days):
        raise ValueError('duplicate futures conflict audit date')
    missing_by_day = defaultdict(set)
    for row in missing:
        if row['date'] in dates:
            key = (row['product'], row['contract'])
            if (not re.fullmatch(r'[A-Z0-9]{2,12}', key[0])
                    or not re.fullmatch(r'\d{6}(?:W[1-5])?', key[1])):
                raise ValueError('invalid futures conflict key')
            missing_by_day[row['date']].add(key)
    expected = (pl.scan_parquet(official)
                .filter(pl.col('date').is_in([date.fromisoformat(day) for day in dates])
                        & (pl.col('volume') > 0) & (pl.col('session') == '一般')
                        & pl.col('series_type').is_in(['monthly', 'weekly']))
                .select('date', 'product', 'contract').unique().collect())
    expected_by_day = defaultdict(set)
    for d, product, contract in expected.iter_rows():
        expected_by_day[d.isoformat()].add((str(product).strip().upper(), str(contract).strip().upper()))
    requests = []
    for day in dates:
        entry = by_day.get(day)
        keys = expected_by_day.get(day, set())
        if not entry or len(keys) != entry.get('official_positive_contracts'):
            raise ValueError('futures conflict official contract count differs')
        receipt_sha = entry.get('receipt_sha256', '')
        if not re.fullmatch(r'[a-f0-9]{64}', receipt_sha):
            raise ValueError('futures conflict receipt digest missing')
        wanted = missing_by_day.get(day, set())
        prior = None
        if entry.get('status') == 'verified_source_payload' and entry.get('provider_status') == 'complete':
            if not wanted or len(wanted) != entry.get('unmatched_contracts') or not wanted <= keys:
                raise ValueError('futures conflict missing contract set differs')
            prior = _within(workspace, entry['payload_path'])
            if not prior.is_relative_to(root) or sha256_file(prior) != entry['payload_sha256']:
                raise ValueError('futures conflict original source object changed')
            actual = (pl.scan_parquet(prior).filter(pl.col('volume') > 0).select(
                pl.col('futures_id').cast(pl.String).str.strip_chars().str.to_uppercase(),
                pl.col('contract_date').cast(pl.String).str.strip_chars().str.to_uppercase(),
            ).unique().collect())
            if wanted & set(actual.iter_rows()):
                raise ValueError('futures conflict already occurs in original source object')
            original_state = 'complete'
        elif (entry.get('status') == 'receipt_not_complete'
              and entry.get('provider_status') == 'observed_empty' and not wanted and keys):
            wanted = keys
            original_state = 'observed_empty'
        else:
            raise ValueError('futures conflict has no admissible original source evidence')
        identity = hashlib.sha256(f"{day}:{receipt_sha}:{plan['summary_sha256']}".encode()).hexdigest()
        request = {
            'correction_id': f'futures-tick-conflict-{identity}', 'owner': 'complement',
            'dataset': 'TaiwanFuturesTick', 'start_date': day, 'end_date': day,
            'data_ids': None, 'required_after_utc': created.isoformat(), 'allow_empty': False,
            'reason': 'official positive futures contract absent from retained natural-day vendor object',
            'source_url': 'https://www.taifex.com.tw/cht/3/futDailyMarketView',
            'watermark_basis': 'source_conflict_detection',
            'exact_source_conflict_key': {'dataset': 'TaiwanFuturesTick', 'data_id': '',
                                          'partition': day, 'kind': 'id_day'},
            'expected_original_task_state': original_state, 'original_receipt_sha256': receipt_sha,
            'expected_source_futures_contracts': [list(value) for value in sorted(wanted)],
        }
        if prior is not None:
            request.update(original_parquet_path=str(prior.relative_to(root)),
                           original_parquet_sha256=entry['payload_sha256'])
        requests.append(request)
    if sidecar.read_bytes() != raw or sha256_file(official) != summary['official_sha256']:
        raise ValueError('futures conflict evidence changed during verification')
    digest = hashlib.sha256(raw).hexdigest()
    return {'schema_version': 1, 'plan_id': f'futures-tick-conflict-{digest}', 'requests': requests}, digest
