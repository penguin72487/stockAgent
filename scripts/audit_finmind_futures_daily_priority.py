#!/usr/bin/env python3
"""Read back exact FinMind futures daily priority responses without training adoption."""
from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
from pathlib import Path

import polars as pl

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.provider_priority import inspect


REPO = Path(__file__).resolve().parents[1]
ROOT = REPO / 'data_finmind/complement'


def audit(root: Path, plan_path: Path, request_id: str) -> dict:
    root, plan_path = root.resolve(), plan_path.resolve()
    plan_sha = sha256_file(plan_path)
    plan = json.loads(plan_path.read_bytes())
    if plan.get('contract') != 'finmind-finite-product-query-verification-v1':
        raise ValueError('wrong product verification plan')
    keys = {tuple(item.get(k) for k in ('dataset', 'data_id', 'partition'))
            for item in plan.get('keys', [])}
    if not keys or any(k[0] != 'TaiwanFuturesDaily' or k[2] != 'history'
                       or not isinstance(k[1], str) or not k[1] for k in keys):
        raise ValueError('unexpected daily futures priority scope')
    status = inspect(root, request_id, now=datetime.now(UTC))
    if status.get('provider') != 'finmind' or status.get('request_id') != request_id:
        raise ValueError('priority identity differs')
    items = {tuple(json.loads(item['key'])): item for item in status.get('items', [])}
    if set(items) != keys or len(items) != len(status.get('items', [])):
        raise ValueError('priority items differ from source plan')
    rows = []
    for key in sorted(keys):
        native = items[key]
        result = {'dataset': key[0], 'data_id': key[1], 'partition': key[2],
                  'task_state': native['state']}
        if native['state'] not in {'complete', 'observed_empty'}:
            result['status'] = 'not_yet_observed'
            rows.append(result)
            continue
        receipt_path = (root / native['receipt_path']).resolve()
        if not receipt_path.is_relative_to(root):
            raise ValueError('native receipt outside owner root')
        receipt_sha = sha256_file(receipt_path)
        receipt = json.loads(receipt_path.read_bytes())
        if (tuple(receipt.get(k) for k in ('dataset', 'data_id', 'partition')) != key
                or receipt.get('status') != native['state'] or receipt.get('rows') != native['rows']):
            raise ValueError('native receipt identity or row count differs')
        result.update(receipt_path=str(receipt_path), receipt_sha256=receipt_sha)
        if native['state'] == 'observed_empty':
            if receipt['rows'] != 0:
                raise ValueError('empty receipt has nonzero rows')
            result['status'] = 'provider_observed_empty_receipt_verified'
        else:
            payload_path = (root / receipt['parquet_path']).resolve()
            if not payload_path.is_relative_to(root) or sha256_file(payload_path) != receipt['sha256']:
                raise ValueError('native daily payload differs from receipt')
            data = pl.read_parquet(payload_path)
            if data.height != receipt['rows'] or data.height == 0 or not {
                'date', 'futures_id', 'contract_date', 'volume', 'open', 'trading_session'
            } <= set(data.columns):
                raise ValueError('native daily payload schema or row count differs')
            if data.filter(pl.col('futures_id').is_null() | (pl.col('futures_id') != key[1])).height:
                raise ValueError('native daily payload contains another product')
            dates = data.select(pl.col('date').cast(pl.String).str.to_date(strict=False).alias('day'))['day']
            result.update(status='source_payload_verified', rows=data.height,
                          first_date=str(dates.min()), last_date=str(dates.max()),
                          invalid_date_rows=dates.null_count(),
                          positive_volume_rows=data.filter(pl.col('volume') > 0).height,
                          distinct_contract_labels=data['contract_date'].n_unique(),
                          parquet_path=str(payload_path), parquet_sha256=receipt['sha256'])
            if sha256_file(payload_path) != receipt['sha256']:
                raise ValueError('native daily payload changed during readback')
        if sha256_file(receipt_path) != receipt_sha:
            raise ValueError('native receipt changed during readback')
        rows.append(result)
    if sha256_file(plan_path) != plan_sha:
        raise ValueError('product verification plan changed during readback')
    return {'contract': 'finmind_futures_daily_product_source_readback_v1',
            'observed_at_utc': datetime.now(UTC).isoformat(), 'request_id': request_id,
            'verification_plan': str(plan_path), 'verification_plan_sha256': plan_sha,
            'audit_sha256': sha256_file(Path(__file__)), 'results': rows,
            'provider_calls': 0, 'training_ready': False,
            'limitations': ['Source responses do not establish historical source completeness, point-in-time availability, dated currency or execution.',
                            'Daily prices are not decision-clock trades or executable quotes.',
                            'No source observation is admitted to the training dataset by this audit.']}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=ROOT)
    parser.add_argument('--verification-plan', type=Path, required=True)
    parser.add_argument('--request-id', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    if not output.is_relative_to(REPO / 'artifacts/markets') or output.exists() or output.is_symlink():
        raise ValueError('new versioned output must be inside artifacts/markets')
    result = audit(args.root, args.verification_plan, args.request_id)
    atomic_write_json(output, result)
    print(json.dumps({'output': str(output), 'verified': sum(x['status'] == 'source_payload_verified' for x in result['results']),
                      'total': len(result['results']), 'training_ready': False}, ensure_ascii=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
