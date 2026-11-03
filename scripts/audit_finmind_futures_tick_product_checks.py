#!/usr/bin/env python3
"""Read native product/day futures tick results without admitting training data."""
from __future__ import annotations

from datetime import UTC, datetime
import json
from pathlib import Path

import polars as pl

from downloader.artifact_io import sha256_file
from scripts.run_finmind_option_tick_product_priority_wait import checked_inputs


def audit(source_plan: Path, root: Path, request_id: str, now: datetime) -> dict:
    root = root.resolve()
    plan, status = checked_inputs(source_plan, root, request_id, now)
    states = {tuple(json.loads(item['key'])): item for item in status['items']}
    output = []
    for item in plan['checks']:
        key = (item['dataset'], item['data_id'], item['partition'])
        native = states[key]
        result = {'dataset': key[0], 'data_id': key[1], 'date': key[2],
                  'task_state': native['state'], 'expected_contracts': len(item['audited_missing_contracts'])}
        if native['state'] in {'complete', 'observed_empty'}:
            receipt = (root / native['receipt_path']).resolve()
            if not receipt.is_relative_to(root):
                raise ValueError('native receipt is outside the canonical owner')
            digest = sha256_file(receipt)
            data = json.loads(receipt.read_bytes())
            if (tuple(data.get(k) for k in ('dataset', 'data_id', 'partition')) != key
                    or data.get('status') != native['state'] or data.get('rows') != native['rows']):
                raise ValueError('native receipt identity or row count differs')
            result.update(receipt_path=str(receipt), receipt_sha256=digest)
            if native['state'] == 'observed_empty':
                if data['rows'] != 0:
                    raise ValueError('empty receipt has nonzero rows')
                result['status'] = 'provider_observed_empty_receipt_verified'
            else:
                parquet = (root / data['parquet_path']).resolve()
                if not parquet.is_relative_to(root) or sha256_file(parquet) != data['sha256']:
                    raise ValueError('native tick payload differs from receipt')
                raw = pl.read_parquet(parquet)
                if raw.height != data['rows'] or not {'date', 'futures_id', 'contract_date', 'price', 'volume'} <= set(raw.columns):
                    raise ValueError('native tick payload schema or rows differ')
                if raw.filter(pl.col('futures_id').is_null() | pl.col('date').is_null()
                              | (pl.col('futures_id') != key[1]) | ~pl.col('date').str.starts_with(key[2])).height:
                    raise ValueError('native tick contains another product or date')
                valid = raw.filter((pl.col('volume') > 0) & (pl.col('price') > 0))
                observed = valid.select(pl.col('futures_id').str.strip_chars().str.to_uppercase(),
                                        pl.col('contract_date').str.strip_chars().str.to_uppercase()).unique()
                expected = pl.DataFrame(item['audited_missing_contracts'],
                                        schema={'futures_id': pl.String, 'contract_date': pl.String}, orient='row').unique()
                missing = expected.join(observed, on=['futures_id', 'contract_date'], how='anti')
                result.update(status='candidate_keys_present' if not missing.height else 'missing_keys_remain',
                              matched_contracts=expected.height - missing.height,
                              missing_contracts=missing.to_dicts(), positive_price_quantity_rows=valid.height,
                              parquet_path=str(parquet), parquet_sha256=data['sha256'])
                if sha256_file(parquet) != data['sha256']:
                    raise ValueError('native payload changed during readback')
            if sha256_file(receipt) != digest:
                raise ValueError('native receipt changed during readback')
        else:
            result['status'] = 'not_yet_observed'
        output.append(result)
    return {'contract': 'finmind_futures_tick_product_source_readback_v1',
            'observed_at_utc': now.astimezone(UTC).isoformat(), 'priority_active': status['priority_active'],
            'request_id': request_id, 'source_plan': str(source_plan.resolve()),
            'source_plan_sha256': sha256_file(source_plan), 'audit_sha256': sha256_file(Path(__file__)),
            'results': output, 'provider_calls': 0, 'training_ready': False,
            'limitations': ['An observed-empty receipt proves only that response, not provider completeness.',
                'Physical-day contract presence does not establish opening time, complete ticks, or executable fills.',
                'No source observation is injected into a training dataset by this readback.']}
