#!/usr/bin/env python3
"""Compare finite per-product tick receipts with their audited missing keys.

The original audit used float32 strikes, so matches here are candidate source
presence at that precision, not tick/event/session or execution completeness.
"""
from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
from pathlib import Path

import polars as pl

from downloader.artifact_io import atomic_write_json, sha256_file
from scripts.audit_taifex_option_contract_presence import compare_contracts
from scripts.run_finmind_option_tick_product_priority_wait import ROOT, checked_inputs


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
        if native['state'] == 'complete':
            receipt = (root / native['receipt_path']).resolve()
            if not receipt.is_relative_to(root):
                raise ValueError('native receipt is outside the canonical owner')
            receipt_sha = sha256_file(receipt)
            data = json.loads(receipt.read_bytes())
            if (tuple(data.get(k) for k in ('dataset', 'data_id', 'partition')) != key
                    or data.get('status') != 'complete' or data.get('rows') != native['rows']):
                raise ValueError('native receipt identity or row count differs')
            parquet = (root / data['parquet_path']).resolve()
            if not parquet.is_relative_to(root) or sha256_file(parquet) != data['sha256']:
                raise ValueError('native tick payload differs from receipt')
            raw = pl.read_parquet(parquet)
            if raw.height != data['rows'] or not {'date', 'option_id', 'contract_date', 'ExercisePrice',
                                                   'PutCall', 'price', 'volume'} <= set(raw.columns):
                raise ValueError('native tick payload schema or rows differ')
            if raw.filter(pl.col('option_id').is_null() | pl.col('date').is_null()
                          | (pl.col('option_id') != key[1]) | ~pl.col('date').str.starts_with(key[2])).height:
                raise ValueError('native tick contains another product or date')
            valid = raw.filter((pl.col('volume') > 0) & (pl.col('price') > 0))
            expected = pl.DataFrame(item['audited_missing_contracts'],
                schema={'product': pl.String, 'series': pl.String, 'strike': pl.Float32, 'right': pl.String},
                orient='row')
            missing = compare_contracts(expected, valid)
            result.update(status='candidate_keys_present' if not missing.height else 'missing_keys_remain',
                          matched_contracts=expected.unique().height - missing.height,
                          missing_contracts=missing.to_dicts(), positive_price_quantity_rows=valid.height,
                          receipt_path=str(receipt), receipt_sha256=receipt_sha,
                          parquet_path=str(parquet), parquet_sha256=data['sha256'])
            if sha256_file(receipt) != receipt_sha or sha256_file(parquet) != data['sha256']:
                raise ValueError('native receipt or payload changed during readback')
        elif native['state'] == 'observed_empty':
            receipt = (root / native['receipt_path']).resolve()
            if not receipt.is_relative_to(root):
                raise ValueError('native receipt is outside the canonical owner')
            receipt_sha = sha256_file(receipt)
            data = json.loads(receipt.read_bytes())
            if (tuple(data.get(k) for k in ('dataset', 'data_id', 'partition')) != key
                    or data.get('status') != 'observed_empty' or data.get('rows') != 0
                    or native.get('rows') != 0):
                raise ValueError('native empty receipt identity or row count differs')
            if sha256_file(receipt) != receipt_sha:
                raise ValueError('native receipt changed during readback')
            result.update(status='provider_observed_empty_receipt_verified',
                          receipt_path=str(receipt), receipt_sha256=receipt_sha)
        else:
            result['status'] = 'not_yet_observed'
        output.append(result)
    return {'contract': 'finmind_option_tick_product_source_readback_v2', 'observed_at_utc': now.isoformat(),
            'priority_active': status['priority_active'], 'request_id': request_id,
            'source_plan': str(source_plan.resolve()), 'source_plan_sha256': sha256_file(source_plan),
            'audit_sha256': sha256_file(Path(__file__)),
            'comparison_sha256': sha256_file(Path(__file__).with_name('audit_taifex_option_contract_presence.py')),
            'series_normalizer_sha256': sha256_file(Path('downloader/finmind_option_contract_keys.py')),
            'results': output, 'provider_calls': 0, 'training_ready': False,
            'limitations': ['Candidate contract keys use the original audit float32 strike precision.',
                'Vendor monthly YYYYMM.0 is compared as YYYYMM; weekly series and other decimals are not converted.',
                'An observed-empty receipt proves only that response, not unavailable data or complete coverage.',
                'Natural-day contract presence does not prove exact opening, complete ticks, historical access, or fill.',
                'No source observation is injected into a training dataset by this readback.']}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-plan', type=Path, required=True)
    parser.add_argument('--root', type=Path, default=ROOT)
    parser.add_argument('--request-id', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    result = audit(args.source_plan, args.root, args.request_id, datetime.now(UTC))
    atomic_write_json(args.output, result)
    print(json.dumps({'output': str(args.output), 'results': len(result['results']),
                      'training_ready': False, 'provider_calls': 0}))


if __name__ == '__main__':
    main()
