#!/usr/bin/env python3
"""Read back monthly official option settlement identities and detect collisions."""
from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
from pathlib import Path

import polars as pl

from downloader.artifact_io import atomic_write_json, sha256_file
from stockagent.data.tw_option_settlement_collisions import settlement_collisions


def audit(readback: Path, source_root: Path) -> dict:
    audit_sha = sha256_file(readback)
    upstream = json.loads(readback.read_bytes())
    if (upstream.get('contract') != 'taifex_stock_option_settlement_native_html_readback_v1'
            or upstream.get('training_ready') is not False):
        raise ValueError('independent source readback required')
    months = upstream.get('month_readbacks', [])
    if len(months) != upstream.get('months') or len({m['query_month'] for m in months}) != len(months):
        raise ValueError('monthly readback is incomplete or duplicated')
    pins: dict[str, str] = {str(readback): audit_sha}
    rows = []
    for month in months:
        name = month['query_month']
        receipt_path = (source_root / 'receipts' / f'{name}.json').resolve()
        if sha256_file(receipt_path) != month['receipt_sha256']:
            raise ValueError(f'receipt changed for {name}')
        receipt = json.loads(receipt_path.read_bytes())
        if (receipt.get('contract') != 'taifex_stock_option_final_settlement_month_v1'
                or receipt.get('query_month') != name or receipt.get('training_ready') is not False):
            raise ValueError(f'unsupported receipt for {name}')
        raw_path, data_path = Path(receipt['raw_path']), Path(receipt['payload_path'])
        for path, key, audit_key in ((raw_path, 'raw_sha256', 'raw_sha256'),
                                     (data_path, 'payload_sha256', 'payload_sha256')):
            digest = sha256_file(path)
            if digest != receipt[key] or digest != month[audit_key]:
                raise ValueError(f'source changed for {name}')
            pins[str(path)] = digest
        pins[str(receipt_path)] = month['receipt_sha256']
        frame = pl.read_parquet(data_path)
        if frame.height != month['rows']:
            raise ValueError(f'row count changed for {name}')
        for row in frame.iter_rows(named=True):
            row['query_month'] = name
            rows.append(row)
    if len(rows) != upstream['totals']['rows']:
        raise ValueError('source row total differs from upstream readback')
    collisions = settlement_collisions(rows)
    if any(sha256_file(Path(path)) != digest for path, digest in pins.items()):
        raise ValueError('a source changed during the readback')
    return {
        'contract': 'taifex_stock_option_settlement_key_collisions_v1',
        'generated_at_utc': datetime.now(UTC).isoformat(),
        'upstream_readback': str(readback), 'upstream_readback_sha256': audit_sha,
        'months': len(months), 'source_rows': len(rows),
        'colliding_product_series_keys': len(collisions), 'collisions': collisions,
        'implementation_sha256': {
            str(Path(__file__).resolve()): sha256_file(Path(__file__).resolve()),
            str(Path('stockagent/data/tw_option_settlement_collisions.py').resolve()):
                sha256_file(Path('stockagent/data/tw_option_settlement_collisions.py').resolve()),
        },
        'training_ready': False,
        'limitations': [
            'A collision may be separate generations or a duplicate; this diagnostic does not decide.',
            'Retrospective settlement dates must not become prior-decision features or eligibility.',
            'No source record does not prove the absence of a contract or settlement.',
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--readback', type=Path, required=True)
    parser.add_argument('--source-root', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists() or args.output.is_symlink():
        raise FileExistsError('preserve previous diagnostic')
    result = audit(args.readback.resolve(), args.source_root.resolve())
    atomic_write_json(args.output, result)
    print(json.dumps({'output': str(args.output), 'months': result['months'],
                      'rows': result['source_rows'],
                      'collisions': result['collisions']}, ensure_ascii=False))


if __name__ == '__main__':
    main()
