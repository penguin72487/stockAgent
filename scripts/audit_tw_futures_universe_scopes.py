#!/usr/bin/env python3
"""Read back source-declared TWD futures category selections without admission."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path

import polars as pl

from downloader.artifact_io import sha256_file
from stockagent.data.tw_futures_margin_preparation import (
    FUTURES_UNIVERSE_SCOPES, FUTURES_UNIVERSE_SCOPE_CONTRACT, futures_preparation_universe,
)


def audit(source: Path) -> dict:
    source = source.resolve()
    before = sha256_file(source)
    universe = pl.read_csv(source, infer_schema=False)
    scopes = {}
    for scope in FUTURES_UNIVERSE_SCOPES:
        try:
            selected = futures_preparation_universe(universe, scope=scope)
        except ValueError as error:
            if str(error) != 'requested futures universe is empty':
                raise
            scopes[scope] = {'source_product_count': 0, 'products': []}
        else:
            scopes[scope] = {'source_product_count': selected.height,
                             'products': sorted(selected['product'].to_list())}
    if sha256_file(source) != before:
        raise ValueError('futures scope catalog changed during readback')
    return {
        'contract': 'tw_futures_source_category_readback_v1',
        'observed_at_utc': datetime.now(timezone.utc).isoformat(),
        'source_path': str(source), 'source_sha256': before,
        'scope_contract': FUTURES_UNIVERSE_SCOPE_CONTRACT,
        'implementation_path': str(Path(__file__).resolve().parent.parent /
                                   'stockagent/data/tw_futures_margin_preparation.py'),
        'implementation_sha256': sha256_file(Path(__file__).resolve().parent.parent /
                                             'stockagent/data/tw_futures_margin_preparation.py'),
        'catalog_rows': universe.height, 'scopes': scopes,
        'limitations': ['Source catalog categories and currency are not dated listing or contract evidence.',
                        'Counts are historical product-code sets, not simultaneous executable contracts.',
                        'Scope selection does not establish data, account, training or fill readiness.'],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    result = audit(args.source)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open('x', encoding='utf-8') as target:
        json.dump(result, target, ensure_ascii=False, indent=2)
        target.write('\n')
    print(json.dumps({'output': str(args.output), 'source_sha256': result['source_sha256'],
                      'scope_counts': {scope: data['source_product_count']
                                       for scope, data in result['scopes'].items()}}, ensure_ascii=False))


if __name__ == '__main__':
    main()
