#!/usr/bin/env python3
"""Read back provisional option categories on one retained official tick file."""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
import csv
import hashlib
import io
import json
from pathlib import Path
import zipfile

import polars as pl

from downloader.artifact_io import atomic_write_json, sha256_file
from stockagent.data.tw_futures_official_ticks import pin_official_option_tick_inventory
from stockagent.data.tw_option_product_ticks import _COLUMNS, iter_official_option_trade_batches
from stockagent.data.tw_option_source_scope import annotate_option_tick_source_scope, load_option_source_scope


def audit(day: date, inventory: Path, inventory_sha256: str, manifest: Path) -> dict:
    implementations = [Path(__file__).resolve(), Path('stockagent/data/tw_option_product_ticks.py').resolve(),
                       Path('stockagent/data/tw_option_source_scope.py').resolve()]
    pins = {str(path): sha256_file(path) for path in implementations}
    scope = load_option_source_scope(inventory, expected_sha256=inventory_sha256, scopes=['all_options'])
    source = pin_official_option_tick_inventory(manifest, [day])[0]
    counts: Counter[str] = Counter()
    products: dict[str, set[str]] = defaultdict(set)
    total = matched = 0
    fingerprints: set[str] = set()
    reader_digest = hashlib.sha256()

    def add_row(digest, row):
        digest.update(json.dumps(row, ensure_ascii=False, separators=(',', ':')).encode('utf-8') + b'\n')

    for frame in iter_official_option_trade_batches(source):
        annotated = annotate_option_tick_source_scope(frame, scope)
        total += annotated.height
        matched += annotated.filter(pl.col('source_scope_match')).height
        fingerprints.update(annotated['tick_scope_fingerprint'].unique().to_list())
        for row in annotated.select('source_row_number', *_COLUMNS).iter_rows():
            add_row(reader_digest, row)
        for category, product, n in annotated.group_by('source_scope_class', 'product').len().iter_rows():
            counts[category] += n
            products[category].add(product)
    raw_digest, raw_rows = hashlib.sha256(), 0
    with zipfile.ZipFile(source['path']) as archive:
        with archive.open(f"OptionsDaily_{day.strftime('%Y_%m_%d')}.csv") as binary:
            rows = csv.reader(io.TextIOWrapper(binary, encoding='cp950', newline=''))
            next(rows)
            next(rows)
            for ordinal, row in enumerate(rows, start=3):
                if len(row) != len(_COLUMNS):
                    raise ValueError(f'invalid original CSV row {ordinal}')
                add_row(raw_digest, (ordinal, *(value.strip() for value in row)))
                raw_rows += 1
    if (sha256_file(inventory) != inventory_sha256
            or sha256_file(Path(source['path'])) != source['sha256']
            or sha256_file(Path(source['receipt_path'])) != source['receipt_sha256']
            or any(sha256_file(Path(path)) != digest for path, digest in pins.items())):
        raise ValueError('source inventory or audit implementation changed during readback')
    if (total == 0 or total != matched or total != raw_rows or len(fingerprints) != 1
            or reader_digest.hexdigest() != raw_digest.hexdigest()):
        raise ValueError('source rows, order, values or all-options scope differ')
    return {
        'contract': 'tw_option_tick_provisional_source_scope_readback_v1',
        'observed_at_utc': datetime.now(timezone.utc).isoformat(), 'date': str(day),
        'source': source, 'inventory': str(inventory.resolve()), 'inventory_sha256': inventory_sha256,
        'implementation_sha256': pins, 'scope': ['all_options'],
        'tick_scope_fingerprint': next(iter(fingerprints)), 'source_side_rows': total,
        'selected_source_side_rows': matched, 'rows_by_class': dict(sorted(counts.items())),
        'independent_raw_csv_rows': raw_rows, 'independent_raw_csv_rows_sha256': raw_digest.hexdigest(),
        'reader_rows_sha256': reader_digest.hexdigest(),
        'products_by_class': {key: sorted(value) for key, value in sorted(products.items())},
        'limitations': [
            'Catalog classifications are provisional, not date-specific listing or currency verification.',
            'Side rows include source duplicates and are not independent trades, executable fills or a training admission.',
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--date', type=date.fromisoformat, required=True)
    parser.add_argument('--inventory', type=Path, required=True)
    parser.add_argument('--inventory-sha256', required=True)
    parser.add_argument('--tick-manifest', type=Path, default=Path('data_tw_index_derivatives_ticks/manifest.json'))
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise FileExistsError('preserve the previous audit; choose a new output path')
    result = audit(args.date, args.inventory, args.inventory_sha256, args.tick_manifest)
    atomic_write_json(args.output, result)
    print(f"{result['selected_source_side_rows']}/{result['source_side_rows']} source side rows retained; output={args.output}")


if __name__ == '__main__':
    main()
