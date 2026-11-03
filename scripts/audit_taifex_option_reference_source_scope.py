#!/usr/bin/env python3
"""Summarize provisional category choices on an observed reference window.

Classes and observed keys do not establish dated listing, currency or trading.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path

import polars as pl

from downloader.artifact_io import atomic_write_json, sha256_file
from scripts.build_taifex_option_reference_source_window import CONTRACT
from stockagent.data.tw_option_source_scope import annotate_option_source_scope, load_option_source_scope


def audit(window: Path) -> dict:
    manifest_path = window / 'manifest.json'
    manifest_sha = sha256_file(manifest_path)
    record = json.loads(manifest_path.read_bytes())
    if record.get('contract') != CONTRACT or record.get('training_ready') is not False:
        raise ValueError('expected an observed source-only reference window')
    data, policy = Path(record['data_path']), Path(record['policy_path'])
    if sha256_file(data) != record['data_sha256'] or sha256_file(policy) != record['policy_sha256']:
        raise ValueError('reference-window input SHA differs')
    pin = json.loads(policy.read_bytes())['inventory_comparison_pins']
    inventory = Path(pin['inventory_path'])
    scope = load_option_source_scope(inventory, expected_sha256=pin['inventory_sha256'], scopes=['all_options'])
    frame = pl.read_parquet(data).with_columns(pl.col('product_raw').alias('product'))
    classified = annotate_option_source_scope(frame, scope)
    if classified.height != record['rows'] or classified.filter(~pl.col('source_scope_match')).height:
        raise ValueError('all-options source scope did not retain every candidate row')
    classes = {}
    for row in (classified.group_by('source_scope_class')
                .agg(pl.len().alias('observed_clock_rows'),
                     pl.col('product').unique().sort().alias('observed_products'))
                .sort('source_scope_class').to_dicts()):
        classes[row.pop('source_scope_class')] = row
    if (sha256_file(manifest_path) != manifest_sha or sha256_file(data) != record['data_sha256']
            or sha256_file(policy) != record['policy_sha256']
            or sha256_file(inventory) != pin['inventory_sha256']):
        raise ValueError('source-window or category inventory changed during readback')
    return {'contract': 'taifex_option_reference_provisional_scope_readback_v1',
            'observed_at_utc': datetime.now(timezone.utc).isoformat(),
            'window_manifest': str(manifest_path.resolve()), 'window_manifest_sha256': manifest_sha,
            'data_sha256': record['data_sha256'], 'category_inventory': str(inventory.resolve()),
            'category_inventory_sha256': pin['inventory_sha256'], 'scope_fingerprint': scope.fingerprint,
            'source_clock_candidate_rows': classified.height, 'classes': classes,
            'script_sha256': sha256_file(Path(__file__)), 'training_ready': False,
            'limitations': ['Classes are provisional, not dated listing or quote-currency evidence.',
                            'Rows may include expired or otherwise ineligible contracts.',
                            'A source reference is neither executable quote nor a position or option account.']}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--window', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    if args.output.exists():
        raise FileExistsError('preserve previous category readback')
    result = audit(args.window.resolve())
    atomic_write_json(args.output, result)
    print(json.dumps({'output': str(args.output), 'classes': result['classes']}, ensure_ascii=False))


if __name__ == '__main__':
    main()
