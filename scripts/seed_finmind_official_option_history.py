#!/usr/bin/env python3
"""Seed official historical option candidates into the existing FinMind queue.

The input is a source-bound, independently read-back TAIFEX daily inventory.
The existing FinMind owner remains responsible for every API call, quota,
retry, response receipt and financial validation. Official observation does
not prove that FinMind accepts an identifier or that a contract is executable.
"""
from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
import io
import json
from pathlib import Path
import re
import sqlite3

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.download_finmind_complement import _add_missing_identifiers


def _read_source(path: Path) -> tuple[bytes, str]:
    if not path.is_absolute() or path.is_symlink() or any(p.is_symlink() for p in path.parents):
        raise ValueError(f"source path must be absolute and not redirected: {path}")
    data = path.read_bytes()
    import hashlib
    return data, hashlib.sha256(data).hexdigest()


def verified_official_candidates(summary_path: Path, readback_path: Path) -> tuple[list[str], dict]:
    summary_bytes, summary_sha = _read_source(summary_path)
    readback_bytes, readback_sha = _read_source(readback_path)
    summary, readback = json.loads(summary_bytes), json.loads(readback_bytes)
    if (summary.get('contract') != 'taifex_option_archive_product_day_inventory_v1'
            or readback.get('contract') != 'tw_option_all_product_daily_full_inventory_readback_v1'
            or readback.get('reference_summary_path') != str(summary_path)
            or readback.get('reference_summary_sha256') != summary_sha
            or readback.get('per_product_counts_equal') is not True
            or readback.get('product_count') != summary.get('distinct_observed_products')):
        raise ValueError('unverified_official_inventory_summary')
    inventory_path = Path(summary['product_inventory'])
    inventory_bytes, inventory_sha = _read_source(inventory_path)
    if (summary.get('product_inventory_sha256') != inventory_sha
            or readback.get('reference_inventory_sha256') != inventory_sha):
        raise ValueError('official_inventory_changed')
    manifest_path = Path(summary['source_manifest'])
    manifest_bytes, manifest_sha = _read_source(manifest_path)
    if summary.get('source_manifest_sha256') != manifest_sha:
        raise ValueError('official_manifest_changed')
    manifest = json.loads(manifest_bytes)
    receipts = {row['path']: row['sha256'] for row in manifest['receipts']}
    audited = summary['annual_inputs']
    verified = readback['years']
    if not audited or len(audited) != len(verified):
        raise ValueError('incomplete_independent_annual_readback')
    years = []
    for source, check in zip(audited, verified):
        path = Path(source['path'])
        year = check['year']
        if (not isinstance(year, int) or path.name != f'{year}_opt.zip'
                or check['source_path'] != str(path) or check['source_sha256'] != source['sha256']
                or receipts.get(str(path)) != source['sha256']
                or not path.is_absolute() or path.is_symlink() or any(p.is_symlink() for p in path.parents)
                or sha256_file(path) != source['sha256']):
            raise ValueError(f'annual_source_receipt_mismatch: {path}')
        years.append(year)
    if years != list(range(years[0], years[-1] + 1)):
        raise ValueError('annual_source_years_not_contiguous')
    rows = list(csv.DictReader(io.StringIO(inventory_bytes.decode('utf-8-sig'), newline='')))
    products = [row.get('product', '') for row in rows]
    if (not products or len(set(products)) != len(products)
            or any(not re.fullmatch(r'[A-Z0-9]{2,12}', code) for code in products)
            or len(products) != summary.get('distinct_observed_products')):
        raise ValueError('invalid_official_product_inventory')
    # Recheck the small controlling inputs after streaming archive hashes.
    for path, expected in ((summary_path, summary_sha), (readback_path, readback_sha),
                           (inventory_path, inventory_sha), (manifest_path, manifest_sha)):
        if sha256_file(path) != expected:
            raise ValueError(f'official_source_changed_during_validation: {path}')
    return sorted(products), dict(summary=str(summary_path), summary_sha256=summary_sha,
                                  inventory=str(inventory_path), inventory_sha256=inventory_sha,
                                  readback=str(readback_path), readback_sha256=readback_sha,
                                  manifest=str(manifest_path), manifest_sha256=manifest_sha,
                                  annual_years=years)


def seed(queue: Path, products: list[str]) -> dict:
    if queue.is_symlink() or not queue.is_file():
        raise ValueError('existing_canonical_finmind_queue_required')
    with sqlite3.connect(queue, timeout=10) as con:
        con.execute('BEGIN IMMEDIATE')
        rows = con.execute("SELECT data_id,partition,kind FROM tasks "
                           "WHERE dataset='TaiwanOptionDaily'").fetchall()
        existing = {row[0] for row in rows}
        conflicts = sorted({code for code, partition, kind in rows if code in products
                            and (partition, kind) != ('history', 'id_history')})
        if conflicts:
            raise ValueError(f'conflicting_existing_native_tasks: {conflicts}')
        missing = sorted(set(products) - existing)
        # This is the original owner's seeding function, including INSERT OR
        # IGNORE and its commit; no provider session or task reset is created.
        _add_missing_identifiers(con, 'TaiwanOptionDaily', products, priority=3)
        rows = con.execute("SELECT data_id,partition,kind,state FROM tasks "
                           "WHERE dataset='TaiwanOptionDaily'").fetchall()
        after = {row[0]: row[1:] for row in rows}
        if any(after.get(code) != ('history', 'id_history', 'pending') for code in missing):
            raise ValueError('seeded_native_task_readback_failed')
    return dict(candidate_count=len(products), existing_count=len(products)-len(missing),
                seeded_identifiers=missing, provider_calls=0,
                source_only_no_training_injection=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--summary', required=True, type=Path)
    parser.add_argument('--readback', required=True, type=Path)
    parser.add_argument('--queue', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    if args.output.exists():
        raise ValueError('output_receipt_already_exists')
    products, evidence = verified_official_candidates(args.summary.absolute(), args.readback.absolute())
    result = seed(args.queue, products)
    receipt = dict(contract='finmind_official_option_history_seed_v1',
                   observed_at_utc=datetime.now(timezone.utc).isoformat(),
                   source=evidence, **result,
                   limitations=['Official daily observation is not a FinMind availability guarantee.',
                                'No point-in-time listing, quote, fill or training readiness is asserted.'])
    atomic_write_json(args.output, receipt)
    print(json.dumps(receipt, ensure_ascii=False))


if __name__ == '__main__':
    main()
