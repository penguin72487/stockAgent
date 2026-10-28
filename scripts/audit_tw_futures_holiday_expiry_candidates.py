#!/usr/bin/env python3
"""Read back retrospective expiry candidates for 2012 holiday positions.

This matches product and delivery month only. It does not prove corporate
generation continuity, the announcement's historical availability, broker
holiday valuation, or permission to trade or resume training.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import hashlib
import json
from pathlib import Path

import polars as pl


def digest(path: Path) -> str:
    before = path.stat()
    value = hashlib.sha256()
    with path.open('rb') as stream:
        while chunk := stream.read(8 * 1024 * 1024):
            value.update(chunk)
    after = path.stat()
    signature = lambda info: (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)
    if signature(before) != signature(after):
        raise ValueError('source changed while hashing')
    return value.hexdigest()


def match_candidates(pre: pl.DataFrame, final: pl.DataFrame) -> pl.DataFrame:
    if pre['physical_contract'].null_count() or pre['physical_contract'].n_unique() != pre.height:
        raise ValueError('physical contract identity is absent or ambiguous')
    pre = pre.with_columns(pl.col('physical_contract').str.extract(r':(\d{6})@', 1).alias('contract'))
    if pre['contract'].null_count():
        raise ValueError('physical contract lacks a six-digit delivery month')
    multiplicity = pre.group_by('product', 'contract').len(name='pre_generations_for_key')
    matches = pre.join(multiplicity, on=['product', 'contract'], how='left')
    matches = matches.join(final, on=['product', 'contract'], how='left')
    return matches.sort(['product', 'contract', 'physical_contract'])


def audit(source: Path, final: Path, manifest_path: Path, source_sha: str, final_sha: str,
          manifest_sha_expected: str, script_sha: str) -> dict:
    manifest_sha = digest(manifest_path)
    manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
    expected = manifest['outputs']['futures_final_settlement_history']
    if (manifest.get('status') != 'complete' or expected['sha256'] != final_sha
            or manifest_sha != manifest_sha_expected
            or digest(source) != source_sha or digest(final) != final_sha):
        raise ValueError('pinned futures source or manifest mismatch')
    pre = (pl.scan_parquet(source)
           .filter((pl.col('date') == date(2012, 1, 18)) & pl.col('can_hold_overnight')
                   & pl.col('official_expiry').is_null())
           .select('product', 'asset_class', 'physical_contract', 'corporate_generation').collect())
    if not pre.height:
        raise ValueError('no null-expiry holiday positions in pinned source')
    finals = pl.read_parquet(final).select('product', 'contract', 'settlement_date',
        'source_kind', 'source_file', 'source_sha256', 'reported_date_role', 'settlement_method')
    joined = match_candidates(pre, finals)
    receipt_by_sha = {item['sha256']: item for item in manifest['receipts']}
    raw_pins = {}
    for row in joined.filter(pl.col('settlement_date').is_not_null()).to_dicts():
        raw = Path(row['source_file'])
        receipt = receipt_by_sha.get(row['source_sha256'])
        if not receipt or Path(receipt['path']) != raw or digest(raw) != row['source_sha256']:
            raise ValueError('matched official raw receipt is absent or changed')
        raw_pins[str(raw)] = row['source_sha256']
    if digest(source) != source_sha or digest(final) != final_sha or digest(manifest_path) != manifest_sha:
        raise ValueError('source changed during audit')
    rows = joined.with_columns(pl.col('settlement_date').cast(pl.String)).to_dicts()
    return {
        'contract': 'retrospective_2012_holiday_expiry_candidates_v1',
        'observed_at_utc': datetime.now(timezone.utc).isoformat(), 'pre_date': '2012-01-18',
        'source_path': str(source), 'source_sha256': source_sha, 'final_path': str(final),
        'final_sha256': final_sha, 'final_manifest_path': str(manifest_path),
        'final_manifest_sha256': manifest_sha, 'audit_script_sha256': script_sha,
        'null_expiry_physical_contracts': pre.height,
        'rows_with_official_product_month_match': sum(row['settlement_date'] is not None for row in rows),
        'official_raw_receipt_sha256': raw_pins, 'candidates': rows,
        'limitations': ['Product and delivery month do not establish corporate generation identity.',
                        'Later final settlement is retrospective and not point-in-time availability.',
                        'A match does not establish broker holiday accounting, marks, cash or margin.',
                        'The audit does not alter source, execution, or training admission.'],
        'financial_or_training_adopted': False,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--final', type=Path, required=True)
    parser.add_argument('--manifest', type=Path, required=True)
    parser.add_argument('--source-sha', required=True)
    parser.add_argument('--final-sha', required=True)
    parser.add_argument('--manifest-sha', required=True)
    parser.add_argument('--script-sha', required=True)
    args = parser.parse_args()
    print(json.dumps(audit(args.source, args.final, args.manifest, args.source_sha,
                           args.final_sha, args.manifest_sha, args.script_sha), ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
