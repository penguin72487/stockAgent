#!/usr/bin/env python3
"""Read-only continuity audit for the existing holiday expiry candidates.

Product and delivery-month matches alone do not prove that an initial physical
position is the same corporate-action generation as a later settlement row.
This audit reports what one pinned research history records; it never adopts a
price, resolves a transfer, or admits a contract to accounting or training.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import json
from pathlib import Path

import polars as pl

from downloader.artifact_io import sha256_file


CONTRACT = 'tw_futures_holiday_generation_paths_source_audit_v1'
HISTORY_COLUMNS = ('product', 'contract', 'date', 'physical_contract', 'corporate_generation',
                   'lifetime_status', 'corporate_boundary', 'corporate_transfer_target',
                   'corporate_boundary_sources', 'corporate_identity_candidate_only', 'terminal_event')


def stable_digest(path: Path) -> str:
    before = path.stat()
    value = sha256_file(path)
    after = path.stat()
    signature = lambda item: (item.st_dev, item.st_ino, item.st_size, item.st_mtime_ns, item.st_ctime_ns)
    if signature(before) != signature(after):
        raise ValueError('source changed while hashing')
    return value


def compare_paths(candidates: list[dict], history: pl.DataFrame, pre_date: date) -> list[dict]:
    """Keep maturity and initial-generation identities separate and explicit."""
    if not candidates or set(HISTORY_COLUMNS) - set(history.columns):
        raise ValueError('nonempty candidates and required source columns are required')
    by_key = {}
    for item in candidates:
        key = (item.get('product'), item.get('contract'))
        if (not all(isinstance(x, str) and x for x in key) or key in by_key
                or not isinstance(item.get('physical_contract'), str)
                or type(item.get('corporate_generation')) is not int
                or not isinstance(item.get('settlement_date'), str)):
            raise ValueError('candidate product, month or generation identity is ambiguous')
        settlement = date.fromisoformat(item['settlement_date'])
        if settlement < pre_date:
            raise ValueError('settlement precedes the initial position')
        by_key[key] = (item, settlement)
    rows_by_key = {key: [] for key in by_key}
    for row in history.select(HISTORY_COLUMNS).iter_rows(named=True):
        key = (row['product'], row['contract'])
        if key not in by_key:
            continue
        if not isinstance(row['date'], date) or type(row['corporate_generation']) is not int:
            raise ValueError('historical date or generation is missing')
        if not isinstance(row['physical_contract'], str) or not row['physical_contract']:
            raise ValueError('historical physical identity is missing')
        if pre_date <= row['date'] <= by_key[key][1]:
            rows_by_key[key].append(row)
    result = []
    for key, (item, settlement) in sorted(by_key.items()):
        rows = rows_by_key[key]
        initial = [row for row in rows if row['date'] == pre_date
                   and row['physical_contract'] == item['physical_contract']]
        if len(initial) != 1 or initial[0]['corporate_generation'] != item['corporate_generation']:
            raise ValueError('candidate does not match exactly one initial physical source row')
        first_generation = item['corporate_generation']
        changed = [row for row in rows if row['date'] > pre_date
                   and row['corporate_generation'] != first_generation]
        expiry_rows = [row for row in rows if row['date'] == settlement]
        boundaries = sorted({row['corporate_boundary'] for row in rows
                             if row['corporate_boundary'] is not None
                             and pre_date < row['corporate_boundary'] <= settlement})
        result.append({
            'product': key[0], 'contract': key[1],
            'pre_date': pre_date.isoformat(), 'pre_physical_contract': item['physical_contract'],
            'pre_generation': first_generation, 'settlement_date': settlement.isoformat(),
            'pre_lifetime_status': initial[0]['lifetime_status'],
            'pre_transfer_target_candidate': initial[0]['corporate_transfer_target'],
            'pre_boundary_source_sha256s': initial[0]['corporate_boundary_sources'],
            'pre_corporate_identity_candidate_only': initial[0]['corporate_identity_candidate_only'],
            'source_rows_in_interval': len(rows),
            'recorded_generations': sorted({row['corporate_generation'] for row in rows}),
            'recorded_physical_contracts': sorted({row['physical_contract'] for row in rows}),
            'recorded_corporate_boundaries': [day.isoformat() for day in boundaries],
            'first_different_generation_date': min((row['date'] for row in changed), default=None).isoformat()
                if changed else None,
            'settlement_date_generations': sorted({row['corporate_generation'] for row in expiry_rows}),
            'original_generation_present_on_settlement_date': any(
                row['corporate_generation'] == first_generation for row in expiry_rows),
            'status': ('recorded_generation_change' if changed else
                       'same_generation_recorded_not_proof_of_continuity'),
        })
    return result


def audit(source: Path, candidates_path: Path, source_sha: str, candidates_sha: str) -> dict:
    if stable_digest(source) != source_sha or stable_digest(candidates_path) != candidates_sha:
        raise ValueError('pinned source or candidate identity differs')
    candidate = json.loads(candidates_path.read_text())
    if (candidate.get('contract') != 'retrospective_2012_holiday_expiry_candidates_v1'
            or candidate.get('source_sha256') != source_sha):
        raise ValueError('candidate receipt does not bind the exact source')
    pre_date = date.fromisoformat(candidate['pre_date'])
    items = candidate['candidates']
    if len(items) != candidate['null_expiry_physical_contracts']:
        raise ValueError('candidate count differs from its source receipt')
    pairs = pl.DataFrame([{'product': item['product'], 'contract': item['contract']} for item in items]).lazy()
    last_settlement = max(date.fromisoformat(item['settlement_date']) for item in items)
    history = (pl.scan_parquet(source).filter(pl.col('date').is_between(pre_date, last_settlement))
               .select(HISTORY_COLUMNS).join(pairs, on=['product', 'contract'], how='semi').collect())
    rows = compare_paths(items, history, pre_date)
    if stable_digest(source) != source_sha or stable_digest(candidates_path) != candidates_sha:
        raise ValueError('source changed during audit')
    return {
        'contract': CONTRACT, 'observed_at_utc': datetime.now(timezone.utc).isoformat(),
        'source_path': str(source), 'source_sha256': source_sha,
        'candidate_receipt_sha256': candidates_sha,
        'audit_script_sha256': stable_digest(Path(__file__)),
        'candidate_contracts': len(rows),
        'recorded_generation_change_contracts': sum(x['status'] == 'recorded_generation_change' for x in rows),
        'original_generation_present_on_settlement_date': sum(x['original_generation_present_on_settlement_date'] for x in rows),
        'results': rows,
        'limitations': ['This records the pinned research history, not an independent exchange proof of every boundary.',
                        'Generation differences do not by themselves establish how a customer position transfers.',
                        'A same-generation observation cannot establish uninterrupted listing or historical availability.',
                        'Broker holiday valuation and terminal cash, margin and prices remain separately unverified.'],
        'financial_or_training_adopted': False,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--candidates', type=Path, required=True)
    parser.add_argument('--source-sha', required=True)
    parser.add_argument('--candidates-sha', required=True)
    args = parser.parse_args()
    print(json.dumps(audit(args.source, args.candidates, args.source_sha, args.candidates_sha), ensure_ascii=False))


if __name__ == '__main__':
    main()
