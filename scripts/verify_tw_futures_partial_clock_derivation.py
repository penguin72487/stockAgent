#!/usr/bin/env python3
"""Independently compare every derived clock row with its strict parent.

Uses PyArrow rather than the Polars construction. The source-market evidence
is still the same vendor data; this is a derivation/readback check only.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json, sha256_file

CHANGED = ('status', 'entry_quote', 'entry_volume', 'source_file_sha256')
KEYS = ('date', 'physical_contract')


def compare_tables(old: pa.Table, new: pa.Table, candidates: pa.Table, *,
                   decision_time: str, window_minutes: int) -> int:
    if old.schema != new.schema or old.column_names != new.column_names or old.num_rows != new.num_rows:
        raise ValueError('clock schema or row count changed')
    for name in old.column_names:
        if name not in CHANGED and not old[name].equals(new[name]):
            raise ValueError(f'unchanged clock field differs: {name}')
    mask = pc.equal(new['status'], 'partial_observed_minute')
    if mask.null_count:
        raise ValueError('derived clock has null status')
    unchanged = pc.invert(mask)
    for name in CHANGED:
        if not pc.filter(old[name], unchanged).equals(pc.filter(new[name], unchanged)):
            raise ValueError(f'non-candidate clock field differs: {name}')
    before = old.filter(mask).select([*KEYS, 'status']).to_pylist()
    after = new.filter(mask).select([*KEYS, *CHANGED]).to_pylist()
    if any(row['status'] != 'unverified_execution_quarantined' for row in before):
        raise ValueError('a nonquarantined row was promoted')
    if [(r['date'], r['physical_contract']) for r in before] != [
            (r['date'], r['physical_contract']) for r in after]:
        raise ValueError('partial clock physical identity changed')
    wanted = candidates.filter(pc.and_(pc.equal(candidates['decision_time'], decision_time),
                                      pc.equal(candidates['window_minutes'], window_minutes)))
    source = wanted.select([*KEYS, 'entry_quote', 'entry_volume', 'source_file_sha256']).to_pylist()
    source_by_key = {(r['date'], r['physical_contract']): r for r in source}
    if len(source_by_key) != len(source) or len(after) != len(source):
        raise ValueError('partial candidates are duplicated or missing')
    for row in after:
        key = (row['date'], row['physical_contract'])
        if key not in source_by_key or any(row[k] != source_by_key[key][k] for k in (
                'entry_quote', 'entry_volume', 'source_file_sha256')):
            raise ValueError('partial clock values differ from candidate observations')
    return len(after)


def verify(*, clocks: Path, derived: Path, diagnostic_dir: Path, output: Path) -> dict:
    if output.exists():
        raise ValueError('independent readback output must not already exist')
    candidate_path = diagnostic_dir / 'observed_window_candidates.parquet'
    diag_path = diagnostic_dir / 'manifest.json'
    candidates_sha = sha256_file(candidate_path)
    diagnostic = json.loads(diag_path.read_text())
    if diagnostic.get('candidate_file', {}).get('sha256') != candidates_sha:
        raise ValueError('candidate file differs from diagnostic')
    candidates = pq.read_table(candidate_path)
    results = []
    for suffix, window in (('', 1), ('entry_window_300m', 300)):
        for clock in ('0845', '0900'):
            parent, child = clocks / suffix / clock, derived / suffix / clock
            parent_path, child_path = parent / 'entries.parquet', child / 'entries.parquet'
            manifest = json.loads((child / 'manifest.json').read_text())
            if (manifest.get('entries_sha256') != sha256_file(child_path)
                    or manifest.get('partial_observations', {}).get('data_sha256') != candidates_sha
                    or sha256_file(parent / 'opening_reference.parquet')
                    != sha256_file(child / 'opening_reference.parquet')
                    or sha256_file(parent / 'execution_quarantine.json')
                    != sha256_file(child / 'execution_quarantine.json')):
                raise ValueError('derived entry or retained evidence identity differs')
            old, new = pq.read_table(parent_path), pq.read_table(child_path)
            decision = clock[:2] + ':' + clock[2:]
            changed = compare_tables(old, new, candidates, decision_time=decision, window_minutes=window)
            if changed != manifest['partial_observations']['contract_days']:
                raise ValueError('derived count differs from receipt')
            results.append(dict(clock=decision, window_minutes=window, rows_checked=old.num_rows,
                candidate_rows_checked=changed, parent_sha256=sha256_file(parent_path),
                derived_sha256=sha256_file(child_path), retained_reference_and_quarantine=True))
    proof = dict(contract='tw_futures_partial_observed_clock_independent_readback_v1',
        independent_implementation=True, independent_market_source=False,
        training_ready=False, source_coverage_complete=False, candidate_sha256=candidates_sha,
        clocks=results)
    atomic_write_json(output, proof)
    return proof


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('clocks', 'derived', 'diagnostic-dir', 'output'):
        parser.add_argument('--' + name, type=Path, required=True)
    print(json.dumps(verify(**vars(parser.parse_args())), ensure_ascii=False), flush=True)


if __name__ == '__main__':
    main()
