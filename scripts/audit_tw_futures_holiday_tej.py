#!/usr/bin/env python3
"""Compare holiday generation candidates to retained, independently pinned TEJ cells.

This source readback does not establish historical availability, inventory
transfer, valuation or permission to train. Missing cells remain unknown.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import json
from pathlib import Path

import polars as pl

from downloader.artifact_io import atomic_write_json, sha256_file
from scripts.audit_tw_futures_corporate_vendor import _inside, reconcile, verify_raw_vendor_cells
from scripts.audit_tw_futures_holiday_generation_terms import verify_originals


def audit(workspace: Path, report: Path, worklist: Path, terms: Path,
          native_manifest: Path, output: Path) -> dict:
    workspace = workspace.resolve()
    report, worklist, terms, native_manifest, output = (
        p.resolve() for p in (report, worklist, terms, native_manifest, output))
    if any(not p.is_relative_to(workspace) for p in (report, worklist, terms, native_manifest)):
        raise ValueError('source outside workspace')
    if not output.is_relative_to(workspace / 'artifacts') or output.exists():
        raise ValueError('output must be a new artifact')
    prior = json.loads(report.read_text())
    if prior.get('contract') != 'tw_futures_holiday_generation_terms_source_audit_v8':
        raise ValueError('unexpected candidate source contract')
    if sha256_file(worklist) != prior.get('worklist_sha256'):
        raise ValueError('candidate worklist changed')
    if sha256_file(terms) != prior['input_pins'].get(str(terms.relative_to(workspace))):
        raise ValueError('official terms differ from candidate manifest')
    candidates = prior['results']
    if len(candidates) != prior['candidate_contracts']:
        raise ValueError('candidate count changed')
    selected = [x for x in candidates if x['status'] == 'unique_candidate_term']
    keys = [(x['pre_transfer_target_candidate'], x['contract'], x['term']['effective_date']) for x in selected]
    if len(set(keys)) != len(keys):
        raise ValueError('ambiguous candidate keys')
    work = pl.read_csv(worklist, schema_overrides={'contract': pl.String}).to_dicts()
    if {(x['product'], x['contract'], x['exact_dates']) for x in work} != set(keys) or len(work) != len(keys):
        raise ValueError('candidate worklist does not match official terms')
    originals = verify_originals(workspace, selected)

    native = json.loads(native_manifest.read_text())
    values = native_manifest.parent / 'native_adjustment_values.parquet'
    if sha256_file(values) != native['outputs']['native_adjustment_values.parquet']:
        raise ValueError('retained native aggregate changed')
    members = {x['path']: x['sha256'] for x in native['native_source_members']}
    if len(members) != len(native['native_source_members']):
        raise ValueError('duplicate native source manifest member')
    table = pl.read_parquet(values)
    terms_rows = pl.read_parquet(terms).to_dicts()
    pins = {str(p.relative_to(workspace)): sha256_file(p)
            for p in (report, worklist, terms, native_manifest, values, Path(__file__).resolve())}
    results, missing = [], []
    tej = workspace / 'data_tej'
    for row, key in zip(selected, keys, strict=True):
        product, month, day = key
        symbol = product + month
        matching = table.filter((pl.col('_query_symbol') == symbol) & (pl.col('_query_period') == day)).to_dicts()
        if not matching:
            missing.append({'symbol': symbol, 'period': day, 'state': 'not_in_retained_aggregate'})
            continue
        if len(matching) != 1:
            raise ValueError('retained vendor observation is ambiguous')
        task = matching[0]['source_task_id']
        receipt = _inside(tej, f'receipts/{task}.json')
        receipt_ref = str(receipt.relative_to(workspace))
        if sha256_file(receipt) != members.get(receipt_ref):
            raise ValueError('retained vendor receipt changed')
        metadata = json.loads(receipt.read_text())
        if metadata['task_id'] != task or metadata['table_id'] != '117392c87504f94e71a04d01':
            raise ValueError('vendor receipt identity differs')
        pins[receipt_ref] = members[receipt_ref]
        for kind in ('raw', 'parquet'):
            path = _inside(tej, metadata[f'{kind}_path'])
            name = str(path.relative_to(workspace))
            digest = sha256_file(path)
            if digest != metadata[f'{kind}_sha256'] or digest != members.get(name):
                raise ValueError('retained vendor source changed')
            pins[name] = digest
        source_rows = pl.read_parquet(_inside(tej, metadata['parquet_path'])).to_dicts()
        candidate = {'physical_contract': row['pre_physical_contract'], 'successor_product': product,
                     'contract_month': month, 'adjusted_date': day,
                     'tej_shares': matching[0]['Shares per Contract'], 'task_id': task,
                     'receipt': receipt_ref}
        verify_raw_vendor_cells(candidate, json.loads(_inside(tej, metadata['raw_path']).read_text()), source_rows)
        results.append(reconcile(candidate, terms_rows, source_rows))
    if any(sha256_file(workspace / name) != digest for name, digest in pins.items()):
        raise ValueError('an input changed during vendor readback')
    result = {'contract': 'tw_futures_holiday_tej_source_comparison_v1',
              'observed_at_utc': datetime.now(timezone.utc).isoformat(),
              'candidate_count': len(candidates), 'compared_count': len(results),
              'missing_count': len(missing), 'missing': missing,
              'official_originals_verified': len(originals),
              'share_comparison_counts': dict(Counter(r['shares_per_contract']['status'] for r in results)),
              'cash_comparison_counts': dict(Counter(r['cash_credit_per_contract_twd']['status'] for r in results)),
              'results': results, 'input_pins': pins,
              'historical_point_in_time_verified': False, 'financial_or_training_adopted': False,
              'scope': 'Source-cell comparison only; no account transition or execution admission'}
    atomic_write_json(output, result)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('workspace', 'report', 'worklist', 'terms', 'native-manifest', 'output'):
        parser.add_argument('--' + name, type=Path, required=True)
    args = parser.parse_args()
    result = audit(args.workspace, args.report, args.worklist, args.terms, args.native_manifest, args.output)
    print(json.dumps({k: result[k] for k in ('candidate_count', 'compared_count', 'missing_count',
                                             'share_comparison_counts', 'cash_comparison_counts')}))


if __name__ == '__main__':
    main()
