#!/usr/bin/env python3
"""Queue source-backed missing option contracts through the native tick owner.

The selected contracts have an exchange reference on the preceding natural
day and are absent from that day's retained whole-market vendor object. A
per-product query is only an independent source check; it cannot certify a
reference price, historical availability, execution, or an option account.
"""
from __future__ import annotations

import argparse
from datetime import UTC, date, datetime, timedelta
import json
from pathlib import Path

import polars as pl

from downloader.artifact_io import atomic_write_json, sha256_file
from scripts.audit_taifex_finmind_option_references import KEYS, _prepare, compare
from scripts.audit_taifex_option_contract_presence import compare_contracts
from scripts.build_taifex_option_finmind_reference_window import pin_source
from scripts.request_finmind_option_tick_product_checks import CONTRACT, DATASET, register_evidenced


def missing_contracts(official: pl.DataFrame, vendor: pl.DataFrame, raw: pl.DataFrame,
                      source_day: date, decision_day: date) -> tuple[pl.DataFrame, dict]:
    """Find prior-day exchange contracts absent from the whole vendor object."""
    if decision_day != source_day + timedelta(days=1):
        raise ValueError('the reference comparison must cover consecutive natural days')
    official = _prepare(official, [decision_day], source_day)
    vendor = _prepare(vendor, [decision_day], source_day)
    absent = (official.join(vendor.select(KEYS), on=KEYS, how='anti')
              .filter(pl.col('trade_ts').dt.date() == source_day))
    candidates = absent.select(
        pl.col('product_raw').alias('product'), pl.col('series_raw').alias('series'),
        pl.col('strike_f32').alias('strike'), pl.col('right_raw').alias('right'),
    ).unique()
    missing = compare_contracts(candidates, raw.filter((pl.col('volume') > 0) & (pl.col('price') > 0)))
    missing = missing.sort('product', 'series', 'strike', 'right')
    counts = {'official_only_decision_keys_from_source_day': absent.height,
              'distinct_candidate_contracts': candidates.height,
              'absent_from_vendor_natural_day_contracts': missing.height,
              'by_product': missing.group_by('product').len().sort('product').to_dicts()}
    return missing, counts


def register(root: Path, comparison_path: Path, source_day: date, decision_day: date,
             evidence_path: Path, output: Path, now: datetime) -> dict:
    """Re-verify both source windows, then register exact native product tasks."""
    root, comparison_path = root.resolve(), comparison_path.resolve()
    workspace = root.parents[1]
    evidence_path, output = evidence_path.resolve(), output.resolve()
    if (not comparison_path.is_relative_to(workspace)
            or not evidence_path.is_relative_to(workspace / 'artifacts/markets')
            or not output.is_relative_to(workspace / 'artifacts/markets')
            or evidence_path.exists() or output.exists() or now.tzinfo is None):
        raise ValueError('new in-workspace source evidence and an aware clock are required')
    comparison_sha = sha256_file(comparison_path)
    comparison = json.loads(comparison_path.read_bytes())
    if (comparison.get('contract') != 'taifex_finmind_option_reference_source_comparison_v1'
            or comparison.get('training_ready') is not False
            or comparison.get('comparison_dates') != [source_day.isoformat(), decision_day.isoformat()]):
        raise ValueError('an exact, pinned two-day reference comparison is required')
    manifests = comparison['source_manifest_pins']
    if len(manifests) != 2:
        raise ValueError('exactly two source manifests are required')
    documents = {}
    pins = {str(comparison_path): comparison_sha, str(Path(__file__).resolve()): sha256_file(Path(__file__))}
    for name, digest in manifests.items():
        path = Path(name).resolve()
        if not path.is_relative_to(workspace) or sha256_file(path) != digest:
            raise ValueError('source comparison manifest changed')
        doc = json.loads(path.read_bytes())
        documents[doc['contract']] = doc
        pins[str(path)] = digest
    official = documents['taifex_option_observed_reference_source_window_v1']
    vendor = documents['finmind_option_observed_reference_source_window_v1']
    if (official.get('policy_sha256') != comparison.get('policy_sha256')
            or vendor.get('policy_sha256') != comparison.get('policy_sha256')
            or official.get('training_ready') is not False or vendor.get('training_ready') is not False):
        raise ValueError('source policies or research-only contracts differ')
    frames = []
    for doc in (official, vendor):
        path = Path(doc['data_path']).resolve()
        if (not path.is_relative_to(workspace) or sha256_file(path) != doc['data_sha256']
                or comparison['data_pins'].get(str(path)) != doc['data_sha256']):
            raise ValueError('source comparison data changed')
        pins[str(path)] = doc['data_sha256']
        frames.append(pl.read_parquet(path))
    actual = compare(*frames, [source_day, decision_day])
    for field in ('official_rows', 'vendor_rows', 'matched_keys', 'matched_value_differences',
                  'official_only_keys', 'vendor_only_keys'):
        if actual[field] != comparison['comparison'][field]:
            raise ValueError('source comparison readback differs')
    source = pin_source(root, source_day)
    original = [item for item in vendor['source_pins'] if item['day'] == source_day.isoformat()]
    if len(original) != 1 or source != original[0]:
        raise ValueError('original whole-market source changed; re-audit before requesting')
    raw = pl.read_parquet(source['path'])
    missing, counts = missing_contracts(*frames, raw, source_day, decision_day)
    if missing.is_empty() or len(counts['by_product']) > 64:
        raise ValueError('the source comparison has no bounded missing product-day scope')
    expires = now + timedelta(days=6)
    evidence = {'contract': 'finmind_option_reference_missing_contract_source_evidence_v1',
                'generated_at_utc': now.isoformat(), 'source_day': source_day.isoformat(),
                'decision_day': decision_day.isoformat(), 'comparison_path': str(comparison_path),
                'comparison_sha256': comparison_sha, 'source_pins': pins,
                'original_source': source, 'counts': counts,
                'missing_contracts': missing.to_dicts(), 'training_ready': False,
                'limitations': ['Float32 contract identity and prior-day source presence only.',
                                'References do not prove historical information availability or fills.',
                                'A product response requires its own session and value readback.']}
    atomic_write_json(evidence_path, evidence)
    pins[str(evidence_path)] = sha256_file(evidence_path)
    checks = []
    for product in missing['product'].unique().sort().to_list():
        contracts = missing.filter(pl.col('product') == product).rows()
        checks.append({'dataset': DATASET, 'data_id': product, 'partition': source_day.isoformat(),
                       'audited_missing_contracts': [list(row) for row in contracts],
                       'original_receipt': source['receipt_path'],
                       'original_receipt_sha256': source['receipt_sha256'],
                       'original_parquet': source['path'], 'original_parquet_sha256': source['sha256'],
                       'conflict_plan': str(comparison_path), 'conflict_plan_sha256': comparison_sha,
                       'conflict_plan_expires_at_utc': expires.isoformat()})
    return register_evidenced(root, checks, now, output, dataset=DATASET, contract=CONTRACT,
        reason='Check official prior-day option contracts absent from the retained whole-market tick object using the native per-product source.',
        additional_evidence_pins=pins)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path('data_finmind/complement'))
    parser.add_argument('--comparison', type=Path, required=True)
    parser.add_argument('--source-day', type=date.fromisoformat, required=True)
    parser.add_argument('--decision-day', type=date.fromisoformat, required=True)
    parser.add_argument('--evidence', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    result = register(args.root, args.comparison, args.source_day, args.decision_day,
                      args.evidence, args.output, datetime.now(UTC))
    print(json.dumps(result, ensure_ascii=False), flush=True)


if __name__ == '__main__':
    main()
