#!/usr/bin/env python3
"""Compare verified reference-price candidates with the original execution windows.

An observed opening reference, a research-only fallback candidate, and a
positive window print are separate facts. This audit writes no training data.
"""
from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
from pathlib import Path

import polars as pl

from downloader.artifact_io import atomic_write_json, sha256_file
from stockagent.data.tw_futures_decision_clock import opening_reference_contract_from_manifest


def classify(ref: pl.DataFrame, entries: pl.DataFrame, candidates: pl.DataFrame, clock: str) -> dict:
    key = ['date', 'physical_contract']
    if clock not in {'08:45', '09:00'}:
        raise ValueError('unsupported decision clock')
    if any(ref[c].null_count() for c in [*key, 'decision_ts', 'reference_kind']):
        raise ValueError('reference identity or state is null')
    if any(entries[c].null_count() for c in [*key, 'status']):
        raise ValueError('execution identity or state is null')
    if (ref.select(key).is_duplicated().any() or entries.select(key).is_duplicated().any()
            or candidates.select(key).is_duplicated().any()):
        raise ValueError('duplicate physical contract-day within a decision clock')
    if ref.filter(pl.col('decision_ts').dt.strftime('%H:%M') != clock).height:
        raise ValueError('reference clock differs')
    joined = ref.join(entries, on=key, how='left', validate='1:1')
    if joined.height != ref.height or joined['status'].null_count() or entries.height != ref.height:
        raise ValueError('execution window and reference axes differ')
    # A candidate must refer to an unavailable original reference at exactly
    # the same clock. Its existence does not make the candidate causal or admitted.
    selected = candidates.select(*key, 'decision_ts').join(
        ref.select(*key, 'decision_ts', 'reference_kind'),
        on=[*key, 'decision_ts'], how='left', validate='1:1')
    if (selected.height != candidates.height or selected['reference_kind'].null_count()
            or selected.filter(pl.col('reference_kind') != 'unavailable').height):
        raise ValueError('candidate is not aligned with an unavailable reference')
    annotated = (joined.join(candidates.select(*key).with_columns(pl.lit(True).alias('_candidate')),
                             on=key, how='left', validate='1:1')
        .with_columns(pl.when(pl.col('reference_kind') != 'unavailable').then(pl.lit('original'))
                      .when(pl.col('_candidate').fill_null(False)).then(pl.lit('candidate_only'))
                      .otherwise(pl.lit('still_unavailable')).alias('reference_state'),
                      (pl.col('entry_volume').is_finite().fill_null(False)
                       & (pl.col('entry_volume') > 0)).fill_null(False).alias('positive_window_print'),
                      pl.col('physical_contract').str.extract(r'^([^:]+):', 1).alias('product')))
    if annotated['product'].null_count():
        raise ValueError('physical contract has no product identity')
    rows = (annotated.group_by('reference_state', 'status')
            .agg(pl.len().alias('contract_days'),
                 pl.col('product').n_unique().alias('historical_products'),
                 pl.col('positive_window_print').sum().alias('positive_window_prints'))
            .sort('reference_state', 'status').to_dicts())
    totals = (annotated.group_by('reference_state')
              .agg(pl.len().alias('contract_days'),
                   pl.col('product').n_unique().alias('historical_products'),
                   pl.col('positive_window_print').sum().alias('positive_window_prints'))
              .sort('reference_state').to_dicts())
    return {'clock': clock, 'contract_days': ref.height, 'candidate_rows': selected.height,
            'by_reference_state': totals, 'by_reference_state_and_entry_status': rows}


def build(union_path: Path, early_dir: Path, late_dir: Path, report: Path) -> dict:
    if report.exists() or report.is_symlink():
        raise FileExistsError('preserve earlier evidence; select a new report')
    union_sha = sha256_file(union_path)
    union = json.loads(union_path.read_text())
    if (union.get('contract') != 'tw_futures_prenight_candidate_union_v1'
            or union.get('training_ready') is not False or not union.get('sources')):
        raise ValueError('expected the verified research candidate union')
    pins = [(union_path, union_sha)]
    candidates = []
    for item in union['sources']:
        paths = [Path(item['manifest']), Path(item['data']), Path(item['independent_readback'])]
        hashes = [item['manifest_sha256'], item['data_sha256'], item['readback_sha256']]
        if any(sha256_file(path) != sha for path, sha in zip(paths, hashes)):
            raise ValueError('candidate, manifest or independent readback changed')
        meta, check = (json.loads(paths[i].read_text()) for i in (0, 2))
        if (meta.get('training_ready') is not False or check.get('training_ready') is not False
                or check.get('accepted') is not True or check.get('failure_count') != 0
                or meta.get('data_sha256') != hashes[1] or check.get('candidate_sha256') != hashes[1]
                or check.get('candidate_manifest_sha256') != hashes[0]):
            raise ValueError('candidate is not bound to an accepted independent readback')
        frame = pl.read_parquet(paths[1], columns=['date', 'physical_contract', 'decision_ts'])
        if frame.height != meta['candidate_rows'] or frame.height != check['candidate_rows']:
            raise ValueError('candidate counts differ')
        candidates.append(frame)
        pins.extend(zip(paths, hashes))
    all_candidates = pl.concat(candidates)
    if all_candidates.height != union['candidate_rows']:
        raise ValueError('union candidate count differs')
    results = []
    pair_identity = None
    expected_by_clock = {item['clock']: item['rows'] for item in union['by_clock']}
    for clock, directory in [('08:45', early_dir), ('09:00', late_dir)]:
        ref_path, ref_doc_path = directory / 'opening_reference.parquet', directory / 'opening_reference_manifest.json'
        entries_path, entries_doc_path = directory / 'entries.parquet', directory / 'manifest.json'
        ref_doc, entry_doc = json.loads(ref_doc_path.read_text()), json.loads(entries_doc_path.read_text())
        opening_reference_contract_from_manifest(ref_doc, clock)
        ref_sha, entries_sha = sha256_file(ref_path), sha256_file(entries_path)
        if (ref_doc['data_sha256'] != ref_sha or entry_doc['entries_sha256'] != entries_sha
                or ref_doc['pair_identity'] != entry_doc['pair_identity']
                or ref_doc['source_daily_sha256'] != entry_doc['source_daily_sha256']):
            raise ValueError('opening reference and execution source identities differ')
        identity = (ref_doc['pair_identity'], ref_doc['source_daily_sha256'])
        if pair_identity is None:
            pair_identity = identity
        elif identity != pair_identity:
            raise ValueError('decision clocks do not share the same source pair')
        pins.extend([(ref_path, ref_sha), (entries_path, entries_sha),
                     (ref_doc_path, sha256_file(ref_doc_path)), (entries_doc_path, sha256_file(entries_doc_path))])
        ref = pl.read_parquet(ref_path, columns=['date', 'physical_contract', 'decision_ts', 'reference_kind'])
        entries = pl.read_parquet(entries_path, columns=['date', 'physical_contract', 'status', 'entry_volume'])
        selected = all_candidates.filter(pl.col('decision_ts').dt.strftime('%H:%M') == clock)
        if ref.height != ref_doc['rows'] or entries.height != entry_doc['rows']:
            raise ValueError('manifest axis counts differ')
        result = classify(ref, entries, selected, clock)
        if result['candidate_rows'] != expected_by_clock.get(clock):
            raise ValueError('per-clock candidate count differs from union')
        results.append(result)
    if any(sha256_file(path) != sha for path, sha in pins):
        raise ValueError('an input changed while computing coverage')
    output = {'contract': 'tw_futures_reference_and_window_coverage_v1',
              'created_at_utc': datetime.now(UTC).isoformat(), 'clocks': results,
              'inputs_sha256': {str(path): sha for path, sha in pins},
              'implementation_sha256': sha256_file(Path(__file__)), 'training_ready': False,
              'limitations': ['Candidates are not integrated into the strict reference clock.',
                  'Candidate selection uses retrospective source evidence and has no historical receive-time proof.',
                  'A positive market print is not an executable order, capacity or a broker fill.',
                  'Original strict data, candidates and execution windows remain independent facts.']}
    atomic_write_json(report, output)
    return output


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--union', type=Path, required=True)
    parser.add_argument('--early-dir', type=Path, required=True)
    parser.add_argument('--late-dir', type=Path, required=True)
    parser.add_argument('--report', type=Path, required=True)
    args = parser.parse_args()
    result = build(args.union, args.early_dir, args.late_dir, args.report)
    print(json.dumps(result['clocks'], ensure_ascii=False))


if __name__ == '__main__':
    main()
