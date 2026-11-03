#!/usr/bin/env python3
"""Derive opt-in research clocks from source-bound partial trade observations.

The result retains the complete parent clock axis, original quarantine, and
opening references.  Positive observations add only bounded research capacity;
they do not establish complete historical books, PIT, or an actual fill.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file
from stockagent.data.tw_futures_decision_clock import (
    clock_dataset, decision_clock_contract, validate_clock_source,
    validate_opening_reference_source,
)

KEYS = ['date', 'physical_contract']
VALUES = ['entry_quote', 'entry_volume', 'source_file_sha256']


def merge_partial_observations(entries: pl.DataFrame, candidates: pl.DataFrame,
                               *, decision_time: str, window_minutes: int) -> pl.DataFrame:
    """Change only candidate rows already in the exact unknown-day quarantine."""
    expected = {*KEYS, *VALUES, 'status'}
    needed = {*KEYS, *VALUES, 'window_minutes', 'decision_time'}
    if expected - set(entries.columns) or needed - set(candidates.columns):
        raise ValueError('partial merge input schema is incomplete')
    if entries.select(KEYS).is_duplicated().any() or candidates.select(
            *KEYS, 'window_minutes', 'decision_time').is_duplicated().any():
        raise ValueError('partial merge input identity is duplicated')
    selected = candidates.filter((pl.col('decision_time') == decision_time)
                                 & (pl.col('window_minutes') == window_minutes)).select(*KEYS, *VALUES)
    if selected.filter(pl.col('date').is_null() | pl.col('physical_contract').is_null()
            | pl.col('entry_quote').is_null() | ~pl.col('entry_quote').is_finite()
            | (pl.col('entry_quote') <= 0) | pl.col('entry_volume').is_null()
            | ~pl.col('entry_volume').is_finite() | (pl.col('entry_volume') <= 0)
            | (pl.col('entry_volume') != pl.col('entry_volume').floor())
            | pl.col('source_file_sha256').is_null()
            | (pl.col('source_file_sha256').str.len_chars() != 64)).height:
        raise ValueError('partial candidate must contain a positive real print and source digest')
    eligible = entries.filter(pl.col('status') == 'unverified_execution_quarantined').select(KEYS)
    if selected.join(eligible, on=KEYS, how='anti').height:
        raise ValueError('partial candidate is not in the original quarantine')
    source = selected.rename({name: '_partial_' + name for name in VALUES})
    joined = entries.join(source, on=KEYS, how='left', validate='1:1', maintain_order='left')
    present = pl.col('_partial_entry_quote').is_not_null()
    result = joined.with_columns(
        *(pl.when(present).then(pl.col('_partial_' + name)).otherwise(pl.col(name)).alias(name)
          for name in VALUES),
        pl.when(present).then(pl.lit('partial_observed_minute')).otherwise(pl.col('status')).alias('status'),
    ).drop(*('_partial_' + name for name in VALUES)).select(entries.columns)
    if result.height != entries.height or result.filter(pl.col('status') == 'partial_observed_minute').height != selected.height:
        raise ValueError('partial merge changed the axis or candidate count')
    return result


def derive(*, clocks: Path, diagnostic_dir: Path, readback: Path, daily: Path,
           rules: Path, output: Path, report: Path) -> dict:
    """Publish four source-pinned clocks atomically after their own readback."""
    if output.exists() or report.exists():
        raise ValueError('derived clock output and report must not already exist')
    diagnostic_file = diagnostic_dir / 'manifest.json'
    data_file = diagnostic_dir / 'observed_window_candidates.parquet'
    diagnostic = json.loads(diagnostic_file.read_text())
    verification = json.loads(readback.read_text())
    diagnostic_sha, data_sha, readback_sha = (sha256_file(p) for p in (diagnostic_file, data_file, readback))
    daily_sha, rules_sha = sha256_file(daily), sha256_file(rules)
    if (diagnostic.get('contract') != 'tw_futures_partial_window_observations_diagnostic_v1'
            or diagnostic.get('research_only') is not True or diagnostic.get('training_ready') is not False
            or diagnostic.get('source_coverage_complete') is not False
            or diagnostic.get('candidate_file', {}).get('sha256') != data_sha
            or verification.get('contract') != 'tw_futures_partial_window_independent_arithmetic_v1'
            or verification.get('independent_implementation') is not True
            or verification.get('candidate_sha256') != data_sha
            or verification.get('diagnostic_manifest_sha256') != diagnostic_sha
            or verification.get('rows_checked') != diagnostic.get('candidate_rows')):
        raise ValueError('partial diagnostic and independent arithmetic do not match')
    candidates = pl.read_parquet(data_file)
    if candidates.height != diagnostic['candidate_rows']:
        raise ValueError('partial diagnostic candidate count differs')
    pinned = {p: sha256_file(p) for p in (daily, rules, diagnostic_file, data_file, readback, Path(__file__))}
    output.parent.mkdir(parents=True, exist_ok=True)
    report.parent.mkdir(parents=True, exist_ok=True)
    checks = []
    with tempfile.TemporaryDirectory(prefix='.partial-clock-', dir=output.parent) as temp:
        stage = Path(temp) / 'result'
        for suffix, window in (('', 1), ('entry_window_300m', 300)):
            parent_dirs = [clocks / suffix / c for c in ('0845', '0900')]
            parent_manifests = [json.loads((p / 'manifest.json').read_text()) for p in parent_dirs]
            if parent_manifests[0]['pair_identity'] != parent_manifests[1]['pair_identity']:
                raise ValueError('parent decision clocks do not share a pair identity')
            identity = dict(contract='tw_futures_partial_observed_pair_v1', window_minutes=window,
                parent_pair_identity=parent_manifests[0]['pair_identity'],
                partial_manifest_sha256=diagnostic_sha, partial_data_sha256=data_sha,
                partial_independent_readback_sha256=readback_sha,
                derivation_script_sha256=pinned[Path(__file__)])
            pair = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
            for parent, parent_manifest, clock in zip(parent_dirs, parent_manifests, ('0845', '0900')):
                decision = clock[:2] + ':' + clock[2:]
                members = ['entries.parquet', 'manifest.json', 'opening_reference.parquet',
                           'opening_reference_manifest.json', 'execution_quarantine.json',
                           'entry_specifications/specifications.parquet', 'entry_specifications/manifest.json']
                for member in members:
                    path = parent / member
                    pinned[path] = sha256_file(path)
                entries, checked = validate_clock_source(parent / 'entries.parquet', daily_path=daily,
                                                          rules_path=rules, decision_time=decision)
                _, ref_manifest = validate_opening_reference_source(parent / 'opening_reference.parquet',
                    daily_path=daily, decision_time=decision, entry_manifest=checked)
                if (checked != parent_manifest or checked['clock'].get('asset_scope') != 'all_twd'
                        or checked['clock'].get('window_minutes', 1) != window
                        or checked['clock'].get('quarantine_unknown_execution') is not True
                        or checked['clock'].get('allow_partial_observations', False)):
                    raise ValueError('parent clock is not the expected strict quarantined all-TWD clock')
                source = diagnostic.get('sources', {})
                if (source.get('daily_sha256') != daily_sha or source.get('rules_sha256') != rules_sha
                        or source.get('quarantine_sha256') != checked['execution_quarantine']['sha256']):
                    raise ValueError('diagnostic is not bound to this parent financial source and quarantine')
                merged = merge_partial_observations(entries, candidates, decision_time=decision, window_minutes=window)
                folder = stage / suffix / clock
                folder.mkdir(parents=True)
                atomic_write_parquet(folder / 'entries.parquet', merged)
                for member in ('opening_reference.parquet', 'execution_quarantine.json',
                               'entry_specifications/specifications.parquet', 'entry_specifications/manifest.json'):
                    target = folder / member
                    target.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copyfile(parent / member, target)
                for source_file, name in ((data_file, 'partial_observations.parquet'),
                                          (diagnostic_file, 'partial_observations_manifest.json'),
                                          (readback, 'partial_observations_readback.json')):
                    shutil.copyfile(source_file, folder / name)
                options = dict(window_minutes=window, quarantine_unknown=True, allow_partial_observations=True)
                manifest = dict(checked, dataset=clock_dataset('all_twd', **options),
                    clock=decision_clock_contract(decision, asset_scope='all_twd', **options), pair_identity=pair,
                    entries_sha256=sha256_file(folder / 'entries.parquet'),
                    counts=dict(merged.group_by('status').len().iter_rows()),
                    research_only=True, historical_point_in_time=False, training_ready=False,
                    partial_derivation=identity,
                    partial_observations=dict(data_file='partial_observations.parquet', data_sha256=data_sha,
                        manifest_file='partial_observations_manifest.json', manifest_sha256=diagnostic_sha,
                        readback_file='partial_observations_readback.json', readback_sha256=readback_sha,
                        contract_days=merged.filter(pl.col('status') == 'partial_observed_minute').height))
                atomic_write_json(folder / 'manifest.json', manifest)
                atomic_write_json(folder / 'opening_reference_manifest.json', dict(ref_manifest, pair_identity=pair))
                verified, verify_manifest = validate_clock_source(folder / 'entries.parquet', daily_path=daily,
                                                                    rules_path=rules, decision_time=decision)
                validate_opening_reference_source(folder / 'opening_reference.parquet', daily_path=daily,
                                                   decision_time=decision, entry_manifest=verify_manifest)
                if not verified.equals(merged) or sha256_file(folder / 'opening_reference.parquet') != pinned[parent / 'opening_reference.parquet']:
                    raise ValueError('derived clock or unchanged opening reference differs after readback')
                checks.append(dict(clock=decision, window_minutes=window, rows=verified.height,
                    partial_contract_days=manifest['partial_observations']['contract_days'],
                    unknown_contract_days=manifest['coverage_gaps'], pair_identity=pair,
                    entries_sha256=manifest['entries_sha256'], counts=manifest['counts']))
        if any(sha256_file(path) != digest for path, digest in pinned.items()):
            raise ValueError('an input changed during derivation')
        if output.exists():
            raise ValueError('derived clock output appeared during validation')
        os.rename(stage, output)
    proof = dict(contract='tw_futures_partial_observed_clock_derivation_v1', research_only=True,
        training_ready=False, historical_point_in_time=False, source_coverage_complete=False,
        market_source_independent=False, output=str(output), diagnostic_sha256=diagnostic_sha,
        candidates_sha256=data_sha, independent_arithmetic_sha256=readback_sha, clocks=checks,
        policy='only observed capacity in quarantined contract windows; opening references unchanged')
    atomic_write_json(report, proof)
    return proof


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('clocks', 'diagnostic-dir', 'readback', 'daily', 'rules', 'output', 'report'):
        parser.add_argument('--' + name, type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(derive(**vars(args)), ensure_ascii=False), flush=True)


if __name__ == '__main__':
    main()
