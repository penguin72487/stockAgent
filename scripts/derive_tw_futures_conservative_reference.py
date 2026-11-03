#!/usr/bin/env python3
"""Make a provenance-bound reference-only derivative of existing futures clocks.

This deliberately withholds every legacy reference: legacy records have no
per-event order or cross-session continuity certificate. It changes neither
execution prices nor financial inputs, and does not claim full source coverage.
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

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import polars as pl
from downloader.artifact_io import atomic_write_json, sha256_file
from stockagent.data.tw_futures_decision_clock import (
    OPENING_REFERENCE_FEATURES, opening_reference_contract,
    validate_clock_source, validate_opening_reference_source,
)
from stockagent.data.tw_futures_reference_gaps import (withhold_uncertified_fallbacks,
    verify_partial_current_openings)
from stockagent.runtime_identity import verify_source_release


def derive(source: Path, output: Path, daily: Path, rules: Path, *, source_sha256: str,
           candidate: Path | None = None, candidate_readback: Path | None = None,
           candidate_release: Path | None = None) -> dict:
    source, output, daily, rules = map(lambda x: x.resolve(), (source, output, daily, rules))
    if output.exists() or output == source or source in output.parents or output in source.parents:
        raise ValueError('output must be a new independent clock root')
    if len(source_sha256) != 64 or any(c not in '0123456789abcdef' for c in source_sha256):
        raise ValueError('a verified source release SHA-256 is required')
    if any(x is not None for x in (candidate, candidate_readback, candidate_release)) and not all(
            x is not None for x in (candidate, candidate_readback, candidate_release)):
        raise ValueError('partial opening admission requires the candidate, independent readback and source release')
    candidate_inputs = None
    if candidate is not None:
        from scripts.prepare_tw_futures_decision_clocks import _bulk_checkpoint_parent, _bulk_checkpoint_read
        candidate, candidate_readback, candidate_release = map(Path, (candidate, candidate_readback, candidate_release))
        readback = json.loads(candidate_readback.read_bytes())
        source_proof = verify_source_release(candidate_release, candidate_release.parent/'build-source')
        if (readback.get('contract') != 'partial_raw_opening_independent_readback_v1'
                or readback.get('state') != 'verified' or readback.get('source_sha256') != source_proof['source_sha256']
                or Path(readback.get('candidate', '')).resolve() != candidate.resolve()
                or readback.get('candidate_is_not_a_complete_training_release') is not True):
            raise ValueError('partial opening independent readback is incompatible')
        checkpoint, = (candidate/'bulk_readback_checkpoints').iterdir()
        if sha256_file(checkpoint/'manifest.json') != readback['checkpoint_manifest_sha256']:
            raise ValueError('partial opening checkpoint changed')
        checkpoint_manifest = json.loads((checkpoint/'manifest.json').read_bytes())
        _bulk_checkpoint_parent(checkpoint, candidate_release, checkpoint_manifest['identity'])
        restored = _bulk_checkpoint_read(checkpoint, checkpoint_manifest['identity'],
                                         checkpoint_manifest['identity']['objects'], 8)
        if (sha256_file(candidate/'source_coverage.parquet') != readback['source_coverage_sha256']
                or sha256_file(candidate/'coverage_gaps.parquet') != readback['gaps_sha256']):
            raise ValueError('partial opening source coverage changed')
        gaps = pl.read_parquet(candidate/'coverage_gaps.parquet')
        if gaps.height != readback['gap_count'] or not restored['coverage'].equals(
                pl.read_parquet(candidate/'source_coverage.parquet')):
            raise ValueError('partial opening coverage readback differs')
        candidate_inputs = dict(readback=readback, readback_sha256=sha256_file(candidate_readback),
                                candidate=candidate, coverage=restored['coverage'], trades=restored['trades'],
                                gaps=gaps, source_sha256=source_proof['source_sha256'])
    output.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix=f'.{output.name}.', dir=output.parent))
    checks = []
    try:
        for window in ('', 'entry_window_300m'):
            loaded = []
            for clock in ('0845', '0900'):
                old = source / 'all_twd' / 'clocks' / window / clock
                when = f'{clock[:2]}:{clock[2:]}'
                entries, entry_manifest = validate_clock_source(old / 'entries.parquet',
                    daily_path=daily, rules_path=rules, decision_time=when)
                refs, ref_manifest = validate_opening_reference_source(old / 'opening_reference.parquet',
                    daily_path=daily, decision_time=when, entry_manifest=entry_manifest)
                loaded.append((clock, old, when, entries, entry_manifest, refs, ref_manifest))
            if loaded[0][4]['pair_identity'] != loaded[1][4]['pair_identity']:
                raise ValueError('parent clocks are not the same source generation')
            pair_input = dict(contract='conservative_unproven_reference_derivative_v1', window=window,
                parent_pair=loaded[0][4]['pair_identity'], source_sha256=source_sha256,
                builder_sha256=sha256_file(Path(__file__)),
                inputs=[dict(clock=x[0], entries=sha256_file(x[1]/'entries.parquet'),
                    reference=sha256_file(x[1]/'opening_reference.parquet'),
                    entry_manifest=sha256_file(x[1]/'manifest.json'),
                    reference_manifest=sha256_file(x[1]/'opening_reference_manifest.json')) for x in loaded])
            if candidate_inputs is not None:
                pair_input['partial_current_opening_readback_sha256'] = candidate_inputs['readback_sha256']
                pair_input['partial_source_sha256'] = candidate_inputs['source_sha256']
            pair = hashlib.sha256(json.dumps(pair_input, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
            for clock, old, when, entries, entry_manifest, refs, ref_manifest in loaded:
                dest = stage / 'all_twd' / 'clocks' / window / clock
                dest.mkdir(parents=True)
                shutil.copy2(old/'entries.parquet', dest/'entries.parquet')
                shutil.copytree(old/'entry_specifications', dest/'entry_specifications')
                if entry_manifest['clock'].get('quarantine_unknown_execution', False):
                    # The parent was validated above. Preserve its exact
                    # source-bound order quarantine with the execution tape;
                    # without the receipt the derivative cannot prove that
                    # an unknown contract day has zero order capacity.
                    shutil.copy2(old/'execution_quarantine.json', dest/'execution_quarantine.json')
                if candidate_inputs is None:
                    # Absence of per-event proof deliberately masks even legacy opens.
                    conservative = withhold_uncertified_fallbacks(refs, pl.DataFrame())
                else:
                    candidate_dir = candidate_inputs['candidate']/clock
                    candidate_path = candidate_dir/'opening_reference.parquet'
                    candidate_manifest = json.loads((candidate_dir/'opening_reference_manifest.json').read_bytes())
                    observed = next(x for x in candidate_inputs['readback']['observations'] if x['clock'] == when)
                    if (candidate_manifest.get('status') != 'partial'
                            or candidate_manifest.get('contract') != opening_reference_contract(when, version=3)
                            or candidate_manifest.get('data_sha256') != sha256_file(candidate_path)
                            or candidate_manifest['data_sha256'] != observed['reference_sha256']):
                        raise ValueError('partial candidate reference changed')
                    conservative = pl.read_parquet(candidate_path)
                    verify_partial_current_openings(conservative, candidate_inputs['trades'],
                        candidate_inputs['coverage'], candidate_inputs['gaps'])
                    if conservative.filter(pl.col('reference_quote').is_not_null()).height != observed['proven_same_session_opens']:
                        raise ValueError('partial candidate opening count changed')
                if (conservative.height != refs.height or
                        not conservative.select('date', 'physical_contract', 'symbol', 'decision_ts').equals(
                            refs.select('date', 'physical_contract', 'symbol', 'decision_ts')) or
                        conservative.filter(pl.col('prior_trade_quote').is_not_null()).height or
                        (candidate_inputs is None and (conservative.filter(
                            pl.col('reference_kind') != 'unavailable').height or
                            conservative.select(OPENING_REFERENCE_FEATURES).sum().row(0) != (0., 0., 0., 0.)))):
                    raise ValueError('reference-only derivative failed its conservative invariant')
                conservative.write_parquet(dest/'opening_reference.parquet')
                entry_manifest = dict(entry_manifest, pair_identity=pair,
                    sources=dict(contract='conservative_unproven_reference_derivative_v1',
                        parent=pair_input, all_history_or_products_training_ready=False,
                        execution_values_unchanged=True, original_reference_values_retained=False))
                ref_manifest = dict(ref_manifest, contract=opening_reference_contract(when, version=3),
                    pair_identity=pair, data_sha256=sha256_file(dest/'opening_reference.parquet'),
                    counts=dict(conservative.group_by('reference_kind').len().iter_rows()),
                    derivation=dict(policy=('independently_verified_current_opens_other_references_unavailable'
                        if candidate_inputs is not None else 'all_legacy_references_unavailable_without_event_or_continuity_proof'),
                        parent_reference_sha256=sha256_file(old/'opening_reference.parquet'),
                        preexisting_source_coverage_complete=False))
                atomic_write_json(dest/'manifest.json', entry_manifest)
                atomic_write_json(dest/'opening_reference_manifest.json', ref_manifest)
                new_entries, new_manifest = validate_clock_source(dest/'entries.parquet',
                    daily_path=daily, rules_path=rules, decision_time=when)
                readback, _ = validate_opening_reference_source(dest/'opening_reference.parquet',
                    daily_path=daily, decision_time=when, entry_manifest=new_manifest)
                if not new_entries.equals(entries) or not readback.equals(conservative):
                    raise ValueError('independent parquet readback differs from inputs')
                checks.append(dict(window=window or '1m', clock=clock, rows=refs.height,
                    unavailable=conservative.filter(pl.col('reference_kind') == 'unavailable').height,
                    proven_current_opens=conservative.filter(pl.col('reference_kind') == 'observed_session_open').height,
                    entries_sha256=sha256_file(dest/'entries.parquet'),
                    reference_sha256=sha256_file(dest/'opening_reference.parquet'), pair_identity=pair,
                    parent_reference_kinds=refs.group_by('reference_kind').len().to_dicts()))
        receipt = dict(contract='conservative_unproven_reference_readback_v1', state='verified',
            source=str(source), output=str(output), daily_sha256=sha256_file(daily), rules_sha256=sha256_file(rules),
            source_sha256=source_sha256, builder_sha256=sha256_file(Path(__file__)), checks=checks,
            execution_unchanged=True, financial_inputs_unchanged=True,
            all_references_withheld=candidate_inputs is None,
            partial_opening_readback_sha256=(candidate_inputs['readback_sha256'] if candidate_inputs else None),
            source_coverage_complete=False, all_history_or_products_training_ready=False)
        atomic_write_json(stage/'derivation_readback.json', receipt)
        if output.exists():
            raise FileExistsError(output)
        os.rename(stage, output)
        return receipt
    except BaseException:
        shutil.rmtree(stage)
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('source', 'output', 'daily', 'rules'):
        parser.add_argument('--'+name, type=Path, required=True)
    parser.add_argument('--source-sha256', required=True)
    parser.add_argument('--candidate', type=Path)
    parser.add_argument('--candidate-readback', type=Path)
    parser.add_argument('--candidate-release', type=Path)
    args = parser.parse_args()
    print(json.dumps(derive(args.source, args.output, args.daily, args.rules,
                            source_sha256=args.source_sha256, candidate=args.candidate,
                            candidate_readback=args.candidate_readback,
                            candidate_release=args.candidate_release), ensure_ascii=False))


if __name__ == '__main__':
    main()
