#!/usr/bin/env python3
"""Bind verified futures clocks to a physical market axis.

This is a projection of verified observations, not another tick collector. It
requires exact equality of every market field used by the clock builder, keeps
the original opening-reference observations, and rejects every unknown fill.
It permits symbol-slot changes only after matching the physical contract/day.
An optional independently rebuilt delta can add complete, disjoint physical
lifetimes; a partial market-wide clock is never itself admitted.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import sys

import polars as pl

if os.environ.get('STOCKAGENT_FROZEN_SOURCE'):
    sys.path.insert(0, os.environ['STOCKAGENT_FROZEN_SOURCE'])

from downloader.artifact_io import atomic_write_bytes, atomic_write_json, atomic_write_parquet, sha256_file
from stockagent.data.tw_futures_decision_clock import (
    clock_dataset, decision_clock_contract, validate_clock_source,
    validate_opening_reference_source,
)
from stockagent.data.tw_futures_margin import validate_margin_rule_source
from stockagent.runtime_identity import verify_source_release

KEYS = ['date', 'physical_contract']
MARKET_COLUMNS = [
    'date', 'product', 'contract', 'asset_class', 'physical_contract',
    'open', 'high', 'low', 'close', 'volume', 'executable',
    'observed_open', 'observed_close', 'source_row_observed',
    'official_source_sha256', 'outright_volume', 'tenor_rank', 'official_expiry',
]


def validate_market_axis(parent: pl.DataFrame, candidate: pl.DataFrame) -> pl.DataFrame:
    """Prove candidate physical market observations are an exact parent subset."""
    for frame in (parent, candidate):
        missing = set(MARKET_COLUMNS + ['symbol']) - set(frame.columns)
        if missing or not frame.height or frame.select(KEYS).is_duplicated().any():
            raise ValueError(f'invalid physical market axis; missing={sorted(missing)}')
    parent_extra = 'shioaji_roots' in parent.columns
    if parent_extra != ('shioaji_roots' in candidate.columns):
        raise ValueError('market alias schema changed')
    columns = MARKET_COLUMNS + (['shioaji_roots'] if parent_extra else [])
    left = candidate.select(columns).sort(KEYS)
    right = parent.select(columns).join(left.select(KEYS), on=KEYS, how='semi').sort(KEYS)
    if not left.equals(right):
        raise ValueError('candidate market observations differ from parent physical source')
    return candidate.select(*KEYS, 'symbol').sort(KEYS)


def project_observations(parent: pl.DataFrame, parent_axis: pl.DataFrame,
                         candidate_axis: pl.DataFrame) -> pl.DataFrame:
    """Preserve every parent value while adopting only verified physical slots."""
    for frame in (parent, parent_axis, candidate_axis):
        if frame.select(KEYS).is_duplicated().any():
            raise ValueError('duplicate physical contract/day')
    selected = parent.join(candidate_axis.select(KEYS), on=KEYS, how='semi')
    expected_parent = parent_axis.join(candidate_axis.select(KEYS), on=KEYS, how='semi')
    if (selected.height != candidate_axis.height
            or not selected.select(*KEYS, 'symbol').sort(KEYS).equals(
                expected_parent.select(*KEYS, 'symbol').sort(KEYS))):
        raise ValueError('parent observations do not match their physical source axis')
    return (selected.drop('symbol').join(candidate_axis, on=KEYS, how='inner', validate='1:1')
            .select(parent.columns).sort(KEYS))


def split_disjoint_axis(parent: pl.DataFrame, candidate: pl.DataFrame):
    """Retain an exact parent and isolate entirely new physical lifetimes."""
    for frame in (parent, candidate):
        if not frame.height or frame.select(KEYS).is_duplicated().any():
            raise ValueError('invalid physical market axis')
    common = candidate.join(parent.select(KEYS), on=KEYS, how='semi')
    delta = candidate.join(parent.select(KEYS), on=KEYS, how='anti')
    removed = parent.join(candidate.select(KEYS), on=KEYS, how='anti')
    if removed.height or not common.height or not delta.height:
        raise ValueError('disjoint clock delta requires the entire parent and a nonempty addition')
    if delta.join(parent.select('physical_contract').unique(), on='physical_contract', how='semi').height:
        raise ValueError('delta cannot extend an existing physical lifetime')
    return validate_market_axis(parent, common), delta.select(*KEYS, 'symbol').sort(KEYS)


def merge_disjoint_observations(parent, parent_axis, common_axis, delta, delta_axis):
    """Combine source-bound observations only after exact identity/slot checks."""
    common = project_observations(parent, parent_axis, common_axis)
    if (delta.columns != common.columns or delta.schema != common.schema
            or delta.select(KEYS).is_duplicated().any()
            or not delta.select(*KEYS, 'symbol').sort(KEYS).equals(delta_axis.sort(KEYS))):
        raise ValueError('independent delta observations do not match their exact physical axis')
    result = pl.concat([common, delta], how='vertical').sort(KEYS)
    if result.select(KEYS).is_duplicated().any():
        raise ValueError('overlapping physical observations')
    return result


def validate_disjoint_delta(report: Path, *, old_daily: Path, old_rules: Path,
                            daily: Path, rules: Path, specs: Path,
                            start: date, end: date, axis: pl.DataFrame) -> dict:
    """Accept a raw-rebuilt delta receipt, never a partial aggregate clock."""
    proof = json.loads(report.read_text())
    if (proof.get('contract') != 'independent_disjoint_physical_clock_delta_v1'
            or proof.get('start') != str(start) or proof.get('end') != str(end)
            or proof.get('full_lifetimes_rebuilt') is not True
            or proof.get('partial_clock_promoted') is not False
            or proof.get('all_history_or_products_training_ready') is not False
            or proof.get('added_rows') != axis.height
            or set(proof.get('added_physical_contracts', [])) != set(axis['physical_contract'].to_list())):
        raise ValueError('independent delta receipt has an incompatible scope')
    inputs, outputs = proof.get('inputs'), proof.get('outputs')
    if not isinstance(inputs, dict) or not isinstance(outputs, dict) or len(outputs) != 8:
        raise ValueError('independent delta receipt is incomplete')
    source = proof.get('source_release', {})
    releases = [Path(path) for path, digest in inputs.items()
                if Path(path).name == 'release.json' and digest == source.get('receipt_sha256')]
    if (source.get('source_files_verified') is not True or len(releases) != 1
            or json.loads(releases[0].read_text()).get('code', {}).get('source_sha256')
            != source.get('source_sha256')):
        raise ValueError('independent delta does not identify a verified source release')
    for path in (old_daily, old_rules, daily, rules, specs, specs.with_name('manifest.json')):
        if inputs.get(str(path)) != sha256_file(path):
            raise ValueError('independent delta does not bind the exact financial/specification source')
    for path, digest in {**inputs, **outputs}.items():
        if sha256_file(Path(path)) != digest:
            raise ValueError('independent delta input or output changed after raw readback')
    # Source files read by the independent audit are still pinned.  The same
    # source object can appear more than once in the detailed provenance.
    def visit(value):
        if isinstance(value, dict):
            for path_key, hash_key in (('path', 'sha256'), ('receipt_path', 'receipt_sha256'),
                                       ('inventory_path', 'inventory_sha256')):
                if path_key in value and hash_key in value:
                    path, digest = value[path_key], value[hash_key]
                    if isinstance(path, str) and isinstance(digest, str) and len(digest) == 64:
                        if sha256_file(Path(path)) != digest:
                            raise ValueError('independent delta original source changed')
            for child in value.values():
                visit(child)
        elif isinstance(value, list):
            for child in value:
                visit(child)
    visit(proof.get('raw_reconstruction', {}))
    return proof


def rebind(*, parent_daily: Path, parent_rules: Path, parent_clock: Path,
           daily: Path, rules: Path, candidate_specs: Path, output: Path, start: date, end: date,
           source_receipt: Path, report: Path, delta_report: Path | None = None) -> dict:
    if not start <= end or output.exists() or report.exists():
        raise ValueError('invalid range or existing immutable output')
    frozen = os.environ.get('STOCKAGENT_FROZEN_SOURCE')
    if not frozen:
        raise ValueError('a verified frozen implementation is required')
    verify_source_release(source_receipt, Path(frozen))
    for rules_path, daily_path in ((parent_rules, parent_daily), (rules, daily)):
        validate_margin_rule_source(rules_path, daily_path)
    paths = [parent_daily, parent_rules, daily, rules, candidate_specs,
             candidate_specs.with_name('manifest.json'), source_receipt, Path(__file__).resolve()]
    for suffix in ('', 'entry_window_300m'):
        for clock in ('0845', '0900'):
            directory = parent_clock / suffix / clock
            paths.extend(directory / name for name in (
                'entries.parquet', 'manifest.json', 'opening_reference.parquet',
                'opening_reference_manifest.json', 'entry_specifications/specifications.parquet',
                'entry_specifications/manifest.json'))
            quarantine = directory / 'execution_quarantine.json'
            if quarantine.exists():
                paths.append(quarantine)
    pins = {str(p): sha256_file(p) for p in paths}
    before = lambda frame: frame.filter(pl.col('date').is_between(start, end))
    def market(path):
        scan = pl.scan_parquet(path)
        extras = ['shioaji_roots'] if 'shioaji_roots' in scan.collect_schema().names() else []
        return scan.filter(pl.col('date').is_between(start, end)).select(
            *MARKET_COLUMNS, 'symbol', *extras).collect()
    old, new = market(parent_daily), market(daily)
    if delta_report is None:
        common_axis, delta_axis = validate_market_axis(old, new), None
        delta_proof = None
    else:
        common_axis, delta_axis = split_disjoint_axis(old, new)
        delta_proof = validate_disjoint_delta(delta_report, old_daily=parent_daily, old_rules=parent_rules,
            daily=daily, rules=rules, specs=candidate_specs, start=start, end=end, axis=delta_axis)
        paths.append(delta_report)
        pins[str(delta_report)] = sha256_file(delta_report)
        paths.extend(Path(p) for p in delta_proof['outputs'])
        pins.update(delta_proof['outputs'])
    if new['date'].min() != start or new['date'].max() != end:
        raise ValueError('bounded financial horizon does not reach both requested dates')
    old_axis = old.select(*KEYS, 'symbol').sort(KEYS)
    candidate_sha, candidate_rules_sha = pins[str(daily)], pins[str(rules)]
    prepared = []
    for suffix, window in (('', 1), ('entry_window_300m', 300)):
        pair_sources = []
        projected = []
        for clock in ('0845', '0900'):
            directory = parent_clock / suffix / clock
            decision = clock[:2] + ':' + clock[2:]
            entries, manifest = validate_clock_source(directory/'entries.parquet', daily_path=parent_daily,
                                                       rules_path=parent_rules, decision_time=decision)
            refs, ref_manifest = validate_opening_reference_source(
                directory/'opening_reference.parquet', daily_path=parent_daily,
                decision_time=decision, entry_manifest=manifest)
            if (manifest['clock'].get('asset_scope') != 'all_twd'
                    or manifest['clock'].get('window_minutes', 1) != window):
                raise ValueError('parent clock scope or window differs')
            if (pins[str(candidate_specs)] != pins[str(directory/'entry_specifications/specifications.parquet')]
                    or pins[str(candidate_specs.with_name('manifest.json'))]
                    != pins[str(directory/'entry_specifications/manifest.json')]):
                raise ValueError('candidate product specifications differ from the parent clock')
            if delta_proof is None:
                entries = project_observations(before(entries), old_axis, common_axis)
                refs = project_observations(before(refs), old_axis, common_axis)
            else:
                def delta_output(name):
                    matches = [Path(p) for p in delta_proof['outputs']
                               if Path(p).parts[-3:] == (str(window), clock, name)]
                    if len(matches) != 1:
                        raise ValueError('delta receipt must contain each exact window/clock output')
                    return pl.read_parquet(matches[0])
                entries = merge_disjoint_observations(before(entries), old_axis, common_axis,
                    delta_output('entries.parquet'), delta_axis)
                refs = merge_disjoint_observations(before(refs), old_axis, common_axis,
                    delta_output('opening_reference.parquet'), delta_axis)
            if entries.filter(pl.col('status') == 'unverified_execution_quarantined').height:
                raise ValueError('requested period contains unverified execution; do not invent a trade')
            projected.append((clock, directory, entries, refs, ref_manifest))
            pair_sources.append(dict(clock=clock, manifest_sha256=pins[str(directory/'manifest.json')],
                                     entries_sha256=pins[str(directory/'entries.parquet')],
                                     reference_sha256=pins[str(directory/'opening_reference.parquet')]))
        identity = dict(contract=('verified_clock_disjoint_physical_union_v1' if delta_proof
                                  else 'verified_clock_physical_axis_slice_v1'), start=str(start), end=str(end),
                        window_minutes=window, parent_daily_sha256=pins[str(parent_daily)],
                        candidate_daily_sha256=candidate_sha, candidate_rules_sha256=candidate_rules_sha,
                        exact_market_axis_verified=True, original_reference_values_retained=True,
                        sources=pair_sources, script_sha256=pins[str(Path(__file__).resolve())])
        if delta_proof:
            identity.update(independent_delta_receipt_sha256=pins[str(delta_report)],
                            independent_delta_rows=delta_axis.height,
                            delta_original_raw_rebuilt=True, partial_market_clock_promoted=False)
        pair = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
        prepared.append((suffix, window, pair, identity, projected))
    if any(sha256_file(Path(path)) != digest for path, digest in pins.items()):
        raise ValueError('source changed while validating the clock slice')
    checks = []
    for suffix, window, pair, identity, projected in prepared:
        for clock, parent, entries, refs, ref_manifest in projected:
            directory = output / suffix / clock
            directory.mkdir(parents=True)
            atomic_write_parquet(directory/'entries.parquet', entries)
            atomic_write_parquet(directory/'opening_reference.parquet', refs)
            spec_dir = directory/'entry_specifications'
            spec_dir.mkdir()
            for name in ('specifications.parquet', 'manifest.json'):
                atomic_write_bytes(spec_dir/name, (parent/'entry_specifications'/name).read_bytes())
            decision = clock[:2]+':'+clock[2:]
            options = dict(asset_scope='all_twd', window_minutes=window, quarantine_unknown=False)
            manifest = dict(dataset=clock_dataset('all_twd', window_minutes=window, quarantine_unknown=False),
                status='complete', clock=decision_clock_contract(decision, **options), pair_identity=pair,
                entries_sha256=sha256_file(directory/'entries.parquet'), rows=entries.height,
                source_daily_sha256=candidate_sha, source_rules_sha256=candidate_rules_sha,
                start_date=str(start), end_date=str(end), sources=identity, coverage_gaps=0,
                source_coverage_complete=False, counts=dict(entries.group_by('status').len().iter_rows()),
                entry_specifications=dict(directory='entry_specifications',
                    data_sha256=sha256_file(spec_dir/'specifications.parquet'),
                    manifest_sha256=sha256_file(spec_dir/'manifest.json')))
            atomic_write_json(directory/'manifest.json', manifest)
            atomic_write_json(directory/'opening_reference_manifest.json', dict(
                dataset=ref_manifest['dataset'], status='complete', contract=ref_manifest['contract'],
                pair_identity=pair, source_daily_sha256=candidate_sha,
                data_sha256=sha256_file(directory/'opening_reference.parquet'), rows=refs.height,
                counts=dict(refs.group_by('reference_kind').len().iter_rows())))
            admitted, checked = validate_clock_source(directory/'entries.parquet', daily_path=daily,
                                                        rules_path=rules, decision_time=decision)
            validate_opening_reference_source(directory/'opening_reference.parquet', daily_path=daily,
                                               decision_time=decision, entry_manifest=checked)
            products = admitted.filter(pl.col('status') == 'observed_minute').join(
                new.select(*KEYS, 'product'), on=KEYS, how='left').get_column('product').n_unique()
            checks.append(dict(clock=decision, window_minutes=window, rows=admitted.height,
                               observed_products=products, counts=manifest['counts'], pair_identity=pair))
    if any(sha256_file(Path(path)) != digest for path, digest in pins.items()):
        raise ValueError('source changed while publishing the clock slice')
    proof = dict(contract=('verified_clock_disjoint_physical_union_readback_v1' if delta_proof
                           else 'verified_clock_physical_axis_slice_readback_v1'),
                 completed_at_utc=datetime.now(timezone.utc).isoformat(), start=str(start), end=str(end),
                 input_pins=pins, checks=checks, financial_point_in_time_verified=False,
                 all_history_or_products_training_ready=False, source_coverage_complete=False,
                 investment_performance_verified=False)
    atomic_write_json(report, proof)
    return proof


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('parent-daily', 'parent-rules', 'parent-clock', 'daily', 'rules', 'candidate-specs',
                 'output', 'source-receipt', 'report'):
        parser.add_argument('--'+name, type=Path, required=True)
    parser.add_argument('--start', type=date.fromisoformat, required=True)
    parser.add_argument('--end', type=date.fromisoformat, required=True)
    parser.add_argument('--delta-report', type=Path)
    args = vars(parser.parse_args())
    print(json.dumps(rebind(**args), default=str))


if __name__ == '__main__':
    main()
