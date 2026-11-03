#!/usr/bin/env python3
"""Read back a finite, source-pinned official/FinMind two-clock union."""
from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import json
from pathlib import Path
import sqlite3
import tempfile

import polars as pl

from downloader.artifact_io import atomic_write_json, sha256_file
from scripts.build_option_official_gap_overlay import check_strike_identity
from scripts.build_taifex_option_finmind_reference_window import candidate_policy, pin_source
from stockagent.data.tw_option_finmind_reference import candidate_rows
from stockagent.data.tw_option_full_carry_index import add_events, create_schema, lookup
from stockagent.data.tw_option_reference_event_index import compact_decision_events
from stockagent.data.tw_option_source_union import connect_union, verified_overlay


KEYS = ['product_raw', 'series_raw', 'strike_decimal', 'right_raw']


def audit(root: Path, expected_manifest: Path) -> dict:
    root, expected_manifest = root.resolve(), expected_manifest.resolve()
    manifest_sha = sha256_file(expected_manifest)
    doc = json.loads(expected_manifest.read_bytes())
    if (doc.get('contract') != 'option_official_gap_overlay_research_v1'
            or doc.get('training_ready') is not False):
        raise ValueError('a pinned research-only mixed source baseline is required')
    policy = Path(doc['policy_path'])
    if sha256_file(policy) != doc['policy_sha256']:
        raise ValueError('baseline policy changed')
    sessions, linked_pins = candidate_policy(policy)
    pins = {doc['plan_path']: doc['plan_sha256'],
            doc['official_manifest_path']: doc['official_manifest_sha256'],
            doc['gap_receipt_path']: doc['gap_receipt_sha256'],
            doc['data_path']: doc['data_sha256'], **linked_pins,
            **doc.get('implementation_sha256', {})}
    if any(sha256_file(Path(p)) != digest for p, digest in pins.items()):
        raise ValueError('baseline source or implementation pin changed')
    gap_receipt = json.loads(Path(doc['gap_receipt_path']).read_bytes())
    gap_day = date.fromisoformat(doc['official_fallback_day'])
    decision_day = date.fromisoformat(doc['decision_date'])
    if (gap_receipt.get('partition') != gap_day.isoformat()
            or gap_receipt.get('status') != 'observed_empty' or gap_receipt.get('rows') != 0):
        raise ValueError('original provider gap changed')
    official, provenance = verified_overlay(Path(doc['official_manifest_path']), gap_day,
                                            policy_sha256=doc['policy_sha256'])
    raw_sources, source_stats = [], []
    for wanted in doc['vendor_source_pins']:
        day = date.fromisoformat(wanted['day'])
        source = pin_source(root, day)
        if (source['sha256'] != wanted['sha256'] or source['receipt_sha256'] != wanted['receipt_sha256']):
            raise ValueError('vendor source differs from the pinned baseline')
        raw, stats = candidate_rows(pl.read_parquet(source['path']), day=day,
                                    source_sha256=source['sha256'], candidate_products=set(sessions))
        if raw.is_empty() or stats['excluded_rows']:
            raise ValueError('vendor day has empty or excluded source observations')
        raw_sources.append((day, raw))
        source_stats.append({'day': day.isoformat(), **stats})
    if [day.isoformat() for day, _ in raw_sources] != [doc['prior_source_day'], doc['decision_date']]:
        raise ValueError('baseline source days differ')
    strike_check = check_strike_identity(official, pl.concat([x[1] for x in raw_sources]))
    expected_all = pl.read_parquet(doc['data_path'])
    if expected_all.height != doc['rows']:
        raise ValueError('baseline row count differs')
    results = {}
    with tempfile.TemporaryDirectory(prefix='stockagent-option-union-', dir='/var/tmp') as scratch:
        database = Path(scratch) / 'parent.sqlite3'
        with sqlite3.connect(database) as parent:
            create_schema(parent)
            for number, (day, raw) in enumerate(raw_sources):
                add_events(parent, compact_decision_events(raw, day=day, session_windows=sessions),
                           source_window=number)
        before = sha256_file(database)
        with connect_union(database, [(gap_day, official)]) as conn:
            for clock in ('08:45', '09:00'):
                expected = (expected_all.filter(pl.col('decision_time') == clock)
                            .drop('decision_time', 'decision_date').sort(*KEYS))
                keys = [tuple(row) for row in expected.select(*KEYS).iter_rows()]
                actual = lookup(conn, keys, day=decision_day, clock=clock, session_windows=sessions)
                if not actual.equals(expected):
                    raise ValueError(f'{clock} union differs from native-checked baseline')
                results[clock] = {'compared_contract_keys': actual.height, 'mismatches': 0,
                                  'references_from_gap_day':
                                      actual.filter(pl.col('trade_ts').dt.date() == gap_day).height}
        if sha256_file(database) != before:
            raise ValueError('read-only parent index changed')
    if (sha256_file(expected_manifest) != manifest_sha
            or any(sha256_file(Path(p)) != digest for p, digest in pins.items())):
        raise ValueError('source or baseline changed during union readback')
    return {'contract': 'tw_option_source_union_finite_readback_v1',
            'observed_at_utc': datetime.now(timezone.utc).isoformat(),
            'baseline_manifest_path': str(expected_manifest), 'baseline_manifest_sha256': manifest_sha,
            'policy_sha256': doc['policy_sha256'], 'gap_day': gap_day.isoformat(),
            'decision_day': decision_day.isoformat(), 'official_overlay': provenance,
            'vendor_source_counts': source_stats, 'cross_source_strike_identity': strike_check,
            'clock_readback': results,
            'implementation_sha256': {str(path.resolve()): sha256_file(path) for path in
                (Path(__file__), Path('stockagent/data/tw_option_source_union.py'),
                 Path('stockagent/data/tw_option_full_carry_index.py'))},
            'training_ready': False,
            'limitations': ['Only three pinned calendar days are composed; earlier source state is omitted.',
                            'The frozen full-history carry index has not been attached by this finite readback.',
                            'Source completeness, historical delivery, eligibility, execution and accounting remain unproven.']}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path('data_finmind/complement'))
    parser.add_argument('--baseline-manifest', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.output.is_symlink() or args.output.exists():
        raise FileExistsError('preserve earlier readback evidence')
    result = audit(args.root, args.baseline_manifest)
    atomic_write_json(args.output, result)
    print(json.dumps({'output': str(args.output), 'clock_readback': result['clock_readback'],
                      'training_ready': False}, ensure_ascii=False), flush=True)


if __name__ == '__main__':
    main()
