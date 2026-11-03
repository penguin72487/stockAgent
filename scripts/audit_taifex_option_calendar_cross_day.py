#!/usr/bin/env python3
"""Check cross-day option references against retained TAIFEX native files.

This verifies one finite source overlay. Prior history, all contract lifecycle,
historic availability and executable option accounting remain separate gates.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import json
from pathlib import Path

import polars as pl

from downloader.artifact_io import atomic_write_json, sha256_file
from scripts.build_taifex_option_calendar_event_index import CONTRACT, compare_native
from scripts.build_taifex_option_finmind_reference_window import candidate_policy
from stockagent.data.tw_option_reference_candidates import reference_candidates


def audit(manifests: list[Path], day: date) -> dict:
    if len(manifests) != 2 or len(set(manifests)) != 2:
        raise ValueError('exactly two distinct ordered official calendar indexes are required')
    docs, pins, frames, sources = [], [], [], {}
    for path in manifests:
        path = path.resolve()
        digest = sha256_file(path)
        doc = json.loads(path.read_bytes())
        payload = Path(doc['data_path'])
        if (doc.get('contract') != CONTRACT or doc.get('training_ready') is not False
                or payload.resolve().parent != path.parent or payload.is_symlink()
                or sha256_file(payload) != doc['data_sha256']):
            raise ValueError('official index identity differs from its manifest')
        for code, code_sha in doc['implementation_sha256'].items():
            if sha256_file(Path(code)) != code_sha:
                raise ValueError('official index implementation changed')
        for linked, linked_sha in doc['policy_linked_pins'].items():
            if sha256_file(Path(linked)) != linked_sha:
                raise ValueError('official index policy evidence changed')
        frame = pl.read_parquet(payload)
        if frame.height != doc['rows']:
            raise ValueError('official index row readback differs')
        for source in doc['source_pins']:
            if (sha256_file(Path(source['path'])) != source['sha256']
                    or sha256_file(Path(source['receipt_path'])) != source['receipt_sha256']):
                raise ValueError('official source or receipt changed')
            key = source['date']
            if key in sources and sources[key] != source:
                raise ValueError('same official trading date has inconsistent source identity')
            sources[key] = source
        pins.append({'path': str(path), 'sha256': digest, 'data_sha256': doc['data_sha256']})
        docs.append(doc)
        frames.append(frame)
    if (date.fromisoformat(docs[0]['event_calendar_date']) >= day
            or docs[1]['event_calendar_date'] != day.isoformat()
            or docs[0]['policy_sha256'] != docs[1]['policy_sha256']
            or docs[0]['source_manifest_sha256'] != docs[1]['source_manifest_sha256']):
        raise ValueError('ordered official dates and a single source policy are required')
    source_receipt = Path(docs[0]['source_manifest_path'])
    official = json.loads(source_receipt.read_bytes())
    dates = official['listing_page_dates']['options']
    try:
        first, second = (dates.index(doc['event_calendar_date']) for doc in docs)
    except ValueError:
        raise ValueError('index dates absent from official listed trading days') from None
    if second != first + 1 or second + 1 >= len(dates) or set(sources) != set(dates[first:second + 2]):
        raise ValueError('complete consecutive official trading-file prefix is required')
    policy = Path(docs[0]['policy_path'])
    if sha256_file(policy) != docs[0]['policy_sha256']:
        raise ValueError('official candidate policy changed')
    sessions, _ = candidate_policy(policy)
    index = pl.concat(frames).unique()
    ordered_sources = [sources[key] for key in sorted(sources)]
    comparison, native_counts = compare_native(index, ordered_sources, day, sessions)
    carried = {}
    previous_day = date.fromisoformat(docs[0]['event_calendar_date'])
    for clock in ('08:45', '09:00'):
        refs = reference_candidates(index, trading_date=day, decision_time=clock, session_windows=sessions)
        selected = refs.filter(pl.col('trade_ts').dt.date() == previous_day)
        carried[clock] = {'selected_prior_calendar_day_keys': selected.height,
                          'by_product': selected.group_by('product_raw').len().sort('product_raw').to_dicts()}
    if (any(sha256_file(Path(p['path'])) != p['sha256'] for p in pins)
            or any(sha256_file(Path(doc['data_path'])) != doc['data_sha256'] for doc in docs)
            or sha256_file(source_receipt) != docs[0]['source_manifest_sha256']
            or sha256_file(policy) != docs[0]['policy_sha256']):
        raise ValueError('official source or indexes changed during cross-day audit')
    return {'contract': 'taifex_option_cross_calendar_day_oracle_v1',
            'generated_at_utc': datetime.now(timezone.utc).isoformat(),
            'decision_date': day.isoformat(), 'previous_event_calendar_date': previous_day.isoformat(),
            'index_manifests': pins, 'source_pins': ordered_sources,
            'source_policy_sha256': docs[0]['policy_sha256'],
            'implementation_sha256': sha256_file(Path(__file__)),
            'native_counts': native_counts, 'clock_readback': comparison, 'carried': carried,
            'training_ready': False,
            'limitations': ['Sequential oracle reads the same official files, not a second venue.',
                            'Prior event days may be partial and the full older source history is unknown.',
                            'Contract expiry, generation, historical arrival, execution and accounting remain unverified.',
                            'This does not update the frozen FinMind plan or clear its missing-day marker.']}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--manifest', type=Path, action='append', required=True)
    parser.add_argument('--date', type=date.fromisoformat, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists() or args.output.is_symlink():
        raise FileExistsError('preserve earlier cross-day readback')
    result = audit(args.manifest, args.date)
    atomic_write_json(args.output, result)
    print(json.dumps({'output': str(args.output), 'clock_readback': result['clock_readback'],
                      'carried': result['carried']}, ensure_ascii=False))


if __name__ == '__main__':
    main()
