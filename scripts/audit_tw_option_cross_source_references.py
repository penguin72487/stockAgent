#!/usr/bin/env python3
"""Independently read a bounded cross-source option reference publication."""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import json
from pathlib import Path
from zoneinfo import ZoneInfo

import pyarrow.parquet as pq

from downloader.artifact_io import atomic_write_json, sha256_file


CONTRACT = 'tw_option_cross_source_observed_reference_v1'
KINDS = {'observed_session_open_candidate', 'last_observed_trade_candidate',
         'ambiguous_first_second', 'ambiguous_last_second'}


def audit_rows(rows: list[dict], day: date, official: set[str], finmind: set[str]) -> dict:
    if not official or not finmind or official & finmind:
        raise ValueError('distinct source identities required')
    counts = {clock: Counter() for clock in ('08:45', '09:00')}
    seen = set()
    for row in rows:
        clock = row['decision_time']
        if clock not in counts or row['decision_date'] != day:
            raise ValueError('unexpected decision date or clock')
        key = (clock, row['product_raw'], row['series_raw'], row['strike_decimal'], row['right_raw'])
        if key in seen or any(not x for x in key) or row['right_raw'] not in {'C', 'P'}:
            raise ValueError('duplicate or invalid contract key')
        seen.add(key)
        hour, minute = map(int, clock.split(':'))
        cutoff = datetime(day.year, day.month, day.day, hour, minute, tzinfo=ZoneInfo('Asia/Taipei'))
        trade, available = row['trade_ts'], row['available_by']
        if (trade.tzinfo is None or available.tzinfo is None or available != trade + timedelta(seconds=1)
                or available > cutoff):
            raise ValueError('reference was not observable by its decision boundary')
        kind, price = row['reference_kind'], row['reference_price_decimal']
        if kind not in KINDS or (kind.startswith('ambiguous_') != (price is None)):
            raise ValueError('reference ambiguity and price disagree')
        if price is not None:
            try:
                value = Decimal(price)
            except (InvalidOperation, TypeError):
                raise ValueError('invalid native reference price') from None
            if not value.is_finite() or value <= 0:
                raise ValueError('invalid native reference price')
        hashes = row['source_hashes']
        if not hashes or hashes != sorted(set(hashes)) or set(hashes) - (official | finmind):
            raise ValueError('reference source provenance differs')
        origins = ('official' if set(hashes) & official else '') + ('finmind' if set(hashes) & finmind else '')
        counts[clock]['keys'] += 1
        counts[clock][f'{origins}_selected_provenance'] += 1
        counts[clock]['ambiguous'] += int(price is None)
    return {clock: dict(values) for clock, values in counts.items()}


def audit(manifest: Path, output: Path) -> dict:
    if manifest.is_symlink() or not manifest.is_file() or output.is_symlink() or output.exists():
        raise ValueError('a regular manifest and new independent report are required')
    digest = sha256_file(manifest)
    doc = json.loads(manifest.read_bytes())
    if doc.get('contract') != CONTRACT or doc.get('training_ready') is not False:
        raise ValueError('research-only publication identity required')
    sources = doc['official_source_pins'] + doc['finmind_source_pins']
    official = {s['sha256'] for s in doc['official_source_pins']}
    finmind = {s['sha256'] for s in doc['finmind_source_pins']}
    payload = Path(doc['data_path'])
    pins = [(payload, doc['data_sha256']), (Path(doc['policy_path']), doc['policy_sha256']),
            (Path(doc['official_index_path']), doc['official_index_sha256']),
            (Path(doc['finmind_index_path']), doc['finmind_index_sha256'])]
    pins += [(Path(s[field]), s[digest_field]) for s in sources
             for field, digest_field in (('path', 'sha256'), ('receipt_path', 'receipt_sha256'))]
    if any(path.is_symlink() or not path.is_file() or sha256_file(path) != expected for path, expected in pins):
        raise ValueError('published data, source or receipt hash differs')
    table = pq.read_table(payload)
    if table.num_rows != doc['rows']:
        raise ValueError('published row count differs')
    day = date.fromisoformat(doc['decision_date'])
    counts = audit_rows(table.to_pylist(), day, official, finmind)
    for clock, values in counts.items():
        expected = doc['clock_comparison'][clock]
        if values['keys'] != expected['union_keys'] or values['ambiguous'] != expected['union_ambiguous_keys']:
            raise ValueError('independent clock counts differ')
    if sha256_file(manifest) != digest or any(sha256_file(path) != expected for path, expected in pins):
        raise ValueError('a source or publication changed during independent readback')
    result = {'contract': 'tw_option_cross_source_reference_independent_readback_v1',
              'observed_at_utc': datetime.now(timezone.utc).isoformat(),
              'manifest_path': str(manifest.resolve()), 'manifest_sha256': digest,
              'data_path': str(payload), 'data_sha256': doc['data_sha256'],
              'rows': table.num_rows, 'clock_readback': counts,
              'training_ready': False,
              'limitations': ['Readback verifies the derived object and source identity, not source completeness,',
                              'historical delivery, listing, fills, margin, or training admission.']}
    atomic_write_json(output, result)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--manifest', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    result = audit(args.manifest, args.output)
    print(json.dumps({'output': str(args.output), 'clock_readback': result['clock_readback'],
                      'training_ready': False}, ensure_ascii=False), flush=True)


if __name__ == '__main__':
    main()
