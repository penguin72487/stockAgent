#!/usr/bin/env python3
"""Bounded public-source evidence for audited candles using canonical clients.

This verifier does not write source data, use another quota bucket, or infer
observations from another venue. It records what the exact native endpoint
returned, so a collector success cannot masquerade as a historical gap fill.
"""
from __future__ import annotations

import argparse
import csv
from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import polars as pl
from downloader import download_binance_perp_15m as binance
from downloader import download_bybit_perp_daily as bybit
from downloader import download_okx_perp_daily as okx
from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.ohlcv_hot_tail import invalid_candle_values
from scripts.audit_source_anomalies import csv_file


def classify_response(owner: str, payload, lower: int, upper: int) -> dict:
    """Typed pages, exact native keys and source values; no empty-page guessing."""
    if owner == 'data_binance':
        chunk, width, limit = payload, 12, 499
        normalize = binance._normalize_candles
    elif owner == 'data_bybit':
        result = payload.get('result') if isinstance(payload, dict) else None
        chunk = result.get('list') if isinstance(result, dict) else None
        width, limit, normalize = 7, 1000, bybit._normalize_candles
    elif owner == 'data_okx':
        chunk = payload.get('data') if isinstance(payload, dict) else None
        width, limit, normalize = 9, 100, okx._normalize_candles
    else:
        raise ValueError('unsupported source')
    if not isinstance(chunk, list) or len(chunk) > limit:
        raise ValueError('unverified source page contract')
    keys = []
    selected = []
    for row in chunk:
        if not isinstance(row, (list, tuple)) or len(row) < width:
            raise ValueError('incomplete source candle')
        timestamp = int(row[0])
        if timestamp % 60000:
            raise ValueError('noncanonical source minute key')
        keys.append(timestamp)
        if lower <= timestamp <= upper and (owner != 'data_okx' or str(row[8]) == '1'):
            selected.append(row)
    # These audit windows are <= 499 minutes. A full page whose oldest key is
    # later than the lower bound needs another page, not an empty-source claim.
    covered = not chunk or len(chunk) < limit or min(keys) <= lower
    frame = normalize(selected)
    bad = frame.filter(invalid_candle_values()) if not frame.is_empty() else frame
    returned_keys = {int(row[0]) for row in selected}
    wanted = set(range(lower, upper + 1, 60000))
    return {
        'page_covers_requested_lower_bound': covered,
        'returned_page_rows': len(chunk), 'requested_minute_keys': len(wanted),
        'returned_requested_keys': len(returned_keys),
        'not_returned_requested_keys': len(wanted - returned_keys) if covered else None,
        'source_invalid_requested_rows': bad.height,
        'source_rows_in_requested_window': selected,
        'source_response_sha256': hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest(),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=ROOT)
    parser.add_argument('--acceptance-dir', type=Path, required=True)
    parser.add_argument('--output-dir', type=Path, required=True)
    args = parser.parse_args()
    summary = json.loads((args.acceptance_dir / 'summary.json').read_text())
    input_path = args.acceptance_dir / 'logical_price_verification.json'
    if not summary.get('TEJ_excluded') or sha256_file(input_path) != summary['outputs'][input_path.name]['sha256']:
        raise ValueError('acceptance evidence mismatch')
    profiles = json.loads(input_path.read_text())
    clients, catalogs = {}, {}
    result_rows = []
    for profile in profiles:
        path = Path(profile['path'])
        if not path.resolve().is_relative_to(args.root.resolve()):
            raise ValueError('untrusted audit path')
        owner = path.relative_to(args.root).parts[0]
        code = path.name.removesuffix('_features.parquet')
        for lower, upper in profile.get('repair_windows_ms', []):
            if upper < lower or upper - lower >= 499 * 60000 or lower % 60000 or upper % 60000:
                raise ValueError('quality probe window exceeds bounded scope')
            if owner not in clients:
                if owner == 'data_binance':
                    clients[owner] = binance.BinanceClient(requested_weight_per_minute=None, max_retries=2, retry_base=1)
                elif owner == 'data_bybit':
                    clients[owner] = bybit.BybitClient(None, 2, 1)
                elif owner == 'data_okx':
                    clients[owner] = okx.OkxClient(None, 2, 1)
                else:
                    raise ValueError('TEJ and noncanonical owners not supported')
                with (args.root / owner / '1m/symbols.csv').open(newline='') as stream:
                    catalogs[owner] = {row['code']: row for row in csv.DictReader(stream)}
            item = {'owner': owner, 'code': code, 'lower_ms': lower, 'upper_ms': upper,
                    'observed_at_utc': datetime.now(UTC).isoformat(), 'source_file_rewritten': False}
            try:
                record = catalogs[owner][code]
                if owner == 'data_binance':
                    payload = clients[owner].get(binance.KLINE_ENDPOINT,
                        {'symbol': record['binance_symbol'], 'interval': '1m', 'startTime': lower,
                         'endTime': upper, 'limit': 499}, weight=2)
                elif owner == 'data_bybit':
                    payload = clients[owner].get(bybit.KLINE_ENDPOINT,
                        {'category': record['category'], 'symbol': record['bybit_symbol'],
                         'interval': '1', 'start': lower, 'end': upper, 'limit': '1000'})
                else:
                    payload = clients[owner].get(okx.HISTORY_CANDLES_ENDPOINT,
                        {'instId': record['okx_symbol'], 'bar': '1m', 'after': upper + 1, 'limit': '100'})
                item.update(classify_response(owner, payload, lower, upper), status='source_response_verified')
            except Exception as exc:
                # No response bodies/URLs/secrets in diagnostic exceptions.
                item.update(status='source_probe_unverified', error_type=type(exc).__name__)
            result_rows.append(item)
            print(f"[source-window] {owner}:{code} status={item['status']} absent="
                  f"{item.get('not_returned_requested_keys')} bad={item.get('source_invalid_requested_rows')}", flush=True)
    atomic_write_json(args.output_dir / 'source_window_evidence.json', result_rows)
    csv_file(args.output_dir / 'source_window_evidence.csv', result_rows)
    atomic_write_json(args.output_dir / 'summary.json', {
        'contract': 'exact_native_candle_source_window_verification_v1', 'TEJ_excluded': True,
        'observed_at_utc': datetime.now(UTC).isoformat(), 'windows': len(result_rows),
        'confirmed_not_returned_minutes': sum(r.get('not_returned_requested_keys') or 0 for r in result_rows),
        'confirmed_source_invalid_rows': sum(r.get('source_invalid_requested_rows') or 0 for r in result_rows),
        'unverified_windows': sum(r['status'] != 'source_response_verified' or
            not r['page_covers_requested_lower_bound'] for r in result_rows),
        'evidence_sha256': sha256_file(args.output_dir / 'source_window_evidence.json'),
        'official_client_shared_limiters_used': True, 'source_files_rewritten': 0,
        'new_venues_used': False, 'missing_source_data_not_synthesized': True,
        'all_source_history_or_field_semantics_complete': False,
    })


if __name__ == '__main__':
    main()
