#!/usr/bin/env python3
"""Compare pinned official and two original FinMind futures-daily query shapes.

This is an observation audit, not an execution-price or training admission.
It never merges or overwrites provider payloads.
"""
from __future__ import annotations

import argparse
from datetime import UTC, date, datetime
import json
import math
from pathlib import Path

import polars as pl

from downloader.artifact_io import atomic_write_json, sha256_file


REPO = Path(__file__).resolve().parents[1]
KEYS = ['date', 'product', 'contract', 'session']
VALUES = ['open', 'high', 'low', 'close', 'volume']
SESSIONS = {'position': '一般', 'after_market': '盤後'}


def _read_pinned(path: Path, expected: str) -> bytes:
    data = path.read_bytes()
    # Hash the actual bytes read, not just a path inspected at another time.
    import hashlib
    if hashlib.sha256(data).hexdigest() != expected:
        raise ValueError(f'source hash mismatch: {path}')
    return data


def _source_under(root: Path, path: str) -> Path:
    resolved = (root / path).resolve()
    if not resolved.is_relative_to(root.resolve()):
        raise ValueError('provider source path escapes its owner root')
    return resolved


def _unique(frame: pl.DataFrame, label: str) -> None:
    if frame.select(pl.any_horizontal(*(pl.col(k).is_null() for k in KEYS)).any()).item():
        raise ValueError(f'{label} has null or unsupported key')
    if frame.group_by(KEYS).len().filter(pl.col('len') != 1).height:
        raise ValueError(f'{label} has duplicate physical session keys')


def _vendor_keys(frame: pl.DataFrame) -> pl.DataFrame:
    required = {'date', 'futures_id', 'contract_date', 'trading_session', 'open', 'volume'}
    if required - set(frame.columns):
        raise ValueError('FinMind daily source schema incomplete')
    return frame.select(
        pl.col('date').cast(pl.String),
        pl.col('futures_id').cast(pl.String).alias('product'),
        pl.col('contract_date').cast(pl.String).alias('contract'),
        pl.col('trading_session').replace_strict(SESSIONS, default=None).alias('session'),
        pl.col('open').cast(pl.Float64).alias('vendor_open'),
        pl.col('volume').cast(pl.Int64).alias('vendor_volume'),
        pl.lit(True).alias('vendor_key_present'),
    )


def compare(official: pl.DataFrame, vendor: pl.DataFrame) -> tuple[dict, pl.DataFrame]:
    """Compare only reported-session keys; keep unreported sessions separate."""
    needed = set(KEYS + VALUES + ['session_reported'])
    if needed - set(official.columns):
        raise ValueError('official daily source schema incomplete')
    official = official.with_columns(pl.col('date').cast(pl.String))
    candidates = official.filter((pl.col('volume') > 0) & pl.col('open').is_finite() & (pl.col('open') > 0))
    reported = candidates.filter(pl.col('session_reported').fill_null(False))
    unknown = candidates.filter(~pl.col('session_reported').fill_null(False))
    vk = _vendor_keys(vendor)
    _unique(reported, 'official reported session')
    _unique(vk, 'FinMind per-product session')
    joined = reported.join(vk, on=KEYS, how='left', validate='1:1')
    missing = joined.filter(pl.col('vendor_key_present').is_null())
    positive_vendor = ((pl.col('vendor_volume') > 0) & pl.col('vendor_open').is_finite()
                       & (pl.col('vendor_open') > 0)).fill_null(False)
    zero = joined.filter(pl.col('vendor_key_present').fill_null(False) & ~positive_vendor)
    return ({'official_positive_reported_session_keys': reported.height,
             'official_positive_unreported_session_keys': unknown.height,
             'vendor_reported_session_keys_present': joined.height - missing.height,
             'vendor_reported_session_keys_absent': missing.height,
             'vendor_present_but_not_positive_keys': zero.height}, missing)


def _match_sponsor(missing: pl.DataFrame, frames: dict[str, pl.DataFrame]) -> list[dict]:
    result = []
    indexed = {}
    for day, frame in frames.items():
        required = {'date', 'futures_id', 'contract_date', 'trading_session',
                    'open', 'max', 'min', 'close', 'volume'}
        if required - set(frame.columns):
            raise ValueError('FinMind all-market source schema incomplete')
        renamed = frame.rename({'futures_id': 'product', 'contract_date': 'contract',
                                'max': 'high', 'min': 'low'})
        renamed = renamed.with_columns(
            pl.col('date').cast(pl.String),
            pl.col('product').cast(pl.String),
            pl.col('contract').cast(pl.String),
            pl.col('trading_session').replace_strict(SESSIONS, default=None).alias('session'))
        if renamed.filter(pl.col('date') != day).height:
            raise ValueError('all-market day receipt contains another date')
        _unique(renamed, 'FinMind all-market session')
        indexed[day] = {tuple(row[k] for k in KEYS): row for row in renamed.iter_rows(named=True)}
    for row in missing.sort(KEYS).iter_rows(named=True):
        key = tuple(row[k] for k in KEYS)
        match = indexed.get(row['date'], {}).get(key)
        item = {k: row[k] for k in KEYS}
        item['official'] = {k: row[k] for k in VALUES}
        item['official_source_file'] = row.get('source_file')
        item['official_source_sha256'] = row.get('source_sha256')
        if row['date'] not in frames:
            item['all_market_status'] = 'receipt_unavailable'
        elif match is None:
            item['all_market_status'] = 'key_absent'
        else:
            item['all_market'] = {k: match[k] for k in VALUES}
            same = all(row[k] is not None and match[k] is not None and
                       math.isfinite(float(row[k])) and math.isfinite(float(match[k])) and
                       row[k] == match[k] for k in VALUES)
            item['all_market_status'] = 'exact_ohlcv_match' if same else 'ohlcv_conflict'
        result.append(item)
    return result


def audit(official_manifest: Path, readback_path: Path, sponsor_root: Path,
          complement_root: Path, start: date) -> dict:
    official_manifest, readback_path = official_manifest.resolve(), readback_path.resolve()
    sponsor_root, complement_root = sponsor_root.resolve(), complement_root.resolve()
    manifest_sha, readback_sha = sha256_file(official_manifest), sha256_file(readback_path)
    manifest = json.loads(_read_pinned(official_manifest, manifest_sha))
    readback = json.loads(_read_pinned(readback_path, readback_sha))
    if (manifest.get('dataset') != 'all_taifex_daily_source_snapshot' or
            readback.get('contract') != 'finmind_futures_daily_product_source_readback_v1'):
        raise ValueError('unexpected source audit contract')
    official_path = official_manifest.parent / 'all_futures_daily_sessions.parquet'
    expected = manifest['outputs'][official_path.name]
    if official_path.stat().st_size != expected['bytes'] or sha256_file(official_path) != expected['sha256']:
        raise ValueError('official daily snapshot differs from manifest')
    plan_path = Path(readback['verification_plan']).resolve()
    plan = json.loads(_read_pinned(plan_path, readback['verification_plan_sha256']))
    if plan.get('contract') != 'finmind-finite-product-query-verification-v1':
        raise ValueError('unexpected product plan')
    plan_keys = {(x['dataset'], x['data_id'], x['partition']) for x in plan['keys']}
    results = readback['results']
    result_keys = {(x['dataset'], x['data_id'], x['partition']) for x in results}
    if (len(result_keys) != len(results) or result_keys != plan_keys or not result_keys or
            any(dataset != 'TaiwanFuturesDaily' or partition != 'history' or not product
                for dataset, product, partition in result_keys)):
        raise ValueError('readback does not match the requested product histories')
    end = date.fromisoformat(manifest['quality']['last_date'])
    if end < start:
        raise ValueError('official snapshot predates requested range')
    vendors, source_files = [], []
    for item in results:
        if (item.get('status') != 'source_payload_verified' or item.get('task_state') != 'complete'
                or item.get('invalid_date_rows') != 0):
            raise ValueError('product source has not passed native readback')
        receipt_path = _source_under(complement_root, item['receipt_path'])
        receipt = json.loads(_read_pinned(receipt_path, item['receipt_sha256']))
        if ((receipt.get('dataset'), receipt.get('data_id'), receipt.get('partition')) !=
                (item['dataset'], item['data_id'], item['partition']) or
                receipt.get('status') != 'complete' or receipt.get('rows') != item['rows'] or
                receipt.get('sha256') != item['parquet_sha256']):
            raise ValueError('native per-product receipt identity differs')
        payload = _source_under(complement_root, receipt['parquet_path'])
        if payload != Path(item['parquet_path']).resolve() or sha256_file(payload) != receipt['sha256']:
            raise ValueError('native per-product payload differs')
        data = pl.read_parquet(payload)
        if (data.height != item['rows'] or
                data.filter(pl.col('futures_id').is_null() | (pl.col('futures_id') != item['data_id'])).height):
            raise ValueError('native per-product payload row count or product differs')
        vendors.append(data.filter((pl.col('date') >= start.isoformat()) & (pl.col('date') <= end.isoformat())))
        source_files += [(receipt_path, item['receipt_sha256']), (payload, receipt['sha256'])]
    products = sorted(x[1] for x in result_keys)
    official = (pl.scan_parquet(official_path)
                .filter(pl.col('product').is_in(products) & (pl.col('date') >= start) & (pl.col('date') <= end))
                .collect())
    comparison, missing = compare(official, pl.concat(vendors, how='diagonal_relaxed'))
    sponsor_frames, sponsor_sources = {}, []
    for day in sorted(set(missing['date'].to_list())):
        receipt_path = sponsor_root / 'receipts/TaiwanFuturesDaily/all' / f'{day}.json'
        if not receipt_path.is_file():
            sponsor_sources.append({'date': day, 'status': 'receipt_unavailable'})
            continue
        receipt_sha = sha256_file(receipt_path)
        receipt = json.loads(_read_pinned(receipt_path, receipt_sha))
        if ((receipt.get('dataset'), receipt.get('data_id'), receipt.get('partition'), receipt.get('kind')) !=
                ('TaiwanFuturesDaily', '', day, 'day')):
            raise ValueError('all-market receipt identity differs')
        if receipt.get('status') == 'observed_empty' and receipt.get('rows') == 0:
            sponsor_sources.append({'date': day, 'status': 'observed_empty', 'receipt_path': str(receipt_path),
                                    'receipt_sha256': receipt_sha})
            source_files.append((receipt_path, receipt_sha))
            continue
        if receipt.get('status') != 'complete':
            raise ValueError('all-market receipt is neither complete nor empty')
        payload = _source_under(sponsor_root, receipt['parquet_path'])
        if (payload.stat().st_size != receipt['parquet_size_bytes'] or
                sha256_file(payload) != receipt['sha256']):
            raise ValueError('all-market payload differs from receipt')
        frame = pl.read_parquet(payload)
        if frame.height != receipt['rows']:
            raise ValueError('all-market payload row count differs')
        sponsor_frames[day] = frame
        sponsor_sources.append({'date': day, 'status': 'verified_complete', 'receipt_path': str(receipt_path),
                                'receipt_sha256': receipt_sha, 'parquet_path': str(payload),
                                'parquet_sha256': receipt['sha256'], 'rows': frame.height})
        source_files += [(receipt_path, receipt_sha), (payload, receipt['sha256'])]
    missing_keys = _match_sponsor(missing, sponsor_frames)
    for path, expected_sha in [(official_manifest, manifest_sha), (official_path, expected['sha256']),
                               (readback_path, readback_sha), (plan_path, readback['verification_plan_sha256']),
                               *source_files]:
        if sha256_file(path) != expected_sha:
            raise ValueError(f'source changed during readback: {path}')
    return {'contract': 'taifex_finmind_futures_daily_priority_presence_v1',
            'observed_at_utc': datetime.now(UTC).isoformat(),
            'date_start': start.isoformat(), 'date_end': end.isoformat(), 'products': products,
            'comparison': comparison, 'missing_per_product_keys': missing_keys,
            'all_market_exact_ohlcv_matches': sum(x['all_market_status'] == 'exact_ohlcv_match' for x in missing_keys),
            'all_market_unresolved': sum(x['all_market_status'] != 'exact_ohlcv_match' for x in missing_keys),
            'official_manifest': str(official_manifest), 'official_manifest_sha256': manifest_sha,
            'official_snapshot': str(official_path), 'official_snapshot_sha256': expected['sha256'],
            'per_product_readback': str(readback_path), 'per_product_readback_sha256': readback_sha,
            'verification_plan': str(plan_path), 'verification_plan_sha256': readback['verification_plan_sha256'],
            'all_market_sources': sponsor_sources, 'audit_sha256': sha256_file(Path(__file__)),
            'provider_calls': 0, 'financial_or_training_adopted': False,
            'limitations': ['Only positive official rows with explicitly reported sessions count as exact session matches.',
                            'Presence comparison does not compare OHLCV for keys present in both sources.',
                            'All-market reconciliation applies only to per-product missing keys.',
                            'Original official observations remain the canonical source; no payload is rewritten.',
                            'Daily source observations do not establish PIT, executable prices or financial/training admission.']}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--official-manifest', type=Path, required=True)
    parser.add_argument('--per-product-readback', type=Path, required=True)
    parser.add_argument('--sponsor-root', type=Path, default=REPO / 'data_finmind/sponsor')
    parser.add_argument('--complement-root', type=Path, default=REPO / 'data_finmind/complement')
    parser.add_argument('--start', type=date.fromisoformat, default=date(2011, 1, 1))
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    if not output.is_relative_to(REPO / 'artifacts/markets') or output.exists() or output.is_symlink():
        raise ValueError('new versioned output must be inside artifacts/markets')
    result = audit(args.official_manifest, args.per_product_readback, args.sponsor_root,
                   args.complement_root, args.start)
    atomic_write_json(output, result)
    print(json.dumps({'output': str(output), 'comparison': result['comparison'],
                      'all_market_exact_ohlcv_matches': result['all_market_exact_ohlcv_matches'],
                      'all_market_unresolved': result['all_market_unresolved']}, ensure_ascii=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
