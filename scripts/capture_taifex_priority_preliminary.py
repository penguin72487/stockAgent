#!/usr/bin/env python3
"""Capture a finite pre-17:00 TAIFEX source snapshot under the canonical lock.

This does not publish a canonical completed-day receipt.  The standard owner
must later acquire and validate its completed trading-day archive independently.
"""
from __future__ import annotations

import argparse
from collections import Counter
import csv
from datetime import date, datetime, timedelta
import io
import json
from pathlib import Path
import zipfile

from downloader.artifact_io import atomic_write_bytes, atomic_write_json, sha256_file
from downloader.common import SharedRateLimiter, resolve_request_interval
from downloader.http_transport import HttpRequestPolicy, ResilientHttpTransport
from scripts.check_acquisition_capacity import priority_dispatch_status
from scripts import download_taifex_recent_index_derivatives_ticks as owner


def _readback(path: Path, *, kind: str, day: date, digest: str) -> dict:
    if path.is_symlink() or sha256_file(path) != digest:
        raise ValueError('preliminary ZIP differs from its capture')
    prefix = 'Daily' if kind == 'futures' else 'OptionsDaily'
    member = f"{prefix}_{day.strftime('%Y_%m_%d')}.csv"
    by_date, products = Counter(), set()
    with zipfile.ZipFile(path) as archive, archive.open(member) as raw:
        rows = csv.reader(io.TextIOWrapper(raw, encoding='cp950', newline=''))
        header = next(rows)
        if not header or header[0].strip() != '成交日期' or header[1].strip() != '商品代號':
            raise ValueError('unexpected preliminary CSV header')
        for row in rows:
            if kind == 'options' and len(row) == 1 and row[0].strip().startswith('-'):
                continue
            if len(row) < (9 if kind == 'options' else 6):
                raise ValueError('malformed preliminary source row')
            raw_day, product = row[0].strip(), row[1].strip()
            if len(raw_day) != 8 or not raw_day.isdigit() or not product:
                raise ValueError('invalid preliminary source row identity')
            physical = date(int(raw_day[:4]), int(raw_day[4:6]), int(raw_day[6:8]))
            if not day - timedelta(days=10) <= physical <= day:
                raise ValueError('preliminary source physical date outside trading-file window')
            by_date[physical.isoformat()] += 1
            products.add(product)
    if sha256_file(path) != digest:
        raise ValueError('preliminary ZIP changed during readback')
    return {'source_rows': sum(by_date.values()), 'physical_date_rows': dict(sorted(by_date.items())),
            'source_product_count': len(products)}


def capture(root: Path, output: Path, *, day: date, now: datetime, transport) -> dict:
    root, output = root.resolve(), output.resolve()
    if now.tzinfo is None:
        raise ValueError('a timezone-aware capture time is required')
    local = now.astimezone(owner.TAIPEI)
    if (day != local.date() or day <= owner._latest_completed_listing_date(local)
            or output.exists() or not output.is_relative_to(root / 'research')):
        raise ValueError('a new research-only current pre-completion day is required')
    capacity = priority_dispatch_status(root, additional_headroom_bytes=40 * 1024**3)
    if capacity.get('state') != 'admitted':
        raise RuntimeError('priority capture capacity not admitted')
    with owner._acquire_lock(root / 'state' / 'download.lock'):
        pages, downloads = {}, {}
        for kind, url, pattern in (
            ('futures', owner.FUTURES_LISTING_URL, owner.FUTURES_URL_RE),
            ('options', owner.OPTIONS_LISTING_URL, owner.OPTIONS_URL_RE),
        ):
            payload, headers = owner._fetch_bytes(url, attempts=3, timeout=120, transport=transport)
            listed = owner._extract_downloads(payload, pattern=pattern)
            if day not in listed:
                raise ValueError('requested day absent from the official ' + kind + ' listing')
            pages[kind] = {'url': url, 'payload': payload, 'last_modified': headers.get('last-modified')}
            downloads[kind] = listed[day]
        output.mkdir(parents=True, exist_ok=False)
        receipts, readbacks = {}, {}
        for kind in ('futures', 'options'):
            page = output / f'{kind}_listing.html'
            atomic_write_bytes(page, pages[kind]['payload'], durable=True)
            pages[kind] = {'url': pages[kind]['url'], 'path': str(page), 'sha256': sha256_file(page),
                           'bytes': page.stat().st_size, 'last_modified': pages[kind]['last_modified']}
            prefix = 'Daily' if kind == 'futures' else 'OptionsDaily'
            compact = day.strftime('%Y_%m_%d')
            target = output / f'{prefix}_{compact}.zip'
            receipts[kind] = owner._download_zip(downloads[kind], target, expected_member=f'{prefix}_{compact}.csv',
                                                  attempts=3, timeout=120, transport=transport)
            readbacks[kind] = _readback(target, kind=kind, day=day, digest=receipts[kind]['sha256'])
        capacity_publish = priority_dispatch_status(root, additional_headroom_bytes=40 * 1024**3)
        if capacity_publish.get('state') != 'admitted':
            raise RuntimeError('priority capture publication capacity not admitted')
        if any(sha256_file(Path(info['path'])) != info['sha256'] for info in pages.values()):
            raise ValueError('official listing changed before capture publication')
        manifest = {'contract': 'taifex_precompletion_source_snapshot_v1',
                    'captured_at': datetime.now(owner.TAIPEI).isoformat(), 'trading_date': day.isoformat(),
                    'status': 'observed_provisional', 'complete_trading_day': False,
                    'listing_pages': pages, 'raw_downloads': receipts, 'independent_readback': readbacks,
                    'owner_script_sha256': sha256_file(Path(owner.__file__)),
                    'implementation_sha256': sha256_file(Path(__file__)),
                    'capacity_at_start': capacity, 'capacity_before_publication': capacity_publish,
                    'training_ready': False,
                    'limitations': ['TAIFEX current-day archive can change until the normal publication cutoff.',
                        'Presence and row counts do not establish full trades, decision-time execution or accounting.',
                        'Canonical completed-day collection and receipt remain independent and required.']}
        atomic_write_json(output / 'manifest.json', manifest)
        return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path('data_tw_index_derivatives_ticks'))
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--date', type=date.fromisoformat, required=True)
    args = parser.parse_args()
    interval = resolve_request_interval('taifex_public', 0.25)
    transport = ResilientHttpTransport(HttpRequestPolicy(provider='taifex_public', timeout_seconds=120,
                                        max_retries=2, retry_base_seconds=0.5),
                                       limiter=SharedRateLimiter(interval, name='taifex_public'))
    result = capture(args.root, args.output, day=args.date, now=datetime.now(owner.TAIPEI), transport=transport)
    print(json.dumps({'manifest': str(args.output / 'manifest.json'),
                      'independent_readback': result['independent_readback'],
                      'status': result['status'], 'training_ready': False}, ensure_ascii=False), flush=True)


if __name__ == '__main__':
    main()
