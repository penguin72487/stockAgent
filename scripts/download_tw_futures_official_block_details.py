#!/usr/bin/env python3
"""Retain dated TAIFEX block detail responses for exact-print reconciliation."""
from __future__ import annotations

import argparse
from datetime import UTC, date, datetime
import json
from pathlib import Path
import sys

import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_bytes, atomic_write_json, sha256_file
from stockagent.data.tw_futures_official_block_details import ENDPOINTS, response_status


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--date', action='append', required=True)
    p.add_argument('--output', required=True, type=Path)
    args = p.parse_args()
    out = args.output.resolve()
    manifest = out/'manifest.json'
    if manifest.exists():
        raise FileExistsError('official detail manifest already exists')
    days = sorted({date.fromisoformat(day) for day in args.date})
    records = []
    with requests.Session() as session:
        for day in days:
            text_day = day.strftime('%Y/%m/%d')
            form = dict(queryType='', goDay='', dateaddcnt='', queryStartDate=text_day,
                        queryEndDate=text_day)
            for kind, (endpoint, _) in ENDPOINTS.items():
                folder = out/str(day)
                folder.mkdir(parents=True, exist_ok=True)
                member = folder/(kind+'.raw')
                if member.exists():
                    raise FileExistsError(f'preserve existing official detail response: {member}')
                url = 'https://www.taifex.com.tw/cht/3/'+endpoint
                response = session.post(url, data=form, timeout=40)
                atomic_write_bytes(member, response.content)
                try:
                    status = response_status(response.content, kind) if response.status_code == 200 else 'rejected_http'
                except (UnicodeError, ValueError):
                    status = 'rejected_encoding'
                records.append(dict(date=str(day), kind=kind, url=url, method='POST', form=form,
                    http_status=response.status_code, status=status, path=str(member.relative_to(out)),
                    bytes=member.stat().st_size, sha256=sha256_file(member),
                    observed_at_utc=datetime.now(UTC).isoformat()))
                print(json.dumps({k:v for k,v in records[-1].items() if k not in ('form',)}, ensure_ascii=False), flush=True)
    atomic_write_json(manifest, dict(contract='taifex_official_block_detail_inventory_v1',
        observed_at_utc=datetime.now(UTC).isoformat(), requested_dates=[str(d) for d in days],
        sources=records))
    return int(any(r['status'] not in ('complete', 'official_explicit_empty') for r in records))


if __name__ == '__main__':
    raise SystemExit(main())
