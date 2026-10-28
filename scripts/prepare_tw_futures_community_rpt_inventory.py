#!/usr/bin/env python3
"""Freeze downloaded public-mirror bytes separately from exchange sources."""
from __future__ import annotations

import argparse
from datetime import UTC, datetime, date
import json
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json, sha256_file
from stockagent.data.tw_futures_community_rpt import INVENTORY, SOURCE


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--catalog', required=True, type=Path)
    p.add_argument('--raw-root', required=True, type=Path)
    p.add_argument('--output', required=True, type=Path)
    p.add_argument('--date', action='append', default=[], help='Optional exact pilot dates')
    args = p.parse_args()
    catalog, raw_root, output = args.catalog.resolve(), args.raw_root.resolve(), args.output.resolve()
    if output.exists():
        raise FileExistsError('an immutable community inventory already exists')
    if raw_root.parent != output.parent or raw_root.name != 'raw':
        raise ValueError('inventory must be beside its raw source directory')
    catalog_sha = sha256_file(catalog)
    doc = json.loads(catalog.read_text())
    if doc.get('contract') != 'community_public_drive_universe765_unique_gap_catalog_v1':
        raise ValueError('unknown community Drive catalog contract')
    selected = {str(date.fromisoformat(d)) for d in args.date}
    objects, absent, seen = [], [], set()
    for row in doc['items']:
        day = date.fromisoformat(row['date'])
        if selected and str(day) not in selected:
            continue
        expected = f"Daily_{day.strftime('%Y_%m_%d')}.zip"
        if (row['title'] != expected or str(day) in seen
                or not re.fullmatch(r'[A-Za-z0-9_-]+', row['id'])):
            raise ValueError('community catalog identity is not unique')
        seen.add(str(day))
        member = raw_root/expected
        if not member.is_file():
            absent.append(str(day))
            continue
        before = member.stat()
        if before.st_size != int(row['size']):
            raise ValueError(f'community source size mismatch: {day}')
        digest = sha256_file(member)
        after = member.stat()
        if (before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (
                after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns):
            raise ValueError(f'community source changed during pinning: {day}')
        objects.append(dict(date=str(day), path=f'raw/{expected}', drive_id=row['id'],
            bytes=before.st_size, sha256=digest, drive_created_time=row.get('created_time'),
            drive_modified_time=row.get('modified_time')))
    if selected and seen != selected:
        raise ValueError('pilot date is missing from the unique source catalog')
    if sha256_file(catalog) != catalog_sha:
        raise ValueError('community catalog changed during pinning')
    payload = dict(contract=INVENTORY, source=SOURCE, created_at_utc=datetime.now(UTC).isoformat(),
        catalog_path=str(catalog), catalog_sha256=catalog_sha,
        historical_point_in_time_verified=False, source_is_official_publication=False,
        absent_requested_dates=absent, objects=objects)
    atomic_write_json(output, payload)
    print(json.dumps(dict(output=str(output), sha256=sha256_file(output),
                          dates_pinned=len(objects), dates_absent=len(absent))))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
