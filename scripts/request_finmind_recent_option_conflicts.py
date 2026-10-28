#!/usr/bin/env python3
"""Register exact recent exchange-tick conflicts with the existing FinMind owner.

Registration snapshots evidence and publishes only finite metadata. The owner
alone controls provider sessions, task state, quota, capacity and receipts.
"""
from __future__ import annotations

import argparse
from datetime import UTC, datetime, timedelta
import hashlib
import json
import os
from pathlib import Path
import tempfile

from downloader.finmind_source_conflict_rechecks import (
    RECENT_BATCHES, RECENT_CONFLICT, load_recent_option_conflicts,
)


def _snapshot(path: Path, payload: bytes) -> str:
    with path.open('xb') as file:
        file.write(payload)
        file.flush()
        os.fsync(file.fileno())
    return hashlib.sha256(payload).hexdigest()


def register(root: Path, manifest: Path, output: Path, dates: list[str], now: datetime) -> dict:
    root = root.resolve()
    workspace = root.parents[1]
    manifest, output = manifest.resolve(), output.resolve()
    if not manifest.is_relative_to(workspace) or not output.is_relative_to(workspace):
        raise ValueError('recent option evidence must be in the workspace')
    if not dates or len(dates) > 64 or len(set(dates)) != len(dates):
        raise ValueError('recent option registration requires 1 to 64 unique dates')
    output.mkdir(parents=True, exist_ok=False)
    manifest_bytes = manifest.read_bytes()
    manifest_snapshot = output / 'official_manifest.json'
    manifest_sha = _snapshot(manifest_snapshot, manifest_bytes)
    entries = []
    for day in sorted(dates):
        receipt = root / 'receipts/TaiwanOptionTick/all' / f'{day}.json'
        receipt_snapshot = output / f'{day}_finmind_original_receipt.json'
        receipt_bytes = receipt.read_bytes()
        receipt_sha = _snapshot(receipt_snapshot, receipt_bytes)
        entries.append({'date': day,
                        'original_receipt_snapshot_path': str(receipt_snapshot.relative_to(workspace)),
                        'original_receipt_sha256': receipt_sha})
    if manifest.read_bytes() != manifest_bytes:
        raise ValueError('live exchange manifest changed during snapshot')
    plan = {'contract': RECENT_CONFLICT, 'created_at_utc': now.isoformat(),
            'expires_at_utc': (now + timedelta(days=7)).isoformat(),
            'manifest_snapshot_path': str(manifest_snapshot.relative_to(workspace)),
            'manifest_snapshot_sha256': manifest_sha, 'dates': entries}
    directory = root / RECENT_BATCHES
    directory.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile('w', encoding='utf-8', dir=directory,
                                         suffix='.pending', delete=False) as file:
            temporary = Path(file.name)
            json.dump(plan, file, ensure_ascii=False, sort_keys=True, indent=2)
            file.write('\n')
            file.flush()
            os.fsync(file.fileno())
        checked, digest = load_recent_option_conflicts(root, now, sidecar=temporary)
        target = directory / f'{entries[0]["date"]}_{entries[-1]["date"]}_{digest[:16]}.json'
        os.link(temporary, target)
        result = {'contract': RECENT_CONFLICT, 'plan_path': str(target), 'plan_sha256': digest,
                  'dates': sorted(dates), 'requests': len(checked['requests']),
                  'expected_source_products': {request['start_date']: request['expected_source_products']
                                               for request in checked['requests']},
                  'expires_at_utc': plan['expires_at_utc'], 'provider_calls': 0,
                  'task_states_changed': False}
        _snapshot(output / 'registration.json', (json.dumps(result, ensure_ascii=False, indent=2) + '\n').encode())
        return result
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path('data_finmind/complement'))
    parser.add_argument('--manifest', type=Path, default=Path('data_tw_index_derivatives_ticks/manifest.json'))
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--date', action='append', required=True)
    args = parser.parse_args()
    print(json.dumps(register(args.root, args.manifest, args.output, args.date, datetime.now(UTC)),
                     ensure_ascii=False))


if __name__ == '__main__':
    main()
