#!/usr/bin/env python3
"""Register evidence-bound futures tick rechecks with the existing owner."""
from __future__ import annotations

import argparse
from datetime import UTC, datetime, timedelta
import hashlib
import json
import os
from pathlib import Path
import tempfile

from downloader.finmind_futures_tick_conflicts import BATCHES, CONTRACT, load_futures_tick_conflicts


def register(root: Path, summary_path: Path, now: datetime) -> dict:
    root, summary_path = root.resolve(), summary_path.resolve()
    workspace = root.parents[1]
    if not summary_path.is_relative_to(workspace):
        raise ValueError('audit summary must be inside the workspace')
    summary_bytes = summary_path.read_bytes()
    summary = json.loads(summary_bytes)
    days_path = (workspace / summary['days_path']).resolve()
    if not days_path.is_relative_to(workspace):
        raise ValueError('audit days must be inside the workspace')
    days_bytes = days_path.read_bytes()
    if hashlib.sha256(days_bytes).hexdigest() != summary['days_sha256']:
        raise ValueError('audit days changed')
    days = json.loads(days_bytes)
    dates = sorted(row['date'] for row in days if row.get('unmatched_contracts', 0) > 0 or
                   (row.get('status') == 'receipt_not_complete'
                    and row.get('provider_status') == 'observed_empty'))
    if not 0 < len(dates) <= 64 or len(dates) != len(set(dates)):
        raise ValueError('futures conflict requires 1 to 64 distinct audited dates')
    directory = root / BATCHES
    directory.mkdir(parents=True, exist_ok=True)
    if list(directory.glob('*.json')):
        raise FileExistsError('futures conflict batches already registered')
    plan = {'contract': CONTRACT, 'created_at_utc': now.isoformat(),
            'expires_at_utc': (now + timedelta(days=7)).isoformat(),
            'summary_path': str(summary_path.relative_to(workspace)),
            'summary_sha256': hashlib.sha256(summary_bytes).hexdigest(), 'dates': dates}
    temporary = None
    try:
        with tempfile.NamedTemporaryFile('w', encoding='utf-8', dir=directory,
                                         suffix='.pending', delete=False) as file:
            temporary = Path(file.name)
            json.dump(plan, file, ensure_ascii=False, sort_keys=True, indent=2)
            file.write('\n')
            file.flush()
            os.fsync(file.fileno())
        checked, digest = load_futures_tick_conflicts(root, now, sidecar=temporary)
        target = directory / f'{dates[0]}_{dates[-1]}_{digest[:16]}.json'
        os.link(temporary, target)
        return {'contract': CONTRACT, 'dates': len(dates),
                'contract_day_keys': sum(len(item['expected_source_futures_contracts'])
                                         for item in checked['requests']),
                'plan_path': str(target), 'expires_at_utc': plan['expires_at_utc'],
                'provider_calls': 0, 'task_states_changed': False}
    finally:
        if temporary:
            temporary.unlink(missing_ok=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path('data_finmind/complement'))
    parser.add_argument('--summary', type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(register(args.root, args.summary, datetime.now(UTC)), ensure_ascii=False))


if __name__ == '__main__':
    main()
