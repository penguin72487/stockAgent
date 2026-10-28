#!/usr/bin/env python3
"""Register exact audited option-day conflicts with the canonical owner.

This publishes only an immutable short-lived intent. No task, lock, credential,
quota, provider request, receipt or data object is modified by this command.
"""
from __future__ import annotations

import argparse
from datetime import UTC, datetime, timedelta
import hashlib
import json
import os
from pathlib import Path
import tempfile

from downloader.finmind_source_conflict_rechecks import CONTRACT, SIDECAR, load_option_conflicts


def register(root: Path, summary_path: Path, dates: list[str], now: datetime) -> dict:
    root = root.resolve()
    workspace = root.parents[1]
    summary_path = summary_path.resolve()
    if not summary_path.is_relative_to(workspace):
        raise ValueError('coverage evidence must be inside the workspace')
    plan = {
        'contract': CONTRACT, 'created_at_utc': now.isoformat(),
        'expires_at_utc': (now + timedelta(days=7)).isoformat(),
        'coverage_summary_path': str(summary_path.relative_to(workspace)),
        'coverage_summary_sha256': hashlib.sha256(summary_path.read_bytes()).hexdigest(),
        'dates': sorted(dates),
    }
    root.mkdir(parents=True, exist_ok=True)
    name = None
    try:
        with tempfile.NamedTemporaryFile('w', encoding='utf-8', dir=root, delete=False) as output:
            name = output.name
            json.dump(plan, output, ensure_ascii=False, sort_keys=True, indent=2)
            output.write('\n')
            output.flush()
            os.fsync(output.fileno())
        checked, digest = load_option_conflicts(root, now, sidecar=Path(name))
        target = root / SIDECAR
        # Link publishes fully written bytes atomically and refuses to replace
        # any existing plan, including one registered by another operator.
        os.link(name, target)
        return {'contract': CONTRACT, 'plan_path': str(target), 'plan_sha256': digest,
                'dates': dates, 'requests': len(checked['requests']),
                'expires_at_utc': plan['expires_at_utc'], 'provider_calls': 0,
                'task_states_changed': False}
    finally:
        if name and os.path.exists(name):
            os.unlink(name)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path('data_finmind/complement'))
    parser.add_argument('--coverage-summary', type=Path, required=True)
    parser.add_argument('--date', action='append', required=True)
    args = parser.parse_args()
    print(json.dumps(register(args.root, args.coverage_summary, args.date, datetime.now(UTC)), ensure_ascii=False))


if __name__ == '__main__':
    main()
