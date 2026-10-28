#!/usr/bin/env python3
"""Register bounded, audited product-day rechecks without calling a provider."""
from __future__ import annotations

import argparse
import csv
from datetime import UTC, datetime, timedelta
import hashlib
import json
import os
from pathlib import Path
import tempfile

from downloader.finmind_source_conflict_rechecks import (
    PRODUCT_BATCHES, PRODUCT_CONTRACT, load_option_product_conflicts,
)


def register(root: Path, coverage: Path, presence: Path, opening: Path, now: datetime) -> dict:
    root = root.resolve()
    workspace = root.parents[1]
    sources = {'coverage': coverage.resolve(), 'presence': presence.resolve(), 'open': opening.resolve()}
    if any(not path.is_relative_to(workspace) for path in sources.values()):
        raise ValueError('source audits must be inside the workspace')
    evidence = {}
    for label, path in sources.items():
        evidence[f'{label}_summary_path'] = str(path.relative_to(workspace))
        evidence[f'{label}_summary_sha256'] = hashlib.sha256(path.read_bytes()).hexdigest()
    opening_summary = json.loads(sources['open'].read_bytes())
    inventory = Path(opening_summary['inventory']).resolve()
    if not inventory.is_relative_to(workspace):
        raise ValueError('opening evidence must be inside the workspace')
    if hashlib.sha256(inventory.read_bytes()).hexdigest() != opening_summary['inventory_sha256']:
        raise ValueError('opening inventory changed')
    with inventory.open(encoding='utf-8', newline='') as source:
        dates = sorted({row['date'] for row in csv.DictReader(source)
                        if row['classification'] == 'official_open_missing_from_object'
                        and int(row['official_positive_open_rows']) > 0})
    if not dates or len(dates) > 16 * 64:
        raise ValueError('product-conflict date count is outside bounded scope')
    directory = root / PRODUCT_BATCHES
    directory.mkdir(parents=True, exist_ok=True)
    if list(directory.glob('*.json')):
        raise FileExistsError('product-conflict batches already registered')
    staged = []
    try:
        for offset in range(0, len(dates), 64):
            batch = dates[offset:offset + 64]
            plan = {'contract': PRODUCT_CONTRACT, 'created_at_utc': now.isoformat(),
                    'expires_at_utc': (now + timedelta(days=7)).isoformat(),
                    **evidence, 'dates': batch}
            with tempfile.NamedTemporaryFile('w', encoding='utf-8', dir=directory,
                                             suffix='.pending', delete=False) as file:
                temporary = Path(file.name)
                staged.append((temporary, None, None))
                json.dump(plan, file, ensure_ascii=False, sort_keys=True, indent=2)
                file.write('\n')
                file.flush()
                os.fsync(file.fileno())
            checked, digest = load_option_product_conflicts(root, now, sidecar=temporary)
            target = directory / f'{batch[0]}_{batch[-1]}_{digest[:16]}.json'
            staged[-1] = (temporary, target, checked)
        if any(target.exists() for _, target, _ in staged):
            raise FileExistsError('product-conflict batch would overwrite an existing plan')
        for temporary, target, _ in staged:
            os.link(temporary, target)
        return {'contract': PRODUCT_CONTRACT, 'batches': len(staged), 'dates': len(dates),
                'product_days': sum(len(req['expected_missing_source_products'])
                                    for _, _, plan in staged for req in plan['requests']),
                'plan_paths': [str(target) for _, target, _ in staged],
                'expires_at_utc': (now + timedelta(days=7)).isoformat(),
                'provider_calls': 0, 'task_states_changed': False}
    finally:
        for temporary, _, _ in staged:
            temporary.unlink(missing_ok=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path('data_finmind/complement'))
    parser.add_argument('--coverage-summary', type=Path, required=True)
    parser.add_argument('--presence-summary', type=Path, required=True)
    parser.add_argument('--open-summary', type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(register(args.root, args.coverage_summary, args.presence_summary,
                              args.open_summary, datetime.now(UTC)), ensure_ascii=False))


if __name__ == '__main__':
    main()
