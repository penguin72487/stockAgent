#!/usr/bin/env python3
"""Register bounded contract-key source rechecks with the existing owner."""
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
    CONTRACT_BATCHES, CONTRACT_CONFLICT, load_option_contract_conflicts,
)


def register(root: Path, coverage: Path, contract_summary: Path, now: datetime, *, append: bool = False) -> dict:
    root = root.resolve()
    workspace = root.parents[1]
    coverage, contract_summary = coverage.resolve(), contract_summary.resolve()
    if not coverage.is_relative_to(workspace) or not contract_summary.is_relative_to(workspace):
        raise ValueError('contract audits must be inside the workspace')
    summary = json.loads(contract_summary.read_bytes())
    inventory = Path(summary['day_inventory']).resolve()
    if not inventory.is_relative_to(workspace):
        raise ValueError('contract inventory must be inside the workspace')
    if hashlib.sha256(inventory.read_bytes()).hexdigest() != summary['day_inventory_sha256']:
        raise ValueError('contract inventory changed')
    with inventory.open(encoding='utf-8', newline='') as source:
        dates = sorted({row['date'] for row in csv.DictReader(source)
                        if row['source_status'] == 'contract_key_readback_verified'
                        and int(row['missing_contracts']) > 0})
    if not dates or len(dates) > 16 * 64:
        raise ValueError('contract-conflict date count is outside bounded scope')
    directory = root / CONTRACT_BATCHES
    directory.mkdir(parents=True, exist_ok=True)
    existing = sorted(directory.glob('*.json'))
    if existing and not append:
        raise FileExistsError('contract-conflict batches already registered')
    if len(existing) + (len(dates) + 63) // 64 > 16:
        raise ValueError('contract-conflict batch limit would be exceeded')
    if append:
        for path in existing:
            prior = json.loads(path.read_bytes())
            if prior.get('contract') != CONTRACT_CONFLICT or set(prior.get('dates', ())) & set(dates):
                raise ValueError('existing contract-conflict dates are invalid or overlap')
    coverage_object = json.loads(coverage.read_bytes())
    staged = []
    try:
        for offset in range(0, len(dates), 64):
            batch = dates[offset:offset + 64]
            plan = {'contract': CONTRACT_CONFLICT, 'created_at_utc': now.isoformat(),
                    'expires_at_utc': (now + timedelta(days=7)).isoformat(),
                    'coverage_summary_path': str(coverage.relative_to(workspace)),
                    'coverage_summary_sha256': hashlib.sha256(coverage.read_bytes()).hexdigest(),
                    'contract_summary_path': str(contract_summary.relative_to(workspace)),
                    'contract_summary_sha256': hashlib.sha256(contract_summary.read_bytes()).hexdigest(),
                    'dates': batch}
            if coverage_object.get('contract') == 'tw_option_all_product_monthly_range_readback_v1':
                from downloader.finmind_option_monthly_evidence import make_pins
                plan['monthly_source_pins'] = make_pins(coverage_object, batch, workspace)
            with tempfile.NamedTemporaryFile('w', encoding='utf-8', dir=directory,
                                             suffix='.pending', delete=False) as file:
                temporary = Path(file.name)
                staged.append((temporary, None, None))
                json.dump(plan, file, ensure_ascii=False, sort_keys=True, indent=2)
                file.write('\n')
                file.flush()
                os.fsync(file.fileno())
            checked, digest = load_option_contract_conflicts(root, now, sidecar=temporary)
            target = directory / f'{batch[0]}_{batch[-1]}_{digest[:16]}.json'
            staged[-1] = (temporary, target, checked)
        if any(target.exists() for _, target, _ in staged):
            raise FileExistsError('contract-conflict batch would overwrite an existing plan')
        for temporary, target, _ in staged:
            os.link(temporary, target)
        return {'contract': CONTRACT_CONFLICT, 'batches': len(staged), 'dates': len(dates),
                'contract_day_keys': sum(len(req['expected_source_contracts'])
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
    parser.add_argument('--contract-summary', type=Path, required=True)
    parser.add_argument('--append', action='store_true',
                        help='add non-overlapping evidence without replacing existing batches')
    args = parser.parse_args()
    print(json.dumps(register(args.root, args.coverage_summary, args.contract_summary, datetime.now(UTC),
                              append=args.append),
                     ensure_ascii=False))


if __name__ == '__main__':
    main()
