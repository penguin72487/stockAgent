#!/usr/bin/env python3
"""Queue audited missing evening option contract-days through the native owner."""
from __future__ import annotations

import argparse
from datetime import UTC, date, datetime, timedelta
import json
from pathlib import Path

import polars as pl

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.provider_priority import inspect
from scripts.audit_taifex_finmind_evening_option_contracts import missing_for_day
from scripts.build_taifex_option_finmind_reference_window import pin_source
from scripts.request_finmind_option_tick_product_checks import CONTRACT, DATASET, register_evidenced


def covered_by_previous(row: dict, previous: dict) -> bool:
    """Reuse an active exact check only if it covers this evidence and source."""
    if (previous.get('contract') != CONTRACT
            or previous.get('scope') != 'per_product_physical_day_source_comparison_only_no_training_injection'):
        raise ValueError('previous source check has an incompatible contract')
    source = row['original_source']
    expected = {tuple(key) for key in row['missing_contracts']}
    for check in previous.get('checks', []):
        if (check.get('dataset') == DATASET and check.get('partition') == row['source_day']
                and check.get('data_id') == row['product']
                and check.get('original_receipt_sha256') == source['receipt_sha256']
                and check.get('original_parquet_sha256') == source['sha256']
                and expected <= {tuple(key) for key in check.get('audited_missing_contracts', [])}):
            return True
    return False


def register(root: Path, audit_path: Path, output: Path, now: datetime, *,
             previous_plan: Path | None = None, previous_registration: Path | None = None) -> dict:
    root, audit_path, output = root.resolve(), audit_path.resolve(), output.resolve()
    workspace = root.parents[1]
    if (not audit_path.is_relative_to(workspace / 'artifacts/markets')
            or not output.is_relative_to(workspace / 'artifacts/markets')
            or output.exists() or output.is_symlink() or now.tzinfo is None):
        raise ValueError('new in-workspace source plan and an aware clock are required')
    audit_sha = sha256_file(audit_path)
    audit = json.loads(audit_path.read_bytes())
    if (audit.get('contract') != 'taifex_finmind_evening_option_contract_audit_v1'
            or audit.get('training_ready') is not False):
        raise ValueError('official evening source audit is required')
    pins = {str(audit_path): audit_sha, str(Path(__file__).resolve()): sha256_file(Path(__file__))}
    implementation = Path('scripts/audit_taifex_finmind_evening_option_contracts.py').resolve()
    if sha256_file(implementation) != audit['implementation_sha256']:
        raise ValueError('evening audit implementation changed')
    pins[str(implementation)] = audit['implementation_sha256']
    for path_name, hash_name in (('official_manifest', 'official_manifest_sha256'),
                                 ('official_data', 'official_data_sha256')):
        path = Path(audit[path_name]).resolve()
        if not path.is_relative_to(workspace) or sha256_file(path) != audit[hash_name]:
            raise ValueError('official source evidence changed')
        pins[str(path)] = audit[hash_name]
    references = pl.read_parquet(audit['official_data'])
    previous = None
    if previous_plan is not None or previous_registration is not None:
        if previous_plan is None or previous_registration is None:
            raise ValueError('previous plan and registration must both be supplied')
        previous_plan, previous_registration = previous_plan.resolve(), previous_registration.resolve()
        if not previous_plan.is_relative_to(workspace) or not previous_registration.is_relative_to(workspace):
            raise ValueError('previous source evidence is outside the workspace')
        previous = json.loads(previous_plan.read_bytes())
        registration = json.loads(previous_registration.read_bytes())
        if registration.get('source_plan') != str(previous_plan):
            raise ValueError('previous registration does not identify its source plan')
        status = inspect(root, registration['request_id'], now=now)
        if status.get('priority_active') is not True:
            previous = None
        else:
            pins[str(previous_plan)] = sha256_file(previous_plan)
            pins[str(previous_registration)] = sha256_file(previous_registration)
    checks, reused = [], []
    cached_days = {}
    for row in audit['missing']:
        day, product = date.fromisoformat(row['source_day']), row['product']
        if day not in cached_days:
            source = pin_source(root, day)
            missing, _ = missing_for_day(references, pl.read_parquet(source['path']), day)
            cached_days[day] = source, missing
        source, missing = cached_days[day]
        actual = [list(key) for key in missing.filter(pl.col('product') == product).rows()]
        if source != row['original_source'] or actual != row['missing_contracts'] or not actual:
            raise ValueError('night contract source changed; re-audit before registration')
        if previous is not None and covered_by_previous(row, previous):
            reused.append({'date': day.isoformat(), 'product': product, 'contracts': len(actual)})
            continue
        checks.append({'dataset': DATASET, 'data_id': product, 'partition': day.isoformat(),
                       'audited_missing_contracts': actual,
                       'original_receipt': source['receipt_path'],
                       'original_receipt_sha256': source['receipt_sha256'],
                       'original_parquet': source['path'], 'original_parquet_sha256': source['sha256'],
                       'conflict_plan': str(audit_path), 'conflict_plan_sha256': audit_sha,
                       'conflict_plan_expires_at_utc': (now + timedelta(days=6)).isoformat()})
    if not 0 < len(checks) <= 64:
        raise ValueError('one finite source batch requires 1 to 64 uncovered product-days')
    result = register_evidenced(root, checks, now, output, dataset=DATASET, contract=CONTRACT,
        reason='Check official prior-evening option contracts absent from retained whole-market tick objects using native per-product queries.',
        additional_evidence_pins=pins)
    result['reused_existing_checks'] = reused
    result['missing_contract_days_requested'] = sum(len(row['audited_missing_contracts']) for row in checks)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path('data_finmind/complement'))
    parser.add_argument('--audit', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--registration', type=Path, required=True)
    parser.add_argument('--previous-plan', type=Path)
    parser.add_argument('--previous-registration', type=Path)
    args = parser.parse_args()
    if args.registration.exists() or args.registration.is_symlink():
        raise FileExistsError('preserve the prior registration')
    result = register(args.root, args.audit, args.output, datetime.now(UTC),
                      previous_plan=args.previous_plan, previous_registration=args.previous_registration)
    atomic_write_json(args.registration, result)
    print(json.dumps(result, ensure_ascii=False), flush=True)


if __name__ == '__main__':
    main()
