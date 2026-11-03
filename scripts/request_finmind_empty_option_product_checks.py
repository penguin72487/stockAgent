#!/usr/bin/env python3
"""Queue documented option products on whole-market observed-empty source days.

TAIFEX daily positive volume proves a retrospective daily observation, not a
trade at a decision clock. The generated Parquet is an official daily source
subset, never a FinMind tick object. The canonical FinMind owner performs all
provider calls and keeps the old whole-market receipt and task intact.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, InvalidOperation
import json
from pathlib import Path
import re
import tempfile

import numpy as np
import polars as pl

from downloader.artifact_io import atomic_write_json, durable_replace, sha256_file
from scripts.audit_taifex_option_daily_tick_coverage import receipt_status
from scripts.build_taifex_option_finmind_reference_window import capacity_check
from scripts.request_finmind_option_tick_product_checks import CONTRACT, DATASET, register_evidenced
from stockagent.data.tw_option_product_daily import iter_option_product_daily_batches


EVIDENCE = 'taifex_option_daily_finmind_empty_product_days_v1'


def expected_contracts(frame: pl.DataFrame) -> dict[tuple[str, str], list[list]]:
    """Retain exact source keys and reject collisions in the downstream float ABI."""
    result: dict[tuple[str, str], set[tuple]] = defaultdict(set)
    exact_at_float: dict[tuple, Decimal] = {}
    for day, product, series, strike, right, session in frame.select(
            'date', 'product', 'series', 'strike_raw', 'right', 'session').iter_rows():
        if (type(day) is not date or not re.fullmatch(r'[A-Z0-9]{2,8}', product or '')
                or not re.fullmatch(r'\d{6}(?:[WF][1-5])?', series or '')
                or right not in {'C', 'P'} or session not in {'day', 'unreported'}):
            raise ValueError('unsupported positive daily contract identity')
        try:
            exact = Decimal(str(strike).strip().replace(',', ''))
            value = float(np.float32(exact))
        except (InvalidOperation, ValueError, OverflowError):
            raise ValueError('invalid positive daily strike') from None
        if not exact.is_finite() or exact <= 0 or not np.isfinite(value) or value <= 0:
            raise ValueError('invalid positive daily strike')
        key = (day.isoformat(), product, series, value, right)
        if key in exact_at_float and exact_at_float[key] != exact:
            raise ValueError('distinct official strikes collide at FinMind Float32 precision')
        exact_at_float[key] = exact
        result[(day.isoformat(), product)].add((product, series, value, right))
    if not result:
        raise ValueError('no official positive-volume option product-days')
    return {key: [list(item) for item in sorted(values)] for key, values in sorted(result.items())}


def build_evidence(root: Path, days: list[date], output: Path) -> Path:
    root, output = root.resolve(), output.resolve()
    workspace = root.parents[1]
    if (not days or len(set(days)) != len(days) or any(type(day) is not date for day in days)
            or output.exists() or not output.is_relative_to(workspace / 'data_tw_index_options_daily/research')):
        raise ValueError('unique source dates and a new official research directory are required')
    manifest = workspace / 'data_tw_index_options_daily/manifest.json'
    manifest_sha = sha256_file(manifest)
    doc = json.loads(manifest.read_bytes())
    archive_pins = {}
    receipts = {}
    frames = []
    code_paths = [Path(__file__), workspace / 'stockagent/data/tw_option_product_daily.py',
                  workspace / 'stockagent/data/tw_index_futures.py',
                  workspace / 'scripts/audit_taifex_option_source_universe.py',
                  workspace / 'scripts/audit_taifex_option_daily_tick_coverage.py']
    code_pins = {str(p.resolve()): sha256_file(p) for p in code_paths}
    capacity_start = capacity_check(output.parent)
    for day in sorted(days):
        archive = workspace / f'data_tw_index_options_daily/raw/annual/{day.year}_opt.zip'
        matching = [row for row in doc['receipts'] if Path(row['path']).name == archive.name]
        if len(matching) != 1 or sha256_file(archive) != matching[0]['sha256']:
            raise ValueError('official annual source is not uniquely receipt-bound')
        archive_pins[str(archive)] = matching[0]['sha256']
        state = receipt_status(day, root, verify_payload_sha=True)
        if state.get('status') != 'observed_empty' or state.get('rows') != 0:
            raise ValueError('an unchanged FinMind observed-empty source receipt is required')
        receipt = json.loads(Path(state['receipt_path']).read_bytes())
        if (receipt.get('kind') != 'id_day' or receipt.get('data_id') != ''
                or receipt.get('request', {}).get('query_shape') != 'whole_market_storage_object_day'):
            raise ValueError('the empty observation must identify the whole-market object query')
        receipts[day.isoformat()] = state
        source = list(iter_option_product_daily_batches(archive, expected_sha256=matching[0]['sha256'],
                                                        trading_dates={day}))
        if not source:
            raise ValueError('official daily source contains no selected date')
        frame = (pl.concat(source).with_columns(
            pl.col('volume_raw').str.strip_chars().str.replace_all(',', '').cast(pl.Int64, strict=False).alias('_volume'))
            .filter((pl.col('_volume') > 0) & pl.col('session').is_in(['day', 'unreported']))
            .drop('_volume'))
        if frame.is_empty():
            raise ValueError('official daily source has no day or unreported-session positive rows')
        frames.append(frame)
    source_rows = pl.concat(frames, how='diagonal_relaxed')
    candidates = expected_contracts(source_rows)
    if {day for day, _ in candidates} != {day.isoformat() for day in days}:
        raise ValueError('not all requested days have supported product evidence')

    def verify_inputs() -> None:
        pins = {str(manifest): manifest_sha, **archive_pins, **code_pins}
        pins.update({row['receipt_path']: row['receipt_sha256'] for row in receipts.values()})
        if any(sha256_file(Path(path)) != digest for path, digest in pins.items()):
            raise ValueError('official source, implementation or empty receipt changed')

    verify_inputs()
    capacity_publish = capacity_check(output.parent)
    output.mkdir(parents=True, exist_ok=False)
    payload = output / 'official_daily_positive_rows.parquet'
    with tempfile.NamedTemporaryFile(dir=output, prefix='.source-', suffix='.tmp', delete=False) as stream:
        temporary = Path(stream.name)
    try:
        source_rows.write_parquet(temporary, compression='zstd', statistics=True)
        if not source_rows.equals(pl.read_parquet(temporary)):
            raise ValueError('official daily source subset readback differs')
        durable_replace(temporary, payload)
    finally:
        temporary.unlink(missing_ok=True)
    verify_inputs()
    evidence = {'contract': EVIDENCE, 'generated_at_utc': datetime.now(UTC).isoformat(),
                'official_manifest': str(manifest), 'official_manifest_sha256': manifest_sha,
                'official_archives': archive_pins, 'finmind_empty_receipts': receipts,
                'implementation_sha256': code_pins, 'data_path': str(payload),
                'data_sha256': sha256_file(payload), 'official_positive_rows': source_rows.height,
                'product_days': len(candidates), 'candidate_contract_keys': sum(map(len, candidates.values())),
                'product_days_by_date': {day.isoformat(): sum(key[0] == day.isoformat() for key in candidates)
                                         for day in sorted(days)},
                'capacity_at_start': capacity_start, 'capacity_before_publication': capacity_publish,
                'training_ready': False,
                'limitations': ['FinMind observed-empty describes its whole-market object response only.',
                                'Official unreported sessions do not prove an observation at a decision clock.',
                                'Contract-key comparisons use Float32 and are candidate-only, not tick completeness.',
                                'Retrospective official daily data is not proof of historical delivery or execution.']}
    path = output / 'manifest.json'
    atomic_write_json(path, evidence)
    return path


def register_checks(root: Path, evidence_path: Path, output: Path, now: datetime) -> dict:
    root, evidence_path, output = root.resolve(), evidence_path.resolve(), output.resolve()
    workspace = root.parents[1]
    if (now.tzinfo is None or not evidence_path.is_relative_to(workspace / 'data_tw_index_options_daily/research')
            or not output.is_relative_to(workspace / 'artifacts/markets')):
        raise ValueError('aware clock and workspace-bound official evidence and priority paths required')
    evidence_sha = sha256_file(evidence_path)
    evidence = json.loads(evidence_path.read_bytes())
    if evidence.get('contract') != EVIDENCE or evidence.get('training_ready') is not False:
        raise ValueError('unsupported official observed-empty evidence')
    payload = Path(evidence['data_path']).resolve()
    if payload.parent != evidence_path.parent or sha256_file(payload) != evidence['data_sha256']:
        raise ValueError('official daily audit subset changed')
    pins = {str(evidence_path): evidence_sha, evidence['official_manifest']: evidence['official_manifest_sha256'],
            **evidence['official_archives'], **evidence['implementation_sha256']}
    if any(not Path(path).resolve().is_relative_to(workspace) or sha256_file(Path(path)) != digest
           for path, digest in pins.items()):
        raise ValueError('official source evidence changed')
    candidates = expected_contracts(pl.read_parquet(payload))
    if (len(candidates) != evidence['product_days']
            or sum(map(len, candidates.values())) != evidence['candidate_contract_keys']):
        raise ValueError('official product/contract counts differ from evidence')
    checks = []
    for (day, product), contracts in candidates.items():
        observed = receipt_status(date.fromisoformat(day), root, verify_payload_sha=True)
        prior = evidence['finmind_empty_receipts'][day]
        if observed != prior or observed.get('status') != 'observed_empty' or observed.get('rows') != 0:
            raise ValueError('whole-market empty receipt changed; re-audit before registration')
        checks.append({'dataset': DATASET, 'data_id': product, 'partition': day,
                       'audited_missing_contracts': contracts,
                       'original_receipt': observed['receipt_path'],
                       'original_receipt_sha256': observed['receipt_sha256'],
                       # Reuse the existing source-check primitive; explicitly identify
                       # this pin as an official daily subset, not a vendor tick file.
                       'original_parquet': str(payload), 'original_parquet_sha256': evidence['data_sha256'],
                       'original_parquet_role': 'official_daily_positive_source_subset_not_finmind_tick',
                       'conflict_plan': str(evidence_path), 'conflict_plan_sha256': evidence_sha,
                       'conflict_plan_expires_at_utc': (now + timedelta(days=6)).isoformat()})
    if len(checks) > 64:
        raise ValueError('use separate evidenced finite batches for the existing 64-item waiter')
    result = register_evidenced(root, checks, now, output, dataset=DATASET, contract=CONTRACT,
        reason=('Check all official positive-volume option products on whole-market observed-empty days '
                'using existing per-product/day FinMind tasks; official daily subset is evidence, not tick data.'),
        additional_evidence_pins=pins)
    result.update(official_evidence=str(evidence_path), distinct_product_days=len(checks),
                  candidate_contract_keys=sum(len(row['audited_missing_contracts']) for row in checks))
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path('data_finmind/complement'))
    sub = parser.add_subparsers(dest='command', required=True)
    prepare = sub.add_parser('prepare')
    prepare.add_argument('--day', action='append', type=date.fromisoformat, required=True)
    prepare.add_argument('--output-dir', type=Path, required=True)
    register = sub.add_parser('register')
    register.add_argument('--evidence', type=Path, required=True)
    register.add_argument('--source-plan', type=Path, required=True)
    register.add_argument('--registration', type=Path, required=True)
    args = parser.parse_args()
    if args.command == 'prepare':
        path = build_evidence(args.root, args.day, args.output_dir)
        print(json.dumps({'manifest': str(path), **json.loads(path.read_text())}, ensure_ascii=False), flush=True)
    else:
        if args.registration.exists() or args.registration.is_symlink():
            raise FileExistsError('preserve the prior registration')
        result = register_checks(args.root, args.evidence, args.source_plan, datetime.now(UTC))
        atomic_write_json(args.registration, result)
        print(json.dumps(result, ensure_ascii=False), flush=True)


if __name__ == '__main__':
    main()
