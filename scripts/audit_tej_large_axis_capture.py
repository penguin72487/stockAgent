#!/usr/bin/env python3
"""Independently read back one source-bound TEJ company-menu observation.

The resulting receipt is not a historical universe, market observation,
provider download, query-plan admission, or training input.
"""
from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
from pathlib import Path
import re
import sys

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.tej_company_axis_snapshot import verify_snapshot
from downloader.tej_history import DesktopBridge

NAME = re.compile(r'^([0-9a-f]{24}-[0-9a-f]{32})\.json\.company-axis\.snapshot\.json$')


def audit(root: Path, manifest_path: Path) -> dict:
    root = root.resolve(strict=True)
    manifest_path = manifest_path.absolute()
    match = NAME.fullmatch(manifest_path.name)
    if (not match or manifest_path.is_symlink() or not manifest_path.is_file()
            or manifest_path.parent.resolve(strict=True) != (root / 'raw').resolve(strict=True)
            or manifest_path.stat().st_size > 65536):
        raise ValueError('Exact retained TEJ snapshot manifest required')
    attempt = match.group(1)
    task_id = attempt[:24]
    manifest = json.loads(manifest_path.read_text(encoding='utf-8-sig'))
    request = root / 'requests' / (attempt + '.json')
    _, script_sha = DesktopBridge.bound_script_release(root, attempt, request)
    wire = json.loads(request.read_text(encoding='utf-8-sig'))
    claim = root / 'large_axis_capture_intents' / 'claimed' / (attempt + '.json')
    if claim.is_symlink() or not claim.is_file() or claim.stat().st_size > 65536:
        raise ValueError('Exact immutable capture intent claim required')
    intent = json.loads(claim.read_text(encoding='utf-8-sig'))
    binary_name = attempt + '.json.company-axis.snapshot.bin'
    binary = root / 'raw' / binary_name
    if (manifest.get('contract') != 'tej_large_company_axis_two_pass_observation_v1'
            or manifest.get('task_id') != task_id or manifest.get('query_attempt_id') != attempt
            or manifest.get('binary_name') != binary_name
            or manifest.get('market_data_query_submitted') is not False
            or manifest.get('full_axis_admitted') is not False
            or manifest.get('complete_ordered_live_reads') != 2
            or not all(manifest.get(side, {}).get('binding_readback_verified') is True
                       and manifest.get(side, {}).get('vendor_notices_absent') is True
                       for side in ('before_scope', 'after_scope'))
            or manifest.get('prepared_request_sha256') != sha256_file(request)
            or manifest.get('bridge_script_sha256') != script_sha
            or manifest.get('intent_sha256') != sha256_file(claim)
            or intent.get('contract') != 'tej_large_company_axis_capture_once_v1'
            or intent.get('task_id') != task_id or intent.get('market_data_query_authorized') is not False
            or intent.get('prepared_request_sha256') != manifest.get('prepared_request_sha256')
            or intent.get('bridge_script_sha256') != script_sha
            or intent.get('priority_request_id') != manifest.get('priority_request_id')
            or wire.get('action') != 'plan' or wire.get('task_id') != task_id
            or any(manifest.get(k) != wire.get(k) or intent.get(k) != wire.get(k)
                   for k in ('type', 'smart_id', 'table'))):
        raise ValueError('Capture request, source binding or one-shot claim differs')
    sample_name = intent.get('prior_sample_basename', '')
    if not re.fullmatch(r'[0-9a-f]{24}-[0-9a-f]{32}\.json\.company-axis-sample\.json', sample_name):
        raise ValueError('Prior sample identity differs')
    sample_path = root / 'raw' / sample_name
    if (sample_path.is_symlink() or not sample_path.is_file()
            or sha256_file(sample_path) != intent.get('prior_sample_sha256')
            or manifest.get('prior_sample_sha256') != intent.get('prior_sample_sha256')):
        raise ValueError('Prior sample content differs')
    sample = json.loads(sample_path.read_text(encoding='utf-8-sig'))
    if (sample.get('task_id') != task_id or sample.get('full_axis_admitted') is not False
            or sample.get('prepared_request_sha256') != manifest['prepared_request_sha256']
            or sample.get('available_count') != intent.get('expected_count')):
        raise ValueError('Prior sample source or count differs')
    proof = verify_snapshot(binary)
    if (proof['sha256'] != manifest.get('binary_sha256') or proof['bytes'] != manifest.get('binary_bytes')
            or proof['count'] != manifest.get('labels') or proof['count'] != intent.get('expected_count')
            or not 150000 < proof['count'] <= 2_000_000):
        raise ValueError('Independent snapshot framing, hash or count differs')
    return {'contract': 'tej_large_company_axis_independent_readback_v1',
            'observed_at_utc': datetime.now(UTC).isoformat(), 'task_id': task_id, 'query_attempt_id': attempt,
            'type': wire['type'], 'smart_id': wire['smart_id'], 'table': wire['table'],
            'snapshot_path': str(binary), 'snapshot_manifest_path': str(manifest_path),
            'snapshot_manifest_sha256': sha256_file(manifest_path), 'source_proof': proof,
            'provider_queries_by_audit': 0, 'market_observations_obtained': False,
            'full_axis_admitted_to_plan': False, 'training_input_admitted': False,
            'limitations': ['Current source menu only; no historical eligibility or native price proof.',
                            'Unverified expiry, reused codes, summary labels and duplicate semantics remain.']}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=REPO / 'data_tej')
    parser.add_argument('--manifest', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise FileExistsError('Retain the existing independent readback')
    receipt = audit(args.root, args.manifest)
    atomic_write_json(args.output, receipt, durable=True)
    print(json.dumps({'receipt': str(args.output), 'count': receipt['source_proof']['count'],
                      'source_data_admitted': False}, ensure_ascii=False))


if __name__ == '__main__':
    main()
