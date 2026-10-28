#!/usr/bin/env python3
"""ABBA compare full closed-delivery verifiers on one unchanged real batch.

No source mutation, cache clearing, transport publication or deletion. These are
complete transport verification timings, not an end-to-end NAS/retirement ETA.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.verify_backup_delivery import regular, verify
from stockagent.data_sync.desync_snapshots import atomic_write_json


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--batch', type=Path, required=True)
    parser.add_argument('--reference', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--workers', type=int, default=4, choices=range(1, 17))
    parser.add_argument('--maximum-bytes', type=int, default=128 * 1024**2)
    args = parser.parse_args()
    batch = args.batch.absolute()
    if args.output.exists() or args.output.absolute().is_relative_to(batch):
        parser.error('use a fresh output outside the fixed batch')
    raw = regular(batch, 'backup-envelope.json').read_bytes()
    envelope = json.loads(raw)
    if sum(r['bytes'] for r in envelope['files']) > args.maximum_bytes:
        parser.error('complete batch exceeds the explicit read-only benchmark budget')
    key = envelope['identity_sha256']
    before = args.reference.read_bytes()
    candidate_path = ROOT / 'scripts/verify_backup_delivery.py'
    candidate_before = candidate_path.read_bytes()
    spec = importlib.util.spec_from_file_location('retained_delivery_verifier', args.reference)
    if spec is None or spec.loader is None:
        parser.error('reference verifier cannot be imported')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    trials, expected = [], None
    result = {'scope': 'complete fixed closed-delivery verifier only', 'trials': trials,
              'state': 'running', 'ABBA_complete': False,
              'envelope_identity_sha256': key, 'native_windows_io': True,
              'reference_code_sha256': hashlib.sha256(before).hexdigest(),
              'candidate_code_sha256': hashlib.sha256(candidate_before).hexdigest(),
              'source_files_deleted': 0, 'NAS_acceptance_proved': False}
    atomic_write_json(args.output, result)
    try:
        for name, function in [('reference', module.verify), ('candidate', verify),
                               ('candidate', verify), ('reference', module.verify)]:
            start, cpu = time.perf_counter(), time.process_time()
            proof = function(batch, key, workers=args.workers, native_windows_io=True,
                             include_file_signatures=True)
            elapsed, used_cpu = time.perf_counter() - start, time.process_time() - cpu
            semantic = {k: v for k, v in proof.items() if k != 'observed_at_utc'}
            if expected is None:
                expected = semantic
            if semantic != expected:
                raise RuntimeError('complete delivery verification proofs differ')
            row = {'variant': name, 'workers': args.workers, 'seconds': elapsed,
                   'cpu_seconds': used_cpu, 'full_file_and_signature_proof_equal': True}
            trials.append(row)
            result.update(files=proof['files_verified'], bytes=proof['bytes_verified'])
            # Retain completed measurements if a later native launch fails.
            atomic_write_json(args.output, result)
            print(json.dumps(row), flush=True)
        if (regular(batch, 'backup-envelope.json').read_bytes() != raw
                or args.reference.read_bytes() != before or candidate_path.read_bytes() != candidate_before):
            raise RuntimeError('fixed benchmark code or envelope changed')
    except Exception as error:
        result.update(state='failed', error_type=type(error).__name__, error_reason=str(error))
        atomic_write_json(args.output, result)
        raise
    result.update(state='complete', ABBA_complete=True)
    atomic_write_json(args.output, result)
    print(json.dumps(result), flush=True)


if __name__ == '__main__':
    main()
