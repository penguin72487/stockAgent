#!/usr/bin/env python3
"""Read-only full fixed-release validation with optional cooperative workers.

This invokes the canonical verifier and emits engineering evidence. It does not
write an original migration receipt, change a head, or authorize source cleanup.
"""
import argparse
import json
from pathlib import Path
import sys
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json
from stockagent.data_sync.packed_snapshots import resolve_packed_snapshot_id, verify_packed_snapshot
from stockagent.data_sync.packed_verification import configuration


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cold-root', type=Path, default=Path('/srv/stockagent-packed'))
    parser.add_argument('--dataset', required=True)
    parser.add_argument('--snapshot-id', required=True)
    parser.add_argument('--policy', type=Path, default=Path('/etc/stockagent/pack-verification.json'))
    parser.add_argument('--serial', action='store_true')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists() or args.output.is_symlink():
        raise ValueError('preserve existing verification evidence')
    started = time.monotonic()
    resolved = resolve_packed_snapshot_id(args.cold_root, args.dataset, args.snapshot_id, require_objects=False)
    policy = None if args.serial else configuration(args.dataset, args.policy)
    if not args.serial and policy is None:
        raise ValueError('dataset not explicitly enrolled for cooperative verification')
    if policy is not None:
        policy = {**policy, 'evidence_directory': str(args.output.parent/('pack-lanes-'+uuid.uuid4().hex))}
    progress_path = args.output.with_suffix('.progress.json')
    last, last_stage = 0.0, None
    def progress(row):
        nonlocal last, last_stage
        now = time.monotonic()
        if row.get('stage') != last_stage or now-last >= 5:
            last, last_stage = now, row.get('stage')
            atomic_write_json(progress_path, {**row, 'acceptance': False}, durable=False)
    native = args.cold_root in (Path('/srv/stockagent-packed'),
                               Path('/srv/stockagent-packed/.local-state/private-history'))
    proof = verify_packed_snapshot(args.cold_root, resolved, reconstruct_all=True,
                                  d_primary_native_pack_reads=native,
                                  d_primary_native_chunk_reads=native,
                                  adaptive_inventory_memory=native,
                                  pack_verification=policy, progress=progress)
    result = {'state': 'fixed_release_reconstruction_verified', 'dataset': args.dataset,
              'snapshot_id': args.snapshot_id, 'mode': 'serial' if args.serial else 'cooperative',
              'complete_wall_seconds': time.monotonic()-started, 'verification': proof,
              'scope': 'canonical full fixed-release engineering validation; original migration owner must accept separately',
              'source_retirement': False, 'source_or_cold_files_deleted': 0}
    atomic_write_json(args.output, result)
    print(json.dumps({'output': str(args.output), 'state': result['state'],
                      'seconds': result['complete_wall_seconds'],
                      'files': proof.get('independently_reconstructed_files'),
                      'execution': proof.get('pack_reconstruction_execution')}))


if __name__ == '__main__':
    main()
