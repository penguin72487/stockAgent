#!/usr/bin/env python3
"""Explicitly retire the withdrawn, disposable Vast pack-verification cache.

Only closed owned cache pairs and their own orphan proofs are eligible. The
original per-object lock and fresh process-reference census remain in force;
pack data is neither extracted nor read back from D. Lock files, unknown
entries, training files, source generations and NAS data are untouched.
"""
from __future__ import annotations

import argparse
from contextlib import ExitStack
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import time
import uuid

from stockagent.data_sync import pack_compute_cache, packed_verification
from stockagent.data_sync.desync_snapshots import atomic_write_json
from stockagent.data_sync.materialized_cache import process_references_many

CONTRACT = 'stockagent.withdrawn_vast_verification_cache.v1'
DEPLOYED_ROOT = Path('/root/stockAgent/service_data/pack_compute_cache')
RECEIPT_ROOT = Path('/var/lib/stockagent-vast-verification-retirement')
SHA = re.compile(r'[a-f0-9]{64}')
BATCH = 128


def _signature(info):
    return (info.st_dev, info.st_ino, info.st_mode, info.st_size,
            info.st_mtime_ns, info.st_ctime_ns, info.st_nlink, info.st_uid)


def _owned(info):
    return (stat.S_ISREG(info.st_mode) and info.st_uid == os.geteuid()
            and not info.st_mode & 0o077 and info.st_nlink == 1)


def _proof_ok(proof, digest):
    return (isinstance(proof, dict) and proof.get('contract') == packed_verification.CONTRACT
            and proof.get('object_sha256') == digest and proof.get('inventory_bound') is False
            and type(proof.get('object_bytes')) is int
            and 0 < proof['object_bytes'] <= pack_compute_cache.MAX_OBJECT_BYTES
            and all(isinstance(proof.get(key), str) and SHA.fullmatch(proof[key])
                    for key in ('worker_sha256', 'cache_worker_sha256', 'originals_sha256')))


def inventory(cache):
    """Plan fixed regular names from closed cache proofs; unknown names stay."""
    cache.guard()
    rows, rejected = [], 0
    for proof_path in sorted(cache.root.glob('*.json')):
        digest = proof_path.stem
        if SHA.fullmatch(digest) is None:
            continue
        try:
            proof = cache.read_proof(proof_path)
            if not _proof_ok(proof, digest):
                raise ValueError('unrecognized cache proof')
            lock = cache.root/(digest+'.lock')
            lock_info = lock.lstat()
            if not _owned(lock_info):
                raise ValueError('unrecognized cache lock')
            payload = cache.root/(digest+'.zip')
            proof_sig = _signature(proof_path.lstat())
            if os.path.lexists(payload):
                info = payload.lstat()
                if not _owned(info) or info.st_size != proof['object_bytes']:
                    raise ValueError('cache payload changed or shared')
                payload_sig = _signature(info)
                allocated = info.st_blocks*512 + proof_path.lstat().st_blocks*512
            else:
                payload_sig = None
                allocated = proof_path.lstat().st_blocks*512
            rows.append({'digest': digest, 'object_bytes': proof['object_bytes'],
                         'payload_signature': payload_sig, 'proof_signature': proof_sig,
                         'lock_signature': _signature(lock_info),
                         'proof_sha256': hashlib.sha256(json.dumps(proof, sort_keys=True,
                             separators=(',', ':')).encode()).hexdigest(),
                         'allocated_bytes': allocated})
        except (OSError, ValueError, KeyError, TypeError):
            rejected += 1
    return rows, rejected


def retire(cache, receipt_dir: Path, *, apply=False, references=process_references_many):
    """Retire a new explicit plan in bounded, restartable key-locked batches."""
    if type(apply) is not bool:
        raise ValueError('cache withdrawal requires an explicit boolean')
    rows, rejected = inventory(cache)
    planned_bytes = sum(row['allocated_bytes'] for row in rows)
    result = {'contract': CONTRACT, 'state': 'planned', 'apply': apply,
              'planned_pairs': sum(row['payload_signature'] is not None for row in rows),
              'planned_orphan_proofs': sum(row['payload_signature'] is None for row in rows),
              'planned_allocated_bytes': planned_bytes, 'rejected_entries': rejected,
              'removed_pairs': 0, 'removed_orphan_proofs': 0, 'busy_or_changed': 0,
              'released_allocated_bytes': 0, 'source_retirement': False,
              'training_data_removed': False, 'nas_acceptance': False,
              'observed_at_epoch': time.time()}
    if not apply:
        return result
    receipt_dir = Path(receipt_dir).absolute()
    receipt_dir.mkdir(parents=True, mode=0o700, exist_ok=True)
    info = receipt_dir.lstat()
    if (receipt_dir.is_symlink() or receipt_dir.resolve(strict=True) != receipt_dir
            or not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o077):
        raise ValueError('cache withdrawal receipt root is not private')
    receipt = receipt_dir/(uuid.uuid4().hex+'.json')
    atomic_write_json(receipt, {**result, 'state': 'running'}, durable=True)
    try:
        for offset in range(0, len(rows), BATCH):
            cache.guard()
            with ExitStack() as owners:
                locked = []
                for row in rows[offset:offset+BATCH]:
                    item = {'kind': 'pack', 'sha256': row['digest'], 'bytes': row['object_bytes']}
                    try:
                        lock_path = cache.root/(row['digest']+'.lock')
                        if _signature(lock_path.lstat()) != tuple(row['lock_signature']):
                            raise ValueError('cache key lock changed')
                        payload, proof_path = owners.enter_context(cache.lock(item))
                        if _signature(lock_path.lstat()) != tuple(row['lock_signature']):
                            raise ValueError('cache key lock changed')
                        locked.append((row, payload, proof_path))
                    except (OSError, ValueError):
                        result['busy_or_changed'] += 1
                if references([cache.root]):
                    raise RuntimeError('verification cache is referenced by a running process')
                for row, payload, proof_path in locked:
                    try:
                        proof = cache.read_proof(proof_path)
                        proof_hash = hashlib.sha256(json.dumps(proof, sort_keys=True,
                            separators=(',', ':')).encode()).hexdigest()
                        if (proof_hash != row['proof_sha256'] or not _proof_ok(proof, row['digest'])
                                or _signature(proof_path.lstat()) != tuple(row['proof_signature'])):
                            raise ValueError('cache proof changed')
                        if row['payload_signature'] is None:
                            if os.path.lexists(payload):
                                raise ValueError('orphan proof gained a payload')
                        elif _signature(payload.lstat()) != tuple(row['payload_signature']):
                            raise ValueError('cache payload changed')
                        if row['payload_signature'] is not None:
                            payload.unlink()
                            result['removed_pairs'] += 1
                        else:
                            result['removed_orphan_proofs'] += 1
                        proof_path.unlink()
                        result['released_allocated_bytes'] += row['allocated_bytes']
                    except (OSError, ValueError, TypeError):
                        result['busy_or_changed'] += 1
            descriptor = os.open(cache.root, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            result['observed_at_epoch'] = time.time()
            atomic_write_json(receipt, {**result, 'state': 'running'}, durable=True)
        result['state'] = 'completed' if not result['busy_or_changed'] and not rejected else 'partial'
    except BaseException:
        atomic_write_json(receipt, {**result, 'state': 'interrupted',
                                    'observed_at_epoch': time.time()}, durable=True)
        raise
    atomic_write_json(receipt, result, durable=True)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    root = DEPLOYED_ROOT
    if root.is_symlink() or not root.is_dir() or root.resolve(strict=True) != root:
        raise ValueError('deployed verification cache root is unavailable or redirected')
    cache = pack_compute_cache.ComputeCache(root, packed_verification, 1024**3)
    print(json.dumps(retire(cache, RECEIPT_ROOT, apply=args.apply), sort_keys=True), flush=True)


if __name__ == '__main__':
    main()
