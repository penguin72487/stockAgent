"""Reclaim one disposable compute-cache pair under its original key lock.

The caller owns source retention and must freshly hash the unchanged D object
against an independently reconstructed fixed release. This never retires D, a training result or a NAS
backup. Unknown/busy cache members are retained.
"""
from __future__ import annotations

import os
import json
import hashlib
from pathlib import Path
import re
import shutil
import stat
import subprocess
import time
import uuid

SOURCE_SHA256 = (hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
                 if '__file__' in globals() else globals().get('PRESSURE_WORKER_SHA256'))


def _generation(info):
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns,
            info.st_ctime_ns, info.st_mode, info.st_nlink, info.st_uid, info.st_gid)


def retire_cached_pair(cache, item, expected, *, references, record):
    """Retire one exact cache pair; retain a prepared journal on interruption."""
    if (not isinstance(expected, dict) or expected.get('source_verified') is not True
            or expected.get('object_sha256') != item.get('sha256')
            or expected.get('object_bytes') != item.get('bytes')
            or not isinstance(expected.get('source_manifest_sha256'), str)
            or re.fullmatch('[0-9a-f]{64}', expected['source_manifest_sha256']) is None
            or not isinstance(expected.get('source_snapshot_id'), str)
            or re.fullmatch('[A-Za-z0-9][A-Za-z0-9._-]{0,127}', expected['source_snapshot_id']) is None
            or expected.get('source_retirement') is not False):
        raise ValueError('an unchanged original source proof is required')
    with cache.lock(item) as (payload, metadata):
        proof = cache.read_proof(metadata)
        fields = ('object_sha256', 'object_bytes', 'worker_sha256',
                  'cache_worker_sha256', 'computed_at_unix')
        if (any(proof.get(key) != expected.get(key) for key in fields)
                or proof.get('inventory_bound') is not False):
            raise ValueError('cache proof differs from the exact staged object')
        info = payload.lstat()
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
                or info.st_mode & 0o077 or info.st_nlink != 1
                or info.st_size != item['bytes']):
            raise ValueError('cache payload is redirected, shared or changed')
        if references(payload, info):
            return {'state': 'busy', 'removed': False, 'released_allocated_bytes': 0}
        # Prepare is durable before any name is removed. If a process or host
        # dies between unlinks, the remaining journal is evidence, not success.
        prepared = {'state': 'prepared', 'object_sha256': item['sha256'],
                    'object_bytes': item['bytes'], 'source_generation': expected['source_generation'],
                    'source_manifest_sha256': expected['source_manifest_sha256'],
                    'source_snapshot_id': expected['source_snapshot_id'],
                    'allocated_bytes': info.st_blocks * 512, 'source_retirement': False,
                    'nas_acceptance': False}
        record(prepared)
        # Re-read both names after the process check and before unlink.
        if cache.read_proof(metadata) != proof or _generation(payload.lstat()) != _generation(info):
            raise ValueError('cache changed immediately before reclamation')
        payload.unlink()
        metadata.unlink()
        descriptor = os.open(cache.root, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        result = {**prepared, 'state': 'removed', 'removed': True,
                  'released_allocated_bytes': prepared['allocated_bytes']}
        record(result)
        return result


def _referenced(payload, _info):
    """A nonzero/unknown process census is not proof of an unused cache."""
    result = subprocess.run(['lsof', '-nP', '-t', '--', str(payload)],
                            capture_output=True, text=True, timeout=10)
    if result.returncode == 0:
        return True
    if result.returncode == 1 and not result.stdout.strip() and not result.stderr.strip():
        return False
    raise RuntimeError('remote process-reference census unavailable')


def _journal(root: Path, plan: str, digest: str, value: dict):
    """Private durable per-object intent; an interrupted unlink is never success."""
    directory = root / '.pressure-journal'
    directory.mkdir(mode=0o700, exist_ok=True)
    info = directory.lstat()
    if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid()
            or info.st_mode & 0o077 or directory.is_symlink()):
        raise ValueError('pressure journal directory is not private')
    target = directory / f'{plan}-{digest}.json'
    if target.is_symlink():
        raise ValueError('pressure journal is redirected')
    temporary = directory / ('.' + uuid.uuid4().hex + '.tmp')
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        with os.fdopen(fd, 'w') as stream:
            json.dump(value, stream, separators=(',', ':'), sort_keys=True)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, target)
        descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    finally:
        temporary.unlink(missing_ok=True)


def relieve(cache, request, *, references=_referenced):
    """Bounded reserve recovery for previously proved disposable copies only."""
    cache.guard()
    plan = request.get('plan')
    candidates = request.get('candidates')
    reserve, target = request.get('reserve_bytes'), request.get('target_free_bytes')
    if (not isinstance(plan, str) or re.fullmatch('[0-9a-f]{32}', plan) is None
            or not isinstance(candidates, list) or not 0 < len(candidates) <= 128
            or type(reserve) is not int or type(target) is not int
            or not 1024**3 <= reserve <= target <= reserve + 16 * 1024**3):
        raise ValueError('invalid bounded pressure relief plan')
    if request.get('boot_id') != Path('/proc/sys/kernel/random/boot_id').read_text().strip():
        raise ValueError('remote compute boot changed')
    before = shutil.disk_usage(cache.root).free
    result = {'state': 'not_needed' if before >= reserve else 'running',
              'free_before_bytes': before, 'reserve_bytes': reserve,
              'target_free_bytes': target, 'removed': [], 'busy': 0,
              'missing_or_changed': 0, 'failure_categories': {},
              'source_retirement': False, 'nas_acceptance': False}
    if before >= reserve:
        return result
    seen = set()
    for candidate in candidates:
        if shutil.disk_usage(cache.root).free >= target:
            break
        if not isinstance(candidate, dict) or set(candidate) != {'item', 'expected'}:
            raise ValueError('invalid pressure candidate')
        item, expected = candidate['item'], candidate['expected']
        digest = item.get('sha256') if isinstance(item, dict) else None
        if not isinstance(digest, str) or re.fullmatch('[0-9a-f]{64}', digest) is None or digest in seen:
            raise ValueError('duplicate or invalid pressure candidate')
        seen.add(digest)
        try:
            def record(value):
                _journal(cache.root, plan, digest, {**value, 'plan': plan, 'observed_at_unix': time.time()})
            row = retire_cached_pair(cache, item, expected, references=references, record=record)
        except BlockingIOError:
            result['busy'] += 1
            continue
        except FileNotFoundError:
            result['missing_or_changed'] += 1
            categories = result['failure_categories']
            categories['missing_pair'] = categories.get('missing_pair', 0) + 1
            continue
        except ValueError as error:
            result['missing_or_changed'] += 1
            categories = result['failure_categories']
            reason = {
                'an unchanged original source proof is required': 'source_proof',
                'cache proof differs from the exact staged object': 'cache_proof',
                'cache payload is redirected, shared or changed': 'payload_identity',
                'cache changed immediately before reclamation': 'generation_changed',
            }.get(str(error), 'other')
            categories[reason] = categories.get(reason, 0) + 1
            continue
        if row['removed']:
            result['removed'].append({'object_sha256': digest, 'object_bytes': item['bytes'],
                                      'released_allocated_bytes': row['released_allocated_bytes']})
    result['free_after_bytes'] = shutil.disk_usage(cache.root).free
    result['state'] = 'target_reached' if result['free_after_bytes'] >= target else 'partial'
    return result
