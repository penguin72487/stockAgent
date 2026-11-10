#!/usr/bin/env python3
"""Match staged E object identities to a peer-paired historical recovery index.

This is metadata provenance only: the historical index is explicitly not a
current NAS readback, a complete release, or deletion authorization.
"""
from __future__ import annotations

from collections import defaultdict
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.build_lab203_source_evidence import fixed_identity, load_fixed
from stockagent.data_sync import packed_snapshots as packed, workspace_volume
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json

CONTRACT = 'stockagent.staged_recovery_index_candidates.v1'
INDEX = Path('/srv/stockagent-backup-ingress-lab203/tools/recovery-index.json')
RECEIPT_ID = 'd42351d8614894bbb2a615e02a74396c9aadc4cd2cc8d940be175ba49bb7b1af'
RECEIPT = Path('/srv/stockagent-backup-receipts-lab203')/(
    'lab203-training-fixed-manifest-lookup-non-ack-v1-'+RECEIPT_ID+'.json')
OUTPUT = 'orphan-recovery-index-candidates-v1.json'
STATUS = 'orphan-recovery-index-status-v1.json'
MAX_INDEX_BYTES = 64*1024*1024
MAX_INDEX_FILES = 1_000_000
MAX_SOURCE_RECEIPTS = 128
RECEIPT_PREFIX = 'lab203-training-fixed-manifest-lookup-non-ack-v1-'
IMPLEMENTATION_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()


def _stamp(path: Path):
    value = path.lstat()
    if not stat.S_ISREG(value.st_mode) or path.is_symlink():
        raise SnapshotError('recovery index metadata is not a regular file')
    return [value.st_dev, value.st_ino, value.st_mode, value.st_size, value.st_mtime_ns, value.st_ctime_ns]


def _load(path: Path, maximum: int):
    before = _stamp(path)
    if before[3] <= 0 or before[3] > maximum:
        raise SnapshotError('recovery index metadata exceeds its bound')
    value, raw = load_fixed(path)
    if _stamp(path) != before:
        raise SnapshotError('recovery index metadata changed while reading')
    fixed_identity(value)
    return value, raw, before


def _local_identity(objects: dict, known: set[str], deferred: list) -> str:
    rows = sorted((sha, row['bytes'], row['relative'], row['E_signature']) for sha, row in objects.items())
    return hashlib.sha256(json.dumps({'objects': rows, 'known': sorted(known), 'deferred': deferred},
        sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def _result_identity(value: dict) -> str:
    body = {key: item for key, item in value.items() if key not in ('identity_sha256', 'last_checked_at_epoch')}
    return hashlib.sha256(json.dumps(body, ensure_ascii=False, sort_keys=True,
                                     separators=(',', ':'), allow_nan=False).encode()).hexdigest()


def _validate_primary(primary: dict, objects: dict, deferred: list) -> set[str]:
    if (not isinstance(primary, dict) or primary.get('contract') != 'stockagent.staged_cold_orphan_attribution.v1'
            or primary.get('state') != 'metadata_attributed' or primary.get('payloads_read') is not False
            or primary.get('complete_dataset_reconstructed') is not False
            or primary.get('deletion_authorized') is not False or not isinstance(primary.get('matches'), list)):
        raise SnapshotError('staged local attribution metadata is unavailable')
    expected = hashlib.sha256(json.dumps({'objects': sorted((sha, row['bytes'], row['E_signature'])
        for sha, row in objects.items()), 'deferred': deferred}, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    known = {row.get('sha256') for row in primary['matches'] if isinstance(row, dict)}
    if (primary.get('input_identity_sha256') != expected or len(known) != len(primary['matches'])
            or not known.issubset(objects)):
        raise SnapshotError('staged local attribution generation differs')
    return known


def _matching_source_receipt(anchor: dict, anchor_path: Path, index: dict, index_sha: str):
    """Select only a fixed receipt from the original peer and repository.

    A newer index is a provenance hint, not a NAS acceptance or permission to
    retire data. The fixed original receipt remains the pairing trust anchor.
    """
    def matches(value):
        return (value.get('contract') == anchor.get('contract')
            and value.get('not_NAS_or_recovery_ACK') is True
            and all(value.get(key) == anchor.get(key) for key in
                    ('paired_producer_device_id', 'paired_receiver_device_id', 'repository_id'))
            and value.get('repository_id') == index.get('repository_id')
            and value.get('source_index_file_sha256') == index_sha
            and value.get('source_index_identity_sha256') == index.get('identity_sha256'))

    if matches(anchor):
        return anchor, anchor_path, _stamp(anchor_path)
    candidates = list(anchor_path.parent.glob(RECEIPT_PREFIX+'*.json'))
    if len(candidates) > MAX_SOURCE_RECEIPTS:
        raise SnapshotError('recovery index source receipt scope exceeds its bound')
    for path in sorted(candidates):
        if not re.fullmatch(re.escape(RECEIPT_PREFIX)+r'[a-f0-9]{64}\.json', path.name):
            continue
        try:
            candidate, _, stamp = _load(path, 2*1024*1024)
        except (OSError, ValueError, TypeError, KeyError, SnapshotError):
            continue
        if (candidate.get('identity_sha256') == path.stem[len(RECEIPT_PREFIX):]
                and matches(candidate)):
            return candidate, path, stamp
    raise SnapshotError('recovery index identity or evidence scope has no matching source receipt')


def build(objects: dict, known: set[str], deferred: list, *, index_path=INDEX, receipt_path=RECEIPT):
    """Use only exact relative path, SHA-256 and length; read no staged payload."""
    receipt_path = Path(receipt_path)
    anchor, _, _ = _load(receipt_path, 2*1024*1024)
    if (anchor.get('contract') != 'stockagent.lab203_training_fixed_manifest_index_lookup_non_ack.v1'
            or anchor.get('identity_sha256') != RECEIPT_ID or anchor.get('not_NAS_or_recovery_ACK') is not True
            or any(not isinstance(anchor.get(key), str) or not anchor[key] for key in
                   ('paired_producer_device_id', 'paired_receiver_device_id', 'repository_id'))):
        raise SnapshotError('recovery index source receipt differs')
    index, raw, stamp = _load(Path(index_path), MAX_INDEX_BYTES)
    index_sha = hashlib.sha256(raw).hexdigest()
    if (index.get('contract') != 'fixed_nas_snapshot_recovery_index_v1'
            or index.get('nas_provenance_verified_by_index') is not False
            or index.get('full_history_backup_verified') is not False
            or index.get('canonical_reconstruction_verified') is not False):
        raise SnapshotError('recovery index identity or evidence scope differs')
    receipt, source_path, source_stamp = _matching_source_receipt(anchor, receipt_path, index, index_sha)
    deliveries = index.get('deliveries')
    if not isinstance(deliveries, list) or len(deliveries) > 4096:
        raise SnapshotError('recovery index delivery scope exceeds its bound')
    matches, visited = defaultdict(set), 0
    for delivery in deliveries:
        if not isinstance(delivery, dict) or not isinstance(delivery.get('files'), list):
            raise SnapshotError('recovery index delivery is invalid')
        envelope = packed._validate_hash(delivery.get('envelope_identity_sha256'), 'index envelope SHA')
        for row in delivery['files']:
            visited += 1
            if visited > MAX_INDEX_FILES or not isinstance(row, dict):
                raise SnapshotError('recovery index file scope is invalid or too large')
            sha = row.get('sha256')
            if (isinstance(sha, str) and sha in objects and type(row.get('bytes')) is int
                    and row['bytes'] == objects[sha]['bytes'] and row.get('relative') == objects[sha]['relative']):
                matches[sha].add(envelope)
    records = [{'sha256': sha, 'bytes': objects[sha]['bytes'], 'relative': objects[sha]['relative'],
                'locally_referenced': sha in known, 'historical_envelope_identities': sorted(envelopes)}
               for sha, envelopes in sorted(matches.items())]
    new = [row for row in records if not row['locally_referenced']]
    return {'contract': CONTRACT, 'state': 'metadata_index_matched',
        'source_index_sha256': index_sha, 'source_index_identity_sha256': index['identity_sha256'],
        'anchor_receipt_identity_sha256': anchor['identity_sha256'],
        'source_receipt_identity_sha256': receipt['identity_sha256'],
        'source_receipt_name': source_path.name, 'source_receipt_signature': source_stamp,
        'source_receipt_observed_at_utc': receipt.get('observed_at_utc'),
        'index_signature': stamp, 'objects_identity_sha256': _local_identity(objects, known, deferred),
        'objects_scanned': len(objects), 'deferred_batches': len(deferred),
        'historical_index_matched_objects': len(records), 'historical_index_matched_bytes': sum(r['bytes'] for r in records),
        'not_locally_referenced_objects': len(new), 'not_locally_referenced_bytes': sum(r['bytes'] for r in new),
        'records': records, 'payloads_read': False, 'current_NAS_readback': False,
        'complete_dataset_reconstructed': False, 'deletion_authorized': False,
        'implementation_sha256': IMPLEMENTATION_SHA256, 'observed_at_epoch': time.time()}


def refresh(root: Path, objects: dict, primary: dict, deferred: list, *, index_path=INDEX, receipt_path=RECEIPT):
    """Call under the original attribution lock; reuse unchanged metadata."""
    known = _validate_primary(primary, objects, deferred)
    target = root/OUTPUT
    if target.is_symlink():
        raise SnapshotError('recovery candidate output redirected')
    local_id, index_stamp, receipt_stamp = (_local_identity(objects, known, deferred),
                                             _stamp(Path(index_path)), _stamp(Path(receipt_path)))
    if target.is_file() and target.stat().st_size <= 2*1024*1024:
        try:
            previous = json.loads(target.read_bytes())
        except (OSError, ValueError):
            previous = {}
        source_name = previous.get('source_receipt_name') if isinstance(previous, dict) else None
        source_stamp = None
        if (isinstance(source_name, str) and
                (source_name == Path(receipt_path).name or re.fullmatch(
                    re.escape(RECEIPT_PREFIX)+r'[a-f0-9]{64}\.json', source_name))):
            try:
                source_stamp = _stamp(Path(receipt_path).parent/source_name)
            except OSError:
                pass
        if (isinstance(previous, dict) and previous.get('contract') == CONTRACT
                and previous.get('state') == 'metadata_index_matched'
                and previous.get('identity_sha256') == _result_identity(previous)
                and previous.get('implementation_sha256') == IMPLEMENTATION_SHA256
                and previous.get('primary_input_identity_sha256') == primary['input_identity_sha256']
                and previous.get('objects_identity_sha256') == local_id
                and previous.get('index_signature') == index_stamp
                and previous.get('receipt_signature') == receipt_stamp
                and previous.get('anchor_receipt_identity_sha256') == RECEIPT_ID
                and source_stamp is not None and previous.get('source_receipt_signature') == source_stamp
                and previous.get('payloads_read') is False and previous.get('current_NAS_readback') is False
                and previous.get('complete_dataset_reconstructed') is False and previous.get('deletion_authorized') is False):
            previous['last_checked_at_epoch'] = time.time()
            atomic_write_json(target, previous, durable=True)
            return previous
    result = build(objects, known, deferred, index_path=index_path, receipt_path=receipt_path)
    source = Path(receipt_path).parent/result['source_receipt_name']
    if (result['index_signature'] != index_stamp or _stamp(Path(index_path)) != index_stamp
            or _stamp(Path(receipt_path)) != receipt_stamp
            or _stamp(source) != result['source_receipt_signature']):
        raise SnapshotError('recovery source metadata changed during candidate matching')
    result['receipt_signature'] = receipt_stamp
    result['primary_input_identity_sha256'] = primary['input_identity_sha256']
    result['last_checked_at_epoch'] = time.time()
    result['identity_sha256'] = _result_identity(result)
    atomic_write_json(target, result, durable=True)
    return result


def refresh_with_status(root: Path, objects: dict, primary: dict, deferred: list):
    """A missing historical index must not stop the original attribution owner."""
    try:
        result = refresh(root, objects, primary, deferred)
        status = {'contract': CONTRACT+'.status', 'state': 'available',
                  'candidate_identity_sha256': result['identity_sha256'], 'observed_at_epoch': time.time()}
    except (OSError, ValueError, TypeError, KeyError, SnapshotError) as error:
        result = None
        status = {'contract': CONTRACT+'.status', 'state': 'unavailable',
                  'error_type': type(error).__name__, 'observed_at_epoch': time.time()}
    try:
        atomic_write_json(root/STATUS, status, durable=True)
    except (OSError, ValueError, SnapshotError):
        pass
    return result


def main():
    from scripts import attribute_staged_cold_objects as attribution
    config = workspace_volume.configuration()
    workspace_volume.check(c=config)
    root = workspace_volume.path('migration')/'d-cold-object-staging-v1'
    if root.is_symlink() or root.resolve(strict=True) != root:
        raise SnapshotError('staged attribution root redirected')
    fd = os.open(root/'orphan-attribution-owner.lock', os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        objects, deferred = attribution.collect(root)
        primary = attribution.staging._stable_json(root/'orphan-attribution-v1.json')
        result = refresh_with_status(root, objects, primary, deferred)
        if result is None:
            raise SnapshotError('historical recovery index candidate matching unavailable')
        print(json.dumps({key: result[key] for key in ('state', 'objects_scanned',
            'historical_index_matched_objects', 'historical_index_matched_bytes',
            'not_locally_referenced_objects', 'not_locally_referenced_bytes')}, ensure_ascii=False))
    finally:
        os.close(fd)


if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, TypeError, KeyError, SnapshotError) as error:
        print(f'{type(error).__name__}: {error}', file=sys.stderr)
        raise SystemExit(1)
