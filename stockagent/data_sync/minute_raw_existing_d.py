"""Bind one streamed minute-source generation to its existing D cold pack.

This compares only the original source-stream SHA index with the already
published compressed inventory. It never opens a packed data payload, makes
another pack, changes a D head, or claims independent D/NAS readback.
"""
from __future__ import annotations

from itertools import zip_longest
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import stat
import time

from stockagent.data_sync import packed_snapshots as p, stream_repack as s, workspace_relocation as relocation
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json
from stockagent.data_sync.packed_member_stream import read_stable_bytes

CONTRACT = 'stockagent.minute_raw_existing_D_coverage.v1'
DATASET = 'tw-minute-source-workspace'
COVERING_DATASET = 'tw-minute-source-cold'
PROOF_NAME = 'minute-raw-existing-D-coverage.json'
COLD = Path('/srv/stockagent-packed')
MATERIALIZED = Path('/srv/stockagent-packed-materialized')


def _source_stream_receipt(path: Path, expected: str, files: int, logical_bytes: int):
    raw = read_stable_bytes(path, maximum=128*1024)
    if hashlib.sha256(raw).hexdigest() != expected:
        raise SnapshotError('minute source-stream receipt changed')
    value = json.loads(raw)
    if (not isinstance(value, dict) or value.get('contract') != 'stockagent.source_stream_staging.v1'
            or value.get('state') != 'source_sha256_streamed_no_destination_readback'
            or value.get('source_sha256_streamed') is not True
            or value.get('destination_independent_readback') is not False
            or value.get('source_namespace_unchanged') is not True
            or value.get('source_files_deleted') != 0
            or value.get('files') != files or value.get('logical_bytes') != logical_bytes):
        raise SnapshotError('minute source-stream receipt scope differs')


def _index_signature(index: Path):
    index = Path(index).absolute()
    wal = index.with_name(index.name+'-wal')
    if (index.is_symlink() or not index.is_file()
            or wal.is_symlink() or wal.exists() and wal.stat().st_size):
        raise SnapshotError('minute source index is missing or has an uncheckpointed WAL')
    return relocation.signature(index)


def _pin(proof: dict, stage: Path, materialized: Path):
    materialized = Path(materialized).absolute()
    pin_root = materialized/'.stream-repack-pins'
    pin_dir = pin_root/COVERING_DATASET
    if (materialized.is_symlink() or not materialized.is_dir()
            or materialized.resolve(strict=True) != materialized
            or pin_root.is_symlink() or pin_dir.is_symlink()
            or not pin_dir.is_dir() or pin_dir.resolve(strict=True) != pin_dir):
        raise SnapshotError('minute D pin directory is missing or redirected')
    digest = p._validate_hash(proof.get('D_manifest_sha256'), 'minute D manifest SHA')
    snapshot = p.validate_slug(proof.get('D_snapshot_id'), 'minute D snapshot')
    path = pin_dir/s._repack_pin_name(digest, stage, scoped=True)
    value = {'contract': s.CONTRACT+'.source-pin', 'dataset': COVERING_DATASET,
             'manifest': {'snapshot_id': snapshot}, 'manifest_sha256': digest, 'stage': str(stage)}
    s._check_repack_pin((path, value))
    return path, value


def compare_index(cold: Path, resolved, index: Path, *, files: int, logical_bytes: int,
                  guard=lambda: None, progress=None) -> dict:
    """Match every path, kind, mode, length and source-stream SHA to D metadata."""
    before = _index_signature(index)
    source = resolved.manifest.get('source', {})
    if (source.get('snapshot_root_name') != 'shioaji_1m' or source.get('files') != files
            or source.get('logical_bytes') != logical_bytes or source.get('symlinks') != 0
            or source.get('omitted_files_above_maximum', 0) != 0
            or source.get('omitted_uncommitted_staging_files', 0) != 0
            or source.get('excluded_subtrees') != []):
        raise SnapshotError('existing minute D source scope or totals differ')
    count = matched_files = matched_bytes = 0
    last = time.monotonic()
    with sqlite3.connect(f'file:{index}?mode=ro', uri=True) as db:
        rows = db.execute('SELECT path,kind,sha,bytes,sig,state FROM files ORDER BY path COLLATE BINARY')
        for cold_row, local in zip_longest(p._iter_inventory(cold, resolved.manifest), rows):
            if cold_row is None or local is None:
                raise SnapshotError('existing minute D inventory length differs')
            name, kind, digest, size, encoded, state = local
            signature = json.loads(encoded)
            if (p._safe_relative_path(name, 'minute source member').as_posix() != name
                    or kind not in {'file', 'directory'} or not isinstance(signature, list)
                    or len(signature) != 9 or cold_row.get('path') != name or cold_row.get('kind') != kind
                    or cold_row.get('mode') != stat.S_IMODE(signature[2])):
                raise SnapshotError('existing minute D inventory member differs')
            if kind == 'file':
                p._validate_hash(digest, 'minute source-stream member SHA')
                if (state != 'verified' or type(size) is not int or size != signature[3]
                        or signature[8] != 1 or cold_row.get('size') != size
                        or cold_row.get('sha256') != digest):
                    raise SnapshotError('existing minute D file content or link scope differs')
                matched_files += 1
                matched_bytes += size
            count += 1
            if count % 8192 == 0:
                guard()
                if progress is not None and time.monotonic()-last >= 2:
                    last = time.monotonic()
                    progress({'stage': 'comparing_existing_D_inventory', 'files_completed': matched_files,
                              'files_total': files, 'logical_bytes_processed': matched_bytes,
                              'logical_bytes_total': logical_bytes})
    if ((matched_files, matched_bytes) != (files, logical_bytes)
            or not s._generation_matches(index, before, _index_signature(index))):
        raise SnapshotError('minute source index changed or did not match all D members')
    return {'matched_items': count, 'files': matched_files, 'logical_bytes': matched_bytes,
            'source_index_signature': before}


def prepare(cold: Path, stage: Path, index: Path, copy_result: Path, *, identity: str,
            copy_result_sha256: str, original_inode: int, original_lock_inode: int,
            files: int, logical_bytes: int, materialized: Path, guard=lambda: None, progress=None) -> dict:
    """Persist an exact D coverage proof and pin before any C retirement."""
    from stockagent.data_sync.index_archive_existing import _locked_head
    cold, stage, index, copy_result, materialized = (Path(v).absolute() for v in
                                                     (cold, stage, index, copy_result, materialized))
    p._validate_hash(identity, 'minute source identity')
    p._validate_hash(copy_result_sha256, 'minute stream receipt SHA')
    if (stage.is_symlink() or stage.resolve(strict=True) != stage
            or p._paths_overlap(cold, stage) or p._paths_overlap(cold, index)
            or type(original_inode) is not int or original_inode <= 0
            or type(original_lock_inode) is not int or original_lock_inode <= 0
            or type(files) is not int or files < 0 or type(logical_bytes) is not int or logical_bytes < 0):
        raise SnapshotError('minute D reuse scope differs')
    _source_stream_receipt(copy_result, copy_result_sha256, files, logical_bytes)
    path = stage/PROOF_NAME
    if os.path.lexists(path):
        proof = validate(cold, stage, index, copy_result, identity=identity,
                         copy_result_sha256=copy_result_sha256, original_inode=original_inode,
                         original_lock_inode=original_lock_inode, materialized=materialized, guard=guard)
        if (proof['files'], proof['logical_bytes']) != (files, logical_bytes):
            raise SnapshotError('minute existing D coverage totals changed')
        return proof
    with p._try_publication_lock(cold/'.local-state/locks/publish-retention-global.lock', shared=True):
        guard()
        resolved = _locked_head(cold, COVERING_DATASET)
        matched = compare_index(cold, resolved, index, files=files, logical_bytes=logical_bytes,
                                guard=guard, progress=progress)
        guard()
        pin = s._repack_pin(materialized, COVERING_DATASET, resolved.manifest_sha256,
                            resolved.manifest['snapshot_id'], stage, scoped=True)
        s._check_repack_pin(pin)
        proof = {'contract': CONTRACT, 'dataset': DATASET, 'source_identity': identity,
                 'source_index_path': str(index), 'source_index_signature': matched['source_index_signature'],
                 'copy_result_path': str(copy_result), 'copy_result_sha256': copy_result_sha256,
                 'original_inode': original_inode, 'original_lock_inode': original_lock_inode,
                 'D_dataset': COVERING_DATASET, 'D_snapshot_id': resolved.manifest['snapshot_id'],
                 'D_manifest_sha256': resolved.manifest_sha256,
                 'D_inventory_sha256': resolved.manifest['archive']['inventory']['sha256'],
                 'matched_items': matched['matched_items'], 'files': files, 'logical_bytes': logical_bytes,
                 'pin_stage': str(stage), 'source_sha_already_streamed': True,
                 'source_payload_read_again': False, 'independent_D_readback': False,
                 'NAS_accepted': False, 'observed_at_epoch': time.time()}
        proof['identity_sha256'] = s._identity(proof)
        atomic_write_json(path, proof, durable=True)
        return proof


def validate(cold: Path, stage: Path, index: Path, copy_result: Path, *, identity: str,
             copy_result_sha256: str, original_inode: int, original_lock_inode: int,
             materialized: Path, guard=lambda: None) -> dict:
    cold, stage, index, copy_result, materialized = (Path(v).absolute() for v in
                                                     (cold, stage, index, copy_result, materialized))
    p._validate_hash(identity, 'minute source identity')
    p._validate_hash(copy_result_sha256, 'minute stream receipt SHA')
    if (stage.is_symlink() or stage.resolve(strict=True) != stage
            or p._paths_overlap(cold, stage) or p._paths_overlap(cold, index)):
        raise SnapshotError('minute existing D coverage path differs')
    proof = json.loads(read_stable_bytes(stage/PROOF_NAME, maximum=128*1024))
    if (not isinstance(proof, dict) or proof.get('contract') != CONTRACT
            or proof.get('identity_sha256') != s._identity(proof)
            or proof.get('dataset') != DATASET or proof.get('source_identity') != identity
            or proof.get('source_index_path') != str(index) or proof.get('copy_result_path') != str(copy_result)
            or proof.get('copy_result_sha256') != copy_result_sha256
            or proof.get('original_inode') != original_inode or proof.get('original_lock_inode') != original_lock_inode
            or proof.get('D_dataset') != COVERING_DATASET or proof.get('pin_stage') != str(stage)
            or proof.get('source_sha_already_streamed') is not True or proof.get('source_payload_read_again') is not False
            or proof.get('independent_D_readback') is not False or proof.get('NAS_accepted') is not False
            or type(proof.get('files')) is not int or proof['files'] < 0
            or type(proof.get('logical_bytes')) is not int or proof['logical_bytes'] < 0
            or type(proof.get('matched_items')) is not int or proof['matched_items'] < proof['files']
            or not s._generation_matches(index, proof.get('source_index_signature'), _index_signature(index))):
        raise SnapshotError('minute existing D coverage proof differs')
    _source_stream_receipt(copy_result, copy_result_sha256, proof['files'], proof['logical_bytes'])
    _pin(proof, stage, materialized)
    guard()
    resolved = p.resolve_packed_snapshot_id(cold, COVERING_DATASET, proof['D_snapshot_id'], require_objects=True)
    if (resolved.manifest_sha256 != proof.get('D_manifest_sha256')
            or resolved.manifest['archive']['inventory']['sha256'] != proof.get('D_inventory_sha256')
            or resolved.manifest.get('source', {}).get('snapshot_root_name') != 'shioaji_1m'
            or resolved.manifest['source'].get('files') != proof['files']
            or resolved.manifest['source'].get('logical_bytes') != proof['logical_bytes']
            or resolved.manifest['source'].get('symlinks') != 0
            or type(resolved.manifest['source'].get('directories')) is not int
            or proof['matched_items'] != proof['files'] + resolved.manifest['source']['directories']):
        raise SnapshotError('pinned minute D generation differs')
    return proof


def retirement_preparation(source: Path, stage: Path, index: Path, *, dataset: str, identity: str,
                           original_inode: int, original_lock_inode: int):
    """Resolve the sole reviewed active handoff and its pinned D proof."""
    from stockagent.data_sync import provider_workspace, workspace_source_handoff as h, workspace_volume
    if dataset != DATASET:
        raise SnapshotError('existing D retirement is not enrolled for this dataset')
    source, stage, index = (Path(value).absolute() for value in (source, stage, index))
    with h.selected_source(h.MINUTE_RAW_NAME):
        value = h.read_journal()
        scope = h._scope()
        workspace = Path(workspace_volume.configuration()['mount_point'])
        if (not isinstance(value, dict) or value.get('state') != 'active'
                or source != scope.backup or index != Path(value.get('evidence', ''))/'files.sqlite'
                or stage != workspace/'migration/handoff-index-archive-v1'/DATASET/identity
                or value.get('original_inode') != original_inode
                or value.get('original_lock_inode') != original_lock_inode):
            raise SnapshotError('minute existing D retirement handoff differs')
        h._assert_retained(value)
        if not provider_workspace.exact_mount(scope.source, Path(value['destination'])):
            raise SnapshotError('minute existing D retirement E alias is unavailable')
        proof = validate(COLD, stage, index, Path(value['evidence'])/'copy-result.json', identity=identity,
                         copy_result_sha256=value['copy_result_sha256'], original_inode=original_inode,
                         original_lock_inode=original_lock_inode, materialized=MATERIALIZED)
    prepared = {'identity_sha256': proof['identity_sha256'], 'source_mode': 'retained_handoff_C_index',
                'source_root_inode': original_inode, 'source_index_signature': proof['source_index_signature'],
                'files': proof['files'], 'logical_bytes': proof['logical_bytes'],
                'preserved_paths': ['.download.lock'], 'supplemental_root_lock': True}
    return prepared, proof


def check_retirement_pin(proof: dict, stage: Path):
    """Recheck the durable canonical D pin at each retirement boundary."""
    _pin(proof, Path(stage).absolute(), MATERIALIZED)


def publication_receipt(proof: dict, stage: Path) -> dict:
    """Describe the already matched public D generation without inventing a pack."""
    from stockagent.data_sync import index_archive_existing as existing
    stage = Path(stage).absolute()
    match = {'contract': existing.CONTRACT, 'dataset': DATASET,
             'source_identity': proof['source_identity'], 'prepared_sha256': proof['identity_sha256'],
             'snapshot_id': proof['D_snapshot_id'], 'manifest_sha256': proof['D_manifest_sha256'],
             'root_name': 'shioaji_1m', 'excluded_coordination_paths': ['.download.lock'],
             'matched_items': proof['matched_items'], 'files': proof['files'],
             'logical_bytes': proof['logical_bytes'], 'source_payload_read': False,
             'independent_D_readback': False, 'NAS_accepted': False,
             'covering_dataset': COVERING_DATASET, 'covering_root_name': 'shioaji_1m',
             'coverage_prefix': '', 'covering_snapshot_pinned': True, 'pin_stage': str(stage),
             'pin_format': 'stage-v1', 'covering_authority': 'public'}
    match['identity_sha256'] = s._identity(match)
    receipt = {'contract': 'stockagent.source_local_exact_repack.v1',
               'state': 'existing_D_head_matches_source_inventory', 'dataset': DATASET,
               'old_manifest_sha256': proof['source_identity'], 'new_snapshot_id': proof['D_snapshot_id'],
               'new_manifest_sha256': proof['D_manifest_sha256'], 'files': proof['files'],
               'logical_bytes': proof['logical_bytes'], 'new_object_count': 0,
               'source_mode': 'retained_handoff_C_index', 'independent_D_readback': False,
               'NAS_accepted': False, 'existing_match': match, 'observed_at_epoch': time.time()}
    if not existing.receipt_matches(receipt, proof['identity_sha256']):
        raise SnapshotError('minute existing D publication receipt is not accepted')
    return receipt
